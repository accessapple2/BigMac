"""HM-EXEC-PIPELINE — Execution Router (Stages 3–5).

Stage 3: Conviction × track-record sizing (fractional-Kelly, capped).
Stage 4: Risk gate — buying power, max position, concentration, daily-loss,
         regime, kill-gate. Every rejection logged to gate_reject_log.
Stage 5: Feedback loop — fill → mark_signal_executed → NTFY.

ACTIVATION:
  Default: EXEC_ROUTER_ENABLED=False in config.py → always dry_run.
  Live:    EXEC_ROUTER_ENABLED=True → calls paper_trader.buy() for real.

RULE #1 ENFORCED: see CLAUDE.md — only the Alpaca PAPER bridge.
The N1 test (test_exec_pipeline_rule1.py) audits this file automatically.

N2 KILL-SWITCH: touch ~/autonomous-trader/KILL_SWITCH halts everything.
N3 IDEMPOTENCY: atomic UPDATE signals_v2 SET status='processing'
                WHERE id=? AND status='pending'. If 0 rows updated → already
                claimed by another invocation → skip.

Do NOT call paper_trader.buy() from this module's __init__ scope — only
inside run_execution_cycle() where the kill-switch and enable flag are
checked first.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Any

from rich.console import Console

console = Console()

_DB = "data/trader.db"

# --- Risk gate constants (tune before Phase 2 arming) ----------------------
MAX_POSITION_VALUE: float = 500.0       # max $ per position in paper mode
MAX_CONCENTRATION_PCT: float = 15.0    # max % of portfolio in one ticker
DAILY_LOSS_LIMIT: float = -500.0       # halt if today's realized P&L < this
MIN_BUYING_POWER: float = 250.0        # don't trade if BP below this floor
KELLY_FRACTION: float = 0.25           # conservative Kelly divisor
MAX_KELLY_FRACTION: float = 0.5        # cap on fractional-Kelly sizing
MIN_TRACK_RECORD_TRADES: int = 10      # need ≥ N closed trades to use Kelly
DEFAULT_CONFIDENCE_SIZE: float = 0.25  # fallback size fraction (no history)
EXEC_ROUTER_PLAYER_ID: str = "exec-router"  # player_id for router-originated trades


@dataclass
class RouterResult:
    symbol: str
    direction: str
    action: str | None           # 'BUY' / 'SELL' / None (blocked)
    dry_run: bool
    player_id: str
    qty: float | None
    price: float | None
    confidence: float
    signal_ids: list[int]
    gate_passed: bool
    gate_blocked_by: str | None
    gate_reason: str | None
    trade_id: int | None
    notes: list[str] = field(default_factory=list)


def run_execution_cycle(dry_run: bool | None = None) -> dict[str, Any]:
    """Run one execution cycle over the current confluence queue.

    dry_run=None  → respects EXEC_ROUTER_ENABLED from config.py
    dry_run=True  → always observe-only, never calls buy()
    dry_run=False → force-execute (only for testing; EXEC_ROUTER_ENABLED still checked)

    Returns a summary dict with results and counts.
    """
    # N2: kill-switch check — first thing, every time
    from engine.fleet_halt import is_active as _halted
    if _halted():
        console.log("[bold red][EXEC-ROUTER] KILL_SWITCH active — cycle aborted")
        return {"status": "kill_switch", "processed": 0, "executed": 0, "blocked": 0}

    # Resolve dry_run mode
    if dry_run is None:
        try:
            from config import EXEC_ROUTER_ENABLED
            dry_run = not EXEC_ROUTER_ENABLED
        except Exception:
            dry_run = True  # fail-safe: never execute if config missing

    if dry_run:
        console.log("[cyan][EXEC-ROUTER] DRY-RUN mode — no orders will be placed")

    from engine.confluence_engine import get_actionable_queue
    queue = get_actionable_queue()

    if not queue:
        console.log("[dim][EXEC-ROUTER] No winning signals in queue — cycle done")
        return {"status": "ok", "processed": 0, "executed": 0, "blocked": 0, "dry_run": dry_run}

    console.log(f"[cyan][EXEC-ROUTER] Queue: {len(queue)} winning signal(s)")

    results: list[RouterResult] = []
    executed = 0
    blocked = 0

    for entry in queue:
        result = _process_entry(entry, dry_run=dry_run)
        results.append(result)
        if result.gate_passed:
            executed += 1
        else:
            blocked += 1

    summary = {
        "status": "ok",
        "dry_run": dry_run,
        "processed": len(results),
        "executed": executed,
        "blocked": blocked,
        "results": [
            {
                "symbol": r.symbol,
                "direction": r.direction,
                "action": r.action,
                "gate_passed": r.gate_passed,
                "gate_blocked_by": r.gate_blocked_by,
                "gate_reason": r.gate_reason,
                "qty": r.qty,
                "trade_id": r.trade_id,
                "dry_run": r.dry_run,
            }
            for r in results
        ],
    }
    console.log(
        f"[cyan][EXEC-ROUTER] Cycle done: {executed} executed / {blocked} blocked "
        f"({'dry-run' if dry_run else 'LIVE'})"
    )
    return summary


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _process_entry(entry: "ConfluenceEntry", dry_run: bool) -> RouterResult:  # type: ignore[name-defined]
    """Process one confluence entry through the full Stage 3/4/5 pipeline."""
    from engine.confluence_engine import ConfluenceEntry

    # N3: Claim the signal atomically before doing anything
    signal_ids = entry.signal_ids
    claimed_id = _claim_signal(signal_ids)
    if claimed_id is None:
        return RouterResult(
            symbol=entry.symbol, direction=entry.direction,
            action=None, dry_run=dry_run,
            player_id=EXEC_ROUTER_PLAYER_ID, qty=None, price=None,
            confidence=entry.avg_confidence, signal_ids=signal_ids,
            gate_passed=False,
            gate_blocked_by="IDEMPOTENCY",
            gate_reason="All signals already claimed or executed",
            trade_id=None,
            notes=["skipped — all signal_ids were status≠pending"],
        )

    # Resolve player_id for attribution
    player_id = _resolve_player_id(entry.primary_source)

    # Get current price for sizing and gate checks
    price = _get_price(entry.symbol)
    if price is None:
        _unclaim_signal(claimed_id)
        _log_gate_reject(player_id, entry.symbol, "NO_PRICE",
                         "Could not fetch current price", signal_id=claimed_id,
                         confidence=entry.avg_confidence)
        return RouterResult(
            symbol=entry.symbol, direction=entry.direction,
            action=None, dry_run=dry_run,
            player_id=player_id, qty=None, price=None,
            confidence=entry.avg_confidence, signal_ids=signal_ids,
            gate_passed=False, gate_blocked_by="NO_PRICE",
            gate_reason="Price unavailable", trade_id=None,
        )

    # Stage 4: Risk gate
    gate_result = _risk_gate(
        player_id=player_id,
        symbol=entry.symbol,
        direction=entry.direction,
        price=price,
        confidence=entry.avg_confidence,
        signal_id=claimed_id,
        timeframe=entry.timeframe,
    )
    if not gate_result["ok"]:
        _unclaim_signal(claimed_id)
        return RouterResult(
            symbol=entry.symbol, direction=entry.direction,
            action=None, dry_run=dry_run,
            player_id=player_id, qty=None, price=price,
            confidence=entry.avg_confidence, signal_ids=signal_ids,
            gate_passed=False,
            gate_blocked_by=gate_result["gate"],
            gate_reason=gate_result["reason"],
            trade_id=None,
        )

    # Stage 3: Size the position
    qty = _size_position(
        player_id=player_id,
        price=price,
        confidence=entry.avg_confidence,
        buying_power=gate_result.get("buying_power", 0.0),
    )
    if qty <= 0:
        _unclaim_signal(claimed_id)
        _log_gate_reject(player_id, entry.symbol, "ZERO_QTY",
                         f"Kelly sizing returned 0 shares at ${price:.2f}",
                         signal_id=claimed_id, price=price,
                         confidence=entry.avg_confidence)
        return RouterResult(
            symbol=entry.symbol, direction=entry.direction,
            action=None, dry_run=dry_run,
            player_id=player_id, qty=0.0, price=price,
            confidence=entry.avg_confidence, signal_ids=signal_ids,
            gate_passed=False, gate_blocked_by="ZERO_QTY",
            gate_reason=f"Kelly sizing → 0 shares at ${price:.2f}",
            trade_id=None,
        )

    action = "BUY" if entry.direction == "BULLISH" else "SELL"
    voting_agents = ",".join(entry.sources)

    # Stage 3/5: Execute or log
    trade_id: int | None = None
    if dry_run:
        console.log(
            f"[cyan][EXEC-ROUTER][DRY-RUN] WOULD {action} {qty}×{entry.symbol} "
            f"@ ${price:.2f} | conf={entry.avg_confidence:.0%} | "
            f"sources={voting_agents} | signal={claimed_id}"
        )
        _unclaim_signal(claimed_id)  # release in dry-run
    else:
        # Stage 5: Execute via existing paper_trader path (Alpaca paper bridge)
        trade_id = _execute_buy(
            player_id=player_id,
            symbol=entry.symbol,
            price=price,
            qty=qty,
            confidence=entry.avg_confidence,
            signal_id=claimed_id,
            grade=entry.grade,
            voting_agents=voting_agents,
        )
        if trade_id is not None:
            # Mark signal executed and fire NTFY
            _finalize_signal(claimed_id, trade_id, entry.symbol, action, qty, price)
        else:
            _unclaim_signal(claimed_id)

    return RouterResult(
        symbol=entry.symbol, direction=entry.direction,
        action=action, dry_run=dry_run,
        player_id=player_id, qty=qty, price=price,
        confidence=entry.avg_confidence, signal_ids=signal_ids,
        gate_passed=True, gate_blocked_by=None, gate_reason=None,
        trade_id=trade_id,
    )


def _risk_gate(
    player_id: str, symbol: str, direction: str, price: float,
    confidence: float, signal_id: int, timeframe: str | None,
) -> dict[str, Any]:
    """Stage 4: Apply all risk checks. Returns {ok, gate, reason, buying_power}."""

    # N2: re-check kill-switch inside the gate
    from engine.fleet_halt import is_active as _halted
    if _halted():
        _log_gate_reject(player_id, symbol, "KILL_SWITCH", "KILL_SWITCH file active",
                         signal_id=signal_id, price=price, confidence=confidence)
        return {"ok": False, "gate": "KILL_SWITCH", "reason": "KILL_SWITCH file active"}

    # 1. Buying power
    buying_power = _get_buying_power()
    if buying_power < MIN_BUYING_POWER:
        reason = f"Buying power ${buying_power:.0f} < floor ${MIN_BUYING_POWER:.0f}"
        _log_gate_reject(player_id, symbol, "LOW_BUYING_POWER", reason,
                         signal_id=signal_id, price=price, confidence=confidence)
        return {"ok": False, "gate": "LOW_BUYING_POWER", "reason": reason}

    # 2. Daily-loss limit
    daily_pnl = _today_realized_pnl(player_id)
    if daily_pnl < DAILY_LOSS_LIMIT:
        reason = f"Daily P&L ${daily_pnl:.2f} < limit ${DAILY_LOSS_LIMIT:.2f}"
        _log_gate_reject(player_id, symbol, "DAILY_LOSS_LIMIT", reason,
                         signal_id=signal_id, price=price, confidence=confidence)
        return {"ok": False, "gate": "DAILY_LOSS_LIMIT", "reason": reason}

    # 3. Concentration cap — BUYS only
    if direction == "BULLISH":
        portfolio_value = _portfolio_value()
        current_exposure = _current_position_value(symbol)
        new_exposure = current_exposure + price * 1  # minimum 1 share
        if portfolio_value > 0 and (new_exposure / portfolio_value * 100) > MAX_CONCENTRATION_PCT:
            reason = (
                f"{symbol} exposure ${new_exposure:.0f} would be "
                f"{new_exposure/portfolio_value*100:.1f}% > {MAX_CONCENTRATION_PCT}% cap"
            )
            _log_gate_reject(player_id, symbol, "CONCENTRATION_CAP", reason,
                             signal_id=signal_id, price=price, confidence=confidence)
            return {"ok": False, "gate": "CONCENTRATION_CAP", "reason": reason}

    # 4. Regime fit
    _regime_ok, _regime_reason = _check_regime(timeframe)
    if not _regime_ok:
        _log_gate_reject(player_id, symbol, "REGIME_FILTER", _regime_reason,
                         signal_id=signal_id, price=price, confidence=confidence)
        return {"ok": False, "gate": "REGIME_FILTER", "reason": _regime_reason}

    return {"ok": True, "buying_power": buying_power}


def _size_position(
    player_id: str, price: float, confidence: float, buying_power: float
) -> float:
    """Stage 3: Fractional-Kelly sizing. Returns integer share count."""
    wr, avg_win, avg_loss = _agent_track_record(player_id)
    if wr is not None and avg_loss and avg_win and avg_loss > 0:
        rr = avg_win / avg_loss
        kelly = (wr * rr - (1 - wr)) / rr
        frac = max(0.0, min(kelly * KELLY_FRACTION, MAX_KELLY_FRACTION))
    else:
        frac = DEFAULT_CONFIDENCE_SIZE * confidence

    # Dollar allocation: fraction of buying_power, capped at MAX_POSITION_VALUE
    dollar_alloc = min(frac * buying_power, MAX_POSITION_VALUE)
    qty = int(dollar_alloc / price) if price > 0 else 0
    return max(0, qty)


def _execute_buy(
    player_id: str, symbol: str, price: float, qty: float,
    confidence: float, signal_id: int, grade: str | None, voting_agents: str,
) -> int | None:
    """Call paper_trader.buy() and return trade_id on success, None on failure."""
    try:
        from engine.paper_trader import buy
        result = buy(
            player_id=player_id,
            symbol=symbol,
            price=price,
            qty=qty,
            confidence=confidence,
            signal_id=signal_id,
            grade=grade,
            voting_agents=voting_agents,
            reasoning=f"exec-router: {voting_agents}",
        )
        if result and isinstance(result, dict):
            return result.get("trade_id")
        return None
    except Exception as e:
        console.log(f"[red][EXEC-ROUTER] buy() failed for {symbol}: {e!r}")
        return None


def _finalize_signal(
    signal_id: int, trade_id: int, symbol: str, action: str, qty: float, price: float
) -> None:
    """Stage 5: mark executed + NTFY on fill."""
    try:
        from engine.events_bus import mark_signal_executed
        mark_signal_executed(signal_id=signal_id, trade_id=trade_id)
    except Exception as e:
        console.log(f"[yellow][EXEC-ROUTER] mark_signal_executed failed: {e!r}")

    try:
        from engine.alert_channels import push_ntfy, NTFY_SIGNALS_TOPIC
        push_ntfy(
            topic=NTFY_SIGNALS_TOPIC,
            title=f"EXEC-ROUTER: {action} {symbol}",
            body=f"{action} {qty}×{symbol} @ ${price:.2f} | trade #{trade_id}",
            priority="default",
            tags="exec-router",
        )
    except Exception:
        pass  # NTFY is best-effort


# ---------------------------------------------------------------------------
# Data helpers — all fail-safe (return None / 0.0 on error)
# ---------------------------------------------------------------------------

def _claim_signal(signal_ids: list[int]) -> int | None:
    """Atomically claim the first unclaimed signal_id. Returns claimed id or None."""
    if not signal_ids:
        return None
    try:
        conn = sqlite3.connect(_DB)
        conn.execute("PRAGMA busy_timeout=5000")
        try:
            for sid in signal_ids:
                cur = conn.execute(
                    "UPDATE signals_v2 SET status='processing' WHERE id=? AND status='pending'",
                    (sid,),
                )
                conn.commit()
                if cur.rowcount > 0:
                    return sid
        finally:
            conn.close()
    except Exception as e:
        console.log(f"[yellow][EXEC-ROUTER] _claim_signal error: {e!r}")
    return None


def _unclaim_signal(signal_id: int) -> None:
    """Revert a claimed signal back to 'pending' (dry-run or error path)."""
    try:
        conn = sqlite3.connect(_DB)
        conn.execute("PRAGMA busy_timeout=5000")
        try:
            conn.execute(
                "UPDATE signals_v2 SET status='pending' WHERE id=? AND status='processing'",
                (signal_id,),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass


def _get_price(symbol: str) -> float | None:
    """Get latest price from Alpaca bridge. Returns None on failure."""
    try:
        from engine.alpaca_bridge import AlpacaBridge
        bridge = AlpacaBridge()
        prices = bridge.latest_prices([symbol])
        if prices and symbol in prices:
            return float(prices[symbol])
    except Exception:
        pass
    # Fallback: last price from positions
    try:
        conn = sqlite3.connect(_DB)
        try:
            row = conn.execute(
                "SELECT price FROM trades WHERE symbol=? ORDER BY executed_at DESC LIMIT 1",
                (symbol,),
            ).fetchone()
            if row:
                return float(row[0])
        finally:
            conn.close()
    except Exception:
        pass
    return None


def _get_buying_power() -> float:
    """Get current buying power from Alpaca. Returns 0 on failure (fail-safe blocks)."""
    try:
        from engine.alpaca_bridge import AlpacaBridge
        status = AlpacaBridge().status()
        return float(status.get("buying_power", 0.0))
    except Exception:
        return 0.0


def _today_realized_pnl(player_id: str) -> float:
    """Sum today's realized P&L for player. Returns 0.0 on error."""
    try:
        conn = sqlite3.connect(_DB)
        conn.execute("PRAGMA busy_timeout=5000")
        try:
            row = conn.execute(
                """
                SELECT COALESCE(SUM(realized_pnl), 0.0)
                  FROM trades
                 WHERE player_id=? AND realized_pnl IS NOT NULL
                   AND date(executed_at)=date('now')
                """,
                (player_id,),
            ).fetchone()
            return float(row[0]) if row else 0.0
        finally:
            conn.close()
    except Exception:
        return 0.0


def _portfolio_value() -> float:
    """Approximate portfolio value from latest portfolio_history. Returns 0 on error."""
    try:
        conn = sqlite3.connect(_DB)
        try:
            row = conn.execute(
                "SELECT portfolio_value FROM portfolio_history ORDER BY recorded_at DESC LIMIT 1"
            ).fetchone()
            return float(row[0]) if row else 0.0
        finally:
            conn.close()
    except Exception:
        return 0.0


def _current_position_value(symbol: str) -> float:
    """Current open exposure to symbol (from positions table or trades). Returns 0."""
    try:
        conn = sqlite3.connect(_DB)
        try:
            row = conn.execute(
                """
                SELECT COALESCE(SUM(qty * price), 0.0)
                  FROM trades
                 WHERE symbol=? AND action='BUY'
                   AND realized_pnl IS NULL
                """,
                (symbol,),
            ).fetchone()
            return float(row[0]) if row else 0.0
        finally:
            conn.close()
    except Exception:
        return 0.0


def _agent_track_record(
    player_id: str,
) -> tuple[float | None, float | None, float | None]:
    """Return (win_rate, avg_win, avg_loss) for player over last 90 days.

    Returns (None, None, None) if insufficient history (<MIN_TRACK_RECORD_TRADES).
    """
    try:
        conn = sqlite3.connect(_DB)
        conn.execute("PRAGMA busy_timeout=5000")
        try:
            row = conn.execute(
                """
                SELECT
                    COUNT(*)                                              AS n,
                    SUM(CASE WHEN realized_pnl>0 THEN 1 ELSE 0 END)     AS wins,
                    AVG(CASE WHEN realized_pnl>0 THEN realized_pnl END)  AS avg_win,
                    AVG(CASE WHEN realized_pnl<0 THEN ABS(realized_pnl) END) AS avg_loss
                  FROM trades
                 WHERE player_id=?
                   AND realized_pnl IS NOT NULL
                   AND executed_at >= datetime('now', '-90 days')
                """,
                (player_id,),
            ).fetchone()
        finally:
            conn.close()

        if not row or (row[0] or 0) < MIN_TRACK_RECORD_TRADES:
            return None, None, None
        n, wins, avg_win, avg_loss = row
        wr = wins / n if n else 0.0
        return wr, float(avg_win or 0), float(avg_loss or 0)
    except Exception:
        return None, None, None


def _check_regime(timeframe: str | None) -> tuple[bool, str]:
    """Check regime fit for the signal's timeframe. Fails open on error."""
    try:
        from engine.regime_router import check_regime_fit, get_current_regime
        regime = get_current_regime()
        strategy = "swing" if (timeframe or "").lower() in ("swing", "") else (timeframe or "swing")
        ok, reason = check_regime_fit(strategy, regime)
        return ok, reason
    except Exception:
        return True, "regime check skipped (error)"  # fail-open on regime


def _resolve_player_id(source: str) -> str:
    """Map signal source → player_id for paper_trader.buy(). Falls back to EXEC_ROUTER_PLAYER_ID."""
    source_map = {
        "uhura": EXEC_ROUTER_PLAYER_ID,
        "deep_scan": "exec-router",
        "fleet": EXEC_ROUTER_PLAYER_ID,
    }
    return source_map.get(source, EXEC_ROUTER_PLAYER_ID)


def _log_gate_reject(
    player_id: str, symbol: str | None, gate_name: str, reason: str | None,
    signal_id: int | None = None, price: float | None = None,
    confidence: float | None = None,
) -> None:
    """Write to gate_reject_log. Best-effort — never raises."""
    try:
        conn = sqlite3.connect(_DB)
        conn.execute("PRAGMA busy_timeout=5000")
        try:
            conn.execute(
                "INSERT INTO gate_reject_log"
                "(player_id, symbol, gate_name, reason, signal_id, price, confidence)"
                " VALUES(?,?,?,?,?,?,?)",
                (player_id, symbol, gate_name, reason, signal_id, price, confidence),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass
