"""
Execution Routing Model

This module enforces a strict separation between:

1. AGENTS (signal generators)
   - Produce BUY / SELL / HOLD decisions
   - Include confidence and reasoning
   - Example: super-agent, dalio-metals (Ray), neo-matrix

2. PORTFOLIOS (execution containers)
   - Hold capital
   - Control execution behavior
   - Determined via _resolve_execution_portfolio()

Routing Modes
-------------
Each signal is routed into one of:

- trading:
    - Real execution path
    - Orders forwarded to Alpaca
    - execution_status = "EXECUTED"

- paper:
    - Simulated execution
    - DB updated (positions, trades)
    - No external broker calls
    - execution_status = "SIMULATED"

- tracking:
    - No execution
    - No DB mutation (positions/cash unchanged)
    - Signal is logged only
    - execution_status = "LOG_ONLY"

Special Case: Metals (Physical Holdings)
---------------------------------------
- Portfolio name: "Enterprise Computer" (UI alias: Metals)
- type = "physical"
- execution_mode = "tracking"

Rules:
- NEVER execute trades
- NEVER mutate capital or positions
- ONLY log signals for comparison

Agent Mapping
-------------
- super-agent → Alpaca Paper (trading)
- dalio-metals → Enterprise Computer (tracking)
- neo-matrix → Neo Matrix (trading)
- default → Arena Paper (paper)

Benchmark Model: Anderson vs Ray vs Metals

Roles
-----
- Mr. Anderson:
    Active execution system (agent-driven)
    Executes trades through its assigned portfolio

- Ray (Dalio Strategy):
    Signal-only agent
    Generates BUY / SELL / HOLD decisions
    Does NOT execute trades directly

- Metals (Physical Holdings):
    Tracking-only portfolio
    execution_mode = "tracking"
    type = "physical"
    Never executes trades or mutates state

Model Rules
-----------
- Agents produce signals
- Portfolios determine execution behavior
- Tracking portfolios log signals only (no execution, no mutation)

Comparison Purpose
------------------
Evaluate performance across:
- Active trading (Anderson)
- Strategy signals (Ray)
- Passive holdings (Metals)

Key Guarantee
-------------
Agents NEVER directly execute trades.

All execution decisions are determined by the resolved portfolio route.
- No agent can mutate the Metals portfolio.
- Metals remains a ground-truth benchmark.
"""

from __future__ import annotations
import json
import os
import sqlite3
from datetime import datetime
from rich.console import Console

console = Console()


def _first_trade_notification(player_id: str, symbol: str, action: str, price: float) -> None:
    """Fire macOS notification + War Room post on an agent's very first trade."""
    try:
        conn_check = sqlite3.connect(DB, check_same_thread=False, timeout=10)
        count = conn_check.execute(
            "SELECT COUNT(*) FROM trades WHERE player_id=?", (player_id,)
        ).fetchone()[0]
        conn_check.close()
        if count != 1:  # Only fire on exactly 1 trade (just-inserted first trade)
            return
        # macOS notification
        try:
            import subprocess
            msg = f"{player_id} placed their FIRST trade: {action} {symbol} @ ${price:.2f}"
            subprocess.Popen(
                ["osascript", "-e",
                 f'display notification "{msg}" with title "🚀 First Trade!" sound name "Glass"'],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
        except Exception:
            pass
        # War Room post
        try:
            from engine.war_room import save_hot_take
            save_hot_take(
                player_id, symbol,
                f"🚀 FIRST TRADE MILESTONE: {player_id} has placed their very first trade! "
                f"{action} {symbol} @ ${price:.2f}. Welcome to the arena, recruit.",
            )
        except Exception:
            pass
    except Exception:
        pass


DB = os.environ.get(
    "TRADEMINDS_DB",
    os.path.expanduser("~/autonomous-trader/data/trader.db"),
)
EQUITY_CURVE_FILE = os.path.join(os.path.dirname(DB), "equity_curve.json")

# Post-sell trade grading callback: set by Arena at init to enable AI self-grading
# Signature: _on_sell_callback(player_id, symbol, entry_price, exit_price, pnl, reasoning)
_on_sell_callback = None

_EXECUTION_PORTFOLIO_BY_PLAYER = {
    "super-agent": "Alpaca Paper",  # KEEP THIS
    "dalio-metals": "Enterprise Computer",
    "neo-matrix": "Neo Matrix",
    "ollie-auto": "Alpaca Paper",   # Ollie Super Trader → Alpaca paper account
}


def register_sell_callback(callback):
    """Register a callback to fire after every full SELL (for trade grading)."""
    global _on_sell_callback
    _on_sell_callback = callback


# === SHORT EQUITY FEATURE FLAG ===
# Set to True to enable short-sell execution for agents with short_enabled=1 in ai_players.
# Wiring added 2026-04-21. Flip to True only after reviewing Counselor Troi's
# ghost-trade performance. To enable: set SHORT_ENABLED = True below.
SHORT_ENABLED = False  # Admiral: flip this to True when ready

# === ALPACA PAPER TRADING BRIDGE ===
# Forwards DB trades to Alpaca paper account for real execution.
# Only stocks, only AI models, never human portfolios.
_alpaca = None
_alpaca_init_attempted = False


def _get_alpaca():
    """Lazy-init Alpaca bridge (import once, reuse)."""
    global _alpaca, _alpaca_init_attempted
    if _alpaca_init_attempted:
        return _alpaca
    _alpaca_init_attempted = True
    try:
        from engine.alpaca_bridge import alpaca
        if alpaca.client:
            _alpaca = alpaca
            console.log("[green]Paper trader: Alpaca bridge connected — trades will execute on Alpaca")
        else:
            console.log("[yellow]Paper trader: Alpaca not configured — DB-only mode")
    except Exception as e:
        console.log(f"[yellow]Paper trader: Alpaca bridge unavailable ({e})")
    return _alpaca


def _alpaca_position_qty(bridge, symbol: str) -> float:
    """Return Alpaca's current qty for symbol (signed: + long, - short, 0 flat).

    Short-circuit guard patch 2026-04-17: queries the live Alpaca paper account
    so _forward_to_alpaca can cap SELLs against drift. Never raises; returns
    None if we can't tell (treat unknown as 'do not forward').
    """
    try:
        if not bridge or not getattr(bridge, "client", None):
            return None
        try:
            pos = bridge.client.get_open_position(symbol)
            return float(pos.qty)
        except Exception:
            # Alpaca returns 404 when no position exists — treat as flat.
            return 0.0
    except Exception:
        return None


def _reconcile_phantom_position(player_id: str, symbol: str) -> None:
    """HM-TRADES-MIRROR-GAP forward-guard 2026-05-28: when a routed SELL is
    skipped because the shared Alpaca account holds 0 of the symbol, the
    internal per-player ledger is carrying a phantom position (drift, often a
    pre-fix contaminated BUY that never filled on Alpaca). Zero the internal
    stock position so the fleet stops generating/retrying sells for something
    the broker doesn't hold. Routed-players only (this path is gated on
    route_mode='trading'); stock-only; never raises."""
    try:
        conn = _conn()
        cur = conn.execute(
            "DELETE FROM positions WHERE player_id=? AND symbol=? AND asset_type='stock'",
            (player_id, symbol),
        )
        conn.commit()
        conn.close()
        if cur.rowcount:
            console.log(
                f"[yellow][PHANTOM-RECONCILE] zeroed internal {symbol} for "
                f"{player_id} (Alpaca holds 0 — internal ledger drift cleared)"
            )
    except Exception as e:
        console.log(
            f"[yellow][PHANTOM-RECONCILE] failed {player_id} {symbol}: "
            f"{type(e).__name__}: {e!r}"
        )


def _forward_to_alpaca(action: str, player_id: str, symbol: str, qty: float,
                        asset_type: str = "stock", price: float = 0.0):
    """Forward a trade to Alpaca paper account. Never raises.

    Uses fractional qty (rounded to 2 dp) — Alpaca paper supports fractional shares.
    Falls back to whole shares only if Alpaca rejects the fractional order.
    For ollie-auto during extended-hours windows, issues limit orders with
    extended_hours=True so Alpaca accepts the order outside regular session.

    SHORT-GUARD patch 2026-04-17: multiple players share one Alpaca account,
    so internal per-player ledger can drift from Alpaca aggregate. Before
    forwarding a SELL, check Alpaca's current qty — if flat or short, skip
    the forward entirely so we never accidentally open or worsen a short.

    HM-TRADES-PRICE-WRITEBACK-FIX 2026-05-21: returns the Alpaca result dict
    (containing filled_avg_price + order_id) so the caller can persist the
    actual broker fill into trades.entry_price / trades.exit_price. Returns
    None when no Alpaca attempt was made (not stock / no bridge / qty too
    small / short-guard tripped / options path).
    """
    if asset_type == "stock":
        bridge = _get_alpaca()
        if not bridge:
            return None
        frac_qty = round(qty, 2)
        if frac_qty < 0.01:
            return None
        # SHORT-GUARD: before SELL, verify Alpaca actually holds enough to sell.
        if action == "SELL":
            alpaca_qty = _alpaca_position_qty(bridge, symbol)
            if alpaca_qty is None:
                console.log(f"[yellow]Alpaca SELL {symbol} skipped: could not verify Alpaca position (drift protection)")
                return None
            if alpaca_qty <= 0:
                console.log(f"[yellow]Alpaca SELL {symbol} skipped: Alpaca qty={alpaca_qty} (would create/worsen short — internal ledger drift, player={player_id})")
                _reconcile_phantom_position(player_id, symbol)  # HM-MIRROR-GAP forward-guard: clear the phantom so it isn't retried
                return None
            if alpaca_qty < frac_qty:
                old = frac_qty
                frac_qty = round(alpaca_qty, 2)
                console.log(f"[yellow]Alpaca SELL {symbol} capped: {old} → {frac_qty} (Alpaca holds {alpaca_qty}, player={player_id})")
                if frac_qty < 0.01:
                    return None
        # Extended-hours flag: all agents trade pre/post market via Alpaca
        use_ext = False
        try:
            from engine.risk_manager import RiskManager
            use_ext = RiskManager.is_extended_trading_hours()
        except Exception:
            pass
        try:
            if action == "BUY":
                result = bridge.buy(symbol, frac_qty,
                                    extended_hours=use_ext, limit_price=price)
            elif action == "SELL":
                result = bridge.sell(symbol, frac_qty,
                                     extended_hours=use_ext, limit_price=price)
            else:
                return None
            if result.get("error"):
                # Fractional rejected — retry with whole shares
                whole_qty = int(qty)
                if whole_qty <= 0:
                    console.log(f"[yellow]Alpaca {action} {symbol} failed (frac+whole): {result['error']}")
                    return result
                if action == "BUY":
                    result = bridge.buy(symbol, whole_qty,
                                        extended_hours=use_ext, limit_price=price)
                else:
                    result = bridge.sell(symbol, whole_qty,
                                         extended_hours=use_ext, limit_price=price)
                if result.get("error"):
                    console.log(f"[yellow]Alpaca {action} {symbol} failed: {result['error']}")
                else:
                    console.log(f"[bold cyan]Alpaca {action} {whole_qty} {symbol} (whole fallback) — order {result.get('order_id', 'ok')} ({player_id})")
            else:
                console.log(f"[bold cyan]Alpaca {action} {frac_qty} {symbol} — order {result.get('order_id', 'ok')} ({player_id})")
            # HM-TRADES-PRICE-WRITEBACK-FIX 2026-05-21: surface the Alpaca
            # result (incl. filled_avg_price from bridge poll) to the caller.
            return result
        except Exception as e:
            # HM-U: NTFY first occurrence per error class per day (architecture-class
            # forward-to-Alpaca catch-all in _forward_to_alpaca).
            console.log(f"[yellow]Alpaca forward error: {type(e).__name__}: {e!r}")
            try:
                from engine.alert_channels import send_alert, AlertLevel
                send_alert(
                    message=f"_forward_to_alpaca {action} {symbol} ({player_id}) {type(e).__name__}: {e!r}",
                    level=AlertLevel.WARNING,
                    alert_type=f"hm-u-forward_to_alpaca-{type(e).__name__}",
                    rate_limit_secs=86400,
                )
            except Exception:
                pass
            return None


def estimate_option_price(option_type: str, strike_price: float | None,
                          stock_price: float, entry_premium: float,
                          expiry_date: str = None) -> float:
    """Estimate current option value using intrinsic value + time value floor.

    For calls: max(0, stock_price - strike_price)
    For puts:  max(0, strike_price - stock_price)

    Time value floor: options with >3 days to expiry are worth at least 15% of
    entry premium (approximating residual time value).  This prevents false
    stop-losses on near-ATM options that still have significant extrinsic value.

    Falls back to entry_premium if strike_price is unknown.
    """
    if strike_price is None or strike_price <= 0:
        # No strike data — estimate using stock price delta as proxy for option P&L.
        # A rough ATM option moves ~50% (delta) of the underlying's % change.
        # This prevents all null-strike options from showing $0 P&L.
        if entry_premium > 0 and stock_price > 0:
            # Assume entry was ATM, estimate what strike would have been
            assumed_strike = stock_price  # best guess: ATM at current price
            # But we need the stock price at entry. Use entry_premium as proxy:
            # For ATM options, premium ≈ stock_price * 0.03-0.05 for 30 DTE
            # Better approach: use delta ≈ 0.5 for ATM, so option moves ~$0.50 per $1 stock move
            # We don't know entry stock price, so return entry_premium (unchanged)
            return entry_premium
        return entry_premium

    if option_type == "call":
        intrinsic = max(0.0, stock_price - strike_price)
    elif option_type == "put":
        intrinsic = max(0.0, strike_price - stock_price)
    else:
        return entry_premium

    # Add time value floor for options with remaining life.
    # Options retain significant extrinsic value until close to expiry.
    # Approximate: an ATM option with 30 DTE retains ~60-80% of its premium.
    # We use a square-root decay model (theta decays faster near expiry).
    time_value_floor = 0.0
    if expiry_date:
        try:
            days_left = (datetime.strptime(expiry_date, "%Y-%m-%d").date() - datetime.now().date()).days
            if days_left > 0:
                # sqrt decay: time_value ∝ sqrt(days_left / 30)
                # At 30 DTE: floor = 70% of entry premium
                # At 7 DTE:  floor = 34% of entry premium
                # At 1 DTE:  floor = 13% of entry premium
                # At 0 DTE:  floor = 0 (intrinsic only)
                time_value_floor = entry_premium * 0.70 * min((days_left / 30) ** 0.5, 1.0)
        except (ValueError, TypeError):
            pass

    return max(intrinsic, time_value_floor)


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    return c


def _resolve_execution_portfolio(player_id: str) -> dict:
    """Resolve the execution portfolio route for a player.

    Returns a normalized route dict:
      route_mode: trading | paper | tracking
      execution_mode: auto | manual | tracking
      type: trading | paper | physical
    """
    portfolio_name = _EXECUTION_PORTFOLIO_BY_PLAYER.get(player_id)
    if not portfolio_name:
        return {
            "player_id": player_id,
            "portfolio_id": None,
            "portfolio_name": "Arena Paper",
            "execution_mode": "manual",
            "type": "paper",
            "route_mode": "paper",
        }

    conn = _conn()
    try:
        row = conn.execute(
            "SELECT id, name, execution_mode, type FROM portfolios WHERE name=?",
            (portfolio_name,),
        ).fetchone()
    finally:
        conn.close()

    if not row:
        return {
            "player_id": player_id,
            "portfolio_id": None,
            "portfolio_name": portfolio_name,
            "execution_mode": "manual",
            "type": "paper",
            "route_mode": "paper",
        }

    execution_mode = (row[2] or "manual").lower()
    portfolio_type = (row[3] or "paper").lower()
    if execution_mode == "tracking" or portfolio_type == "physical":
        route_mode = "tracking"
    elif execution_mode == "auto":
        route_mode = "trading"
    else:
        route_mode = "paper"

    return {
        "player_id": player_id,
        "portfolio_id": row[0],
        "portfolio_name": row[1],
        "execution_mode": execution_mode,
        "type": portfolio_type,
        "route_mode": route_mode,
    }


def _log_gate_reject(player_id: str, symbol: str | None, gate_name: str,
                     reason: str | None, signal_id: int | None = None,
                     price: float | None = None, confidence: float | None = None) -> None:
    """HM-GATE-REJECT-TELEMETRY-V1 2026-05-26: fail-safe writer for gate_reject_log.

    Never blocks the calling gate. Any DB / connection / payload error is
    silently swallowed (HM-Z/HM-AA error posture). Consolidates rejection
    telemetry previously scattered across trader.log lines + _last_rejection
    in-memory dict + decision_audit gate_reject events."""
    try:
        conn = _conn()
        try:
            conn.execute(
                "INSERT INTO gate_reject_log "
                "(player_id, symbol, gate_name, reason, signal_id, price, confidence) "
                "VALUES (?,?,?,?,?,?,?)",
                (player_id, symbol, gate_name, reason, signal_id, price, confidence),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass  # fail-safe — gate logic must never be blocked by telemetry


def _log_signal_only(player_id: str, action: str, symbol: str, route: dict, reasoning: str,
                     confidence: float) -> dict:
    msg = (
        f"{player_id}: LOG ONLY {action} {symbol} — "
        f"{route['portfolio_name']} is tracking-only"
    )
    console.log(f"[yellow]{msg}")
    _last_rejection[player_id] = "tracking-only portfolio"
    try:
        from engine.signal_scorecard import log_signal
        log_signal({"ticker": symbol, "direction": action, "indicator": "agent", "strategy": player_id, "confidence": confidence})
    except Exception:
        pass
    return {
        "action": action,
        "symbol": symbol,
        "player_id": player_id,
        "qty": 0,
        "price": 0,
        "confidence": confidence,
        "reasoning": reasoning,
        "portfolio_name": route["portfolio_name"],
        "execution_mode": route["execution_mode"],
        "portfolio_type": route["type"],
        "route_mode": route["route_mode"],
        "execution_status": "LOG_ONLY",
    }


def _current_season() -> int:
    """Read current season from settings table, default 1."""
    try:
        c = _conn()
        row = c.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
        c.close()
        return int(row[0]) if row else 1
    except Exception:
        return 1


def get_portfolio(player_id: str) -> dict:
    # HM-POSITIONS-CONVICTION-DENORM 2026-05-24: include conviction so the
    # exit-evaluation site (engine/risk_manager.py:785) can read it without
    # an extra DB roundtrip. NULL conviction is expected for non-AI-signal
    # players (alpaca-mirror, enterprise-computer) and pre-denorm legacy
    # rows — the consumer's allow-list + NULL-fallback handles those.
    conn = _conn()
    row = conn.execute("SELECT cash FROM ai_players WHERE id=?", (player_id,)).fetchone()
    pos = conn.execute(
        "SELECT symbol, qty, avg_price, asset_type, option_type, strike_price, expiry_date, "
        "high_watermark, conviction "
        "FROM positions WHERE player_id=?", (player_id,)
    ).fetchall()
    conn.close()
    return {
        "cash": row[0] if row else 0,
        "positions": [
            {"symbol": p[0], "qty": p[1], "avg_price": p[2], "asset_type": p[3],
             "option_type": p[4], "strike_price": p[5], "expiry_date": p[6],
             "high_watermark": p[7], "conviction": p[8]}
            for p in pos
        ]
    }


def get_position(player_id: str, symbol: str, asset_type: str = "stock",
                 option_type: str = None) -> dict | None:
    conn = _conn()
    if asset_type == "stock":
        row = conn.execute(
            "SELECT qty, avg_price, strike_price, option_type, opened_at FROM positions "
            "WHERE player_id=? AND symbol=? AND asset_type='stock'",
            (player_id, symbol)
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT qty, avg_price, strike_price, option_type, opened_at FROM positions "
            "WHERE player_id=? AND symbol=? AND option_type=?",
            (player_id, symbol, option_type)
        ).fetchone()
    conn.close()
    if not row:
        return None
    return {"qty": row[0], "avg_price": row[1], "strike_price": row[2], "option_type": row[3], "opened_at": row[4]}


def _is_human_player(player_id: str) -> bool:
    """Check if player must be skipped by auto-trade paths.

    HM-Y (2026-05-05): name preserved for backwards-compat with existing call
    sites (542, 1087, 1229, 1348, 1548, 2009, 2073). Semantics now extended:
    returns True for humans AND passive broker mirrors (alpaca-mirror, etc.).
    Delegates to engine.halt_gate.is_auto_tradeable for single source of truth.

    Belt-and-braces "steve"/"webull" string fallback retained — handles edge
    cases where ai_players row is missing or DB is briefly unavailable.
    """
    if False:  # HM-WEBULL-NEUTRALIZED "steve" in player_id.lower() or "webull" in player_id.lower() (account liquidated 2026-05-13)
        return True
    try:
        # HM-Y: gate via halt_gate helper — composes humans + passive mirrors.
        from engine.halt_gate import is_auto_tradeable
        conn = _conn()
        try:
            return not is_auto_tradeable(player_id, conn)
        finally:
            conn.close()
    except Exception:
        return False


def _detect_ghost_option(player_id: str, symbol: str, price: float,
                          reasoning: str, option_type, expiry_date):
    """Detect when a 'stock' trade is actually an option premium.

    Returns (asset_type, option_type, expiry_date) — possibly reclassified.
    Triggers when price < 10% of live stock price for stocks trading above $50.
    """
    try:
        from engine.market_data import get_stock_price
        live = get_stock_price(symbol)
        live_price = live.get("price", 0)
        if live_price >= 50 and price < live_price * 0.10:
            # Heuristic: infer call/put from reasoning text
            ot = option_type
            if not ot:
                lower = reasoning.lower()
                ot = "put" if "put" in lower else "call"
            # Infer expiry: look for 0DTE cues, else default to today
            exp = expiry_date
            if not exp:
                lower = reasoning.lower()
                if "0dte" in lower or "today" in lower or "same day" in lower or "0 dte" in lower:
                    exp = datetime.now().strftime("%Y-%m-%d")
                else:
                    # Default to end of current week (Friday)
                    from datetime import date, timedelta
                    today = date.today()
                    days_to_friday = (4 - today.weekday()) % 7
                    exp = (today + timedelta(days=days_to_friday)).strftime("%Y-%m-%d")
            console.log(
                f"[bold yellow]GHOST OPTION DETECTED: {player_id} {symbol} @ ${price:.2f} "
                f"(live ${live_price:.2f}) → reclassified as {ot.upper()} option, expiry={exp}"
            )
            return "option", ot, exp
    except Exception:
        pass
    return "stock", option_type, expiry_date


def buy(player_id: str, symbol: str, price: float, asset_type: str = "stock",
        qty: float = None, reasoning: str = "", confidence: float = 0.0,
        option_type: str = None, strike_price: float = None, expiry_date: str = None,
        sources: str = "", timeframe: str = "SWING", sizing_multiplier: float = 1.0,
        signal_id: int | None = None,
        strategy_id: str | None = None) -> dict | None:
    """HM-SPREAD-STRATEGY-ID-WRITESITE 2026-05-23: strategy_id is opt-in
    kwarg so single-leg strategies (long_call, csp legs that don't go
    through the multi-leg alpaca_options path) can stamp trades.strategy_id.
    Multi-leg spreads continue to land in options_trades.strategy_id —
    already correctly populated (25 bull_spread_v1 rows verified)."""
    # HM-MARKET-HOLIDAY-CALENDAR Phase B 2026-05-25 — primary gate against
    # closed-market signal/order fires. Memorial Day 2026-05-25 fired 6
    # Alpaca orders before this gate existed. Block FIRST, before any
    # side effect (events_bus emit, DB writes, Alpaca submit).
    from engine.market_calendar import market_closed_reason as _mcr
    _mkt_block_reason = _mcr()
    if _mkt_block_reason is not None:
        _last_rejection[player_id] = f"[HM-MARKET-CLOSED] {_mkt_block_reason}"
        _log_gate_reject(player_id, symbol, "MARKET_CLOSED", _mkt_block_reason,
                         signal_id=signal_id, price=price, confidence=confidence)
        console.log(
            f"[yellow][HM-MARKET-CLOSED] {player_id} BUY {symbol} "
            f"blocked — {_mkt_block_reason}"
        )
        return None
    # HM-SIGNAL-TRADE-FK 2026-05-20: signal_id is the rowid of the originating
    # row in `signals` returned by save_signal(). Optional — callers without
    # the signal_id in scope pass None, and the trade row stores NULL.
    #
    # HM-EVENTS-BUS-DIRECT-BUY-HOOK 2026-05-22: emit a signals_v2 row at the
    # ENTRANCE of buy() so direct-call agents (ollie-auto, neo-matrix,
    # capitol-trades, navigator) — which fire trades without going through
    # save_signal — still populate the events bus. Without this hook,
    # ~70% of fleet trade intent was invisible to the cockpit + Ghost
    # Scorecard. Fires before any gate so even rejected intent shows up
    # as "pending" (transitions to 'executed' on successful fill via
    # _emit_trade_to_bus, or stays pending if blocked).
    # Fail-safe: any error → continue without bus emit.
    try:
        from engine.events_bus import emit_signal_v2
        _direct_strategy = (
            'long_equity' if asset_type == 'stock'
            else (('long_' + option_type.lower())
                  if (asset_type == 'option' and option_type) else 'unknown')
        )
        _audit_sid = None
        _audit_sid = emit_signal_v2(
            source=player_id, signal_type='direct_buy_intent', symbol=symbol,
            direction='LONG', confidence=confidence or 0.0,
            timeframe=('scalp' if (timeframe or 'swing').lower() == 'short' else (timeframe or 'swing').lower()),
            strategy_tag=_direct_strategy,
            metadata={'direct_call': True, 'caller': player_id, 'asset_type': asset_type},
        )
    except Exception as _ebd_e:
        console.log(
            f"[yellow][EVENTS-BUS-WARN] direct buy hook "
            f"player={player_id} sym={symbol}: "
            f"{type(_ebd_e).__name__}: {_ebd_e!r}"
        )
    # GUARD: Never auto-trade human portfolios
    if _is_human_player(player_id):
        console.log(f"[red]BLOCKED: {player_id} is human — cannot auto-trade")
        return None
    # === HALT GATE === (halt_mode-aware; blocks new positions in exit_only OR full)
    # HM-A: dropped unused is_halted column from SELECT; halt_mode is single source of truth
    _halt = _conn().execute(
        "SELECT halt_reason, halt_mode FROM ai_players WHERE id=?", (player_id,)
    ).fetchone()
    if _halt and (_halt[1] != "active"):
        console.log(f"[red]HALTED: {player_id} ({_halt[1]}) — {_halt[0] or 'no reason given'}")
        _last_rejection[player_id] = f"Halted ({_halt[1]}): {_halt[0] or 'no reason given'}"
        _log_gate_reject(player_id, symbol, "HALT",
                         f"halt_mode={_halt[1]} reason={_halt[0] or 'no reason given'}",
                         signal_id=signal_id, price=price, confidence=confidence)
        return None
    route = _resolve_execution_portfolio(player_id)
    if route["route_mode"] == "tracking":
        return _log_signal_only(player_id, "BUY", symbol, route, reasoning, confidence)

    # === HM-EVENTS-BUS-FOUNDATION stale-signal gate 2026-05-22 ===
    # Gate order at this insertion point (post-conflict resolution):
    #   1. Stale-signal gate     (this block)        — latency budget
    #   2. Regime-router gate    (HM-REGIME-ROUTER)  — strategy/regime fit
    #   3. Grade B fleet gate    (HM-GRADE-B-FLEET-GATE, below) — quality band
    # Stale runs first so we don't waste regime/quality work on a signal
    # that already aged past its budget.
    #
    # Per-timeframe latency budget: 0dte=2s, swing=30s, position=300s. We use
    # the v1 signals.created_at (canonical emit timestamp) for the age check;
    # signals_v2 carries the same budget via stale_after for non-buy readers.
    # If signal_id passed and (now - created_at) > budget → reject with
    # [STALE-SIGNAL] + decision_audit gate_reject. Fail-safe: any error
    # → allow the trade (HM-Z/HM-AA error posture).
    if signal_id is not None and signal_id >= 0:
        try:
            from engine.events_bus import _STALE_BUDGET_S
            _tf_key = (timeframe or "swing").lower()
            _budget_s = _STALE_BUDGET_S.get(_tf_key)
            if _budget_s is not None:
                _scn = _conn()
                _srow = _scn.execute(
                    "SELECT created_at FROM signals WHERE rowid=?",
                    (int(signal_id),),
                ).fetchone()
                _scn.close()
                if _srow and _srow[0]:
                    from datetime import datetime as _dt
                    # SQLite default CURRENT_TIMESTAMP is UTC 'YYYY-MM-DD HH:MM:SS'
                    _emit_dt = _dt.strptime(
                        str(_srow[0])[:19], "%Y-%m-%d %H:%M:%S"
                    )
                    _age_s = (_dt.utcnow() - _emit_dt).total_seconds()
                    if _age_s > _budget_s:
                        console.log(
                            f"[yellow][STALE-SIGNAL] symbol={symbol} "
                            f"player={player_id} timeframe={_tf_key} "
                            f"age={_age_s:.1f}s budget={_budget_s}s — BLOCKED"
                        )
                        _last_rejection[player_id] = (
                            f"STALE-SIGNAL: age={_age_s:.1f}s > "
                            f"budget={_budget_s}s ({_tf_key})"
                        )
                        try:
                            _ac = _conn()
                            _ac.execute(
                                "INSERT INTO decision_audit "
                                "(event_type, player_id, symbol, signal_id, "
                                " gate_verdict, reasoning_snippet) "
                                "VALUES (?,?,?,?,?,?)",
                                ("gate_reject", player_id, symbol,
                                 signal_id, "stale_signal",
                                 f"[stale_signal] age={_age_s:.1f}s "
                                 f"budget={_budget_s}s timeframe={_tf_key}"),
                            )
                            _ac.commit()
                            _ac.close()
                        except Exception:
                            pass  # audit never blocks
                        return None
        except Exception as _stale_e:
            console.log(
                f"[red][STALE-SIGNAL] gate fail-open: "
                f"{type(_stale_e).__name__}: {_stale_e!r}"
            )
    # === /HM-EVENTS-BUS-FOUNDATION stale-signal gate ===

    # HM-DEEPSEEK-CONCENTRATION-CAP-V2: cap single-symbol concentration at 25%
    # for deepseek-7b-grok4. MU grew to ~90% of portfolio causing phantom -$960
    # loss when price feed failed. Prevents recurrence.
    # HM-CONCENTRATION-CAP-NLV 2026-05-26: denominator is true NLV (cash +
    # sum(qty * avg_price)), not cash-only. Cash-only was tighter than
    # nominal 25% — true NLV reflects the deployable-capital base properly.
    if player_id == "deepseek-7b-grok4":
        _port = get_portfolio(player_id)
        _pos = get_position(player_id, symbol, asset_type)
        _current_exposure = (_pos["qty"] * price) if _pos else 0
        _pos_rows = _conn().execute(
            "SELECT qty, avg_price FROM positions WHERE player_id=?", (player_id,)
        ).fetchall()
        _equity = sum(r[0] * r[1] for r in _pos_rows if r[0] and r[1])
        _port_value = max(_port.get("cash", 0) + _equity, 1)
        if _current_exposure / _port_value >= 0.25:
            console.log(
                f"[yellow][CONCENTRATION-CAP] deepseek-7b-grok4 BUY {symbol} blocked: "
                f"exposure ${_current_exposure:.0f} already >= 25% of NLV ${_port_value:.0f}"
            )
            _last_rejection[player_id] = f"[CONCENTRATION-CAP] {symbol} >= 25% of NLV"
            _log_gate_reject(player_id, symbol, "CONCENTRATION_CAP",
                             f"exposure ${_current_exposure:.0f} >= 25% of NLV ${_port_value:.0f}",
                             signal_id=signal_id, price=price, confidence=confidence)
            return None

    # === HM-REGIME-ROUTER 2026-05-22 ===
    # Strategy-regime fit gate. Fires upstream of HM-GRADE-B-FLEET-GATE so a
    # regime mismatch rejects at the coarse strategy-fit layer before the
    # fine-grained quality-band gate runs. Fail-safe: unknown regime →
    # allow trade + log [REGIME-ROUTER-UNKNOWN] (HM-Z/HM-AA error posture).
    # Spec: project_hm_ic_squadron_approved.md Pillar 1.
    try:
        from engine.regime_router import (
            check_regime_fit, get_current_regime, log_regime_reject,
        )
        _rr_regime = get_current_regime()
        # Strategy label for the matrix lookup. Equity → long_equity;
        # options carry option_type (call/put) → long_call / long_put.
        # Strategy-specific callers (BullSpreadV1 etc.) flow through
        # different paths and don't hit paper_trader.buy() directly.
        if asset_type == "stock":
            _rr_strategy = "long_equity"
        elif asset_type == "option" and option_type:
            _rr_strategy = "long_" + option_type.lower()
        else:
            _rr_strategy = asset_type or "long_equity"
        _rr_allowed, _rr_reason = check_regime_fit(_rr_strategy, _rr_regime)
        if not _rr_allowed:
            console.log(
                f"[yellow][REGIME-ROUTER] player={player_id} symbol={symbol} "
                f"strategy={_rr_strategy} regime={_rr_regime} — "
                f"BLOCKED: {_rr_reason}"
            )
            log_regime_reject(
                player_id=player_id, symbol=symbol, strategy=_rr_strategy,
                regime=_rr_regime, reason=_rr_reason, confidence=confidence,
            )
            _last_rejection[player_id] = f"REGIME-ROUTER: {_rr_reason}"
            return None
    except Exception as _rr_e:
        # Fail-safe: any router error → allow trade, log loudly.
        console.log(
            f"[red][REGIME-ROUTER] fail-open: "
            f"{type(_rr_e).__name__}: {_rr_e!r}"
        )
    # === /HM-REGIME-ROUTER ===

    # === HM-GRADE-B-FLEET-GATE 2026-05-20 ===
    # Generalize the Grade B regime + SPY-intraday gates (PR #43 + #47, originally
    # ollie-auto-only) to the entire fleet. Grade B's analog in non-ollie-auto
    # agents is the "marginal conviction" band: confidence in [0.60, 0.75).
    # When confidence is in that band AND today's regime is bearish OR SPY is
    # down >0.1% intraday, block the BUY. Grade A (conf ≥ 0.75) and very low
    # conviction (conf < 0.60, which won't typically trade anyway) bypass.
    # Stocks only (options have separate gates upstream).
    # Backtest validation: scripts/grade_b_regime_backtest.py showed the same
    # gate pattern saves $295.20 across 48 May Grade B trades for ollie-auto.
    # Fleet-wide application extends the protection to deepseek/navigator/etc.
    # Fail-safe: any lookup failure → ALLOW the trade.
    if asset_type == "stock" and confidence is not None and 0.60 <= float(confidence) < 0.75:
        _fleet_block_reason = None
        # Layer 1: bearish regime
        try:
            _gbc = _conn()
            try:
                _gbrow = _gbc.execute(
                    "SELECT regime FROM regime_history "
                    "WHERE date = date('now','localtime') "
                    "ORDER BY id DESC LIMIT 1"
                ).fetchone()
                _gbregime = _gbrow[0] if _gbrow else None
            finally:
                _gbc.close()
            if _gbregime in ("BEAR_CROSS", "CAUTIOUS_BEAR"):
                _fleet_block_reason = f"regime={_gbregime}"
        except Exception:
            _gbregime = None  # fail-safe: open
        # Layer 2: SPY intraday (only run if regime didn't already block)
        if _fleet_block_reason is None:
            try:
                from engine.market_data import get_stock_price as _gbgsp
                _gbspy = _gbgsp("SPY") or {}
                if "error" not in _gbspy:
                    _gbspy_chg = _gbspy.get("change_pct")
                    if isinstance(_gbspy_chg, (int, float)) and _gbspy_chg < -0.1:
                        _fleet_block_reason = f"SPY={_gbspy_chg:+.3f}%"
            except Exception:
                pass  # fail-safe: open
        if _fleet_block_reason:
            console.log(
                f"[yellow][GRADE-B-FLEET-GATE] player={player_id} symbol={symbol} "
                f"conf={confidence:.2f} reason={_fleet_block_reason} — BLOCKED"
            )
            _last_rejection[player_id] = (
                f"GRADE-B-FLEET-GATE: {_fleet_block_reason}"
            )
            _log_gate_reject(player_id, symbol, "GRADE_B", _fleet_block_reason,
                             signal_id=signal_id, price=price, confidence=confidence)
            return None
    # === /HM-GRADE-B-FLEET-GATE ===

    # === GHOST PROMOTION BLOCKER ===
    # Catch models whose reasoning says "hold/no trade" but action leaked as BUY.
    _GHOST_PHRASES = [
        "no new position will be initiated",
        "outside my operational zone",
        "violates my directives",
        "outside this specified sector",
        "no position",
    ]
    _reasoning_lower = reasoning.lower()
    for _phrase in _GHOST_PHRASES:
        if _phrase in _reasoning_lower:
            console.log(
                f"[red]GHOST PROMOTION BLOCKED: {player_id} {symbol} — "
                f"reasoning contains '{_phrase}'. Trade rejected."
            )
            _last_rejection[player_id] = f"Ghost promotion: reasoning contains '{_phrase}'"
            return None
    # Also block if reasoning contains a standalone HOLD directive
    import re as _re
    if _re.search(r'\bHOLD\b', reasoning):
        console.log(
            f"[red]GHOST PROMOTION BLOCKED: {player_id} {symbol} — "
            f"reasoning contains HOLD directive. Trade rejected."
        )
        _last_rejection[player_id] = "Ghost promotion: HOLD directive in reasoning"
        return None

    # === GHOST OPTION DETECTION ===
    # If a model is trading what looks like an option premium as a "stock", reclassify it.
    if asset_type == "stock" and option_type is None:
        asset_type, option_type, expiry_date = _detect_ghost_option(
            player_id, symbol, price, reasoning, option_type, expiry_date
        )

    # === CREW SPECIALIZATION MANDATE GATE ===
    # Block trades that fall outside an agent's assigned strategy mandate.
    try:
        from engine.crew_specialization import should_agent_trade as _mandate_check, is_bridge_voter as _is_voter
        if _is_voter(player_id):
            console.log(f"[red]MANDATE BLOCKED: {player_id} is a Bridge Voter — no individual trades allowed")
            _last_rejection[player_id] = "Bridge Voter: no individual trading"
            return None
        # Build lightweight market_data snapshot from latest briefing
        _mandate_market = {}
        try:
            from engine.ready_room import get_latest_briefing as _get_briefing
            _brief = _get_briefing() or {}
            _mandate_market = {
                "session_type": _brief.get("session_type", ""),
                "vix": _brief.get("vix", 0),
                "pc_ratio": _brief.get("pc_ratio", 1.0),
            }
        except Exception:
            pass
        _mandate_ok, _mandate_reason = _mandate_check(player_id, _mandate_market)
        if not _mandate_ok:
            console.log(f"[yellow]MANDATE BLOCKED: {player_id} → {_mandate_reason}")
            _last_rejection[player_id] = f"Mandate: {_mandate_reason}"
            return None
    except Exception as _mandate_err:
        # Don't block on import errors — mandate check is advisory
        pass

    # === HM-DEEPSEEK-CONC-CAP 2026-05-23 ===
    # Per-strategy concentration cap, deepseek-7b-grok4 only. Reads
    # agent_strategy_pauses + tripwires on >20 trades/30d with negative
    # cum PnL. Helper at top of file (_check_deepseek_conc_cap). Inert
    # at deploy time because deepseek's signal path emits strategy_id=
    # NULL — fires the moment that gap closes.
    if player_id == "deepseek-7b-grok4" and strategy_id:
        try:
            _conc_allowed, _conc_reason = _check_deepseek_conc_cap(strategy_id)
            if not _conc_allowed:
                console.log(
                    f"[red][HM-DEEPSEEK-CONC-CAP] {player_id} {symbol} "
                    f"strategy={strategy_id}: {_conc_reason}"
                )
                _last_rejection[player_id] = (
                    f"Strategy conc-cap [{strategy_id}]: {_conc_reason}"
                )
                return None
        except Exception as _conc_e:
            console.log(
                f"[yellow][HM-DEEPSEEK-CONC-CAP] fail-open "
                f"{type(_conc_e).__name__}: {_conc_e!r}"
            )
    # === /HM-DEEPSEEK-CONC-CAP ===

    # === UNIVERSAL GUARDRAIL GATE (Strategy Lab S4 fixes) ===
    # These checks run BEFORE any trade execution, cannot be overridden.
    try:
        from engine.risk_manager import RiskManager
        _rm = RiskManager()

        # 1. Daily trade limit (per-model + bear market aware)
        _daily_limit = _rm.get_effective_daily_limit(player_id)
        _today = datetime.now().strftime("%Y-%m-%d")
        _tc = _conn()
        _trade_count = _tc.execute(
            "SELECT COUNT(*) FROM trades WHERE player_id=? AND date(executed_at)=?",
            (player_id, _today)
        ).fetchone()[0]
        _tc.close()
        if _trade_count >= _daily_limit:
            console.log(f"[red]MAX_TRADES_REACHED: {player_id} at {_trade_count}/{_daily_limit} trades today — REJECTED {symbol}")
            _last_rejection[player_id] = "Daily trade limit reached"
            return None

        # 2. Universal minimum conviction
        _min_conv = max(
            _rm.UNIVERSAL_MIN_CONVICTION,
            _rm.get_model_guardrail(player_id, "min_conviction") or 0,
        )
        # 0DTE and intraday agents operate on shorter timeframes — lower floor
        if player_id in ("dayblade-0dte", "dayblade-sulu"):
            _min_conv = min(_min_conv, 0.45)
        if _rm.is_bear_market() and player_id not in ("dayblade-sulu", "navigator", "dalio-metals"):
            # Exempt: Sulu (day trader), Chekov (convergence scanner), Dalio (All Weather — trades in all regimes)
            _min_conv = max(_min_conv, _rm.BEAR_MIN_CONVICTION)
        if confidence < _min_conv:
            console.log(f"[red]LOW_CONVICTION: {player_id} {symbol} conf={confidence:.0%} < {_min_conv:.0%} — REJECTED")
            _last_rejection[player_id] = f"Below confidence threshold ({confidence:.0%} < {_min_conv:.0%})"
            return None

        # 3. V2: Conviction-scaled stop-loss + target — wider stops for high conviction.
        # HM-AN2-TARGET-INJECTION 2026-05-22: symmetric target auto-injection so
        # the swing-gate's has_target check (line ~968) passes alongside has_stop.
        # Before this fix, AN2-consumed Signal Center signals (Spock/Chekov/etc.
        # routed through neo-matrix) lost the swing-gate because upstream
        # reasoning text rarely included "target" text — auto-injection covered
        # stop only, producing a 100% block rate on neo-matrix AN2 swing trades.
        # Target default = 2× stop (2:1 RR floor). Text-only injection — actual
        # exit behavior still driven by downstream tier-exit logic.
        _model_sl = _rm.get_model_guardrail(player_id, "stop_loss_pct")
        _sl_pct = _model_sl if _model_sl else _rm.get_stop_loss_pct(confidence)
        # === HM-DEEPSEEK-STOP-CAP 2026-05-23 ===
        # Hard dollar ceiling on per-trade max loss for agents that declare
        # `max_loss_dollar` in MODEL_GUARDRAILS. Tightens _sl_pct so the
        # implied (qty × price × sl_pct) loss can't exceed the cap.
        # Targets deepseek-7b-grok4 first (worst trade −$671 → cap $150).
        # Generic: any agent that adds max_loss_dollar inherits the same
        # protection. Fail-safe: skip if any required arg is None/zero.
        _max_loss_dollar = _rm.get_model_guardrail(player_id, "max_loss_dollar")
        if _max_loss_dollar and _sl_pct and qty and price:
            try:
                _implied_loss = float(qty) * float(price) * float(_sl_pct)
                _cap = float(_max_loss_dollar)
                if _implied_loss > _cap:
                    _orig_sl = _sl_pct
                    _sl_pct = _cap / (float(qty) * float(price))
                    console.log(
                        f"[yellow][HM-DEEPSEEK-STOP-CAP] {player_id} {symbol} "
                        f"qty={qty} price=${price:.2f}: implied max loss "
                        f"${_implied_loss:.2f} > cap ${_cap:.2f} — "
                        f"tightening stop {_orig_sl:.2%} → {_sl_pct:.2%}"
                    )
            except Exception as _slc_e:
                console.log(
                    f"[red][HM-DEEPSEEK-STOP-CAP] fail-open: "
                    f"{type(_slc_e).__name__}: {_slc_e!r}"
                )
        # === /HM-DEEPSEEK-STOP-CAP ===
        if "stop" not in reasoning.lower() and "sl" not in reasoning.lower():
            reasoning = f"{reasoning} [AUTO-STOP: -{_sl_pct:.0%} from entry]"
        if "target" not in reasoning.lower():
            reasoning = f"{reasoning} [AUTO-TARGET: +{_sl_pct * 2:.0%} from entry]"

        # 4. V3: Per-model position limit (fewer picks, bigger bets)
        _portfolio = get_portfolio(player_id)
        _unique_syms = set(p["symbol"] for p in _portfolio["positions"])
        _model_max = _rm.MAX_POSITIONS_PER_MODEL.get(
            player_id, _rm.MAX_POSITIONS_PER_MODEL["default"])
        if _rm.is_bear_market() and player_id not in ("dayblade-sulu", "navigator", "dalio-metals"):
            _model_max = min(_model_max, _rm.BEAR_MAX_POSITIONS)
        if len(_unique_syms) >= _model_max and symbol not in _unique_syms:
            console.log(f"[red]MAX_POSITIONS_REACHED: {player_id} already has "
                        f"{len(_unique_syms)}/{_model_max} positions — REJECTED {symbol}")
            _last_rejection[player_id] = f"Maximum positions reached ({len(_unique_syms)}/{_model_max})"
            return None

        # 5. V3: Quality gate (stock must pass 3/5 fundamental checks)
        # Exempt: capitol-trades follows Congress members, not AI analysis
        _QUALITY_GATE_EXEMPT = {"capitol-trades", "navigator", "webull"}
        if asset_type == "stock" and player_id not in _QUALITY_GATE_EXEMPT:
            try:
                from engine.quality_gate import passes_quality_gate
                _indicators = {}
                try:
                    from engine.market_data import get_technical_indicators
                    _indicators = get_technical_indicators(symbol) or {}
                except Exception:
                    pass
                _passes, _qscore, _qdetails = passes_quality_gate(symbol, _indicators)
                if not _passes:
                    console.log(f"[red]QUALITY_GATE_FAILED: {player_id} {symbol} scored "
                                f"{_qscore}/5 — {', '.join(_qdetails[:3])}")
                    _last_rejection[player_id] = f"Failed quality gate ({_qscore}/5)"
                    return None
            except ImportError:
                pass

        # 6. Warp 9: Scanner validation — prefer scanner picks over AI guesses
        # Exempt: capitol-trades uses Congress trade data, not AI universe scanner
        _SCANNER_EXEMPT = {"capitol-trades", "webull"}
        if asset_type == "stock" and player_id not in _SCANNER_EXEMPT:
            try:
                from engine.strategies import get_todays_signals
                from engine.universe_scanner import get_latest_universe_scan
                _conv = get_todays_signals()
                _univ = get_latest_universe_scan()
                _conv_tickers = [s["ticker"] for s in (_conv or [])]
                _univ_tickers = [s["ticker"] for s in (_univ or {}).get("results", [])[:50]]
                # Also allow existing watchlist stocks
                from engine.universe import get_active_universe
                _watchlist = set(get_active_universe())

                if symbol in _conv_tickers:
                    pass  # Best: convergence signal — full green light
                elif symbol in _univ_tickers:
                    if confidence < 0.70:
                        console.log(f"[yellow]SCANNER_FILTER: {player_id} {symbol} in universe but "
                                    f"conv={confidence:.0%} < 70% — REJECTED")
                        _last_rejection[player_id] = "In scanner universe but confidence too low (need 70%)"
                        return None
                elif symbol in _watchlist:
                    pass  # Watchlist stocks always allowed
                else:
                    if confidence < 0.90:
                        console.log(f"[yellow]SCANNER_FILTER: {player_id} {symbol} NOT in scanner "
                                    f"results — need 0.90+ conv (got {confidence:.0%})")
                        _last_rejection[player_id] = "Not in scanner results (need 90%+ confidence)"
                        return None
            except ImportError:
                pass
            except Exception:
                pass  # Scanner not populated yet — allow trade

    except ImportError:
        pass  # First run before risk_manager exists
    except Exception as _e:
        console.log(f"[yellow]Guardrail check warning: {_e}")

    # GUARD: Options only during regular market hours (9:30 AM - 4 PM ET)
    # AND not in first/last 30 min (avoid wide spreads at open/close)
    if asset_type == "option" or option_type:
        try:
            from engine.risk_manager import RiskManager
            session = RiskManager.is_market_hours()
            if session in ("pre_market", "post_market") or not session:
                console.log(f"[yellow]BLOCKED: {player_id} {symbol} — Options only during market hours")
                _last_rejection[player_id] = "Options only allowed during market hours (9:30 AM - 4 PM ET)"
                return None
            # Block first 30 min (9:30-10:00) and last 30 min (3:30-4:00 ET)
            import pytz as _pytz
            _et = _pytz.timezone("US/Eastern")
            _now_et = datetime.now(_et)
            _mins = _now_et.hour * 60 + _now_et.minute
            if _mins < 600 or _mins > 930:  # before 10:00 AM or after 3:30 PM ET
                console.log(f"[yellow]BLOCKED: {player_id} {symbol} — No options in first/last 30 min (spreads too wide)")
                _last_rejection[player_id] = "Options blocked: first/last 30 min of session (wide spreads)"
                return None
        except Exception:
            pass

    # VIX CIRCUIT BREAKER: Pause all new entries when VIX > 30
    _vix = _get_vix_cached()
    if _vix and _vix > _VIX_CIRCUIT_BREAKER and player_id not in ("dayblade-sulu", "navigator"):
        console.log(f"[bold red]VIX CIRCUIT BREAKER: VIX={_vix:.1f} > {_VIX_CIRCUIT_BREAKER} — {player_id} blocked (reduce sizes in high vol)")
        _last_rejection[player_id] = f"VIX circuit breaker ({_vix:.1f} > {_VIX_CIRCUIT_BREAKER})"
        return None

    # DRAWDOWN PAUSE: Block new entries if player is down 20%+ from peak portfolio value
    # HM-DRAWDOWN-GATE-SYNC 2026-05-26: season-scope the peak (match
    # risk_manager.check_drawdown at risk_manager.py:1018-1021) so stale
    # S1-S5 peaks don't inflate S6 drawdown. Threshold 0.15→0.20 matches
    # risk_manager.max_drawdown_pct default so the two gates agree.
    try:
        _pf_check = get_portfolio(player_id)
        _pos_val = sum(abs(p["qty"]) * p["avg_price"] for p in _pf_check["positions"])
        _cur_val = _pf_check["cash"] + _pos_val
        _peak_conn = _conn()
        _season_row = _peak_conn.execute(
            "SELECT value FROM settings WHERE key='current_season'"
        ).fetchone()
        _season = int(_season_row[0]) if _season_row else 1
        _peak_row = _peak_conn.execute(
            "SELECT MAX(total_value) FROM portfolio_history WHERE player_id=? AND season=?",
            (player_id, _season),
        ).fetchone()
        _peak_conn.close()
        _peak = _peak_row[0] if _peak_row and _peak_row[0] else None
        if _peak and _peak > 0 and (_peak - _cur_val) / _peak >= 0.20:
            console.log(f"[yellow]DRAWDOWN PAUSE: {player_id} at {((_peak-_cur_val)/_peak*100):.1f}% drawdown — no new entries until recovery")
            _last_rejection[player_id] = f"Drawdown pause: {((_peak-_cur_val)/_peak*100):.1f}% below peak (threshold 20%)"
            return None
    except Exception:
        pass

    # READY ROOM ADVISORY (Counselor Troi): Gate on market condition before execution
    _adv_mult = 1.0  # default: full size
    _ADVISOR_EXEMPT = {"capitol-trades", "webull", "dalio-metals"}
    # T'Pol and McCoy operate on their own mandate/VIX gates — Troi STAND_DOWN
    # (CHOP regime) should not block them. CAUTION sizing still applies.
    _TROI_STAND_DOWN_EXEMPT = {"dayblade-0dte", "ollama-plutus"}
    if player_id not in _ADVISOR_EXEMPT:
        try:
            from engine.ready_room_advisor import should_i_trade as _advisory
            _adv = _advisory(symbol=symbol, proposed_action="BUY", player_id=player_id)
            _adv_signal = _adv.get("signal", "GO")
            _adv_mult   = _adv.get("position_size_multiplier", 1.0)
            if _adv_signal == "STAND_DOWN":
                if player_id in _TROI_STAND_DOWN_EXEMPT:
                    console.log(
                        f"[yellow]COUNSELOR TROI: STAND_DOWN override — {player_id} {symbol} "
                        f"exempt from CHOP gate, proceeding."
                    )
                else:
                    console.log(
                        f"[bold red]COUNSELOR TROI: STAND_DOWN — {player_id} {symbol} "
                        f"blocked. {_adv.get('reason', 'RED condition')}"
                    )
                    _last_rejection[player_id] = f"Ready Room STAND_DOWN: {_adv.get('reason', 'RED condition')}"
                    return None
            elif _adv_signal == "CAUTION":
                console.log(
                    f"[yellow]COUNSELOR TROI: CAUTION — {player_id} {symbol} "
                    f"(×{_adv_mult:.2f}). {_adv.get('reason', 'YELLOW condition')}"
                )
        except Exception:
            _adv_mult = 1.0

    # GLOBAL OPTION RISK: Max 6 open option positions across all models combined
    if asset_type == "option" or option_type:
        try:
            _opt_conn = _conn()
            _total_opts = _opt_conn.execute(
                "SELECT COUNT(*) FROM positions WHERE asset_type='option'"
            ).fetchone()[0]
            _opt_conn.close()
            if _total_opts >= 6:
                console.log(f"[yellow]OPTION LIMIT: {_total_opts}/6 global option positions — {player_id} {symbol} blocked")
                _last_rejection[player_id] = f"Global option limit reached ({_total_opts}/6 positions)"
                return None
        except Exception:
            pass

        # CORRELATION CHECK: Block if 3+ models already hold this ticker (options)
        try:
            _corr_conn = _conn()
            _holders = _corr_conn.execute(
                "SELECT COUNT(DISTINCT player_id) FROM positions WHERE symbol=? AND asset_type='option'",
                (symbol,)
            ).fetchone()[0]
            _corr_conn.close()
            if _holders >= 3:
                console.log(f"[yellow]CORRELATION: {_holders} models already hold {symbol} options — blocking {player_id}")
                _last_rejection[player_id] = f"Correlation block: {_holders} models already hold {symbol} options"
                return None
        except Exception:
            pass

    portfolio = get_portfolio(player_id)
    cash = portfolio["cash"]

    # GUARD: Options exposure cap — max 20% of account value in options
    if asset_type == "option" or option_type:
        total_options_value = sum(
            p["qty"] * p["avg_price"]
            for p in portfolio["positions"]
            if p.get("asset_type") == "option"
        )
        positions_value = sum(
            p["qty"] * p["avg_price"] for p in portfolio["positions"]
        )
        account_value = cash + positions_value
        proposed_cost = (qty if qty else round((cash * 0.10) / price, 4)) * price
        if account_value > 0 and (total_options_value + proposed_cost) / account_value > 0.20:
            console.log(f"[yellow]OPTIONS CAP: 20% max options exposure reached. "
                        f"{player_id} {symbol} blocked — options ${total_options_value:.0f} + "
                        f"${proposed_cost:.0f} would exceed 20% of ${account_value:.0f}")
            _last_rejection[player_id] = f"Options exposure cap reached (20% of portfolio)"
            return None

    # SWING TRADE RULES
    is_swing = timeframe.upper() in ("SWING", "SWING_3D", "SWING_5D", "SWING_15D")
    if is_swing:
        # Require stop + target embedded in reasoning
        import re as _re2
        has_stop = bool(_re2.search(r'\[STOP[:\s]', reasoning, _re2.IGNORECASE)
                        or "stop" in reasoning.lower())
        has_target = bool(_re2.search(r'\[TARGET[:\s]', reasoning, _re2.IGNORECASE)
                          or "target" in reasoning.lower())
        if not (has_stop and has_target):
            console.log(f"[red]SWING TRADE BLOCKED: {player_id} {symbol} — "
                        "swing trades require explicit stop AND target in reasoning.")
            _last_rejection[player_id] = "Swing trade missing stop or target"
            return None

    if qty is None:
        if is_swing:
            alloc_pct = 0.25
        elif asset_type == "option":
            # Options: Kelly-based sizing, max 2% per single option (5% for spreads)
            # HM-KELLY-TIER-MULTIPLIER 2026-05-23: tier-scale cap for
            # high-Sharpe agents — Sharpe>10 → 2x, Sharpe>5 → 1.5x cap.
            kelly_pct = get_kelly_fraction(player_id)
            _km = get_kelly_cap_multiplier(player_id)
            alloc_pct = min(kelly_pct, 0.02 * _km)
            if _km > 1.0:
                console.log(
                    f"[cyan][KELLY-TIER] {player_id} options cap "
                    f"2.0%→{0.02 * _km:.1%} (Sharpe-tier {_km:.1f}×)"
                )
        else:
            # Stocks: half-Kelly, capped at 10% (tier-scaled).
            kelly_pct = get_kelly_fraction(player_id)
            _km = get_kelly_cap_multiplier(player_id)
            alloc_pct = min(kelly_pct, 0.10 * _km)
            if _km > 1.0:
                console.log(
                    f"[cyan][KELLY-TIER] {player_id} stock cap "
                    f"10.0%→{0.10 * _km:.1%} (Sharpe-tier {_km:.1f}×)"
                )
            alloc_pct, _alloc_reasons = _target_weight_adjustment(
                player_id, symbol, portfolio, alloc_pct, price, confidence
            )
            if _alloc_reasons:
                console.log(f"[cyan]TARGET WEIGHT: {player_id} {symbol} sizing adjusted for {', '.join(_alloc_reasons)}")
        _allocation_policy = get_capital_allocation_policy(player_id)
        if _allocation_policy["multiplier"] != 1.0:
            _base_alloc = alloc_pct
            alloc_pct = max(0.02, alloc_pct * _allocation_policy["multiplier"])
            console.log(
                f"[cyan]ALLOCATION {player_id}: {_allocation_policy['tier']} "
                f"{_base_alloc:.2%}->{alloc_pct:.2%} "
                f"(cycle_ret={_allocation_policy['return_pct']:.2f}% "
                f"win_rate={_allocation_policy['win_rate']:.1f}% "
                f"trades={_allocation_policy['trade_count']})"
            )
        # HM-MASTER-PLAN W5-C Blend E enforcement (2026-05-23). For stock
        # BUYs only — the long_equity bucket is the dominant case and the
        # one most likely to push the portfolio off target. Other buckets
        # (csp / bull_put_spread / ic / etc.) are mapped in a follow-up
        # wave once the strategy_id → bucket vocabulary is finalized.
        if asset_type == "stock":
            _cap_alloc, _cap_reason = _apply_regime_long_equity_cap(
                player_id=player_id, portfolio=portfolio, cash=cash,
                alloc_pct=alloc_pct,
            )
            if _cap_reason:
                console.log(f"[cyan][REGIME-ALLOC] {player_id} {symbol}: {_cap_reason}")
                alloc_pct = _cap_alloc
                if alloc_pct <= 0:
                    _last_rejection[player_id] = (
                        f"Regime long_equity cap reached: {_cap_reason}"
                    )
                    return None
        qty = round((cash * alloc_pct) / price, 4)
    # Apply sizing multiplier: prefer caller-provided (scan path already ran Troi),
    # otherwise use the internal Ready Room advisory multiplier.  Never double-apply.
    if sizing_multiplier < 1.0:
        # Caller (e.g. crew_scanner) already resolved Troi CAUTION — use their value
        # and skip _adv_mult to prevent double reduction (0.5 × 0.5 = 0.25).
        if qty:
            qty = round(qty * sizing_multiplier, 4)
    elif _adv_mult < 1.0 and qty:
        qty = round(qty * _adv_mult, 4)
    if qty <= 0:
        return None

    # Minimum order floor: after CAUTION sizing, ensure the position is at least
    # min($50, 1 share) in value — whichever costs less — so Alpaca receives a
    # meaningful order.  Only bumps up; never reduces.  Skips if cash is too low.
    if asset_type == "stock" and price > 0:
        _min_qty = round(min(50.0 / price, 1.0), 4)
        if qty < _min_qty and cash >= _min_qty * price:
            console.log(
                f"[cyan]MIN ORDER: {player_id} {symbol} qty {qty:.4f} → {_min_qty:.4f} "
                f"(${_min_qty * price:.2f} floor)"
            )
            qty = _min_qty

    # Swing trade 25% position size cap (absolute)
    if is_swing:
        max_swing_cost = cash * 0.25
        if qty * price > max_swing_cost:
            qty = round(max_swing_cost / price, 4)
            console.log(f"[cyan]SWING CAP: {player_id} {symbol} capped at 25% (${max_swing_cost:.0f})")

    # Option position size cap: 2% of account per single option
    if asset_type == "option" and not is_swing:
        _acct_value = cash + sum(p["qty"] * p["avg_price"] for p in portfolio["positions"])
        max_opt_cost = _acct_value * 0.02
        if qty * price > max_opt_cost:
            qty = round(max_opt_cost / price, 4)
            console.log(f"[cyan]OPTION CAP: {player_id} {symbol} capped at 2% (${max_opt_cost:.0f})")

    # 8/21 MA Cross Regime: scale position size by trend modifier
    # DayBlade (dayblade-sulu) is exempt — it uses its own intraday sizing
    if player_id != "dayblade-sulu" and asset_type == "stock":
        try:
            from engine.regime_ma import get_ma_cross_size_modifier
            _ma_modifier = get_ma_cross_size_modifier()
            if _ma_modifier < 1.0:
                _orig_qty = qty
                qty = round(qty * _ma_modifier, 4)
                console.log(
                    f"[cyan]8/21 Regime modifier {_ma_modifier:.0%}: "
                    f"{player_id} {symbol} qty {_orig_qty}→{qty}"
                )
        except Exception:
            pass

    cost = round(qty * price, 2)
    if cost > cash:
        console.log(f"[red]{player_id}: Not enough cash for {symbol}")
        _last_rejection[player_id] = f"Insufficient buying power (need ${cost:.0f}, have ${cash:.0f})"
        return None

    conn = _conn()
    conn.execute("UPDATE ai_players SET cash=? WHERE id=?", (round(cash - cost, 2), player_id))

    # HM-POSITIONS-AVG-PRICE-WRITEBACK 2026-05-21: capture the pre-write
    # (ex_qty, ex_avg_price) so the post-Alpaca-fill correction below can
    # recompute avg_price from broker truth instead of the trader-internal
    # `price`. Initialize defaults to handle the options branch / non-stock.
    _pos_ex_qty_before = 0.0
    _pos_ex_avg_before = 0.0
    _pos_was_new = True
    # HM-POSITIONS-CONVICTION-DENORM 2026-05-24: stamp conviction from the
    # BUY signal's confidence onto positions.conviction so risk_manager.py
    # can scale stops without re-fetching. 0.0 / negative confidence becomes
    # NULL so the Phase 4 NULL-fallback uses flat stops rather than the
    # tightest scaled tier (8%) — "no real signal" should not narrow stops.
    _conv = float(confidence) if confidence and confidence > 0 else None
    if asset_type == "stock":
        ex = conn.execute(
            "SELECT qty, avg_price FROM positions WHERE player_id=? AND symbol=? AND asset_type='stock'",
            (player_id, symbol)
        ).fetchone()
        if ex:
            _pos_ex_qty_before = float(ex[0])
            _pos_ex_avg_before = float(ex[1])
            _pos_was_new = False
            nq = ex[0] + qty
            na = round(((ex[0] * ex[1]) + cost) / nq, 4)
            conn.execute(
                "UPDATE positions SET qty=?, avg_price=?, conviction=?, conviction_source='live_buy' "
                "WHERE player_id=? AND symbol=? AND asset_type='stock'",
                (nq, na, _conv, player_id, symbol)
            )
        else:
            conn.execute(
                "INSERT INTO positions(player_id, symbol, qty, avg_price, asset_type, "
                "conviction, conviction_source) VALUES(?,?,?,?,?,?,?)",
                (player_id, symbol, qty, price, "stock", _conv, "live_buy")
            )
    else:
        conn.execute(
            "INSERT INTO positions(player_id, symbol, qty, avg_price, asset_type, option_type, "
            "strike_price, expiry_date, conviction, conviction_source) "
            "VALUES(?,?,?,?,?,?,?,?,?,?)",
            (player_id, symbol, qty, price, "option", option_type, strike_price, expiry_date,
             _conv, "live_buy")
        )

    # HM-PROMPT-VERSIONING (POC Day 2b) 2026-05-22: look up signals.prompt_version
    # by signal_id so the trade row inherits the agent's prompt revision tag.
    # Fail-safe: any lookup error → write NULL prompt_version on the trade.
    _pv = None
    if signal_id is not None and signal_id >= 0:
        try:
            _pvrow = conn.execute(
                "SELECT prompt_version FROM signals WHERE rowid=?",
                (int(signal_id),),
            ).fetchone()
            if _pvrow:
                _pv = _pvrow[0]
        except Exception:
            pass
    _trade_cur = conn.execute(
        # HM-SIGNAL-TRADE-FK 2026-05-20: trades.signal_id captures originating
        # signals.id (rowid) for traceability. NULL if signal_id not in scope.
        # HM-SPREAD-STRATEGY-ID-WRITESITE 2026-05-23: trades.strategy_id is
        # opt-in (caller passes strategy_id kwarg). NULL for default fleet
        # path; populated for single-leg strategies that want to be queryable
        # alongside the multi-leg options_trades.strategy_id surface.
        # HM-MU-PRICE-WRITEBACK 2026-05-23: stamp entry_price = price at INSERT
        # for ALL routes (Alpaca + simulated). Previously simulated BUYs left
        # entry_price=NULL and only Alpaca-routed trades got their entry_price
        # filled via _persist_alpaca_fill writeback (line ~1893). That left
        # 88/88 simulated BUYs with NULL entry_price in the last 14d audit.
        # For Alpaca trades, _persist_alpaca_fill below STILL overwrites
        # entry_price with the actual filled_avg_price — that path is
        # unchanged. This INSERT just seeds a sane non-null default so PnL
        # math + cockpit queries don't choke on NULL entries.
        "INSERT INTO trades(player_id, symbol, action, qty, price, asset_type, option_type, "
        "strike_price, expiry_date, reasoning, confidence, season, sources, timeframe, signal_id, "
        "prompt_version, strategy_id, entry_price) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (player_id, symbol, "BUY", qty, price, asset_type, option_type,
         strike_price, expiry_date, reasoning, confidence, _current_season(), sources, timeframe, signal_id,
         _pv, strategy_id, price)
    )
    _trade_id = _trade_cur.lastrowid  # HM-DECISION-AUDIT-V1 2026-05-20
    conn.commit()
    conn.close()
    # HM-SIGNAL-AUDIT-CLOSE: self-close the direct_buy_intent audit row
    try:
        if _audit_sid and _trade_id:
            from engine.events_bus import mark_signal_executed
            mark_signal_executed(signal_id=_audit_sid, trade_id=_trade_id)
    except Exception:
        pass  # audit close is best-effort, never block fill
    console.log(f"[green]{player_id}: BUY {qty} {symbol} @ ${price:.2f}")
    # HM-DECISION-AUDIT-V1 2026-05-20: trade_fire hook
    _write_decision_audit(
        event_type="trade_fire",
        player_id=player_id,
        symbol=symbol,
        signal_id=signal_id,
        trade_id=_trade_id,
        confidence=confidence,
        reasoning_snippet=reasoning,
    )
    # HM-EVENTS-BUS-FOUNDATION 2026-05-22: events + signals_v2 fire hook
    _emit_trade_to_bus(
        player_id=player_id, symbol=symbol, action="BUY",
        qty=qty, price=price, asset_type=asset_type,
        signal_id=signal_id, trade_id=_trade_id, reasoning=reasoning,
    )
    _first_trade_notification(player_id, symbol, "BUY", price)

    # Forward to Alpaca paper trading (non-blocking)
    # HM-TRADES-PRICE-WRITEBACK-FIX 2026-05-21: persist actual broker fill into
    # the just-inserted trade row (was leaving trader-internal price in place).
    # HM-POSITIONS-AVG-PRICE-WRITEBACK 2026-05-21: also recompute positions.avg_price
    # from the broker fill so subsequent SELL closes use broker truth as the
    # realized_pnl entry basis (stock-only; options + SHORT out of scope).
    if route["route_mode"] == "trading":
        _alpaca_result = _forward_to_alpaca("BUY", player_id, symbol, qty, asset_type, price=price)
        _persist_alpaca_fill(_trade_id, "BUY", qty, _alpaca_result, player_id, symbol)
        if asset_type == "stock":
            _persist_alpaca_fill_position(
                player_id, symbol, qty,
                _pos_ex_qty_before, _pos_ex_avg_before, _pos_was_new,
                _alpaca_result,
            )

    return {
        "action": "BUY",
        "symbol": symbol,
        "qty": qty,
        "price": price,
        "player_id": player_id,
        "trade_id": _trade_id,  # HM-EVENTS-BUS-CONSUMER-TRADE-ID 2026-05-26
        "portfolio_name": route["portfolio_name"],
        "execution_mode": route["execution_mode"],
        "portfolio_type": route["type"],
        "route_mode": route["route_mode"],
        "execution_status": "EXECUTED" if route["route_mode"] == "trading" else "SIMULATED",
    }


def _get_buy_timeframe(player_id: str, symbol: str) -> str:
    """Look up the timeframe tag on the original BUY trade for this position."""
    try:
        conn = _conn()
        row = conn.execute(
            "SELECT timeframe FROM trades WHERE player_id=? AND symbol=? AND action='BUY' "
            "ORDER BY executed_at DESC LIMIT 1",
            (player_id, symbol),
        ).fetchone()
        conn.close()
        return (row["timeframe"] or "").upper() if row else ""
    except Exception:
        return ""


def _check_min_hold(player_id: str, symbol: str, pos: dict, reasoning: str) -> bool:
    """Return True if the position can be sold (held >= min hold or stop-loss).

    - All trades: 24h minimum hold
    - SWING trades: 3-day (72h) minimum hold
    Stop-loss/target sells always bypass the hold timer.
    """
    opened_at = pos.get("opened_at")
    if not opened_at:
        return True
    try:
        opened = datetime.strptime(opened_at, "%Y-%m-%d %H:%M:%S")
        hours_held = (datetime.now() - opened).total_seconds() / 3600

        is_stop_or_target = any(kw in reasoning.lower() for kw in
                                ("stop", "sl", "expired", "target", "take-profit", "tp"))

        # Swing trades: 3-day minimum unless stop/target
        tf = _get_buy_timeframe(player_id, symbol)
        if tf in ("SWING", "SWING_3D", "SWING_5D", "SWING_15D"):
            min_hours = 72
            if hours_held < min_hours and not is_stop_or_target:
                days_held = hours_held / 24
                console.log(
                    f"[yellow]{player_id}: HOLD {symbol} — SWING trade only held "
                    f"{days_held:.1f}d (min 3d). Reason: {reasoning[:60]}"
                )
                return False

        if hours_held < 24 and not is_stop_or_target:
            console.log(f"[yellow]{player_id}: HOLD {symbol} — only held {hours_held:.1f}h (min 24h). Reason: {reasoning[:60]}")
            return False
    except (ValueError, TypeError):
        pass
    return True


def sell(player_id: str, symbol: str, price: float, asset_type: str = "stock",
         reasoning: str = "", confidence: float = 0.0,
         option_type: str = None) -> dict | None:
    # HM-MARKET-HOLIDAY-CALENDAR Phase B 2026-05-25 — primary gate.
    from engine.market_calendar import market_closed_reason as _mcr
    _mkt_block_reason = _mcr()
    if _mkt_block_reason is not None:
        _last_rejection[player_id] = f"[HM-MARKET-CLOSED] {_mkt_block_reason}"
        _log_gate_reject(player_id, symbol, "MARKET_CLOSED", _mkt_block_reason,
                         price=price, confidence=confidence)
        console.log(
            f"[yellow][HM-MARKET-CLOSED] {player_id} SELL {symbol} "
            f"blocked — {_mkt_block_reason}"
        )
        return None
    # GUARD: Never auto-trade human portfolios
    if _is_human_player(player_id):
        console.log(f"[red]BLOCKED: {player_id} is human — cannot auto-trade")
        return None
    # === HALT GATE === (halt_mode-aware; exit_only PERMITS sells, only 'full' blocks)
    # HM-A: dropped unused is_halted column from SELECT; halt_mode is single source of truth
    _halt = _conn().execute(
        "SELECT halt_reason, halt_mode FROM ai_players WHERE id=?", (player_id,)
    ).fetchone()
    if _halt and _halt[1] == "full":
        console.log(f"[red]HALTED (full): {player_id} — {_halt[0] or 'no reason given'}")
        _last_rejection[player_id] = f"Halted (full): {_halt[0] or 'no reason given'}"
        _log_gate_reject(player_id, symbol, "HALT",
                         f"halt_mode=full reason={_halt[0] or 'no reason given'}",
                         price=price, confidence=confidence)
        return None
    route = _resolve_execution_portfolio(player_id)
    if route["route_mode"] == "tracking":
        return _log_signal_only(player_id, "SELL", symbol, route, reasoning, confidence)
    pos = get_position(player_id, symbol, asset_type, option_type)
    if not pos:
        console.log(f"[red]{player_id}: No position in {symbol}")
        return None

    # HM-SELL-PRICE-SANITY-GLOBAL: reject sell if price < 20% of avg_price
    # Prevents garbage post-market prices from creating phantom losses fleet-wide.
    # Same threshold as HM-CHEKOV-PRICE-SANITY-GATE (chekov_autotrade.py:553).
    _avg = pos.get("avg_price", 0)
    if asset_type == "stock" and _avg > 0 and price < (_avg * 0.20):
        console.log(
            f"[yellow][PRICE-SANITY-REJECT] {player_id} SELL {symbol}: "
            f"price=${price:.2f} < 20% of avg=${_avg:.2f} — blocked"
        )
        _last_rejection[player_id] = f"[PRICE-SANITY-REJECT] price={price:.2f} avg={_avg:.2f}"
        _log_gate_reject(player_id, symbol, "PRICE_SANITY",
                         f"SELL price={price:.2f} < 20% of avg={_avg:.2f}",
                         price=price, confidence=confidence)
        return None

    # GUARD: Minimum 24h hold period (unless stop-loss)
    if not _check_min_hold(player_id, symbol, pos, reasoning):
        return None

    # HM-BP-FOLLOW-UP-2 P3 2026-05-26: symmetric data-shape race fix for sell().
    # 9/27 corrupt option exit_price rows came through this full-sell path.
    if asset_type == "stock" and pos.get("asset_type") == "option":
        console.log(
            f"[yellow][HM-BP-FU-2] {player_id} SELL {symbol}: caller said "
            f"'stock' but position is 'option' — reclassifying to prevent spot-as-premium leak"
        )
        asset_type = "option"
        option_type = option_type or pos.get("option_type")

    # For options, estimate current premium using intrinsic value + time value
    # (caller passes stock price — we convert to option value via strike)
    if asset_type == "option" or (asset_type != "stock" and pos.get("asset_type") == "option"):
        ot = option_type or pos.get("option_type")
        price = estimate_option_price(ot, pos.get("strike_price"), price, pos["avg_price"],
                                      pos.get("expiry_date"))

    # GUARD: Refuse to sell options at $0.00 — price was not captured correctly
    if asset_type != "stock" and price < 0.01:
        console.log(f"[bold red]⚠ BLOCKED SELL {symbol} for {player_id}: estimated exit price ${price:.4f} < $0.01 — skipping to protect position")
        return None

    # Short position detection: negative qty means short, covering it is a BUY-to-cover
    is_short = pos["qty"] < 0
    qty = abs(pos["qty"])

    portfolio = get_portfolio(player_id)

    if is_short:
        # Covering a short: return original margin (qty × entry) + P&L
        margin = round(qty * pos["avg_price"], 2)
        pnl = round(qty * (pos["avg_price"] - price), 2)  # profit when price fell
        new_cash = round(portfolio["cash"] + margin + pnl, 2)
        trade_action = "COVER"
    else:
        proceeds = round(qty * price, 2)
        pnl = round(proceeds - (qty * pos["avg_price"]), 2)
        new_cash = round(portfolio["cash"] + proceeds, 2)
        trade_action = "SELL"

    conn = _conn()
    conn.execute("UPDATE ai_players SET cash=? WHERE id=?", (new_cash, player_id))

    if asset_type == "stock":
        conn.execute(
            "DELETE FROM positions WHERE player_id=? AND symbol=? AND asset_type='stock'",
            (player_id, symbol)
        )
    else:
        conn.execute(
            "DELETE FROM positions WHERE player_id=? AND symbol=? AND option_type=?",
            (player_id, symbol, option_type)
        )

    # HM-MU-PRICE-WRITEBACK 2026-05-23: sanity guard on exit price. The
    # historical MU phantom (+$1907 PnL with exit=$533 on a stock with $80-110
    # range) happened because the caller's `price` arg was a stale/wrong mark
    # at SELL time. Log a [TRADE-PRICE-SANITY-WARN] when exit moves more than
    # 3× the entry basis (stock only; options have legitimate 5-10× swings).
    # This is observability-only — does NOT block the SELL. Future analytics
    # can filter the marker rows.
    if asset_type == "stock" and pos.get("avg_price"):
        try:
            _entry_basis = float(pos["avg_price"])
            _exit_mark = float(price)
            if (_entry_basis > 0 and _exit_mark > 0
                    and (_exit_mark / _entry_basis > 3.0
                         or _exit_mark / _entry_basis < 0.33)):
                # HM-STOCK-PRICE-PROVENANCE-AUDIT 2026-05-23: pull the live
                # quote at SANITY-WARN time and surface its `source` field
                # so the operator can see which data source produced the
                # outlier price. Sources: alpaca | yahoo_direct | finnhub |
                # alpha_vantage | db_cache | db_position. If source is
                # db_cache or db_position, the price is from stale trade
                # history — the original $533 MU phantom probably had a
                # poisoned db_cache row that fed downstream callers.
                _quote_source = "unknown"
                _quote_price = None
                try:
                    from engine.market_data import get_stock_price as _gsp
                    _q = _gsp(symbol) or {}
                    _quote_source = _q.get("source", "no_source_key") or "no_source_key"
                    _quote_price = _q.get("price")
                except Exception as _qe:
                    _quote_source = f"lookup_failed_{type(_qe).__name__}"
                console.log(
                    f"[yellow][TRADE-PRICE-SANITY-WARN] {player_id} {symbol} "
                    f"SELL: entry_basis=${_entry_basis:.2f} → exit=${_exit_mark:.2f} "
                    f"ratio={_exit_mark/_entry_basis:.2f}x — caller price arg "
                    f"may be stale. Live quote: ${_quote_price} "
                    f"source={_quote_source}. PnL may be phantom."
                )
        except Exception:
            pass
    # HM-TRADES-PRICE-WRITEBACK-FIX 2026-05-21: capture lastrowid so the Alpaca
    # fill writeback below can target this specific trade row.
    _sell_cur = conn.execute(
        "INSERT INTO trades(player_id, symbol, action, qty, price, asset_type, option_type, "
        "reasoning, confidence, entry_price, exit_price, realized_pnl, season) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (player_id, symbol, trade_action, qty, price, asset_type, option_type, reasoning, confidence,
         pos["avg_price"], price, pnl, _current_season())
    )
    _sell_trade_id = _sell_cur.lastrowid
    conn.commit()
    conn.close()
    console.log(f"[green]{player_id}: {trade_action} {qty} {symbol} @ ${price:.2f} PnL: ${pnl:.2f}")
    # HM-EVENTS-BUS-FOUNDATION 2026-05-22: SELL trade-fire event
    _emit_trade_to_bus(
        player_id=player_id, symbol=symbol, action=trade_action,
        qty=qty, price=price, asset_type=asset_type,
        signal_id=None, trade_id=_sell_trade_id, reasoning=reasoning,
    )
    # HM-AUTO-POST-MORTEM (POC Day 3a) 2026-05-22: fire-and-forget
    # classifier on every full SELL. Local qwen3:8b, $0, 15s timeout,
    # writes decision_audit event_type='post_mortem' with 5-tag taxonomy.
    _fire_post_mortem_async(
        player_id=player_id, symbol=symbol, side=trade_action,
        entry=pos.get("avg_price") if isinstance(pos, dict) else None,
        exit_price=price, pnl=pnl, asset_type=asset_type,
        reasoning=reasoning, trade_id=_sell_trade_id,
    )

    # === HM-FINMEM 2026-05-23 ===
    # On every SELL close, write a SHORT_TERM memory entry to agent_memory
    # for the 4 pilot agents (McCoy / Worf / Grok / Ollie). The writer is
    # internally fail-safe (non-pilots = no-op; any DB error = silent log),
    # but wrap defensively here too so a memory write can never break the
    # SELL lifecycle. Cache for the agent is auto-invalidated on write so
    # the next finmem read picks up the fresh entry.
    try:
        from engine.finmem_writers import on_sell_close
        on_sell_close(
            player_id=player_id, symbol=symbol,
            entry_price=(pos.get("avg_price") if isinstance(pos, dict) else None),
            exit_price=price, realized_pnl=pnl, reasoning=reasoning,
        )
    except Exception as _fm_e:
        console.log(
            f"[yellow][HM-FINMEM-WRITER] sell hook fail-open "
            f"{type(_fm_e).__name__}: {_fm_e!r}"
        )
    # === /HM-FINMEM ===

    # === HM-POST-EXIT-TRACKER 2026-05-20 ===
    # On every successful SELL (stock asset only), seed a post_exit_watch row.
    # A daily scanner (engine.post_exit_tracker.run_daily_scan) will check
    # whether the symbol continued >5% above the exit price post-close and
    # flag the row, emitting [POST-EXIT-FLAG] to logs/trader.log.
    # Crash-safe: any failure here cannot break the SELL completion. Stock-only
    # (options have different lifecycle semantics; not in v1 scope).
    if asset_type == "stock":
        try:
            from engine.post_exit_tracker import register_exit
            register_exit(
                player_id=player_id,
                symbol=symbol,
                exit_price=price,
                exit_pnl=pnl,
            )
        except Exception as _pex_err:
            console.log(
                f"[yellow][HM-POST-EXIT-TRACKER] register failed for "
                f"{player_id} {symbol}: {type(_pex_err).__name__}: {_pex_err!r}"
            )
    # === /HM-POST-EXIT-TRACKER ===

    # Forward to Alpaca paper trading (non-blocking)
    # HM-TRADES-PRICE-WRITEBACK-FIX 2026-05-21: persist actual broker fill +
    # recompute realized_pnl from (fill - entry_price) * qty.
    if route["route_mode"] == "trading":
        _alpaca_result = _forward_to_alpaca("SELL", player_id, symbol, qty, asset_type, price=price)
        _persist_alpaca_fill(_sell_trade_id, "SELL", qty, _alpaca_result, player_id, symbol)

    # Borg lore: post loss notifications to War Room
    if pnl < 0:
        try:
            from engine.war_room import save_hot_take
            pnl_pct = ((price / pos["avg_price"]) - 1) * 100 if pos["avg_price"] > 0 else 0
            crew_name = player_id
            try:
                from engine.war_room import CREW_NAMES
                crew_name = CREW_NAMES.get(player_id, player_id)
            except Exception:
                pass
            if pnl_pct <= -5:
                msg = (f"🤖 BORG ALERT: {symbol} assimilation complete. "
                       f"{crew_name} lost ${abs(pnl):.2f} ({pnl_pct:.1f}%). "
                       f"The collective grows stronger. We will adapt.")
            else:
                msg = (f"🤖 BORG ALERT: {symbol} has been assimilated. "
                       f"-${abs(pnl):.2f} added to the collective. "
                       f"Resistance was futile.")
            save_hot_take(player_id, symbol, msg)
        except Exception:
            pass

    # Move signal to "watching" for re-entry tracking
    try:
        from engine.signal_tracker import mark_watching
        mark_watching(player_id, symbol, price)
    except Exception:
        pass

    # Fire post-sell trade grading (background)
    if _on_sell_callback:
        try:
            import threading
            threading.Thread(
                target=_on_sell_callback,
                args=(player_id, symbol, pos["avg_price"], price, pnl, reasoning),
                daemon=True,
            ).start()
        except Exception:
            pass

    return {
        "action": "SELL",
        "symbol": symbol,
        "pnl": pnl,
        "player_id": player_id,
        "portfolio_name": route["portfolio_name"],
        "execution_mode": route["execution_mode"],
        "portfolio_type": route["type"],
        "route_mode": route["route_mode"],
        "execution_status": "EXECUTED" if route["route_mode"] == "trading" else "SIMULATED",
    }


def sell_partial(player_id: str, symbol: str, price: float, qty: float,
                 asset_type: str = "stock", reasoning: str = "", confidence: float = 0.0,
                 option_type: str = None) -> dict | None:
    """Sell a partial quantity of a position (for tiered take-profit)."""
    # GUARD: Never auto-trade human portfolios
    if _is_human_player(player_id):
        console.log(f"[red]BLOCKED: {player_id} is human — cannot auto-trade")
        return None
    # HM-I-Option-ε-prime (2026-05-05): tracking-mode early-return mirrors sell()
    # at line 1124. Tracking-route players (Schwab portfolio, Enterprise Computer
    # physical metals) log-only via _log_signal_only instead of executing partial
    # sells against the internal positions table. Was missing from sell_partial
    # despite being present in buy() (line 579) and sell() (line 1124). Same
    # class of inconsistency as Option ε's partial-SELL forward gate (commit d06c33c).
    route = _resolve_execution_portfolio(player_id)
    if route["route_mode"] == "tracking":
        return _log_signal_only(player_id, "SELL", symbol, route, reasoning, confidence)
    pos = get_position(player_id, symbol, asset_type, option_type)
    if pos and not _check_min_hold(player_id, symbol, pos, reasoning):
        return None
    if not pos:
        console.log(f"[red]{player_id}: No position in {symbol}")
        return None

    # HM-SELL-PRICE-SANITY-PARTIAL: symmetric with HM-SELL-PRICE-SANITY-GLOBAL
    _avg = pos.get("avg_price", 0)
    if asset_type == "stock" and _avg > 0 and price < (_avg * 0.20):
        console.log(
            f"[yellow][PRICE-SANITY-REJECT] {player_id} SELL_PARTIAL {symbol}: "
            f"price=${price:.2f} < 20% of avg=${_avg:.2f} — blocked"
        )
        _last_rejection[player_id] = f"[PRICE-SANITY-REJECT] price={price:.2f} avg={_avg:.2f}"
        _log_gate_reject(player_id, symbol, "PRICE_SANITY",
                         f"SELL_PARTIAL price={price:.2f} < 20% of avg={_avg:.2f}",
                         price=price, confidence=confidence)
        return None

    # HM-BP-FOLLOW-UP-2 P2 2026-05-26: data-shape race fix. If caller passed
    # asset_type='stock' but the position is actually an option, the existing
    # conversion gate below skips and the stock spot leaks straight into
    # exit_price (root cause of the 27 corrupt rows on 2026-03-12, where
    # ai_brain dispatched tier-take-profit/autopilot-trim actions with stale
    # asset_type metadata). Reclassify upward to force the conversion.
    if asset_type == "stock" and pos.get("asset_type") == "option":
        console.log(
            f"[yellow][HM-BP-FU-2] {player_id} SELL_PARTIAL {symbol}: caller said "
            f"'stock' but position is 'option' — reclassifying to prevent spot-as-premium leak"
        )
        asset_type = "option"
        option_type = option_type or pos.get("option_type")

    # For options, estimate current premium using intrinsic value + time value
    if asset_type == "option" or (asset_type != "stock" and pos.get("asset_type") == "option"):
        ot = option_type or pos.get("option_type")
        price = estimate_option_price(ot, pos.get("strike_price"), price, pos["avg_price"],
                                      pos.get("expiry_date"))

    # GUARD: Refuse to sell options at $0.00 — price was not captured correctly
    if asset_type != "stock" and price < 0.01:
        console.log(f"[bold red]⚠ BLOCKED SELL {symbol} for {player_id}: estimated exit price ${price:.4f} < $0.01 — skipping to protect position")
        return None

    qty = min(qty, pos["qty"])
    if qty <= 0:
        return None

    proceeds = round(qty * price, 2)
    pnl = round(proceeds - (qty * pos["avg_price"]), 2)
    remaining = round(pos["qty"] - qty, 4)

    portfolio = get_portfolio(player_id)
    new_cash = round(portfolio["cash"] + proceeds, 2)

    conn = _conn()
    conn.execute("UPDATE ai_players SET cash=? WHERE id=?", (new_cash, player_id))

    if remaining <= 0:
        # Close entire position
        if asset_type == "stock":
            conn.execute(
                "DELETE FROM positions WHERE player_id=? AND symbol=? AND asset_type='stock'",
                (player_id, symbol)
            )
        else:
            conn.execute(
                "DELETE FROM positions WHERE player_id=? AND symbol=? AND option_type=?",
                (player_id, symbol, option_type)
            )
    else:
        # Reduce position qty
        if asset_type == "stock":
            conn.execute(
                "UPDATE positions SET qty=? WHERE player_id=? AND symbol=? AND asset_type='stock'",
                (remaining, player_id, symbol)
            )
        else:
            conn.execute(
                "UPDATE positions SET qty=? WHERE player_id=? AND symbol=? AND option_type=?",
                (remaining, player_id, symbol, option_type)
            )

    # HM-TRADES-PRICE-WRITEBACK-FIX 2026-05-21: capture lastrowid so the Alpaca
    # fill writeback below can target this specific partial-SELL trade row.
    _partial_cur = conn.execute(
        "INSERT INTO trades(player_id, symbol, action, qty, price, asset_type, option_type, "
        "reasoning, confidence, entry_price, exit_price, realized_pnl, season) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (player_id, symbol, "SELL", qty, price, asset_type, option_type, reasoning, confidence,
         pos["avg_price"], price, pnl, _current_season())
    )
    _partial_trade_id = _partial_cur.lastrowid
    conn.commit()
    conn.close()
    console.log(f"[green]{player_id}: SELL {qty} {symbol} @ ${price:.2f} (partial) PnL: ${pnl:.2f}")
    # HM-EVENTS-BUS-FOUNDATION 2026-05-22: partial-SELL trade-fire event
    _emit_trade_to_bus(
        player_id=player_id, symbol=symbol, action="SELL",
        qty=qty, price=price, asset_type=asset_type,
        signal_id=None, trade_id=_partial_trade_id, reasoning=reasoning,
    )
    # HM-AUTO-POST-MORTEM 2026-05-22: partial closes also get classified.
    # Realized PnL captured on the partial leg; intent is to learn from
    # *why* the partial was triggered (target, trailing stop, panic).
    _fire_post_mortem_async(
        player_id=player_id, symbol=symbol, side="SELL_PARTIAL",
        entry=pos.get("avg_price") if isinstance(pos, dict) else None,
        exit_price=price, pnl=pnl, asset_type=asset_type,
        reasoning=reasoning, trade_id=_partial_trade_id,
    )

    # Forward to Alpaca paper trading (non-blocking)
    # HM-I-Option-ε (2026-05-05): gate identically to BUY (line ~1015) and full-SELL
    # (line ~1167). sell_partial() does not resolve route at function top like sell()/buy()
    # do — resolve here so the gate has data to check.
    # HM-TRADES-PRICE-WRITEBACK-FIX 2026-05-21: persist actual broker fill +
    # recompute realized_pnl from (fill - entry_price) * qty.
    route = _resolve_execution_portfolio(player_id)
    if route["route_mode"] == "trading":
        _alpaca_result = _forward_to_alpaca("SELL", player_id, symbol, qty, asset_type, price=price)
        _persist_alpaca_fill(_partial_trade_id, "SELL", qty, _alpaca_result, player_id, symbol)

    # Borg lore on loss (partial sell, only if full close)
    if pnl < 0 and remaining <= 0:
        try:
            from engine.war_room import save_hot_take
            pnl_pct = ((price / pos["avg_price"]) - 1) * 100 if pos["avg_price"] > 0 else 0
            if pnl_pct <= -5:
                save_hot_take(player_id, symbol,
                    f"🤖 BORG ALERT: {symbol} assimilation complete. "
                    f"-${abs(pnl):.2f} ({pnl_pct:.1f}%). The collective grows stronger.")
            else:
                save_hot_take(player_id, symbol,
                    f"🤖 BORG ALERT: {symbol} has been assimilated. "
                    f"-${abs(pnl):.2f} added to the collective. Resistance was futile.")
        except Exception:
            pass

    # If fully closed, move signal to "watching" for re-entry tracking
    if remaining <= 0:
        try:
            from engine.signal_tracker import mark_watching
            mark_watching(player_id, symbol, price)
        except Exception:
            pass

        # Fire post-sell trade grading on full close (background)
        if _on_sell_callback:
            try:
                import threading
                threading.Thread(
                    target=_on_sell_callback,
                    args=(player_id, symbol, pos["avg_price"], price, pnl, reasoning),
                    daemon=True,
                ).start()
            except Exception:
                pass

    return {"action": "SELL", "symbol": symbol, "qty": qty, "pnl": pnl, "player_id": player_id}


def execute_signal(player_id: str, signal: dict, price: float, signal_id: int | None = None) -> dict | None:
    # HM-SIGNAL-TRADE-FK 2026-05-20: signal_id is the originating signals.id
    # rowid (returned from save_signal). Threaded through to buy() so the
    # resulting trade row captures the FK. Optional — callers that haven't
    # done save_signal yet pass None.
    # GUARD: Never auto-trade human portfolios
    if _is_human_player(player_id):
        console.log(f"[red]BLOCKED: {player_id} is human — cannot auto-trade")
        return None
    action = signal.get("action", "HOLD")
    symbol = signal.get("symbol")
    reasoning = signal.get("reasoning", "")
    confidence = signal.get("confidence", 0.0)
    sources = signal.get("sources", "")
    timeframe = signal.get("timeframe", "SWING")

    if action == "BUY":
        _asset_type = signal.get("asset_type", "stock")
        return buy(player_id, symbol, price, asset_type=_asset_type, reasoning=reasoning,
                   confidence=confidence, sources=sources, timeframe=timeframe,
                   signal_id=signal_id)
    elif action == "SELL":
        return sell(player_id, symbol, price, reasoning=reasoning, confidence=confidence)
    elif action == "SHORT":
        return short_sell(player_id, symbol, price, reasoning=reasoning, confidence=confidence,
                          sources=sources, timeframe=timeframe)
    elif action in ("BUY_CALL", "BUY_PUT"):
        option_type = "call" if action == "BUY_CALL" else "put"
        # Try to get proper expiry and strike from options chain
        expiry_date = None
        strike_price = None
        buy_price = None
        target_dte = signal.get("dte", 0)
        try:
            from engine.options_selector import select_option
            from config import OPTIONS_DEFAULT_DTE, OPTIONS_MIN_DTE
            dte_req = target_dte if target_dte > 0 else OPTIONS_DEFAULT_DTE
            opt = select_option(symbol, option_type,
                                target_dte=dte_req, min_dte=OPTIONS_MIN_DTE)
            if opt:
                expiry_date = opt["expiry_date"]
                strike_price = opt["strike_price"]
                if opt.get("premium") and opt["premium"] > 0:
                    buy_price = opt["premium"]
        except Exception as e:
            console.log(f"[yellow]Options selector fallback for {symbol}: {e}")
        if expiry_date is None or buy_price is None:
            console.log(f"[red]{player_id}: select_option returned no data for {symbol} {action} — skipping (no stock-price fallback)")
            return None
        result = buy(player_id, symbol, buy_price, asset_type="option", option_type=option_type,
                     reasoning=reasoning, confidence=confidence,
                     strike_price=strike_price, expiry_date=expiry_date, sources=sources, timeframe=timeframe,
                     signal_id=signal_id)
        # Forward to Alpaca for options-enabled players; record order ID
        try:
            from engine.alpaca_options import execute_options_signal
            _ap_res = execute_options_signal(player_id, action, symbol, price, target_dte=target_dte)
            if _ap_res and not _ap_res.get("skipped") and not _ap_res.get("error"):
                _order_id = _ap_res.get("order_id") or _ap_res.get("id", "")
                _exec_type = "alpaca_paper" if _order_id else "simulated"
                _update_trade_alpaca_fields(player_id, symbol, _order_id, _exec_type)
        except Exception as _ae:
            # HM-U: NTFY first occurrence per error class per day (options BUY forward).
            console.log(f"[yellow]Alpaca options forward error ({player_id} {symbol}): {type(_ae).__name__}: {_ae!r}")
            try:
                from engine.alert_channels import send_alert, AlertLevel
                send_alert(
                    message=f"options BUY forward ({player_id} {symbol}) {type(_ae).__name__}: {_ae!r}",
                    level=AlertLevel.WARNING,
                    alert_type=f"hm-u-options_forward_buy-{type(_ae).__name__}",
                    rate_limit_secs=86400,
                )
            except Exception:
                pass
        return result

    elif action in ("BULL_CALL_SPREAD", "BEAR_PUT_SPREAD", "IRON_CONDOR"):
        # Multi-leg strategies: DB-only paper tracking + Alpaca real execution
        option_type = "call" if "CALL" in action else "put"
        target_dte = signal.get("dte", 7)  # spreads default to weekly
        result = buy(player_id, symbol, price * 0.02, asset_type="option", option_type=option_type,
                     reasoning=f"[{action}] {reasoning}", confidence=confidence, sources=sources, timeframe=timeframe)
        try:
            from engine.alpaca_options import execute_options_signal
            _ap_res = execute_options_signal(player_id, action, symbol, price, target_dte=target_dte)
            if _ap_res and not _ap_res.get("skipped") and not _ap_res.get("error"):
                _order_ids = _ap_res.get("order_ids") or []
                _order_id  = ",".join(str(o) for o in _order_ids) if _order_ids else _ap_res.get("order_id", "")
                _exec_type = "alpaca_paper" if _order_id else "simulated"
                _update_trade_alpaca_fields(player_id, symbol, _order_id, _exec_type)
        except Exception as _ae:
            # HM-U: NTFY first occurrence per error class per day (multi-leg spread/IC forward).
            console.log(f"[yellow]Alpaca {action} forward error ({player_id} {symbol}): {type(_ae).__name__}: {_ae!r}")
            try:
                from engine.alert_channels import send_alert, AlertLevel
                send_alert(
                    message=f"{action} forward ({player_id} {symbol}) {type(_ae).__name__}: {_ae!r}",
                    level=AlertLevel.WARNING,
                    alert_type=f"hm-u-multileg_forward-{action}-{type(_ae).__name__}",
                    rate_limit_secs=86400,
                )
            except Exception:
                pass
        return result

    return None


def _update_trade_alpaca_fields(player_id: str, symbol: str, order_id: str, exec_type: str,
                                 action_filter: tuple = ('BUY', 'BUY_CALL', 'BUY_PUT')) -> None:
    """Update most recent trade record for player+symbol with Alpaca order metadata.

    HM-TRADES-ALPACA-PROVENANCE 2026-05-21: action_filter widened so SELL paths
    can also stamp alpaca_order_id + execution_type. Existing options-spread
    caller keeps the BUY-only default.
    """
    try:
        placeholders = ",".join("?" for _ in action_filter)
        conn = _conn()
        conn.execute(
            f"""UPDATE trades SET alpaca_order_id=?, alpaca_status='submitted', execution_type=?
               WHERE player_id=? AND symbol=? AND action IN ({placeholders})
               ORDER BY executed_at DESC LIMIT 1""",
            (order_id or None, exec_type, player_id, symbol, *action_filter),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


def _persist_alpaca_fill(trade_id: int, action: str, qty: float,
                         result, player_id: str, symbol: str) -> None:
    """HM-TRADES-PRICE-WRITEBACK-FIX 2026-05-21: persist Alpaca fill into the
    just-inserted trades row.

    - On a real fill: UPDATE trades.entry_price (BUY) or trades.exit_price +
      realized_pnl (SELL), plus alpaca_order_id + execution_type='alpaca_paper'.
    - On submit-without-fill (poll timeout, partial, rejected): UPDATE only
      alpaca_order_id + execution_type if order_id present; log [TRADE-FILL-WARN]
      so the row's price stays the trader's internal target (audit traceability).
    - On no Alpaca attempt or hard error: do nothing (row keeps internal price).

    Never raises. Trade execution must not depend on this writeback succeeding.
    """
    if not trade_id or not result or not isinstance(result, dict):
        return
    if result.get("error"):
        console.log(
            f"[yellow][TRADE-FILL-WARN] {player_id} {action} {symbol} "
            f"trade_id={trade_id}: alpaca error: {result.get('error')!r}"
        )
        return
    order_id = result.get("order_id")
    fill_price = result.get("filled_avg_price")
    try:
        conn = _conn()
        if fill_price is not None and float(fill_price) > 0:
            fp = float(fill_price)
            if action == "BUY":
                conn.execute(
                    "UPDATE trades SET entry_price=?, alpaca_order_id=?, "
                    "alpaca_status='filled', execution_type='alpaca_paper' WHERE id=?",
                    (fp, order_id or None, trade_id),
                )
            elif action == "SELL":
                row = conn.execute(
                    "SELECT entry_price, qty FROM trades WHERE id=?",
                    (trade_id,),
                ).fetchone()
                entry_px = float(row[0]) if row and row[0] is not None else None
                row_qty = float(row[1]) if row and row[1] is not None else float(qty)
                if entry_px is not None:
                    new_pnl = (fp - entry_px) * row_qty
                    conn.execute(
                        "UPDATE trades SET exit_price=?, realized_pnl=?, "
                        "alpaca_order_id=?, alpaca_status='filled', "
                        "execution_type='alpaca_paper' WHERE id=?",
                        (fp, new_pnl, order_id or None, trade_id),
                    )
                else:
                    conn.execute(
                        "UPDATE trades SET exit_price=?, alpaca_order_id=?, "
                        "alpaca_status='filled', execution_type='alpaca_paper' WHERE id=?",
                        (fp, order_id or None, trade_id),
                    )
            conn.commit()
        else:
            # Submit succeeded but no fill price (poll timeout / extended-hours
            # limit not yet filled / partial without filled_avg_price). Stamp
            # provenance but leave the internal price in place per fail-safe spec.
            if order_id:
                conn.execute(
                    "UPDATE trades SET alpaca_order_id=?, alpaca_status=?, "
                    "execution_type='alpaca_paper' WHERE id=?",
                    (order_id, result.get("status") or "submitted", trade_id),
                )
                conn.commit()
            console.log(
                f"[yellow][TRADE-FILL-WARN] {player_id} {action} {symbol} "
                f"trade_id={trade_id} order_id={order_id} status={result.get('status')!r} "
                f"— no filled_avg_price; keeping internal price"
            )
        conn.close()
    except Exception as e:
        console.log(
            f"[yellow][TRADE-FILL-WARN] {player_id} {action} {symbol} "
            f"trade_id={trade_id}: persist error: {type(e).__name__}: {e!r}"
        )


def _persist_alpaca_fill_position(player_id: str, symbol: str, qty: float,
                                   ex_qty_before: float, ex_avg_before: float,
                                   was_new: bool, result) -> None:
    """HM-POSITIONS-AVG-PRICE-WRITEBACK 2026-05-21: recompute positions.avg_price
    from the Alpaca fill so subsequent SELL closes use broker truth as the
    realized_pnl entry basis.

    Companion to _persist_alpaca_fill which corrects the trades row. The trades
    fix alone left SELL.realized_pnl polluted because the SELL path reads
    pos["avg_price"] (from the positions table) into trades.entry_price, and
    that column was still seeded from the internal `price` (see buy() above
    around line 1077-1090 — UPDATE/INSERT into positions writes the trader's
    pre-submit target, not the broker fill).

    Pricing rules:
      - New position (was_new=True): avg_price = fill
      - Additive: avg_price = (ex_qty * ex_avg + qty * fill) / (ex_qty + qty)
        Note ex_avg may itself carry historical pollution from pre-fix BUYs
        on this symbol; we accept that — it bleeds out as those legs close.
        Future positions opened post-fix will be clean.

    Stock-only by design (caller already gates on asset_type=='stock'). Never
    raises — positions correctness must not block the trade flow.
    """
    if not result or not isinstance(result, dict) or result.get("error"):
        return
    fill_price = result.get("filled_avg_price")
    if fill_price is None:
        console.log(
            f"[yellow][POSITION-FILL-WARN] {player_id} BUY {symbol}: "
            f"no filled_avg_price; keeping internal avg_price"
        )
        return
    try:
        fp = float(fill_price)
        if fp <= 0:
            return
        q = float(qty)
        if was_new:
            new_avg = round(fp, 4)
        else:
            new_qty = ex_qty_before + q
            if new_qty <= 0:
                return
            new_avg = round(((ex_qty_before * ex_avg_before) + (q * fp)) / new_qty, 4)
        conn = _conn()
        conn.execute(
            "UPDATE positions SET avg_price=? "
            "WHERE player_id=? AND symbol=? AND asset_type='stock'",
            (new_avg, player_id, symbol),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        console.log(
            f"[yellow][POSITION-FILL-WARN] {player_id} BUY {symbol}: "
            f"persist error: {type(e).__name__}: {e!r}"
        )


def record_portfolio_snapshot(player_id: str, prices: dict):
    portfolio = get_portfolio(player_id)
    positions_value = 0.0
    for p in portfolio["positions"]:
        if p.get("asset_type") == "option":
            stock_price = prices.get(p["symbol"], {}).get("price", 0)
            est = estimate_option_price(p.get("option_type"), p.get("strike_price"),
                                        stock_price, p["avg_price"], p.get("expiry_date"))
            positions_value += p["qty"] * est
        else:
            positions_value += p["qty"] * prices.get(p["symbol"], {}).get("price", p["avg_price"])
    total = portfolio["cash"] + positions_value
    conn = _conn()
    conn.execute(
        "INSERT INTO portfolio_history (player_id, total_value, cash, positions_value, season) VALUES (?,?,?,?,?)",
        (player_id, total, portfolio["cash"], positions_value, _current_season())
    )
    conn.commit()
    conn.close()


_STARTING_CASH = {"dayblade-0dte": 3500.0, "webull": 7021.81, "super-agent": 100000.0}
_DEFAULT_STARTING_CASH = 7000.0

# Steve's Webull synced value (overrides Yahoo price calculation)
_webull_synced_value = None
_webull_synced_at = None


def _target_weight_adjustment(player_id: str, symbol: str, portfolio: dict, alloc_pct: float,
                              price: float, confidence: float = 0.0) -> tuple[float, list[str]]:
    """Soft sizing adjustment only for prospective Arena stock buys."""
    reasons = []
    # HM-I-β-Item3 (2026-05-05): added alpaca-mirror — broker-sync target,
    # not subject to per-player allocation logic.
    if player_id in {"neo-matrix", "enterprise-computer", "alpaca-mirror", "super-agent"}:
        return alloc_pct, reasons

    try:
        from engine.sector_tracker import build_sector_bucket_profile
        total_value = portfolio["cash"] + sum(
            p["qty"] * p.get("current_price", p["avg_price"])
            for p in portfolio["positions"]
        )
        proposed_cost = portfolio["cash"] * alloc_pct
        bucket = build_sector_bucket_profile(
            portfolio["positions"],
            proposed_symbol=symbol,
            proposed_value=proposed_cost,
            total_value=total_value,
        )
        for row in bucket.get("buckets", []):
            if not row.get("includes_proposed"):
                continue
            if row["status"] == "near_cap":
                alloc_pct *= 0.75
                reasons.append(f"{row['sector']} near cap")
            elif row["status"] == "over_cap":
                alloc_pct *= 0.50
                reasons.append(f"{row['sector']} over cap")
            break
    except Exception:
        pass

    try:
        from engine.correlation import get_position_correlation_profile
        total_value = portfolio["cash"] + sum(
            p["qty"] * p.get("current_price", p["avg_price"])
            for p in portfolio["positions"]
        )
        proposed_cost = portfolio["cash"] * alloc_pct
        corr = get_position_correlation_profile(
            portfolio["positions"],
            proposed_symbol=symbol,
            proposed_cost=proposed_cost,
            total_value=total_value,
        )
        for group in corr.get("group_exposure", []):
            if group.get("includes_proposed") and group.get("pct_of_portfolio", 0) > 30:
                alloc_pct *= 0.75
                reasons.append("high correlated cluster")
                break
    except Exception:
        pass

    try:
        from engine.cross_asset import get_cross_asset_monitor
        bias = (get_cross_asset_monitor().get("macro_bias") or {}).get("bias", "NEUTRAL")
        if bias in ("BEARISH", "RISK-OFF") and confidence < 0.90:
            alloc_pct *= 0.75
            reasons.append(f"macro {bias.lower()}")
    except Exception:
        pass

    return max(0.02, alloc_pct), reasons


# HM-I-β-Item3 (2026-05-05): added alpaca-mirror — broker-sync mirror,
# allocation managed externally (by Alpaca paper account state).
_ALLOCATION_POLICY_EXEMPT = {"super-agent", "neo-matrix", "enterprise-computer", "alpaca-mirror"}


# HM-DEEPSEEK-CONC-CAP 2026-05-23 — per-strategy concentration cap for
# deepseek-7b-grok4. If the agent has >20 closed trades in any single
# strategy_id over the last 30d with negative cumulative PnL, INSERT a
# 7-day pause row in agent_strategy_pauses and block subsequent trades
# for that strategy until paused_until expires. Forward-looking — at
# deploy time, deepseek's trades carry strategy_id=NULL across the
# board (118 closed trades / 30d, all NULL) so the tripwire is inert
# until strategy_id stamping lands on the agent's signal path.
def _check_deepseek_conc_cap(strategy_id: str) -> tuple[bool, str | None]:
    """Return (allowed, reason). Caller is buy() — block when False.

    Two-path logic:
      1. Active pause: existing agent_strategy_pauses row with
         paused_until > now → BLOCK with the recorded reason.
      2. Tripwire: count + sum closed deepseek-7b-grok4 trades for
         this strategy in last 30d. If n>20 AND cum_pnl<0, INSERT a
         7-day pause row and BLOCK.

    Fail-safe: caller wraps in try/except per HM-Z/HM-AA posture; any
    error here returns (True, None) so the gate fails open.
    """
    if not strategy_id:
        return (True, None)
    player_id = "deepseek-7b-grok4"
    conn = _conn()
    try:
        # Path 1 — active pause check.
        row = conn.execute(
            "SELECT paused_until, reason FROM agent_strategy_pauses "
            " WHERE player_id=? AND strategy_id=? "
            "   AND paused_until > datetime('now')",
            (player_id, strategy_id),
        ).fetchone()
        if row:
            return (False, f"already paused until {row[0]} (orig: {row[1]})")
        # Path 2 — tripwire check.
        agg = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(realized_pnl), 0) "
            "  FROM trades "
            " WHERE player_id=? AND strategy_id=? "
            "   AND executed_at >= date('now','-30 days') "
            "   AND action IN ('SELL','COVER') "
            "   AND realized_pnl IS NOT NULL",
            (player_id, strategy_id),
        ).fetchone()
        n_trades = int(agg[0] or 0)
        cum_pnl = float(agg[1] or 0)
        if n_trades > 20 and cum_pnl < 0:
            reason = (
                f"tripwire: {n_trades} trades / 30d, cum PnL ${cum_pnl:.2f}"
            )
            conn.execute(
                "INSERT OR REPLACE INTO agent_strategy_pauses "
                "(player_id, strategy_id, paused_until, reason, created_at) "
                "VALUES (?, ?, datetime('now','+7 days'), ?, datetime('now'))",
                (player_id, strategy_id, reason),
            )
            conn.commit()
            return (False, f"7-day pause initiated — {reason}")
        return (True, None)
    finally:
        conn.close()


# HM-MASTER-PLAN W5-C Blend E enforcement (2026-05-23) — long_equity cap.
# Reads regime_allocations.long_equity_pct and long_equity_max_pct for the
# current regime. If the player's already-deployed long_equity would push
# past target after adding alloc_pct * portfolio_value, scale alloc_pct
# down so total long_equity stays at or under target. Hard ceiling at
# long_equity_max_pct.
def _apply_regime_long_equity_cap(
    *,
    player_id: str,
    portfolio: dict,
    cash: float,
    alloc_pct: float,
) -> tuple[float, str | None]:
    """Return (capped_alloc_pct, log_reason_or_None).

    Fail-safe: any error returns (alloc_pct, None) — alloc unchanged.
    No mutation on rows that don't carry W5-C columns
    (long_equity_pct NULL → no opinion → no cap applied).
    """
    if alloc_pct <= 0:
        return alloc_pct, None
    if player_id in _ALLOCATION_POLICY_EXEMPT or _is_human_player(player_id):
        return alloc_pct, None
    try:
        from engine.regime_router import (
            get_current_regime, get_regime_allocation,
        )
        regime = get_current_regime()
        if not regime:
            return alloc_pct, None
        ra = get_regime_allocation(regime)
        if not ra:
            return alloc_pct, None
        target_pct = ra.get("long_equity_pct")
        ceiling_pct = ra.get("long_equity_max_pct")
        if target_pct is None and ceiling_pct is None:
            return alloc_pct, None  # regime has no Blend E opinion
        # Compute current long-equity deployed value.
        deployed_stock_value = 0.0
        for p in portfolio.get("positions", []):
            if (p.get("asset_type") == "stock") and (float(p.get("qty") or 0) > 0):
                deployed_stock_value += float(p.get("qty") or 0) * float(
                    p.get("avg_price") or 0
                )
        portfolio_value = float(cash or 0) + sum(
            float(p.get("qty") or 0) * float(p.get("avg_price") or 0)
            for p in portfolio.get("positions", [])
        )
        if portfolio_value <= 0:
            return alloc_pct, None
        current_le_pct = deployed_stock_value / portfolio_value
        # Determine the binding cap: hard ceiling wins over target if lower.
        bind_pct = None
        bind_label = None
        if target_pct is not None:
            bind_pct = float(target_pct)
            bind_label = "target"
        if ceiling_pct is not None and (
            bind_pct is None or float(ceiling_pct) < bind_pct
        ):
            bind_pct = float(ceiling_pct)
            bind_label = "ceiling"
        if bind_pct is None or bind_pct <= 0:
            return alloc_pct, None  # bucket explicitly excluded (=0) leaves
            # alloc untouched — only scale if cap is positive but lower than
            # current+intended; future enhancement could block instead.
        # Headroom in pct-of-portfolio terms.
        headroom_pct = bind_pct - current_le_pct
        if headroom_pct <= 0:
            # Already over cap — scale alloc to a token min so the trade
            # doesn't bloat the bucket further. Floor at 0 so we don't add.
            return 0.0, (
                f"long_equity over {bind_label} "
                f"({current_le_pct:.1%} >= {bind_pct:.1%} {regime}) — "
                f"blocked add"
            )
        # alloc_pct is fraction of CASH (per existing sizing math), not
        # of portfolio_value. Translate to portfolio-value fraction:
        # add_pct_of_pv = alloc_pct * cash / portfolio_value
        if portfolio_value <= 0:
            return alloc_pct, None
        intended_add_pct_of_pv = alloc_pct * (float(cash or 0) / portfolio_value)
        if intended_add_pct_of_pv <= headroom_pct:
            return alloc_pct, None  # within cap
        # Scale intended add down to headroom.
        capped_add_pct_of_pv = headroom_pct
        capped_alloc_pct = capped_add_pct_of_pv * portfolio_value / max(
            float(cash or 0), 1.0
        )
        return capped_alloc_pct, (
            f"long_equity {bind_label} {bind_pct:.1%} "
            f"(curr {current_le_pct:.1%}, regime {regime}): "
            f"alloc_pct {alloc_pct:.2%}→{capped_alloc_pct:.2%}"
        )
    except Exception as e:
        console.log(
            f"[yellow][REGIME-ALLOC-WARN] {player_id} {type(e).__name__}: "
            f"{e!r} — leaving alloc unchanged"
        )
        return alloc_pct, None


def get_capital_allocation_policy(player_id: str) -> dict:
    """Tier capital sizing from current benchmark-cycle results.

    Uses only the active benchmark cycle. Players without an active cycle stay neutral.
    """
    if player_id in _ALLOCATION_POLICY_EXEMPT or _is_human_player(player_id):
        return {
            "tier": "neutral",
            "multiplier": 1.0,
            "return_pct": 0.0,
            "win_rate": 0.0,
            "trade_count": 0,
            "benchmark_cycle_start": None,
            "benchmark_label": None,
            "reason": "exempt",
        }

    try:
        conn = _conn()
        cycle = conn.execute(
            """
            SELECT label, benchmark_cycle_start, benchmark_start_equity
            FROM player_benchmark_cycles
            WHERE player_id=? AND COALESCE(is_active, 1)=1
            ORDER BY benchmark_cycle_start DESC, id DESC
            LIMIT 1
            """,
            (player_id,),
        ).fetchone()
        if not cycle:
            conn.close()
            return {
                "tier": "neutral",
                "multiplier": 1.0,
                "return_pct": 0.0,
                "win_rate": 0.0,
                "trade_count": 0,
                "benchmark_cycle_start": None,
                "benchmark_label": None,
                "reason": "no_benchmark_cycle",
            }

        start_equity = float(cycle[2] or 0.0)
        latest = conn.execute(
            """
            SELECT total_value
            FROM portfolio_history
            WHERE player_id=? AND recorded_at >= ?
            ORDER BY recorded_at DESC, id DESC
            LIMIT 1
            """,
            (player_id, cycle[1]),
        ).fetchone()
        if latest and latest[0] is not None:
            current_value = float(latest[0])
        else:
            cash_row = conn.execute("SELECT cash FROM ai_players WHERE id=?", (player_id,)).fetchone()
            pos_row = conn.execute(
                "SELECT COALESCE(SUM(qty * avg_price), 0) FROM positions WHERE player_id=?",
                (player_id,),
            ).fetchone()
            current_value = float(cash_row[0] or 0.0) + float(pos_row[0] or 0.0)

        closed = conn.execute(
            """
            SELECT COUNT(*),
                   COALESCE(SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END), 0),
                   COALESCE(SUM(CASE WHEN realized_pnl < 0 THEN 1 ELSE 0 END), 0)
            FROM trades
            WHERE player_id=?
              AND action IN ('SELL', 'COVER')
              AND realized_pnl IS NOT NULL
              AND executed_at >= ?
            """,
            (player_id, cycle[1]),
        ).fetchone()
        conn.close()

        trade_count = int(closed[0] or 0)
        wins = int(closed[1] or 0)
        losses = int(closed[2] or 0)
        win_rate = round((wins / trade_count) * 100, 1) if trade_count > 0 else 0.0
        return_pct = round(((current_value - start_equity) / start_equity) * 100, 2) if start_equity > 0 else 0.0

        tier = "neutral"
        multiplier = 1.0
        reason = "low_sample"
        if trade_count >= 3:
            if return_pct > 5 and win_rate >= 55:
                tier = "favored"
                multiplier = 1.25
                reason = "strong_benchmark_cycle"
            elif return_pct < -5 or win_rate < 40:
                tier = "probation"
                multiplier = 0.5
                reason = "weak_benchmark_cycle"
            else:
                reason = "mixed_benchmark_cycle"

        return {
            "tier": tier,
            "multiplier": multiplier,
            "return_pct": return_pct,
            "win_rate": win_rate,
            "trade_count": trade_count,
            "benchmark_cycle_start": cycle[1],
            "benchmark_label": cycle[0],
            "reason": reason,
        }
    except Exception:
        return {
            "tier": "neutral",
            "multiplier": 1.0,
            "return_pct": 0.0,
            "win_rate": 0.0,
            "trade_count": 0,
            "benchmark_cycle_start": None,
            "benchmark_label": None,
            "reason": "policy_error",
        }


def sync_webull_value(total_value: float):
    """Manually sync Steve's Webull portfolio value (overrides Yahoo prices)."""
    global _webull_synced_value, _webull_synced_at
    from datetime import datetime
    _webull_synced_value = total_value
    _webull_synced_at = datetime.now().isoformat()
    # Persist to settings table
    try:
        conn = _conn()
        conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('webull_synced_value', ?)", (str(total_value),))
        conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('webull_synced_at', ?)", (_webull_synced_at,))
        conn.commit()
        conn.close()
    except Exception:
        pass


def get_webull_synced() -> dict | None:
    """Get the last synced Webull value."""
    global _webull_synced_value, _webull_synced_at
    if _webull_synced_value is not None:
        return {"total_value": _webull_synced_value, "synced_at": _webull_synced_at}
    # Try loading from DB
    try:
        conn = _conn()
        val_row = conn.execute("SELECT value FROM settings WHERE key='webull_synced_value'").fetchone()
        ts_row = conn.execute("SELECT value FROM settings WHERE key='webull_synced_at'").fetchone()
        conn.close()
        if val_row:
            _webull_synced_value = float(val_row[0])
            _webull_synced_at = ts_row[0] if ts_row else None
            return {"total_value": _webull_synced_value, "synced_at": _webull_synced_at}
    except Exception:
        pass
    return None


# HM-CAPITAL-HANG-EMERGENCY 2026-05-19: module-level shared executor for
# bounded per-position price fallbacks. Long-lived so the .result(timeout=3)
# timeout actually bounds wall time — `with ThreadPoolExecutor(...) as pool`
# calls shutdown(wait=True) in __exit__, which blocks until the in-flight
# future completes even after .result() raised TimeoutError. A persistent
# executor lets us submit-and-discard on timeout: the worker thread finishes
# its yfinance call in the background while the caller has already moved on.
import atexit as _atexit
from concurrent.futures import ThreadPoolExecutor as _TPE_TYPE
_PRICE_FALLBACK_POOL = _TPE_TYPE(max_workers=8, thread_name_prefix='paper_trader_price_fallback')
_atexit.register(lambda: _PRICE_FALLBACK_POOL.shutdown(wait=False))


def get_portfolio_with_pnl(player_id: str, prices: dict) -> dict:
    """Get portfolio with unrealized P&L calculated from live prices.

    For webull: uses manually synced value if available (more accurate
    than Yahoo prices which lag Webull real-time data).
    """
    from engine.market_data import get_stock_price
    portfolio = get_portfolio(player_id)
    enriched_positions = []
    total_unrealized = 0.0
    total_positions_value = 0.0
    total_cost_basis = 0.0

    for pos in portfolio["positions"]:
        symbol = pos["symbol"]
        avg_price = pos["avg_price"]
        qty = pos["qty"]
        cost_basis = qty * avg_price

        price_data = prices.get(symbol, {})
        # Auto-fetch price for non-watchlist symbols (e.g. Steve's Webull holdings)
        if not price_data or "price" not in price_data:
            # Metal positions use Yahoo futures symbols, not stock tickers
            _METAL_YAHOO = {"GOLD": "GC=F", "SILVER": "SI=F", "PLATINUM": "PL=F", "PALLADIUM": "PA=F"}
            fetch_symbol = _METAL_YAHOO.get(symbol, symbol)
            # HM-CAPITAL-HANG-EMERGENCY 2026-05-19: cap per-position network fallback at 3s.
            # Submit to the module-level _PRICE_FALLBACK_POOL (not a per-call executor — that
            # leaks back into __exit__/shutdown(wait=True) blocking on the in-flight future).
            # On timeout, abandon the future; the worker keeps running and may even populate
            # _price_cache as a side benefit, but the caller falls through to avg_price now.
            try:
                price_data = _PRICE_FALLBACK_POOL.submit(get_stock_price, fetch_symbol).result(timeout=3) or {}
            except Exception:
                price_data = {}
        if pos.get("asset_type") == "option":
            stock_price = price_data.get("price", 0)
            current_price = estimate_option_price(
                pos.get("option_type"), pos.get("strike_price"),
                stock_price, avg_price, pos.get("expiry_date"))
        else:
            current_price = price_data.get("price", avg_price)
        market_value = qty * current_price
        unrealized_pnl = market_value - cost_basis
        unrealized_pnl_pct = ((current_price - avg_price) / avg_price * 100) if avg_price > 0 else 0.0

        day_change_pct = price_data.get("change_pct", 0.0) if price_data else 0.0

        enriched_positions.append({
            **pos,
            "current_price": round(current_price, 2),
            "market_value": round(market_value, 2),
            "cost_basis": round(cost_basis, 2),
            "unrealized_pnl": round(unrealized_pnl, 2),
            "unrealized_pnl_pct": round(unrealized_pnl_pct, 2),
            "day_change_pct": round(day_change_pct, 2),
        })

        total_unrealized += unrealized_pnl
        total_positions_value += market_value
        total_cost_basis += cost_basis

    total_value = portfolio["cash"] + total_positions_value

    starting = _STARTING_CASH.get(player_id, _DEFAULT_STARTING_CASH)
    return_pct = round((total_value - starting) / starting * 100, 2) if starting > 0 else 0.0

    return {
        "cash": portfolio["cash"],
        "positions": enriched_positions,
        "total_positions_value": round(total_positions_value, 2),
        "total_cost_basis": round(total_cost_basis, 2),
        "total_unrealized_pnl": round(total_unrealized, 2),
        "total_value": round(total_value, 2),
        "return_pct": return_pct,
    }


def save_equity_snapshot(player_id: str, prices: dict):
    """Append a timestamped equity snapshot to the JSON equity curve file."""
    pnl_data = get_portfolio_with_pnl(player_id, prices)
    snapshot = {
        "player_id": player_id,
        "timestamp": datetime.now().isoformat(),
        "total_value": pnl_data["total_value"],
        "cash": pnl_data["cash"],
        "positions_value": pnl_data["total_positions_value"],
        "unrealized_pnl": pnl_data["total_unrealized_pnl"],
        "return_pct": pnl_data["return_pct"],
    }

    # Read existing data
    curve = []
    if os.path.exists(EQUITY_CURVE_FILE):
        try:
            with open(EQUITY_CURVE_FILE, "r") as f:
                curve = json.load(f)
        except (json.JSONDecodeError, IOError):
            curve = []

    curve.append(snapshot)

    # Keep last 30 days of data (at ~48 snapshots/day per player = ~1440 per player)
    max_entries = 50000
    if len(curve) > max_entries:
        curve = curve[-max_entries:]

    with open(EQUITY_CURVE_FILE, "w") as f:
        json.dump(curve, f)


def expire_options(prices: dict = None) -> dict:
    """Auto-close all options positions whose expiry_date has passed.

    HM-EXPIRE-OPTIONS-CANONICAL 2026-05-17: dual-table scan.
      • Canonical path → options_trades + close_options_trade (Fix #4
        opens, all structures: csp, bull_put_spread, bull_call_spread,
        long_call, long_put, ...).
      • Legacy path → positions-table long options (navigator,
        alpaca-mirror, dalio-metals — 7 rows as of audit 2026-05-17).
    CSP ITM-at-expiry NTFY+skip per HM-WHEEL-ASSIGNMENT-LEDGER deferral
    (Fix #5 Option C).

    Returns {"expired": N, "closed": [...]} combined across both paths.
    """
    closed_canonical = _expire_canonical(prices)
    closed_legacy = _expire_legacy(prices)
    closed = closed_canonical + closed_legacy
    if closed:
        console.log(
            f"[yellow]expire_options: closed {len(closed)} "
            f"({len(closed_canonical)} canonical + {len(closed_legacy)} legacy)"
        )
    return {"expired": len(closed), "closed": closed}


def _expire_canonical(prices: dict = None) -> list[dict]:
    """HM-EXPIRE-OPTIONS-CANONICAL canonical path — iterate options_trades.

    HM-WHEEL-CHECK-ASSIGNMENTS-DOCUMENT-DUAL-COVERAGE 2026-05-17:
    Dual-coverage with engine/wheel_strategy.py::check_wheel_assignments
    (HM-W1F5, commit 418c092). This helper fires every ~120s from
    ai_brain.run_scan with caller-passed prices dict. When prices is
    None (e.g., admin POST /api/wheel/force-expire path), the CSP-ITM
    branch below at the `if structure == 'csp'` check is SILENTLY SKIPPED
    (the `if short_puts and prices and symbol in prices` guard), and
    the function falls through to OTM-close-at-$0 for any past-expiry
    CSP — INCORRECT for an actually-ITM put. check_wheel_assignments
    is the hourly defensive fallback that fetches its own spot price
    via get_stock_price() and correctly detects ITM in this case.
    Until HM-EXPIRE-OPTIONS-PRICES-NONE-HARDENING ships, both functions
    are needed.
    """
    import json
    from datetime import date as _date
    from engine.options_exec import close_options_trade

    today_str = _date.today().strftime("%Y-%m-%d")
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT id, agent_id, structure, symbol, expiration, legs_json, entry_credit_debit "
            "FROM options_trades "
            "WHERE status='open' AND substr(expiration,1,10) <= ?",
            (today_str,),
        ).fetchall()
    finally:
        conn.close()

    closed: list[dict] = []
    for row in rows:
        trade_id, agent_id, structure, symbol, expiration, legs_json, _ent_cd = row
        try:
            legs = json.loads(legs_json)
        except (json.JSONDecodeError, TypeError):
            console.log(f"[red]expire_options canonical: bad legs_json trade {trade_id} — skipping")
            continue

        # HM-WHEEL-ASSIGNMENT-LEDGER 2026-05-18: CSP ITM-at-expiry now records
        # an assignment via engine.wheel_assignment_ledger.assign_csp instead
        # of NTFY+skip. G20=C decoupling preserved (no ai_players.cash debit).
        # Failure falls back to the original NTFY+skip behavior so a broken
        # ledger never silently swallows an ITM event.
        # HM-EXPIRE-OPTIONS-PRICES-NONE-HARDENING 2026-05-18: when prices dict
        # is None (admin force-expire path) OR missing the symbol, fall back
        # to engine.market_data.get_stock_price for the ITM probe. Closes
        # the silent OTM-close-as-$0 bug for the admin path. NTFY WARNING on
        # dual-source failure.
        if structure == "csp":
            short_puts = [l for l in legs if l.get("side") == "short" and l.get("type") == "put"]
            if short_puts:
                strike = float(short_puts[0].get("strike", 0) or 0)
                spot = 0.0
                if prices and symbol in prices:
                    spot = float((prices.get(symbol) or {}).get("price", 0) or 0)
                if (not spot) and strike > 0 and symbol:
                    try:
                        from engine.market_data import get_stock_price
                        _pd = get_stock_price(symbol)
                        spot = float((_pd or {}).get("price", 0) or 0)
                    except Exception:
                        spot = 0.0
                if (not spot) and strike > 0:
                    # Dual-source failure — surface so Captain can intervene
                    # before silent OTM-close happens.
                    try:
                        from engine.alert_channels import send_alert, AlertLevel
                        send_alert(
                            message=(
                                f"expire_options: spot resolution FAILED for "
                                f"{symbol} (trade_id={trade_id}) — both prices "
                                f"dict + get_stock_price returned no value. "
                                f"CSP ITM check skipped; OTM-close fallback "
                                f"will fire if past expiry."
                            ),
                            level=AlertLevel.WARNING,
                            alert_type=f"hm-expire-csp-spot-failed-{symbol}",
                            rate_limit_secs=86400,
                        )
                    except Exception:
                        pass
                if strike > 0 and spot > 0 and spot < strike:
                    intrinsic = round(strike - spot, 2)
                    try:
                        from engine.wheel_assignment_ledger import assign_csp
                        result = assign_csp(trade_id, spot, assignment_date=today_str)
                    except Exception as _e:
                        result = {"status": "exception", "reason": f"{type(_e).__name__}: {_e!r}"}

                    if result.get("status") == "assigned":
                        console.log(
                            f"[green]🎡 expire_options: {symbol} CSP ASSIGNED ITM — "
                            f"strike ${strike}, spot ${spot:.2f}, intrinsic ${intrinsic:.2f}, "
                            f"trade_id={trade_id} → assignment_id={result.get('assignment_id')}, "
                            f"shares={result.get('shares')}, capital=${result.get('capital'):.0f}, "
                            f"pnl=${result.get('pnl'):.2f}"
                        )
                        try:
                            from engine.alert_channels import send_alert, AlertLevel
                            send_alert(
                                message=(
                                    f"Wheel CSP assigned: {symbol} {result.get('shares')} shares @ "
                                    f"${strike} (spot ${spot:.2f}), trade_id={trade_id}, "
                                    f"assignment_id={result.get('assignment_id')}"
                                ),
                                level=AlertLevel.INFO,
                                alert_type=f"hm-csp-assigned-{symbol}",
                                rate_limit_secs=86400,
                            )
                        except Exception:
                            pass
                        closed.append({
                            "trade_id": trade_id, "agent_id": agent_id,
                            "structure": structure, "symbol": symbol,
                            "expiration": expiration, "pnl": result.get("pnl"),
                            "exit_reason": "expired_itm_assigned",
                            "path": "canonical",
                            "assignment_id": result.get("assignment_id"),
                        })
                        continue

                    # Fallback: assign_csp returned noop/partial/exception. Surface
                    # via WARNING alert so Captain can close manually. Do NOT close
                    # the row here — preserve original NTFY+skip semantics.
                    console.log(
                        f"[red]🎡 expire_options: {symbol} CSP ITM but assign_csp "
                        f"{result.get('status')} ({result.get('reason')}) — "
                        f"strike ${strike}, spot ${spot:.2f}, trade_id={trade_id} — "
                        f"MANUAL ADMIRAL CLOSE"
                    )
                    try:
                        from engine.alert_channels import send_alert, AlertLevel
                        send_alert(
                            message=(
                                f"Wheel CSP ITM but assign_csp {result.get('status')}: "
                                f"{symbol} strike ${strike}, spot ${spot:.2f}, "
                                f"trade_id={trade_id} ({result.get('reason')}) — "
                                f"manual close needed"
                            ),
                            level=AlertLevel.WARNING,
                            alert_type=f"hm-csp-assign-failed-{symbol}",
                            rate_limit_secs=86400,
                        )
                    except Exception:
                        pass
                    continue  # skip — Admiral handles fallback

        # OTM (or non-CSP structure) at expiry → close at $0 intrinsic.
        # exit_price=0.0 works for both short and long sides — neither party pays/receives.
        exit_legs = [dict(leg, exit_price=0.0) for leg in legs]
        try:
            pnl = close_options_trade(trade_id, exit_legs, exit_reason="expired_otm")
        except Exception as e:
            console.log(f"[red]expire_options canonical: close failed trade {trade_id} ({symbol}): {type(e).__name__}: {e!r}")
            continue
        if pnl is None:
            console.log(
                f"[red]expire_options canonical: close_options_trade returned None for "
                f"trade_id={trade_id} ({symbol}) — already closed or row missing"
            )
            continue
        console.log(
            f"[yellow]expire_options canonical: {symbol} {structure} trade_id={trade_id} "
            f"expired_otm pnl=${pnl:.2f}"
        )
        closed.append({
            "trade_id": trade_id, "agent_id": agent_id, "structure": structure,
            "symbol": symbol, "expiration": expiration, "pnl": pnl,
            "exit_reason": "expired_otm", "path": "canonical",
        })
    return closed


def _expire_legacy(prices: dict = None) -> list[dict]:
    """HM-EXPIRE-OPTIONS-CANONICAL legacy path — positions-table long options
    (navigator, alpaca-mirror, dalio-metals). Body preserved verbatim from
    pre-HM-EXPIRE expire_options to keep the 7 legacy rows working unchanged.
    """
    from datetime import date
    today_str = date.today().strftime("%Y-%m-%d")

    conn = _conn()
    expired_rows = conn.execute(
        "SELECT player_id, symbol, qty, avg_price, option_type, strike_price, expiry_date "
        "FROM positions WHERE asset_type='option' AND expiry_date IS NOT NULL AND expiry_date <= ?",
        (today_str,),
    ).fetchall()
    conn.close()

    closed = []
    for row in expired_rows:
        pid = row[0]; sym = row[1]; qty = row[2]; avg_price = row[3]
        ot = row[4]; strike = row[5]; exp = row[6]

        # Compute close price: intrinsic value (never negative), else $0 (expired worthless)
        close_price = 0.0
        if prices and sym in prices and strike:
            stock_price = prices[sym].get("price", 0)
            if ot == "call":
                close_price = max(0.0, round(stock_price - strike, 2))
            elif ot == "put":
                close_price = max(0.0, round(strike - stock_price, 2))

        outcome = f"${close_price:.2f} intrinsic" if close_price > 0 else "expired worthless ($0)"
        reason = f"AUTO-EXPIRED: expiry_date={exp} — {outcome}"

        result = sell(pid, sym, close_price,
                      reasoning=reason, confidence=0.0, option_type=ot)
        closed.append({
            "player_id": pid, "symbol": sym, "option_type": ot,
            "expiry_date": exp, "close_price": close_price,
            "pnl": round((close_price - avg_price) * qty, 2) if avg_price > 0 else 0,
            "path": "legacy",
        })
        console.log(
            f"[yellow]OPTION EXPIRED (legacy): {pid} {sym} {(ot or '').upper()} "
            f"exp={exp} → {outcome}"
        )

    return closed


# === HM-DECISION-AUDIT-V1 2026-05-20 ===
# Unified decision-event log. One row per: signal_emit / gate_reject /
# trade_fire. Captures market snapshot (regime + spy_change + vix) plus FK
# refs to signals.id and trades.id. See setup_db.py for schema +
# project_hm_decision_support_observability_audit Audit B for design.
#
# Fail-safe: this writer NEVER raises. A failed audit must not break the
# calling signal/trade flow. Per-snapshot lookups are independently
# try/except'd so a slow VIX fetch can't gate a signal emit.

def _capture_decision_snapshot() -> dict:
    """Best-effort market-state snapshot for decision_audit rows.

    Returns dict with regime / spy_change / vix; any field that fails
    individual capture is None. Crash-safe — never raises.
    """
    snap: dict = {"regime": None, "spy_change": None, "vix": None}
    # 1. Today's regime
    try:
        _rc = _conn()
        try:
            _row = _rc.execute(
                "SELECT regime FROM regime_history WHERE date = date('now', 'localtime') "
                "ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if _row is not None:
                snap["regime"] = _row[0]
        finally:
            _rc.close()
    except Exception:
        pass
    # 2. SPY intraday change
    try:
        from engine.market_data import get_stock_price as _gsp_da
        _spy = _gsp_da("SPY") or {}
        if "error" not in _spy:
            _chg = _spy.get("change_pct")
            if isinstance(_chg, (int, float)):
                snap["spy_change"] = float(_chg)
    except Exception:
        pass
    # 3. VIX (uses the 30-min cached helper below — sub-ms warm)
    try:
        _v = _get_vix_cached()
        if isinstance(_v, (int, float)):
            snap["vix"] = float(_v)
    except Exception:
        pass
    return snap


def _fire_post_mortem_async(*, player_id: str, symbol: str, side: str,
                            entry: float | None, exit_price: float,
                            pnl: float, asset_type: str,
                            reasoning: str | None = None,
                            trade_id: int | None = None) -> None:
    """HM-AUTO-POST-MORTEM (POC Day 3a) 2026-05-22 — fire-and-forget
    classifier on every close.

    Calls local qwen3:8b via OLLIE_URL within 30s of close to classify
    the outcome and write a one-sentence post-mortem to decision_audit.

    Taxonomy:
      WIN_AS_EXPECTED       — thesis held, gains realized
      WIN_BY_ACCIDENT       — gains realized but thesis didn't hold
                              (regime move, lucky catalyst, etc.)
      LOSS_AS_EXPECTED      — stop fired cleanly, thesis still valid
      LOSS_FROM_THESIS_BREAK— thesis broke (earnings miss, etc.)
      LOSS_FROM_REGIME_CHANGE — macro/regime shifted under the trade

    Cost: $0 (local qwen3:8b on Ollie Box per CLAUDE.md Free Models
    First doctrine). Timeout: 15s. Fail-safe: any error logs [PM-TIMEOUT]
    or [PM-ERROR] and continues — the SELL fill is never affected.
    """
    import threading

    def _worker() -> None:
        try:
            import os
            import requests
            from datetime import datetime as _dt
            ollie_url = os.getenv("OLLIE_URL", "http://192.168.1.166:11434")
            # Pull current regime so the classifier has macro context.
            _regime = "UNKNOWN"
            try:
                _rcn = _conn()
                _rr = _rcn.execute(
                    "SELECT regime FROM regime_history "
                    "WHERE date = date('now','localtime') "
                    "ORDER BY id DESC LIMIT 1"
                ).fetchone()
                if _rr:
                    _regime = _rr[0]
                _rcn.close()
            except Exception:
                pass
            # Build the prompt — short, structured, under 200 tokens out.
            _reason_snip = (reasoning or "")[:120].replace("\n", " ")
            _entry_str = f"{entry:.2f}" if entry is not None else "n/a"
            prompt = (
                f"Trade closed: {symbol} {side} entry={_entry_str} "
                f"exit={exit_price:.2f} pnl={pnl:.2f} "
                f"regime={_regime} reasoning_snippet=\"{_reason_snip}\"\n"
                f"Classify outcome with ONE tag from: "
                f"WIN_AS_EXPECTED | WIN_BY_ACCIDENT | LOSS_AS_EXPECTED | "
                f"LOSS_FROM_THESIS_BREAK | LOSS_FROM_REGIME_CHANGE.\n"
                f"Then write ONE sentence explaining why.\n"
                f"Format: TAG: explanation"
            )
            try:
                r = requests.post(
                    f"{ollie_url}/api/generate",
                    json={
                        "model": "qwen3:8b",
                        "prompt": prompt,
                        "stream": False,
                        "think": False,
                        "options": {"num_predict": 120, "temperature": 0.3},
                    },
                    timeout=15,
                )
                if r.status_code != 200:
                    console.log(
                        f"[yellow][PM-ERROR] sym={symbol} player={player_id} "
                        f"HTTP {r.status_code}"
                    )
                    return
                text = (r.json() or {}).get("response", "").strip()
            except requests.exceptions.Timeout:
                console.log(
                    f"[yellow][PM-TIMEOUT] symbol={symbol} player={player_id}"
                )
                return
            except Exception as _e:
                console.log(
                    f"[yellow][PM-ERROR] sym={symbol} player={player_id}: "
                    f"{type(_e).__name__}: {_e!r}"
                )
                return
            if not text:
                return
            # Extract leading TAG (first ALL-CAPS_UNDERSCORED token).
            _tag = "UNCLASSIFIED"
            for _candidate in (
                "WIN_AS_EXPECTED", "WIN_BY_ACCIDENT",
                "LOSS_AS_EXPECTED", "LOSS_FROM_THESIS_BREAK",
                "LOSS_FROM_REGIME_CHANGE",
            ):
                if _candidate in text.upper():
                    _tag = _candidate
                    break
            # Persist to decision_audit. Crash-safe — bus failures never
            # block; we already returned the trade fill to the caller.
            try:
                conn = _conn()
                conn.execute(
                    "INSERT INTO decision_audit "
                    "(event_type, player_id, symbol, trade_id, regime, "
                    " gate_verdict, reasoning_snippet) "
                    "VALUES (?,?,?,?,?,?,?)",
                    ("post_mortem", player_id, symbol, trade_id, _regime,
                     _tag, text[:600]),
                )
                conn.commit()
                conn.close()
                console.log(
                    f"[cyan][POST-MORTEM] {symbol} {side} pnl=${pnl:+.2f} "
                    f"tag={_tag}"
                )
            except Exception as _pe:
                console.log(
                    f"[yellow][PM-AUDIT-WARN] persist sym={symbol}: "
                    f"{type(_pe).__name__}: {_pe!r}"
                )
        except Exception as _outer:
            console.log(
                f"[red][PM-WORKER-CRASH] sym={symbol}: "
                f"{type(_outer).__name__}: {_outer!r}"
            )

    try:
        threading.Thread(
            target=_worker, daemon=True,
            name=f"post_mortem_{symbol}",
        ).start()
    except Exception as _te:
        console.log(
            f"[red][PM-SPAWN-FAIL] sym={symbol}: "
            f"{type(_te).__name__}: {_te!r}"
        )


def _emit_trade_to_bus(*, player_id: str, symbol: str, action: str,
                       qty: float, price: float, asset_type: str,
                       signal_id: int | None, trade_id: int | None,
                       reasoning: str | None = None) -> None:
    """HM-EVENTS-BUS-FOUNDATION 2026-05-22 — push a trade-fire event onto the
    canonical bus + flip the originating signals_v2 row to status='executed'.

    Fail-safe: any error logs [EVENTS-BUS-WARN] and returns. A bus write
    MUST NEVER block a trade. Mirrors the safety posture of
    _write_decision_audit() above.
    """
    try:
        from engine.events_bus import emit_event, mark_signal_executed
        emit_event(
            source=player_id, event_type="trade", symbol=symbol,
            payload={
                "action": action, "qty": qty, "price": price,
                "asset_type": asset_type, "signal_id": signal_id,
                "trade_id": trade_id,
                "reasoning_snippet": (reasoning or "")[:300],
            },
        )
        if signal_id is not None and signal_id >= 0:
            mark_signal_executed(signal_id=signal_id, trade_id=trade_id)
    except Exception as _e:
        console.log(
            f"[yellow][EVENTS-BUS-WARN] trade-fire hook "
            f"player={player_id} sym={symbol} action={action}: "
            f"{type(_e).__name__}: {_e!r}"
        )


def _write_decision_audit(
    event_type: str,
    player_id: str | None = None,
    symbol: str | None = None,
    signal_id: int | None = None,
    trade_id: int | None = None,
    confidence: float | None = None,
    gate_verdict: str | None = None,
    reasoning_snippet: str | None = None,
    raw_confidence: float | None = None,
    meta_confidence: float | None = None,
    confidence_modifier: float | None = None,
) -> None:
    """Write a single decision_audit row. Crash-safe — never raises.

    event_type ∈ {'signal_emit', 'gate_reject', 'trade_fire'}. Other fields
    are event-dependent: gate_reject usually has gate_verdict; trade_fire
    has trade_id; signal_emit has signal_id + reasoning_snippet.

    HM-DECISION-AUDIT-V1.1 2026-05-22: gate_reject rows additionally populate
    raw_confidence (LLM emit), meta_confidence (post-learning_engine downgrade),
    and confidence_modifier (model_adjustments.confidence_modifier) so the
    24-point downgrade math (e.g. deepseek 0.85 × 0.72 = 0.61) is queryable.
    """
    try:
        snap = _capture_decision_snapshot()
        _snippet = (reasoning_snippet or "")[:300] if reasoning_snippet else None
        _ac = _conn()
        try:
            _ac.execute(
                "INSERT INTO decision_audit "
                "(event_type, player_id, symbol, signal_id, trade_id, "
                " regime, spy_change, vix, confidence, gate_verdict, reasoning_snippet, "
                " raw_confidence, meta_confidence, confidence_modifier) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    event_type,
                    player_id,
                    symbol,
                    signal_id if (signal_id is not None and signal_id >= 0) else None,
                    trade_id if (trade_id is not None and trade_id >= 0) else None,
                    snap.get("regime"),
                    snap.get("spy_change"),
                    snap.get("vix"),
                    confidence,
                    (gate_verdict or "")[:300] if gate_verdict else None,
                    _snippet,
                    raw_confidence,
                    meta_confidence,
                    confidence_modifier,
                ),
            )
            _ac.commit()
        finally:
            _ac.close()
    except Exception:
        # Audit must never break the calling flow. Suppress all errors.
        pass
# === /HM-DECISION-AUDIT-V1 ===


def save_signal(player_id: str, symbol: str, signal: str, confidence: float,
                reasoning: str, asset_type: str = "stock", option_type: str = None,
                sources: str = "", timeframe: str = "SWING",
                prompt_version: str | None = None) -> int:
    """Save signal and return its rowid for status tracking. Returns -1 on error.

    HM-PROMPT-VERSIONING (POC Day 2b) 2026-05-22: prompt_version is optional;
    callers that don't specify it fall back to `f"{player_id}_v1"` as the
    default tag. Bump the tag at the call site when the agent's prompt
    template changes (e.g. v1 → v2), so the learning loop can compare
    WR/expectancy across prompt revisions.
    """
    # HOLD signals are informational — mark as SKIPPED immediately
    _default_status = "SKIPPED" if signal == "HOLD" else "PENDING"
    # Default prompt_version tag — caller can override.
    if prompt_version is None:
        prompt_version = f"{player_id}_v1"
    try:
        conn = _conn()
        # === HALT GATE === Suppress signals from halted players (any non-active mode).
        # Per XO_AUDIT_2026-05-03 #1: ollama-llama leaked 947 post-halt rows here.
        from engine.halt_gate import can_emit_signal
        if not can_emit_signal(conn, player_id):
            conn.close()
            console.log(f"[yellow][HALT-GATE] Suppressed signal from {player_id} (not active)")
            return -1
        cur = conn.execute(
            "INSERT INTO signals (player_id, symbol, signal, confidence, reasoning, "
            "asset_type, option_type, season, sources, timeframe, execution_status, "
            "prompt_version) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (player_id, symbol, signal, confidence, reasoning,
             asset_type, option_type, _current_season(), sources, timeframe,
             _default_status, prompt_version)
        )
        signal_id = cur.lastrowid
        conn.commit()
        conn.close()
        # HM-DECISION-AUDIT-V1 2026-05-20: signal_emit hook
        _write_decision_audit(
            event_type="signal_emit",
            player_id=player_id,
            symbol=symbol,
            signal_id=signal_id,
            confidence=confidence,
            reasoning_snippet=reasoning,
        )
        # HM-EVENTS-BUS-FOUNDATION 2026-05-22: drop a row in the canonical
        # events bus + a normalized signals_v2 row. Fail-safe — bus errors
        # never block the calling path.
        try:
            from engine.events_bus import emit_event, emit_signal_v2
            _tf_norm = (timeframe or "SWING").lower()
            _ebus_payload = {
                "signal_id_v1": signal_id,
                "action": signal,
                "asset_type": asset_type,
                "option_type": option_type,
                "sources": sources,
                "reasoning_snippet": (reasoning or "")[:300],
            }
            _event_id = emit_event(
                source=player_id, event_type="signal", symbol=symbol,
                payload=_ebus_payload,
            )
            # Direction: BUY → LONG, SELL → SHORT, HOLD → NEUTRAL.
            _dir_map = {"BUY": "LONG", "SELL": "SHORT", "HOLD": "NEUTRAL"}
            _direction = _dir_map.get((signal or "").upper(), signal)
            _strategy_tag = (
                "long_call" if asset_type == "option" and option_type == "call"
                else "long_put" if asset_type == "option" and option_type == "put"
                else "long_equity"
            )
            emit_signal_v2(
                source=player_id, signal_type="momentum", symbol=symbol,
                direction=_direction, confidence=confidence,
                timeframe=_tf_norm, strategy_tag=_strategy_tag,
                event_id=_event_id,
                metadata={"reasoning_excerpt": (reasoning or "")[:200]},
            )
        except Exception as _ebus_e:
            console.log(
                f"[yellow][EVENTS-BUS-WARN] save_signal hook "
                f"player={player_id} sym={symbol}: "
                f"{type(_ebus_e).__name__}: {_ebus_e!r}"
            )
        return signal_id
    except Exception as e:
        console.log(f"[red]DB error: {e}")
        return -1


def update_signal_status(signal_id: int, status: str, reason: str = None):
    """Update execution_status and rejection_reason for a saved signal by rowid."""
    if signal_id < 0:
        return
    try:
        conn = _conn()
        conn.execute(
            "UPDATE signals SET execution_status=?, rejection_reason=? WHERE rowid=?",
            (status, reason[:300] if reason else None, signal_id)
        )
        conn.commit()
        # HM-DECISION-AUDIT-V1 2026-05-20: gate_reject hook (only on REJECTED)
        # HM-DECISION-AUDIT-V1.1 2026-05-22: capture raw/meta/modifier on
        # LOW_CONVICTION rejects so the gate-downgrade math is queryable.
        if status == "REJECTED":
            _row_for_audit = None
            try:
                _row_for_audit = conn.execute(
                    "SELECT player_id, symbol, confidence FROM signals WHERE rowid=?",
                    (signal_id,),
                ).fetchone()
            except Exception:
                pass
            conn.close()
            _raw = _row_for_audit[2] if _row_for_audit is not None else None
            _meta = None
            _modifier = None
            # Parse meta_confidence from "LOW_CONVICTION: NN% below MM% minimum"
            if reason and "LOW_CONVICTION" in reason:
                import re as _re_da
                _m = _re_da.search(r"LOW_CONVICTION:\s*(\d+(?:\.\d+)?)%", reason)
                if _m is not None:
                    try:
                        _meta = float(_m.group(1)) / 100.0
                    except (TypeError, ValueError):
                        _meta = None
                if _row_for_audit is not None:
                    _pid = _row_for_audit[0]
                    try:
                        _mc = _conn()
                        try:
                            _mrow = _mc.execute(
                                "SELECT new_value FROM model_adjustments "
                                "WHERE player_id=? AND adjustment_type='confidence_modifier' "
                                "AND effective_date <= date('now') "
                                "ORDER BY created_at DESC LIMIT 1",
                                (_pid,),
                            ).fetchone()
                            if _mrow is not None:
                                try:
                                    _modifier = float(_mrow[0])
                                except (TypeError, ValueError):
                                    _modifier = None
                        finally:
                            _mc.close()
                    except Exception:
                        pass
            if _row_for_audit is not None:
                _write_decision_audit(
                    event_type="gate_reject",
                    player_id=_row_for_audit[0],
                    symbol=_row_for_audit[1],
                    signal_id=signal_id,
                    confidence=_raw,
                    gate_verdict=reason,
                    raw_confidence=_raw,
                    meta_confidence=_meta,
                    confidence_modifier=_modifier,
                )
            else:
                _write_decision_audit(
                    event_type="gate_reject",
                    signal_id=signal_id,
                    gate_verdict=reason,
                    meta_confidence=_meta,
                )
        else:
            conn.close()
    except Exception:
        pass


# Per-player last rejection reason — set by buy() so ai_brain can read it
_last_rejection: dict = {}

# ─────────────────────────────────────────────────────────────────────────────
# VIX circuit breaker (cached, refreshed every 30 min)
# ─────────────────────────────────────────────────────────────────────────────
_vix_cache: dict = {"value": None, "fetched_at": 0.0}
_VIX_CIRCUIT_BREAKER = 30.0  # Pause new entries when VIX > 30


def _get_vix_cached() -> float | None:
    """Return VIX with 30-min cache. Returns None if unavailable."""
    import time as _time
    now = _time.time()
    if _vix_cache["value"] is not None and (now - _vix_cache["fetched_at"]) < 1800:
        return _vix_cache["value"]
    try:
        from engine.market_data import get_vix as _get_vix_fn
        v = _get_vix_fn()
        if v:
            _vix_cache["value"] = float(v)
            _vix_cache["fetched_at"] = now
            return float(v)
    except Exception:
        pass
    return _vix_cache.get("value")


# ─────────────────────────────────────────────────────────────────────────────
# Kelly Criterion position sizing
# ─────────────────────────────────────────────────────────────────────────────

def get_kelly_fraction(player_id: str) -> float:
    """Compute half-Kelly fraction from player's historical win rate + avg win/loss.

    Returns a fraction in [0.02, 0.25]. Falls back to 0.10 with < 5 closed trades.
    """
    try:
        conn = _conn()
        row = conn.execute("""
            SELECT
                COUNT(CASE WHEN realized_pnl > 0 THEN 1 END)     AS wins,
                COUNT(CASE WHEN realized_pnl < 0 THEN 1 END)     AS losses,
                AVG(CASE WHEN realized_pnl > 0 THEN realized_pnl ELSE NULL END) AS avg_win,
                AVG(CASE WHEN realized_pnl < 0 THEN ABS(realized_pnl) ELSE NULL END) AS avg_loss
            FROM trades
            WHERE player_id=? AND action='SELL' AND realized_pnl IS NOT NULL
        """, (player_id,)).fetchone()
        conn.close()
        wins = row[0] or 0
        losses = row[1] or 0
        total = wins + losses
        if total < 5:
            return 0.10
        W = wins / total
        avg_win = row[2] or 100.0
        avg_loss = row[3] or 100.0
        R = avg_win / avg_loss if avg_loss > 0 else 1.0
        kelly = W - (1 - W) / R
        return max(0.02, min(0.25, kelly * 0.5))  # half-Kelly, clamped
    except Exception:
        return 0.10


# HM-KELLY-TIER-MULTIPLIER 2026-05-23 — Captain tier rules:
#   Sharpe > 10 → 2.0x cap multiplier  (Tier 2)
#   Sharpe >  5 → 1.5x cap multiplier  (Tier 1)
#   else        → 1.0x (default behavior, no change vs pre-2026-05-23)
#
# Sharpe source: direct trailing 90-day per-trade realized_pnl,
# computed with the same compute_sharpe() formula used in
# run_comprehensive_backtest.py for comparability against the
# published OOS-Sharpe baselines.
#
# Cache: per-process dict keyed by player_id with 5-minute TTL so
# Sharpe isn't recomputed on every buy() call. Cleared on process
# restart — see CLAUDE.md "Alert rate-limit semantics — in-memory only"
# for the same in-memory pattern doctrine.
import time as _time_module
_KELLY_SHARPE_CACHE: dict[str, tuple[float, float]] = {}
_KELLY_SHARPE_TTL_S = 300  # 5 min

def _compute_trailing_sharpe(player_id: str, days: int = 90) -> float:
    """Annualized Sharpe over last N days, per compute_sharpe() formula
    (per-trade PnL, sqrt(N) annualization, 4.5% risk-free).

    Returns 0.0 if < 2 trades in window or stdev=0. Fail-safe: any
    error returns 0.0 (which maps to no tier multiplier — safe default).
    """
    cache_key = f"{player_id}:{days}"
    cached = _KELLY_SHARPE_CACHE.get(cache_key)
    now = _time_module.time()
    if cached and (now - cached[1]) < _KELLY_SHARPE_TTL_S:
        return cached[0]
    try:
        conn = _conn()
        rows = conn.execute(
            "SELECT realized_pnl FROM trades "
            " WHERE player_id=? AND action IN ('SELL','COVER') "
            "   AND realized_pnl IS NOT NULL "
            "   AND executed_at >= date('now', ?) "
            " ORDER BY executed_at",
            (player_id, f'-{int(days)} days')
        ).fetchall()
        conn.close()
        pnls = [float(r[0]) for r in rows]
        if len(pnls) < 2:
            sharpe = 0.0
        else:
            import statistics as _stat
            import math as _math
            s = _stat.stdev(pnls)
            if s == 0:
                sharpe = 0.0
            else:
                daily_rf = 0.045 / 252
                sharpe = round(
                    (_stat.mean(pnls) - daily_rf) / s * _math.sqrt(len(pnls)),
                    2,
                )
    except Exception:
        sharpe = 0.0
    _KELLY_SHARPE_CACHE[cache_key] = (sharpe, now)
    return sharpe


def get_kelly_cap_multiplier(player_id: str) -> float:
    """Return Kelly cap multiplier based on trailing 90d Sharpe.

    Tier 2 (Sharpe > 10): 2.0× — verified high-edge agents get
        their per-trade sizing caps doubled.
    Tier 1 (Sharpe >  5): 1.5× — moderate-edge agents get +50%.
    Default            : 1.0× — unchanged from pre-HM-KELLY-TIER ship.
    """
    s = _compute_trailing_sharpe(player_id, days=90)
    if s > 10:
        return 2.0
    if s > 5:
        return 1.5
    return 1.0


def get_kelly_tier_label(player_id: str) -> str:
    """Return 'tier_2', 'tier_1', or 'default' — used by /api/ratings
    to surface a per-agent Kelly tier badge on the Fleet Report Card."""
    s = _compute_trailing_sharpe(player_id, days=90)
    if s > 10:
        return "tier_2"
    if s > 5:
        return "tier_1"
    return "default"


# ─────────────────────────────────────────────────────────────────────────────
# Auto option exits: 50% TP · 2x SL · 21 DTE time stop
# ─────────────────────────────────────────────────────────────────────────────

# C2-stale-expiry-guard: helper used by check_option_exits to skip already-expired contracts.
def _is_option_expiry_passed(expiry_date) -> bool:
    """Return True if expiry_date is strictly before today.

    Why: deep-ITM expired contracts (e.g. dalio-metals GOOGL CALL exp 2026-05-01,
    strike 275, stock $384) yield intrinsic > 1.5× entry premium, which trips
    AUTO-TP every cycle. expire_options() is the canonical cleanup path; AUTO-TP
    must not race it. For tracking-only routes (dalio-metals) the row never
    deletes, so AUTO-TP would otherwise loop forever.

    Falsy / unparseable values return False (fail-open — let downstream handle).
    """
    if not expiry_date:
        return False
    try:
        from datetime import datetime, date as _date
        if isinstance(expiry_date, _date) and not isinstance(expiry_date, datetime):
            exp = expiry_date
        elif isinstance(expiry_date, datetime):
            exp = expiry_date.date()
        else:
            exp = datetime.strptime(str(expiry_date)[:10], "%Y-%m-%d").date()
        return exp < _date.today()
    except (ValueError, TypeError):
        return False


def check_option_exits(prices: dict = None) -> dict:
    """Check all open option positions and auto-exit on TP/SL/time-stop rules.

    Rules (LONG options only):
    • Take-profit: exit when current value >= 1.5× entry premium (50% gain).
    • Stop-loss:   exit when current value <= 0.5× entry premium (50% loss).
    • Time stop:   exit spreads with DTE ≤ 21 (theta decay accelerates here).

    HM-EXPIRE-OPTIONS-CANONICAL 2026-05-17: dual-table scan, long-only on
    the canonical path. Short-premium structures (csp, bull_put_spread,
    bear_call_spread) skipped from the canonical scan because LONG TP/SL
    rules INVERT for short premium (banked as
    HM-CHECK-OPTION-EXITS-SHORT-PREMIUM-RULES).

    Called once per scanner cycle from ai_brain.run_scan().
    """
    closed_legacy = _check_option_exits_legacy(prices)
    closed_canonical = _check_option_exits_canonical_long_only(prices)
    # HM-CHECK-OPTION-EXITS-SHORT-PREMIUM-RULES 2026-05-18 — wheel CSP path.
    closed_short = _check_option_exits_canonical_short_premium(prices)
    closed = closed_legacy + closed_canonical + closed_short
    return {"auto_exited": len(closed), "closed": closed}


def _check_option_exits_legacy(prices: dict = None) -> list[dict]:
    """HM-EXPIRE-OPTIONS-CANONICAL legacy path — positions-table options.
    Body preserved verbatim from pre-HM-EXPIRE check_option_exits."""
    from datetime import date as _date
    today = _date.today()

    conn = _conn()
    opt_positions = conn.execute(
        "SELECT player_id, symbol, qty, avg_price, option_type, strike_price, expiry_date "
        "FROM positions WHERE asset_type='option'"
    ).fetchall()
    conn.close()

    closed = []
    for row in opt_positions:
        pid, sym, qty, avg_price, ot, strike, expiry = row
        if _is_human_player(pid) or avg_price <= 0:
            continue

        # C2-stale-expiry-guard: skip AUTO-TP / AUTO-SL / TIME-STOP for options past
        # expiration. expire_options() owns the post-expiry path; this prevents
        # futile re-firing on rows that tracking-only routes never delete.
        if _is_option_expiry_passed(expiry):
            console.log(f"[dim]AUTO-TP skip {sym}: option expired {expiry}")
            continue

        # Estimate current option value
        current_price = avg_price
        if prices and sym in prices and strike:
            stock_price = prices[sym].get("price", 0)
            current_price = estimate_option_price(ot, strike, stock_price, avg_price, expiry)

        reason = None

        if current_price >= avg_price * 1.5:
            reason = f"AUTO-TP: +50% on {sym} {ot} (${avg_price:.2f}→${current_price:.2f})"
        elif current_price <= avg_price * 0.50:
            reason = f"AUTO-SL: -50% on {sym} {ot} (${avg_price:.2f}→${current_price:.2f})"
        elif expiry:
            try:
                exp_date = datetime.strptime(expiry, "%Y-%m-%d").date()
                dte = (exp_date - today).days
                if 0 < dte <= 21:
                    reason = f"TIME-STOP: {sym} {ot} at {dte} DTE — exiting spread"
            except (ValueError, TypeError):
                pass

        if reason:
            result = sell(pid, sym, current_price, asset_type="option", option_type=ot,
                         reasoning=reason, confidence=1.0)
            if result:
                closed.append({"player_id": pid, "symbol": sym, "option_type": ot,
                               "reason": reason[:60], "path": "legacy"})
                console.log(f"[cyan]OPTION EXIT (legacy): {pid} {sym} {(ot or '').upper()} — {reason[:60]}")

    return closed


_CANONICAL_LONG_STRUCTURES = ("long_call", "long_put")


def _check_option_exits_canonical_long_only(prices: dict = None) -> list[dict]:
    """HM-EXPIRE-OPTIONS-CANONICAL canonical path — options_trades, LONG ONLY.

    Structure filter is the G14 short-premium guard: csp/bull_put_spread/
    bear_call_spread skipped because LONG TP/SL rules invert for short premium
    (banked as HM-CHECK-OPTION-EXITS-SHORT-PREMIUM-RULES).
    """
    import json
    from datetime import date as _date
    from engine.options_exec import close_options_trade

    today = _date.today()
    placeholders = ",".join("?" for _ in _CANONICAL_LONG_STRUCTURES)
    conn = _conn()
    try:
        rows = conn.execute(
            f"SELECT id, agent_id, structure, symbol, expiration, legs_json, entry_credit_debit "
            f"FROM options_trades WHERE status='open' AND structure IN ({placeholders})",
            _CANONICAL_LONG_STRUCTURES,
        ).fetchall()
    finally:
        conn.close()

    closed: list[dict] = []
    for row in rows:
        trade_id, agent_id, structure, symbol, expiration, legs_json, ent_cd = row
        try:
            legs = json.loads(legs_json)
        except (json.JSONDecodeError, TypeError):
            console.log(f"[red]check_option_exits canonical: bad legs_json trade {trade_id} — skipping")
            continue

        # Skip if expired — expire_options owns post-expiry
        try:
            exp_d = datetime.strptime(expiration[:10], "%Y-%m-%d").date()
            if exp_d <= today:
                continue
            dte = (exp_d - today).days
        except (ValueError, TypeError):
            continue

        long_legs = [l for l in legs if l.get("side") == "long"]
        if not long_legs:
            continue
        leg = long_legs[0]
        entry_premium = float(leg.get("entry_price", 0) or 0)
        strike = float(leg.get("strike", 0) or 0)
        opt_type = leg.get("type")
        qty = int(leg.get("qty", 0) or 0)
        if entry_premium <= 0 or strike <= 0 or qty <= 0:
            continue

        # Current option value via existing helper
        current_price = entry_premium
        if prices and symbol in prices:
            stock_price = float((prices.get(symbol) or {}).get("price", 0) or 0)
            if stock_price > 0:
                current_price = estimate_option_price(opt_type, strike, stock_price, entry_premium, expiration[:10])

        reason = None
        if current_price >= entry_premium * 1.5:
            reason = f"AUTO-TP: +50% on {symbol} {opt_type} (${entry_premium:.2f}→${current_price:.2f})"
            exit_tag = "tp_hit"
        elif current_price <= entry_premium * 0.50:
            reason = f"AUTO-SL: -50% on {symbol} {opt_type} (${entry_premium:.2f}→${current_price:.2f})"
            exit_tag = "sl_hit"
        elif 0 < dte <= 21:
            reason = f"TIME-STOP: {symbol} {opt_type} at {dte} DTE"
            exit_tag = "time_stop"

        if reason is None:
            continue

        # LONG-option close = sell-to-close. close_options_trade's sign convention:
        # close_cost = exit_price × qty × 100; pnl = entry_credit_debit - close_cost.
        # For sell-to-close at $X (credit), close_cost must be NEGATIVE → exit_price = -X.
        # Verified by synthetic test I14 (scripts/hm_expire_options_synthetic.py).
        exit_legs = [dict(leg, exit_price=-current_price)]
        try:
            pnl = close_options_trade(trade_id, exit_legs, exit_reason=exit_tag)
        except Exception as e:
            console.log(f"[red]check_option_exits canonical: close failed trade {trade_id}: {type(e).__name__}: {e!r}")
            continue
        if pnl is None:
            console.log(f"[red]check_option_exits canonical: close_options_trade None trade {trade_id} ({symbol})")
            continue
        console.log(f"[cyan]OPTION EXIT (canonical): {agent_id} {symbol} {opt_type} — {reason[:60]} pnl=${pnl:.2f}")
        closed.append({
            "trade_id": trade_id, "agent_id": agent_id, "structure": structure,
            "symbol": symbol, "reason": reason[:60], "exit_reason": exit_tag,
            "pnl": pnl, "path": "canonical",
        })
    return closed


# ─────────────────────────────────────────────────────────────────────────────
# HM-CHECK-OPTION-EXITS-SHORT-PREMIUM-RULES 2026-05-18 — wheel CSP TP/SL/TIME-STOP.
# Inverted rules vs long path. Initial scope = csp only (bull_put_spread /
# bear_call_spread / iron_condor have own exit_manager).
# Premium source order: Polygon get_option_quote → BSM-style estimate_option_price
# fallback (paper-strike CSPs have no real Polygon chain entry — banked
# HM-WHEEL-STRIKE-SNAP-TO-REAL).
# ─────────────────────────────────────────────────────────────────────────────

_CANONICAL_SHORT_PREMIUM_STRUCTURES = ("csp",)


def _occ_from_csp(symbol: str, expiration: str, strike: float) -> str | None:
    """Build OCC ticker for a wheel CSP short put.
    Format: O:<UNDERLYING><YYMMDD>P<strike-mil-padded-8>.
    Returns None on bad input."""
    try:
        ymd = datetime.strptime(expiration[:10], "%Y-%m-%d").strftime("%y%m%d")
        strike_mil = int(round(float(strike) * 1000))
        if strike_mil <= 0 or not symbol:
            return None
        return f"O:{symbol.upper()}{ymd}P{strike_mil:08d}"
    except Exception:
        return None


def _csp_current_premium(symbol: str, expiration: str, strike: float,
                         entry_premium: float, stock_price: float | None) -> float | None:
    """Resolve current premium for a CSP short put.

    Tries Polygon mid quote first. Falls back to estimate_option_price (BSM-
    style) when Polygon returns None. Returns None when neither source
    yields a positive number.
    """
    occ = _occ_from_csp(symbol, expiration, strike)
    if occ:
        try:
            from engine.providers.polygon_provider import PolygonData
            pd = PolygonData()
            if pd.is_active():
                q = pd.get_option_quote(occ)
                if q and (q.get("mid") or 0) > 0:
                    return float(q["mid"])
        except Exception:
            pass

    # Fallback: BSM-style estimate based on stock + entry premium + time
    if stock_price and stock_price > 0:
        try:
            est = estimate_option_price("put", strike, stock_price, entry_premium, expiration[:10])
            if est and est > 0:
                return float(est)
        except Exception:
            pass

    return None


def _check_option_exits_canonical_short_premium(prices: dict = None) -> list[dict]:
    """HM-CHECK-OPTION-EXITS-SHORT-PREMIUM-RULES 2026-05-18 — wheel CSP TP/SL.

    Rules (INVERTED vs long path):
      - TP:        current_premium <= entry_premium * 0.50 → close (50% decay captured)
      - SL:        current_premium >= entry_premium * 2.00 → defensive close
      - TIME-STOP: 0 < dte <= 21                            → roll-or-close
    """
    import json
    from datetime import date as _date
    from engine.options_exec import close_options_trade

    today = _date.today()
    placeholders = ",".join("?" for _ in _CANONICAL_SHORT_PREMIUM_STRUCTURES)
    conn = _conn()
    try:
        rows = conn.execute(
            f"SELECT id, agent_id, structure, symbol, expiration, legs_json, "
            f"       entry_credit_debit "
            f"FROM options_trades WHERE status='open' AND structure IN ({placeholders})",
            _CANONICAL_SHORT_PREMIUM_STRUCTURES,
        ).fetchall()
    finally:
        conn.close()

    closed: list[dict] = []
    for row in rows:
        trade_id, agent_id, structure, symbol, expiration, legs_json, ent_cd = row
        try:
            legs = json.loads(legs_json)
        except (json.JSONDecodeError, TypeError):
            console.log(f"[red]check_option_exits short-premium: bad legs_json trade {trade_id}")
            continue

        # Skip if at/past expiry — expire_options owns post-expiry path.
        try:
            exp_d = datetime.strptime(expiration[:10], "%Y-%m-%d").date()
            if exp_d <= today:
                continue
            dte = (exp_d - today).days
        except (ValueError, TypeError):
            continue

        short_puts = [l for l in legs if l.get("side") == "short" and l.get("type") == "put"]
        if not short_puts:
            continue
        leg = short_puts[0]
        entry_premium = float(leg.get("entry_price", 0) or 0)
        strike = float(leg.get("strike", 0) or 0)
        qty = int(leg.get("qty", 0) or 0)
        if entry_premium <= 0 or strike <= 0 or qty <= 0:
            continue

        stock_price = None
        if prices and symbol in prices:
            stock_price = float((prices.get(symbol) or {}).get("price", 0) or 0)
            if stock_price <= 0:
                stock_price = None

        current_premium = _csp_current_premium(
            symbol, expiration, strike, entry_premium, stock_price,
        )
        if current_premium is None:
            # No reliable premium today — skip (don't false-fire TP/SL on a
            # zero/missing quote).
            continue

        reason = None
        exit_tag = None
        if current_premium <= entry_premium * 0.50:
            reason = (
                f"AUTO-TP: 50%+ premium decay on {symbol} csp "
                f"(${entry_premium:.2f}→${current_premium:.2f})"
            )
            exit_tag = "tp_premium_decay_50pct"
        elif current_premium >= entry_premium * 2.0:
            reason = (
                f"AUTO-SL: premium 2x expanded on {symbol} csp "
                f"(${entry_premium:.2f}→${current_premium:.2f})"
            )
            exit_tag = "sl_premium_expansion_2x"
        elif 0 < dte <= 21:
            reason = f"TIME-STOP: {symbol} csp at {dte} DTE (Sosnoff 21-DTE roll)"
            exit_tag = "time_stop_21dte"

        if reason is None:
            continue

        # SHORT CSP close = buy-to-close at current premium. Sign convention
        # of close_options_trade: close_cost = exit_price × qty × 100;
        # pnl = entry_credit_debit - close_cost. For buy-to-close (debit),
        # close_cost is positive → exit_price = +current_premium.
        exit_legs = [dict(leg, exit_price=float(current_premium))]
        try:
            pnl = close_options_trade(trade_id, exit_legs, exit_reason=exit_tag)
        except Exception as e:
            console.log(f"[red]check_option_exits short-premium: close failed trade {trade_id}: {type(e).__name__}: {e!r}")
            continue
        if pnl is None:
            console.log(
                f"[red]check_option_exits short-premium: close_options_trade None "
                f"trade {trade_id} ({symbol})"
            )
            continue

        console.log(
            f"[cyan]OPTION EXIT (short-premium): {agent_id} {symbol} csp — "
            f"{reason[:80]} pnl=${pnl:.2f}"
        )
        closed.append({
            "trade_id": trade_id, "agent_id": agent_id, "structure": structure,
            "symbol": symbol, "reason": reason[:80], "exit_reason": exit_tag,
            "pnl": pnl, "path": "canonical_short_premium",
        })

    return closed


# ─────────────────────────────────────────────────────────────────────────────
# Short selling (paper)
# ─────────────────────────────────────────────────────────────────────────────

_SHORT_GHOST_PHRASES = [
    "no new position", "outside my operational zone", "violates my directives",
    "outside this specified sector", "no position",
]

_LONG_ONLY_PLAYERS = {"dayblade-sulu", "deepseek-7b-grok4", "options-sosnoff"}


def short_sell(player_id: str, symbol: str, price: float, qty: float = None,
               reasoning: str = "", confidence: float = 0.0,
               sources: str = "", timeframe: str = "SHORT") -> dict | None:
    """Open a short position on a stock.

    Margin (qty × price) is deducted from cash. Position stored with negative qty.
    To close: model sends BUY or SELL on same symbol — sell() detects negative qty.

    Authorized players only (short_enabled=1). Max 15% of account per short.
    Requires defined stop above entry in reasoning.

    Gated by SHORT_ENABLED module flag. Flip to True when Counselor Troi's
    ghost-trade performance justifies live short execution.
    """
    # HM-MARKET-HOLIDAY-CALENDAR Phase B 2026-05-25 — primary gate.
    from engine.market_calendar import market_closed_reason as _mcr
    _mkt_block_reason = _mcr()
    if _mkt_block_reason is not None:
        _last_rejection[player_id] = f"[HM-MARKET-CLOSED] {_mkt_block_reason}"
        _log_gate_reject(player_id, symbol, "MARKET_CLOSED", _mkt_block_reason,
                         price=price, confidence=confidence)
        console.log(
            f"[yellow][HM-MARKET-CLOSED] {player_id} SHORT {symbol} "
            f"blocked — {_mkt_block_reason}"
        )
        return None
    if not SHORT_ENABLED:
        console.log(f"[yellow]{player_id}: SHORT {symbol} blocked — SHORT_ENABLED=False (set True in paper_trader.py to unlock)")
        return None
    if _is_human_player(player_id):
        console.log(f"[red]BLOCKED: {player_id} is human — cannot short")
        return None
    route = _resolve_execution_portfolio(player_id)
    if route["route_mode"] == "tracking":
        return _log_signal_only(player_id, "SHORT", symbol, route, reasoning, confidence)

    if player_id in _LONG_ONLY_PLAYERS:
        console.log(f"[red]BLOCKED: {player_id} is long-only — no short selling")
        _last_rejection[player_id] = "Long-only player — shorting not permitted"
        return None

    # Ghost promotion blocker
    _rl = reasoning.lower()
    for _ph in _SHORT_GHOST_PHRASES:
        if _ph in _rl:
            _last_rejection[player_id] = f"Ghost promotion: '{_ph}'"
            return None

    import re as _re_s
    if _re_s.search(r'\bHOLD\b', reasoning):
        _last_rejection[player_id] = "Ghost promotion: HOLD in reasoning"
        return None

    conn = _conn()
    row = conn.execute(
        "SELECT short_enabled, cash FROM ai_players WHERE id=?", (player_id,)
    ).fetchone()
    conn.close()
    if not row or not row[0]:
        console.log(f"[red]BLOCKED: {player_id} not authorized for short selling")
        _last_rejection[player_id] = "Short selling not enabled for this player"
        return None

    cash = row[1]

    # Require bearish thesis + stop defined
    if "stop" not in reasoning.lower():
        console.log(f"[red]SHORT BLOCKED: {player_id} {symbol} — stop loss required in reasoning")
        _last_rejection[player_id] = "Short requires stop loss in reasoning"
        return None

    # Check drawdown pause (20% from peak, season-scoped) — HM-DRAWDOWN-GATE-SYNC 2026-05-26
    pf = get_portfolio(player_id)
    pos_value = sum(p["qty"] * p["avg_price"] for p in pf["positions"])
    current_value = cash + pos_value
    try:
        _c = _conn()
        _season_row = _c.execute(
            "SELECT value FROM settings WHERE key='current_season'"
        ).fetchone()
        _season = int(_season_row[0]) if _season_row else 1
        peak_row = _c.execute(
            "SELECT MAX(total_value) FROM portfolio_history WHERE player_id=? AND season=?",
            (player_id, _season),
        ).fetchone()
        _c.close()
        peak = peak_row[0] if peak_row and peak_row[0] else None
        if peak and peak > 0 and (peak - current_value) / peak >= 0.20:
            console.log(f"[yellow]DRAWDOWN PAUSE: {player_id} at {((peak-current_value)/peak*100):.1f}% drawdown — no new shorts")
            _last_rejection[player_id] = "Drawdown pause: portfolio down 20%+ from peak"
            return None
    except Exception:
        pass

    # Size: Kelly-based, max 15% (tier-scaled per HM-KELLY-TIER-MULTIPLIER 2026-05-23).
    kelly_pct = get_kelly_fraction(player_id)
    _km = get_kelly_cap_multiplier(player_id)
    max_short_pct = min(kelly_pct, 0.15 * _km)
    if _km > 1.0:
        console.log(
            f"[cyan][KELLY-TIER] {player_id} short cap "
            f"15.0%→{0.15 * _km:.1%} (Sharpe-tier {_km:.1f}×)"
        )
    if qty is None:
        qty = round((cash * max_short_pct) / price, 4)
    else:
        max_qty = round((cash * 0.15) / price, 4)
        qty = min(qty, max_qty)

    # READY ROOM ADVISORY (Counselor Troi): Gate on market condition before short execution
    _short_adv_mult = 1.0
    _SHORT_ADVISOR_EXEMPT = {"capitol-trades", "webull", "dalio-metals"}
    if player_id not in _SHORT_ADVISOR_EXEMPT:
        try:
            from engine.ready_room_advisor import should_i_trade as _short_advisory
            _sadv = _short_advisory(symbol=symbol, proposed_action="SHORT", player_id=player_id)
            _sadv_signal = _sadv.get("signal", "GO")
            _short_adv_mult = _sadv.get("position_size_multiplier", 1.0)
            if _sadv_signal == "STAND_DOWN":
                if player_id in _TROI_STAND_DOWN_EXEMPT:
                    console.log(
                        f"[yellow]COUNSELOR TROI: STAND_DOWN override — {player_id} {symbol} "
                        f"short exempt from CHOP gate, proceeding."
                    )
                else:
                    console.log(
                        f"[bold red]COUNSELOR TROI: STAND_DOWN — {player_id} {symbol} short "
                        f"blocked. {_sadv.get('reason', 'RED condition')}"
                    )
                    _last_rejection[player_id] = f"Ready Room STAND_DOWN: {_sadv.get('reason', 'RED condition')}"
                    return None
            elif _sadv_signal == "CAUTION":
                console.log(
                    f"[yellow]COUNSELOR TROI: CAUTION — {player_id} {symbol} short "
                    f"(×{_short_adv_mult:.2f}). {_sadv.get('reason', 'YELLOW condition')}"
                )
        except Exception:
            _short_adv_mult = 1.0

    # Apply advisory multiplier to short qty
    if _short_adv_mult < 1.0 and qty:
        qty = round(qty * _short_adv_mult, 4)

    margin = round(qty * price, 2)
    if qty <= 0 or margin > cash:
        console.log(f"[red]{player_id}: Insufficient margin for short {symbol}")
        _last_rejection[player_id] = f"Insufficient cash for short margin (need ${margin:.0f})"
        return None

    conn = _conn()
    # Don't short if already long the same stock
    ex = conn.execute(
        "SELECT qty FROM positions WHERE player_id=? AND symbol=? AND asset_type='stock'",
        (player_id, symbol)
    ).fetchone()
    if ex and ex[0] > 0:
        conn.close()
        console.log(f"[yellow]{player_id}: Already long {symbol} — refusing to add short")
        _last_rejection[player_id] = f"Already long {symbol} — cannot short simultaneously"
        return None

    conn.execute("UPDATE ai_players SET cash=? WHERE id=?", (round(cash - margin, 2), player_id))
    # HM-POSITIONS-CONVICTION-DENORM 2026-05-24: same conviction-stamp policy
    # as the long-side buy() path. 0.0/negative confidence -> NULL conviction
    # so Phase 4 stop-loss falls back to flat rather than the tightest tier.
    _conv_short = float(confidence) if confidence and confidence > 0 else None
    conn.execute(
        "INSERT INTO positions(player_id, symbol, qty, avg_price, asset_type, "
        "conviction, conviction_source) VALUES(?,?,?,?,?,?,?)",
        (player_id, symbol, -qty, price, "stock", _conv_short, "live_buy")  # negative qty = short
    )
    _short_cur = conn.execute(
        "INSERT INTO trades(player_id, symbol, action, qty, price, asset_type, "
        "reasoning, confidence, season, sources, timeframe) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        (player_id, symbol, "SHORT", qty, price, "stock",
         reasoning, confidence, _current_season(), sources, timeframe)
    )
    _short_trade_id = _short_cur.lastrowid
    conn.commit()
    conn.close()
    console.log(f"[bold red]{player_id}: SHORT {qty} {symbol} @ ${price:.2f} (margin ${margin:.0f})")
    # HM-EVENTS-BUS-FOUNDATION 2026-05-22: SHORT trade-fire event
    _emit_trade_to_bus(
        player_id=player_id, symbol=symbol, action="SHORT",
        qty=qty, price=price, asset_type="stock",
        signal_id=None, trade_id=_short_trade_id, reasoning=reasoning,
    )
    _first_trade_notification(player_id, symbol, "SHORT", price)

    # Forward to Alpaca paper account (SELL order opens short when no long position held)
    _exec_type = "simulated"
    if route["route_mode"] == "trading":
        try:
            _alp = _get_alpaca()
            if _alp:
                _ap_res = _alp.short_sell(symbol, qty, agent_id=player_id)
                if _ap_res and not _ap_res.get("error"):
                    _order_id = _ap_res.get("order_id", "")
                    _exec_type = "alpaca_paper"
                    _update_trade_alpaca_fields(player_id, symbol, _order_id, _exec_type)
                    console.log(f"[cyan]Alpaca SHORT {qty} {symbol} → order {_order_id}")
                else:
                    console.log(f"[yellow]Alpaca short forward failed: {(_ap_res or {}).get('error')}")
        except Exception as _ae:
            # HM-U: NTFY first occurrence per error class per day (short forward).
            console.log(f"[yellow]Alpaca short forward error ({player_id} {symbol}): {type(_ae).__name__}: {_ae!r}")
            try:
                from engine.alert_channels import send_alert, AlertLevel
                send_alert(
                    message=f"short forward ({player_id} {symbol}) {type(_ae).__name__}: {_ae!r}",
                    level=AlertLevel.WARNING,
                    alert_type=f"hm-u-short_forward-{type(_ae).__name__}",
                    rate_limit_secs=86400,
                )
            except Exception:
                pass

    return {
        "action": "SHORT",
        "symbol": symbol,
        "qty": qty,
        "price": price,
        "player_id": player_id,
        "portfolio_name": route["portfolio_name"],
        "execution_mode": route["execution_mode"],
        "portfolio_type": route["type"],
        "route_mode": route["route_mode"],
        "execution_status": "EXECUTED" if route["route_mode"] == "trading" else "SIMULATED",
    }
