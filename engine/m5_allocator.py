"""M-5 Multitronic — rules-based regime allocator (the fleet's control arm).

HM-M5-BASELINE-ALLOCATOR 2026-07-10.

Purpose (JPM control-arm pattern, Salopek et al. note 2026-07-09): JPMorgan
benchmarked its AI allocation agents against BOTH a static 60/40 portfolio AND
their own rules-based regime model. OllieTrades has the static leg in
engine/benchmark.py (60/40 blend) but no *tradeable* rules-based regime seat.
M-5 is that seat: a zero-LLM, fully deterministic allocator that shifts a
SPY/AGG two-ETF book by market regime. The LLM fleet must beat M-5 — not just
SPY — to justify its complexity.

Design:
  * Reads the canonical regime from regime_history via
    engine.regime_router.get_current_regime() (same source the fleet's own
    regime gate uses — apples to apples).
  * Maps regime → target SPY weight (remainder AGG). Unknown/missing regime
    → classic 60/40 (the static-benchmark posture; degrade, don't guess).
  * Rebalances at most once per trading day, only when drift from target
    exceeds DRIFT_BAND (5pp) — trend-follower turnover, not day-trader churn.
  * Trades via paper_trader.buy()/sell_partial() so M-5 flows through the
    exact same gates, guardrails, and audit trail as every other seat. This
    is deliberate: M-5 measures the allocation POLICY inside the fleet's
    environment. (The pure ungated 60/40 curve remains benchmark.py's job.)
  * Free Models First: no model, no inference, no RAM. provider='rule-based'.

Known gate interaction (documented, accepted): in BEAR/BEAR_CROSS regimes the
regime router's avoid-list blocks long_equity buys, so M-5 cannot rebalance
equity UP inside a BEAR regime (selling down is unaffected). Since M-5's bear
target is the minimum equity weight anyway (20%), the practical impact is nil
until a BEAR→recovery transition, which the next regime flip unblocks.

Error posture (docs/DOCTRINE.md): no silent catch — every except logs loudly
with type+repr; bounded DB timeouts; degrade, don't crash.

Activation: ships DORMANT (is_paused=1). Admiral activates with:
    UPDATE ai_players SET is_paused=0 WHERE id='m5-allocator';
Scheduler wiring in main.py is a no-op while paused.
"""
from __future__ import annotations

import logging
import os
import sqlite3

from rich.console import Console

console = Console()
logger = logging.getLogger("m5_allocator")

PLAYER_ID = "m5-allocator"
DISPLAY_NAME = "M-5 Multitronic"
GENESIS_CAPITAL = 10_000.0

EQUITY_ETF = "SPY"
BOND_ETF = "AGG"

# Regime → target SPY weight (remainder = AGG). Keys are the regime_history
# taxonomy (the same labels engine/regime_router.py's matrix uses).
M5_TARGETS: dict[str, float] = {
    "BULL": 0.80,
    "BULL_CROSS": 0.80,
    "BULL_LOW_VOL": 0.80,
    "CAUTIOUS_BULL": 0.60,
    "EUPHORIC": 0.40,        # matches the router's EUPHORIC long_equity cap
    "CAUTIOUS_BEAR": 0.40,
    "BEAR": 0.20,
    "BEAR_CROSS": 0.20,
    "BEAR_CHOPPY": 0.20,
}
DEFAULT_TARGET = 0.60        # unknown regime → classic 60/40, logged loudly
DRIFT_BAND = 0.05            # rebalance only when |actual − target| > 5pp
MIN_TRADE_NOTIONAL = 50.0    # skip dust trades

_done_today = False


def _db_path() -> str:
    return os.environ.get(
        "TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db")
    )


def register_player(conn: sqlite3.Connection) -> dict:
    """Idempotent seat registration (Ollie Machine P2a pattern). Ships DORMANT."""
    existed = conn.execute(
        "SELECT 1 FROM ai_players WHERE id=?", (PLAYER_ID,)
    ).fetchone() is not None
    if not existed:
        conn.execute(
            """
            INSERT INTO ai_players
              (id, display_name, provider, model_id, cash, is_active,
               can_trade_live, is_paused, season, halt_mode, halt_reason,
               role, crew_role, timeframe)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                PLAYER_ID, DISPLAY_NAME, "rule-based", "regime-rules-v1",
                GENESIS_CAPITAL,
                1,   # is_active
                0,   # can_trade_live — paper only, always (RULE #1 adjacent)
                1,   # is_paused — ships dormant; Admiral flips to activate
                6, "active",
                "[2026-07-10] HM-M5-BASELINE-ALLOCATOR — rules-based regime "
                "allocator control seat (JPM control-arm pattern). Dormant "
                "until Admiral activation (is_paused=0).",
                "production", "baseline", "position",
            ),
        )
        conn.commit()
    return {"player_id": PLAYER_ID, "created": not existed}


def register_seat() -> dict:
    """Self-contained registration on the canonical DB (main.py wiring entry)."""
    conn = sqlite3.connect(_db_path(), timeout=10)
    try:
        return register_player(conn)
    finally:
        conn.close()


def _seat_is_live() -> bool:
    """True only if the seat exists, halt_mode='active', and not paused."""
    try:
        conn = sqlite3.connect(_db_path(), timeout=5)
        try:
            row = conn.execute(
                "SELECT halt_mode, is_paused FROM ai_players WHERE id=?",
                (PLAYER_ID,),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            return False
        halt_mode, is_paused = row[0], row[1]
        return halt_mode == "active" and not is_paused
    except Exception as e:
        # Fail CLOSED for a trading loop: if we can't read seat state, don't trade.
        logger.error(
            "[m5-allocator] seat-state read failed — failing closed: "
            f"{type(e).__name__}: {e!r}"
        )
        return False


def _traded_today() -> bool:
    """Restart-resistant once-per-day dedup (capitol_fund pattern)."""
    from datetime import date
    try:
        conn = sqlite3.connect(_db_path(), timeout=5)
        try:
            row = conn.execute(
                "SELECT 1 FROM trades WHERE player_id=? AND date(executed_at)=? LIMIT 1",
                (PLAYER_ID, str(date.today())),
            ).fetchone()
        finally:
            conn.close()
        return row is not None
    except Exception as e:
        logger.warning(
            f"[m5-allocator] _traded_today check failed: {type(e).__name__}: {e!r} "
            "— failing closed (treat as already traded)"
        )
        return True


def current_target() -> tuple[float, str]:
    """(target SPY weight, regime label used). Unknown regime → 60/40, loud."""
    from engine.regime_router import get_current_regime
    regime = get_current_regime()
    if regime in M5_TARGETS:
        return M5_TARGETS[regime], regime
    console.log(
        f"[yellow][m5-allocator] regime {regime!r} not in M5_TARGETS — "
        f"defaulting to 60/40"
    )
    return DEFAULT_TARGET, regime or "UNKNOWN"


def run_m5_rebalance() -> None:
    """Scheduler entry point. Fires ~once/day in the 7:45–8:30 AZ window."""
    global _done_today
    from engine.risk_manager import RiskManager
    from engine.market_calendar import az_now

    if not RiskManager.is_market_hours():
        return

    now = az_now()
    if now.hour == 0:
        _done_today = False
        return
    if now.weekday() >= 5:
        return
    if not (7 <= now.hour <= 8):
        return
    if now.hour == 7 and now.minute < 45:
        return
    if _done_today:
        return
    if not _seat_is_live():
        return
    if _traded_today():
        _done_today = True
        return

    _done_today = True
    try:
        _execute_rebalance()
    except Exception as e:
        logger.error(f"[m5-allocator] rebalance error: {type(e).__name__}: {e!r}")
        console.log(f"[red][m5-allocator] rebalance error: {type(e).__name__}: {e!r}")


def _execute_rebalance() -> None:
    """Core rebalance — separated for direct testing."""
    from engine.market_data import get_stock_price
    from engine.paper_trader import buy, get_portfolio, sell_partial

    target_w, regime = current_target()

    portfolio = get_portfolio(PLAYER_ID)
    cash = float(portfolio.get("cash") or 0.0)
    positions = {p["symbol"]: p for p in portfolio.get("positions", [])}

    prices: dict[str, float] = {}
    for sym in (EQUITY_ETF, BOND_ETF):
        pd = get_stock_price(sym)
        px = float(pd.get("price") or 0.0) if pd else 0.0
        if px <= 0:
            console.log(
                f"[red][m5-allocator] no live price for {sym} — aborting this "
                f"rebalance (degrade, don't guess)"
            )
            return
        prices[sym] = px

    def _mv(sym: str) -> float:
        pos = positions.get(sym)
        return float(pos["qty"]) * prices[sym] if pos else 0.0

    equity_mv = _mv(EQUITY_ETF)
    bond_mv = _mv(BOND_ETF)
    total = cash + equity_mv + bond_mv
    if total <= 0:
        console.log("[red][m5-allocator] non-positive book value — aborting")
        return

    actual_w = equity_mv / total
    drift = actual_w - target_w
    console.log(
        f"[m5-allocator] regime={regime} target={target_w:.0%} "
        f"actual={actual_w:.1%} drift={drift:+.1%} "
        f"(book ${total:,.0f}: SPY ${equity_mv:,.0f} / AGG ${bond_mv:,.0f} / "
        f"cash ${cash:,.0f})"
    )

    if abs(drift) <= DRIFT_BAND:
        return  # inside the band — no trade today

    reasoning_base = (
        f"M-5 rules allocator: regime={regime}, target {target_w:.0%} SPY / "
        f"{1 - target_w:.0%} AGG, actual {actual_w:.1%} SPY, drift {drift:+.1%} "
        f"> {DRIFT_BAND:.0%} band. Deterministic rebalance — no model, no "
        f"discretion. Control arm for the LLM fleet (JPM control-arm pattern)."
    )

    if drift > 0:
        # Overweight equity → sell SPY down to target, then top up AGG.
        sell_notional = drift * total
        sell_qty = round(sell_notional / prices[EQUITY_ETF], 4)
        held_qty = float(positions[EQUITY_ETF]["qty"]) if EQUITY_ETF in positions else 0.0
        sell_qty = min(sell_qty, held_qty)
        if sell_qty * prices[EQUITY_ETF] >= MIN_TRADE_NOTIONAL:
            res = sell_partial(
                player_id=PLAYER_ID, symbol=EQUITY_ETF,
                price=prices[EQUITY_ETF], qty=sell_qty,
                reasoning=reasoning_base, confidence=0.99,
            )
            if res is None:
                console.log(
                    f"[yellow][m5-allocator] SPY trim blocked by guardrail — "
                    f"skipping AGG top-up this cycle"
                )
                return
    else:
        # Underweight equity → buy SPY up to target with available cash.
        buy_notional = min(-drift * total, cash)
        buy_qty = round(buy_notional / prices[EQUITY_ETF], 4)
        if buy_qty * prices[EQUITY_ETF] >= MIN_TRADE_NOTIONAL:
            res = buy(
                player_id=PLAYER_ID, symbol=EQUITY_ETF,
                price=prices[EQUITY_ETF], qty=buy_qty,
                reasoning=reasoning_base, confidence=0.99,
                sources="m5-allocator,regime-rules",
                timeframe="POSITION",
            )
            if res is None:
                # Expected in BEAR regimes (router avoid-list) — documented above.
                console.log(
                    f"[yellow][m5-allocator] SPY buy blocked by fleet gate "
                    f"(expected in BEAR regimes) — will retry next cycle"
                )
                return

    # Second leg: park the remainder in AGG (whatever cash is now free beyond
    # a 1% float). Re-read the portfolio so leg 1's fill is reflected.
    portfolio = get_portfolio(PLAYER_ID)
    cash = float(portfolio.get("cash") or 0.0)
    spare = cash - 0.01 * total
    agg_qty = round(spare / prices[BOND_ETF], 4)
    if agg_qty * prices[BOND_ETF] >= MIN_TRADE_NOTIONAL:
        res = buy(
            player_id=PLAYER_ID, symbol=BOND_ETF,
            price=prices[BOND_ETF], qty=agg_qty,
            reasoning=reasoning_base + " (bond leg)", confidence=0.99,
            sources="m5-allocator,regime-rules",
            timeframe="POSITION",
        )
        if res is None:
            console.log(
                "[yellow][m5-allocator] AGG leg blocked by fleet gate — cash "
                "stays parked until next cycle"
            )
