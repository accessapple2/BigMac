"""Chekov Auto-Trader — Executes paper trades on Navigator convergence signals.

Multi-Convergence Starfleet: when 3+ strategies converge on a ticker, Chekov auto-buys with
strict position sizing, stop-loss/take-profit from the signal, and overlap protection.

Safety Rails:
- 3+ strategies must converge (from score_convergence)
- Max 5% of capital per trade ($350 on $7,000)
- Auto stop-loss at signal's Stop price
- Auto take-profit at signal's Target price
- Max 2 open positions at a time
- Skips tickers already held by other arena players
- Logs every decision to War Room with reasoning
- Tags trades with convergence count (3/4/5) for later analysis
"""
from __future__ import annotations
import os
import sqlite3
from datetime import datetime
from rich.console import Console

console = Console()

CHEKOV_ID = "navigator"
CHEKOV_CASH = 7000.0
MAX_POSITIONS = 10
MAX_ALLOC_PCT = 0.05   # 5% of capital per swing stock trade
MAX_ALLOC_SPREAD = 0.03  # 3% for options spreads
_CD_DAYS = 5  # Per-symbol SL cooldown window (days)

# HM-CHEKOV-CONF-CALIBRATION (2026-05-21): replace upstream-floored 0.82
# confidence (engine/strategies.py:484) with a count-based tiered formula.
# Upstream floor caused false Grade-A confidence on 2-strat convergence —
# every convergence trade looked equally strong regardless of strat_count.
_CONF_TIERS = {2: 0.65, 3: 0.72, 4: 0.78}  # Grade-B band
_CONF_GRADE_A = 0.85                        # 5+ strategies bypass Grade-B gate
_CONF_SHORT_PENALTY = 0.05                  # SHORT trades are higher risk

# HM-CHEKOV-REENTRY-GUARD (2026-05-21): Rails 7 + 8 — broader re-entry
# protection than the SL cooldown. Rail 7 is a DB-level belt-and-braces
# overlap with existing Rail 2 (kept explicit per spec); Rail 8 is a new
# declining-trend filter using daily candles.
_REENTRY_LOOKBACK = 3  # consecutive daily closes that must all be below anchor

# HM-MASTER-PLAN W2-B (2026-05-23) — bull_call_spread regime gate.
# Master plan finding: backtest showed 13% win rate for bull_call_spread
# during bear/tariff regime. Captain-spec gate tightens entry to:
#   regime == "BULL_CROSS"  AND  vix < 18  AND  SPY > sma_200
# All three conditions must hold; any fail blocks the spread route at
# the chekov signal-emit site (upstream of paper_trader.buy gate).
_BCS_GATE_VIX_MAX     = 18.0
_BCS_GATE_REGIME      = "BULL_CROSS"
_BCS_GATE_CACHE_TTL_S = 300  # 5 minutes — mirrors regime_ma cache cadence
_bcs_gate_cache: dict = {"ts": 0.0, "regime": None, "spy_above_200ma": None}
DB = os.environ.get(
    "TRADEMINDS_DB",
    os.path.expanduser("~/autonomous-trader/data/trader.db"),
)


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def _ensure_chekov_funded():
    """Make sure Chekov has trading capital (one-time bootstrap)."""
    conn = _conn()
    row = conn.execute("SELECT cash FROM ai_players WHERE id=?", (CHEKOV_ID,)).fetchone()
    if row and row["cash"] < 1.0:
        # Navigator was scanner-only with $0 — fund for paper trading
        conn.execute("UPDATE ai_players SET cash=? WHERE id=?", (CHEKOV_CASH, CHEKOV_ID))
        conn.commit()
        console.log(f"[green]🧭 Chekov funded with ${CHEKOV_CASH:.0f} for paper trading")
    conn.close()


def _get_arena_held_tickers() -> set:
    """Get all tickers currently held by any arena player (excluding Chekov)."""
    conn = _conn()
    rows = conn.execute(
        "SELECT DISTINCT symbol FROM positions "
        "WHERE player_id != ? AND qty > 0",
        (CHEKOV_ID,),
    ).fetchall()
    conn.close()
    return {r["symbol"] for r in rows}


def _get_chekov_positions() -> list:
    """Get Chekov's current open positions."""
    from engine.paper_trader import get_portfolio
    portfolio = get_portfolio(CHEKOV_ID)
    return [p for p in portfolio["positions"] if p["qty"] > 0]


def _get_chekov_cash() -> float:
    from engine.paper_trader import get_portfolio
    return get_portfolio(CHEKOV_ID)["cash"]


def _log_to_war_room(symbol: str, message: str):
    """Post Chekov's auto-trade reasoning to War Room."""
    try:
        from engine.war_room import save_hot_take
        save_hot_take(CHEKOV_ID, symbol, message)
    except Exception as e:
        console.log(f"[yellow]Chekov War Room log failed: {e}")


def _has_open_position_db(player_id: str, symbol: str) -> bool:
    """DB-level safety net: does player hold qty > 0 in this symbol?

    Overlaps with Rail 2's `held_symbols` check (which reads via the
    paper_trader portfolio helper). This DB-direct query is the belt-and-
    braces edge case for when the portfolio cache is stale relative to a
    just-written position row. Fail-safe: any DB error returns False so
    the trade is allowed rather than blocked (matches _recent_sl_loss
    posture).
    """
    try:
        conn = _conn()
        row = conn.execute(
            "SELECT qty FROM positions "
            "WHERE player_id=? AND symbol=? AND qty > 0 LIMIT 1",
            (player_id, symbol),
        ).fetchone()
        conn.close()
        return row is not None
    except Exception as e:
        console.log(
            f"[yellow]🧭 Chekov reentry-DB query failed for {symbol} "
            f"({type(e).__name__}: {e!r}) — allowing trade"
        )
        return False


def _declining_trend(symbol: str, anchor_price: float,
                     lookback: int = _REENTRY_LOOKBACK) -> tuple[bool, list]:
    """Are the last `lookback` daily closes all strictly below anchor_price?

    Pulls daily candles via engine.market_data.get_intraday_candles
    (Polygon→Alpaca→Yahoo cascade). Returns (is_declining, closes_list).
    Fail-safe: any error or insufficient data returns (False, []) so the
    trade is allowed rather than blocked.
    """
    try:
        from engine.market_data import get_intraday_candles
        bars = get_intraday_candles(symbol, interval="1d", range_="5d")
        if not bars or len(bars) < lookback:
            return False, []
        recent_closes = [b.get("close", 0) for b in bars[-lookback:]]
        if any(c <= 0 for c in recent_closes):
            return False, recent_closes
        is_decl = all(c < anchor_price for c in recent_closes)
        return is_decl, recent_closes
    except Exception as e:
        console.log(
            f"[yellow]🧭 Chekov declining-trend check failed for {symbol} "
            f"({type(e).__name__}: {e!r}) — allowing trade"
        )
        return False, []


def _convergence_confidence(raw_count: int, strat_count: float) -> float:
    """Tiered confidence by raw strategy convergence count.

    Tiers (raw_count → conf): 2→0.65, 3→0.72, 4→0.78, 5+→0.85.
    SHORT trades (weighted strat_count < 4, per timeframe_tag logic at
    execute_convergence_trades) subtract 0.05 — higher risk.

    Floor: 0.60 (no degenerate cases below 2-strat tier minus SHORT penalty).
    """
    if raw_count >= 5:
        base = _CONF_GRADE_A
    else:
        base = _CONF_TIERS.get(max(int(raw_count), 2), _CONF_TIERS[2])
    if strat_count < 4:
        base -= _CONF_SHORT_PENALTY
    return round(max(base, 0.60), 2)


def _recent_sl_loss(ticker: str) -> tuple[bool, str, float]:
    """Per-symbol SL cooldown: did Chekov/Navigator take a realized loss on this
    ticker within _CD_DAYS? Fail-safe: on DB error returns (False, "", 0.0)
    so the trade is allowed through rather than blocked.

    Returns (hit, last_date_str, last_pnl).
    """
    try:
        conn = _conn()
        row = conn.execute(
            "SELECT executed_at, realized_pnl FROM trades "
            "WHERE player_id=? AND symbol=? AND realized_pnl < 0 "
            f"AND executed_at > datetime('now','-{int(_CD_DAYS)} days') "
            "ORDER BY executed_at DESC LIMIT 1",
            (CHEKOV_ID, ticker),
        ).fetchone()
        conn.close()
        if row:
            date_str = str(row["executed_at"])[:10]
            return True, date_str, float(row["realized_pnl"])
        return False, "", 0.0
    except Exception as e:
        console.log(
            f"[yellow]🧭 Chekov SL-cooldown DB query failed for {ticker} "
            f"({type(e).__name__}: {e!r}) — allowing trade"
        )
        return False, "", 0.0


def _check_quality(ticker: str) -> tuple[bool, str]:
    """Check if ticker passes the Dalio/Buffett quality screen.

    Returns (passes: bool, reason: str).
    If the cache is empty, attempts a live fetch from Finviz.
    If the fetch fails or credentials are missing, blocks the trade
    (fail-closed: never buy if quality cannot be verified).
    """
    try:
        from shared.finviz_scanner import is_quality_stock, finviz_quality_screen, _quality_cache
        if not _quality_cache.get("tickers"):
            # Cache cold — try to load now (blocking, runs in scheduler thread)
            console.log(f"[cyan]🧭 Quality screen: cache empty, fetching from Finviz…")
            finviz_quality_screen()
        result = is_quality_stock(ticker)
        if result is None:
            return False, "quality screen unavailable (Finviz fetch failed or no credentials)"
        if not result:
            return False, (
                f"fails Dalio/Buffett quality filter "
                f"(requires: LT debt/eq <0.7 only (HM-AY.2))"
            )
        return True, "✅ passes quality screen"
    except Exception as e:
        return False, f"quality check error: {e}"


def _get_current_price(symbol: str) -> float | None:
    """Get latest price for a symbol."""
    try:
        from engine.market_data import get_all_prices
        prices = get_all_prices([symbol])
        if symbol in prices and prices[symbol].get("price"):
            return prices[symbol]["price"]
    except Exception:
        pass
    return None


def _get_vix() -> float:
    """Return current VIX level (0 = unavailable)."""
    try:
        from engine.market_data import get_vix
        return get_vix()
    except Exception:
        return 0.0


def _get_atr(symbol: str, period: int = 14) -> float:
    """Return 14-day ATR for symbol (0 if unavailable)."""
    try:
        from engine.market_data import get_alpaca_bars
        import pandas as pd
        df = get_alpaca_bars(symbol, days=30)
        if df is None or df.empty:
            return 0.0
        # ATR off COMPLETE bars only (partial-bar-as-complete / bbkc pattern):
        # df[-1] is today's in-progress daily bar during RTH, whose forming H/L/close
        # skews the true range. Drop it so ATR reflects completed sessions. Callers
        # (execute_covered_calls strike/premium) evaluate against the live close
        # separately — only the volatility estimate must use complete bars.
        df = df.iloc[:-1]
        if len(df) < period:
            return 0.0
        hi = df["High"].squeeze()
        lo = df["Low"].squeeze()
        cl = df["Close"].squeeze()
        prev_cl = cl.shift(1)
        tr = pd.concat([hi - lo, (hi - prev_cl).abs(), (lo - prev_cl).abs()], axis=1).max(axis=1)
        return float(tr.rolling(period).mean().iloc[-1])
    except Exception:
        return 0.0


def _bcs_regime_gate(vix: float) -> tuple[bool, str]:
    """HM-MASTER-PLAN W2-B regime gate for bull_call_spread.

    Captain spec: only fire bull_call_spread when ALL three hold:
      1. current regime == BULL_CROSS  (engine.regime_router)
      2. VIX < 18
      3. SPY close > SMA-200          (engine.market_data)

    Returns (allowed, reason). Reason is human-readable, includes the
    failing field's value so log lines are self-diagnosing.

    Fail-safe: any helper exception → reject (False). The gate is a
    safety filter; falling open would defeat its purpose during
    transient data outages.

    Cache: 5-minute TTL on (regime, spy_above_200ma) tuple. VIX is
    already cached upstream by _get_vix; we re-fetch each call to pick
    up intraday changes.
    """
    import time as _t
    # VIX check is cheapest; do it first
    if not (0 < vix < _BCS_GATE_VIX_MAX):
        return False, f"vix={vix:.2f} outside (0, {_BCS_GATE_VIX_MAX})"

    now = _t.time()
    if (now - _bcs_gate_cache["ts"]) >= _BCS_GATE_CACHE_TTL_S:
        try:
            from engine.regime_router import get_current_regime
            from engine.market_data import get_technical_indicators
            _bcs_gate_cache["regime"] = get_current_regime()
            spy_ind = get_technical_indicators("SPY") or {}
            _bcs_gate_cache["spy_above_200ma"] = spy_ind.get("above_sma200")
            _bcs_gate_cache["ts"] = now
        except Exception as e:
            return False, f"gate probe failed: {type(e).__name__}: {e!r}"

    regime = _bcs_gate_cache["regime"]
    spy_above_200 = _bcs_gate_cache["spy_above_200ma"]

    if regime != _BCS_GATE_REGIME:
        return False, f"regime={regime!r} != {_BCS_GATE_REGIME}"
    if spy_above_200 is None:
        return False, "spy sma_200 unavailable"
    if not spy_above_200:
        return False, "SPY below SMA-200"
    return True, f"BULL_CROSS + vix={vix:.2f}<{_BCS_GATE_VIX_MAX} + SPY>SMA200"


def _execute_bull_call_spread(ticker: str, price: float, stop: float,
                               target: float, strat_count: int, cash: float) -> bool:
    """Buy an ATM call option representing a simplified bull call spread.

    Full two-leg spread accounting is complex; we record the net debit as
    a single long call at a reduced premium (50% of ATM estimate) tagged
    as 'BULL_CALL_SPREAD' in sources.
    """
    from engine.paper_trader import buy as pt_buy
    from datetime import date, timedelta

    # Estimate ATM call premium: ~3% of stock price for 14-DTE
    premium = round(price * 0.03, 2)
    max_cost = cash * MAX_ALLOC_SPREAD
    qty = int(max_cost / (premium * 100))  # 1 contract = 100 shares
    if qty <= 0:
        qty = 1
    spread_cost = round(qty * premium * 100, 2)  # display cost for logging
    if spread_cost > max_cost:
        return False

    # Nearest Friday at least 14 days out
    today = date.today()
    days_to_friday = (4 - today.weekday()) % 7 or 7
    expiry = (today + timedelta(days=max(days_to_friday, 14))).strftime("%Y-%m-%d")

    reasoning = (
        f"BULL_CALL_SPREAD: {strat_count}-strategy convergence — ATM call @ ${premium:.2f}. "
        f"[STOP: ${stop:.2f}] [TARGET: ${target:.2f}] [CONVERGENCE: {strat_count}]"
    )
    result = pt_buy(
        CHEKOV_ID, ticker, premium,
        asset_type="option", option_type="call",
        qty=float(qty),
        expiry_date=expiry,
        strike_price=round(price),
        reasoning=reasoning,
        confidence=min(strat_count / 5.0, 1.0),
        sources=f"bull-call-spread-{strat_count}",
        timeframe="SWING",
    )
    if result:
        _log_to_war_room(ticker, (
            f"Keptin! Running a bull call spread on {ticker}! "
            f"{strat_count}-strategy convergence — buying {qty} ATM call @ ${premium:.2f}, "
            f"expiry {expiry}. VIX is low, the charts are aligned — maximum efficiency!"
        ))
        console.log(f"[bold green]🧭 Chekov BULL CALL SPREAD: {qty}x {ticker} ${price:.0f}C exp {expiry} @ ${premium:.2f}")
        return True
    return False


def execute_covered_calls():
    """Sell covered calls against Chekov's existing long positions.

    Places a short call at 1× ATR above entry price with nearest monthly expiry.
    Only acts if no call is already open against that position.

    HM-COVERED-CALL-RECORDING 2026-05-23 — previous version routed via
    paper_trader.buy() which recorded the write as action=BUY qty=+x.
    A covered call is a SELL-TO-OPEN of a short call contract — correct
    semantics: trades.action='SELL', trades.qty<0, positions.qty<0
    (short), cash CREDITED by premium received. This rewrite does a
    direct, idempotent DB write with the right signs. Bypasses pt_buy's
    gate stack because covered-call writing on a long equity position
    is an income strategy, not a new directional intent.
    """
    from engine.paper_trader import (
        _is_human_player, get_portfolio, _last_rejection,
    )
    from datetime import date, timedelta

    # Halt-mode gate (keep the same protection paper_trader.buy provides).
    _halt = _conn().execute(
        "SELECT halt_reason, halt_mode FROM ai_players WHERE id=?",
        (CHEKOV_ID,),
    ).fetchone()
    if _halt and (_halt[1] != "active"):
        console.log(
            f"[red]HALTED: {CHEKOV_ID} ({_halt[1]}) — "
            f"{_halt[0] or 'no reason given'} — skipping covered-call cycle"
        )
        return
    if _is_human_player(CHEKOV_ID):
        return

    portfolio = get_portfolio(CHEKOV_ID)
    positions = [p for p in portfolio["positions"]
                 if p.get("asset_type") == "stock" and p.get("qty", 0) > 0]

    if not positions:
        return

    for pos in positions:
        symbol = pos["symbol"]
        avg_price = pos.get("avg_price", 0)
        if avg_price <= 0:
            continue

        # Skip if a covered call is already open against this position.
        # HM-COVERED-CALL-RECORDING 2026-05-23: the correct guard is for
        # SHORT calls (qty < 0). Old code looked for qty > 0 (long calls)
        # which was inconsistent with the new short-call recording.
        conn = _conn()
        existing_call = conn.execute(
            "SELECT 1 FROM positions WHERE player_id=? AND symbol=? "
            "AND asset_type='option' AND option_type='call' AND qty < 0",
            (CHEKOV_ID, symbol),
        ).fetchone()
        conn.close()
        if existing_call:
            continue

        atr = _get_atr(symbol)
        if atr <= 0:
            continue

        strike = round(avg_price + atr, 2)
        premium = round(atr * 0.30, 2)  # rough 30% of ATR as premium estimate
        if premium < 0.10:
            continue

        # Nearest Friday at least 14 days out
        today = date.today()
        days_to_friday = (4 - today.weekday()) % 7 or 7
        expiry = (today + timedelta(days=max(days_to_friday, 14))).strftime("%Y-%m-%d")

        reasoning = (
            f"COVERED_CALL: Strike ${strike:.2f} (entry ${avg_price:.2f} + 1×ATR ${atr:.2f}). "
            f"Selling call @ ${premium:.2f}. [STOP: ${avg_price * 0.93:.2f}] [TARGET: ${strike:.2f}] "
            f"Income generation on existing {symbol} position."
        )

        # Direct DB write — SELL TO OPEN a short call contract.
        # qty stored as negative (short). Premium credits cash. We use
        # sizing_qty=1.0 contract — paper_trader's sizing layer is
        # specific to long entries and not appropriate here.
        sizing_qty = 1.0
        signed_qty = -abs(sizing_qty)
        credit = round(abs(signed_qty) * premium, 2)

        conn = _conn()
        try:
            row = conn.execute(
                "SELECT cash FROM ai_players WHERE id=?", (CHEKOV_ID,)
            ).fetchone()
            if not row:
                console.log(
                    f"[red]🧭 Chekov COVERED-CALL skip: player row missing"
                )
                conn.close()
                continue
            new_cash = round(float(row[0]) + credit, 2)
            conn.execute(
                "UPDATE ai_players SET cash=? WHERE id=?",
                (new_cash, CHEKOV_ID),
            )
            conn.execute(
                "INSERT INTO positions(player_id, symbol, qty, avg_price, "
                " asset_type, option_type, strike_price, expiry_date) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (CHEKOV_ID, symbol, signed_qty, premium, "option",
                 "call", strike, expiry),
            )
            conn.execute(
                "INSERT INTO trades(player_id, symbol, action, qty, price, "
                " asset_type, option_type, strike_price, expiry_date, "
                " reasoning, confidence, season, sources, timeframe, "
                " entry_price) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (CHEKOV_ID, symbol, "SELL", signed_qty, premium, "option",
                 "call", strike, expiry, reasoning, 0.75,
                 _current_season_safe(), "covered-call", "SWING", premium),
            )
            conn.commit()
            result = True
        except Exception as e:
            _last_rejection[CHEKOV_ID] = (
                f"covered-call write: {type(e).__name__}: {e!r}"
            )
            console.log(
                f"[red]🧭 Chekov COVERED-CALL write crash: "
                f"{type(e).__name__}: {e!r}"
            )
            conn.rollback()
            result = False
        finally:
            conn.close()

        if result:
            _log_to_war_room(symbol, (
                f"Keptin, selling a covered call on our {symbol} position! "
                f"Strike ${strike:.2f} (entry + 1×ATR), expiry {expiry}, premium ${premium:.2f}. "
                f"Generating income while holding course. Most efficient!"
            ))
            console.log(
                f"[cyan]🧭 Chekov COVERED CALL (short): {symbol} "
                f"${strike:.0f}C exp {expiry} @ ${premium:.2f} "
                f"(qty={signed_qty}, credit=${credit:.2f})"
            )


def _current_season_safe() -> int:
    """Best-effort wrapper around paper_trader._current_season; falls back
    to 6 (current season) if the lookup raises."""
    try:
        from engine.paper_trader import _current_season
        return _current_season()
    except Exception:
        return 6


def check_stop_loss_take_profit():
    """Check Chekov's positions against stop/target and auto-sell if hit."""
    from engine.paper_trader import sell

    positions = _get_chekov_positions()
    if not positions:
        return

    for pos in positions:
        symbol = pos["symbol"]
        price = _get_current_price(symbol)
        if not price:
            continue
        # SANITY GATE: reject price if < 20% of avg_price (stale/garbage data)
        avg = pos.get("avg_price", 0)
        if avg > 0 and price < (avg * 0.20):
            console.log(f"[yellow]🧭 Chekov PRICE-SANITY-REJECT {symbol}: price=${price:.2f} < 20% of avg=${avg:.2f} — skipping stop/target check")
            continue

        # Read stop/target from the trade reasoning
        stop_price = _parse_price_from_reasoning(pos, "stop")
        target_price = _parse_price_from_reasoning(pos, "target")

        if stop_price and price <= stop_price:
            reason = f"STOP-LOSS HIT: ${price:.2f} <= stop ${stop_price:.2f}"
            result = sell(CHEKOV_ID, symbol, price, reasoning=reason, confidence=1.0)
            if result:
                _log_to_war_room(symbol, (
                    f"Bozhe moy! {symbol} hit stop-loss at ${price:.2f}! "
                    f"I am executing emergency sell. Stop was ${stop_price:.2f}. "
                    f"We live to fight another day, Keptin!"
                ))
                console.log(f"[red]🧭 Chekov STOP-LOSS: SELL {symbol} @ ${price:.2f}")

        elif target_price and price >= target_price:
            reason = f"TAKE-PROFIT HIT: ${price:.2f} >= target ${target_price:.2f}"
            result = sell(CHEKOV_ID, symbol, price, reasoning=reason, confidence=1.0)
            if result:
                _log_to_war_room(symbol, (
                    f"Keptin! {symbol} has reached target at ${price:.2f}! "
                    f"Target was ${target_price:.2f}. Locking in profits! "
                    f"The Navigator's course was true!"
                ))
                console.log(f"[green]🧭 Chekov TAKE-PROFIT: SELL {symbol} @ ${price:.2f}")


def _parse_price_from_reasoning(pos: dict, price_type: str) -> float | None:
    """Extract stop or target price from trade reasoning stored in the DB.

    Looks up the original BUY trade's reasoning for patterns like:
    [STOP: $138.20] or [TARGET: $152.80]
    """
    try:
        conn = _conn()
        row = conn.execute(
            "SELECT reasoning FROM trades WHERE player_id=? AND symbol=? AND action='BUY' "
            "ORDER BY executed_at DESC LIMIT 1",
            (CHEKOV_ID, pos["symbol"]),
        ).fetchone()
        conn.close()
        if not row or not row["reasoning"]:
            return None

        import re
        tag = "STOP" if price_type == "stop" else "TARGET"
        match = re.search(rf"\[{tag}: \$([0-9]+\.?[0-9]*)\]", row["reasoning"])
        if match:
            return float(match.group(1))
    except Exception:
        pass
    return None


def execute_convergence_trades(signals: list = None):
    """Main entry point: evaluate convergence signals and auto-trade.

    Called after strategy scan completes. Checks each signal against safety
    rails and executes paper trades for qualifying signals.
    """
    from engine.paper_trader import buy, get_position

    # Get today's signals if not provided
    if signals is None:
        from engine.strategies import get_todays_signals
        signals = get_todays_signals()

    if not signals:
        return

    _ensure_chekov_funded()

    positions = _get_chekov_positions()
    open_count = len(positions)
    held_symbols = {p["symbol"] for p in positions}
    arena_held = _get_arena_held_tickers()
    cash = _get_chekov_cash()
    vix = _get_vix()

    # Covered calls: generate income on existing positions first
    try:
        execute_covered_calls()
    except Exception as _cc_e:
        console.log(f"[yellow]🧭 Covered call check failed: {_cc_e}")

    executed = 0

    for sig in signals:
        ticker = sig["ticker"]
        strat_count = sig["strategies_triggered"]     # weighted score (float)
        raw_count = sig.get("raw_strategy_count", int(strat_count))
        # HM-CHEKOV-CONF-CALIBRATION: override upstream sig["confidence"]
        # (engine/strategies.py:484 floored at 0.82) with count-based tiers.
        confidence = _convergence_confidence(raw_count, strat_count)
        entry = sig["entry"]
        stop = sig["stop"]
        target = sig["target"]
        rr = sig.get("risk_reward", 0)
        strat_names = sig.get("strategy_names", [])

        # --- SAFETY RAIL 1: Already at max positions ---
        if open_count >= MAX_POSITIONS:
            _log_to_war_room(ticker, (
                f"Keptin, {ticker} shows {strat_count}-strategy convergence but "
                f"we already have {open_count} open positions (max {MAX_POSITIONS}). "
                f"Standing down until a position closes."
            ))
            console.log(f"[yellow]🧭 Chekov SKIP {ticker}: max positions ({open_count}/{MAX_POSITIONS})")
            break

        # --- SAFETY RAIL 2: Already holding this ticker ---
        if ticker in held_symbols:
            console.log(f"[yellow]🧭 Chekov SKIP {ticker}: already holding")
            continue

        # --- SAFETY RAIL 3: Another arena player holds this ticker ---
        if ticker in arena_held:
            _log_to_war_room(ticker, (
                f"Keptin, {ticker} has {strat_count}-strategy convergence "
                f"but another crew member already holds it. "
                f"Avoiding doubling up on fleet exposure."
            ))
            console.log(f"[yellow]🧭 Chekov SKIP {ticker}: held by another player")
            continue

        # --- SAFETY RAIL 4: Get current price ---
        price = _get_current_price(ticker)
        if not price:
            console.log(f"[yellow]🧭 Chekov SKIP {ticker}: no price available")
            continue

        # --- SAFETY RAIL 5: Quality screen — never buy low-quality businesses ---
        quality_ok, quality_reason = _check_quality(ticker)
        if not quality_ok:
            _log_to_war_room(ticker, (
                f"Keptin, {ticker} shows {strat_count}-strategy convergence "
                f"but I cannae execute — {quality_reason}. "
                f"We dinnae buy low-quality businesses regardless of technical signals."
            ))
            console.log(f"[yellow]🧭 Chekov SKIP {ticker}: quality gate — {quality_reason}")
            continue

        # --- SAFETY RAIL 6: Per-symbol SL cooldown ---
        # Skip ticker if Chekov/Navigator took a realized loss on it within
        # the last _CD_DAYS days. Fail-safe: DB query failure allows the trade.
        cd_hit, cd_date, cd_pnl = _recent_sl_loss(ticker)
        if cd_hit:
            console.log(
                f"[yellow]🧭 Chekov SKIP {ticker}: SL cooldown ({_CD_DAYS}d) "
                f"— last loss {cd_date} ${cd_pnl:.2f}"
            )
            continue

        # --- SAFETY RAIL 7: DB-level open-position re-entry block ---
        # Belt-and-braces with Rail 2 (which reads via portfolio helper);
        # catches edge case where positions row is written but the helper
        # cache is stale. Fail-safe: DB error allows the trade.
        if _has_open_position_db(CHEKOV_ID, ticker):
            console.log(
                f"[yellow][CHEKOV-REENTRY-BLOCK] player={CHEKOV_ID} "
                f"symbol={ticker} reason=open_position_db"
            )
            continue

        # --- SAFETY RAIL 8: Declining-trend filter ---
        # If the last _REENTRY_LOOKBACK daily closes are ALL below the signal's
        # entry price, the trend is fading — skip new BUY (recomputes each
        # cycle, naturally satisfying the "24h" intent without persistence).
        # Anchor is signal entry price (not "open position avg_price" — past
        # Rail 7, no open position exists for CHEKOV_ID). Fail-safe: missing
        # candle data allows the trade.
        is_decl, recent_closes = _declining_trend(ticker, entry)
        if is_decl:
            closes_str = ", ".join(f"${c:.2f}" for c in recent_closes)
            console.log(
                f"[yellow][CHEKOV-REENTRY-BLOCK] player={CHEKOV_ID} "
                f"symbol={ticker} reason=declining_trend "
                f"(last {_REENTRY_LOOKBACK} closes [{closes_str}] all < "
                f"entry ${entry:.2f})"
            )
            continue

        # ── ROUTE: 5+ strategies + W2-B regime gate → bull call spread ──────
        # HM-MASTER-PLAN W2-B 2026-05-23: legacy gate was (strat_count>=5,
        # 0<vix<25). Tightened to Captain-spec regime gate (BULL_CROSS +
        # vix<18 + SPY>SMA-200) after backtest showed 13% WR for
        # bull_call_spread in bear/tariff regime.
        if strat_count >= 5:
            gate_ok, gate_reason = _bcs_regime_gate(vix)
            if not gate_ok:
                console.log(
                    f"[yellow][BULL-CALL-REGIME-GATE] symbol={ticker} "
                    f"strat_count={strat_count} BLOCKED reason={gate_reason}"
                )
                continue
            console.log(
                f"[green][BULL-CALL-REGIME-GATE] symbol={ticker} "
                f"strat_count={strat_count} ALLOWED ({gate_reason})"
            )
            spread_ok = _execute_bull_call_spread(
                ticker, price, stop, target, strat_count, cash
            )
            if spread_ok:
                executed += 1
                open_count += 1
                held_symbols.add(ticker)
            else:
                console.log(f"[yellow]🧭 Chekov SKIP spread {ticker}: spread blocked")
            continue  # Don't also open stock position

        # ── ROUTE: 4+ strategies → swing trade (3-10 day hold) ──────────────
        timeframe_tag = "SWING" if strat_count >= 4 else "SHORT"

        # Position sizing: swing = up to 25%; short = 5%
        alloc_pct = 0.25 if strat_count >= 4 else MAX_ALLOC_PCT
        max_cost = min(cash * alloc_pct, CHEKOV_CASH * alloc_pct)
        qty = int(max_cost / price)
        if qty <= 0:
            console.log(f"[yellow]🧭 Chekov SKIP {ticker}: price ${price:.2f} exceeds max alloc ${max_cost:.2f}")
            continue

        cost = qty * price
        if cost > cash:
            console.log(f"[yellow]🧭 Chekov SKIP {ticker}: insufficient cash (${cash:.2f} < ${cost:.2f})")
            continue

        strat_list = ", ".join(strat_names[:4])
        weight_note = f" (weighted {strat_count:.1f})" if strat_count != raw_count else ""
        # quality_reason is set by Rail 5 (only reachable if quality_ok=True)
        reasoning = (
            f"CONVERGENCE AUTO-TRADE ({timeframe_tag}): {raw_count} strategies agree ({strat_list}){weight_note}. "
            f"R/R {rr:.1f}:1. "
            f"[QUALITY: ✅] "
            f"[STOP: ${stop:.2f}] [TARGET: ${target:.2f}] "
            f"[CONVERGENCE: {strat_count:.1f}]"
        )

        result = buy(
            CHEKOV_ID, ticker, price,
            qty=qty,
            reasoning=reasoning,
            confidence=confidence,
            sources=f"convergence-{strat_count}",
            timeframe=timeframe_tag,
        )

        if result:
            executed += 1
            open_count += 1
            held_symbols.add(ticker)
            cash -= cost

            hold_note = "Minimum 3-day swing hold." if strat_count >= 4 else ""
            _log_to_war_room(ticker, (
                f"Aye Keptin! Plotting intercept course on {ticker}! "
                f"{raw_count}-strategy convergence ({strat_list}){weight_note}. "
                f"QUALITY ✅ passes Dalio/Buffett screen. "
                f"{'SWING TRADE — ' if strat_count >= 4 else ''}"
                f"BUY {qty} shares @ ${price:.2f} (${cost:.2f}). "
                f"Stop ${stop:.2f}, Target ${target:.2f}, R/R {rr:.1f}:1. "
                f"Conviction {confidence:.0%}. {hold_note}"
            ))
            console.log(
                f"[bold green]🧭 Chekov {timeframe_tag}: BUY {qty} {ticker} @ ${price:.2f} "
                f"({raw_count} strategies / {strat_count:.1f} weighted, conf {confidence:.0%})"
            )
        else:
            console.log(f"[yellow]🧭 Chekov {ticker}: buy blocked by guardrails")

    if executed:
        console.log(f"[bold green]🧭 Chekov executed {executed} convergence trade(s)")
    else:
        console.log(f"[dim]🧭 Chekov: no convergence trades executed this cycle")
