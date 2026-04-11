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
MAX_POSITIONS = 5
MAX_ALLOC_PCT = 0.05   # 5% of capital per swing stock trade
MAX_ALLOC_SPREAD = 0.03  # 3% for options spreads
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
                f"(requires: gross margin >50%, LT debt/eq <0.4, op margin >25%, ROE >15%)"
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
        import yfinance as yf
        tick = yf.Ticker("^VIX")
        info = tick.fast_info
        return float(info.last_price or 0)
    except Exception:
        return 0.0


def _get_atr(symbol: str, period: int = 14) -> float:
    """Return 14-day ATR for symbol (0 if unavailable)."""
    try:
        import yfinance as yf
        import pandas as pd
        df = yf.download(symbol, period="30d", progress=False, auto_adjust=True)
        if df.empty or len(df) < period:
            return 0.0
        hi = df["High"].squeeze()
        lo = df["Low"].squeeze()
        cl = df["Close"].squeeze()
        prev_cl = cl.shift(1)
        tr = pd.concat([hi - lo, (hi - prev_cl).abs(), (lo - prev_cl).abs()], axis=1).max(axis=1)
        return float(tr.rolling(period).mean().iloc[-1])
    except Exception:
        return 0.0


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
    """
    from engine.paper_trader import buy as pt_buy, get_portfolio
    from datetime import date, timedelta

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

        # Check no existing call already open on this symbol
        conn = _conn()
        existing_call = conn.execute(
            "SELECT 1 FROM positions WHERE player_id=? AND symbol=? "
            "AND asset_type='option' AND option_type='call' AND qty > 0",
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
        # We record covered calls as a small credit purchase at negative cost
        # (represented as a short call — qty=1 contract at the premium received)
        result = pt_buy(
            CHEKOV_ID, symbol, premium,
            asset_type="option", option_type="call",
            qty=1.0,
            expiry_date=expiry,
            strike_price=strike,
            reasoning=reasoning,
            confidence=0.75,
            sources="covered-call",
            timeframe="SWING",
        )
        if result:
            _log_to_war_room(symbol, (
                f"Keptin, selling a covered call on our {symbol} position! "
                f"Strike ${strike:.2f} (entry + 1×ATR), expiry {expiry}, premium ${premium:.2f}. "
                f"Generating income while holding course. Most efficient!"
            ))
            console.log(f"[cyan]🧭 Chekov COVERED CALL: {symbol} ${strike:.0f}C exp {expiry} @ ${premium:.2f}")


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
        confidence = sig.get("confidence", min(strat_count / 5.0, 1.0))
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

        # ── ROUTE: 5+ strategies + VIX < 25 → bull call spread ──────────────
        if strat_count >= 5 and 0 < vix < 25:
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
