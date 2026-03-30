"""Time Machine — backtest a model's strategy against up to 10 years of historical data.

Supports two modes:
  - RAW: Replay all signals as-is (original behavior)
  - GUARDED: Apply risk_manager.py guardrails to filter signals before execution
"""
from __future__ import annotations
import sqlite3
from datetime import datetime, timedelta
from engine.market_data import _yahoo_chart
from rich.console import Console

console = Console()
DB = "data/trader.db"

# Starting cash for backtests (matches config)
STARTING_CASH = 7000.0


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


# ============================================================
# VIX HISTORY (for bear mode detection per backtest day)
# ============================================================

_vix_cache: dict[str, float] = {}


def _get_vix_history(days: int, start_date: str = None, end_date: str = None) -> dict[str, float]:
    """Fetch historical VIX closes. Returns {date_str: vix_close}."""
    global _vix_cache
    if _vix_cache:
        return _vix_cache

    try:
        data = _get_historical_prices(["^VIX"], days + 10, start_date, end_date)
        _vix_cache = data.get("^VIX", {})
    except Exception:
        # Fallback: try yfinance directly
        try:
            import yfinance as yf
            if start_date:
                df = yf.download("^VIX", start=start_date,
                                 end=end_date or datetime.now().strftime("%Y-%m-%d"),
                                 interval="1d", progress=False, auto_adjust=True)
            else:
                period = "1mo" if days <= 30 else ("3mo" if days <= 90 else "6mo")
                df = yf.download("^VIX", period=period,
                                 interval="1d", progress=False, auto_adjust=True)
            if not df.empty:
                for idx, row in df.iterrows():
                    close = row["Close"]
                    if hasattr(close, "item"):
                        close = close.item()
                    _vix_cache[idx.strftime("%Y-%m-%d")] = round(float(close), 2)
        except Exception as e:
            console.log(f"[yellow]VIX history fetch failed: {e}")

    return _vix_cache


def _is_bear_day(date_str: str, vix_history: dict) -> bool:
    """Check if a specific date was a bear market day (VIX > 25)."""
    vix = vix_history.get(date_str)
    if vix is not None:
        return vix > 25
    # Check nearest prior date
    for offset in range(1, 5):
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=offset)
            vix = vix_history.get(dt.strftime("%Y-%m-%d"))
            if vix is not None:
                return vix > 25
        except Exception:
            pass
    return False


# ============================================================
# GUARDRAIL CONFIG V2 (mirrors risk_manager.py)
# V1: Capital preservation (flat limits) — stopped the bleeding
# V2: Alpha generation (conviction-scaled) — let winners run
# ============================================================

DAILY_LIMITS = {
    "default": 3,
    "ollama-local": 2,
    "gemini-2.5-flash": 2,
    "grok-4": 3,
    "ollama-qwen3": 3,
    "ollama-plutus": 3,
    "energy-arnold": 2,
    "options-sosnoff": 2,
}

BEAR_DAILY_LIMITS = {
    "default": 1,
    "ollama-local": 0,      # Geordi: ALL STOP when VIX > 25
    "gemini-2.5-flash": 0,  # Troi: WATCHLIST mode, no buys
    "grok-4": 1,
    "ollama-qwen3": 1,
    "ollama-plutus": 1,
    "energy-arnold": 1,
    "options-sosnoff": 1,
}

MIN_CONVICTION = {
    "default": 0.65,
    "grok-4": 0.75,
    "gemini-2.5-flash": 0.70,
}

BEAR_MIN_CONVICTION = {
    "default": 0.80,
    "grok-4": 0.85,
}

# V2: Conviction-scaled position sizing (replaces flat MAX_POSITION_PCT)
def _get_position_size_pct(conviction: float, is_bear: bool) -> float:
    """Scale position size by conviction — let winners run."""
    if is_bear:
        if conviction >= 0.90:
            return 0.25
        elif conviction >= 0.80:
            return 0.15
        else:
            return 0.10
    else:
        if conviction >= 0.90:
            return 0.33  # Matches Rallies Grok 4's MU allocation
        elif conviction >= 0.80:
            return 0.25
        elif conviction >= 0.70:
            return 0.20
        else:
            return 0.15

# V2: Conviction-scaled stop-loss (replaces flat STOP_LOSS_PCT)
def _get_stop_loss_pct(conviction: float) -> float:
    """Wider stops for higher conviction — let winners breathe."""
    if conviction >= 0.90:
        return 0.18
    elif conviction >= 0.80:
        return 0.15
    elif conviction >= 0.70:
        return 0.12
    else:
        return 0.08

# V2: Minimum holding periods (days)
MIN_HOLD_DAYS = {
    "default": 5,
    "grok-4": 7,
    "gemini-2.5-flash": 5,
    "ollama-local": 5,
    "ollama-qwen3": 10,
    "ollama-plutus": 7,
}

# V2: Position limits
BEAR_MAX_POSITIONS = 3
NORMAL_MAX_POSITIONS = 5  # V3: was 6

# V3: Per-model position limits (fewer picks, bigger bets)
MAX_POSITIONS_PER_MODEL = {
    "default": 5,
    "grok-4": 3,
    "ollama-local": 3,
    "gemini-2.5-flash": 4,
    "ollama-qwen3": 3,
    "ollama-plutus": 4,
    "energy-arnold": 3,
    "options-sosnoff": 5,
}

MIN_CASH_PCT = 0.20           # 20% normal
BEAR_MIN_CASH_PCT = 0.35      # V2: 35% (was 40%, deploy more in best picks)


# ============================================================
# SIMULATION ENGINES
# ============================================================

def _simulate_raw(signals: list, hist_data: dict) -> tuple:
    """Original simulation — no guardrails, every signal executed."""
    cash = STARTING_CASH
    trades = []

    for sig in signals:
        sym = sig["symbol"]
        conf = sig["confidence"] or 0.5
        signal_date = sig["created_at"][:10]

        prices = hist_data.get(sym, {})
        if signal_date not in prices:
            continue

        entry_price = prices[signal_date]
        position_size = cash * 0.10
        qty = position_size / entry_price
        cost = qty * entry_price

        if cost > cash or cost <= 0:
            continue

        # Hold for 5 days, then exit
        exit_date, exit_price = _find_exit(prices, signal_date, entry_price, hold_days=5)

        pnl = (exit_price - entry_price) * qty
        pnl_pct = ((exit_price / entry_price) - 1) * 100

        cash -= cost
        cash += qty * exit_price

        trades.append({
            "symbol": sym, "signal": sig["signal"], "confidence": conf,
            "entry_date": signal_date, "entry_price": round(entry_price, 2),
            "exit_date": exit_date, "exit_price": round(exit_price, 2),
            "pnl": round(pnl, 2), "pnl_pct": round(pnl_pct, 2),
            "skipped": False, "skip_reason": "",
        })

    return trades, cash


def _v3_trailing_stop_pct(gain_pct: float) -> float:
    """V3: Dynamic trailing stop — matches risk_manager._get_trailing_stop_pct."""
    if gain_pct >= 0.20:
        return 0.10
    elif gain_pct >= 0.10:
        return 0.12
    elif gain_pct >= 0.05:
        return 0.15
    else:
        return 0.05


def _simulate_guarded(player_id: str, signals: list, hist_data: dict,
                      vix_history: dict) -> tuple:
    """V3 Guarded simulation — per-model limits, quality gate, trailing stops, pyramids."""
    cash = STARTING_CASH
    trades = []
    skipped = []
    daily_trade_counts: dict[str, int] = {}
    open_positions: list[dict] = []
    v3_stats = {"trailing_stops_hit": 0, "pyramids_executed": 0, "quality_gate_blocked": 0}

    min_hold = MIN_HOLD_DAYS.get(player_id, MIN_HOLD_DAYS["default"])

    # V3: Pre-fetch fundamentals for quality gate (cached, so only fetched once per symbol)
    _fundamentals_cache = {}
    def _get_fund(sym):
        if sym not in _fundamentals_cache:
            try:
                from engine.stock_fundamentals import fetch_fundamentals
                _fundamentals_cache[sym] = fetch_fundamentals(sym)
            except Exception:
                _fundamentals_cache[sym] = None
        return _fundamentals_cache[sym]

    for sig in signals:
        sym = sig["symbol"]
        conf = sig["confidence"] or 0.5
        signal_date = sig["created_at"][:10]
        is_bear = _is_bear_day(signal_date, vix_history)

        prices = hist_data.get(sym, {})
        if signal_date not in prices:
            continue

        # --- Process open positions: trailing stops, pyramids, hold exits ---
        still_open = []
        for pos in open_positions:
            pos_prices = hist_data.get(pos["symbol"], {})

            days_held = len([d for d in pos_prices if pos["entry_date"] < d <= signal_date])

            # V3: Update high watermark and check trailing stop during hold
            closed = False
            for d in sorted(d for d in pos_prices if pos["entry_date"] < d <= signal_date):
                day_price = pos_prices[d]

                # Update high watermark
                if day_price > pos.get("high_watermark", pos["entry_price"]):
                    pos["high_watermark"] = day_price

                gain_from_entry = (day_price - pos["entry_price"]) / pos["entry_price"]

                # V3: Pyramid up — add 50% more at +5% gain (once)
                if (not pos.get("pyramided") and gain_from_entry >= 0.05
                        and not is_bear):
                    pyramid_qty = pos["qty"] * 0.5
                    pyramid_cost = pyramid_qty * day_price
                    total_value = cash + sum(
                        p["qty"] * hist_data.get(p["symbol"], {}).get(d, p["entry_price"])
                        for p in open_positions if p is not pos
                    ) + pos["qty"] * day_price
                    cash_floor = BEAR_MIN_CASH_PCT if is_bear else MIN_CASH_PCT
                    if total_value > 0 and (cash - pyramid_cost) / total_value >= cash_floor and pyramid_cost <= cash:
                        pos["qty"] += pyramid_qty
                        pos["cost"] += pyramid_cost
                        pos["avg_price"] = pos["cost"] / pos["qty"]
                        cash -= pyramid_cost
                        pos["pyramided"] = True
                        v3_stats["pyramids_executed"] += 1

                # V3: Trailing stop check
                hwm = pos.get("high_watermark", pos["entry_price"])
                if gain_from_entry > 0 and hwm > pos["entry_price"]:
                    trail_pct = _v3_trailing_stop_pct(gain_from_entry)
                    trailing_stop = hwm * (1 - trail_pct)
                    if gain_from_entry >= 0.05:
                        trailing_stop = max(trailing_stop, pos["entry_price"] * 0.98)
                    if day_price <= trailing_stop:
                        pnl = (trailing_stop - pos["avg_price"]) * pos["qty"]
                        pnl_pct = ((trailing_stop / pos["avg_price"]) - 1) * 100
                        cash += pos["qty"] * trailing_stop
                        d_held = len([dd for dd in pos_prices if pos["entry_date"] < dd <= d])
                        trades.append({
                            "symbol": pos["symbol"], "signal": "BUY", "confidence": pos["confidence"],
                            "entry_date": pos["entry_date"], "entry_price": round(pos["entry_price"], 2),
                            "exit_date": d, "exit_price": round(trailing_stop, 2),
                            "pnl": round(pnl, 2), "pnl_pct": round(pnl_pct, 2),
                            "skipped": False, "skip_reason": "",
                            "stopped_out": True, "bear_day": pos.get("bear_day", False),
                            "hold_days": d_held, "trailing_stop": True,
                            "pyramided": pos.get("pyramided", False),
                        })
                        v3_stats["trailing_stops_hit"] += 1
                        closed = True
                        break

                # Fixed stop-loss check (for losing positions)
                if day_price <= pos["stop_price"]:
                    pnl = (pos["stop_price"] - pos["avg_price"]) * pos["qty"]
                    pnl_pct = ((pos["stop_price"] / pos["avg_price"]) - 1) * 100
                    cash += pos["qty"] * pos["stop_price"]
                    d_held = len([dd for dd in pos_prices if pos["entry_date"] < dd <= d])
                    trades.append({
                        "symbol": pos["symbol"], "signal": "BUY", "confidence": pos["confidence"],
                        "entry_date": pos["entry_date"], "entry_price": round(pos["entry_price"], 2),
                        "exit_date": d, "exit_price": round(pos["stop_price"], 2),
                        "pnl": round(pnl, 2), "pnl_pct": round(pnl_pct, 2),
                        "skipped": False, "skip_reason": "",
                        "stopped_out": True, "bear_day": pos.get("bear_day", False),
                        "hold_days": d_held, "trailing_stop": False,
                        "pyramided": pos.get("pyramided", False),
                    })
                    closed = True
                    break

            if closed:
                continue

            # Check if min hold period elapsed (normal exit)
            if days_held >= min_hold:
                exit_price = pos_prices.get(signal_date, pos["entry_price"])
                pnl = (exit_price - pos["avg_price"]) * pos["qty"]
                pnl_pct = ((exit_price / pos["avg_price"]) - 1) * 100
                cash += pos["qty"] * exit_price
                trades.append({
                    "symbol": pos["symbol"], "signal": "BUY", "confidence": pos["confidence"],
                    "entry_date": pos["entry_date"], "entry_price": round(pos["entry_price"], 2),
                    "exit_date": signal_date, "exit_price": round(exit_price, 2),
                    "pnl": round(pnl, 2), "pnl_pct": round(pnl_pct, 2),
                    "skipped": False, "skip_reason": "",
                    "stopped_out": False, "bear_day": pos.get("bear_day", False),
                    "hold_days": days_held, "trailing_stop": False,
                    "pyramided": pos.get("pyramided", False),
                })
                continue

            still_open.append(pos)
        open_positions = still_open

        entry_price = prices[signal_date]

        # --- GUARDRAIL 1: Daily trade limit ---
        day_count = daily_trade_counts.get(signal_date, 0)
        day_limit = (BEAR_DAILY_LIMITS if is_bear else DAILY_LIMITS).get(
            player_id, (BEAR_DAILY_LIMITS if is_bear else DAILY_LIMITS)["default"])
        if day_count >= day_limit:
            skipped.append({"symbol": sym, "date": signal_date, "confidence": conf,
                            "reason": f"MAX_TRADES_REACHED: {day_count}/{day_limit}"})
            continue

        # --- GUARDRAIL 2: Conviction filter ---
        min_conv = (BEAR_MIN_CONVICTION if is_bear else MIN_CONVICTION).get(
            player_id, (BEAR_MIN_CONVICTION if is_bear else MIN_CONVICTION)["default"])
        if conf < min_conv:
            skipped.append({"symbol": sym, "date": signal_date, "confidence": conf,
                            "reason": f"LOW_CONVICTION: {conf:.0%} < {min_conv:.0%}"})
            continue

        # --- GUARDRAIL 3: V3 Per-model position count limit ---
        model_max_pos = MAX_POSITIONS_PER_MODEL.get(player_id, MAX_POSITIONS_PER_MODEL["default"])
        max_pos = min(model_max_pos, BEAR_MAX_POSITIONS) if is_bear else model_max_pos
        if len(open_positions) >= max_pos:
            skipped.append({"symbol": sym, "date": signal_date, "confidence": conf,
                            "reason": f"MAX_POSITIONS: {len(open_positions)}/{max_pos}"})
            continue

        # --- GUARDRAIL 4: No duplicate positions ---
        if any(p["symbol"] == sym for p in open_positions):
            skipped.append({"symbol": sym, "date": signal_date, "confidence": conf,
                            "reason": f"DUPLICATE: already holding {sym}"})
            continue

        # --- GUARDRAIL 5: V2 Conviction-scaled position sizing ---
        pos_pct = _get_position_size_pct(conf, is_bear)
        position_size = cash * pos_pct
        qty = position_size / entry_price
        cost = qty * entry_price

        if cost <= 0:
            continue

        # --- GUARDRAIL 6: Cash floor ---
        cash_floor = BEAR_MIN_CASH_PCT if is_bear else MIN_CASH_PCT
        if (cash - cost) / max(cash, 1) < cash_floor:
            skipped.append({"symbol": sym, "date": signal_date, "confidence": conf,
                            "reason": f"CASH_FLOOR: would breach {cash_floor:.0%}"})
            continue

        if cost > cash:
            continue

        # --- GUARDRAIL 7: V3 Quality gate (simplified for backtest) ---
        fund = _get_fund(sym)
        if fund and sym not in ("SPY", "QQQ"):
            qscore = 0
            eg = fund.get("earnings_growth")
            if eg is not None and eg > 0:
                qscore += 1
            rg = fund.get("revenue_growth")
            if rg is not None and rg > 0:
                qscore += 1
            rec = fund.get("recommendation", "")
            if rec and rec.lower() in ("buy", "strongbuy", "strong_buy", "overweight"):
                qscore += 1
            # Give partial credit for missing data
            if eg is None:
                qscore += 0.5
            if rg is None:
                qscore += 0.5
            if qscore < 2:
                skipped.append({"symbol": sym, "date": signal_date, "confidence": conf,
                                "reason": f"QUALITY_GATE: scored {int(qscore)}/3"})
                v3_stats["quality_gate_blocked"] += 1
                continue

        # --- Open position with conviction-scaled stop ---
        stop_pct = _get_stop_loss_pct(conf)
        stop_price = entry_price * (1 - stop_pct)

        cash -= cost
        daily_trade_counts[signal_date] = day_count + 1

        open_positions.append({
            "symbol": sym, "entry_date": signal_date, "entry_price": entry_price,
            "avg_price": entry_price,
            "qty": qty, "cost": cost, "stop_price": stop_price,
            "high_watermark": entry_price,
            "confidence": conf, "bear_day": is_bear,
            "pyramided": False,
        })

    # --- Close any remaining open positions at last available price ---
    for pos in open_positions:
        pos_prices = hist_data.get(pos["symbol"], {})
        if pos_prices:
            last_date = sorted(pos_prices.keys())[-1]
            exit_price = pos_prices[last_date]
            stopped = False
            exit_d = last_date
            for d in sorted(d for d in pos_prices if d > pos["entry_date"]):
                day_price = pos_prices[d]
                # Update HWM
                if day_price > pos.get("high_watermark", pos["entry_price"]):
                    pos["high_watermark"] = day_price
                # Trailing stop
                gain = (day_price - pos["avg_price"]) / pos["avg_price"] if pos["avg_price"] > 0 else 0
                hwm = pos.get("high_watermark", pos["entry_price"])
                if gain > 0 and hwm > pos["avg_price"]:
                    trail = _v3_trailing_stop_pct(gain)
                    ts = hwm * (1 - trail)
                    if gain >= 0.05:
                        ts = max(ts, pos["avg_price"] * 0.98)
                    if day_price <= ts:
                        exit_price = ts
                        exit_d = d
                        stopped = True
                        v3_stats["trailing_stops_hit"] += 1
                        break
                # Fixed stop
                if day_price <= pos["stop_price"]:
                    exit_price = pos["stop_price"]
                    exit_d = d
                    stopped = True
                    break
            pnl = (exit_price - pos["avg_price"]) * pos["qty"]
            pnl_pct = ((exit_price / pos["avg_price"]) - 1) * 100
            days_held = len([d for d in pos_prices if pos["entry_date"] < d <= exit_d])
            cash += pos["qty"] * exit_price
            trades.append({
                "symbol": pos["symbol"], "signal": "BUY", "confidence": pos["confidence"],
                "entry_date": pos["entry_date"], "entry_price": round(pos["entry_price"], 2),
                "exit_date": exit_d, "exit_price": round(exit_price, 2),
                "pnl": round(pnl, 2), "pnl_pct": round(pnl_pct, 2),
                "skipped": False, "skip_reason": "",
                "stopped_out": stopped, "bear_day": pos.get("bear_day", False),
                "hold_days": days_held, "trailing_stop": stopped and not (pos_prices.get(exit_d, 0) <= pos["stop_price"]),
                "pyramided": pos.get("pyramided", False),
            })

    return trades, cash, skipped, v3_stats


def _find_exit(prices: dict, signal_date: str, entry_price: float,
               hold_days: int = 5) -> tuple:
    """Find exit price after hold_days. Returns (exit_date, exit_price)."""
    sorted_dates = sorted(d for d in prices if d > signal_date)
    exit_date = None
    exit_price = entry_price

    for i, d in enumerate(sorted_dates):
        if i >= hold_days - 1:
            exit_date = d
            exit_price = prices[d]
            break

    if not exit_date and sorted_dates:
        exit_date = sorted_dates[-1]
        exit_price = prices[exit_date]

    return exit_date, exit_price


def _find_exit_with_stop(prices: dict, signal_date: str, entry_price: float,
                         hold_days: int = 5, stop_loss_pct: float = 0.12) -> tuple:
    """Find exit with stop-loss check. If price drops below stop at any point
    during hold period, exit at the stop price instead of waiting."""
    sorted_dates = sorted(d for d in prices if d > signal_date)
    stop_price = entry_price * (1 - stop_loss_pct)

    for i, d in enumerate(sorted_dates):
        day_price = prices[d]

        # Check stop-loss hit
        if day_price <= stop_price:
            return d, round(stop_price, 2)

        # Normal exit after hold period
        if i >= hold_days - 1:
            return d, day_price

    # Not enough data — use last available
    if sorted_dates:
        return sorted_dates[-1], prices[sorted_dates[-1]]

    return None, entry_price


# ============================================================
# MAIN BACKTEST FUNCTION
# ============================================================

def backtest_player(player_id: str, days: int = 30,
                    start_date: str = None, end_date: str = None,
                    apply_guardrails: bool = False) -> dict:
    """Simulate replaying a model's actual signals against historical prices.

    Args:
        days: Lookback in days from today (default 30, max ~3650).
        start_date: Optional start date "YYYY-MM-DD" (overrides days).
        end_date: Optional end date "YYYY-MM-DD" (defaults to today).
        apply_guardrails: If True, apply risk_manager.py guardrails to filter signals.
    """
    conn = _conn()

    player = conn.execute(
        "SELECT display_name FROM ai_players WHERE id=?", (player_id,)
    ).fetchone()
    if not player:
        conn.close()
        return {"error": "Player not found"}

    # Resolve date range
    if start_date:
        cutoff = start_date
        end_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.now()
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        effective_days = (end_dt - start_dt).days
    else:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        end_dt = datetime.now()
        effective_days = days

    # Get all BUY signals in the period
    query = """
        SELECT symbol, signal, confidence, created_at
        FROM signals WHERE player_id=? AND signal IN ('BUY', 'BUY_CALL', 'BUY_PUT')
        AND created_at >= ? ORDER BY created_at ASC
    """
    params = [player_id, cutoff]
    if start_date and end_date:
        query = """
            SELECT symbol, signal, confidence, created_at
            FROM signals WHERE player_id=? AND signal IN ('BUY', 'BUY_CALL', 'BUY_PUT')
            AND created_at >= ? AND created_at <= ? ORDER BY created_at ASC
        """
        params.append(end_date + "T23:59:59")

    signals = conn.execute(query, params).fetchall()
    conn.close()

    empty_result = {
        "player_id": player_id,
        "name": player["display_name"],
        "days": effective_days,
        "start_date": start_date or (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d"),
        "end_date": end_date or datetime.now().strftime("%Y-%m-%d"),
        "signals_tested": 0,
        "guardrails_applied": apply_guardrails,
        "trades": [],
        "equity_curve": [],
        "stats": _empty_stats(),
    }

    if not signals:
        return empty_result

    # Get historical prices for all symbols
    symbols = list(set(s["symbol"] for s in signals))
    hist_data = _get_historical_prices(symbols, effective_days + 10,
                                       start_date=start_date, end_date=end_date)

    # Fetch VIX history for guardrailed mode
    vix_history = {}
    if apply_guardrails:
        vix_history = _get_vix_history(effective_days + 10, start_date, end_date)
        bear_days = sum(1 for v in vix_history.values() if v > 25)
        console.log(f"[cyan]Backtest VIX data: {len(vix_history)} days, {bear_days} bear days (VIX>25)")

    # Run simulation
    if apply_guardrails:
        result_tuple = _simulate_guarded(
            player_id, signals, hist_data, vix_history
        )
        # V3 returns 4 values, V2 returned 3
        if len(result_tuple) == 4:
            trades, final_cash, skipped_signals, v3_stats = result_tuple
        else:
            trades, final_cash, skipped_signals = result_tuple
            v3_stats = {}
        console.log(f"[cyan]Guarded backtest: {len(trades)} trades executed, "
                     f"{len(skipped_signals)} signals skipped by guardrails")
    else:
        trades, final_cash = _simulate_raw(signals, hist_data)
        skipped_signals = []

    # Build equity curve
    equity_curve = [{"day": 0, "value": STARTING_CASH}]
    running_value = STARTING_CASH
    for i, t in enumerate(trades):
        running_value += t["pnl"]
        equity_curve.append({"day": i + 1, "value": round(running_value, 2)})

    # Calculate stats
    wins = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]
    total_pnl = sum(t["pnl"] for t in trades)

    stats = {
        "total_trades": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(len(wins) / len(trades) * 100, 1) if trades else 0,
        "total_pnl": round(total_pnl, 2),
        "total_return_pct": round(total_pnl / STARTING_CASH * 100, 2),
        "avg_pnl": round(total_pnl / len(trades), 2) if trades else 0,
        "best_trade": max(trades, key=lambda t: t["pnl"])["pnl"] if trades else 0,
        "worst_trade": min(trades, key=lambda t: t["pnl"])["pnl"] if trades else 0,
        "final_value": round(running_value, 2),
        "signals_skipped": len(skipped_signals),
    }
    # V3: Add trailing stop / pyramid / quality gate stats
    if apply_guardrails and 'v3_stats' in dir():
        stats.update(v3_stats)

    result = {
        "player_id": player_id,
        "name": player["display_name"],
        "days": effective_days,
        "start_date": start_date or (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d"),
        "end_date": end_date or datetime.now().strftime("%Y-%m-%d"),
        "signals_tested": len(signals),
        "guardrails_applied": apply_guardrails,
        "trades": trades,
        "equity_curve": equity_curve,
        "stats": stats,
    }

    if apply_guardrails:
        result["skipped_signals"] = skipped_signals[:50]  # Cap to avoid huge responses
        result["skip_summary"] = _summarize_skips(skipped_signals)

    return result


def _summarize_skips(skipped: list) -> dict:
    """Summarize why signals were skipped."""
    reasons = {}
    for s in skipped:
        key = s["reason"].split(":")[0]
        reasons[key] = reasons.get(key, 0) + 1
    return reasons


# ============================================================
# COMPARISON (run both raw and guarded, return side-by-side)
# ============================================================

def backtest_compare(player_id: str, days: int = 30,
                     start_date: str = None, end_date: str = None) -> dict:
    """Run both raw and guarded backtests, return side-by-side comparison."""
    # Clear VIX cache so both runs use fresh data
    global _vix_cache
    _vix_cache = {}

    raw = backtest_player(player_id, days, start_date, end_date, apply_guardrails=False)
    guarded = backtest_player(player_id, days, start_date, end_date, apply_guardrails=True)

    improvement = {
        "return_pct_change": (guarded["stats"]["total_return_pct"] -
                              raw["stats"]["total_return_pct"]),
        "trades_reduced": raw["stats"]["total_trades"] - guarded["stats"]["total_trades"],
        "win_rate_change": guarded["stats"]["win_rate"] - raw["stats"]["win_rate"],
        "pnl_change": guarded["stats"]["total_pnl"] - raw["stats"]["total_pnl"],
    }

    return {
        "player_id": player_id,
        "name": raw["name"],
        "raw": raw,
        "guarded": guarded,
        "improvement": improvement,
    }


# ============================================================
# PRICE FETCHING
# ============================================================

def _get_historical_prices(symbols: list, days: int,
                           start_date: str = None, end_date: str = None) -> dict:
    """Get daily close prices. Returns {symbol: {date_str: close_price}}."""
    if days > 365 or start_date:
        return _get_historical_prices_yf(symbols, days, start_date, end_date)

    result = {}
    for sym in symbols:
        try:
            chart = _yahoo_chart(sym, interval="1d", range_=f"{days}d")
            if not chart:
                continue
            timestamps = chart.get("timestamp", [])
            quotes = chart.get("indicators", {}).get("quote", [{}])[0]
            closes = quotes.get("close", [])
            if not timestamps or not closes:
                continue
            result[sym] = {}
            for i, ts in enumerate(timestamps):
                if i < len(closes) and closes[i] is not None:
                    date_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
                    result[sym][date_str] = round(closes[i], 2)
        except Exception:
            continue
    return result


def _get_historical_prices_yf(symbols: list, days: int,
                              start_date: str = None, end_date: str = None) -> dict:
    """Get long-range historical prices via yfinance download."""
    import yfinance as yf

    result = {}
    if start_date:
        start = start_date
        end = end_date or datetime.now().strftime("%Y-%m-%d")
    else:
        if days <= 30:
            period = "1mo"
        elif days <= 90:
            period = "3mo"
        elif days <= 180:
            period = "6mo"
        elif days <= 365:
            period = "1y"
        elif days <= 730:
            period = "2y"
        elif days <= 1825:
            period = "5y"
        else:
            period = "10y"
        start = None
        end = None

    for sym in symbols:
        try:
            if start:
                df = yf.download(sym, start=start, end=end,
                                 interval="1d", progress=False, auto_adjust=True)
            else:
                df = yf.download(sym, period=period,
                                 interval="1d", progress=False, auto_adjust=True)
            if df.empty:
                continue
            result[sym] = {}
            for idx, row in df.iterrows():
                date_str = idx.strftime("%Y-%m-%d")
                close = row["Close"]
                if hasattr(close, "item"):
                    close = close.item()
                result[sym][date_str] = round(float(close), 2)
        except Exception as e:
            console.log(f"[yellow]yf download {sym} failed: {e}")
            continue
    return result


def _empty_stats() -> dict:
    return {
        "total_trades": 0, "wins": 0, "losses": 0, "win_rate": 0,
        "total_pnl": 0, "total_return_pct": 0, "avg_pnl": 0,
        "best_trade": 0, "worst_trade": 0, "final_value": STARTING_CASH,
        "signals_skipped": 0,
    }
