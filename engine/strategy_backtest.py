"""Strategy Engine Backtest — test Warp 9 strategies against historical data.

Simulates the universe scanner + strategy engine over a date range using
only Yahoo Finance price data. No AI models involved.
"""
from __future__ import annotations
import numpy as np
from datetime import datetime, timedelta
from rich.console import Console

console = Console()
STARTING_CASH = 10000.0
MAX_POSITIONS = 5
MIN_RR = 1.5
POSITION_PCT = 0.18

# Warp 9.6: Adaptive regime thresholds (replaces fixed 4.0)
BULL_THRESHOLD = 4.0   # VIX < 20: strict quality filter
CAUTION_THRESHOLD = 3.5  # VIX 20-25: middle ground
BEAR_THRESHOLD = 3.0   # VIX > 25 or SPY < 200MA: catch bounces

# Warp 9.5 Tune 2: Strategy weights based on backtest win rates
STRATEGY_WEIGHTS = {
    "rsi_divergence": 2.0,   # 86% WR
    "bb_bounce": 2.0,        # 83% WR
    "rsi_bounce": 1.5,       # 71-100% WR
    "macd_cross": 1.5,       # 62.5% WR
    "rs_high": 1.2,          # 63.6% WR
    "pullback_sma20": 1.2,   # 59.3% WR
    "ema_ribbon": 1.0,       # 58.3% WR
    "breakout_vol": 1.0,
    "higher_hh": 0.8,
    "trend_resume": 0.8,
    "gap_fill": 0.8,
    "vol_surge": 0.8,
    "accumulation": 0.5,
    "vol_dryup": 0.5,
    "hammer_candle": 1.2,
    "bull_bear_trap": 1.5,
    "falling_knife": 0.8,
    "avwap_bounce": 1.5,
    "five_day_bounce": 1.5,
    "alpha_predator": 1.0,
}

# Warp 9.5 Tune 6: Day-of-week confidence multiplier
DOW_MULT = {0: 0.8, 1: 1.0, 2: 1.0, 3: 1.0, 4: 0.8}  # Mon/Fri penalized


def _ema(data, span):
    alpha = 2 / (span + 1)
    result = [float(data[0])]
    for i in range(1, len(data)):
        result.append(alpha * float(data[i]) + (1 - alpha) * result[-1])
    return np.array(result)


def _rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50.0
    deltas = np.diff(closes)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    avg_gain = np.mean(gains[-period:])
    avg_loss = np.mean(losses[-period:])
    if avg_loss == 0:
        return 100.0
    return 100 - (100 / (1 + avg_gain / avg_loss))


def _trailing_stop_pct(gain_pct):
    if gain_pct >= 0.20:
        return 0.10
    elif gain_pct >= 0.10:
        return 0.12
    elif gain_pct >= 0.05:
        return 0.15
    else:
        return 0.05


# ── Strategy checks (all operate on numpy arrays) ───────────────

def _check_breakout_volume(c, h, v, avg_v):
    if len(c) < 21:
        return False
    return float(c[-1]) > float(np.max(h[-21:-1])) and float(v[-1]) > avg_v * 2

def _check_pullback_sma20(c):
    if len(c) < 50:
        return False
    sma20 = float(np.mean(c[-20:]))
    sma50 = float(np.mean(c[-50:]))
    return float(c[-1]) > sma50 and abs(float(c[-1]) - sma20) / float(c[-1]) < 0.02

def _check_rsi_bounce(c):
    if len(c) < 16:
        return False
    return _rsi(c) > 30 and _rsi(c[:-1]) < 30

def _check_macd_cross(c):
    if len(c) < 27:
        return False
    e12 = _ema(c, 12); e26 = _ema(c, 26)
    macd = e12 - e26; sig = _ema(macd, 9)
    e12p = _ema(c[:-1], 12); e26p = _ema(c[:-1], 26)
    macdp = e12p - e26p; sigp = _ema(macdp, 9)
    return float(macd[-1]) > float(sig[-1]) and float(macdp[-1]) <= float(sigp[-1])

def _check_bollinger_bounce(c):
    if len(c) < 21:
        return False
    sma20 = float(np.mean(c[-20:]))
    std20 = float(np.std(c[-20:]))
    bb_lower = sma20 - 2 * std20
    return float(c[-2]) <= bb_lower and float(c[-1]) > bb_lower

def _check_ema_ribbon(c):
    if len(c) < 55:
        return False
    emas = [float(_ema(c, s)[-1]) for s in (8, 13, 21, 34, 55)]
    return all(emas[i] > emas[i+1] for i in range(len(emas)-1))

def _check_higher_highs(h, l):
    if len(h) < 15:
        return False
    hh = [float(np.max(h[i:i+5])) for i in range(-15, 0, 5)]
    ll = [float(np.min(l[i:i+5])) for i in range(-15, 0, 5)]
    return hh[2] > hh[1] > hh[0] and ll[2] > ll[1] > ll[0]

def _check_trend_resumption(c):
    if len(c) < 20:
        return False
    sma20 = float(np.mean(c[-20:]))
    price = float(c[-1])
    if price <= sma20:
        return False
    return float(np.min(c[-5:])) < sma20

def _check_volume_surge(v, avg_v):
    return float(v[-1]) > avg_v * 3

def _check_gap_fill(c):
    if len(c) < 3:
        return False
    gap = (float(c[-2]) - float(c[-3])) / float(c[-3]) * 100
    rec = (float(c[-1]) - float(c[-2])) / float(c[-2]) * 100
    return gap < -3 and rec > 1

def _check_accumulation(c, v):
    if len(c) < 4 or len(v) < 4:
        return False
    flat = abs(float(c[-1]) - float(c[-4])) / float(c[-4]) < 0.01
    vol_up = float(v[-1]) > float(v[-2]) > float(v[-3])
    return flat and vol_up

def _check_rs_high(c, spy_c):
    if len(c) < 50 or spy_c is None or len(spy_c) < 50:
        return False
    n = min(len(c), len(spy_c))
    rs = np.array(c[-n:], dtype=float) / np.array(spy_c[-n:], dtype=float)
    return float(rs[-1]) >= float(np.max(rs[:-1]))

def _check_rsi_divergence(c, l):
    if len(c) < 30:
        return False
    recent_low = float(np.min(l[-5:]))
    prev_low = float(np.min(l[-15:-5]))
    if recent_low >= prev_low:
        return False
    rsi_now = _rsi(c[-15:])
    rsi_prev = _rsi(c[-25:-10])
    return rsi_now > rsi_prev and rsi_now < 40

def _check_volume_dryup(v, avg_v):
    return float(v[-1]) < avg_v * 0.5

def _check_hammer_candle(c, h, l):
    """Hammer: long lower wick >= 2x body, small upper wick, close near high."""
    if len(c) < 2 or len(h) < 2 or len(l) < 2:
        return False
    close = float(c[-1]); open_p = float(c[-2]); hi = float(h[-1]); lo = float(l[-1])
    body = abs(close - open_p)
    lower_wick = min(close, open_p) - lo
    upper_wick = hi - max(close, open_p)
    candle_range = hi - lo
    if candle_range <= 0 or body <= 0:
        return False
    return lower_wick >= 2 * body and upper_wick <= body * 0.5 and close > open_p

def _check_bull_bear_trap(c, h, l):
    """Bull/bear trap: price broke a level then reversed — false breakout reversal."""
    if len(c) < 5 or len(h) < 5 or len(l) < 5:
        return False
    # Bear trap: prior down-break of recent low, now recovered back above
    recent_low = float(np.min(l[-5:-1]))
    prev_low_broke = float(l[-2]) < recent_low * 0.99
    recovered = float(c[-1]) > recent_low
    return prev_low_broke and recovered

def _check_falling_knife(c, l):
    """Falling knife catch: 3+ consecutive down days, RSI <25, current close > prev open."""
    if len(c) < 6:
        return False
    consecutive_down = all(float(c[i]) < float(c[i-1]) for i in range(-4, 0))
    rsi_val = _rsi(c[-20:]) if len(c) >= 20 else 50.0
    bounce = float(c[-1]) > float(c[-2])
    return consecutive_down and rsi_val < 28 and bounce

def _check_avwap_bounce(c, v):
    """AVWAP bounce: price pulled back to approximate VWAP (SMA20-weighted by volume) and bouncing."""
    if len(c) < 22 or len(v) < 22:
        return False
    # Approximate anchored VWAP as volume-weighted mean of last 20 bars
    closes_arr = np.array([float(x) for x in c[-20:]])
    vols_arr = np.array([float(x) for x in v[-20:]])
    total_vol = float(np.sum(vols_arr))
    if total_vol <= 0:
        return False
    vwap_approx = float(np.sum(closes_arr * vols_arr) / total_vol)
    price = float(c[-1])
    prev_price = float(c[-2])
    # Price dipped to within 1% of VWAP then bounced above it
    return prev_price <= vwap_approx * 1.01 and price > vwap_approx and price > prev_price

def _check_five_day_bounce(c, l):
    """Five-day bounce: 5 consecutive down days on close, RSI oversold, now reversing."""
    if len(c) < 8:
        return False
    five_down = all(float(c[-i-1]) < float(c[-i-2]) for i in range(0, 5))
    rsi_val = _rsi(c[-20:]) if len(c) >= 20 else 50.0
    reversal = float(c[-1]) > float(c[-2]) * 1.005  # 0.5% bounce to confirm
    return five_down and rsi_val < 35 and reversal

def _check_alpha_predator(c, v, avg_v):
    """Alpha predator: 3-day acceleration — each day stronger than last (price + volume momentum compound)."""
    if len(c) < 5 or len(v) < 4:
        return False
    # Price gains accelerating over last 3 days
    d1 = float(c[-2]) - float(c[-3])
    d2 = float(c[-1]) - float(c[-2])
    price_accel = d2 > d1 > 0
    # Volume confirming (rising for 3 days)
    vol_rising = float(v[-1]) > float(v[-2]) > float(v[-3]) > avg_v
    # Not already extended (RSI < 75)
    rsi_val = _rsi(c[-20:]) if len(c) >= 20 else 50.0
    return price_accel and vol_rising and rsi_val < 75


STRATEGY_NAMES = [
    "breakout_vol", "pullback_sma20", "rsi_bounce", "macd_cross",
    "bb_bounce", "ema_ribbon", "higher_hh", "trend_resume",
    "vol_surge", "gap_fill", "accumulation", "rs_high",
    "rsi_divergence", "vol_dryup",
    "hammer_candle", "bull_bear_trap", "falling_knife",
    "avwap_bounce", "five_day_bounce", "alpha_predator",
]


def _weighted_score(triggered: list) -> float:
    """Warp 9.5 Tune 2: Sum weighted strategy scores."""
    return sum(STRATEGY_WEIGHTS.get(s, 1.0) for s in triggered)


def _is_good_entry(c, o, h, l) -> bool:
    """Warp 9.5 Tune 3: Don't buy at the top of a green candle."""
    if len(c) < 2 or len(o) < 1:
        return True
    close = float(c[-1])
    open_p = float(o[-1])
    hi = float(h[-1])
    lo = float(l[-1])
    prev_close = float(c[-2])
    candle_range = hi - lo
    if candle_range <= 0:
        return True
    close_pos = (close - lo) / candle_range
    # Avoid buying at top of green candle
    if close_pos > 0.80 and close > prev_close:
        return False
    return True


def _true_range_atr(h, l, c, period=14) -> float:
    """Warp 9.5 Tune 5: Proper ATR using true range."""
    if len(h) < period + 1:
        return float(np.mean(np.abs(np.diff(c[-period:])))) if len(c) >= period else float(c[-1]) * 0.02
    tr_vals = []
    for i in range(-period, 0):
        tr1 = float(h[i]) - float(l[i])
        tr2 = abs(float(h[i]) - float(c[i - 1]))
        tr3 = abs(float(l[i]) - float(c[i - 1]))
        tr_vals.append(max(tr1, tr2, tr3))
    return sum(tr_vals) / len(tr_vals)


def _run_all_strategies(c, h, l, v, avg_v, spy_c):
    """Run 20 strategies, return list of triggered strategy names."""
    triggered = []
    checks = [
        ("breakout_vol", lambda: _check_breakout_volume(c, h, v, avg_v)),
        ("pullback_sma20", lambda: _check_pullback_sma20(c)),
        ("rsi_bounce", lambda: _check_rsi_bounce(c)),
        ("macd_cross", lambda: _check_macd_cross(c)),
        ("bb_bounce", lambda: _check_bollinger_bounce(c)),
        ("ema_ribbon", lambda: _check_ema_ribbon(c)),
        ("higher_hh", lambda: _check_higher_highs(h, l)),
        ("trend_resume", lambda: _check_trend_resumption(c)),
        ("vol_surge", lambda: _check_volume_surge(v, avg_v)),
        ("gap_fill", lambda: _check_gap_fill(c)),
        ("accumulation", lambda: _check_accumulation(c, v)),
        ("rs_high", lambda: _check_rs_high(c, spy_c)),
        ("rsi_divergence", lambda: _check_rsi_divergence(c, l)),
        ("vol_dryup", lambda: _check_volume_dryup(v, avg_v)),
        ("hammer_candle", lambda: _check_hammer_candle(c, h, l)),
        ("bull_bear_trap", lambda: _check_bull_bear_trap(c, h, l)),
        ("falling_knife",  lambda: _check_falling_knife(c, l)),
        ("avwap_bounce",   lambda: _check_avwap_bounce(c, v)),
        ("five_day_bounce",lambda: _check_five_day_bounce(c, l)),
        ("alpha_predator", lambda: _check_alpha_predator(c, v, avg_v)),
    ]
    for name, fn in checks:
        try:
            if fn():
                triggered.append(name)
        except Exception:
            pass
    return triggered


def _score_universe_stock(c, h, l, v):
    """Quick universe score (0-100) for ranking."""
    if len(c) < 20:
        return 0
    score = 0
    close = float(c[-1])
    avg_v = float(np.mean(v[-20:]))
    vol_ratio = float(v[-1]) / avg_v if avg_v > 0 else 0
    rsi = _rsi(c)

    if vol_ratio >= 2:
        score += 20
    if rsi < 30:
        score += 20
    elif rsi > 70:
        score += 5
    sma20 = float(np.mean(c[-20:]))
    sma50 = float(np.mean(c[-50:])) if len(c) >= 50 else sma20
    if close > sma20 and close > sma50:
        score += 15
    if len(c) >= 50:
        high_52 = float(np.max(h))
        if close > high_52 * 0.95:
            score += 10
        low_52 = float(np.min(l))
        if close < low_52 * 1.10:
            score += 10
    if vol_ratio >= 3:
        score += 10
    return score


# ── Main backtest ────────────────────────────────────────────────

def backtest_strategies(start_date: str = "2026-01-20",
                        end_date: str = "2026-03-21",
                        top_n: int = 50) -> dict:
    """Backtest the Warp 9 strategy engine over a date range.

    For each trading day:
      1. Score all tickers (universe scan) using data up to that day
      2. Run 14 strategies on top N tickers
      3. Open positions where 3+ strategies converge with 2:1 R/R
      4. Manage positions: trailing stops, fixed stops, target exits
    """
    import yfinance as yf

    console.log(f"[bold cyan]Strategy Engine Backtest: {start_date} → {end_date}")

    # ── Get tickers ──
    tickers = _get_tickers()
    console.log(f"[cyan]Loading {len(tickers)} tickers + SPY...")

    # ── Download all data at once (with lookback for indicators) ──
    dl_start = (datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=280)).strftime("%Y-%m-%d")
    dl_end = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=5)).strftime("%Y-%m-%d")
    all_tickers = list(set(tickers + ["SPY", "^VIX"]))

    console.log(f"[cyan]Downloading price data ({dl_start} → {dl_end})...")
    raw = yf.download(all_tickers, start=dl_start, end=dl_end,
                      group_by="ticker", threads=True, progress=False, auto_adjust=True)
    console.log(f"[green]Downloaded. Building per-ticker arrays...")

    # ── Build per-ticker DataFrames ──
    ticker_data = {}
    for t in all_tickers:
        try:
            if len(all_tickers) == 1:
                df = raw.dropna()
            else:
                df = raw[t].dropna()
            if len(df) >= 20:
                ticker_data[t] = df
        except Exception:
            pass

    console.log(f"[green]{len(ticker_data)} tickers with sufficient data")

    if "SPY" not in ticker_data:
        return {"error": "SPY data not available"}

    # Warp 9.6: Extract VIX data for adaptive thresholds
    vix_df = ticker_data.pop("^VIX", None)  # Remove from tradeable universe
    vix_closes = {}
    if vix_df is not None and len(vix_df) > 0:
        for idx, row in vix_df.iterrows():
            vix_closes[idx] = float(row["Close"])

    spy_df = ticker_data["SPY"]
    trading_dates = sorted(spy_df.index)

    # Filter to our backtest window
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    bt_dates = [d for d in trading_dates if start_dt <= d.to_pydatetime().replace(tzinfo=None) <= end_dt]

    console.log(f"[cyan]Simulating {len(bt_dates)} trading days...")

    # ── Simulation state ──
    cash = STARTING_CASH
    positions = []  # [{ticker, entry_date, entry_price, qty, stop, target, hwm, strategies}]
    closed_trades = []
    daily_equity = []
    signals_log = []

    for day_idx, day in enumerate(bt_dates):
        day_str = day.strftime("%Y-%m-%d")

        # ── 1. Update & close positions ──
        still_open = []
        for pos in positions:
            pdf = ticker_data.get(pos["ticker"])
            if pdf is None:
                still_open.append(pos)
                continue

            mask = pdf.index <= day
            if mask.sum() == 0:
                still_open.append(pos)
                continue

            today_price = float(pdf.loc[mask, "Close"].iloc[-1])
            today_high = float(pdf.loc[mask, "High"].iloc[-1])

            # Update high watermark
            if today_high > pos["hwm"]:
                pos["hwm"] = today_high

            gain_pct = (today_price - pos["entry_price"]) / pos["entry_price"]

            # Check target hit
            if today_price >= pos["target"]:
                pnl = (pos["target"] - pos["entry_price"]) * pos["qty"]
                closed_trades.append(_make_trade(pos, day_str, pos["target"], pnl, "TARGET"))
                cash += pos["qty"] * pos["target"]
                continue

            # Trailing stop
            if gain_pct > 0 and pos["hwm"] > pos["entry_price"]:
                trail = _trailing_stop_pct(gain_pct)
                trail_price = pos["hwm"] * (1 - trail)
                if gain_pct >= 0.05:
                    trail_price = max(trail_price, pos["entry_price"] * 0.98)
                if today_price <= trail_price:
                    pnl = (trail_price - pos["entry_price"]) * pos["qty"]
                    closed_trades.append(_make_trade(pos, day_str, trail_price, pnl, "TRAILING_STOP"))
                    cash += pos["qty"] * trail_price
                    continue

            # Fixed stop
            if today_price <= pos["stop"]:
                pnl = (pos["stop"] - pos["entry_price"]) * pos["qty"]
                closed_trades.append(_make_trade(pos, day_str, pos["stop"], pnl, "STOP_LOSS"))
                cash += pos["qty"] * pos["stop"]
                continue

            still_open.append(pos)
        positions = still_open

        # ── 2. Score universe & find convergence signals ──
        if len(positions) < MAX_POSITIONS:
            # Warp 9.6: Adaptive regime threshold
            spy_mask = spy_df.index <= day
            spy_c = spy_df.loc[spy_mask, "Close"].values

            # Get VIX for this day
            day_vix = vix_closes.get(day)
            if day_vix is None:
                # Find nearest prior VIX value
                for offset in range(1, 5):
                    for d in [day - timedelta(days=offset)]:
                        day_vix = vix_closes.get(d)
                        if day_vix:
                            break
                    if day_vix:
                        break
            if day_vix is None:
                day_vix = 18.0  # default calm

            # SPY vs 200-day MA
            spy_below_200 = False
            if len(spy_c) >= 200:
                sma200 = float(np.mean(spy_c[-200:]))
                spy_below_200 = float(spy_c[-1]) < sma200

            # Warp 9.7: Regime-switched mode selection
            # BEAR (VIX>25 or SPY<200MA): W9.0 rules — simple 3-strat, no filters, catch bounces
            # CAUTIOUS (VIX 20-25): W9.6 — adaptive threshold, weighted, ATR stops
            # BULL (VIX<20): W9.5 — strict weighted 4.0+, entry timing, DOW filter
            if day_vix > 25 or spy_below_200:
                warp_mode = "W9.0"
            elif day_vix > 20:
                warp_mode = "W9.6"
            else:
                warp_mode = "W9.5"

            held_tickers = set(p["ticker"] for p in positions)
            convergence_candidates = []

            for t, df in ticker_data.items():
                if t == "SPY" or t in held_tickers:
                    continue
                mask = df.index <= day
                if mask.sum() < 50:
                    continue
                subset = df.loc[mask]
                c = subset["Close"].values
                h = subset["High"].values
                l = subset["Low"].values
                v = subset["Volume"].values
                avg_v = float(np.mean(v[-20:])) if len(v) >= 20 else float(np.mean(v))

                triggered = _run_all_strategies(c, h, l, v, avg_v, spy_c)
                if len(triggered) < 3:
                    continue

                if warp_mode == "W9.0":
                    # ── BEAR: Simple 3-strategy count, no weighting/filters ──
                    wscore = float(len(triggered))  # raw count as score
                    atr = float(np.mean(np.abs(np.diff(c[-15:])))) if len(c) >= 15 else float(c[-1]) * 0.02

                elif warp_mode == "W9.6":
                    # ── CAUTIOUS: Weighted score, ATR stops, no entry timing ──
                    wscore = _weighted_score(triggered)
                    if wscore < CAUTION_THRESHOLD and len(triggered) < 4:
                        continue
                    atr = _true_range_atr(h, l, c)

                else:  # W9.5
                    # ── BULL: Full quality filter — weighted, entry timing, DOW ──
                    wscore = _weighted_score(triggered)
                    dow_mult = DOW_MULT.get(day.weekday(), 1.0)
                    wscore *= dow_mult
                    if wscore < BULL_THRESHOLD and len(triggered) < 4:
                        continue
                    # Entry timing filter
                    o = subset["Open"].values if "Open" in subset.columns else c
                    if not _is_good_entry(c, o, h, l):
                        continue
                    atr = _true_range_atr(h, l, c)

                convergence_candidates.append((t, triggered, float(c[-1]), atr, wscore))

            # Sort by score (highest first)
            convergence_candidates.sort(key=lambda x: x[4], reverse=True)

            for t, triggered, entry, atr, wscore in convergence_candidates[:top_n]:
                if len(positions) >= MAX_POSITIONS:
                    break
                if t in held_tickers:
                    continue

                # Stop/target: ATR-based for 9.5/9.6, simple for 9.0
                stop = round(entry - 2 * atr, 2)
                target = round(entry + 3 * atr, 2)

                risk = entry - stop
                reward = target - entry
                if risk <= 0:
                    continue

                rr = reward / risk
                if rr < MIN_RR:
                    continue

                size = cash * POSITION_PCT
                qty = size / entry
                cost = qty * entry
                if cost > cash * 0.80:  # keep 20% cash floor
                    continue
                if cost <= 0:
                    continue

                cash -= cost
                positions.append({
                    "ticker": t, "entry_date": day_str, "entry_price": entry,
                    "qty": qty, "stop": stop, "target": target, "hwm": entry,
                    "strategies": triggered, "wscore": wscore,
                })
                held_tickers.add(t)
                signals_log.append({
                    "date": day_str, "ticker": t,
                    "strategies": len(triggered), "names": triggered,
                    "entry": entry, "stop": stop, "target": target,
                    "wscore": round(wscore, 2),
                })

        # ── 3. Record daily equity ──
        pos_value = 0
        for pos in positions:
            pdf = ticker_data.get(pos["ticker"])
            if pdf is not None:
                mask = pdf.index <= day
                if mask.sum() > 0:
                    pos_value += pos["qty"] * float(pdf.loc[mask, "Close"].iloc[-1])
        daily_equity.append({"date": day_str, "value": round(cash + pos_value, 2)})

    # ── Close remaining positions at end ──
    for pos in positions:
        pdf = ticker_data.get(pos["ticker"])
        if pdf is not None:
            last_price = float(pdf["Close"].iloc[-1])
        else:
            last_price = pos["entry_price"]
        pnl = (last_price - pos["entry_price"]) * pos["qty"]
        closed_trades.append(_make_trade(pos, end_date, last_price, pnl, "END_OF_PERIOD"))
        cash += pos["qty"] * last_price

    # ── SPY buy-and-hold comparison ──
    spy_start = float(spy_df.loc[spy_df.index >= bt_dates[0]].iloc[0]["Close"])
    spy_end = float(spy_df.loc[spy_df.index <= bt_dates[-1]].iloc[-1]["Close"])
    spy_return = (spy_end - spy_start) / spy_start * 100

    # ── Stats ──
    wins = [t for t in closed_trades if t["pnl"] > 0]
    losses = [t for t in closed_trades if t["pnl"] <= 0]
    total_pnl = sum(t["pnl"] for t in closed_trades)
    final_value = STARTING_CASH + total_pnl

    stats = {
        "period": f"{start_date} → {end_date}",
        "trading_days": len(bt_dates),
        "tickers_scanned": len(ticker_data),
        "total_trades": len(closed_trades),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(len(wins) / len(closed_trades) * 100, 1) if closed_trades else 0,
        "total_pnl": round(total_pnl, 2),
        "total_return_pct": round(total_pnl / STARTING_CASH * 100, 2),
        "final_value": round(final_value, 2),
        "best_trade": round(max(t["pnl"] for t in closed_trades), 2) if closed_trades else 0,
        "worst_trade": round(min(t["pnl"] for t in closed_trades), 2) if closed_trades else 0,
        "avg_win": round(sum(t["pnl"] for t in wins) / len(wins), 2) if wins else 0,
        "avg_loss": round(sum(t["pnl"] for t in losses) / len(losses), 2) if losses else 0,
        "spy_return_pct": round(spy_return, 2),
        "alpha": round(total_pnl / STARTING_CASH * 100 - spy_return, 2),
        "signals_generated": len(signals_log),
        "exit_types": {
            "target": len([t for t in closed_trades if t["exit_type"] == "TARGET"]),
            "trailing_stop": len([t for t in closed_trades if t["exit_type"] == "TRAILING_STOP"]),
            "stop_loss": len([t for t in closed_trades if t["exit_type"] == "STOP_LOSS"]),
            "end_of_period": len([t for t in closed_trades if t["exit_type"] == "END_OF_PERIOD"]),
        },
    }

    return {
        "stats": stats,
        "trades": sorted(closed_trades, key=lambda t: t["pnl"], reverse=True),
        "equity_curve": daily_equity,
        "signals": signals_log,
    }


def _make_trade(pos, exit_date, exit_price, pnl, exit_type):
    return {
        "ticker": pos["ticker"],
        "entry_date": pos["entry_date"],
        "exit_date": exit_date,
        "entry_price": round(pos["entry_price"], 2),
        "exit_price": round(exit_price, 2),
        "qty": round(pos["qty"], 4),
        "pnl": round(pnl, 2),
        "pnl_pct": round((exit_price - pos["entry_price"]) / pos["entry_price"] * 100, 2),
        "strategies": pos["strategies"],
        "num_strategies": len(pos["strategies"]),
        "exit_type": exit_type,
    }


def _get_tickers():
    """Get S&P 500 + extras."""
    try:
        import pandas as pd
        import requests
        resp = requests.get(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            headers={"User-Agent": "Mozilla/5.0 TradeMinds/1.0"}, timeout=15,
        )
        resp.raise_for_status()
        tables = pd.read_html(resp.text, attrs={"id": "constituents"})
        if tables:
            tickers = tables[0]["Symbol"].tolist()
            tickers = [t.replace(".", "-") for t in tickers]
            extras = ["CRWD", "SMCI", "MRVL", "ANET", "DDOG", "NET", "COIN",
                       "SOFI", "HOOD", "ARM", "RKLB", "HIMS", "MSTR", "CELH"]
            return list(set(tickers + extras))
    except Exception:
        pass

    # Fallback
    return [
        "AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA", "BRK-B", "AVGO", "JPM",
        "LLY", "V", "UNH", "MA", "XOM", "COST", "HD", "PG", "JNJ", "ABBV",
        "WMT", "NFLX", "BAC", "CRM", "AMD", "CVX", "KO", "MRK", "PEP", "TMO",
        "ACN", "LIN", "MCD", "CSCO", "ADBE", "ABT", "WFC", "DHR", "TXN", "PM",
        "MS", "NEE", "QCOM", "ISRG", "INTU", "GE", "AMGN", "AMAT", "NOW", "IBM",
        "GS", "CAT", "PFE", "RTX", "BLK", "BKNG", "T", "LOW", "UBER", "UNP",
        "SPGI", "SYK", "VRTX", "ADP", "SCHW", "BSX", "GILD", "MMC", "LRCX", "MDT",
        "CB", "TMUS", "DE", "PLD", "ADI", "FI", "MO", "PANW", "SO", "ICE",
        "CI", "DUK", "CL", "EQIX", "PYPL", "CME", "SNPS", "CDNS", "MU", "MCK",
        "SHW", "ZTS", "HCA", "NOC", "CMG", "ORLY", "WM", "APH", "USB", "PNC",
        "DELL", "ORCL", "PLTR", "INTC", "F", "GM", "CRWD", "SMCI", "COIN", "SOFI",
        "ARM", "HIMS", "MSTR", "DDOG", "NET", "HOOD", "RKLB", "CELH",
    ]
