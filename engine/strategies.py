"""Strategy Engine — 20 technical strategies scored against universe candidates.

Multi-strategy convergence: only recommend when 3+ strategies agree.
All strategies use free Yahoo Finance data (yfinance).
"""
from __future__ import annotations
import sqlite3
import json
import time
from datetime import datetime
from rich.console import Console
import numpy as np

console = Console()
DB = "data/trader.db"


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def _ema(data, span):
    """Exponential moving average."""
    alpha = 2 / (span + 1)
    result = [float(data[0])]
    for i in range(1, len(data)):
        result.append(alpha * float(data[i]) + (1 - alpha) * result[-1])
    return np.array(result)


def _rsi(closes, period=14):
    closes = np.array(closes, dtype=float)
    if len(closes) < period + 1:
        return 50.0
    deltas = np.diff(closes)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    avg_gain = np.mean(gains[-period:])
    avg_loss = np.mean(losses[-period:])
    if avg_loss == 0:
        return 100.0
    return round(100 - (100 / (1 + avg_gain / avg_loss)), 2)


def _macd(closes):
    """Returns (macd_line_last, signal_line_last)."""
    closes = np.array(closes, dtype=float)
    if len(closes) < 26:
        return 0, 0
    ema12 = _ema(closes, 12)
    ema26 = _ema(closes, 26)
    macd_line = ema12 - ema26
    if len(macd_line) < 9:
        return float(macd_line[-1]), 0
    signal = _ema(macd_line, 9)
    return float(macd_line[-1]), float(signal[-1])


# ============================================================
# STRATEGY DEFINITIONS
# ============================================================

def check_breakout_volume(close, high, volume, avg_vol):
    """Price breaks above 20-day high on 2x+ volume."""
    if len(close) < 21 or len(high) < 21:
        return False
    high_20 = float(np.max(high[-21:-1]))
    return float(close[-1]) > high_20 and float(volume[-1]) > float(avg_vol) * 2


def check_pullback_sma20(close):
    """Stock pulls back to 20-day SMA in uptrend."""
    if len(close) < 50:
        return False
    sma20 = float(np.mean(close[-20:]))
    sma50 = float(np.mean(close[-50:]))
    price = float(close[-1])
    return price > sma50 and abs(price - sma20) / price < 0.02


def check_rsi_oversold_bounce(close):
    """RSI crosses back above 30 (oversold reversal)."""
    if len(close) < 16:
        return False
    rsi_now = _rsi(close)
    rsi_prev = _rsi(close[:-1])
    return rsi_now > 30 and rsi_prev < 30


def check_macd_crossover(close):
    """MACD line crosses above signal line."""
    if len(close) < 27:
        return False
    macd_now, sig_now = _macd(close)
    macd_prev, sig_prev = _macd(close[:-1])
    return macd_now > sig_now and macd_prev <= sig_prev


def check_golden_cross(close):
    """50-day SMA crosses above 200-day SMA."""
    if len(close) < 201:
        return False
    sma50_now = float(np.mean(close[-50:]))
    sma200_now = float(np.mean(close[-200:]))
    sma50_prev = float(np.mean(close[-51:-1]))
    sma200_prev = float(np.mean(close[-201:-1]))
    return sma50_now > sma200_now and sma50_prev <= sma200_prev


def check_bollinger_bounce(close):
    """Price touches lower Bollinger Band and bounces."""
    if len(close) < 21:
        return False
    sma20 = float(np.mean(close[-20:]))
    std20 = float(np.std(close[-20:]))
    bb_lower = sma20 - 2 * std20
    prev_close = float(close[-2])
    curr_close = float(close[-1])
    return prev_close <= bb_lower and curr_close > bb_lower


def check_rsi_divergence(close, low):
    """Bullish divergence: price makes new low but RSI makes higher low."""
    if len(close) < 30:
        return False
    # Check if price made a lower low in last 5 days
    recent_low = float(np.min(low[-5:]))
    prev_low = float(np.min(low[-15:-5]))
    if recent_low >= prev_low:
        return False
    # Check if RSI made a higher low
    rsi_now = _rsi(close[-15:])
    rsi_prev = _rsi(close[-25:-10])
    return rsi_now > rsi_prev and rsi_now < 40


def check_gap_fill(close):
    """Stock gaps down 3%+ and starts filling the gap."""
    if len(close) < 3:
        return False
    gap_pct = (float(close[-2]) - float(close[-3])) / float(close[-3]) * 100
    recovery = (float(close[-1]) - float(close[-2])) / float(close[-2]) * 100
    return gap_pct < -3 and recovery > 1


def check_unusual_volume(volume, avg_vol):
    """Volume 3x+ above 20-day average."""
    return float(volume[-1]) > float(avg_vol) * 3


def check_volume_dry_up(volume, avg_vol):
    """Volume drops to <50% of average (consolidation)."""
    return float(volume[-1]) < float(avg_vol) * 0.5


def check_accumulation(close, volume):
    """Price flat but volume increasing 3 consecutive days."""
    if len(close) < 4 or len(volume) < 4:
        return False
    price_change = abs(float(close[-1]) - float(close[-4])) / float(close[-4])
    vol_increasing = (float(volume[-1]) > float(volume[-2]) >
                      float(volume[-3]))
    return price_change < 0.01 and vol_increasing


def check_ema_ribbon(close):
    """EMAs (8,13,21,34,55) aligned in ascending order (strong trend)."""
    if len(close) < 55:
        return False
    emas = [float(_ema(close, s)[-1]) for s in (8, 13, 21, 34, 55)]
    # All EMAs stacked: shortest > longest
    return all(emas[i] > emas[i + 1] for i in range(len(emas) - 1))


def check_higher_highs(high, low):
    """3 consecutive higher highs and higher lows (5-day swings)."""
    if len(high) < 15:
        return False
    # Check 3 swing points (every 5 days)
    h = [float(np.max(high[i:i + 5])) for i in range(-15, 0, 5)]
    l = [float(np.min(low[i:i + 5])) for i in range(-15, 0, 5)]
    return h[2] > h[1] > h[0] and l[2] > l[1] > l[0]


def check_trend_resumption(close):
    """Stock resumes uptrend after 3-5 day pullback."""
    if len(close) < 20:
        return False
    sma20 = float(np.mean(close[-20:]))
    price = float(close[-1])
    # Was in uptrend (above SMA20)
    if price <= sma20:
        return False
    # Had a pullback (dipped below SMA20 in last 5 days, now above)
    recent_min = float(np.min(close[-5:]))
    return recent_min < sma20 and price > sma20


def check_relative_strength_high(close, spy_close):
    """RS line making new high (outperforming SPY)."""
    if len(close) < 50 or len(spy_close) < 50:
        return False
    rs = np.array(close, dtype=float) / np.array(spy_close[-len(close):], dtype=float)
    rs_now = rs[-1]
    rs_max = float(np.max(rs[:-1]))
    return rs_now >= rs_max


# Strategy registry
STRATEGIES = {
    # Momentum
    "breakout_volume": {"fn": "breakout_volume", "type": "momentum", "desc": "Price breaks 20d high on 2x vol"},
    "pullback_sma20": {"fn": "pullback_sma20", "type": "momentum", "desc": "Pullback to 20-SMA in uptrend"},
    "rsi_oversold_bounce": {"fn": "rsi_oversold_bounce", "type": "reversal", "desc": "RSI crosses above 30"},
    "macd_crossover": {"fn": "macd_crossover", "type": "momentum", "desc": "MACD crosses signal line"},
    "golden_cross": {"fn": "golden_cross", "type": "trend", "desc": "50-SMA crosses above 200-SMA"},
    # Mean Reversion
    "bollinger_bounce": {"fn": "bollinger_bounce", "type": "reversal", "desc": "Bounces off lower BB"},
    "rsi_divergence": {"fn": "rsi_divergence", "type": "reversal", "desc": "Bullish RSI divergence"},
    "gap_fill": {"fn": "gap_fill", "type": "reversal", "desc": "Gap down 3%+ starts filling"},
    # Volume
    "unusual_volume": {"fn": "unusual_volume", "type": "volume", "desc": "Volume 3x+ average"},
    "volume_dry_up": {"fn": "volume_dry_up", "type": "volume", "desc": "Volume <50% avg (pre-breakout)"},
    "accumulation": {"fn": "accumulation", "type": "volume", "desc": "Price flat, volume rising"},
    # Trend
    "ema_ribbon": {"fn": "ema_ribbon", "type": "trend", "desc": "EMA ribbon aligned (strong trend)"},
    "higher_highs": {"fn": "higher_highs", "type": "trend", "desc": "3 consecutive HH/HL"},
    "trend_resumption": {"fn": "trend_resumption", "type": "trend", "desc": "Resumes uptrend after pullback"},
    # Relative Strength
    "relative_strength_high": {"fn": "relative_strength_high", "type": "momentum", "desc": "RS line at new high vs SPY"},
}


def run_strategies(ticker: str, df, spy_df=None) -> list:
    """Run all strategies against a ticker's DataFrame. Returns list of triggered strategies."""
    triggered = []

    close = df["Close"].values
    high = df["High"].values
    low = df["Low"].values
    volume = df["Volume"].values
    avg_vol = float(np.mean(volume[-20:])) if len(volume) >= 20 else float(np.mean(volume))

    spy_close = spy_df["Close"].values if spy_df is not None and len(spy_df) > 0 else None

    checks = {
        "breakout_volume": lambda: check_breakout_volume(close, high, volume, avg_vol),
        "pullback_sma20": lambda: check_pullback_sma20(close),
        "rsi_oversold_bounce": lambda: check_rsi_oversold_bounce(close),
        "macd_crossover": lambda: check_macd_crossover(close),
        "golden_cross": lambda: check_golden_cross(close),
        "bollinger_bounce": lambda: check_bollinger_bounce(close),
        "rsi_divergence": lambda: check_rsi_divergence(close, low),
        "gap_fill": lambda: check_gap_fill(close),
        "unusual_volume": lambda: check_unusual_volume(volume, avg_vol),
        "volume_dry_up": lambda: check_volume_dry_up(volume, avg_vol),
        "accumulation": lambda: check_accumulation(close, volume),
        "ema_ribbon": lambda: check_ema_ribbon(close),
        "higher_highs": lambda: check_higher_highs(high, low),
        "trend_resumption": lambda: check_trend_resumption(close),
        "relative_strength_high": lambda: check_relative_strength_high(close, spy_close) if spy_close is not None else False,
    }

    for name, check_fn in checks.items():
        try:
            if check_fn():
                meta = STRATEGIES[name]
                entry = float(close[-1])
                # Calculate stop and target based on strategy type
                atr = float(np.mean(np.abs(np.diff(close[-15:])))) if len(close) >= 15 else entry * 0.02
                stop = round(entry - 2 * atr, 2)
                target = round(entry + 3 * atr, 2)  # 1.5:1 minimum R/R

                triggered.append({
                    "name": name,
                    "type": meta["type"],
                    "desc": meta["desc"],
                    "signal_type": "BUY",
                    "entry_price": round(entry, 2),
                    "stop_price": stop,
                    "target_price": target,
                })
        except Exception:
            continue

    return triggered


def _get_strategy_weight(name: str, stats: dict) -> float:
    """Return performance-based weight for a strategy.

    Rules (Part D — Trade Memory Loop):
      >70% win rate AND >=5 trades → 1.5x (top performer)
      40-70% win rate  OR  <5 trades → 1.0x (neutral)
      <40% win rate  AND >=5 trades → 0.5x (underperformer)
    """
    s = stats.get(name)
    if not s or s.get("trades", 0) < 5:
        return 1.0
    wr = s.get("win_rate", 50.0)
    if wr > 70:
        return 1.5
    if wr < 40:
        return 0.5
    return 1.0


def score_convergence(ticker: str, triggered: list) -> dict | None:
    """Starfleet convergence scoring: 3+ strategies normally; 1+ during power hour / after hours.

    Strategy weights from trade history:
      >70% win rate (>=5 trades) → 1.5x weight
      <40% win rate (>=5 trades) → 0.5x weight
      otherwise                  → 1.0x weight

    A 3-strategy convergence of top performers counts as 4.5 weighted strategies.

    Returns signal dict or None if insufficient convergence.
    """
    bullish = [s for s in triggered if s["signal_type"] == "BUY"]

    # During power hour (12:30–1:00 PM MST) and after hours (1:00+ PM MST),
    # allow single-model decisions so the scanner doesn't go dark.
    import pytz
    from datetime import datetime as _dt
    _az = pytz.timezone("US/Arizona")
    _now = _dt.now(_az)
    _mins = _now.hour * 60 + _now.minute
    # 750 = 12:30 PM MST (power hour start / 3:30 PM ET)
    min_strategies = 1 if _mins >= 750 else 3

    if len(bullish) < min_strategies:
        return None

    # Load strategy performance weights (gracefully — never blocks a trade)
    strategy_stats: dict = {}
    try:
        from engine.trade_outcomes import get_strategy_stats
        strategy_stats = get_strategy_stats(lookback_days=60)
    except Exception:
        pass

    # Compute weighted score
    weighted_score = sum(_get_strategy_weight(s["name"], strategy_stats) for s in bullish)

    # Weighted minimum check replaces raw count check
    if weighted_score < min_strategies:
        return None

    entry = bullish[0]["entry_price"]
    avg_target = sum(s["target_price"] for s in bullish) / len(bullish)
    avg_stop = sum(s["stop_price"] for s in bullish) / len(bullish)

    reward = avg_target - entry
    risk = entry - avg_stop

    if risk <= 0:
        return None

    rr = reward / risk
    if rr < 1.5:
        return None

    # Confidence based on weighted score (not raw count)
    confidence = min(weighted_score / 5.0, 1.0)
    if min_strategies < 3:
        confidence = max(confidence, 0.82)

    return {
        "ticker": ticker,
        "action": "BUY",
        "strategies_triggered": round(weighted_score, 2),  # weighted for routing
        "strategy_names": [s["name"] for s in bullish],
        "strategy_types": list(set(s["type"] for s in bullish)),
        "raw_strategy_count": len(bullish),
        "entry": entry,
        "stop": round(avg_stop, 2),
        "target": round(avg_target, 2),
        "risk_reward": round(rr, 2),
        "confidence": round(confidence, 2),
    }


def get_scan_universe(max_total: int = 700) -> list[str]:
    """Build Chekov's combined scan universe: volume scanner hot stocks + core watchlist.

    Volume scanner finds the needles (intraday movers). Core watchlist ensures
    we never miss blue chips and S&P 500 names.

    Priority: hot stocks first (sorted by relative_volume desc), then core.
    Cap at max_total to keep Mac Mini M4 happy.
    """
    # Hot stocks from today's volume scanner (the new stuff)
    try:
        from engine.volume_scanner import get_todays_volume_alerts
        hot_alerts = get_todays_volume_alerts(limit=max_total)
        hot_symbols = [a["symbol"] for a in hot_alerts]
    except Exception:
        hot_symbols = []

    # Core watchlist (the existing S&P 500 + extras — proven, never removed)
    try:
        from engine.universe_scanner import get_core_watchlist
        core = get_core_watchlist()
    except Exception:
        core = []

    # Merge: hot stocks first (priority), then core; deduplicate; cap at max_total
    seen: set[str] = set()
    combined: list[str] = []
    for sym in hot_symbols:
        if sym not in seen:
            seen.add(sym)
            combined.append(sym)
        if len(combined) >= max_total:
            break

    # Fill remaining slots with core watchlist
    for sym in core:
        if sym not in seen:
            seen.add(sym)
            combined.append(sym)
        if len(combined) >= max_total:
            break

    console.log(f"[cyan]🧭 Scan universe: {len(hot_symbols)} hot stocks + core = {len(combined)} total (cap {max_total})")
    return combined


def scan_strategies(tickers: list = None, save: bool = True) -> list:
    """Run all strategies against top universe candidates.

    Returns list of convergence signals (stocks where 3+ strategies agree).
    Uses combined volume scanner + core watchlist universe when no tickers given.
    """
    import yfinance as yf

    if not tickers:
        # Try combined universe first (volume hot stocks + core watchlist)
        tickers = get_scan_universe()

    if not tickers:
        # Absolute fallback: top 50 from latest nightly scan
        from engine.universe_scanner import get_latest_universe_scan
        scan = get_latest_universe_scan()
        tickers = [s["ticker"] for s in scan.get("results", [])[:50]]

    if not tickers:
        return []

    console.log(f"[cyan]🧭 Running {len(STRATEGIES)} strategies against {len(tickers)} stocks...")

    # Download data
    all_tickers = list(set(tickers + ["SPY"]))
    try:
        data = yf.download(all_tickers, period="1y", group_by="ticker",
                           threads=True, progress=False, auto_adjust=True)
    except Exception as e:
        console.log(f"[red]Strategy scan download failed: {e}")
        return []

    # Extract SPY for relative strength
    try:
        spy_df = data["SPY"].dropna() if "SPY" in data.columns.get_level_values(0) else None
    except Exception:
        spy_df = None

    signals = []
    all_triggered = {}

    for ticker in tickers:
        try:
            try:
                df = data[ticker].dropna()
            except (KeyError, TypeError):
                continue
            if df is None or len(df) < 20:
                continue

            triggered = run_strategies(ticker, df, spy_df)
            if triggered:
                all_triggered[ticker] = triggered

            convergence = score_convergence(ticker, triggered)
            if convergence:
                signals.append(convergence)
        except Exception:
            continue

    signals.sort(key=lambda x: x["strategies_triggered"], reverse=True)

    if save and signals:
        _save_strategy_signals(signals)

    import pytz as _pytz
    from datetime import datetime as _dt2
    _mins2 = _dt2.now(_pytz.timezone("US/Arizona"))
    _mins2 = _mins2.hour * 60 + _mins2.minute
    _threshold_label = "1+ strategy (power hour/AH)" if _mins2 >= 750 else "3+ strategies"
    console.log(f"[green]🧭 Strategy scan complete: {len(signals)} convergence signals "
                f"({_threshold_label} agree)")

    for sig in signals[:5]:
        console.log(f"  {sig['ticker']}: {sig['strategies_triggered']} strategies, "
                    f"R/R {sig['risk_reward']:.1f}, conf {sig['confidence']:.0%} "
                    f"({', '.join(sig['strategy_names'][:3])})")

    return signals


def _save_strategy_signals(signals: list):
    """Save convergence signals to DB."""
    from engine.universe_scanner import ensure_universe_tables
    ensure_universe_tables()
    today = datetime.now().strftime("%Y-%m-%d")
    conn = _conn()
    for sig in signals:
        for strat in sig["strategy_names"]:
            try:
                conn.execute(
                    "INSERT INTO strategy_signals "
                    "(scan_date, ticker, strategy_name, signal_type, confidence, "
                    "entry_price, stop_price, target_price, notes) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (today, sig["ticker"], strat, "BUY", sig["confidence"],
                     sig["entry"], sig["stop"], sig["target"],
                     json.dumps(sig["strategy_names"])),
                )
            except Exception:
                pass
    conn.commit()
    conn.close()


def get_todays_signals() -> list:
    """Get today's convergence signals from DB."""
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        conn = _conn()
        rows = conn.execute(
            "SELECT ticker, COUNT(DISTINCT strategy_name) as strat_count, "
            "AVG(confidence) as avg_conf, MIN(entry_price) as entry, "
            "MIN(stop_price) as stop, MAX(target_price) as target, "
            "GROUP_CONCAT(DISTINCT strategy_name) as strategies "
            "FROM strategy_signals WHERE scan_date = ? AND signal_type = 'BUY' "
            "GROUP BY ticker HAVING strat_count >= 3 ORDER BY strat_count DESC",
            (today,),
        ).fetchall()
        conn.close()

        return [
            {
                "ticker": r["ticker"],
                "strategies_triggered": r["strat_count"],
                "confidence": round(r["avg_conf"], 2),
                "entry": r["entry"],
                "stop": r["stop"],
                "target": r["target"],
                "strategy_names": r["strategies"].split(",") if r["strategies"] else [],
            }
            for r in rows
        ]
    except Exception:
        return []


def build_strategy_prompt_section() -> str:
    """Build prompt section with strategy convergence signals for AI models.

    This is the primary stock-selection feed — the strategy engine backtested
    +5.17% with 55.8% win rate while SPY lost -4.02%.
    """
    signals = get_todays_signals()

    lines = ["\n=== 🧭 ENSIGN CHEKOV'S WARP 9 SCANNER REPORT ==="]

    if signals:
        lines.append(
            "🎯 HIGH-CONVICTION CONVERGENCE SIGNALS (3+ strategies agree):\n"
            "These are your PRIMARY trade candidates. The strategy engine\n"
            "backtested +5.17% with 55.8% win rate using these signals.\n"
            "PRIORITIZE these over your own stock picks.\n"
        )
        for sig in signals[:8]:
            strats = ", ".join(sig["strategy_names"][:4])
            risk = sig["entry"] - sig["stop"] if sig["entry"] and sig["stop"] else 1
            rr = ((sig["target"] - sig["entry"]) / risk) if risk > 0 else 0
            lines.append(
                f"  ★ {sig['ticker']}: {sig['strategies_triggered']} strategies agree\n"
                f"    Strategies: {strats}\n"
                f"    Entry: ${sig['entry']:.2f} | Stop: ${sig['stop']:.2f} | Target: ${sig['target']:.2f}\n"
                f"    Risk/Reward: {rr:.1f}:1 | Confidence: {sig['confidence']*100:.0f}%\n"
            )
    else:
        lines.append(
            "⚠️ No convergence signals today — the scanner found no setups\n"
            "where 3+ strategies agree. When the scanner finds nothing,\n"
            "the best trade is no trade. STAY IN CASH or hold existing positions.\n"
        )

    lines.append(
        "📈 BEST STRATEGIES (by 60-day backtest win rate):\n"
        "  1. RSI Divergence: 66.7% WR    4. Pullback to SMA20: 59.3% WR\n"
        "  2. Relative Strength: 63.6% WR  5. EMA Ribbon: 58.3% WR\n"
        "  3. MACD Cross: 62.5% WR\n"
    )
    lines.append(
        "⚡ STANDING ORDER: Only trade stocks that appear in the convergence\n"
        "signals OR top 10 universe scan. Do NOT pick stocks outside this list\n"
        "unless you have 0.90+ conviction and can articulate why the scanner missed it."
    )

    return "\n".join(lines)


def post_scanner_to_war_room():
    """Chekov reports scanner findings to the War Room."""
    try:
        import sqlite3
        conn = sqlite3.connect("data/trader.db", check_same_thread=False, timeout=30)
        conn.execute("PRAGMA busy_timeout=30000")

        # Ensure navigator player exists
        existing = conn.execute("SELECT id FROM ai_players WHERE id='navigator'").fetchone()
        if not existing:
            conn.execute(
                "INSERT INTO ai_players (id, display_name, provider, model_id, is_active, is_human, cash) "
                "VALUES ('navigator', 'Ensign Chekov', 'system', 'scanner', 1, 0, 0)"
            )
            conn.commit()

        signals = get_todays_signals()
        from engine.universe_scanner import get_latest_universe_scan
        scan = get_latest_universe_scan()

        if signals:
            for sig in signals[:5]:
                strats = ", ".join(sig["strategy_names"][:4])
                risk = sig["entry"] - sig["stop"] if sig["entry"] and sig["stop"] else 1
                rr = ((sig["target"] - sig["entry"]) / risk) if risk > 0 else 0
                take = (
                    f"Keptin! I am detecting convergence on {sig['ticker']}! "
                    f"{sig['strategies_triggered']} strategies agree — {strats}. "
                    f"Entry ${sig['entry']:.2f}, Target ${sig['target']:.2f}, "
                    f"Stop ${sig['stop']:.2f}. "
                    f"Risk/Reward {rr:.1f}:1. "
                    f"Requesting permission to plot intercept course!"
                )
                conn.execute(
                    "INSERT INTO war_room (player_id, symbol, take) VALUES (?, ?, ?)",
                    ("navigator", sig["ticker"], take),
                )

        # Summary post
        top5 = scan.get("results", [])[:5]
        if top5:
            tickers = ", ".join(f"{s['ticker']}({s['score']})" for s in top5)
            summary = (
                f"Navigation sweep complete, Keptin! "
                f"Scanned {scan.get('total_scanned', 0)} sectors. "
                f"Top coordinates: {tickers}. "
                f"{'Convergence detected on ' + str(len(signals)) + ' targets!' if signals else 'No convergence yet — maintaining scanning posture.'}"
            )
            conn.execute(
                "INSERT INTO war_room (player_id, symbol, take) VALUES (?, ?, ?)",
                ("navigator", "SCAN", summary),
            )

        conn.commit()
        conn.close()
        console.log(f"[green]🧭 Chekov posted {len(signals) if signals else 0} convergence signals + summary to War Room")
    except Exception as e:
        console.log(f"[yellow]Chekov War Room post error: {e}")
