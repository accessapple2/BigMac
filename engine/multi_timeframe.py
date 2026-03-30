"""Multi-Timeframe Analysis — analyze 5min, 1hr, daily before each trade.
Only trade when all 3 timeframes align directionally.
"""
from __future__ import annotations
import threading
import time
from engine.market_data import _is_yf_limited, _set_yf_limited
import pandas as pd
import numpy as np
from rich.console import Console

console = Console()

_mtf_cache: dict[str, dict] = {}
_mtf_lock = threading.Lock()
_CACHE_TTL = 120  # 2 minutes


def _calc_rsi(series: pd.Series, period: int = 14) -> float:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return round(float(rsi.iloc[-1]), 2) if not rsi.empty and pd.notna(rsi.iloc[-1]) else 50.0


def _analyze_timeframe(symbol: str, period: str, interval: str) -> dict:
    """Analyze a single timeframe and return signal/bias."""
    if _is_yf_limited():
        return {"signal": "neutral", "score": 0, "details": "Rate limited"}
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period=period, interval=interval)
        if hist.empty or len(hist) < 10:
            return {"signal": "neutral", "score": 0, "details": "Insufficient data"}

        close = hist["Close"]
        current = float(close.iloc[-1])

        # RSI
        rsi = _calc_rsi(close) if len(close) >= 14 else 50.0

        # Simple trend: compare to SMA(20) and price direction
        sma20 = float(close.rolling(min(20, len(close))).mean().iloc[-1]) if len(close) >= 5 else current
        above_sma = current > sma20

        # MACD histogram direction
        macd_bullish = False
        if len(close) >= 26:
            ema12 = close.ewm(span=12, adjust=False).mean()
            ema26 = close.ewm(span=26, adjust=False).mean()
            hist_val = float((ema12 - ema26).iloc[-1] - (ema12 - ema26).ewm(span=9, adjust=False).mean().iloc[-1])
            macd_bullish = hist_val > 0

        # Price momentum: last 3 bars
        if len(close) >= 4:
            momentum = float((close.iloc[-1] / close.iloc[-4] - 1) * 100)
        else:
            momentum = 0

        # Scoring
        score = 0
        details = []

        if rsi < 30:
            score += 2
            details.append(f"RSI oversold ({rsi})")
        elif rsi < 45:
            score += 1
            details.append(f"RSI low ({rsi})")
        elif rsi > 70:
            score -= 2
            details.append(f"RSI overbought ({rsi})")
        elif rsi > 55:
            score -= 1
            details.append(f"RSI high ({rsi})")

        if above_sma:
            score += 1
            details.append("Above SMA20")
        else:
            score -= 1
            details.append("Below SMA20")

        if macd_bullish:
            score += 1
            details.append("MACD bullish")
        else:
            score -= 1
            details.append("MACD bearish")

        if momentum > 0.5:
            score += 1
            details.append(f"Momentum +{momentum:.1f}%")
        elif momentum < -0.5:
            score -= 1
            details.append(f"Momentum {momentum:.1f}%")

        if score >= 2:
            signal = "bullish"
        elif score <= -2:
            signal = "bearish"
        else:
            signal = "neutral"

        return {
            "signal": signal,
            "score": score,
            "rsi": rsi,
            "above_sma20": above_sma,
            "macd_bullish": macd_bullish,
            "momentum": round(momentum, 2),
            "price": round(current, 2),
            "details": ", ".join(details),
        }
    except Exception as e:
        err = str(e)
        if "Too Many Requests" in err or "Rate" in err:
            _set_yf_limited()
        return {"signal": "neutral", "score": 0, "details": f"Error: {e}"}


def get_multi_timeframe(symbol: str) -> dict:
    """Analyze 3 timeframes for a symbol.

    Returns {symbol, timeframes: {5min, 1hr, daily}, confluence, aligned, direction}.
    """
    now = time.time()
    with _mtf_lock:
        cached = _mtf_cache.get(symbol)
        if cached and (now - cached.get("_ts", 0)) < _CACHE_TTL:
            return {k: v for k, v in cached.items() if k != "_ts"}

    tf_5min = _analyze_timeframe(symbol, "1d", "5m")
    tf_1hr = _analyze_timeframe(symbol, "5d", "1h")
    tf_daily = _analyze_timeframe(symbol, "3mo", "1d")

    timeframes = {
        "5min": tf_5min,
        "1hr": tf_1hr,
        "daily": tf_daily,
    }

    # Confluence scoring
    signals = [tf_5min["signal"], tf_1hr["signal"], tf_daily["signal"]]
    scores = [tf_5min["score"], tf_1hr["score"], tf_daily["score"]]

    bullish_count = signals.count("bullish")
    bearish_count = signals.count("bearish")

    if bullish_count == 3:
        confluence = "STRONG_BULLISH"
        direction = "bullish"
        aligned = True
    elif bearish_count == 3:
        confluence = "STRONG_BEARISH"
        direction = "bearish"
        aligned = True
    elif bullish_count >= 2:
        confluence = "LEAN_BULLISH"
        direction = "bullish"
        aligned = False
    elif bearish_count >= 2:
        confluence = "LEAN_BEARISH"
        direction = "bearish"
        aligned = False
    else:
        confluence = "MIXED"
        direction = "neutral"
        aligned = False

    # Weighted confluence score: daily=3x, 1hr=2x, 5min=1x
    weighted_score = scores[0] + scores[1] * 2 + scores[2] * 3

    result = {
        "symbol": symbol,
        "timeframes": timeframes,
        "confluence": confluence,
        "aligned": aligned,
        "direction": direction,
        "weighted_score": weighted_score,
    }

    with _mtf_lock:
        _mtf_cache[symbol] = {**result, "_ts": now}

    return result


def build_mtf_prompt_section(symbol: str) -> str:
    """Build text block for injection into AI prompts."""
    mtf = get_multi_timeframe(symbol)
    if not mtf or not mtf.get("timeframes"):
        return ""

    lines = [f"=== MULTI-TIMEFRAME ANALYSIS for {symbol} ==="]

    for label, key in [("5-Min", "5min"), ("1-Hour", "1hr"), ("Daily", "daily")]:
        tf = mtf["timeframes"][key]
        sig = tf["signal"].upper()
        icon = "+" if tf["score"] > 0 else "-" if tf["score"] < 0 else "~"
        lines.append(f"  {label}: {sig} (score: {icon}{abs(tf['score'])}) — {tf['details']}")

    lines.append(f"  CONFLUENCE: {mtf['confluence']} (weighted: {mtf['weighted_score']:+d})")

    if mtf["aligned"]:
        lines.append(f"  *** ALL 3 TIMEFRAMES ALIGNED {mtf['direction'].upper()} — HIGH-PROBABILITY SETUP ***")
    else:
        lines.append(f"  Timeframes NOT aligned — reduce position size or wait for confirmation.")

    return "\n".join(lines)
