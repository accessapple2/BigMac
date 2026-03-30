"""Trend Prediction Engine — predict next 1-5 day direction using RSI, MACD, volume, and price trend."""
from __future__ import annotations
import threading
import time
from engine.market_data import _is_yf_limited, _set_yf_limited
import numpy as np
from datetime import datetime
from rich.console import Console

console = Console()

_cache: dict = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 300  # 5 minutes


def predict_trend(symbol: str) -> dict | None:
    """Predict next 1-5 day trend for a symbol.

    Returns {symbol, direction, confidence, signals, components, updated}.
    """
    cache_key = f"trend_{symbol}"
    with _cache_lock:
        if cache_key in _cache and time.time() - _cache[cache_key]["ts"] < _CACHE_TTL:
            return _cache[cache_key]["data"]

    if _is_yf_limited():
        return None
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="3mo", interval="1d")
        if hist.empty or len(hist) < 30:
            return None

        close = hist["Close"].values
        volume = hist["Volume"].values
        high = hist["High"].values
        low = hist["Low"].values

        # --- RSI Component (0-25 pts) ---
        deltas = np.diff(close[-15:])
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        avg_gain = np.mean(gains) if len(gains) > 0 else 0
        avg_loss = np.mean(losses) if len(losses) > 0 else 0.001
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        rsi_score = 0
        rsi_signal = "neutral"
        if rsi < 30:
            rsi_score = 20  # Oversold — bullish reversal signal
            rsi_signal = "oversold_bullish"
        elif rsi < 40:
            rsi_score = 15
            rsi_signal = "approaching_oversold"
        elif rsi > 70:
            rsi_score = -20  # Overbought — bearish
            rsi_signal = "overbought_bearish"
        elif rsi > 60:
            rsi_score = -10
            rsi_signal = "approaching_overbought"
        else:
            rsi_score = 5  # Neutral slight bullish bias
            rsi_signal = "neutral"

        # --- MACD Component (0-25 pts) ---
        ema12 = _ema(close, 12)
        ema26 = _ema(close, 26)
        macd_line = ema12 - ema26
        signal_line = _ema(macd_line, 9)
        histogram = macd_line - signal_line

        macd_score = 0
        macd_signal = "neutral"
        hist_now = histogram[-1]
        hist_prev = histogram[-2] if len(histogram) > 1 else 0

        if hist_now > 0 and hist_now > hist_prev:
            macd_score = 20  # Bullish and accelerating
            macd_signal = "bullish_accelerating"
        elif hist_now > 0:
            macd_score = 10  # Bullish but decelerating
            macd_signal = "bullish_decelerating"
        elif hist_now < 0 and hist_now < hist_prev:
            macd_score = -20  # Bearish and accelerating
            macd_signal = "bearish_accelerating"
        elif hist_now < 0:
            macd_score = -10  # Bearish but recovering
            macd_signal = "bearish_recovering"

        # Fresh crossover bonus
        if hist_now > 0 and hist_prev <= 0:
            macd_score += 10
            macd_signal = "fresh_bullish_cross"
        elif hist_now < 0 and hist_prev >= 0:
            macd_score -= 10
            macd_signal = "fresh_bearish_cross"

        # --- Volume Component (0-25 pts) ---
        vol_avg_20 = np.mean(volume[-20:]) if len(volume) >= 20 else np.mean(volume)
        vol_ratio = volume[-1] / vol_avg_20 if vol_avg_20 > 0 else 1.0
        price_change = (close[-1] - close[-2]) / close[-2] if len(close) > 1 else 0

        vol_score = 0
        vol_signal = "normal"
        if vol_ratio > 2.0 and price_change > 0:
            vol_score = 20  # High volume up day
            vol_signal = "surge_bullish"
        elif vol_ratio > 1.5 and price_change > 0:
            vol_score = 15
            vol_signal = "elevated_bullish"
        elif vol_ratio > 2.0 and price_change < 0:
            vol_score = -20  # High volume down day
            vol_signal = "surge_bearish"
        elif vol_ratio > 1.5 and price_change < 0:
            vol_score = -15
            vol_signal = "elevated_bearish"
        elif vol_ratio < 0.5:
            vol_score = 0
            vol_signal = "low_volume"

        # --- Price Trend Component (0-25 pts) ---
        sma_5 = np.mean(close[-5:])
        sma_10 = np.mean(close[-10:])
        sma_20 = np.mean(close[-20:])
        current = close[-1]

        trend_score = 0
        trend_signal = "neutral"

        # Price relative to MAs
        if current > sma_5 > sma_10 > sma_20:
            trend_score = 25  # Strong uptrend
            trend_signal = "strong_uptrend"
        elif current > sma_10 > sma_20:
            trend_score = 15
            trend_signal = "uptrend"
        elif current < sma_5 < sma_10 < sma_20:
            trend_score = -25  # Strong downtrend
            trend_signal = "strong_downtrend"
        elif current < sma_10 < sma_20:
            trend_score = -15
            trend_signal = "downtrend"
        else:
            # Mixed / sideways
            pct_range = (max(close[-5:]) - min(close[-5:])) / current * 100
            if pct_range < 2:
                trend_score = 0
                trend_signal = "consolidation"
            else:
                trend_score = 5 if current > sma_20 else -5
                trend_signal = "choppy"

        # --- Combine ---
        total_score = rsi_score + macd_score + vol_score + trend_score
        # Normalize to 0-100 confidence (raw range is roughly -75 to +80)
        confidence = min(95, max(5, 50 + total_score))

        if total_score > 15:
            direction = "bullish"
        elif total_score < -15:
            direction = "bearish"
        else:
            direction = "sideways"

        result = {
            "symbol": symbol,
            "direction": direction,
            "confidence": round(confidence, 1),
            "total_score": total_score,
            "components": {
                "rsi": {"score": rsi_score, "value": round(rsi, 1), "signal": rsi_signal},
                "macd": {"score": macd_score, "signal": macd_signal,
                         "histogram": round(float(hist_now), 4)},
                "volume": {"score": vol_score, "ratio": round(vol_ratio, 2), "signal": vol_signal},
                "trend": {"score": trend_score, "signal": trend_signal,
                          "sma5": round(sma_5, 2), "sma10": round(sma_10, 2), "sma20": round(sma_20, 2)},
            },
            "current_price": round(float(current), 2),
            "updated": datetime.now().isoformat(),
        }

        with _cache_lock:
            _cache[cache_key] = {"data": result, "ts": time.time()}

        return result

    except Exception as e:
        err = str(e)
        if "Too Many Requests" in err or "Rate" in err:
            _set_yf_limited()
        console.log(f"[red]Trend prediction error for {symbol}: {e}")
        return None


def _ema(data, period):
    """Simple EMA calculation."""
    if len(data) < period:
        return data.copy()
    alpha = 2 / (period + 1)
    ema = np.zeros_like(data, dtype=float)
    ema[:period] = np.mean(data[:period])
    for i in range(period, len(data)):
        ema[i] = alpha * data[i] + (1 - alpha) * ema[i - 1]
    return ema


def predict_all_trends(symbols: list) -> list:
    """Predict trends for all watchlist symbols."""
    results = []
    for sym in symbols:
        pred = predict_trend(sym)
        if pred:
            results.append(pred)
    results.sort(key=lambda x: x["confidence"], reverse=True)
    return results


def build_trend_prompt_section(symbol: str) -> str:
    """Build prompt section for AI injection."""
    pred = predict_trend(symbol)
    if not pred:
        return ""

    arrow = {"bullish": "UP", "bearish": "DOWN", "sideways": "SIDEWAYS"}[pred["direction"]]
    lines = [f"\n--- Trend Forecast: {symbol} ---"]
    lines.append(f"  Prediction: {arrow} ({pred['confidence']:.0f}% confidence)")
    c = pred["components"]
    lines.append(f"  RSI({c['rsi']['value']}): {c['rsi']['signal']}")
    lines.append(f"  MACD: {c['macd']['signal']}")
    lines.append(f"  Volume: {c['volume']['signal']} ({c['volume']['ratio']}x avg)")
    lines.append(f"  Trend: {c['trend']['signal']}")
    return "\n".join(lines)
