"""Chart Pattern Recognition — detect basic chart patterns from price data."""
from __future__ import annotations
import threading
import time
from engine.market_data import _yahoo_chart
import pandas as pd
import numpy as np
from rich.console import Console

console = Console()

_cache: dict = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 300


def _find_local_extremes(series: pd.Series, window: int = 5):
    """Find local highs and lows with their indices."""
    highs = []
    lows = []
    arr = series.values
    for i in range(window, len(arr) - window):
        if all(arr[i] >= arr[i - j] for j in range(1, window + 1)) and \
           all(arr[i] >= arr[i + j] for j in range(1, window + 1)):
            highs.append((i, float(arr[i])))
        if all(arr[i] <= arr[i - j] for j in range(1, window + 1)) and \
           all(arr[i] <= arr[i + j] for j in range(1, window + 1)):
            lows.append((i, float(arr[i])))
    return highs, lows


def _detect_double_top(highs: list, lows: list, current_price: float, tolerance: float = 0.02) -> dict | None:
    """Detect double top pattern — two peaks at similar levels with a valley between."""
    if len(highs) < 2:
        return None
    for i in range(len(highs) - 1):
        h1_idx, h1_price = highs[i]
        h2_idx, h2_price = highs[i + 1]
        if abs(h1_price - h2_price) / h1_price <= tolerance and h2_idx - h1_idx >= 5:
            valley_lows = [l for l in lows if h1_idx < l[0] < h2_idx]
            if valley_lows:
                neckline = min(l[1] for l in valley_lows)
                target = neckline - (h1_price - neckline)
                if current_price < h1_price * 0.99:
                    return {
                        "pattern": "double_top",
                        "label": "Double Top",
                        "direction": "bearish",
                        "neckline": round(neckline, 2),
                        "target": round(target, 2),
                        "peak": round((h1_price + h2_price) / 2, 2),
                    }
    return None


def _detect_double_bottom(highs: list, lows: list, current_price: float, tolerance: float = 0.02) -> dict | None:
    """Detect double bottom — two troughs at similar levels."""
    if len(lows) < 2:
        return None
    for i in range(len(lows) - 1):
        l1_idx, l1_price = lows[i]
        l2_idx, l2_price = lows[i + 1]
        if abs(l1_price - l2_price) / l1_price <= tolerance and l2_idx - l1_idx >= 5:
            valley_highs = [h for h in highs if l1_idx < h[0] < l2_idx]
            if valley_highs:
                neckline = max(h[1] for h in valley_highs)
                target = neckline + (neckline - l1_price)
                if current_price > l1_price * 1.01:
                    return {
                        "pattern": "double_bottom",
                        "label": "Double Bottom",
                        "direction": "bullish",
                        "neckline": round(neckline, 2),
                        "target": round(target, 2),
                        "trough": round((l1_price + l2_price) / 2, 2),
                    }
    return None


def _detect_head_and_shoulders(highs: list, lows: list, current_price: float, tolerance: float = 0.02) -> dict | None:
    """Detect head and shoulders — three peaks, middle highest."""
    if len(highs) < 3:
        return None
    for i in range(len(highs) - 2):
        h1_idx, h1 = highs[i]
        h2_idx, h2 = highs[i + 1]
        h3_idx, h3 = highs[i + 2]
        if h2 > h1 and h2 > h3:
            if abs(h1 - h3) / h1 <= tolerance * 2:
                v1 = [l for l in lows if h1_idx < l[0] < h2_idx]
                v2 = [l for l in lows if h2_idx < l[0] < h3_idx]
                if v1 and v2:
                    neckline = (min(l[1] for l in v1) + min(l[1] for l in v2)) / 2
                    target = neckline - (h2 - neckline)
                    return {
                        "pattern": "head_and_shoulders",
                        "label": "Head & Shoulders",
                        "direction": "bearish",
                        "neckline": round(neckline, 2),
                        "target": round(target, 2),
                        "head": round(h2, 2),
                        "left_shoulder": round(h1, 2),
                        "right_shoulder": round(h3, 2),
                    }
    return None


def _detect_ascending_triangle(highs: list, lows: list, current_price: float, tolerance: float = 0.015) -> dict | None:
    """Detect ascending triangle — flat top, rising bottoms."""
    if len(highs) < 2 or len(lows) < 2:
        return None
    recent_highs = highs[-4:] if len(highs) >= 4 else highs
    if len(recent_highs) < 2:
        return None

    high_values = [h[1] for h in recent_highs]
    avg_high = sum(high_values) / len(high_values)
    flat_top = all(abs(h - avg_high) / avg_high <= tolerance for h in high_values)

    recent_lows = lows[-4:] if len(lows) >= 4 else lows
    if len(recent_lows) < 2:
        return None

    rising_lows = all(recent_lows[j][1] >= recent_lows[j - 1][1] * 0.99 for j in range(1, len(recent_lows)))

    if flat_top and rising_lows:
        resistance = round(avg_high, 2)
        height = resistance - recent_lows[0][1]
        target = resistance + height
        return {
            "pattern": "ascending_triangle",
            "label": "Ascending Triangle",
            "direction": "bullish",
            "resistance": resistance,
            "target": round(target, 2),
            "support_trend": round(recent_lows[-1][1], 2),
        }
    return None


def _detect_descending_triangle(highs: list, lows: list, current_price: float, tolerance: float = 0.015) -> dict | None:
    """Detect descending triangle — flat bottom, falling highs."""
    if len(highs) < 2 or len(lows) < 2:
        return None

    recent_lows = lows[-4:] if len(lows) >= 4 else lows
    if len(recent_lows) < 2:
        return None
    low_values = [l[1] for l in recent_lows]
    avg_low = sum(low_values) / len(low_values)
    flat_bottom = all(abs(l - avg_low) / avg_low <= tolerance for l in low_values)

    recent_highs = highs[-4:] if len(highs) >= 4 else highs
    if len(recent_highs) < 2:
        return None
    falling_highs = all(recent_highs[j][1] <= recent_highs[j - 1][1] * 1.01 for j in range(1, len(recent_highs)))

    if flat_bottom and falling_highs:
        support = round(avg_low, 2)
        height = recent_highs[0][1] - support
        target = support - height
        return {
            "pattern": "descending_triangle",
            "label": "Descending Triangle",
            "direction": "bearish",
            "support": support,
            "target": round(target, 2),
            "resistance_trend": round(recent_highs[-1][1], 2),
        }
    return None


def _detect_wedge(highs: list, lows: list, current_price: float) -> dict | None:
    """Detect rising or falling wedge."""
    if len(highs) < 3 or len(lows) < 3:
        return None

    recent_highs = highs[-4:]
    recent_lows = lows[-4:]

    h_vals = [h[1] for h in recent_highs]
    l_vals = [l[1] for l in recent_lows]

    if len(h_vals) >= 2 and len(l_vals) >= 2:
        h_rising = h_vals[-1] > h_vals[0]
        l_rising = l_vals[-1] > l_vals[0]
        h_slope = (h_vals[-1] - h_vals[0]) / len(h_vals)
        l_slope = (l_vals[-1] - l_vals[0]) / len(l_vals)

        if h_rising and l_rising and l_slope > h_slope * 0.5:
            if h_slope < l_slope:
                return {
                    "pattern": "rising_wedge",
                    "label": "Rising Wedge",
                    "direction": "bearish",
                    "target": round(l_vals[0], 2),
                }

        h_falling = h_vals[-1] < h_vals[0]
        l_falling = l_vals[-1] < l_vals[0]
        if h_falling and l_falling:
            if abs(h_slope) > abs(l_slope) * 0.5:
                return {
                    "pattern": "falling_wedge",
                    "label": "Falling Wedge",
                    "direction": "bullish",
                    "target": round(h_vals[0], 2),
                }

    return None


def _fetch_daily_ohlcv(symbol: str, range_: str = "3mo") -> pd.DataFrame | None:
    """Fetch daily OHLCV data via Yahoo direct HTTP."""
    chart = _yahoo_chart(symbol, interval="1d", range_=range_)
    if not chart:
        return None
    try:
        quotes = chart.get("indicators", {}).get("quote", [{}])[0]
        if not quotes.get("close"):
            return None
        df = pd.DataFrame({
            "High": quotes.get("high", []),
            "Low": quotes.get("low", []),
            "Close": quotes.get("close", []),
        })
        df = df.dropna(subset=["Close"])
        return df if len(df) >= 20 else None
    except Exception:
        return None


def detect_patterns(symbol: str) -> list:
    """Detect all chart patterns for a symbol."""
    cache_key = f"pat_{symbol}"
    with _cache_lock:
        if cache_key in _cache and time.time() - _cache[cache_key]["ts"] < _CACHE_TTL:
            return _cache[cache_key]["data"]

    try:
        hist = _fetch_daily_ohlcv(symbol, "3mo")
        if hist is None:
            return []

        close = hist["Close"]
        high = hist["High"]
        low = hist["Low"]
        current = float(close.iloc[-1])

        highs_from_high, lows_from_low = _find_local_extremes(high, window=3)
        _, lows_from_close = _find_local_extremes(close, window=3)
        highs_from_close, _ = _find_local_extremes(close, window=3)

        all_highs = highs_from_high + highs_from_close
        all_lows = lows_from_low + lows_from_close
        all_highs = sorted(set(all_highs), key=lambda x: x[0])
        all_lows = sorted(set(all_lows), key=lambda x: x[0])

        patterns = []

        detectors = [
            _detect_double_top,
            _detect_double_bottom,
            _detect_head_and_shoulders,
            _detect_ascending_triangle,
            _detect_descending_triangle,
            _detect_wedge,
        ]

        for detector in detectors:
            try:
                result = detector(all_highs, all_lows, current)
                if result:
                    result["symbol"] = symbol
                    result["current_price"] = round(current, 2)
                    patterns.append(result)
            except Exception:
                continue

        with _cache_lock:
            _cache[cache_key] = {"data": patterns, "ts": time.time()}

        return patterns
    except Exception as e:
        console.log(f"[red]Pattern detection error for {symbol}: {e}")
        return []


def detect_all_patterns(symbols: list) -> list:
    """Detect patterns for all watchlist symbols."""
    all_patterns = []
    for sym in symbols:
        patterns = detect_patterns(sym)
        all_patterns.extend(patterns)
    return all_patterns


def build_pattern_prompt_section(symbol: str) -> str:
    """Build prompt section with detected chart patterns."""
    patterns = detect_patterns(symbol)
    if not patterns:
        return ""

    lines = ["\n--- Chart Patterns Detected ---"]
    for p in patterns:
        direction = "BULLISH" if p["direction"] == "bullish" else "BEARISH"
        target = p.get("target", "?")
        lines.append(f"  {p['label']} [{direction}] — target ${target}")

    return "\n".join(lines)
