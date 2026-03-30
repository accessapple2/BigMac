"""Auto Trendline Detection — support/resistance levels from 90-day price data."""
from __future__ import annotations
import threading
import time
from engine.market_data import _yahoo_chart
import pandas as pd
import numpy as np
from collections import defaultdict
from rich.console import Console

console = Console()

_cache: dict = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 300  # 5 minutes


def _find_pivots(highs: pd.Series, lows: pd.Series, close: pd.Series, window: int = 3):
    """Find pivot highs and pivot lows."""
    pivot_highs = []
    pivot_lows = []

    for i in range(window, len(highs) - window):
        # Pivot high: higher than surrounding bars
        if all(highs.iloc[i] >= highs.iloc[i - j] for j in range(1, window + 1)) and \
           all(highs.iloc[i] >= highs.iloc[i + j] for j in range(1, window + 1)):
            pivot_highs.append(float(highs.iloc[i]))

        # Pivot low: lower than surrounding bars
        if all(lows.iloc[i] <= lows.iloc[i - j] for j in range(1, window + 1)) and \
           all(lows.iloc[i] <= lows.iloc[i + j] for j in range(1, window + 1)):
            pivot_lows.append(float(lows.iloc[i]))

    return pivot_highs, pivot_lows


def _cluster_levels(prices: list, tolerance_pct: float = 0.8) -> list:
    """Cluster nearby price levels and count touches."""
    if not prices:
        return []

    sorted_prices = sorted(prices)
    clusters = []
    current_cluster = [sorted_prices[0]]

    for i in range(1, len(sorted_prices)):
        centroid = sum(current_cluster) / len(current_cluster)
        if abs(sorted_prices[i] - centroid) / centroid * 100 <= tolerance_pct:
            current_cluster.append(sorted_prices[i])
        else:
            clusters.append(current_cluster)
            current_cluster = [sorted_prices[i]]
    clusters.append(current_cluster)

    result = []
    for cluster in clusters:
        result.append({
            "level": round(sum(cluster) / len(cluster), 2),
            "touches": len(cluster),
        })

    result.sort(key=lambda x: x["touches"], reverse=True)
    return result


def _fetch_daily_ohlcv(symbol: str, range_: str = "3mo") -> pd.DataFrame | None:
    """Fetch daily OHLCV data via Yahoo direct HTTP and return as DataFrame."""
    chart = _yahoo_chart(symbol, interval="1d", range_=range_)
    if not chart:
        return None
    try:
        timestamps = chart.get("timestamp", [])
        quotes = chart.get("indicators", {}).get("quote", [{}])[0]
        if not timestamps or not quotes.get("close"):
            return None

        df = pd.DataFrame({
            "High": quotes.get("high", []),
            "Low": quotes.get("low", []),
            "Close": quotes.get("close", []),
            "Open": quotes.get("open", []),
            "Volume": quotes.get("volume", []),
        })
        # Drop rows with None close
        df = df.dropna(subset=["Close"])
        if len(df) < 15:
            return None
        return df
    except Exception:
        return None


def detect_support_resistance(symbol: str) -> dict | None:
    """Detect top 3 support and top 3 resistance levels for a symbol."""
    cache_key = f"sr_{symbol}"
    with _cache_lock:
        if cache_key in _cache and time.time() - _cache[cache_key]["ts"] < _CACHE_TTL:
            return _cache[cache_key]["data"]

    try:
        hist = _fetch_daily_ohlcv(symbol, "3mo")
        if hist is None:
            return None

        highs = hist["High"]
        lows = hist["Low"]
        close = hist["Close"]
        current_price = float(close.iloc[-1])

        # Find pivot points
        pivot_highs, pivot_lows = _find_pivots(highs, lows, close)

        # Also add recent highs/lows that price bounced from
        for i in range(2, len(close) - 1):
            if float(lows.iloc[i]) < float(lows.iloc[i - 1]) and float(lows.iloc[i]) < float(lows.iloc[i + 1]):
                pivot_lows.append(float(lows.iloc[i]))
            if float(highs.iloc[i]) > float(highs.iloc[i - 1]) and float(highs.iloc[i]) > float(highs.iloc[i + 1]):
                pivot_highs.append(float(highs.iloc[i]))

        # Cluster levels
        resistance_clusters = _cluster_levels(pivot_highs)
        support_clusters = _cluster_levels(pivot_lows)

        # Filter: resistance above current price, support below
        resistances = [r for r in resistance_clusters if r["level"] > current_price * 1.001]
        supports = [s for s in support_clusters if s["level"] < current_price * 0.999]

        if len(resistances) < 3:
            extra = [r for r in resistance_clusters if r not in resistances]
            resistances.extend(extra[:3 - len(resistances)])
        if len(supports) < 3:
            extra = [s for s in support_clusters if s not in supports]
            supports.extend(extra[:3 - len(supports)])

        top_resistance = sorted(resistances[:3], key=lambda x: x["level"])
        top_support = sorted(supports[:3], key=lambda x: x["level"], reverse=True)

        # 30-day high/low
        last_30 = hist.tail(30)
        high_30d = round(float(last_30["High"].max()), 2)
        low_30d = round(float(last_30["Low"].min()), 2)

        result = {
            "symbol": symbol,
            "current_price": round(current_price, 2),
            "support": [s["level"] for s in top_support],
            "resistance": [r["level"] for r in top_resistance],
            "support_details": top_support,
            "resistance_details": top_resistance,
            "high_30d": high_30d,
            "low_30d": low_30d,
        }

        with _cache_lock:
            _cache[cache_key] = {"data": result, "ts": time.time()}

        return result
    except Exception as e:
        console.log(f"[red]Trendline error for {symbol}: {e}")
        return None


def get_all_levels(symbols: list) -> dict:
    """Get S/R levels for all watchlist symbols."""
    result = {}
    for sym in symbols:
        data = detect_support_resistance(sym)
        if data:
            result[sym] = data
    return result


def build_sr_prompt_section(symbol: str) -> str:
    """Build prompt section with support/resistance levels."""
    data = detect_support_resistance(symbol)
    if not data:
        return ""

    support_str = ", ".join(f"${s}" for s in data["support"]) if data["support"] else "None detected"
    resist_str = ", ".join(f"${r}" for r in data["resistance"]) if data["resistance"] else "None detected"

    return (
        f"\n--- Support / Resistance ---\n"
        f"Key Support: {support_str}\n"
        f"Key Resistance: {resist_str}\n"
        f"30-Day Range: ${data['low_30d']} — ${data['high_30d']}"
    )
