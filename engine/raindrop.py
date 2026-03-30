"""Raindrop Charts — intraday volume profile at each price level."""
from __future__ import annotations
import threading
import time
from datetime import datetime
from engine.market_data import _yahoo_chart
import pandas as pd
import numpy as np
from rich.console import Console

console = Console()

_cache: dict = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 120  # 2 minutes for intraday data


def compute_raindrop(symbol: str, bins_per_bar: int = 10) -> dict | None:
    """Compute raindrop/volume profile for intraday data.

    For each time period, distributes volume across the high-low range
    to show where most trading occurred.
    """
    cache_key = f"rain_{symbol}"
    with _cache_lock:
        if cache_key in _cache and time.time() - _cache[cache_key]["ts"] < _CACHE_TTL:
            return _cache[cache_key]["data"]

    try:
        # Get 5-minute intraday data
        chart = _yahoo_chart(symbol, interval="5m", range_="1d")
        if not chart:
            return None

        timestamps = chart.get("timestamp", [])
        quotes = chart.get("indicators", {}).get("quote", [{}])[0]
        if not timestamps or not quotes.get("close"):
            return None

        highs = quotes.get("high", [])
        lows = quotes.get("low", [])
        closes = quotes.get("close", [])
        volumes = quotes.get("volume", [])

        # Build rows
        rows = []
        for i in range(len(timestamps)):
            h = highs[i] if i < len(highs) and highs[i] else None
            l = lows[i] if i < len(lows) and lows[i] else None
            c = closes[i] if i < len(closes) and closes[i] else None
            v = volumes[i] if i < len(volumes) and volumes[i] else 0
            if h is None or l is None or c is None or v <= 0 or h <= l:
                continue
            rows.append({"ts": timestamps[i], "high": h, "low": l, "close": c, "volume": v})

        if len(rows) < 5:
            return None

        # Overall intraday volume profile
        price_min = min(r["low"] for r in rows)
        price_max = max(r["high"] for r in rows)
        price_range = price_max - price_min
        if price_range <= 0:
            return None

        n_bins = 25
        bin_size = price_range / n_bins
        bin_volumes = [0.0] * n_bins
        bin_prices = [round(price_min + (i + 0.5) * bin_size, 2) for i in range(n_bins)]

        for row in rows:
            for i in range(n_bins):
                b_low = price_min + i * bin_size
                b_high = b_low + bin_size
                overlap_low = max(row["low"], b_low)
                overlap_high = min(row["high"], b_high)
                if overlap_high > overlap_low:
                    close_in_bin = b_low <= row["close"] <= b_high
                    weight = 2.0 if close_in_bin else 1.0
                    frac = (overlap_high - overlap_low) / (row["high"] - row["low"])
                    bin_volumes[i] += row["volume"] * frac * weight

        max_vol = max(bin_volumes) if bin_volumes else 1

        # Build raindrop profile
        profile = []
        for i in range(n_bins):
            intensity = round(bin_volumes[i] / max_vol, 3) if max_vol > 0 else 0
            profile.append({
                "price": bin_prices[i],
                "volume": round(bin_volumes[i]),
                "width": intensity,
            })

        # Per-bar raindrops (grouped into hourly blocks)
        hourly_drops = {}
        for row in rows:
            hour_key = datetime.fromtimestamp(row["ts"]).strftime("%H:00")
            if hour_key not in hourly_drops:
                hourly_drops[hour_key] = {"high": 0, "low": 999999, "volume": 0, "closes": []}
            hd = hourly_drops[hour_key]
            hd["high"] = max(hd["high"], row["high"])
            hd["low"] = min(hd["low"], row["low"])
            hd["volume"] += row["volume"]
            hd["closes"].append(row["close"])

        drops = []
        for hour, hd in sorted(hourly_drops.items()):
            if hd["volume"] <= 0 or hd["high"] <= hd["low"]:
                continue
            avg_close = sum(hd["closes"]) / len(hd["closes"])
            drops.append({
                "time": hour,
                "high": round(hd["high"], 2),
                "low": round(hd["low"], 2),
                "volume": round(hd["volume"]),
                "vwap_approx": round(avg_close, 2),
                "range": round(hd["high"] - hd["low"], 2),
            })

        # Point of control
        poc_idx = bin_volumes.index(max(bin_volumes)) if bin_volumes else 0
        poc_price = bin_prices[poc_idx] if bin_prices else 0

        current_price = rows[-1]["close"]

        result = {
            "symbol": symbol,
            "current_price": round(current_price, 2),
            "profile": profile,
            "hourly_drops": drops,
            "poc": round(poc_price, 2),
            "price_range": {"min": round(price_min, 2), "max": round(price_max, 2)},
            "total_volume": round(sum(bin_volumes)),
        }

        with _cache_lock:
            _cache[cache_key] = {"data": result, "ts": time.time()}

        return result
    except Exception as e:
        console.log(f"[red]Raindrop error for {symbol}: {e}")
        return None
