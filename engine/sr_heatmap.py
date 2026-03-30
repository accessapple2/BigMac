"""Support/Resistance Heatmap — volume-weighted price levels."""
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


def compute_volume_profile(symbol: str, bins: int = 30) -> dict | None:
    """Compute volume profile — volume traded at each price level."""
    cache_key = f"vp_{symbol}"
    with _cache_lock:
        if cache_key in _cache and time.time() - _cache[cache_key]["ts"] < _CACHE_TTL:
            return _cache[cache_key]["data"]

    try:
        chart = _yahoo_chart(symbol, interval="1d", range_="1mo")
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

        # Build price-volume pairs
        price_volume = []
        for i in range(len(timestamps)):
            h = highs[i] if i < len(highs) and highs[i] else None
            l = lows[i] if i < len(lows) and lows[i] else None
            c = closes[i] if i < len(closes) and closes[i] else None
            v = volumes[i] if i < len(volumes) and volumes[i] else 0
            if h is None or l is None or c is None or v <= 0 or h <= l:
                continue
            price_volume.append({"high": h, "low": l, "close": c, "volume": v})

        if len(price_volume) < 5:
            return None

        # Create price bins
        all_lows = [pv["low"] for pv in price_volume]
        all_highs = [pv["high"] for pv in price_volume]
        price_min = min(all_lows)
        price_max = max(all_highs)
        price_range = price_max - price_min
        if price_range <= 0:
            return None

        bin_size = price_range / bins
        bin_volumes = [0.0] * bins
        bin_prices = [round(price_min + (i + 0.5) * bin_size, 2) for i in range(bins)]

        # Distribute volume into bins
        for pv in price_volume:
            for i in range(bins):
                bin_low = price_min + i * bin_size
                bin_high = bin_low + bin_size
                overlap_low = max(pv["low"], bin_low)
                overlap_high = min(pv["high"], bin_high)
                if overlap_high > overlap_low:
                    close_in_bin = bin_low <= pv["close"] <= bin_high
                    weight = 1.5 if close_in_bin else 1.0
                    overlap_frac = (overlap_high - overlap_low) / (pv["high"] - pv["low"])
                    bin_volumes[i] += pv["volume"] * overlap_frac * weight

        max_vol = max(bin_volumes) if bin_volumes else 1
        current_price = closes[-1]
        for c in reversed(closes):
            if c is not None:
                current_price = c
                break

        # Build profile
        profile = []
        for i in range(bins):
            intensity = round(bin_volumes[i] / max_vol, 3) if max_vol > 0 else 0
            profile.append({
                "price": bin_prices[i],
                "volume": round(bin_volumes[i]),
                "intensity": intensity,
                "is_high_volume": intensity > 0.7,
            })

        # High-volume nodes
        hvn = [p for p in profile if p["intensity"] > 0.6]
        hvn.sort(key=lambda x: x["volume"], reverse=True)

        # Point of control (highest volume price)
        poc = max(profile, key=lambda x: x["volume"]) if profile else None

        # Value area (70% of volume)
        sorted_by_vol = sorted(profile, key=lambda x: x["volume"], reverse=True)
        total_vol = sum(p["volume"] for p in profile)
        cum = 0
        value_area = []
        for p in sorted_by_vol:
            cum += p["volume"]
            value_area.append(p["price"])
            if cum >= total_vol * 0.7:
                break

        result = {
            "symbol": symbol,
            "current_price": round(current_price, 2),
            "profile": profile,
            "poc": {"price": poc["price"], "volume": poc["volume"]} if poc else None,
            "value_area_high": round(max(value_area), 2) if value_area else None,
            "value_area_low": round(min(value_area), 2) if value_area else None,
            "high_volume_nodes": [{"price": h["price"], "intensity": h["intensity"]} for h in hvn[:5]],
            "price_range": {"min": round(price_min, 2), "max": round(price_max, 2)},
        }

        with _cache_lock:
            _cache[cache_key] = {"data": result, "ts": time.time()}

        return result
    except Exception as e:
        console.log(f"[red]Volume profile error for {symbol}: {e}")
        return None
