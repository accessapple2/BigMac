"""Volume Profile — volume-at-price analysis from free Yahoo Finance data."""
from __future__ import annotations
import numpy as np
from rich.console import Console

console = Console()


def get_volume_profile(ticker: str, period: str = "30d") -> dict:
    """Build volume profile showing volume concentration at each price level."""
    import yfinance as yf

    data = yf.download(ticker, period=period, progress=False)
    if data.empty or len(data) < 5:
        return {"error": f"Insufficient data for {ticker}"}

    close = data["Close"].values.flatten()
    volume = data["Volume"].values.flatten()

    price_min = float(np.min(close))
    price_max = float(np.max(close))
    num_bins = 20
    bins = np.linspace(price_min, price_max, num_bins + 1)
    total_vol = float(np.sum(volume))

    profile = []
    for i in range(len(bins) - 1):
        mask = (close >= bins[i]) & (close < bins[i + 1])
        vol_at_level = float(np.sum(volume[mask]))
        profile.append({
            "price_low": round(float(bins[i]), 2),
            "price_high": round(float(bins[i + 1]), 2),
            "price_mid": round(float((bins[i] + bins[i + 1]) / 2), 2),
            "volume": int(vol_at_level),
            "pct_of_total": round(vol_at_level / total_vol * 100, 1) if total_vol > 0 else 0,
        })

    # Point of Control
    poc = max(profile, key=lambda x: x["volume"])

    # Value Area (70% of volume)
    sorted_profile = sorted(profile, key=lambda x: x["volume"], reverse=True)
    va_vol = 0
    va_levels = []
    for p in sorted_profile:
        va_vol += p["volume"]
        va_levels.append(p)
        if va_vol >= total_vol * 0.7:
            break

    va_prices = [p["price_mid"] for p in va_levels]
    va_high = max(va_prices) if va_prices else poc["price_mid"]
    va_low = min(va_prices) if va_prices else poc["price_mid"]
    current = round(float(close[-1]), 2)

    position = "ABOVE_VA" if current > va_high else "BELOW_VA" if current < va_low else "IN_VALUE_AREA"

    return {
        "ticker": ticker,
        "period": period,
        "profile": profile,
        "poc": poc["price_mid"],
        "value_area_high": round(va_high, 2),
        "value_area_low": round(va_low, 2),
        "current_price": current,
        "position": position,
    }
