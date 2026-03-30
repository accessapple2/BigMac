"""High IV Opportunity Scanner — find elevated premium opportunities when VIX > 25."""
from __future__ import annotations
import threading
import time
from engine.market_data import _is_yf_limited, _set_yf_limited
import pandas as pd
import numpy as np
from rich.console import Console

console = Console()

_cache: dict = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 600  # 10 minutes


def _get_iv_rank(symbol: str) -> dict | None:
    """Calculate IV rank for a symbol.

    IV Rank = (Current IV - 52-week Low IV) / (52-week High IV - 52-week Low IV)
    Uses ATM option implied volatility as proxy.
    """
    if _is_yf_limited():
        return None
    try:
        ticker = yf.Ticker(symbol)
        expirations = ticker.options
        if not expirations:
            return None

        # Get spot price
        hist = ticker.history(period="1d")
        if hist.empty:
            return None
        spot = float(hist["Close"].iloc[-1])

        # Get nearest expiration chain
        nearest = expirations[0]
        chain = ticker.option_chain(nearest)
        calls = chain.calls

        if calls.empty:
            return None

        # Find ATM call (closest strike to spot)
        calls = calls.copy()
        calls["distance"] = abs(calls["strike"] - spot)
        atm = calls.sort_values("distance").iloc[0]
        current_iv = float(atm.get("impliedVolatility", 0))
        if current_iv <= 0 or pd.isna(current_iv):
            return None
        current_iv_pct = round(current_iv * 100, 1)

        # Estimate IV rank from historical volatility as proxy
        # Use 1-year daily data to estimate realized vol range
        yearly = ticker.history(period="1y", interval="1d")
        if yearly.empty or len(yearly) < 30:
            return None

        close = yearly["Close"]
        # Calculate rolling 20-day realized vol (annualized)
        returns = close.pct_change().dropna()
        rolling_vol = returns.rolling(20).std() * np.sqrt(252)
        rolling_vol = rolling_vol.dropna()

        if rolling_vol.empty:
            return None

        vol_high = float(rolling_vol.max()) * 100
        vol_low = float(rolling_vol.min()) * 100
        vol_range = vol_high - vol_low

        if vol_range <= 0:
            iv_rank = 50.0
        else:
            iv_rank = min(100, max(0, (current_iv_pct - vol_low) / vol_range * 100))

        # IV percentile (what % of days had lower IV)
        iv_percentile = float((rolling_vol * 100 < current_iv_pct).sum() / len(rolling_vol) * 100)

        return {
            "symbol": symbol,
            "current_iv": current_iv_pct,
            "iv_rank": round(iv_rank, 1),
            "iv_percentile": round(iv_percentile, 1),
            "vol_high_52w": round(vol_high, 1),
            "vol_low_52w": round(vol_low, 1),
            "spot": round(spot, 2),
            "expiration": nearest,
        }

    except Exception as e:
        err = str(e)
        if "Too Many Requests" in err or "Rate" in err:
            _set_yf_limited()
        console.log(f"[red]IV rank error for {symbol}: {e}")
        return None


def scan_high_iv_opportunities(symbols: list) -> dict:
    """Scan for high IV opportunities when VIX is elevated.

    Returns dict with VIX level and list of stocks with IV rank > 80th percentile.
    """
    with _cache_lock:
        if _cache.get("data") and time.time() - _cache.get("ts", 0) < _CACHE_TTL:
            return _cache["data"]

    # Get current VIX
    vix_level = 0
    try:
        from engine.vix_monitor import get_vix_status
        vix = get_vix_status()
        vix_level = vix.get("price", 0) if vix else 0
    except Exception:
        pass

    opportunities = []
    for sym in symbols:
        data = _get_iv_rank(sym)
        if data:
            data["is_elevated"] = data["iv_rank"] >= 80
            data["is_opportunity"] = vix_level >= 25 and data["iv_rank"] >= 80
            opportunities.append(data)

    # Sort by IV rank descending
    opportunities.sort(key=lambda x: x["iv_rank"], reverse=True)

    # Label
    if vix_level >= 30:
        vix_label = "EXTREME VOLATILITY"
        vix_color = "#f85149"
    elif vix_level >= 25:
        vix_label = "HIGH VOLATILITY"
        vix_color = "#f0883e"
    elif vix_level >= 20:
        vix_label = "ELEVATED"
        vix_color = "#f0883e"
    else:
        vix_label = "LOW VOLATILITY"
        vix_color = "#3fb950"

    elevated_count = sum(1 for o in opportunities if o["is_elevated"])

    result = {
        "vix": vix_level,
        "vix_label": vix_label,
        "vix_color": vix_color,
        "scanning_enabled": vix_level >= 25,
        "opportunities": opportunities,
        "elevated_count": elevated_count,
        "total_scanned": len(opportunities),
    }

    with _cache_lock:
        _cache["data"] = result
        _cache["ts"] = time.time()

    return result
