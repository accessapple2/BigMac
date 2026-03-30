"""VIX monitoring — fetch VIX price and alert on spikes via Yahoo direct."""
from __future__ import annotations
from datetime import datetime
from engine.market_data import _yahoo_chart
from rich.console import Console

console = Console()

_last_vix = None
_vix_open = None
_vix_history = []  # list of {price, change_pct, timestamp}


def fetch_vix() -> dict | None:
    """Fetch current VIX price from Yahoo direct HTTP."""
    global _last_vix, _vix_open
    try:
        chart = _yahoo_chart("^VIX", interval="1m", range_="1d")
        if not chart:
            return None
        meta = chart.get("meta", {})
        price = meta.get("regularMarketPrice")
        if not price:
            return None
        price = round(float(price), 2)

        prev_close = meta.get("chartPreviousClose") or meta.get("previousClose") or price
        if _vix_open is None:
            _vix_open = round(float(prev_close), 2)

        open_price = _vix_open or price
        change_pct = round((price - open_price) / open_price * 100, 2) if open_price > 0 else 0.0

        _last_vix = price

        result = {
            "price": price,
            "open": open_price,
            "change_pct": change_pct,
            "high": round(float(meta.get("regularMarketDayHigh", price)), 2),
            "low": round(float(meta.get("regularMarketDayLow", price)), 2),
            "timestamp": datetime.now().isoformat(),
        }

        # Track history (keep last 100 readings)
        _vix_history.append(result)
        if len(_vix_history) > 100:
            _vix_history.pop(0)

        return result
    except Exception as e:
        console.log(f"[red]VIX fetch error: {e}")
        return None


def check_vix_spike(threshold_pct: float = 5.0) -> dict | None:
    """Fetch VIX and return spike data if change exceeds threshold."""
    data = fetch_vix()
    if not data:
        return None
    if abs(data["change_pct"]) >= threshold_pct:
        console.log(
            f"[bold red]VIX SPIKE: {data['price']:.2f} ({data['change_pct']:+.1f}% intraday)"
        )
        return data
    return None


def get_vix_status() -> dict:
    """Get current VIX data for dashboard."""
    if _last_vix is not None and _vix_history:
        return _vix_history[-1]
    data = fetch_vix()
    return data or {"price": 0, "open": 0, "change_pct": 0, "timestamp": ""}


def get_vix_history() -> list:
    """Return recent VIX readings for sparkline."""
    return _vix_history[-50:]
