"""Auto Fibonacci Levels — retracement levels from 30-day high/low."""
from __future__ import annotations
import threading
import time
from engine.market_data import _is_yf_limited, _set_yf_limited
from rich.console import Console

console = Console()

_cache: dict = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 300

FIB_RATIOS = [0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0]
FIB_LABELS = ["0%", "23.6%", "38.2%", "50%", "61.8%", "78.6%", "100%"]
FIB_COLORS = ["#3fb950", "#58a6ff", "#a371f7", "#f0883e", "#a371f7", "#58a6ff", "#f85149"]


def compute_fibonacci(symbol: str) -> dict | None:
    """Calculate Fibonacci retracement levels from 30-day high/low."""
    cache_key = f"fib_{symbol}"
    with _cache_lock:
        if cache_key in _cache and time.time() - _cache[cache_key]["ts"] < _CACHE_TTL:
            return _cache[cache_key]["data"]

    if _is_yf_limited():
        return None
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="3mo", interval="1d")
        if hist.empty or len(hist) < 10:
            return None

        last_30 = hist.tail(30)
        high = float(last_30["High"].max())
        low = float(last_30["Low"].min())
        current = float(hist["Close"].iloc[-1])
        diff = high - low

        if diff <= 0:
            return None

        # Determine trend direction for fib calculation
        # If current price is closer to high, it's an uptrend retracement
        # If closer to low, it's a downtrend retracement
        is_uptrend = current > (high + low) / 2

        levels = []
        for i, ratio in enumerate(FIB_RATIOS):
            if is_uptrend:
                # Uptrend: measure retracement from high
                price = high - diff * ratio
            else:
                # Downtrend: measure retracement from low
                price = low + diff * ratio

            levels.append({
                "ratio": ratio,
                "label": FIB_LABELS[i],
                "price": round(price, 2),
                "color": FIB_COLORS[i],
            })

        # Find which fib zone the current price is in
        current_zone = "Below 0%"
        for i in range(len(levels) - 1):
            lo = min(levels[i]["price"], levels[i + 1]["price"])
            hi = max(levels[i]["price"], levels[i + 1]["price"])
            if lo <= current <= hi:
                current_zone = f"{levels[i]['label']} — {levels[i + 1]['label']}"
                break
        if current > max(l["price"] for l in levels):
            current_zone = "Above 100%"

        result = {
            "symbol": symbol,
            "current_price": round(current, 2),
            "high_30d": round(high, 2),
            "low_30d": round(low, 2),
            "trend": "uptrend" if is_uptrend else "downtrend",
            "levels": levels,
            "current_zone": current_zone,
        }

        with _cache_lock:
            _cache[cache_key] = {"data": result, "ts": time.time()}

        return result
    except Exception as e:
        err = str(e)
        if "Too Many Requests" in err or "Rate" in err:
            _set_yf_limited()
        console.log(f"[red]Fibonacci error for {symbol}: {e}")
        return None


def build_fib_prompt_section(symbol: str) -> str:
    """Build prompt section with Fibonacci levels."""
    data = compute_fibonacci(symbol)
    if not data:
        return ""

    levels_str = ", ".join(f"{l['label']}=${l['price']}" for l in data["levels"])

    return (
        f"\n--- Fibonacci Retracement ---\n"
        f"30-Day High: ${data['high_30d']} | Low: ${data['low_30d']} | Trend: {data['trend'].upper()}\n"
        f"Fib Levels: {levels_str}\n"
        f"Current Zone: {data['current_zone']}"
    )
