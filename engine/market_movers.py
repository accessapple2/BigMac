"""Market Movers — top gainers, losers, and most active from watchlist."""
from __future__ import annotations
import json
import time
from datetime import datetime
from rich.console import Console

import config

console = Console()

_MOVERS_CACHE_FILE = "data/movers_cache.json"
_movers_disk_cache: dict = {}


def _load_movers_disk_cache():
    global _movers_disk_cache
    try:
        with open(_MOVERS_CACHE_FILE, "r") as f:
            _movers_disk_cache = json.load(f)
    except Exception:
        pass


def _save_movers_disk_cache(data: dict):
    """Persist movers data atomically (temp file → rename) to prevent 0-byte corruption."""
    import os, tempfile
    try:
        payload = {**data, "_ts": time.time()}
        serialized = json.dumps(payload)
        cache_dir = os.path.dirname(os.path.abspath(_MOVERS_CACHE_FILE))
        fd, tmp_path = tempfile.mkstemp(dir=cache_dir, suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                f.write(serialized)
            os.replace(tmp_path, _MOVERS_CACHE_FILE)
        except Exception:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass
            raise
        _movers_disk_cache.update(payload)
    except Exception:
        pass


# Load stale data at import so first request is instant
_load_movers_disk_cache()


def get_market_movers() -> dict:
    """Get top 5 gainers, losers, and most active from the watchlist.
    Uses bulk Yahoo fetch (one request for all watchlist symbols).
    Falls back to disk cache if Yahoo is unavailable."""
    from engine.market_data import get_bulk_prices

    prices = get_bulk_prices(list(config.WATCH_STOCKS), timeout=5)

    # If bulk failed, fall back to disk cache
    if not prices:
        if _movers_disk_cache.get("gainers"):
            return {k: v for k, v in _movers_disk_cache.items() if k != "_ts"}
        from engine.market_data import get_all_prices
        prices = get_all_prices(config.WATCH_STOCKS)

    stocks = []
    for sym, data in prices.items():
        stocks.append({
            "symbol": sym,
            "price": data.get("price", 0),
            "change_pct": data.get("change_pct", 0),
            "volume": data.get("volume", 0),
        })

    gainers     = sorted(stocks, key=lambda x: x["change_pct"], reverse=True)[:5]
    losers      = sorted(stocks, key=lambda x: x["change_pct"])[:5]
    most_active = sorted(stocks, key=lambda x: x["volume"], reverse=True)[:5]

    result = {
        "gainers":     gainers,
        "losers":      losers,
        "most_active": most_active,
        "timestamp":   datetime.now().isoformat(),
    }
    _save_movers_disk_cache(result)
    return result


def build_ai_context() -> str:
    """Build market movers context for AI model prompts."""
    movers = get_market_movers()
    parts = []

    top_gainer = movers["gainers"][0] if movers["gainers"] else None
    top_loser = movers["losers"][0] if movers["losers"] else None

    if top_gainer:
        parts.append(f"Top gainer: {top_gainer['symbol']} +{top_gainer['change_pct']}%")
    if top_loser:
        parts.append(f"Top loser: {top_loser['symbol']} {top_loser['change_pct']}%")

    return "Market movers: " + ", ".join(parts) if parts else ""
