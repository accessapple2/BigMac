"""Stock Racing -- real-time intraday % change race visualization."""
from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.console import Console

import config

console = Console()


def get_stock_race() -> list:
    """Get top 10 watchlist stocks ranked by intraday % change.
    Returns list sorted by change_pct descending.
    """
    from engine.market_data import get_all_prices

    prices = get_all_prices(config.WATCH_STOCKS)

    results = []
    for sym, data in prices.items():
        results.append(
            {
                "symbol": sym,
                "price": data.get("price", 0),
                "change_pct": data.get("change_pct", 0),
                "volume": data.get("volume", 0),
                "source": data.get("source", ""),
            }
        )

    results.sort(key=lambda x: x["change_pct"], reverse=True)
    return results[:16]  # All watchlist stocks
