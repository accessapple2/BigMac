"""Money Machine -- auto-identify and buy top 3 highest-momentum stocks."""
from __future__ import annotations
import json
import sqlite3
from pathlib import Path
from datetime import datetime
from rich.console import Console

import config

console = Console()
DB = "data/trader.db"
STATE_FILE = Path("data/money_machine_state.json")


def identify_momentum_stocks(top_n: int = 3) -> list:
    """Identify top N stocks by momentum (biggest % move + highest relative volume)."""
    from engine.market_data import get_all_prices, _yahoo_chart

    prices = get_all_prices(config.WATCH_STOCKS)

    scored = []
    for sym, data in prices.items():
        pct = abs(data.get("change_pct", 0))
        vol = data.get("volume", 0)

        # Get average volume for relative volume calc
        chart = _yahoo_chart(sym, interval="1d", range_="1mo")
        avg_vol = 0
        if chart:
            quotes = chart.get("indicators", {}).get("quote", [{}])[0]
            vols = [v for v in (quotes.get("volume") or []) if v is not None]
            if len(vols) > 1:
                avg_vol = sum(vols[:-1]) / len(vols[:-1])

        rel_vol = vol / avg_vol if avg_vol > 0 else 1.0
        # Momentum score = % change * relative volume
        momentum_score = round(pct * rel_vol, 2)

        scored.append(
            {
                "symbol": sym,
                "price": data.get("price", 0),
                "change_pct": data.get("change_pct", 0),
                "volume": vol,
                "rel_volume": round(rel_vol, 1),
                "momentum_score": momentum_score,
            }
        )

    scored.sort(key=lambda x: x["momentum_score"], reverse=True)
    return scored[:top_n]


def get_status() -> dict:
    """Get Money Machine current status."""
    # Load state
    state = {"active": False, "positions": [], "last_scan": None}
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text())
        except Exception:
            pass

    # Get current momentum leaders
    top = identify_momentum_stocks()

    return {
        "active": state.get("active", False),
        "momentum_leaders": top,
        "positions": state.get("positions", []),
        "last_scan": state.get("last_scan"),
    }


def save_state(state: dict):
    """Save Money Machine state."""
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))
