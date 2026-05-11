"""
Race tile data producer.

Top gainers since the regular-session open, sorted by pct change.
Computed from one batched Alpaca snapshots call covering the active
universe (~1,300 names today via engine.universe.get_active_universe).

Phase 2 v1. Phase 4 detail-panel will reuse the same compute and add
multi-timeframe data on click-through.
"""
from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from engine.market_data import get_bulk_snapshots
from engine.universe import get_active_universe

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")


@dataclass
class RaceRow:
    rank: int
    ticker: str
    pct_change_since_open: float
    last_price: float
    open_price: float
    volume: int
    market_status: str  # "OPEN" | "PRE" | "AFTER" | "CLOSED"


def compute_race(limit: int = 20) -> list[dict[str, Any]]:
    """Top `limit` gainers since open. Empty list if no snapshot data."""
    universe = get_active_universe()
    if not universe:
        logger.warning("active universe is empty; returning []")
        return []
    snaps = get_bulk_snapshots(universe)
    if not snaps:
        logger.warning("get_bulk_snapshots returned empty; returning []")
        return []
    status = _market_status_now()
    rows: list[RaceRow] = []
    for sym, s in snaps.items():
        opn = s.get("open_price")
        last = s.get("last_price")
        if not opn or opn <= 0 or last is None:
            continue
        pct = (last - opn) / opn * 100.0
        rows.append(RaceRow(
            rank=0,
            ticker=sym,
            pct_change_since_open=round(pct, 2),
            last_price=float(last),
            open_price=float(opn),
            volume=int(s.get("volume") or 0),
            market_status=status,
        ))
    rows.sort(key=lambda r: r.pct_change_since_open, reverse=True)
    top = rows[:limit]
    for i, r in enumerate(top, 1):
        r.rank = i
    return [asdict(r) for r in top]


def _market_status_now() -> str:
    """Naive US-Eastern session label. Weekend = CLOSED; holiday-unaware."""
    now = datetime.now(_ET)
    if now.weekday() >= 5:
        return "CLOSED"
    mins = now.hour * 60 + now.minute
    if mins < 4 * 60:
        return "CLOSED"
    if mins < 9 * 60 + 30:
        return "PRE"
    if mins < 16 * 60:
        return "OPEN"
    if mins < 20 * 60:
        return "AFTER"
    return "CLOSED"
