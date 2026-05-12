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
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

from engine.market_data import get_bulk_snapshots
from engine.universe import get_active_universe

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")

# === HM-DASH.1 === squeeze_watch enrichment substrate
_DB_PATH = str(Path(__file__).resolve().parent.parent.parent / "data" / "trader.db")
# Flag tiers per HM-AO-β: WATCH 50-74, ALERT 75-89, PRIORITY 90+. squeeze_flag
# fires at ALERT-tier or above so the Race UI only surfaces high-conviction names.
_SQUEEZE_FLAG_MIN_SCORE = 75.0
# === /HM-DASH.1 ===


@dataclass
class RaceRow:
    rank: int
    ticker: str
    pct_change_since_open: float
    last_price: float
    open_price: float
    volume: int
    market_status: str  # "OPEN" | "PRE" | "AFTER" | "CLOSED"
    # === HM-DASH.1 === squeeze enrichment fields
    squeeze_score: Optional[float] = None      # 0-100, composite_score from squeeze_watch
    squeeze_tier: Optional[str] = None         # "WATCH" | "ALERT" | "PRIORITY"
    squeeze_flag: bool = False                  # convenience: score >= _SQUEEZE_FLAG_MIN_SCORE
    # === /HM-DASH.1 ===


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
    # === HM-DASH.1 === enrich top-N with latest squeeze_watch score
    sq_by_sym = _latest_squeeze_scores([r.ticker for r in top])
    for r in top:
        sq = sq_by_sym.get(r.ticker)
        if sq is not None:
            r.squeeze_score = sq["score"]
            r.squeeze_tier = sq["tier"]
            r.squeeze_flag = sq["score"] >= _SQUEEZE_FLAG_MIN_SCORE
    # === /HM-DASH.1 ===
    return [asdict(r) for r in top]


# === HM-DASH.1 === squeeze_watch batch lookup
def _latest_squeeze_scores(tickers: list[str]) -> dict[str, dict[str, Any]]:
    """Return latest non-dismissed squeeze_watch row per ticker.

    Single SQL query for the whole top-N set; returns {} on any DB error so the
    Race endpoint never fails because of squeeze enrichment.
    """
    if not tickers:
        return {}
    placeholders = ",".join("?" * len(tickers))
    try:
        conn = sqlite3.connect(_DB_PATH, timeout=5)
        conn.row_factory = sqlite3.Row
        # Get the highest-score row per symbol from non-dismissed watch entries.
        # Multiple rows can exist per symbol over time; we pick the latest tier.
        sql = f"""
            SELECT symbol, composite_score, threshold_tier
            FROM squeeze_watch
            WHERE symbol IN ({placeholders}) AND dismissed = 0
            ORDER BY symbol ASC, scan_ts DESC
        """
        seen: dict[str, dict[str, Any]] = {}
        for row in conn.execute(sql, tickers).fetchall():
            sym = row["symbol"]
            if sym in seen:
                continue  # keep first (most recent scan_ts due to ORDER BY)
            seen[sym] = {
                "score": float(row["composite_score"] or 0),
                "tier": row["threshold_tier"] or "WATCH",
            }
        conn.close()
        return seen
    except Exception as e:
        logger.warning(f"_latest_squeeze_scores DB error (returning empty): {e}")
        return {}
# === /HM-DASH.1 ===


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
