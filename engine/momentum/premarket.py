"""
Pre-market gap scanner (Dashboard Remodel v2.1 / Phase 6).

Window: 04:00–09:30 ET. Outside that window, compute_premarket() returns
hits=[] unless caller passes force=True (for testing or regular-hours
preview from the UI).

Filter:
  |gap_pct| >= MIN_GAP_PCT (default 3.0)
  AND premarket_volume >= MIN_VOLUME (default 50_000)

Output sorted by absolute gap_pct, descending.

Architecture note — parallel, not replacement:
  engine/premarket_scanner.py is the LEGACY pre-market scanner — per-symbol
  yfinance + Finviz scraper, runs nightly via launchd, writes to
  data/premarket_gaps.json + DB, consumed by engine.ai_brain via the
  /api/premarket-gaps endpoint. We do NOT touch any of that.

  This module is the NEW UI-side scanner: one batched Alpaca snapshots
  call over the full active universe, polled every 60s by the UI tile.
  Read-only — no DB writes, no AI analysis.

Field semantics (verified against engine/market_data.py:268):
  snap['prev_close']     -> from prevDailyBar.c
  snap['last_price']     -> from latestTrade.p (includes pre-market trades on IEX feed)
  snap['volume']         -> from dailyBar.v (cumulative session-so-far volume,
                            equals cumulative pre-market volume during 04:00–09:30 ET)

Flags integration deferred. engine.momentum.flags doesn't exist yet
(separate ticket). flags=[] always for v1.
"""
from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

from engine.market_data import get_bulk_snapshots
from engine.momentum.race import _market_status_now
from engine.universe import get_active_universe

logger = logging.getLogger(__name__)

MIN_GAP_PCT = 3.0
MIN_VOLUME = 50_000
DEFAULT_LIMIT = 30


@dataclass
class PremarketHit:
    rank: int
    ticker: str
    gap_pct: float
    prev_close: float
    premarket_price: float
    premarket_volume: int
    flags: list
    direction: str
    market_status: str


def compute_premarket(limit: int = DEFAULT_LIMIT, force: bool = False) -> dict[str, Any]:
    """Compute pre-market gap hits.

    Args:
        limit: max hits to return (clamped to [1, 100])
        force: bypass window gate (for testing / off-hours preview)

    Returns dict with keys:
        ts:           ISO UTC timestamp
        window_state: "PRE" | "OPEN" | "AFTER" | "CLOSED" (from race._market_status_now)
        hits:         list of hit dicts, or empty list
        error:        present only on failure (e.g. snapshot_fetch_failed)
    """
    status = _market_status_now()
    now_ts = datetime.utcnow().isoformat() + "Z"

    if not force and status != "PRE":
        return {"ts": now_ts, "window_state": status, "hits": []}

    universe = get_active_universe()
    if not universe:
        logger.warning("get_active_universe() returned empty")
        return {"ts": now_ts, "window_state": status, "hits": []}

    snapshots = get_bulk_snapshots(universe)
    if not snapshots:
        logger.warning("get_bulk_snapshots returned empty for universe size=%d", len(universe))
        return {"ts": now_ts, "window_state": status, "hits": [], "error": "snapshot_fetch_failed"}

    hits: list[PremarketHit] = []
    skipped_no_prev = 0
    skipped_filter = 0
    for ticker, snap in snapshots.items():
        try:
            prev_close = snap.get("prev_close")
            premarket_price = snap.get("last_price")
            volume = int(snap.get("volume") or 0)

            if not prev_close or prev_close <= 0 or not premarket_price:
                skipped_no_prev += 1
                continue

            gap_pct = (premarket_price - prev_close) / prev_close * 100.0
            if abs(gap_pct) < MIN_GAP_PCT or volume < MIN_VOLUME:
                skipped_filter += 1
                continue

            hits.append(PremarketHit(
                rank=0,
                ticker=ticker,
                gap_pct=round(gap_pct, 2),
                prev_close=round(float(prev_close), 2),
                premarket_price=round(float(premarket_price), 2),
                premarket_volume=volume,
                flags=[],
                direction="UP" if gap_pct > 0 else "DOWN",
                market_status=status,
            ))
        except (AttributeError, TypeError, ValueError) as e:
            logger.debug("premarket parse failed for %s: %s", ticker, e)
            continue

    hits.sort(key=lambda h: abs(h.gap_pct), reverse=True)
    clamped = max(1, min(limit, 100))
    top = hits[:clamped]
    for i, h in enumerate(top, 1):
        h.rank = i

    if not top:
        logger.info(
            "premarket: no hits (universe=%d, snaps=%d, skipped_no_prev=%d, skipped_filter=%d, status=%s)",
            len(universe), len(snapshots), skipped_no_prev, skipped_filter, status,
        )

    return {
        "ts": now_ts,
        "window_state": status,
        "hits": [asdict(h) for h in top],
    }
