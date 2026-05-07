"""HM-AQ-β 2026-05-07 — Active universe accessor.

Replaces `from config import WATCH_STOCKS` (deleted in commit 4).

Reads from `scan_universe` table populated weekly by
`engine/universe_refresh.py` (Polygon-driven, Sundays 14:00 MST).
Filters at read time to ensure consumers always get a fresh,
criteria-matching universe even between refreshes.

Criteria (per HM-AQ Captain decision, docs/UNIVERSE.md):
- market_cap >= $5B
- avg_dollar_volume = avg_volume * avg_price >= $50M
- last_updated within the last 14 days (staleness guard)

Returns a list[str] for callers that previously iterated WATCH_STOCKS,
or list[dict] for callers needing metadata (cap/volume/options_eligible).

30-second TTL cache: callers may import and re-call across many
scheduler ticks; rereading 500-800 rows from SQLite each call is wasteful.
The cache is process-local (no need to share across the trader+dashboard
because both run in the same uvicorn process per CLAUDE.md).
"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Optional

_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "trader.db"

# Filter thresholds — Captain decision 2026-05-07 (HM-AQ).
MIN_MARKET_CAP = 5_000_000_000.0      # $5B (stocks only — ETFs use volume-only filter)
# HM-AQ-β v3 2026-05-07: threshold raised from $50M → $100M after v2 dry-run
# surfaced 1,554 finalists (would strain dashboard latency at chunks of 50 ≈
# 3.1s/snapshot). $100M still catches DDOG/FTNT/MDB and other "missed mover"
# names; drops thin-trading mid-caps that add noise without value.
# Predicted v3 final count: 600-900.
MIN_DOLLAR_VOLUME = 100_000_000.0     # $100M avg daily (v3 raise)
MAX_STALENESS_DAYS = 14               # rolling 14-day window after last refresh

# Captain refinement 2026-05-07 during HM-AQ-β dry-run: ETF inclusion.
# ETFs lack a market_cap analog from Polygon (they have AUM, not cap), so the
# cap filter would exclude every ETF — losing TQQQ, IWM, XLE, sector SPDRs, etc.
# Resolution: ETFs included on dollar_volume parity with stocks; ETNs skipped.
ETF_DOLLAR_VOLUME_THRESHOLD = MIN_DOLLAR_VOLUME  # $100M parity (v3)
INCLUDE_ETFS = True                              # Captain 2026-05-07
INCLUDE_ETNS = False                             # debt notes — different risk profile

# 30s TTL cache — process-local.
_CACHE_TTL = 30.0
_cache_universe: Optional[tuple[float, list[str]]] = None
_cache_metadata: Optional[tuple[float, list[dict]]] = None

# Fail-safe fallback: if scan_universe is empty/missing, callers shouldn't
# crash. Returns the legacy mega-cap list so the system remains operational
# during a refresh-failure window.
_FALLBACK_UNIVERSE = [
    "SPY", "QQQ", "TQQQ", "NVDA", "TSLA", "AAPL", "AMD", "META", "MSFT",
    "GOOGL", "AMZN", "MU", "ORCL", "NOW", "AVGO", "PLTR", "DELL", "XLE",
    "INTC", "NUKZ",
]

# HM-AQ-β v2 2026-05-07: type-aware filter to support ETF inclusion.
# CS rows: require market_cap >= MIN_MARKET_CAP AND dollar_volume >= MIN_DOLLAR_VOLUME.
# ETF rows: require dollar_volume >= ETF_DOLLAR_VOLUME_THRESHOLD only (no cap analog).
# ETN rows: filtered out by the refresher itself (never written), but the SQL
# also excludes them defensively.
# Order by `last_updated DESC, COALESCE(market_cap, 0) DESC` so freshest rows
# come first, then by cap within the same refresh batch (ETFs sort last on cap;
# fine — they're a smaller subset).
_BASE_SQL = (
    "SELECT symbol, market_cap, avg_volume, avg_price, options_eligible, "
    "       last_updated, ticker_type "
    "FROM scan_universe "
    "WHERE last_updated > datetime('now', '-' || ? || ' days') "
    "  AND ticker_type != 'ETN' "
    "  AND ( "
    "        (ticker_type = 'CS' AND market_cap >= ? AND (avg_volume * avg_price) >= ?) "
    "     OR (ticker_type = 'ETF' AND (avg_volume * avg_price) >= ?) "
    "  ) "
    "ORDER BY COALESCE(market_cap, 0) DESC, avg_volume DESC"
)


def _conn() -> sqlite3.Connection:
    return sqlite3.connect(_DB_PATH, timeout=10)


def _query() -> list[dict]:
    """Run the universe filter query. Returns list[dict] with metadata.

    Parameters bound (per _BASE_SQL above):
      ?1 = MAX_STALENESS_DAYS
      ?2 = MIN_MARKET_CAP        (CS branch)
      ?3 = MIN_DOLLAR_VOLUME     (CS branch)
      ?4 = ETF_DOLLAR_VOLUME_THRESHOLD (ETF branch)
    """
    try:
        with _conn() as c:
            c.row_factory = sqlite3.Row
            rows = c.execute(
                _BASE_SQL,
                (MAX_STALENESS_DAYS, MIN_MARKET_CAP, MIN_DOLLAR_VOLUME,
                 ETF_DOLLAR_VOLUME_THRESHOLD),
            ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


def get_active_universe(force_refresh: bool = False) -> list[str]:
    """Return active universe as list[str] (drop-in for old WATCH_STOCKS).

    Set force_refresh=True to bypass the 30s cache (useful for tests).

    Falls back to a 20-name mega-cap list if scan_universe is empty
    (refresh never ran, or cleared) so the system stays operational.
    """
    global _cache_universe
    now = time.time()
    if not force_refresh and _cache_universe is not None:
        ts, syms = _cache_universe
        if now - ts < _CACHE_TTL:
            return syms
    rows = _query()
    if not rows:
        # Fail-safe: refresh never populated, table empty, or query failed.
        # Return the legacy 20-name list so consumers stay operational.
        # The refresher logs/NTFYs on failure; this just keeps the boat afloat.
        syms = list(_FALLBACK_UNIVERSE)
    else:
        syms = [r["symbol"] for r in rows]
    _cache_universe = (now, syms)
    return syms


def get_universe_with_metadata(force_refresh: bool = False) -> list[dict]:
    """Return active universe with full metadata (market_cap, volume, options).

    Each dict has keys: symbol, market_cap, avg_volume, avg_price,
    options_eligible, last_updated.

    Returns empty list if the refresher hasn't populated the table yet —
    callers needing metadata should handle the empty case explicitly
    (unlike `get_active_universe()` which fails open to the fallback list).
    """
    global _cache_metadata
    now = time.time()
    if not force_refresh and _cache_metadata is not None:
        ts, meta = _cache_metadata
        if now - ts < _CACHE_TTL:
            return meta
    meta = _query()
    _cache_metadata = (now, meta)
    return meta


def get_options_eligible_universe(force_refresh: bool = False) -> list[str]:
    """Return only symbols with options_eligible=1.

    Useful for option-strategy callers that need the broader cap+volume
    filter PLUS confirmation that Polygon Options has active contracts.
    """
    return [
        r["symbol"]
        for r in get_universe_with_metadata(force_refresh=force_refresh)
        if r.get("options_eligible")
    ]


def universe_health() -> dict:
    """Return health metrics for the universe — useful for the dashboard.

    Provides row count, last refresh, count passing filters (split CS vs ETF),
    and whether the system is on the fallback list.
    """
    try:
        with _conn() as c:
            c.row_factory = sqlite3.Row
            total = c.execute("SELECT COUNT(*) AS n FROM scan_universe").fetchone()["n"]
            last_refresh = c.execute(
                "SELECT MAX(last_updated) AS ts FROM scan_universe"
            ).fetchone()["ts"]
            cs_passing = c.execute(
                "SELECT COUNT(*) AS n FROM scan_universe "
                "WHERE ticker_type = 'CS' AND market_cap >= ? "
                "AND (avg_volume * avg_price) >= ? "
                "AND last_updated > datetime('now', '-' || ? || ' days')",
                (MIN_MARKET_CAP, MIN_DOLLAR_VOLUME, MAX_STALENESS_DAYS),
            ).fetchone()["n"]
            etf_passing = c.execute(
                "SELECT COUNT(*) AS n FROM scan_universe "
                "WHERE ticker_type = 'ETF' AND (avg_volume * avg_price) >= ? "
                "AND last_updated > datetime('now', '-' || ? || ' days')",
                (ETF_DOLLAR_VOLUME_THRESHOLD, MAX_STALENESS_DAYS),
            ).fetchone()["n"]
        passing = cs_passing + etf_passing
        on_fallback = passing == 0
        return {
            "total_rows": total,
            "passing_filter": passing,
            "cs_passing": cs_passing,
            "etf_passing": etf_passing,
            "last_refresh": last_refresh,
            "on_fallback": on_fallback,
            "fallback_size": len(_FALLBACK_UNIVERSE) if on_fallback else 0,
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e!r}"}


__all__ = [
    "get_active_universe",
    "get_universe_with_metadata",
    "get_options_eligible_universe",
    "universe_health",
    "MIN_MARKET_CAP",
    "MIN_DOLLAR_VOLUME",
    "MAX_STALENESS_DAYS",
]
