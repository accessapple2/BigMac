#!/usr/bin/env python3
"""HM-BK — Polygon gainers/losers poller.

Runs every 5 min during market hours (09:30–16:00 ET, self-gated).
Fetches /v2/snapshot/locale/us/markets/stocks/{gainers,losers}, applies
|pct_change| >= 5% filter, writes to data/trader.db::mover_watchlist.

Self-gating market hours check means the launchd plist can fire every 5 min
all day; off-hours invocations exit cheaply.

Filters that are deferred to phase 2 enrichment:
- market cap >= $500M (needs per-ticker /v3/reference/tickers call)
- optionable=true (needs options chain availability check)
- avg_volume >= 500K (snapshot returns intraday volume only)

For tonight, we capture the raw mover universe; downstream filtering can
be tightened later as enrichment lands.

Run modes:
    python3 scrapers/polygon_movers.py            # one-shot (launchd mode)
    python3 scrapers/polygon_movers.py --verbose  # one-shot with prints
"""
from __future__ import annotations

import os
import sys
import sqlite3
import json
import time
from datetime import datetime, timezone, timedelta
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# Add project root to path for config import
_HERE = os.path.dirname(os.path.abspath(__file__))
_PROJ = os.path.dirname(_HERE)
if _PROJ not in sys.path:
    sys.path.insert(0, _PROJ)

from dotenv import load_dotenv  # type: ignore

load_dotenv(os.path.join(_PROJ, ".env"))

POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY", "")
DB_PATH = os.path.join(_PROJ, "data", "trader.db")
MIN_ABS_PCT_CHANGE = 5.0  # |%| threshold per directive

# Eastern Time market hours: 09:30–16:00 ET. AZ is UTC-7 year-round.
# ET in summer (DST) = UTC-4; ET in winter = UTC-5.
# Simplest: compute current US/Eastern time via timezone delta from UTC.
def _now_et() -> datetime:
    """Return current Eastern Time as naive datetime."""
    # Use system timezone awareness via datetime.now(timezone.utc) + offset.
    # During DST (March-second-Sunday through November-first-Sunday): UTC-4
    # Else: UTC-5
    now_utc = datetime.now(timezone.utc)
    # Crude DST detection: March-November = DST
    is_dst = 3 <= now_utc.month <= 10 or (
        now_utc.month == 11 and now_utc.day < 7
    ) or (now_utc.month == 3 and now_utc.day > 7)
    offset_hours = 4 if is_dst else 5
    return (now_utc - timedelta(hours=offset_hours)).replace(tzinfo=None)


def _is_market_hours() -> bool:
    """09:30–16:00 ET, Mon-Fri."""
    et = _now_et()
    if et.weekday() >= 5:  # 5=Sat, 6=Sun
        return False
    open_time = et.replace(hour=9, minute=30, second=0, microsecond=0)
    close_time = et.replace(hour=16, minute=0, second=0, microsecond=0)
    return open_time <= et <= close_time


def fetch_polygon_snapshot(direction: str) -> list[dict]:
    """Fetch /v2/snapshot/locale/us/markets/stocks/{gainers|losers}."""
    if direction not in ("gainers", "losers"):
        raise ValueError(f"direction must be 'gainers' or 'losers', got {direction!r}")
    url = (
        f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/{direction}"
        f"?apikey={POLYGON_API_KEY}"
    )
    req = Request(url, headers={"User-Agent": "OllieTrades/HM-BK"})
    try:
        with urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
            return data.get("tickers", [])
    except (HTTPError, URLError, json.JSONDecodeError) as e:
        print(f"[HM-BK] fetch {direction} failed: {type(e).__name__}: {e}", file=sys.stderr)
        return []


def parse_ticker(t: dict, direction: str) -> dict | None:
    """Extract canonical row from polygon snapshot ticker entry."""
    symbol = t.get("ticker")
    if not symbol:
        return None
    pct_change = t.get("todaysChangePerc")
    if pct_change is None:
        return None
    if abs(pct_change) < MIN_ABS_PCT_CHANGE:
        return None
    day = t.get("day", {}) or {}
    return {
        "symbol": symbol,
        "last_price": day.get("c") or t.get("lastTrade", {}).get("p"),
        "pct_change": pct_change,
        "day_change_abs": t.get("todaysChange"),
        "volume": day.get("v") or 0,
        "avg_volume": None,  # snapshot doesn't include; deferred enrichment
        "mcap": None,        # deferred enrichment
        "optionable": None,  # deferred enrichment
        "direction": direction[:-1],  # 'gainer' or 'loser'
    }


def upsert_rows(rows: list[dict], verbose: bool = False) -> int:
    """INSERT OR REPLACE into mover_watchlist; return row count written."""
    if not rows:
        return 0
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.executemany(
            """
            INSERT OR REPLACE INTO mover_watchlist
                (symbol, last_price, pct_change, day_change_abs, volume,
                 avg_volume, mcap, optionable, direction, refreshed_at, source)
            VALUES
                (:symbol, :last_price, :pct_change, :day_change_abs, :volume,
                 :avg_volume, :mcap, :optionable, :direction, CURRENT_TIMESTAMP, 'polygon_snapshot')
            """,
            rows,
        )
        conn.commit()
        if verbose:
            print(f"[HM-BK] upserted {len(rows)} rows")
        return len(rows)
    finally:
        conn.close()


def main():
    verbose = "--verbose" in sys.argv or "-v" in sys.argv
    if not POLYGON_API_KEY:
        print("[HM-BK] POLYGON_API_KEY not configured — exit", file=sys.stderr)
        sys.exit(1)

    if not _is_market_hours():
        if verbose:
            print(f"[HM-BK] off-hours ({_now_et()}), skipping")
        sys.exit(0)

    if verbose:
        print(f"[HM-BK] running at {_now_et()} ET")

    rows_total = 0
    for direction in ("gainers", "losers"):
        tickers = fetch_polygon_snapshot(direction)
        if verbose:
            print(f"[HM-BK] fetched {len(tickers)} {direction}")
        parsed = [r for t in tickers if (r := parse_ticker(t, direction)) is not None]
        if verbose:
            print(f"[HM-BK] {len(parsed)} passed |pct_change|>={MIN_ABS_PCT_CHANGE}% filter")
        rows_total += upsert_rows(parsed, verbose=verbose)

    if verbose:
        print(f"[HM-BK] cycle complete: {rows_total} rows written")
    print(f"[HM-BK] {datetime.now().isoformat(timespec='seconds')} cycle: {rows_total} movers")


if __name__ == "__main__":
    main()
