"""scripts/deepen_holly_intraday_cache.py — HM-HOLLY-INTRADAY-CACHE-DEEPEN (2026-05-30).

Holly's DAILY path already backtests on ~5 months of history (backtest_market_data,
Jan 6 → May 29). Her INTRADAY cache (holly_intraday_cache_ohlcv) only reached back to
2026-05-11 (~3 weeks) — so faithful-Holly's intraday strategy selection (Batch 1's 14
strategies + every future batch) was being fit on thin intraday data.

Polygon Starter serves 5-minute aggregates back ≥2 years (verified 2026-05-30: a
2024-06 window returns status=OK). This backfills the intraday cache to match the daily
window so intraday strategies validate on comparable depth.

WHY SELF-CONTAINED (not reusing _fetch_polygon_ohlcv): that function is cache-first with
a 12h freshness guard — since the cache already holds fresh May rows (>200), it would
short-circuit and never fetch the older Jan–Apr window. This script fetches Polygon
directly and INSERT-OR-IGNOREs into the SAME table/schema, so it deepens past the guard.

IDEMPOTENT: holly_intraday_cache_ohlcv has UNIQUE(symbol, bar, ts); INSERT OR IGNORE
only fills gaps, never duplicates. KEEPS ALL DATA (sacred-data rule — append only).

Run under .venv-backtest (requests + dotenv available there):
    ./.venv-backtest/bin/python3 scripts/deepen_holly_intraday_cache.py [START_DATE]
START_DATE default 2026-01-02 (matches the daily corpus start).
"""
import os
import sys
import time
import sqlite3
from datetime import date, timedelta, datetime, timezone

# Run-from-anywhere: make the repo root importable and resolve paths from it.
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(_ROOT, ".env"))
except Exception:
    pass

import requests

DB_PATH = os.path.join(_ROOT, "data", "backtest.db")
BAR_MULTIPLIER = 5
BAR_TIMESPAN = "minute"
BAR_LABEL = "5m"          # MUST match engine.holly_intraday.BAR_LABEL exactly
KEY = os.getenv("POLYGON_API_KEY")


def _cache_symbols(conn: sqlite3.Connection) -> list[str]:
    return [r[0] for r in conn.execute(
        "SELECT DISTINCT symbol FROM holly_intraday_cache_ohlcv ORDER BY symbol")]


def _span(conn: sqlite3.Connection, symbol: str | None = None) -> tuple:
    if symbol:
        return conn.execute(
            "SELECT MIN(ts),MAX(ts),COUNT(*) FROM holly_intraday_cache_ohlcv WHERE symbol=?",
            (symbol,)).fetchone()
    return conn.execute(
        "SELECT MIN(ts),MAX(ts),COUNT(*) FROM holly_intraday_cache_ohlcv").fetchone()


def _backfill_symbol(conn: sqlite3.Connection, sym: str, start: date, end: date) -> int:
    """Fetch 5min bars [start, end) in 10-day windows; INSERT OR IGNORE. Returns rows added."""
    now = time.time()
    bars: dict[str, tuple] = {}
    win = start
    while win < end:
        win_end = min(win + timedelta(days=10), end)
        url = (f"https://api.polygon.io/v2/aggs/ticker/{sym}"
               f"/range/{BAR_MULTIPLIER}/{BAR_TIMESPAN}/{win}/{win_end}"
               f"?adjusted=true&sort=asc&limit=50000")
        try:
            r = requests.get(url, headers={"Authorization": f"Bearer {KEY}"}, timeout=30)
            if r.status_code == 200:
                for b in (r.json().get("results") or []):
                    if b.get("t") is None:
                        continue
                    iso = datetime.fromtimestamp(b["t"] / 1000, tz=timezone.utc).isoformat()
                    bars[iso] = (b.get("o", 0), b.get("h", 0), b.get("l", 0),
                                 b.get("c", 0), b.get("v", 0))
            else:
                print(f"    {sym} {win}..{win_end}: HTTP {r.status_code}")
        except Exception as e:
            print(f"    {sym} {win}..{win_end}: ERR {type(e).__name__}: {e}")
        win = win_end
    if not bars:
        return 0
    before = conn.execute("SELECT COUNT(*) FROM holly_intraday_cache_ohlcv WHERE symbol=?",
                          (sym,)).fetchone()[0]
    conn.executemany(
        "INSERT OR IGNORE INTO holly_intraday_cache_ohlcv(symbol,bar,ts,o,h,l,c,v,fetched_at) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        [(sym, BAR_LABEL, ts, o, h, l, c, v, now) for ts, (o, h, l, c, v) in bars.items()])
    conn.commit()
    after = conn.execute("SELECT COUNT(*) FROM holly_intraday_cache_ohlcv WHERE symbol=?",
                         (sym,)).fetchone()[0]
    return after - before


def main() -> int:
    if not KEY:
        print("FAIL: POLYGON_API_KEY missing (load_dotenv failed)")
        return 1

    start = (datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
             if len(sys.argv) > 1 else date(2026, 1, 2))
    end = datetime.now(timezone.utc).date()

    conn = sqlite3.connect(DB_PATH, timeout=60.0)
    conn.execute("""CREATE TABLE IF NOT EXISTS holly_intraday_cache_ohlcv (
        symbol TEXT, bar TEXT, ts TEXT, o REAL, h REAL, l REAL, c REAL, v REAL,
        fetched_at REAL, UNIQUE(symbol, bar, ts))""")
    conn.commit()

    symbols = _cache_symbols(conn)
    before = _span(conn)
    print(f"BEFORE: rows={before[2]} span={before[0]} → {before[1]} "
          f"| symbols={len(symbols)} | backfill {start} → {end}")

    total_added, ok, empty = 0, 0, 0
    for i, sym in enumerate(symbols, 1):
        try:
            added = _backfill_symbol(conn, sym, start, end)
            sp = _span(conn, sym)
            total_added += added
            if added > 0 or sp[2] > 0:
                ok += 1
            else:
                empty += 1
            print(f"  [{i}/{len(symbols)}] {sym}: +{added} → {sp[2]} rows, {sp[0]} → {sp[1]}")
        except Exception as e:
            empty += 1
            print(f"  [{i}/{len(symbols)}] {sym}: ERROR {type(e).__name__}: {e}")

    after = _span(conn)
    conn.close()
    print(f"AFTER: rows={after[2]} span={after[0]} → {after[1]}")
    print(f"SUMMARY: ok={ok} empty={empty} added={total_added} rows")
    if total_added == 0:
        print("WARN: no new rows added (already deep, or fetch failed)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
