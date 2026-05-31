"""scripts/build_smallcap_mover_universe.py — HM-HOLLY-UNIVERSE-FIX (2026-05-31).

THE CATCH (Scotty's, Admiral-ruled): Holly's TI strategies are small-cap-momentum, but
the intraday cache held 13 large-caps (CB/CME/COST/IBM…). The universe selector
(_get_top_volume_movers) ranks universe_scan by volume_ratio with NO price/cap filter, so
it grabbed high-$ names. Backtesting momentum strategies on large-caps falsely makes Holly
lose. Fix: build the SMALL-CAP MOVER universe the strategies actually target, deepen its
5min cache, and re-validate Batch 1+2 there.

UNIVERSE DEFINITION (derived from the TI strategy price bands, not guessed):
  float_on $1-12 · five_day_bounce ≤$20 · the_continuation $0.5-50 · breakout $10-150 …
  → the common small-cap-mover band is $1-50 with a MOVER filter. We reconstruct it from
  the REAL mover history in universe_scan (46 scan-days, Mar 21–May 29):
    close ∈ [1, 50]  AND  volume_ratio ≥ 2  (a genuine relative-volume mover)
  ranked by how OFTEN each name appeared as a mover (appearances) then avg volume_ratio.
  This is faithful to how Holly works — it scans the day's movers — reconstructed as the
  union of names that were actually movers over the window (dynamic, not a static guess).
  Low-float $1-12 names (LCID/SOUN/ACHR…) are deliberately retained so float_on engages.

Pulls 5min ~5mo (matching the daily corpus depth) into holly_intraday_cache_ohlcv.
IDEMPOTENT (UNIQUE(symbol,bar,ts) + INSERT OR IGNORE). Append-only (sacred-data).

    ./.venv-backtest/bin/python3 scripts/build_smallcap_mover_universe.py [N] [START]
N default 30 symbols; START default 2026-01-02.
"""
import os
import sys
import time
import sqlite3
from datetime import date, datetime, timedelta, timezone

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(_ROOT, ".env"))
except Exception:
    pass
import requests

TRADER_DB = os.path.join(_ROOT, "data", "trader.db")
BT_DB = os.path.join(_ROOT, "data", "backtest.db")
KEY = os.getenv("POLYGON_API_KEY")
BAR_MULT, BAR_SPAN, BAR_LABEL = 5, "minute", "5m"

# Small-cap mover band (derived from TI strategy price bands).
PRICE_LO, PRICE_HI, MIN_VOL_RATIO = 1.0, 50.0, 2.0


def select_universe(n: int) -> list[str]:
    """Top-N small-cap movers from universe_scan by appearances then avg volume_ratio."""
    c = sqlite3.connect(TRADER_DB)
    rows = c.execute(
        """SELECT ticker, COUNT(*) app, AVG(close) px, AVG(volume_ratio) vr
           FROM universe_scan
           WHERE close BETWEEN ? AND ? AND volume_ratio >= ?
           GROUP BY ticker
           ORDER BY app DESC, vr DESC""",
        (PRICE_LO, PRICE_HI, MIN_VOL_RATIO),
    ).fetchall()
    c.close()
    return [r[0] for r in rows[:n]]


def backfill(conn: sqlite3.Connection, sym: str, start: date, end: date) -> int:
    now = time.time()
    bars: dict = {}
    win = start
    while win < end:
        we = min(win + timedelta(days=10), end)
        url = (f"https://api.polygon.io/v2/aggs/ticker/{sym}"
               f"/range/{BAR_MULT}/{BAR_SPAN}/{win}/{we}?adjusted=true&sort=asc&limit=50000")
        try:
            r = requests.get(url, headers={"Authorization": f"Bearer {KEY}"}, timeout=30)
            if r.status_code == 200:
                for b in (r.json().get("results") or []):
                    if b.get("t") is None:
                        continue
                    iso = datetime.fromtimestamp(b["t"] / 1000, tz=timezone.utc).isoformat()
                    bars[iso] = (b.get("o", 0), b.get("h", 0), b.get("l", 0), b.get("c", 0), b.get("v", 0))
        except Exception as e:
            print(f"    {sym} {win}: ERR {type(e).__name__}")
        win = we
    if not bars:
        return 0
    before = conn.execute("SELECT COUNT(*) FROM holly_intraday_cache_ohlcv WHERE symbol=?", (sym,)).fetchone()[0]
    conn.executemany(
        "INSERT OR IGNORE INTO holly_intraday_cache_ohlcv(symbol,bar,ts,o,h,l,c,v,fetched_at) VALUES (?,?,?,?,?,?,?,?,?)",
        [(sym, BAR_LABEL, ts, o, h, l, c, v, now) for ts, (o, h, l, c, v) in bars.items()])
    conn.commit()
    after = conn.execute("SELECT COUNT(*) FROM holly_intraday_cache_ohlcv WHERE symbol=?", (sym,)).fetchone()[0]
    return after - before


def main() -> int:
    if not KEY:
        print("FAIL: POLYGON_API_KEY missing")
        return 1
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    start = (datetime.strptime(sys.argv[2], "%Y-%m-%d").date() if len(sys.argv) > 2 else date(2026, 1, 2))
    end = datetime.now(timezone.utc).date()

    universe = select_universe(n)
    print(f"SMALL-CAP MOVER UNIVERSE ({len(universe)} symbols, ${PRICE_LO:.0f}-{PRICE_HI:.0f}, vr>={MIN_VOL_RATIO}):")
    print("  " + ", ".join(universe))

    conn = sqlite3.connect(BT_DB, timeout=60.0)
    conn.execute("""CREATE TABLE IF NOT EXISTS holly_intraday_cache_ohlcv (
        symbol TEXT, bar TEXT, ts TEXT, o REAL, h REAL, l REAL, c REAL, v REAL,
        fetched_at REAL, UNIQUE(symbol, bar, ts))""")
    conn.commit()
    # persist the universe for the re-validation step + future runs
    conn.execute("CREATE TABLE IF NOT EXISTS holly_smallcap_universe (symbol TEXT PRIMARY KEY, added_at REAL)")
    for s in universe:
        conn.execute("INSERT OR IGNORE INTO holly_smallcap_universe(symbol,added_at) VALUES (?,?)", (s, time.time()))
    conn.commit()

    ok = tot = 0
    for i, s in enumerate(universe, 1):
        added = backfill(conn, s, start, end)
        sp = conn.execute("SELECT MIN(ts),MAX(ts),COUNT(*) FROM holly_intraday_cache_ohlcv WHERE symbol=?", (s,)).fetchone()
        tot += added
        if sp[2] > 0:
            ok += 1
        print(f"  [{i}/{len(universe)}] {s}: +{added} → {sp[2]} rows, {str(sp[0])[:10]} → {str(sp[1])[:10]}")
    conn.close()
    print(f"SUMMARY: {ok}/{len(universe)} symbols have data, +{tot} rows")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
