#!/usr/bin/env python3
"""
backfill_realized_return.py — Carrier P1 · fill fwd_return_1d_realized for
all already-evaluated rows that are missing it.

SAFETY:
  - Read-only Alpaca API calls (GET /v2/stocks/{sym}/bars).
  - Writes ONLY fwd_return_1d_realized to signal_observations; never touches
    fwd_return_1d or any other column.
  - Idempotent: skips rows where fwd_return_1d_realized IS NOT NULL.
  - Uncomputable rows (missing price history, API errors) stay NULL — never faked.
  - Caches bars per (ticker, ts_date, expiry_date) — avoids duplicate API calls
    for the same ticker+date pair.
  - No restart needed; safe to run alongside the live trader.

Usage:
  .venv/bin/python3 _nightcrew/backfill_realized_return.py
  .venv/bin/python3 _nightcrew/backfill_realized_return.py --dry-run
  .venv/bin/python3 _nightcrew/backfill_realized_return.py --sample 20
"""

import argparse, os, sqlite3, sys, time
from datetime import date as _date, datetime, timedelta, timezone
try:
    from dotenv import load_dotenv
    load_dotenv(override=True)
except ImportError:
    pass

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

_DB      = os.path.join(_REPO_ROOT, "data", "trader.db")
_ALPACA  = "https://data.alpaca.markets/v2/stocks/{sym}/bars"
_RATE_S  = 0.22   # ~4.5 req/s — well inside Alpaca free tier


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _fetch_closes(ticker: str, ts_date: str, expiry_date: str, _cache: dict) -> dict:
    """Return {date_str: close_price} for the given ticker and date window.
    Uses _cache to avoid duplicate API calls.
    """
    cache_key = (ticker, ts_date, expiry_date)
    if cache_key in _cache:
        return _cache[cache_key]

    try:
        import requests
        key    = os.environ.get("APCA_API_KEY_ID", "")
        secret = os.environ.get("APCA_API_SECRET_KEY", "")
        if not key or not secret:
            _cache[cache_key] = {}
            return {}

        end_date = (_date.fromisoformat(expiry_date) + timedelta(days=2)).isoformat()
        r = requests.get(
            _ALPACA.format(sym=ticker),
            headers={"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret},
            params={
                "timeframe": "1Day",
                "start":     ts_date,
                "end":       end_date,
                "feed":      "iex",
                "sort":      "asc",
                "limit":     10,
            },
            timeout=10,
        )
        if not r.ok:
            _cache[cache_key] = {}
            return {}

        closes: dict[str, float] = {}
        for b in (r.json().get("bars") or []):
            bar_date = (b.get("t") or "")[:10]
            if bar_date:
                closes[bar_date] = float(b["c"])

        _cache[cache_key] = closes
        return closes

    except Exception:
        _cache[cache_key] = {}
        return {}


def _realized(ticker: str, ts_iso: str, expiry_iso: str, _cache: dict) -> float | None:
    ts_date     = ts_iso[:10]
    expiry_date = expiry_iso[:10]
    closes      = _fetch_closes(ticker, ts_date, expiry_date, _cache)
    if not closes:
        return None

    sorted_dates = sorted(closes)
    entry_close  = closes.get(ts_date) or closes.get(sorted_dates[0])
    expiry_close = closes.get(expiry_date) or closes.get(sorted_dates[-1])

    if entry_close is None or expiry_close is None or entry_close <= 0:
        return None
    return round((expiry_close - entry_close) / entry_close, 6)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=_DB)
    ap.add_argument("--dry-run", action="store_true",
                    help="Compute but do not write — print comparison table")
    ap.add_argument("--sample", type=int, default=0,
                    help="Limit to N rows (0 = all)")
    ap.add_argument("--sleep", type=float, default=_RATE_S,
                    help="Sleep between API calls (seconds)")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA journal_mode=WAL")

    # Fetch pending rows
    limit_sql = f"LIMIT {args.sample}" if args.sample > 0 else ""
    rows = conn.execute(f"""
        SELECT id, ticker, ts, expiry, fwd_return_1d
          FROM signal_observations
         WHERE evaluated_at IS NOT NULL
           AND fwd_return_1d_realized IS NULL
           AND expiry IS NOT NULL
         ORDER BY ts ASC
         {limit_sql}
    """).fetchall()

    total  = len(rows)
    filled = skipped = errors = api_calls = 0
    _cache: dict = {}

    print(f"[{_now_iso()}] START  pending={total}  db={args.db}"
          + ("  DRY-RUN" if args.dry_run else ""))

    if args.dry_run:
        print(f"\n{'ticker':<8} {'ts_date':<12} {'exp_date':<12} "
              f"{'projected':>12} {'realized':>12} {'delta':>10}")
        print("-" * 70)

    for i, row in enumerate(rows):
        obs_id     = row["id"]
        ticker     = row["ticker"]
        ts         = row["ts"]
        expiry     = row["expiry"]
        projected  = row["fwd_return_1d"]

        cache_key = (ticker, ts[:10], expiry[:10])
        if cache_key not in _cache:
            api_calls += 1
            time.sleep(args.sleep)

        realized = _realized(ticker, ts, expiry, _cache)

        if args.dry_run:
            proj_s = f"{projected*100:+.3f}%" if projected is not None else "null"
            real_s = f"{realized*100:+.3f}%"  if realized  is not None else "null"
            delta  = ""
            if projected is not None and realized is not None:
                delta = f"{(realized-projected)*100:+.3f}%"
            print(f"{ticker:<8} {ts[:10]:<12} {expiry[:10]:<12} "
                  f"{proj_s:>12} {real_s:>12} {delta:>10}")
            filled += 1
            continue

        if realized is not None:
            try:
                conn.execute(
                    "UPDATE signal_observations SET fwd_return_1d_realized=? WHERE id=?",
                    (realized, obs_id),
                )
                conn.commit()
                filled += 1
            except Exception as e:
                errors += 1
                print(f"[WARN] row {obs_id} write failed: {e}")
        else:
            skipped += 1

        if (i + 1) % 100 == 0:
            print(f"[{_now_iso()}] progress  {i+1}/{total}  "
                  f"filled={filled}  skipped(null)={skipped}  "
                  f"api_calls={api_calls}  errors={errors}")

    conn.close()
    print(f"\n[{_now_iso()}] DONE  total={total}  filled={filled}  "
          f"skipped(null)={skipped}  api_calls={api_calls}  errors={errors}")
    if not args.dry_run:
        print("NEXT: capture /api/observations/summary for the real alpha read.")


if __name__ == "__main__":
    main()
