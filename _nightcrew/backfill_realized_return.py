#!/usr/bin/env python3
"""
backfill_realized_return.py — Carrier P1 / HM-REALIZED-RETRY · one-time
throttled pass to fill fwd_return_1d_realized for the backlog of rows
stuck by the pre-2026-07-15 bug (realized-fetch attempted before Alpaca's
daily bar for the expiry date existed, then permanently abandoned with no
retry — see engine/signal_evaluator.py module docstring). Every targeted
row's expiry is long past by the time this runs, so the bar-availability
gate that evaluate_realized_pending() applies going forward is moot here;
this pass tries once, immediately, for everything still stuck.

SAFETY:
  - Read-only Alpaca API calls (GET /v2/stocks/{sym}/bars).
  - Writes ONLY fwd_return_1d_realized + realized_at/realized_attempts/
    realized_fail_reason to signal_observations; never touches evaluated_at,
    fwd_return_1d, acted_by_fleet, or any other column.
  - Idempotent: skips rows where realized_at IS NOT NULL.
  - Uncomputable rows (delisted ticker, missing price history, API errors)
    are marked realized_at + realized_fail_reason — never faked as 0.0, and
    never left silently NULL for evaluate_realized_pending() to retry forever.
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


def _fetch_closes(ticker: str, ts_date: str, expiry_date: str, _cache: dict) -> tuple[dict, str | None]:
    """Return ({date_str: close_price}, fail_reason). fail_reason is None
    when closes is non-empty. Uses _cache to avoid duplicate API calls.
    """
    cache_key = (ticker, ts_date, expiry_date)
    if cache_key in _cache:
        return _cache[cache_key]

    try:
        import requests
        key    = os.environ.get("APCA_API_KEY_ID", "")
        secret = os.environ.get("APCA_API_SECRET_KEY", "")
        if not key or not secret:
            result = ({}, "no_keys")
            _cache[cache_key] = result
            return result

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
            result = ({}, f"http_{r.status_code}")
            _cache[cache_key] = result
            return result

        closes: dict[str, float] = {}
        for b in (r.json().get("bars") or []):
            bar_date = (b.get("t") or "")[:10]
            if bar_date:
                closes[bar_date] = float(b["c"])

        result = (closes, None) if closes else ({}, "no_bars")
        _cache[cache_key] = result
        return result

    except Exception as exc:
        result = ({}, f"error:{type(exc).__name__}")
        _cache[cache_key] = result
        return result


def _realized(ticker: str, ts_iso: str, expiry_iso: str, _cache: dict) -> tuple[float | None, str | None]:
    ts_date     = ts_iso[:10]
    expiry_date = expiry_iso[:10]
    closes, reason = _fetch_closes(ticker, ts_date, expiry_date, _cache)
    if not closes:
        return None, reason

    sorted_dates = sorted(closes)

    entry_date_used  = ts_date     if ts_date     in closes else (sorted_dates[0]  if sorted_dates else None)
    expiry_date_used = expiry_date if expiry_date in closes else (sorted_dates[-1] if sorted_dates else None)

    # Same bar for both endpoints → no valid window → NULL, not 0.0
    if entry_date_used is None or expiry_date_used is None:
        return None, "no_bars"
    if entry_date_used == expiry_date_used:
        return None, "same_bar"

    entry_close  = closes[entry_date_used]
    expiry_close = closes[expiry_date_used]

    if entry_close <= 0:
        return None, "bad_close"
    return round((expiry_close - entry_close) / entry_close, 6), None


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

    # Fetch pending rows — realized_at IS NULL (not fwd_return_1d_realized
    # IS NULL) is the authoritative "still stuck" filter post-HM-REALIZED-RETRY;
    # direction IS NOT NULL excludes context-only rows, which never had a
    # realized return to begin with.
    limit_sql = f"LIMIT {args.sample}" if args.sample > 0 else ""
    rows = conn.execute(f"""
        SELECT id, ticker, ts, expiry, fwd_return_1d
          FROM signal_observations
         WHERE evaluated_at IS NOT NULL
           AND realized_at IS NULL
           AND direction IS NOT NULL
           AND expiry IS NOT NULL
         ORDER BY ts ASC
         {limit_sql}
    """).fetchall()

    total  = len(rows)
    filled = skipped = errors = api_calls = 0
    reason_tally: dict = {}
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

        realized, reason = _realized(ticker, ts, expiry, _cache)

        if args.dry_run:
            proj_s = f"{projected*100:+.3f}%" if projected is not None else "null"
            real_s = f"{realized*100:+.3f}%"  if realized  is not None else f"null({reason})"
            delta  = ""
            if projected is not None and realized is not None:
                delta = f"{(realized-projected)*100:+.3f}%"
            print(f"{ticker:<8} {ts[:10]:<12} {expiry[:10]:<12} "
                  f"{proj_s:>12} {real_s:>12} {delta:>10}")
            filled += 1
            continue

        try:
            if realized is not None:
                conn.execute(
                    """
                    UPDATE signal_observations
                       SET fwd_return_1d_realized = ?, realized_at = ?,
                           realized_attempts = realized_attempts + 1,
                           realized_fail_reason = NULL
                     WHERE id = ?
                    """,
                    (realized, _now_iso(), obs_id),
                )
                conn.commit()
                filled += 1
            else:
                # Every row targeted here is long past expiry — this pass IS
                # the retry, immediately. No data now means no data ever
                # (delisted/no coverage), so mark permanent rather than
                # leaving it for evaluate_realized_pending() to retry blind.
                conn.execute(
                    """
                    UPDATE signal_observations
                       SET realized_at = ?, realized_attempts = realized_attempts + 1,
                           realized_fail_reason = ?
                     WHERE id = ?
                    """,
                    (_now_iso(), reason, obs_id),
                )
                conn.commit()
                skipped += 1
                reason_tally[reason] = reason_tally.get(reason, 0) + 1
        except Exception as e:
            errors += 1
            print(f"[WARN] row {obs_id} write failed: {e}")

        if (i + 1) % 100 == 0:
            print(f"[{_now_iso()}] progress  {i+1}/{total}  "
                  f"filled={filled}  skipped(null)={skipped}  "
                  f"api_calls={api_calls}  errors={errors}")

    conn.close()
    print(f"\n[{_now_iso()}] DONE  total={total}  filled={filled}  "
          f"skipped(null)={skipped}  api_calls={api_calls}  errors={errors}")
    if reason_tally:
        print("reasons:", ", ".join(f"{k}={v}" for k, v in sorted(reason_tally.items(), key=lambda kv: -kv[1])))
    if not args.dry_run:
        print("NEXT: capture /api/observations/summary for the real alpha read.")


if __name__ == "__main__":
    main()
