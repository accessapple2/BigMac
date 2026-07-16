"""HM-EXEC-PIPELINE observe-first outcome evaluator (Part 3).

Two independent periodic jobs, both wired to main.py scheduler at 30-minute
cadence:

- evaluate_pending(): acted-by-fleet join + fwd_return_1d proxy. Runs once
  per observation, stamps evaluated_at, never revisited.
- evaluate_realized_pending(): fwd_return_1d_realized (actual Alpaca
  close-to-close return). Split out from evaluate_pending on 2026-07-15
  (HM-REALIZED-RETRY) because the two have different readiness conditions:
  the acted-join/proxy are available immediately at expiry, but Alpaca's
  daily bar for the expiry date isn't published until ~market close + a
  settlement buffer. Gating the realized fetch on evaluated_at (and
  re-running the whole row) would re-run the acted-join/proxy repeatedly
  and clog the ts-ASC pending queue in evaluate_pending; instead this uses
  its own state (realized_at / realized_attempts / realized_next_retry_at)
  so a temporarily-unavailable bar just waits and retries, without ever
  disturbing the acted-join result that already completed correctly.
"""
from __future__ import annotations

import logging
import os
import sqlite3
import time
from datetime import date as _date, datetime, timedelta, timezone

logger = logging.getLogger(__name__)

_DB = "data/trader.db"
_ALPACA_BARS = "https://data.alpaca.markets/v2/stocks/{sym}/bars"

# HM-REALIZED-RETRY: realized-return retry policy.
_REALIZED_MAX_ATTEMPTS = 5
_REALIZED_RETRY_INTERVAL = timedelta(hours=18)   # ~5 attempts / 3.75 days
_REALIZED_CLOSE_BUFFER_UTC = (21, 30)             # 20:00 UTC close + 90min settlement buffer
_REALIZED_RATE_S = 0.22                           # ~4.5 req/s — matches backfill_realized_return.py;
                                                   # sustained back-to-back calls hit Alpaca 429s (confirmed
                                                   # 2026-07-15 sweep: 1075/2574 attempts with no delay)


def _bar_available_at(expiry_iso: str) -> datetime:
    """Earliest time Alpaca's consolidated daily bar for expiry_iso's date
    should exist: that date's close (20:00 UTC) plus a settlement buffer.

    Before this, a null-bars response for the expiry date is EXPECTED, not
    a failure — attempting earlier just burns an attempt for nothing.
    """
    expiry_date = _date.fromisoformat(expiry_iso[:10])
    hour, minute = _REALIZED_CLOSE_BUFFER_UTC
    return datetime(
        expiry_date.year, expiry_date.month, expiry_date.day,
        hour, minute, tzinfo=timezone.utc,
    )


def _fetch_realized_return(
    ticker: str, ts_iso: str, expiry_iso: str
) -> tuple[float | None, str | None]:
    """Return (realized_return, fail_reason). fail_reason is None on success.

    Realized = (close_at_expiry_date - close_at_ts_date) / close_at_ts_date.
    Never raises — any API/parse failure returns (None, reason) so the
    caller can log/count/retry instead of silently losing the row.
    """
    try:
        import requests as _req
        key    = os.environ.get("APCA_API_KEY_ID", "")
        secret = os.environ.get("APCA_API_SECRET_KEY", "")
        if not key or not secret:
            return None, "no_keys"

        ts_date     = ts_iso[:10]      # YYYY-MM-DD
        expiry_date = expiry_iso[:10]

        # Add +1 day buffer so the expiry date's bar is included in the response
        end_date = (_date.fromisoformat(expiry_date) + timedelta(days=2)).isoformat()

        r = _req.get(
            _ALPACA_BARS.format(sym=ticker),
            headers={"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret},
            params={
                "timeframe": "1Day",
                "start":     ts_date,
                "end":       end_date,
                "feed":      "iex",
                "sort":      "asc",
                "limit":     10,
            },
            timeout=8,
        )
        if not r.ok:
            return None, f"http_{r.status_code}"

        bars = r.json().get("bars") or []
        if not bars:
            return None, "no_bars"

        # Build date → close map ("t": "2026-06-25T00:00:00Z" → "2026-06-25")
        closes: dict[str, float] = {}
        for b in bars:
            bar_date = (b.get("t") or "")[:10]
            if bar_date:
                closes[bar_date] = float(b["c"])

        sorted_dates = sorted(closes)

        # Track which actual bar date is used for each endpoint
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

    except Exception as exc:
        return None, f"error:{type(exc).__name__}"

_BULLISH = frozenset({
    "BULL", "LONG", "BULLISH", "CONFIRM", "CONFIRMBULLISH",
})
_BEARISH = frozenset({
    "BEAR", "SHORT", "BEARISH", "CAUTION", "CAUTIONBEARISH",
})


def _to_utc_space(iso_str: str) -> str:
    """Normalize any ISO timestamp to 'YYYY-MM-DD HH:MM:SS' (UTC, no tz, no µs).

    trades.executed_at uses space-separated UTC without tz suffix.
    signal_observations.ts/expiry use ISO-8601 with 'T' separator and +00:00.
    SQLite TEXT comparison fails across these formats ('T' > ' ' in ASCII, so
    any space-format date compares as strictly less than any T-format date at
    the same moment, causing BETWEEN to never match).
    """
    s = iso_str.strip().replace("T", " ")
    for suffix in ("+00:00", "+0000", "-00:00", "Z"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
            break
    if "." in s:
        s = s[: s.index(".")]
    return s


def _is_bullish(direction: str | None) -> bool | None:
    """Map raw direction string to bool. Returns None for neutral/context-only."""
    if direction is None:
        return None
    d = direction.upper().replace(" ", "")
    if d in _BULLISH:
        return True
    if d in _BEARISH:
        return False
    return None


def evaluate_pending(db_path: str | None = None, batch: int = 200) -> dict:
    """Score up to `batch` un-evaluated expired observations.

    For each observation:
    - acted_by_fleet: 1 if trades table has a matching (symbol, direction) within window
    - fleet_trade_id: matching trade id if acted
    - fwd_return_1d: proxy from deep_scan_results entry/target prices (best effort)
    - evaluated_at: set to now

    Realized (actual) forward return is handled separately by
    evaluate_realized_pending() — see module docstring.

    Returns: {evaluated: int, acted: int, errors: int}
    """
    db = db_path or _DB
    now_iso = datetime.now(timezone.utc).isoformat()
    evaluated = acted = errors = 0

    try:
        conn = sqlite3.connect(db, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=8000")
        conn.execute("PRAGMA journal_mode=WAL")
    except Exception as exc:
        logger.error("[signal_eval] DB connect failed: %s", exc)
        return {"evaluated": 0, "acted": 0, "errors": 1}

    try:
        rows = conn.execute(
            """
            SELECT id, source, ticker, direction, ts, expiry
              FROM signal_observations
             WHERE evaluated_at IS NULL
               AND (expiry IS NULL OR expiry < ?)
             ORDER BY ts ASC
             LIMIT ?
            """,
            (now_iso, batch),
        ).fetchall()

        for row in rows:
            try:
                obs_id = row["id"]
                ticker = row["ticker"]
                direction = row["direction"]
                ts = row["ts"]
                expiry = row["expiry"] or now_iso

                fleet_trade_id = None
                fleet_acted = None
                fwd_return_1d = None

                is_bull = _is_bullish(direction)
                if is_bull is not None:
                    action_filter = "BUY" if is_bull else "SELL"
                    # Normalize obs timestamps to space-UTC to match trades.executed_at
                    # format ('YYYY-MM-DD HH:MM:SS').  Without this, the 'T' separator
                    # in ISO+offset strings causes all BETWEEN comparisons to fail
                    # because ' ' (32) < 'T' (84) in ASCII, making every trade appear
                    # before the lower bound.
                    ts_cmp     = _to_utc_space(ts)
                    expiry_cmp = _to_utc_space(expiry)
                    trade = conn.execute(
                        """
                        SELECT id, price
                          FROM trades
                         WHERE symbol = ?
                           AND action = ?
                           AND executed_at BETWEEN ? AND ?
                         ORDER BY executed_at ASC
                         LIMIT 1
                        """,
                        (ticker, action_filter, ts_cmp, expiry_cmp),
                    ).fetchone()

                    if trade:
                        fleet_trade_id = trade["id"]
                        fleet_acted = 1
                        acted += 1
                    else:
                        fleet_acted = 0

                    # Forward return proxy: deep_scan entry → target (best effort)
                    ds = conn.execute(
                        """
                        SELECT entry_price, target_price
                          FROM deep_scan_results
                         WHERE symbol = ?
                           AND scan_date >= ?
                         ORDER BY scan_date DESC, id DESC
                         LIMIT 1
                        """,
                        (ticker, ts[:10]),
                    ).fetchone()

                    if ds and ds["entry_price"] and ds["target_price"]:
                        ep = float(ds["entry_price"])
                        tp = float(ds["target_price"])
                        if ep > 0:
                            fwd_return_1d = round((tp - ep) / ep, 6)

                conn.execute(
                    """
                    UPDATE signal_observations
                       SET evaluated_at   = ?,
                           acted_by_fleet = ?,
                           fleet_trade_id = ?,
                           fwd_return_1d  = ?
                     WHERE id = ?
                    """,
                    (now_iso, fleet_acted, fleet_trade_id, fwd_return_1d, obs_id),
                )
                conn.commit()
                evaluated += 1
            except Exception as exc:
                logger.debug("[signal_eval] row %s error: %s", row["id"], exc)
                errors += 1

    except Exception as exc:
        logger.error("[signal_eval] evaluate_pending failed: %s", exc)
        errors += 1
    finally:
        conn.close()

    if evaluated:
        logger.info(
            "[signal_eval] evaluated=%d acted=%d errors=%d", evaluated, acted, errors
        )
    return {"evaluated": evaluated, "acted": acted, "errors": errors}


def evaluate_realized_pending(db_path: str | None = None, batch: int = 200) -> dict:
    """Fetch fwd_return_1d_realized for observations that already cleared
    evaluate_pending() but haven't resolved a realized return yet.

    Eligibility: evaluated_at IS NOT NULL, direction is BULL/BEAR (context
    rows are skipped — never had a realized return to begin with), realized_at
    IS NULL, attempts < _REALIZED_MAX_ATTEMPTS, and not on retry cooldown.

    Rows whose expiry date's close hasn't settled yet (per _bar_available_at)
    are skipped WITHOUT counting an attempt — a null-bars response before
    that point is expected, not a failure, and shouldn't burn retry budget.

    On success: realized_at=now, fwd_return_1d_realized=value, reason cleared.
    On failure: attempts += 1; if attempts hit the cap, realized_at=now with
    the fail reason recorded (permanent — row stops being retried); otherwise
    realized_next_retry_at is pushed out and the row is picked up again later.

    DOCTRINE: every outcome (success / retry / permanent-fail / gated-skip)
    is tallied and logged at INFO — no silent catch. See CLAUDE.md ALPHA
    READ open item #2 (measurement-health RED threshold) for why: the prior
    inline version of this fetch swallowed every failure into a NULL column
    with no counter, so a 100% failure rate ran undetected for 16 days.

    Returns: {attempted, succeeded, retry_scheduled, permanent_fail,
              gated_skip, reasons: {reason: count}}
    """
    db = db_path or _DB
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    tally = {
        "attempted": 0, "succeeded": 0, "retry_scheduled": 0,
        "permanent_fail": 0, "gated_skip": 0, "reasons": {},
    }

    try:
        conn = sqlite3.connect(db, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=8000")
        conn.execute("PRAGMA journal_mode=WAL")
    except Exception as exc:
        logger.error("[realized_eval] DB connect failed: %s", exc)
        return tally

    try:
        rows = conn.execute(
            """
            SELECT id, ticker, direction, ts, expiry
              FROM signal_observations
             WHERE evaluated_at IS NOT NULL
               AND realized_at IS NULL
               AND direction IS NOT NULL
               AND realized_attempts < ?
               AND (realized_next_retry_at IS NULL OR realized_next_retry_at <= ?)
             ORDER BY ts ASC
             LIMIT ?
            """,
            (_REALIZED_MAX_ATTEMPTS, now_iso, batch),
        ).fetchall()

        for row in rows:
            obs_id = row["id"]
            direction = row["direction"]
            expiry = row["expiry"]

            if _is_bullish(direction) is None or not expiry:
                # Context-only / malformed row — will never have a realized
                # return. Mark permanent immediately, don't burn a retry cycle.
                conn.execute(
                    """
                    UPDATE signal_observations
                       SET realized_at = ?, realized_attempts = realized_attempts + 1,
                           realized_fail_reason = 'not_directional'
                     WHERE id = ?
                    """,
                    (now_iso, obs_id),
                )
                conn.commit()
                tally["permanent_fail"] += 1
                tally["reasons"]["not_directional"] = tally["reasons"].get("not_directional", 0) + 1
                continue

            if row["ts"][:10] == expiry[:10]:
                # Same-calendar-day ts/expiry (bk_orb/uhura-style intraday
                # sources) can never resolve via daily bars — ts_date and
                # expiry_date are the same single bar by construction, no
                # amount of waiting produces a second one. Confirmed 100%
                # same_bar rate for these sources (commit 96a29c7). Mark
                # permanent on sight instead of burning 5 retry attempts
                # and 3+ days on something mathematically unresolvable.
                conn.execute(
                    """
                    UPDATE signal_observations
                       SET realized_at = ?, realized_attempts = realized_attempts + 1,
                           realized_fail_reason = 'same_day_expiry'
                     WHERE id = ?
                    """,
                    (now_iso, obs_id),
                )
                conn.commit()
                tally["permanent_fail"] += 1
                tally["reasons"]["same_day_expiry"] = tally["reasons"].get("same_day_expiry", 0) + 1
                continue

            if now < _bar_available_at(expiry):
                tally["gated_skip"] += 1
                continue

            tally["attempted"] += 1
            if tally["attempted"] > 1:
                time.sleep(_REALIZED_RATE_S)
            value, reason = _fetch_realized_return(row["ticker"], row["ts"], expiry)

            try:
                if value is not None:
                    conn.execute(
                        """
                        UPDATE signal_observations
                           SET realized_at = ?, realized_attempts = realized_attempts + 1,
                               fwd_return_1d_realized = ?, realized_fail_reason = NULL
                         WHERE id = ?
                        """,
                        (now_iso, value, obs_id),
                    )
                    tally["succeeded"] += 1
                else:
                    cur = conn.execute(
                        "SELECT realized_attempts FROM signal_observations WHERE id = ?",
                        (obs_id,),
                    ).fetchone()
                    attempts_after = (cur["realized_attempts"] or 0) + 1
                    if attempts_after >= _REALIZED_MAX_ATTEMPTS:
                        conn.execute(
                            """
                            UPDATE signal_observations
                               SET realized_at = ?, realized_attempts = ?,
                                   realized_fail_reason = ?
                             WHERE id = ?
                            """,
                            (now_iso, attempts_after, reason, obs_id),
                        )
                        tally["permanent_fail"] += 1
                    else:
                        next_retry = (now + _REALIZED_RETRY_INTERVAL).isoformat()
                        conn.execute(
                            """
                            UPDATE signal_observations
                               SET realized_attempts = ?, realized_next_retry_at = ?,
                                   realized_fail_reason = ?
                             WHERE id = ?
                            """,
                            (attempts_after, next_retry, reason, obs_id),
                        )
                        tally["retry_scheduled"] += 1
                    tally["reasons"][reason] = tally["reasons"].get(reason, 0) + 1
                conn.commit()
            except Exception as exc:
                logger.debug("[realized_eval] row %s write failed: %s", obs_id, exc)

    except Exception as exc:
        logger.error("[realized_eval] evaluate_realized_pending failed: %s", exc)
    finally:
        conn.close()

    if tally["attempted"] or tally["gated_skip"]:
        logger.info(
            "[realized_eval] attempted=%d succeeded=%d retry_scheduled=%d "
            "permanent_fail=%d gated_skip=%d reasons=%s",
            tally["attempted"], tally["succeeded"], tally["retry_scheduled"],
            tally["permanent_fail"], tally["gated_skip"], tally["reasons"],
        )
    return tally
