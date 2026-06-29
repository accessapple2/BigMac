"""HM-EXEC-PIPELINE observe-first outcome evaluator (Part 3).

Periodic job: for observations whose expiry has passed and that have not
yet been evaluated, determine whether the fleet acted on the signal and
compute an approximate forward return.

Wired to main.py scheduler at 30-minute cadence.
"""
from __future__ import annotations

import logging
import os
import sqlite3
from datetime import date as _date, datetime, timedelta, timezone

logger = logging.getLogger(__name__)

_DB = "data/trader.db"
_ALPACA_BARS = "https://data.alpaca.markets/v2/stocks/{sym}/bars"


def _fetch_realized_return(ticker: str, ts_iso: str, expiry_iso: str) -> float | None:
    """Return actual close-to-close return from Alpaca daily bars, or None.

    Realized = (close_at_expiry_date - close_at_ts_date) / close_at_ts_date.
    Never raises — any API/parse failure returns None so evaluation is never blocked.
    """
    try:
        import requests as _req
        key    = os.environ.get("APCA_API_KEY_ID", "")
        secret = os.environ.get("APCA_API_SECRET_KEY", "")
        if not key or not secret:
            return None

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
            return None

        bars = r.json().get("bars") or []
        if not bars:
            return None

        # Build date → close map ("t": "2026-06-25T00:00:00Z" → "2026-06-25")
        closes: dict[str, float] = {}
        for b in bars:
            bar_date = (b.get("t") or "")[:10]
            if bar_date:
                closes[bar_date] = float(b["c"])

        entry_close  = closes.get(ts_date)
        expiry_close = closes.get(expiry_date)

        # Nearest-available fallback (handles weekends/holidays at boundaries)
        sorted_dates = sorted(closes)
        if entry_close is None and sorted_dates:
            entry_close = closes[sorted_dates[0]]
        if expiry_close is None and sorted_dates:
            expiry_close = closes[sorted_dates[-1]]

        if entry_close is None or expiry_close is None or entry_close <= 0:
            return None

        return round((expiry_close - entry_close) / entry_close, 6)

    except Exception:
        return None

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
                fwd_return_1d_realized = None

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

                    # Realized return: actual Alpaca close at ts vs close at expiry
                    fwd_return_1d_realized = _fetch_realized_return(ticker, ts, expiry)

                conn.execute(
                    """
                    UPDATE signal_observations
                       SET evaluated_at           = ?,
                           acted_by_fleet         = ?,
                           fleet_trade_id         = ?,
                           fwd_return_1d          = ?,
                           fwd_return_1d_realized = ?
                     WHERE id = ?
                    """,
                    (now_iso, fleet_acted, fleet_trade_id,
                     fwd_return_1d, fwd_return_1d_realized, obs_id),
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
