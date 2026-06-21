"""HM-EXEC-PIPELINE observe-first outcome evaluator (Part 3).

Periodic job: for observations whose expiry has passed and that have not
yet been evaluated, determine whether the fleet acted on the signal and
compute an approximate forward return.

Wired to main.py scheduler at 30-minute cadence.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_DB = "data/trader.db"

_BULLISH = frozenset({
    "BULL", "LONG", "BULLISH", "CONFIRM", "CONFIRMBULLISH",
})
_BEARISH = frozenset({
    "BEAR", "SHORT", "BEARISH", "CAUTION", "CAUTIONBEARISH",
})


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

                is_bull = _is_bullish(direction)
                if is_bull is not None:
                    action_filter = "BUY" if is_bull else "SELL"
                    trade = conn.execute(
                        """
                        SELECT id, fill_price
                          FROM trades
                         WHERE symbol = ?
                           AND action = ?
                           AND executed_at BETWEEN ? AND ?
                         ORDER BY executed_at ASC
                         LIMIT 1
                        """,
                        (ticker, action_filter, ts, expiry),
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
