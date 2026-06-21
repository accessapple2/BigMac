"""HM-EXEC-PIPELINE observe-first measurement layer (Part 2).

Single write point for all signal source observations.
Pure side-effect: callers never inspect the return value.
Any exception is swallowed after debug-logging so the caller's behavior
is never affected.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

_DB = "data/trader.db"

_TTL_MINUTES: dict[str, int] = {
    "uhura":        60,
    "bk_avwap":   1440,
    "bk_orb":      390,
    "bk_box":     1440,
    "deep_scan":  1440,
    "fred_bankrate": 360,
    "gex_flow":     15,
}


def _expiry(source: str) -> str:
    ttl = _TTL_MINUTES.get(source, 60)
    return (datetime.now(timezone.utc) + timedelta(minutes=ttl)).isoformat()


def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signal_observations (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            ts              TEXT NOT NULL,
            source          TEXT NOT NULL,
            ticker          TEXT NOT NULL,
            direction       TEXT NOT NULL,
            conviction      TEXT,
            grade           TEXT,
            confluence_meta TEXT,
            expiry          TEXT,
            is_context      INTEGER NOT NULL DEFAULT 0,
            acted_by_fleet  INTEGER,
            fleet_trade_id  INTEGER,
            fwd_return_1h   REAL,
            fwd_return_1d   REAL,
            fwd_return_exp  REAL,
            evaluated_at    TEXT
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS ix_sigobs_ts "
        "ON signal_observations(ts)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS ix_sigobs_source "
        "ON signal_observations(source)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS ix_sigobs_eval "
        "ON signal_observations(evaluated_at)"
    )


def emit_observation(
    source: str,
    ticker: str,
    direction: str,
    conviction: str | None = None,
    grade: str | None = None,
    confluence_meta: dict | None = None,
    expiry: str | None = None,
    is_context: bool = False,
    db_path: str | None = None,
) -> int | None:
    """Write one signal observation row. Never raises.

    Returns the new row id on success, None on failure.
    """
    try:
        now = datetime.now(timezone.utc).isoformat()
        exp = expiry or _expiry(source)
        meta_json = json.dumps(confluence_meta) if confluence_meta else None
        db = db_path or _DB
        conn = sqlite3.connect(db, timeout=3)
        conn.execute("PRAGMA journal_mode=WAL")
        try:
            _ensure_table(conn)
            cur = conn.execute(
                """
                INSERT INTO signal_observations
                    (ts, source, ticker, direction, conviction, grade,
                     confluence_meta, expiry, is_context)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (now, source, ticker, direction, conviction, grade,
                 meta_json, exp, int(is_context)),
            )
            conn.commit()
            return cur.lastrowid
        finally:
            conn.close()
    except Exception as exc:
        logger.debug(
            "[signal_obs] emit failed source=%s ticker=%s: %s", source, ticker, exc
        )
        return None
