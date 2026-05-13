"""
HM-AN read bridge: Signal Center (:9000) -> Dashboard backend (:8080).

Phase 1 scope: heartbeat + read-only signal fetch (trader.db.signals).
Phase 2 HM-AN2.C: fetch_signal_center_active_signals reads Signal Center
DB directly (signal-center/signals.db) for neo-matrix consumption.

This module never writes to Signal Center. All access is read-only.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import requests

logger = logging.getLogger(__name__)

SIGNAL_CENTER_URL = "http://127.0.0.1:9000"
SIGNAL_CENTER_TIMEOUT_S = 2.0


@dataclass
class BridgeHealth:
    """Snapshot of the bridge's view of Signal Center."""
    signal_center_reachable: bool
    signal_center_endpoint: str | None
    last_check_ts: str
    error: str | None = None


def check_signal_center_health() -> BridgeHealth:
    """Ping Signal Center for liveness. Discovery-tolerant: tries known paths.

    allow_redirects=False so the Flask auth wall (302 -> /login) is never
    misread as a healthy 200. Discovery 2026-05-10 confirmed /api/health is
    the only path that returns a true 200; /, /health, /status all 302.
    """
    candidates = ["/api/health", "/health", "/api/status", "/status", "/"]
    now = datetime.utcnow().isoformat() + "Z"
    last_err = None
    for path in candidates:
        url = f"{SIGNAL_CENTER_URL}{path}"
        try:
            r = requests.get(
                url,
                timeout=SIGNAL_CENTER_TIMEOUT_S,
                allow_redirects=False,
            )
            if r.status_code == 200:
                return BridgeHealth(
                    signal_center_reachable=True,
                    signal_center_endpoint=path,
                    last_check_ts=now,
                )
        except requests.RequestException as e:
            last_err = str(e)
            continue
    return BridgeHealth(
        signal_center_reachable=False,
        signal_center_endpoint=None,
        last_check_ts=now,
        error=last_err or "no 200 response from any candidate path",
    )


def fetch_recent_signals(
    since_minutes: int = 60,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """
    Read recent signals via the DB (Phase 1 -- direct read).

    Phase 2+ may swap this to Signal Center's /api/signals if that surface
    proves richer for the Race/Scanner tiles. Until then, trader.db is the
    source of truth.

    Column is 'signal' (not 'side'); LEGACY_BIMODAL rows excluded per
    directive. Discovery 2026-05-10 confirmed the schema.
    """
    import sqlite3
    from pathlib import Path

    db = Path.home() / "autonomous-trader" / "data" / "trader.db"
    if not db.exists():
        logger.warning("trader.db not found at %s", db)
        return []

    cutoff = (datetime.utcnow() - timedelta(minutes=since_minutes)).isoformat()
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT id, player_id, symbol, signal, confidence,
                   created_at, reasoning
            FROM signals
            WHERE created_at >= ?
              AND (reasoning IS NULL OR reasoning NOT LIKE '%[LEGACY_BIMODAL%')
            ORDER BY id DESC
            LIMIT ?
            """,
            (cutoff, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# === HM-AN2.C === Signal Center active-signal consumer for neo-matrix.
# Direct-DB read (no HTTP hop, no auth) — same pattern as
# dashboard/app.py:11082's /api/signal-center/top reader.
def fetch_signal_center_active_signals(
    min_confidence: int = 70,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Read active (status='NEW') BUY signals from Signal Center DB.

    Filters: status='NEW', action IN ('BUY','LONG'), confidence >= min_confidence.
    Ordered by created_at DESC. Returns [] on any DB error — never raises so
    the calling scan cycle is never broken by a transient SC DB lock.
    """
    import sqlite3
    from pathlib import Path

    sc_db = Path.home() / "autonomous-trader" / "signal-center" / "signals.db"
    if not sc_db.exists():
        logger.warning("signal-center signals.db not found at %s", sc_db)
        return []
    conn = None
    try:
        conn = sqlite3.connect(str(sc_db), timeout=5)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT id, type, symbol, action, entry_price, stop_loss, take_profit,
                   confidence, agent_name, model_used, reasoning, timeframe,
                   status, created_at
            FROM trade_signals
            WHERE status = 'NEW'
              AND action IN ('BUY', 'LONG')
              AND confidence >= ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (min_confidence, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.warning("fetch_signal_center_active_signals error: %s", e)
        return []
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
# === /HM-AN2.C ===
