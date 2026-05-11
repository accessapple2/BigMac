"""
HM-AN read bridge: Signal Center (:9000) -> Dashboard backend (:8080).

Phase 1 scope: heartbeat + read-only signal fetch.
Phase 2+: Race tile feed, Scanner tile feed, detail panel feed.

This module never writes to Signal Center. All Signal Center calls are GET.
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
