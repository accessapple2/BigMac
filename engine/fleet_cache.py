"""
Fleet Cache — Zero-Latency Collective Intelligence
====================================================
Singleton background thread refreshes fleet stats every 5 min.
All prompt reads are instant (<1ms) — no DB hit at scan time.

Plugs into brain_context.py as the fast-path for fleet_intelligence.
"""

from __future__ import annotations

import sqlite3
import threading
import time
import os
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "trader.db")

# ---------------------------------------------------------------------------

class FleetCache:
    """
    Singleton cache for fleet intelligence.
    Background refresh every 5 min, instant reads (<1ms).
    """

    _instance: Optional["FleetCache"] = None
    _class_lock = threading.Lock()

    def __new__(cls) -> "FleetCache":
        with cls._class_lock:
            if cls._instance is None:
                inst = super().__new__(cls)
                inst._initialized = False
                cls._instance = inst
        return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return
        self._initialized = True
        self._lock = threading.Lock()
        self._cache: dict = {"prompt": "", "last_refresh": None, "refresh_count": 0}
        self._refresh_interval = 300  # 5 minutes
        # Initial synchronous refresh so first scan has data
        try:
            self._refresh_cache()
        except Exception as e:
            logger.warning(f"Fleet cache initial refresh failed: {e}")
        # Start background thread
        t = threading.Thread(target=self._refresh_loop, daemon=True, name="fleet-cache")
        t.start()
        logger.info("🧠 Fleet Cache: ACTIVE (background refresh every 5m)")

    # ------------------------------------------------------------------
    # Background loop

    def _refresh_loop(self) -> None:
        while True:
            time.sleep(self._refresh_interval)
            try:
                self._refresh_cache()
            except Exception as e:
                logger.error(f"Fleet cache refresh error: {e}")

    def _refresh_cache(self) -> None:
        conn = sqlite3.connect(_DB_PATH, timeout=5)
        conn.row_factory = sqlite3.Row
        try:
            # ── Recent trades (7d) ──────────────────────────────────────
            trades = conn.execute("""
                SELECT p.display_name, t.action, t.symbol, t.realized_pnl
                FROM trades t
                JOIN ai_players p ON t.player_id = p.id
                WHERE t.executed_at > datetime('now', '-7 days')
                  AND t.realized_pnl IS NOT NULL
                ORDER BY t.executed_at DESC
                LIMIT 15
            """).fetchall()

            # ── Strategy leaderboard (30d, via timeframe column) ────────
            strategies = conn.execute("""
                SELECT
                    COALESCE(timeframe, 'unknown') as strat,
                    COUNT(*) as cnt,
                    SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as wins,
                    SUM(realized_pnl) as total_pnl
                FROM trades
                WHERE executed_at > datetime('now', '-30 days')
                  AND realized_pnl IS NOT NULL
                GROUP BY timeframe
                HAVING COUNT(*) >= 3
                ORDER BY SUM(realized_pnl) DESC
                LIMIT 5
            """).fetchall()

            # ── Hot agents (7d) ─────────────────────────────────────────
            agents = conn.execute("""
                SELECT p.display_name, SUM(t.realized_pnl) as pnl
                FROM trades t
                JOIN ai_players p ON t.player_id = p.id
                WHERE t.executed_at > datetime('now', '-7 days')
                  AND t.realized_pnl IS NOT NULL
                GROUP BY t.player_id
                ORDER BY SUM(t.realized_pnl) DESC
                LIMIT 3
            """).fetchall()

            # ── Danger tickers (14d) ─────────────────────────────────────
            dangers = conn.execute("""
                SELECT symbol, COUNT(*) as cnt, SUM(realized_pnl) as total_loss
                FROM trades
                WHERE executed_at > datetime('now', '-14 days')
                  AND realized_pnl < 0
                GROUP BY symbol
                HAVING COUNT(*) >= 2
                ORDER BY SUM(realized_pnl) ASC
                LIMIT 3
            """).fetchall()
        finally:
            conn.close()

        prompt = self._build_prompt(trades, strategies, agents, dangers)
        with self._lock:
            self._cache["prompt"] = prompt
            self._cache["last_refresh"] = datetime.now()
            self._cache["refresh_count"] += 1

    # ------------------------------------------------------------------
    # Prompt builder

    @staticmethod
    def _build_prompt(trades, strategies, agents, dangers) -> str:
        lines = ["=== FLEET INTELLIGENCE (cached) ==="]

        # Win rate summary
        settled = [(t["action"], t["symbol"], t["realized_pnl"]) for t in trades
                   if t["realized_pnl"] is not None]
        if settled:
            wins = sum(1 for _, _, p in settled if p > 0)
            lines.append(f"7d Fleet WR: {wins}/{len(settled)} ({wins/len(settled)*100:.0f}%)")
            for action, symbol, pnl in settled[:8]:
                lines.append(f"  {action} {symbol} → {'WIN' if pnl > 0 else 'LOSS'} ${pnl:+.0f}")

        # Top strategies
        if strategies:
            lines.append("\nTOP STRATEGIES (30d):")
            for row in strategies[:3]:
                wr = row["wins"] / row["cnt"] * 100 if row["cnt"] else 0
                lines.append(f"  {row['strat']}: {wr:.0f}% WR, ${row['total_pnl']:+.0f}")

        # Hot agents
        if agents:
            lines.append("\nHOT AGENTS (7d):")
            for row in agents:
                lines.append(f"  {row['display_name']}: ${row['pnl']:+.0f}")

        # Danger tickers
        if dangers:
            lines.append("\nAVOID (repeated losses):")
            for row in dangers:
                lines.append(f"  {row['symbol']} ({row['cnt']} losses, ${row['total_loss']:+.0f})")

        lines.append("=== END FLEET ===\n")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Public API

    def get_fleet_context(self) -> str:
        with self._lock:
            return self._cache["prompt"]

    def last_refresh(self) -> Optional[datetime]:
        with self._lock:
            return self._cache["last_refresh"]

    def refresh_count(self) -> int:
        with self._lock:
            return self._cache["refresh_count"]


# ---------------------------------------------------------------------------
# Module-level singleton accessor (used by brain_context fast-path)

_fleet_cache: Optional[FleetCache] = None
_init_lock = threading.Lock()


def get_fleet_context() -> str:
    """Return cached fleet context string. Thread-safe, <1ms."""
    global _fleet_cache
    if _fleet_cache is None:
        with _init_lock:
            if _fleet_cache is None:
                _fleet_cache = FleetCache()
    return _fleet_cache.get_fleet_context()


def init_fleet_cache() -> FleetCache:
    """Explicitly initialize the singleton (call at startup)."""
    return FleetCache()
