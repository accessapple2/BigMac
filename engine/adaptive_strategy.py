"""adaptive_strategy.py — Adaptive strategy engine.

Tracks signal trust scores and adjusts agent allocations weekly.
All changes are logged with reasons. Agent benching is reversible.
Trust scores start at 0.5 (neutral).
"""
from __future__ import annotations

import sqlite3
import threading
import time
import logging
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

DB_PATH = "data/trader.db"
_lock = threading.Lock()

_TRUST_INIT = 0.5
_trust_cache: dict[str, float] = {}
_trust_ts: float = 0
_TRUST_TTL = 3600  # 1 hour


def _ensure_tables(db: sqlite3.Connection) -> None:
    db.executescript("""
        CREATE TABLE IF NOT EXISTS trust_scores (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_type TEXT NOT NULL,
            regime      TEXT,
            trust_score REAL NOT NULL DEFAULT 0.5,
            sample_size INTEGER DEFAULT 0,
            updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_trust_type_regime
            ON trust_scores(signal_type, regime);

        CREATE TABLE IF NOT EXISTS adaptive_rules (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_type   TEXT NOT NULL,
            player_id   TEXT,
            symbol      TEXT,
            regime      TEXT,
            rule_key    TEXT NOT NULL,
            rule_value  TEXT NOT NULL,
            reason      TEXT,
            active      INTEGER DEFAULT 1,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS agent_allocation (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id   TEXT NOT NULL UNIQUE,
            multiplier  REAL NOT NULL DEFAULT 1.0,
            benched     INTEGER DEFAULT 0,
            bench_reason TEXT,
            updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    db.commit()


def get_trust_score(signal_type: str, regime: str | None = None) -> float:
    """Return current trust score for a signal type in the given regime."""
    try:
        db = sqlite3.connect(DB_PATH, timeout=5)
        row = db.execute("""
            SELECT trust_score FROM trust_scores
            WHERE signal_type=? AND (regime=? OR regime IS NULL)
            ORDER BY CASE WHEN regime=? THEN 0 ELSE 1 END
            LIMIT 1
        """, (signal_type, regime, regime)).fetchone()
        db.close()
        return float(row[0]) if row else _TRUST_INIT
    except Exception:
        return _TRUST_INIT


def update_trust_scores() -> None:
    """Hourly: recalculate trust scores from recent signal outcomes."""
    try:
        db = sqlite3.connect(DB_PATH, timeout=10)
        _ensure_tables(db)

        # Get signal outcomes from last 30 days
        try:
            rows = db.execute("""
                SELECT s.signal_type, s.outcome_pct, s.regime
                FROM signals s
                WHERE s.outcome_pct IS NOT NULL
                  AND s.created_at >= datetime('now', '-30 days')
            """).fetchall()
        except Exception:
            rows = []

        by_type: dict[tuple, list[float]] = {}
        for signal_type, outcome, regime in rows:
            k = (signal_type or "UNKNOWN", regime or "ALL")
            by_type.setdefault(k, []).append(outcome or 0)

        for (st, reg), outcomes in by_type.items():
            if len(outcomes) < 3:
                continue
            wins = sum(1 for o in outcomes if o > 0)
            wr = wins / len(outcomes)
            avg_win = sum(o for o in outcomes if o > 0) / max(1, wins)
            avg_loss = abs(sum(o for o in outcomes if o <= 0) / max(1, len(outcomes) - wins))
            profit_factor = avg_win / avg_loss if avg_loss > 0 else 2.0
            trust = wr * min(profit_factor, 2.0)  # capped at 1.0
            trust = round(min(1.0, max(0.0, trust)), 3)
            db.execute("""
                INSERT INTO trust_scores (signal_type, regime, trust_score, sample_size)
                VALUES (?,?,?,?)
                ON CONFLICT(signal_type, regime) DO UPDATE SET
                    trust_score=excluded.trust_score,
                    sample_size=excluded.sample_size,
                    updated_at=CURRENT_TIMESTAMP
            """, (st, reg if reg != "ALL" else None, trust, len(outcomes)))
        db.commit()
        db.close()
        logger.debug("adaptive: trust scores updated for %d combos", len(by_type))
    except Exception as e:
        logger.warning("update_trust_scores: %s", e)


def weekly_agent_review() -> None:
    """Sunday 4 PM AZ: evaluate agent performance, adjust allocations."""
    try:
        db = sqlite3.connect(DB_PATH, timeout=10)
        _ensure_tables(db)
        db.row_factory = sqlite3.Row

        # Get 30-day closed trade P&L per agent
        rows = db.execute("""
            SELECT player_id, realized_pnl
            FROM trades
            WHERE action='SELL' AND realized_pnl IS NOT NULL
              AND executed_at >= datetime('now', '-30 days')
            ORDER BY player_id
        """).fetchall()

        by_agent: dict[str, list[float]] = {}
        for r in rows:
            by_agent.setdefault(r["player_id"], []).append(r["realized_pnl"])

        if not by_agent:
            db.close()
            return

        win_rates = {pid: sum(1 for p in pnls if p > 0) / len(pnls) * 100
                     for pid, pnls in by_agent.items() if pnls}

        if not win_rates:
            db.close()
            return

        sorted_agents = sorted(win_rates.items(), key=lambda x: x[1], reverse=True)
        top_pid    = sorted_agents[0][0]
        bottom_pid = sorted_agents[-1][0]

        reason_ts = datetime.now().isoformat()

        for pid, wr in win_rates.items():
            n = len(by_agent[pid])
            if n < 5:
                continue  # not enough data
            if wr < 30:
                # Bench
                db.execute("""
                    INSERT INTO agent_allocation (player_id, multiplier, benched, bench_reason)
                    VALUES (?,0.5,1,?)
                    ON CONFLICT(player_id) DO UPDATE SET
                        benched=1, multiplier=0.5,
                        bench_reason=excluded.bench_reason,
                        updated_at=CURRENT_TIMESTAMP
                """, (pid, f"Benched {reason_ts}: {wr:.0f}% WR over {n} trades (<30% threshold)"))
                db.execute("""
                    INSERT INTO adaptive_rules (rule_type, player_id, rule_key, rule_value, reason)
                    VALUES ('BENCH', ?, 'benched', '1', ?)
                """, (pid, f"Win rate {wr:.0f}% < 30% threshold over 30 days"))
                logger.info("adaptive: BENCHED %s (%.0f%% WR)", pid, wr)
            elif pid == top_pid:
                db.execute("""
                    INSERT INTO agent_allocation (player_id, multiplier)
                    VALUES (?,1.2)
                    ON CONFLICT(player_id) DO UPDATE SET
                        multiplier=MIN(2.0, multiplier+0.2),
                        benched=0,
                        updated_at=CURRENT_TIMESTAMP
                """, (pid,))
            elif pid == bottom_pid and wr < 50:
                db.execute("""
                    INSERT INTO agent_allocation (player_id, multiplier)
                    VALUES (?,0.8)
                    ON CONFLICT(player_id) DO UPDATE SET
                        multiplier=MAX(0.5, multiplier-0.2),
                        updated_at=CURRENT_TIMESTAMP
                """, (pid,))

        db.commit()
        db.close()
        logger.info("adaptive: weekly review complete for %d agents", len(win_rates))
    except Exception as e:
        logger.warning("weekly_agent_review: %s", e)


def get_confidence_modifier(player_id: str, signal_type: str, regime: str | None = None) -> float:
    """Return confidence multiplier: 1.1 for high-trust, 0.8 for low-trust, 1.0 neutral."""
    try:
        trust = get_trust_score(signal_type, regime)
        if trust > 0.65:
            return 1.1
        elif trust < 0.35:
            return 0.8
        return 1.0
    except Exception:
        return 1.0
