"""Persist StrategySignal objects to registry_signals table.

Note: Uses 'registry_signals' not 'strategy_signals' due to Task 3
collision-avoidance rename.
"""
from __future__ import annotations
import json
import sqlite3
from pathlib import Path
from .base import StrategySignal

DB_PATH = Path(__file__).parent.parent / "data" / "trader.db"


def persist(signal: StrategySignal) -> int:
    """Write signal to registry_signals. Returns inserted row id."""
    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.execute("""
            INSERT INTO registry_signals
                (strategy_id, ticker, action, asset_type, direction,
                 exit_tag, max_risk_usd, confidence, payload_json,
                 reasoning, generated_at, executed, executed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL)
        """, (
            signal.strategy_id, signal.ticker, signal.action,
            signal.asset_type, signal.direction, signal.exit_tag,
            signal.max_risk_usd, signal.confidence,
            json.dumps(signal.payload, default=str),
            signal.reasoning, signal.generated_at.isoformat(),
        ))
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def persist_many(signals: list[StrategySignal]) -> list[int]:
    return [persist(s) for s in signals]
