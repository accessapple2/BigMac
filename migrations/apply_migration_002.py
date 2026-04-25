"""Run migration 002. Idempotent. Sacred DB rule applies."""
from __future__ import annotations
import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "trader.db"
MIGRATION_SQL = Path(__file__).parent / "002_iv_history.sql"


def apply():
    if not DB_PATH.exists():
        print(f"ERROR: DB not found at {DB_PATH}")
        return 1
    if not MIGRATION_SQL.exists():
        print(f"ERROR: migration SQL not found at {MIGRATION_SQL}")
        return 1

    conn = sqlite3.connect(str(DB_PATH))
    try:
        with open(MIGRATION_SQL) as f:
            conn.executescript(f.read())
        conn.commit()
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='iv_history'"
        )
        tables = [r[0] for r in cur.fetchall()]
        print(f"[migration 002] verified tables: {tables}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(apply())
