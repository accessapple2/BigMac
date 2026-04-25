"""
Idempotent migration runner. Safe to run multiple times.
Sacred DB rule: never drops or truncates.
"""
from __future__ import annotations
import sqlite3
import sys
from pathlib import Path


DB_PATH = Path(__file__).parent.parent / "data" / "trader.db"
MIGRATION_SQL = Path(__file__).parent / "001_strategy_registry.sql"


def column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cur.fetchall())


def apply():
    if not DB_PATH.exists():
        print(f"ERROR: DB not found at {DB_PATH}")
        return 1
    if not MIGRATION_SQL.exists():
        print(f"ERROR: migration SQL not found at {MIGRATION_SQL}")
        return 1

    conn = sqlite3.connect(str(DB_PATH))
    try:
        # Run the SQL migration (all CREATE IF NOT EXISTS)
        with open(MIGRATION_SQL) as f:
            conn.executescript(f.read())
        conn.commit()
        print("[migration] tables created (idempotent)")

        # Add strategy_id column to trades if missing
        if not column_exists(conn, "trades", "strategy_id"):
            conn.execute("ALTER TABLE trades ADD COLUMN strategy_id TEXT")
            conn.commit()
            print("[migration] added trades.strategy_id column")
        else:
            print("[migration] trades.strategy_id already exists")

        # Same for options_trades
        if not column_exists(conn, "options_trades", "strategy_id"):
            conn.execute("ALTER TABLE options_trades ADD COLUMN strategy_id TEXT")
            conn.commit()
            print("[migration] added options_trades.strategy_id column")
        else:
            print("[migration] options_trades.strategy_id already exists")

        # Same for exit_tag on options_trades
        if not column_exists(conn, "options_trades", "exit_tag"):
            conn.execute("ALTER TABLE options_trades ADD COLUMN exit_tag TEXT DEFAULT 'single'")
            conn.commit()
            print("[migration] added options_trades.exit_tag column")
        else:
            print("[migration] options_trades.exit_tag already exists")

        # Verify end state
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('strategies', 'registry_signals')"
        )
        tables = [r[0] for r in cur.fetchall()]
        print(f"[migration] verified tables: {tables}")

    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(apply())
