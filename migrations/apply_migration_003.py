"""Migration 003 — add executor metadata columns to options_trades."""
from __future__ import annotations
import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "trader.db"


def column_exists(conn, table, column):
    cur = conn.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cur.fetchall())


def apply():
    if not DB_PATH.exists():
        print(f"ERROR: DB not found at {DB_PATH}")
        return 1
    conn = sqlite3.connect(str(DB_PATH))
    try:
        for col, ddl in [
            ("broker_order_id",
             "ALTER TABLE options_trades ADD COLUMN broker_order_id TEXT"),
            ("signal_id",
             "ALTER TABLE options_trades ADD COLUMN signal_id INTEGER"),
            ("exec_status",
             "ALTER TABLE options_trades ADD COLUMN exec_status TEXT DEFAULT 'pending'"),
        ]:
            if not column_exists(conn, "options_trades", col):
                conn.execute(ddl)
                conn.commit()
                print(f"[migration 003] added options_trades.{col}")
            else:
                print(f"[migration 003] options_trades.{col} already exists")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(apply())
