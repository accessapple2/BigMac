"""
Migration 004 — partial close tracking.

Adds contracts_closed_so_far to options_trades (safe if already present).
"""
from __future__ import annotations
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "trader.db"


def run():
    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.execute("PRAGMA table_info(options_trades)")
        cols = {row[1] for row in cur.fetchall()}

        added = []
        if "contracts_closed_so_far" not in cols:
            conn.execute(
                "ALTER TABLE options_trades ADD COLUMN "
                "contracts_closed_so_far INTEGER DEFAULT 0"
            )
            added.append("contracts_closed_so_far")

        conn.commit()
        if added:
            print(f"[migration 004] Added columns: {added}")
        else:
            print("[migration 004] Already up to date — no columns added")
    finally:
        conn.close()


if __name__ == "__main__":
    run()
