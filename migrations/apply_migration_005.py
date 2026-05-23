"""Migration 005 — HM-TRADE-DESK-AUTOPILOT 2026-05-22.

Adds nullable stop_loss_order_id + take_profit_order_id to trades so the
Trade Desk's auto-attached protective Alpaca GTC orders link back to the
primary mirror row. Idempotent: re-running is a no-op.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "trader.db"


def run() -> None:
    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.execute("PRAGMA table_info(trades)")
        cols = {row[1] for row in cur.fetchall()}

        added: list[str] = []
        if "stop_loss_order_id" not in cols:
            conn.execute("ALTER TABLE trades ADD COLUMN stop_loss_order_id TEXT")
            added.append("stop_loss_order_id")
        if "take_profit_order_id" not in cols:
            conn.execute("ALTER TABLE trades ADD COLUMN take_profit_order_id TEXT")
            added.append("take_profit_order_id")

        conn.commit()
        if added:
            print(f"[migration 005] Added columns: {added}")
        else:
            print("[migration 005] Already up to date — no columns added")
    finally:
        conn.close()


if __name__ == "__main__":
    run()
