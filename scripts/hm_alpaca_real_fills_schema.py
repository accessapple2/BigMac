#!/usr/bin/env python3
"""HM-HAIRCUT-AND-EVIDENCE-TIERS — alpaca_real_fills schema (2026-07-06).

Additive, idempotent DDL only. Creates the broker-truth reconciliation table
and the venue tagging columns approved by the Admiral:

  venue values: NULL (unreviewed) | 'internal-sim' (reviewed, confirmed no
  broker fill) | 'broker-clean' (real fill, 1:1 attributable, Tier-A eligible
  once P&L is read from alpaca_real_fills, not the local pnl column) |
  'broker-commingled' (real fill, fragmented across multiple agent_ids by
  timestamp coincidence — excluded from clean-sim evidence AND from Tier-A).

Original trades/options_trades rows are never overwritten by this script;
alpaca_real_fills is the separate record of what was actually true.
Safe to run multiple times — every statement checks existence first.
"""
import sqlite3
from pathlib import Path

DB = Path.home() / "autonomous-trader" / "data" / "trader.db"

DDL_TABLE = """
CREATE TABLE IF NOT EXISTS alpaca_real_fills (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    broker_order_id       TEXT NOT NULL UNIQUE,
    client_order_id       TEXT,
    symbol                TEXT NOT NULL,
    asset_class           TEXT NOT NULL,
    order_class           TEXT NOT NULL DEFAULT 'simple',
    side                  TEXT,
    qty                   REAL,
    filled_qty            REAL,
    filled_avg_price      REAL,
    submitted_at          TIMESTAMP,
    filled_at             TIMESTAMP,
    legs_json             TEXT,
    matched_trade_id          INTEGER REFERENCES trades(id),
    matched_options_trade_id  INTEGER REFERENCES options_trades(id),
    match_method          TEXT NOT NULL DEFAULT 'unmatched',
    match_confidence       TEXT,
    reconciliation_batch  TEXT,
    notes                 TEXT,
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CHECK (matched_trade_id IS NULL OR matched_options_trade_id IS NULL)
)
"""

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_arf_symbol ON alpaca_real_fills(symbol)",
    "CREATE INDEX IF NOT EXISTS idx_arf_matched_trade ON alpaca_real_fills(matched_trade_id)",
    "CREATE INDEX IF NOT EXISTS idx_arf_matched_options ON alpaca_real_fills(matched_options_trade_id)",
    "CREATE INDEX IF NOT EXISTS idx_arf_batch ON alpaca_real_fills(reconciliation_batch)",
]


def ensure_column(conn, table, column, coltype):
    cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")
        print(f"  [ADDED] {table}.{column}")
    else:
        print(f"  [SKIP] {table}.{column} already exists")


def main():
    conn = sqlite3.connect(DB)
    try:
        conn.execute(DDL_TABLE)
        print("[OK] alpaca_real_fills table ensured")
        for stmt in INDEXES:
            conn.execute(stmt)
        print("[OK] indexes ensured")
        ensure_column(conn, "trades", "venue", "TEXT DEFAULT NULL")
        ensure_column(conn, "options_trades", "venue", "TEXT DEFAULT NULL")
        conn.commit()
    finally:
        conn.close()
    print("[DONE]")


if __name__ == "__main__":
    main()
