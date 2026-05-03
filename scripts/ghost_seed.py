#!/usr/bin/env python3
"""Ghost Trader seeder - one-shot Schwab snapshot import.

Refuses to run if ghost_portfolio or ghost_cash already has rows.
"""
import sqlite3
import sys
from datetime import datetime, timezone

DB = "data/trader.db"
NOW = datetime.now(timezone.utc).isoformat()
SOURCE = "schwab_snapshot_2026-04-28T20:11ET"

POSITIONS = [
    ("AMD",  2,  564.18),
    ("AMZN", 4,  1019.95),
    ("ANET", 11, 1876.40),
    ("AVGO", 2,  802.39),
    ("BWXT", 6,  1310.22),
    ("CCJ",  10, 1180.00),
    ("CEG",  4,  1232.00),
    ("CRDO", 8,  1363.92),
    ("CRWD", 3,  1391.88),
    ("DELL", 7,  1517.85),
    ("MU",   2,  1000.00),
    ("PLTR", 7,  1031.42),
    ("VRT",  6,  1867.96),
]
CASH = 9460.71
GENESIS_EQUITY = 25453.05

def main():
    con = sqlite3.connect(DB)
    cur = con.cursor()

    # Safety guard - refuse if already seeded
    n_pos = cur.execute("SELECT COUNT(*) FROM ghost_portfolio").fetchone()[0]
    n_cash = cur.execute("SELECT COUNT(*) FROM ghost_cash").fetchone()[0]
    if n_pos or n_cash:
        print(f"ABORT: ghost already seeded (positions={n_pos}, cash={n_cash}).")
        print("If you really want to re-seed, manually clear ghost_portfolio, ghost_cash, ghost_seed first.")
        sys.exit(1)

    # Seed positions
    for sym, qty, cost_basis in POSITIONS:
        avg_cost = cost_basis / qty
        cur.execute("""
            INSERT INTO ghost_portfolio (symbol, qty, avg_cost, opened_at, source_advisor, notes)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (sym, qty, avg_cost, NOW, "schwab_seed", "genesis position from Schwab snapshot"))
        cur.execute("""
            INSERT INTO ghost_seed (seeded_at, symbol, qty, cost_basis, cash_at_seed, source)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (NOW, sym, qty, cost_basis, None, SOURCE))

    # Seed cash + genesis equity
    cur.execute("""
        INSERT INTO ghost_cash (id, cash, equity, last_updated)
        VALUES (1, ?, ?, ?)
    """, (CASH, GENESIS_EQUITY, NOW))

    # Log cash row in seed audit
    cur.execute("""
        INSERT INTO ghost_seed (seeded_at, symbol, qty, cost_basis, cash_at_seed, source)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (NOW, "__CASH__", None, None, CASH, SOURCE))

    # Log day-zero equity
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    cur.execute("""
        INSERT OR REPLACE INTO ghost_equity_history (date, ghost_equity, schwab_equity, delta)
        VALUES (?, ?, ?, ?)
    """, (today, GENESIS_EQUITY, GENESIS_EQUITY, 0.0))

    con.commit()
    con.close()

    print(f"SEEDED: {len(POSITIONS)} positions, cash=${CASH:,.2f}, equity=${GENESIS_EQUITY:,.2f}")

if __name__ == "__main__":
    main()
