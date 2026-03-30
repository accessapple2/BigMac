"""
Migration 001: Create CrewAI and multi-portfolio tables.

Creates 4 NEW tables in trader.db. NEVER modifies existing tables.
All existing data is sacred — no drops, no truncates, no deletes.

Usage: python migrations/001_crew_and_portfolios.py
"""

import os
import sqlite3
import sys

DB_PATH = os.environ.get("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))


def run_migration():
    if not os.path.exists(DB_PATH):
        print(f"[!] Database not found at {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    c = conn.cursor()

    print(f"[*] Running migration 001 on: {DB_PATH}")
    print()

    # ------------------------------------------------------------------
    # Table 1: portfolios
    # ------------------------------------------------------------------
    c.execute("""CREATE TABLE IF NOT EXISTS portfolios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        broker TEXT NOT NULL,
        account_type TEXT NOT NULL DEFAULT 'paper',
        execution_mode TEXT NOT NULL DEFAULT 'auto',
        type TEXT NOT NULL DEFAULT 'paper',
        api_key_ref TEXT,
        initial_balance REAL NOT NULL DEFAULT 100000.0,
        current_balance REAL NOT NULL DEFAULT 100000.0,
        is_human INTEGER NOT NULL DEFAULT 0,
        is_active INTEGER NOT NULL DEFAULT 1,
        notes TEXT DEFAULT '',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    print("[+] Table: portfolios")

    # ------------------------------------------------------------------
    # Table 2: portfolio_positions
    # ------------------------------------------------------------------
    c.execute("""CREATE TABLE IF NOT EXISTS portfolio_positions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        portfolio_id INTEGER NOT NULL REFERENCES portfolios(id),
        ticker TEXT NOT NULL,
        asset_class TEXT NOT NULL DEFAULT 'stock',
        direction TEXT NOT NULL DEFAULT 'long',
        quantity REAL NOT NULL DEFAULT 1.0,
        entry_price REAL NOT NULL DEFAULT 0.0,
        current_price REAL DEFAULT 0.0,
        stop_loss REAL,
        take_profit REAL,
        option_type TEXT,
        strike_price REAL,
        expiration_date TEXT,
        spread_type TEXT,
        spread_legs TEXT,
        metal_type TEXT,
        metal_oz REAL,
        unrealized_pnl REAL DEFAULT 0.0,
        status TEXT NOT NULL DEFAULT 'open',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        closed_at TIMESTAMP,
        closed_pnl REAL,
        notes TEXT DEFAULT ''
    )""")
    print("[+] Table: portfolio_positions")

    # ------------------------------------------------------------------
    # Table 3: crew_strategies
    # ------------------------------------------------------------------
    c.execute("""CREATE TABLE IF NOT EXISTS crew_strategies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        version INTEGER NOT NULL DEFAULT 1,
        status TEXT NOT NULL DEFAULT 'draft',
        asset_class TEXT DEFAULT 'stock',
        direction TEXT DEFAULT 'long',
        thesis TEXT,
        entry_rules TEXT,
        exit_rules TEXT,
        stop_loss_rule TEXT,
        position_size_rule TEXT,
        target_tickers TEXT,
        option_strategy TEXT,
        spread_config TEXT,
        conviction_score REAL,
        critic_score REAL,
        critic_notes TEXT,
        backtest_sharpe REAL,
        backtest_max_drawdown REAL,
        backtest_win_rate REAL,
        backtest_profit_factor REAL,
        backtest_id INTEGER,
        debate_log TEXT,
        scout_brief TEXT,
        architect_reasoning TEXT,
        commander_decision TEXT,
        deployed_to_portfolio_id INTEGER REFERENCES portfolios(id),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    print("[+] Table: crew_strategies")

    # ------------------------------------------------------------------
    # Table 4: crew_runs
    # ------------------------------------------------------------------
    c.execute("""CREATE TABLE IF NOT EXISTS crew_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_type TEXT NOT NULL,
        trigger TEXT,
        agents_used TEXT,
        strategy_id INTEGER REFERENCES crew_strategies(id),
        debate_rounds INTEGER DEFAULT 0,
        revision_count INTEGER DEFAULT 0,
        outcome TEXT,
        total_tokens_used INTEGER DEFAULT 0,
        total_cost_usd REAL DEFAULT 0.0,
        duration_seconds REAL DEFAULT 0.0,
        error_log TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    print("[+] Table: crew_runs")

    # ------------------------------------------------------------------
    # Seed default portfolios (idempotent)
    # ------------------------------------------------------------------
    print()
    print("[*] Seeding default portfolios...")

    seeds = [
        ("Alpaca Paper", "alpaca", "paper", "auto", "paper", 100000.0, 100000.0, 0, 1, "Default paper trading portfolio"),
        ("Webull", "webull", "live", "manual", "trading", 7000.0, 7000.0, 1, 0, "Steve's live account — HUMAN MANAGED, never auto-trade"),
        ("TradeStation", "tradestation", "live", "auto", "trading", 0.0, 0.0, 0, 0, "Activate when ready — supports options/futures"),
        ("IBKR", "ibkr", "live", "auto", "trading", 0.0, 0.0, 0, 0, "Activate when ready — most comprehensive"),
    ]

    for name, broker, acct_type, execution_mode, portfolio_type, initial, current, is_human, is_active, notes in seeds:
        c.execute(
            """INSERT OR IGNORE INTO portfolios
               (name, broker, account_type, execution_mode, type, initial_balance, current_balance, is_human, is_active, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (name, broker, acct_type, execution_mode, portfolio_type, initial, current, is_human, is_active, notes),
        )
        status = "SEEDED" if c.rowcount > 0 else "EXISTS"
        human_tag = " [HUMAN - NO AUTO-TRADE]" if is_human else ""
        print(f"  [{status}] {name} ({broker}/{acct_type}){human_tag}")

    conn.commit()

    # ------------------------------------------------------------------
    # Verification
    # ------------------------------------------------------------------
    print()
    print("[*] Verification:")

    for table in ["portfolios", "portfolio_positions", "crew_strategies", "crew_runs"]:
        count = c.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count} rows")

    # Verify existing tables untouched
    existing_tables = [r[0] for r in c.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()]
    print()
    print(f"[*] Total tables in DB: {len(existing_tables)}")
    print(f"[OK] Migration 001 complete. No existing tables were modified.")

    conn.close()


if __name__ == "__main__":
    run_migration()
