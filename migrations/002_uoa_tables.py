"""
TradeMinds Migration 002: Unusual Options Activity (UOA) Module
===============================================================
Adds tables to trader.db. NEVER drops or modifies existing tables.
All existing data (trader.db, arena.db) remains sacred and untouched.

Run: python migrations/002_uoa_tables.py
"""

import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'trader.db')

TABLES = [
    # Raw options flow data - every unusual trade we detect
    """
    CREATE TABLE IF NOT EXISTS uoa_flow (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scan_date TEXT NOT NULL,
        scan_time TEXT NOT NULL,
        ticker TEXT NOT NULL,
        contract_type TEXT NOT NULL,          -- 'CALL' or 'PUT'
        strike REAL NOT NULL,
        expiration TEXT NOT NULL,
        dte INTEGER,                          -- days to expiry
        volume INTEGER,
        open_interest INTEGER,
        vol_oi_ratio REAL,                    -- volume / open_interest
        last_price REAL,
        bid REAL,
        ask REAL,
        implied_volatility REAL,
        premium_total REAL,                   -- volume * last_price * 100
        moneyness TEXT,                       -- 'ITM', 'ATM', 'OTM'
        underlying_price REAL,
        pct_otm REAL,                         -- % out of the money
        sentiment TEXT,                       -- 'BULLISH', 'BEARISH', 'NEUTRAL'
        source TEXT,                          -- 'yfinance', 'barchart', 'cboe'
        created_at TEXT DEFAULT (datetime('now')),
        UNIQUE(scan_date, ticker, contract_type, strike, expiration)
    )
    """,

    # Aggregated UOA alerts - flagged by our scanner logic
    """
    CREATE TABLE IF NOT EXISTS uoa_alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        alert_date TEXT NOT NULL,
        alert_time TEXT NOT NULL,
        ticker TEXT NOT NULL,
        alert_type TEXT NOT NULL,             -- 'VOL_SPIKE', 'BIG_PREMIUM', 'PUT_WALL', 'CALL_SWEEP', 'SMART_MONEY'
        severity TEXT NOT NULL,               -- 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'
        contract_type TEXT,
        strike REAL,
        expiration TEXT,
        vol_oi_ratio REAL,
        premium_total REAL,
        underlying_price REAL,
        description TEXT,                     -- human-readable summary
        chekov_match INTEGER DEFAULT 0,       -- 1 if ticker is in Chekov's watchlist
        convergence_score REAL DEFAULT 0,     -- 0-100, how many signals align
        acted_on INTEGER DEFAULT 0,           -- 1 if crew reviewed it
        outcome TEXT,                         -- filled post-hoc: 'WIN', 'LOSS', 'EXPIRED'
        outcome_pnl REAL,                     -- profit/loss if tracked
        created_at TEXT DEFAULT (datetime('now'))
    )
    """,

    # Daily put/call ratio and flow summary per ticker
    """
    CREATE TABLE IF NOT EXISTS uoa_daily_summary (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scan_date TEXT NOT NULL,
        ticker TEXT NOT NULL,
        total_call_volume INTEGER DEFAULT 0,
        total_put_volume INTEGER DEFAULT 0,
        put_call_ratio REAL,
        total_call_premium REAL DEFAULT 0,
        total_put_premium REAL DEFAULT 0,
        premium_put_call_ratio REAL,
        max_vol_oi_ratio REAL,
        unusual_contracts INTEGER DEFAULT 0,  -- count of contracts flagged unusual
        avg_iv REAL,
        underlying_close REAL,
        created_at TEXT DEFAULT (datetime('now')),
        UNIQUE(scan_date, ticker)
    )
    """,

    # Scan run log
    """
    CREATE TABLE IF NOT EXISTS uoa_scan_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scan_date TEXT NOT NULL,
        scan_time TEXT NOT NULL,
        scan_type TEXT NOT NULL,              -- 'FULL', 'WATCHLIST', 'QUICK'
        tickers_scanned INTEGER DEFAULT 0,
        contracts_found INTEGER DEFAULT 0,
        alerts_generated INTEGER DEFAULT 0,
        errors TEXT,
        duration_seconds REAL,
        created_at TEXT DEFAULT (datetime('now'))
    )
    """
]

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_uoa_flow_ticker ON uoa_flow(ticker)",
    "CREATE INDEX IF NOT EXISTS idx_uoa_flow_date ON uoa_flow(scan_date)",
    "CREATE INDEX IF NOT EXISTS idx_uoa_flow_ratio ON uoa_flow(vol_oi_ratio)",
    "CREATE INDEX IF NOT EXISTS idx_uoa_flow_premium ON uoa_flow(premium_total)",
    "CREATE INDEX IF NOT EXISTS idx_uoa_alerts_date ON uoa_alerts(alert_date)",
    "CREATE INDEX IF NOT EXISTS idx_uoa_alerts_ticker ON uoa_alerts(ticker)",
    "CREATE INDEX IF NOT EXISTS idx_uoa_alerts_severity ON uoa_alerts(severity)",
    "CREATE INDEX IF NOT EXISTS idx_uoa_daily_date ON uoa_daily_summary(scan_date)",
]


def run_migration(db_path=None):
    """Run migration - safe to run multiple times (IF NOT EXISTS)."""
    path = db_path or DB_PATH
    print(f"[UOA Migration] Connecting to {path}")

    conn = sqlite3.connect(path)
    cursor = conn.cursor()

    for sql in TABLES:
        table_name = sql.split("CREATE TABLE IF NOT EXISTS")[1].split("(")[0].strip()
        print(f"  Creating table: {table_name}")
        cursor.execute(sql)

    for sql in INDEXES:
        cursor.execute(sql)
        idx_name = sql.split("CREATE INDEX IF NOT EXISTS")[1].split(" ON")[0].strip()
        print(f"  Creating index: {idx_name}")

    conn.commit()
    conn.close()
    print(f"[UOA Migration] Done. 4 tables, {len(INDEXES)} indexes created.")


if __name__ == "__main__":
    run_migration()
