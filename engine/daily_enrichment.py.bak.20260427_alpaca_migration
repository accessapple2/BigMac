"""daily_enrichment.py — Collects end-of-day data to make tomorrow smarter.

Runs at 3 PM AZ (5 PM ET) every trading day via main.py schedule.
All data is INSERT-only — never modifies existing records.
"""
from __future__ import annotations

import sqlite3
import logging
import time
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)
DB_PATH = "data/trader.db"

_ran_today: str | None = None


def _ensure_tables(db: sqlite3.Connection) -> None:
    db.executescript("""
        CREATE TABLE IF NOT EXISTS earnings_impact (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            report_date TEXT NOT NULL,
            expected_eps REAL,
            actual_eps REAL,
            beat_miss TEXT,
            price_reaction_1d REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_ei_sym ON earnings_impact(symbol, report_date);

        CREATE TABLE IF NOT EXISTS sector_rotation (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT NOT NULL,
            sector TEXT NOT NULL,
            change_pct REAL,
            rank INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_sr_date_sector ON sector_rotation(trade_date, sector);

        CREATE TABLE IF NOT EXISTS options_flow_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            gex_value REAL,
            gex_regime TEXT,
            put_call_ratio REAL,
            iv_rank REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_ofh_date_sym ON options_flow_history(symbol, trade_date);

        CREATE TABLE IF NOT EXISTS news_impact (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            signal_ts TEXT NOT NULL,
            sentiment TEXT,
            price_1h_pct REAL,
            price_4h_pct REAL,
            price_1d_pct REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    db.commit()


def _collect_sector_rotation(db: sqlite3.Connection) -> int:
    """Track today's sector ETF performance."""
    sector_etfs = {
        "Technology": "XLK", "Healthcare": "XLV", "Financials": "XLF",
        "Energy": "XLE", "Consumer Discretionary": "XLY",
        "Industrials": "XLI", "Materials": "XLB", "Utilities": "XLU",
        "Real Estate": "XLRE", "Communication": "XLC",
    }
    try:
        import yfinance as yf
        today = datetime.now().strftime("%Y-%m-%d")
        tickers = list(sector_etfs.values())
        data = yf.download(tickers, period="2d", interval="1d",
                           group_by="ticker", auto_adjust=True, progress=False)
        changes: list[tuple[str, float]] = []
        for sector, etf in sector_etfs.items():
            try:
                df = data[etf] if len(tickers) > 1 else data
                if df is None or len(df) < 2:
                    continue
                c0, c1 = float(df.iloc[-2]["Close"]), float(df.iloc[-1]["Close"])
                pct = (c1 - c0) / c0 * 100
                changes.append((sector, pct))
            except Exception:
                pass
        changes.sort(key=lambda x: x[1], reverse=True)
        for rank, (sector, pct) in enumerate(changes, 1):
            db.execute("""
                INSERT OR REPLACE INTO sector_rotation (trade_date, sector, change_pct, rank)
                VALUES (?,?,?,?)
            """, (today, sector, round(pct, 3), rank))
        db.commit()
        return len(changes)
    except Exception as e:
        logger.debug("sector_rotation: %s", e)
        return 0


def _collect_options_flow(db: sqlite3.Connection) -> int:
    """Store GEX snapshot for tracked symbols."""
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        atdb = sqlite3.connect("autonomous_trader.db", timeout=5)
        atdb.row_factory = sqlite3.Row
        rows = atdb.execute(
            "SELECT symbol, gex_value, regime, put_wall, call_wall FROM gex_levels"
            " ORDER BY updated_at DESC"
        ).fetchall()
        atdb.close()
        count = 0
        for r in rows:
            db.execute("""
                INSERT OR REPLACE INTO options_flow_history
                    (symbol, trade_date, gex_value, gex_regime)
                VALUES (?,?,?,?)
            """, (r["symbol"], today, r.get("gex_value"), r.get("regime")))
            count += 1
        db.commit()
        return count
    except Exception as e:
        logger.debug("options_flow: %s", e)
        return 0


def run_daily_enrichment() -> None:
    """Main entry point — called at 3 PM AZ on trading days."""
    global _ran_today
    today = datetime.now().strftime("%Y-%m-%d")
    if _ran_today == today:
        return
    # Only run on weekdays
    if datetime.now().weekday() >= 5:
        return
    # Only run after 2:30 PM AZ
    now_h = datetime.now().hour + datetime.now().minute / 60
    if now_h < 14.5:
        return

    _ran_today = today
    try:
        db = sqlite3.connect(DB_PATH, timeout=10)
        _ensure_tables(db)
        n_sectors = _collect_sector_rotation(db)
        n_options = _collect_options_flow(db)
        db.close()
        logger.info("[ENRICHMENT] %s: %d sector readings, %d GEX snapshots",
                    today, n_sectors, n_options)
    except Exception as e:
        logger.warning("daily_enrichment: %s", e)
