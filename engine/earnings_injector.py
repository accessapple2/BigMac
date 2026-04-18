#!/usr/bin/env python3
"""
Earnings Day Injector
Auto-adds today's earnings reporters to earnings_universe table
so the scanner can prioritize them.

Runs at 6:00 AM AZ before market open.
Source: earnings_impact table (populated by main pipeline).

Usage: cd ~/autonomous-trader && venv/bin/python3 engine/earnings_injector.py
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [EARNINGS] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "data" / "trader.db"


def init_table():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS earnings_universe (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker     TEXT NOT NULL,
            added_date TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ticker, added_date)
        )
    """)
    conn.commit()
    conn.close()


def get_todays_earnings() -> list[str]:
    """
    Pull tickers reporting earnings today from earnings_impact.
    Schema: symbol TEXT, report_date TEXT (YYYY-MM-DD).
    """
    conn  = sqlite3.connect(DB_PATH)
    today = datetime.now().strftime("%Y-%m-%d")
    tickers: list[str] = []

    try:
        rows = conn.execute(
            "SELECT DISTINCT symbol FROM earnings_impact WHERE date(report_date) = ?",
            (today,),
        ).fetchall()
        tickers = [r[0] for r in rows if r[0]]
    except Exception as e:
        log.warning(f"earnings_impact query failed: {e}")

    conn.close()
    return tickers


def inject_to_universe(tickers: list[str]) -> int:
    """Insert today's earnings tickers; UNIQUE(ticker, added_date) prevents duplicates."""
    if not tickers:
        return 0

    conn  = sqlite3.connect(DB_PATH)
    today = datetime.now().strftime("%Y-%m-%d")
    added = 0

    for ticker in tickers:
        try:
            conn.execute(
                "INSERT OR IGNORE INTO earnings_universe (ticker, added_date) VALUES (?, ?)",
                (ticker, today),
            )
            added += 1
        except Exception as e:
            log.warning(f"Insert failed for {ticker}: {e}")

    conn.commit()
    conn.close()
    return added


def get_active_earnings_universe() -> list[str]:
    """Return today's injected earnings tickers (for use by scanner)."""
    conn  = sqlite3.connect(DB_PATH)
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        rows = conn.execute(
            "SELECT ticker FROM earnings_universe WHERE added_date = ?", (today,)
        ).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []
    finally:
        conn.close()


def run():
    log.info(f"=== Earnings Injector — {datetime.now().strftime('%Y-%m-%d %H:%M')} ===")
    init_table()

    tickers = get_todays_earnings()
    if tickers:
        log.info(f"Found {len(tickers)} reporters today: {', '.join(tickers)}")
        added = inject_to_universe(tickers)
        log.info(f"Injected {added} tickers to earnings_universe")
    else:
        log.info("No earnings reporters found in earnings_impact for today")

    # Also confirm what's in the universe
    active = get_active_earnings_universe()
    log.info(f"Active earnings universe today: {active or 'empty'}")
    log.info("=== Earnings Injector done ===")


if __name__ == "__main__":
    run()
