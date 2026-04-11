"""Volume Baselines — 20-day rolling average volume for every stock in the universe.

Data source: Alpaca snapshots (prevDailyBar.v = yesterday's completed volume).
Accumulates one row per symbol per day in volume_daily_log, then computes the
rolling 20-day average into volume_baselines for fast scanner lookups.

Free-tier safe: uses the same snapshot API as the volume scanner (batch of 1000 symbols).
No per-symbol REST calls, no Yahoo Finance.

Functions:
    update_baselines()  — Nightly 11 PM MST (weeknights): snapshot all symbols,
                          log yesterday's volume, recompute 20d averages.
    get_baselines()     — Returns {symbol: avg_volume_20d} for fast scanner use.
"""
from __future__ import annotations

import logging
import os
import sqlite3
import time
from datetime import date, datetime, timedelta
from typing import Optional

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TRADER_DB = "data/trader.db"
ALPACA_DATA_BASE = "https://data.alpaca.markets"
SNAPSHOT_BATCH = 1000
REQ_DELAY = 0.15     # between snapshot batches
ROLLING_DAYS = 20    # days to average

logging.basicConfig(level=logging.INFO, format="%(asctime)s [volume_baselines] %(levelname)s: %(message)s")
logger = logging.getLogger("volume_baselines")


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(TRADER_DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def _init_tables():
    with _conn() as c:
        # Rolling daily volume log — one row per symbol per day
        c.execute("""
            CREATE TABLE IF NOT EXISTS volume_daily_log (
                symbol TEXT NOT NULL,
                log_date DATE NOT NULL,
                daily_volume REAL NOT NULL,
                PRIMARY KEY (symbol, log_date)
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_vdl_date ON volume_daily_log(log_date)")
        # Computed 20-day average — updated after each nightly log run
        c.execute("""
            CREATE TABLE IF NOT EXISTS volume_baselines (
                symbol TEXT PRIMARY KEY,
                avg_volume_20d REAL,
                days_sampled INTEGER DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        c.commit()


# ---------------------------------------------------------------------------
# Alpaca auth headers
# ---------------------------------------------------------------------------

def _alpaca_headers() -> dict:
    from dotenv import load_dotenv
    load_dotenv()
    key = os.getenv("ALPACA_API_KEY", "")
    secret = os.getenv("ALPACA_SECRET_KEY", "")
    if not key or not secret:
        raise RuntimeError("ALPACA_API_KEY / ALPACA_SECRET_KEY not set in .env")
    return {
        "APCA-API-KEY-ID": key,
        "APCA-API-SECRET-KEY": secret,
        "Accept": "application/json",
    }


# ---------------------------------------------------------------------------
# Snapshot-based volume logging
# ---------------------------------------------------------------------------

def _fetch_snapshot_volumes(symbols: list[str], headers: dict) -> dict[str, float]:
    """Fetch prevDailyBar.v (yesterday's completed volume) from snapshots.

    Returns {symbol: yesterday_volume} for all symbols that returned data.
    """
    symbols_csv = ",".join(symbols)
    try:
        resp = requests.get(
            f"{ALPACA_DATA_BASE}/v2/stocks/snapshots",
            headers=headers,
            params={"symbols": symbols_csv, "feed": "iex"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning(f"Snapshot fetch failed ({len(symbols)} symbols): {e}")
        return {}

    result = {}
    for sym, snap in data.items():
        try:
            prev_bar = snap.get("prevDailyBar") or snap.get("prev_daily_bar") or {}
            vol = prev_bar.get("v", 0)
            if vol and vol > 0:
                result[sym] = float(vol)
        except Exception:
            continue
    return result


def _recompute_averages(symbols: list[str], log_date: str) -> int:
    """Recompute rolling ROLLING_DAYS-day average for given symbols.

    Uses the last ROLLING_DAYS rows from volume_daily_log per symbol.
    Returns count of baselines updated.
    """
    cutoff = (date.fromisoformat(log_date) - timedelta(days=ROLLING_DAYS + 5)).isoformat()
    now = datetime.now().isoformat()
    updated = 0

    with _conn() as c:
        placeholders = ",".join("?" * len(symbols))
        rows = c.execute(
            f"""
            SELECT symbol, AVG(daily_volume) as avg_vol, COUNT(*) as days
            FROM volume_daily_log
            WHERE symbol IN ({placeholders})
              AND log_date >= ?
            GROUP BY symbol
            HAVING COUNT(*) >= 1
            """,
            symbols + [cutoff],
        ).fetchall()

        for row in rows:
            c.execute(
                """
                INSERT INTO volume_baselines (symbol, avg_volume_20d, days_sampled, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    avg_volume_20d=excluded.avg_volume_20d,
                    days_sampled=excluded.days_sampled,
                    updated_at=excluded.updated_at
                """,
                (row["symbol"], row["avg_vol"], row["days"], now),
            )
            updated += 1
        c.commit()

    return updated


# ---------------------------------------------------------------------------
# Core function
# ---------------------------------------------------------------------------

def update_baselines() -> int:
    """Log yesterday's volume for all universe symbols; recompute 20-day averages.

    Runs nightly at 11 PM MST (weeknights). Uses Alpaca snapshots — no per-symbol API calls.
    Returns count of symbols with updated baselines.
    """
    _init_tables()
    headers = _alpaca_headers()

    from engine.full_universe import get_universe
    universe = get_universe()
    if not universe:
        logger.warning("Universe is empty — run refresh_universe() first")
        return 0

    # Yesterday's date (the completed trading day we're logging)
    yesterday = (date.today() - timedelta(days=1)).isoformat()

    logger.info(f"Updating volume baselines for {len(universe)} symbols (date={yesterday})...")
    total_logged = 0

    # Collect volumes from snapshots
    vol_map: dict[str, float] = {}
    for i in range(0, len(universe), SNAPSHOT_BATCH):
        batch = universe[i : i + SNAPSHOT_BATCH]
        vols = _fetch_snapshot_volumes(batch, headers)
        vol_map.update(vols)
        if i > 0 and i % 5000 == 0:
            logger.info(f"  Snapshot progress: {i}/{len(universe)}, {len(vol_map)} volumes collected")
        time.sleep(REQ_DELAY)

    logger.info(f"Volumes collected: {len(vol_map)} symbols")
    if not vol_map:
        return 0

    # Log to volume_daily_log
    with _conn() as c:
        c.executemany(
            "INSERT OR REPLACE INTO volume_daily_log (symbol, log_date, daily_volume) VALUES (?,?,?)",
            [(sym, yesterday, vol) for sym, vol in vol_map.items()],
        )
        c.commit()
    total_logged = len(vol_map)

    # Recompute averages in batches to avoid long transactions
    symbols = list(vol_map.keys())
    updated = 0
    for i in range(0, len(symbols), 500):
        batch = symbols[i : i + 500]
        updated += _recompute_averages(batch, yesterday)

    # Prune old log entries (keep last 30 days)
    cutoff = (date.today() - timedelta(days=30)).isoformat()
    with _conn() as c:
        c.execute("DELETE FROM volume_daily_log WHERE log_date < ?", (cutoff,))
        c.commit()

    logger.info(f"Volume baselines updated: {updated} stocks (logged {total_logged} daily volumes)")
    return updated


def get_baselines(symbols: Optional[list[str]] = None) -> dict[str, float]:
    """Return {symbol: avg_volume_20d} from DB.

    Falls back to prevDailyBar snapshot data if baseline table is sparse.
    If symbols is given, only returns entries for those symbols.
    """
    _init_tables()
    try:
        with _conn() as c:
            if symbols:
                placeholders = ",".join("?" * len(symbols))
                rows = c.execute(
                    f"SELECT symbol, avg_volume_20d FROM volume_baselines WHERE symbol IN ({placeholders})",
                    symbols,
                ).fetchall()
            else:
                rows = c.execute("SELECT symbol, avg_volume_20d FROM volume_baselines").fetchall()
        return {r["symbol"]: r["avg_volume_20d"] for r in rows if r["avg_volume_20d"]}
    except Exception as e:
        logger.warning(f"get_baselines failed: {e}")
        return {}


def bootstrap_baselines_from_snapshots(symbols: Optional[list[str]] = None) -> int:
    """One-time bootstrap: collect prevDailyBar volume for all symbols via snapshots.

    Call this manually once to seed the baselines before the nightly job accumulates data.
    Returns count of symbols seeded.
    """
    _init_tables()
    headers = _alpaca_headers()

    from engine.full_universe import get_universe
    target = symbols or get_universe()
    if not target:
        return 0

    logger.info(f"Bootstrapping baselines for {len(target)} symbols via snapshots...")
    vol_map: dict[str, float] = {}
    for i in range(0, len(target), SNAPSHOT_BATCH):
        batch = target[i : i + SNAPSHOT_BATCH]
        vols = _fetch_snapshot_volumes(batch, headers)
        vol_map.update(vols)
        time.sleep(REQ_DELAY)

    if not vol_map:
        return 0

    # Store as both log entry (yesterday) and direct baseline
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    now = datetime.now().isoformat()

    with _conn() as c:
        c.executemany(
            "INSERT OR REPLACE INTO volume_daily_log (symbol, log_date, daily_volume) VALUES (?,?,?)",
            [(sym, yesterday, vol) for sym, vol in vol_map.items()],
        )
        c.executemany(
            """INSERT INTO volume_baselines (symbol, avg_volume_20d, days_sampled, updated_at)
               VALUES (?,?,?,?)
               ON CONFLICT(symbol) DO UPDATE SET
                   avg_volume_20d=excluded.avg_volume_20d,
                   days_sampled=1,
                   updated_at=excluded.updated_at""",
            [(sym, vol, 1, now) for sym, vol in vol_map.items()],
        )
        c.commit()

    count = len(vol_map)
    logger.info(f"Baseline bootstrap complete: {count} symbols seeded (1-day proxy until 20-day data accumulates)")
    return count


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    cmd = sys.argv[1] if len(sys.argv) > 1 else "update"
    if cmd == "bootstrap":
        count = bootstrap_baselines_from_snapshots()
        print(f"Bootstrap complete: {count} symbols seeded")
    elif cmd == "status":
        b = get_baselines()
        print(f"Baselines in DB: {len(b)} symbols")
        # Show a few
        for sym in list(b.keys())[:10]:
            print(f"  {sym}: {b[sym]:,.0f}")
    else:
        count = update_baselines()
        print(f"Baselines updated: {count} symbols")
