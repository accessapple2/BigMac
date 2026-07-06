"""Full Universe Manager — all tradeable US stocks from Alpaca.

Maintains the master list of every active, tradeable US equity so the
volume scanner has a complete universe to sweep. Stored in universe_stocks.

Functions:
    refresh_universe()  — Pulls from Alpaca /v2/assets, called weekly (Sunday 10 PM MST)
    get_universe()      — Returns list of all symbols from DB
"""
from __future__ import annotations

import logging
import os
import sqlite3
import contextlib
from datetime import datetime, timezone

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TRADER_DB = "data/trader.db"
ALPACA_TRADING_BASE = "https://paper-api.alpaca.markets"
EXCHANGES = {"NASDAQ", "NYSE", "AMEX", "ARCA"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [full_universe] %(levelname)s: %(message)s")
logger = logging.getLogger("full_universe")


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
    with contextlib.closing(_conn()) as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS universe_stocks (
                symbol TEXT PRIMARY KEY,
                name TEXT,
                exchange TEXT,
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
    key = os.getenv("APCA_API_KEY_ID", "")
    secret = os.getenv("APCA_API_SECRET_KEY", "")
    if not key or not secret:
        raise RuntimeError("APCA_API_KEY_ID / APCA_API_SECRET_KEY not set in .env")
    return {
        "APCA-API-KEY-ID": key,
        "APCA-API-SECRET-KEY": secret,
        "Accept": "application/json",
    }


_TRADABLE_CACHE: dict = {"set": None, "ts": 0.0}
_TRADABLE_TTL = 6 * 3600  # 6h


def tradable_symbols(ttl: int = _TRADABLE_TTL):
    """HM-MCW-PHANTOM 2026-06-15 — ground-truth set of currently Alpaca-tradable US-equity
    symbols (status=active & tradable=true), cached ~6h. Used to prune delisted/suspended
    names (MCW/HOLX/CTRA/… all return no bars + status=inactive) from the scan-emit path.
    Returns None on fetch failure or a suspiciously small set → callers MUST FAIL-OPEN
    (do not filter when None), so an API blip never guts the universe."""
    import time as _t
    now = _t.time()
    cached = _TRADABLE_CACHE.get("set")
    if cached is not None and (now - _TRADABLE_CACHE.get("ts", 0)) < ttl:
        return cached
    try:
        resp = requests.get(
            f"{ALPACA_TRADING_BASE}/v2/assets", headers=_alpaca_headers(),
            params={"status": "active", "tradable": "true", "asset_class": "us_equity"},
            timeout=60,
        )
        resp.raise_for_status()
        syms = {a.get("symbol") for a in resp.json() if a.get("tradable") and a.get("symbol")}
        if len(syms) < 1000:  # sanity: a real fetch returns thousands; refuse partial/garbage
            logger.warning(f"tradable_symbols got suspiciously few ({len(syms)}) — fail-open")
            return None
        _TRADABLE_CACHE["set"] = syms
        _TRADABLE_CACHE["ts"] = now
        return syms
    except Exception as e:
        logger.warning(f"tradable_symbols fetch failed (fail-open): {type(e).__name__}: {e}")
        return None


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def refresh_universe() -> int:
    """Pull all active tradeable US equities from Alpaca and store in universe_stocks.

    Runs weekly (Sunday 10 PM MST). Returns count of symbols loaded.
    """
    _init_tables()
    headers = _alpaca_headers()

    logger.info("Fetching full asset list from Alpaca...")
    all_assets: list[dict] = []
    url = f"{ALPACA_TRADING_BASE}/v2/assets"

    try:
        resp = requests.get(
            url,
            headers=headers,
            params={"status": "active", "tradable": "true", "asset_class": "us_equity"},
            timeout=60,
        )
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        logger.error(f"Alpaca assets fetch failed: {e}")
        return 0

    # Filter by exchange
    for asset in raw:
        if not asset.get("tradable"):
            continue
        exch = asset.get("exchange", "")
        if exch not in EXCHANGES:
            continue
        sym = asset.get("symbol", "")
        # Skip symbols with special characters (warrants, preferred, etc.)
        if not sym or not sym.isalpha() or len(sym) > 5:
            continue
        all_assets.append({
            "symbol": sym,
            "name": asset.get("name", ""),
            "exchange": exch,
        })

    if not all_assets:
        logger.warning("No assets returned from Alpaca — check API keys")
        return 0

    # Upsert into DB
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')  # HM-TZ-COMPLETION 2026-06-02: universe_stocks.updated_at canonical space-UTC
    with contextlib.closing(_conn()) as c:
        c.executemany(
            """
            INSERT INTO universe_stocks (symbol, name, exchange, updated_at)
            VALUES (:symbol, :name, :exchange, :updated_at)
            ON CONFLICT(symbol) DO UPDATE SET
                name=excluded.name,
                exchange=excluded.exchange,
                updated_at=excluded.updated_at
            """,
            [{**a, "updated_at": now} for a in all_assets],
        )
        c.commit()

    count = len(all_assets)
    logger.info(f"Universe refreshed: {count} tradeable symbols loaded")
    return count


_UNIVERSE_MIN_VOLUME = 1_000_000  # only liquid symbols; cuts 10k → ~1,700
_UNIVERSE_HARD_CAP   = 500        # fallback cap when volume data unavailable


def get_universe() -> list[str]:
    """Return liquid symbols from universe_stocks, filtered by avg_volume >= 1M.

    Joins scan_universe for volume data (same DB).  If that join yields nothing
    (scan_universe not yet populated), falls back to top-500 alphabetically.
    Falls back to S&P 500 + extras if the table is empty.
    """
    _init_tables()
    try:
        from config import DELISTED_BLACKLIST
    except Exception:
        DELISTED_BLACKLIST = set()

    try:
        with contextlib.closing(_conn()) as c:
            # Primary: join scan_universe for volume filter, sorted best-first
            rows = c.execute(
                """
                SELECT u.symbol
                FROM universe_stocks u
                INNER JOIN scan_universe s ON u.symbol = s.symbol
                WHERE s.avg_volume >= ?
                ORDER BY s.avg_volume DESC
                """,
                (_UNIVERSE_MIN_VOLUME,),
            ).fetchall()
        symbols = [r["symbol"] for r in rows if r["symbol"] not in DELISTED_BLACKLIST]
        if symbols:
            logger.info("get_universe: %d liquid symbols (avg_vol >= %s)", len(symbols), f"{_UNIVERSE_MIN_VOLUME:,}")
            return symbols
    except Exception as e:
        logger.warning(f"get_universe volume-join failed: {e}")

    # Fallback: no volume data yet — return top 500 alphabetically
    try:
        with contextlib.closing(_conn()) as c:
            rows = c.execute(
                "SELECT symbol FROM universe_stocks ORDER BY symbol LIMIT ?",
                (_UNIVERSE_HARD_CAP,),
            ).fetchall()
        symbols = [r["symbol"] for r in rows if r["symbol"] not in DELISTED_BLACKLIST]
        if symbols:
            logger.info("get_universe fallback (no volume data): %d symbols (cap=%d)", len(symbols), _UNIVERSE_HARD_CAP)
            return symbols
    except Exception as e:
        logger.warning(f"get_universe DB read failed: {e}")

    # Last resort: S&P 500 from universe_scanner
    try:
        from engine.universe_scanner import _get_sp500_tickers, EXTRA_TICKERS
        tickers = list(set(_get_sp500_tickers() + EXTRA_TICKERS))
        logger.info(f"get_universe last-resort: {len(tickers)} S&P 500 + extras")
        return tickers
    except Exception:
        return []


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    count = refresh_universe()
    print(f"Universe refreshed: {count} symbols")
    universe = get_universe()
    print(f"get_universe() returns {len(universe)} symbols")
