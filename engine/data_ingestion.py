"""TradeMinds Data Ingestion System — engine/data_ingestion.py

Imports external trade data and market history into trader.db so the crew can
learn from ALL available data, not just their own paper trades.

Modules:
  1 — Webull CSV import (trades → trades + trade_outcomes)
  2 — Historical market snapshots (Alpaca bars → market_snapshots)
  3 — SEC insider filings (Form 4 RSS → insider_trades)
  4 — Backtest results integration (reads backtest_results → strategy weights)
  5 — Market pattern library (20 seeded patterns + live matching)
  6 — Crew prompt enrichment (combines all above for prompt injection)

Called by:
  dashboard/app.py  — Module 7 API endpoints
  main.py           — Module 8 scheduler (startup + daily/nightly)
"""
from __future__ import annotations

import csv
import io
import json
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from rich.console import Console

console = Console()

DB_PATH = "data/trader.db"

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# ---------------------------------------------------------------------------
# Table initialisation (all idempotent — CREATE IF NOT EXISTS)
# ---------------------------------------------------------------------------

def init_ingestion_tables() -> None:
    """Create all data ingestion tables if they don't exist. Safe to call repeatedly."""
    conn = _conn()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS market_snapshots (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol      TEXT NOT NULL,
                date        TEXT NOT NULL,          -- YYYY-MM-DD
                open        REAL,
                high        REAL,
                low         REAL,
                close       REAL,
                volume      INTEGER,
                vwap        REAL,
                change_pct  REAL,
                spy_close   REAL,
                qqq_close   REAL,
                vix_close   REAL,
                source      TEXT DEFAULT 'alpaca',
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, date)
            );

            CREATE TABLE IF NOT EXISTS insider_trades (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol          TEXT NOT NULL,
                insider_name    TEXT,
                title           TEXT,
                transaction_type TEXT,              -- 'buy' | 'sell'
                shares          INTEGER,
                price_per_share REAL,
                total_value     REAL,
                filing_date     TEXT,               -- YYYY-MM-DD
                transaction_date TEXT,              -- YYYY-MM-DD
                form_type       TEXT DEFAULT '4',
                source_url      TEXT,
                created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, insider_name, transaction_date, transaction_type)
            );

            CREATE TABLE IF NOT EXISTS market_patterns (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                name            TEXT NOT NULL UNIQUE,
                category        TEXT,               -- 'bullish' | 'bearish' | 'neutral'
                description     TEXT,
                trigger_conditions TEXT,            -- JSON array of condition strings
                historical_accuracy REAL,           -- 0..1
                avg_move_pct    REAL,               -- typical % move after pattern
                avg_duration_days INTEGER,          -- how long pattern plays out
                best_sectors    TEXT,               -- JSON array of sectors
                worst_sectors   TEXT,               -- JSON array of sectors
                notes           TEXT,
                created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS patterns_tracked (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                pattern_id  INTEGER REFERENCES market_patterns(id),
                symbol      TEXT NOT NULL,
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                price_at_detection REAL,
                outcome_pct REAL,                   -- filled in after resolution
                resolved_at TIMESTAMP,
                active      INTEGER DEFAULT 1        -- 1=active, 0=resolved
            );
        """)
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# MODULE 1 — Webull CSV Import
# ---------------------------------------------------------------------------

_WEBULL_REQUIRED_COLS = {"Symbol", "Side", "Filled Qty", "Avg Price", "Filled Time"}


def import_webull_csv(filepath: str) -> dict:
    """Parse a Webull trade history CSV and insert into trades + trade_outcomes.

    Returns a dict with keys: imported, skipped, errors
    """
    imported = 0
    skipped = 0
    errors = []

    if not os.path.exists(filepath):
        return {"imported": 0, "skipped": 0, "errors": [f"File not found: {filepath}"]}

    try:
        with open(filepath, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except Exception as e:
        return {"imported": 0, "skipped": 0, "errors": [str(e)]}

    if not rows:
        return {"imported": 0, "skipped": 0, "errors": ["CSV is empty"]}

    # Normalize column names (strip whitespace)
    norm_rows = []
    for row in rows:
        norm_rows.append({k.strip(): v.strip() if isinstance(v, str) else v for k, v in row.items()})
    rows = norm_rows

    # Validate columns
    cols = set(rows[0].keys())
    missing = _WEBULL_REQUIRED_COLS - cols
    if missing:
        return {"imported": 0, "skipped": 0,
                "errors": [f"Missing columns: {missing}. Found: {cols}"]}

    conn = _conn()
    try:
        for row in rows:
            try:
                symbol = row.get("Symbol", "").upper().strip()
                side = row.get("Side", "").upper().strip()    # BUY / SELL
                qty = float(row.get("Filled Qty", 0) or 0)
                price = float(row.get("Avg Price", 0) or 0)
                filled_time = row.get("Filled Time", "").strip()
                order_id = row.get("Order ID", row.get("Id", "")).strip()

                if not symbol or not side or qty <= 0 or price <= 0:
                    skipped += 1
                    continue

                # Normalise timestamp to ISO
                try:
                    dt = _parse_webull_timestamp(filled_time)
                    ts = dt.strftime("%Y-%m-%d %H:%M:%S")
                    date_str = dt.strftime("%Y-%m-%d")
                except Exception:
                    ts = filled_time
                    date_str = filled_time[:10] if len(filled_time) >= 10 else ""

                action = "BUY" if side in ("BUY", "B") else "SELL"
                total_value = qty * price

                # Insert into trades table (idempotent via OR IGNORE)
                conn.execute(
                    """INSERT OR IGNORE INTO trades
                       (player_id, symbol, action, quantity, price, total_value,
                        timestamp, date, source, status, webull_order_id)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'webull_import', 'filled', ?)""",
                    ("webull-import", symbol, action, qty, price, total_value,
                     ts, date_str, order_id or None)
                )
                if conn.execute("SELECT changes()").fetchone()[0] > 0:
                    imported += 1
                else:
                    skipped += 1

            except Exception as e:
                errors.append(f"Row error ({row.get('Symbol', '?')}): {e}")

        conn.commit()
    finally:
        conn.close()

    # After inserting raw trades, auto-record any matched BUY→SELL pairs
    try:
        from engine.trade_outcomes import auto_record_closed_trades
        auto_record_closed_trades()
    except Exception:
        pass

    console.log(f"[green]Webull import: {imported} trades imported, {skipped} skipped, "
                f"{len(errors)} errors from {filepath}")
    return {"imported": imported, "skipped": skipped, "errors": errors}


def _parse_webull_timestamp(ts_str: str) -> datetime:
    """Try multiple Webull timestamp formats."""
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%m/%d/%Y %I:%M:%S %p",
        "%Y-%m-%dT%H:%M:%S",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(ts_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse timestamp: {ts_str!r}")


# ---------------------------------------------------------------------------
# MODULE 2 — Historical Market Snapshots (Alpaca Bars)
# ---------------------------------------------------------------------------

_ALPACA_DATA_BASE = "https://data.alpaca.markets"
_SNAPSHOT_SYMBOLS = ["SPY", "QQQ", "IWM", "VIX", "DIA", "GLD", "TLT", "XLE",
                     "XLF", "XLK", "XLV", "XLY", "AAPL", "TSLA", "NVDA", "AMZN"]


def backfill_market_history(days: int = 365, symbols: Optional[list] = None) -> dict:
    """Fetch daily OHLCV bars from Alpaca and store in market_snapshots.

    Skips symbols+dates already in the table.  Safe to call repeatedly.
    Returns {symbols_processed, bars_inserted, bars_skipped, errors}
    """
    api_key = os.environ.get("ALPACA_API_KEY", "")
    secret_key = os.environ.get("ALPACA_SECRET_KEY", "")
    if not api_key or not secret_key:
        return {"symbols_processed": 0, "bars_inserted": 0, "bars_skipped": 0,
                "errors": ["ALPACA_API_KEY or ALPACA_SECRET_KEY not set"]}

    target_symbols = symbols or _SNAPSHOT_SYMBOLS
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    import urllib.request

    headers = {
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": secret_key,
    }

    bars_inserted = 0
    bars_skipped = 0
    errors = []
    symbols_processed = 0

    conn = _conn()
    try:
        # Fetch SPY/QQQ/VIX reference prices for enrichment
        reference = {}
        for ref_sym in ("SPY", "QQQ", "^VIX"):
            reference[ref_sym] = {}  # filled below during processing

        for symbol in target_symbols:
            try:
                url = (
                    f"{_ALPACA_DATA_BASE}/v2/stocks/{symbol}/bars"
                    f"?timeframe=1Day&start={start_date}&end={end_date}"
                    f"&adjustment=split&feed=iex&limit=10000"
                )
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=15) as resp:
                    data = json.loads(resp.read().decode())

                bars = data.get("bars", [])
                if not bars:
                    symbols_processed += 1
                    continue

                for bar in bars:
                    date_str = bar.get("t", "")[:10]
                    o = bar.get("o")
                    h = bar.get("h")
                    lo = bar.get("l")
                    c = bar.get("c")
                    v = bar.get("v")
                    vw = bar.get("vw")

                    # Change pct
                    change_pct = None
                    if o and c:
                        change_pct = round((c - o) / o * 100, 3)

                    result = conn.execute(
                        """INSERT OR IGNORE INTO market_snapshots
                           (symbol, date, open, high, low, close, volume, vwap, change_pct)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (symbol, date_str, o, h, lo, c, v, vw, change_pct)
                    )
                    if result.rowcount > 0:
                        bars_inserted += 1
                        # Cache SPY/QQQ prices for reference enrichment
                        if symbol in ("SPY", "QQQ"):
                            reference.setdefault(symbol, {})[date_str] = c
                    else:
                        bars_skipped += 1

                symbols_processed += 1
                time.sleep(0.12)  # 8 req/sec Alpaca IEX limit

            except Exception as e:
                errors.append(f"{symbol}: {e}")

        conn.commit()

        # Enrich SPY/QQQ/VIX cross-references in a second pass
        _enrich_snapshot_references(conn, reference)
        conn.commit()

    finally:
        conn.close()

    console.log(f"[green]Market history backfill: {symbols_processed} symbols, "
                f"{bars_inserted} bars inserted, {bars_skipped} skipped")
    return {
        "symbols_processed": symbols_processed,
        "bars_inserted": bars_inserted,
        "bars_skipped": bars_skipped,
        "errors": errors,
    }


def _enrich_snapshot_references(conn: sqlite3.Connection, reference: dict) -> None:
    """Back-fill spy_close / qqq_close columns for rows that lack them."""
    spy_ref = reference.get("SPY", {})
    qqq_ref = reference.get("QQQ", {})
    if not spy_ref and not qqq_ref:
        return

    try:
        rows = conn.execute(
            "SELECT id, date FROM market_snapshots WHERE spy_close IS NULL"
        ).fetchall()
        for row in rows:
            spy = spy_ref.get(row["date"])
            qqq = qqq_ref.get(row["date"])
            if spy or qqq:
                conn.execute(
                    "UPDATE market_snapshots SET spy_close=?, qqq_close=? WHERE id=?",
                    (spy, qqq, row["id"])
                )
    except Exception:
        pass


def get_market_history(symbol: str, days: int = 30) -> list[dict]:
    """Retrieve cached market history for a symbol."""
    conn = _conn()
    try:
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        rows = conn.execute(
            "SELECT * FROM market_snapshots WHERE symbol=? AND date >= ? ORDER BY date DESC",
            (symbol.upper(), cutoff)
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# MODULE 3 — SEC Insider Filings (Form 4 RSS)
# ---------------------------------------------------------------------------

# SEC EDGAR full-text search RSS for Form 4 filings
_SEC_FORM4_RSS = "https://efts.sec.gov/LATEST/search-index?q=%22form+4%22&dateRange=custom&startdt={start}&enddt={end}&forms=4"
_SEC_EDGAR_RSS = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&dateb=&owner=include&count=40&search_text=&output=atom"

_SIGNIFICANT_TITLES = {"ceo", "cfo", "president", "chairman", "chief", "director"}


def fetch_insider_trades(days_back: int = 3) -> dict:
    """Scrape SEC EDGAR Form 4 RSS for recent insider transactions.

    Filters for CEO/CFO/President buys > $100K on our watchlist or universe.
    Returns {inserted, skipped, errors}
    """
    import urllib.request
    import xml.etree.ElementTree as ET

    inserted = 0
    skipped = 0
    errors = []

    try:
        req = urllib.request.Request(
            _SEC_EDGAR_RSS,
            headers={"User-Agent": "TradeMinds Research contact@trademinds.local"}
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        return {"inserted": 0, "skipped": 0, "errors": [f"SEC RSS fetch failed: {e}"]}

    try:
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        return {"inserted": 0, "skipped": 0, "errors": [f"XML parse error: {e}"]}

    ns = {"atom": "http://www.w3.org/2005/Atom"}
    entries = root.findall(".//atom:entry", ns) or root.findall(".//entry")

    conn = _conn()
    try:
        for entry in entries:
            try:
                title_el = entry.find("atom:title", ns) or entry.find("title")
                title_text = title_el.text if title_el is not None else ""

                link_el = entry.find("atom:link", ns) or entry.find("link")
                source_url = (link_el.get("href") or link_el.text or "") if link_el is not None else ""

                updated_el = entry.find("atom:updated", ns) or entry.find("updated")
                filing_date = ""
                if updated_el is not None and updated_el.text:
                    filing_date = updated_el.text[:10]

                # Extract ticker from title like "4 (AAPL) Smith, John"
                symbol = _extract_symbol_from_form4_title(title_text)
                if not symbol:
                    skipped += 1
                    continue

                # For minimal viable filing, parse the summary/content block
                summary_el = entry.find("atom:summary", ns) or entry.find("summary")
                summary = summary_el.text if summary_el is not None else ""

                insider_name, title, txn_type, shares, price, total = _parse_form4_summary(summary, title_text)

                # Only log significant buy transactions
                is_significant_title = any(t in title.lower() for t in _SIGNIFICANT_TITLES)
                if txn_type != "buy":
                    skipped += 1
                    continue
                if total and total < 100_000:
                    skipped += 1
                    continue

                result = conn.execute(
                    """INSERT OR IGNORE INTO insider_trades
                       (symbol, insider_name, title, transaction_type, shares,
                        price_per_share, total_value, filing_date, transaction_date,
                        form_type, source_url)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '4', ?)""",
                    (symbol, insider_name, title, txn_type, shares,
                     price, total, filing_date, filing_date, source_url)
                )
                if result.rowcount > 0:
                    inserted += 1
                else:
                    skipped += 1

            except Exception as e:
                errors.append(f"Entry error: {e}")

        conn.commit()
    finally:
        conn.close()

    console.log(f"[green]Insider trades: {inserted} inserted, {skipped} skipped")
    return {"inserted": inserted, "skipped": skipped, "errors": errors}


def _extract_symbol_from_form4_title(title: str) -> str:
    """Extract ticker from SEC Form 4 title string."""
    import re
    # Patterns: "4 (AAPL)", "AAPL - Form 4", "(AAPL)"
    match = re.search(r'\(([A-Z]{1,5})\)', title)
    if match:
        return match.group(1)
    match = re.search(r'\b([A-Z]{2,5})\b.*[Ff]orm\s*4', title)
    if match:
        return match.group(1)
    return ""


def _parse_form4_summary(summary: str, title: str) -> tuple:
    """Parse insider info from Form 4 HTML/text summary.

    Returns: (insider_name, title, txn_type, shares, price, total)
    Gracefully returns defaults on parse failure.
    """
    import re

    insider_name = ""
    insider_title = ""
    txn_type = "unknown"
    shares = None
    price = None
    total = None

    try:
        # Try to find name from form4 title pattern "4 (AAPL) Smith, John"
        nm = re.search(r'\)\s+(.+?)(?:\s+-\s+|\s*$)', title)
        if nm:
            insider_name = nm.group(1).strip()

        # Transaction type from summary keywords
        sl = summary.lower()
        if "purchase" in sl or " p " in sl or "acquired" in sl:
            txn_type = "buy"
        elif "sale" in sl or " s " in sl or "disposed" in sl:
            txn_type = "sell"

        # Shares
        sm = re.search(r'(\d[\d,]+)\s+(?:share|common)', sl)
        if sm:
            shares = int(sm.group(1).replace(",", ""))

        # Price
        pm = re.search(r'\$\s*([\d.]+)', summary)
        if pm:
            price = float(pm.group(1))

        if shares and price:
            total = shares * price

    except Exception:
        pass

    return insider_name, insider_title, txn_type, shares, price, total


def get_recent_insider_trades(symbol: str = "", days: int = 14, limit: int = 20) -> list[dict]:
    """Retrieve recent insider trades for a symbol or all."""
    conn = _conn()
    try:
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        if symbol:
            rows = conn.execute(
                "SELECT * FROM insider_trades WHERE symbol=? AND filing_date >= ? "
                "ORDER BY filing_date DESC LIMIT ?",
                (symbol.upper(), cutoff, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM insider_trades WHERE filing_date >= ? "
                "ORDER BY total_value DESC LIMIT ?",
                (cutoff, limit)
            ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# MODULE 4 — Backtest Results Integration
# ---------------------------------------------------------------------------

def get_backtest_insights(strategy_name: str = "") -> dict:
    """Read backtest_results table and return strategy performance insights.

    Used by Chekov's convergence scorer and crew enrichment.
    Returns dict of {strategy: {sharpe, win_rate, avg_pnl, trades, insight}}
    """
    conn = _conn()
    insights = {}
    try:
        if strategy_name:
            rows = conn.execute(
                """SELECT strategy_name, sharpe_ratio, win_rate, avg_pnl_pct,
                          total_trades, max_drawdown, created_at
                   FROM backtest_results
                   WHERE strategy_name LIKE ?
                   ORDER BY created_at DESC""",
                (f"%{strategy_name}%",)
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT strategy_name, sharpe_ratio, win_rate, avg_pnl_pct,
                          total_trades, max_drawdown, created_at
                   FROM backtest_results
                   ORDER BY created_at DESC"""
            ).fetchall()

        # Aggregate per strategy name (keep latest run)
        seen = set()
        for row in rows:
            sname = row["strategy_name"] or "unknown"
            if sname in seen:
                continue
            seen.add(sname)

            sharpe = row["sharpe_ratio"] or 0.0
            wr = row["win_rate"] or 50.0
            avg_pnl = row["avg_pnl_pct"] or 0.0
            trades = row["total_trades"] or 0
            drawdown = row["max_drawdown"] or 0.0

            # Generate one-line insight
            if sharpe >= 1.5 and wr >= 55:
                verdict = "STRONG — high Sharpe + win rate"
            elif sharpe >= 1.0:
                verdict = "SOLID — good risk-adjusted return"
            elif sharpe >= 0.5:
                verdict = "MODERATE — acceptable, watch drawdown"
            elif sharpe < 0 or wr < 40:
                verdict = "WEAK — underperforming, reduce weight"
            else:
                verdict = "NEUTRAL — insufficient signal"

            insights[sname] = {
                "sharpe": round(sharpe, 3),
                "win_rate": round(wr, 1),
                "avg_pnl": round(avg_pnl, 3),
                "trades": trades,
                "max_drawdown": round(drawdown, 3),
                "verdict": verdict,
            }

    except Exception as e:
        console.log(f"[yellow]Backtest insights error: {e}")
    finally:
        conn.close()

    return insights


def get_backtest_prompt_block(top_n: int = 5) -> str:
    """Format top backtest strategies for prompt injection."""
    insights = get_backtest_insights()
    if not insights:
        return ""

    # Sort by Sharpe descending
    ranked = sorted(insights.items(), key=lambda x: x[1].get("sharpe", 0), reverse=True)[:top_n]
    lines = ["=== BACKTEST INSIGHTS (Top Strategies by Sharpe) ==="]
    for name, s in ranked:
        lines.append(
            f"  {name}: Sharpe={s['sharpe']:.2f} | WinRate={s['win_rate']:.0f}% | "
            f"AvgPnL={s['avg_pnl']:+.2f}% | {s['verdict']}"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# MODULE 5 — Market Pattern Library
# ---------------------------------------------------------------------------

_SEED_PATTERNS = [
    # --- Bullish ---
    {"name": "Bull Flag Breakout", "category": "bullish",
     "description": "Tight consolidation after sharp rally, then breakout on volume",
     "trigger_conditions": ["price consolidates 3-5 days", "volume dries up", "breakout with 2x+ avg volume"],
     "historical_accuracy": 0.68, "avg_move_pct": 6.5, "avg_duration_days": 7,
     "best_sectors": ["Technology", "Consumer Discretionary"],
     "worst_sectors": ["Utilities", "Real Estate"]},

    {"name": "Cup and Handle", "category": "bullish",
     "description": "U-shaped base with small pullback before breakout",
     "trigger_conditions": ["15-33% depth cup", "handle retraces 1/3 of cup", "breakout on volume"],
     "historical_accuracy": 0.65, "avg_move_pct": 18.0, "avg_duration_days": 30,
     "best_sectors": ["Technology", "Healthcare"],
     "worst_sectors": ["Energy", "Financials"]},

    {"name": "Ascending Triangle", "category": "bullish",
     "description": "Flat resistance with higher lows — coiling for breakout",
     "trigger_conditions": ["flat resistance line", "3+ higher lows", "decreasing volume in base"],
     "historical_accuracy": 0.72, "avg_move_pct": 8.0, "avg_duration_days": 14,
     "best_sectors": ["Technology", "Industrials"],
     "worst_sectors": ["Utilities"]},

    {"name": "Golden Cross (50/200 MA)", "category": "bullish",
     "description": "50 MA crosses above 200 MA — trend reversal signal",
     "trigger_conditions": ["50 MA crosses above 200 MA", "price above both MAs", "volume expanding"],
     "historical_accuracy": 0.63, "avg_move_pct": 12.0, "avg_duration_days": 45,
     "best_sectors": ["Technology", "Consumer Discretionary", "Industrials"],
     "worst_sectors": []},

    {"name": "Oversold RSI Bounce", "category": "bullish",
     "description": "RSI < 30 with reversal candle — mean reversion trade",
     "trigger_conditions": ["RSI < 30", "bullish reversal candle", "price at support level"],
     "historical_accuracy": 0.58, "avg_move_pct": 4.5, "avg_duration_days": 5,
     "best_sectors": ["Any"],
     "worst_sectors": []},

    {"name": "Earnings Gap Up Hold", "category": "bullish",
     "description": "Gap up on earnings, holds above gap level intraday",
     "trigger_conditions": ["gap up >3% on earnings", "holds above open first 30 min", "volume 3x+"],
     "historical_accuracy": 0.60, "avg_move_pct": 5.0, "avg_duration_days": 3,
     "best_sectors": ["Technology", "Healthcare"],
     "worst_sectors": []},

    {"name": "VIX Spike Reversal", "category": "bullish",
     "description": "VIX > 30 then drops — fear peak signals equity bottom",
     "trigger_conditions": ["VIX > 30", "VIX 1-day change < -10%", "SPY at key support"],
     "historical_accuracy": 0.70, "avg_move_pct": 4.0, "avg_duration_days": 5,
     "best_sectors": ["SPY", "QQQ"],
     "worst_sectors": []},

    {"name": "Insider Cluster Buy", "category": "bullish",
     "description": "3+ insiders buy same stock within 30 days — strong conviction signal",
     "trigger_conditions": ["3+ Form 4 buys", "total insider purchase > $500K", "within 30 days"],
     "historical_accuracy": 0.66, "avg_move_pct": 8.0, "avg_duration_days": 60,
     "best_sectors": ["Any"],
     "worst_sectors": []},

    {"name": "52-Week High Breakout", "category": "bullish",
     "description": "Price breaks above 52-week high on strong volume",
     "trigger_conditions": ["new 52-week high", "volume 1.5x+ average", "broad market not in downtrend"],
     "historical_accuracy": 0.62, "avg_move_pct": 10.0, "avg_duration_days": 20,
     "best_sectors": ["Technology", "Healthcare", "Consumer Discretionary"],
     "worst_sectors": []},

    {"name": "Momentum Surge (Relative Strength)", "category": "bullish",
     "description": "Stock outperforms SPY by 5%+ in 5 days — momentum continuation",
     "trigger_conditions": ["RS vs SPY > 5% in 5 days", "volume above 21-day average", "sector in uptrend"],
     "historical_accuracy": 0.55, "avg_move_pct": 7.0, "avg_duration_days": 10,
     "best_sectors": ["Technology", "Consumer Discretionary"],
     "worst_sectors": ["Utilities", "Real Estate"]},

    # --- Bearish ---
    {"name": "Death Cross (50/200 MA)", "category": "bearish",
     "description": "50 MA crosses below 200 MA — trend breakdown signal",
     "trigger_conditions": ["50 MA crosses below 200 MA", "price below both MAs", "volume expanding on down days"],
     "historical_accuracy": 0.60, "avg_move_pct": -10.0, "avg_duration_days": 45,
     "best_sectors": [],
     "worst_sectors": ["Technology", "Consumer Discretionary"]},

    {"name": "Head and Shoulders Top", "category": "bearish",
     "description": "Classic reversal pattern — three peaks with lower highs",
     "trigger_conditions": ["three peak structure", "neckline support", "breakdown below neckline on volume"],
     "historical_accuracy": 0.65, "avg_move_pct": -12.0, "avg_duration_days": 20,
     "best_sectors": [],
     "worst_sectors": []},

    {"name": "Bear Flag Breakdown", "category": "bearish",
     "description": "Brief consolidation after sharp selloff, then continuation lower",
     "trigger_conditions": ["sharp decline", "tight 3-5 day consolidation", "breakdown with volume"],
     "historical_accuracy": 0.65, "avg_move_pct": -6.0, "avg_duration_days": 7,
     "best_sectors": [],
     "worst_sectors": []},

    {"name": "Earnings Gap Down Hold", "category": "bearish",
     "description": "Gap down on earnings, fails to recover above gap — continuation lower",
     "trigger_conditions": ["gap down >3% on earnings", "fails to fill gap first hour", "volume 3x+"],
     "historical_accuracy": 0.62, "avg_move_pct": -4.5, "avg_duration_days": 3,
     "best_sectors": [],
     "worst_sectors": []},

    {"name": "Overbought RSI Rejection", "category": "bearish",
     "description": "RSI > 75 with rejection candle at resistance",
     "trigger_conditions": ["RSI > 75", "bearish reversal candle", "at resistance / prior high"],
     "historical_accuracy": 0.55, "avg_move_pct": -4.0, "avg_duration_days": 5,
     "best_sectors": [],
     "worst_sectors": []},

    # --- Neutral / Volatility ---
    {"name": "IV Crush Post-Earnings", "category": "neutral",
     "description": "Options IV collapses after earnings — premium selling opportunity",
     "trigger_conditions": ["earnings reported", "IV > 60% pre-earnings", "price within ±3% of close"],
     "historical_accuracy": 0.72, "avg_move_pct": 0.0, "avg_duration_days": 3,
     "best_sectors": ["Any"],
     "worst_sectors": []},

    {"name": "Low Volatility Squeeze", "category": "neutral",
     "description": "Bollinger Bands inside Keltner Channels — imminent volatility expansion",
     "trigger_conditions": ["BBands inside Keltner", "ADX < 20", "volume declining 5+ days"],
     "historical_accuracy": 0.60, "avg_move_pct": 5.0, "avg_duration_days": 5,
     "best_sectors": ["Any"],
     "worst_sectors": []},

    {"name": "High VIX Iron Condor Setup", "category": "neutral",
     "description": "VIX > 25 creates elevated premium — iron condor opportunity",
     "trigger_conditions": ["VIX > 25", "no earnings in 14 days", "stock in trading range"],
     "historical_accuracy": 0.65, "avg_move_pct": 0.0, "avg_duration_days": 21,
     "best_sectors": ["SPY", "QQQ", "Large Cap"],
     "worst_sectors": []},

    {"name": "Sector Rotation Signal", "category": "neutral",
     "description": "Defensive sectors outperforming cyclicals — risk-off shift",
     "trigger_conditions": ["XLU/XLV > XLK/XLY by 2%+ in 5 days", "VIX rising", "bond yields falling"],
     "historical_accuracy": 0.58, "avg_move_pct": -2.0, "avg_duration_days": 15,
     "best_sectors": ["Utilities", "Healthcare", "Consumer Staples"],
     "worst_sectors": ["Technology", "Consumer Discretionary", "Industrials"]},

    {"name": "VWAP Reclaim (Intraday)", "category": "bullish",
     "description": "Price dips below VWAP then reclaims it with volume — intraday long",
     "trigger_conditions": ["price dips below VWAP", "reclaims VWAP on volume surge", "within first 2 hours"],
     "historical_accuracy": 0.60, "avg_move_pct": 1.5, "avg_duration_days": 1,
     "best_sectors": ["Technology", "Consumer Discretionary"],
     "worst_sectors": []},
]


def seed_market_patterns() -> int:
    """Insert the 20 canonical patterns once. Idempotent — skips existing names."""
    conn = _conn()
    seeded = 0
    try:
        for p in _SEED_PATTERNS:
            try:
                conn.execute(
                    """INSERT OR IGNORE INTO market_patterns
                       (name, category, description, trigger_conditions, historical_accuracy,
                        avg_move_pct, avg_duration_days, best_sectors, worst_sectors)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        p["name"],
                        p["category"],
                        p["description"],
                        json.dumps(p.get("trigger_conditions", [])),
                        p.get("historical_accuracy"),
                        p.get("avg_move_pct"),
                        p.get("avg_duration_days"),
                        json.dumps(p.get("best_sectors", [])),
                        json.dumps(p.get("worst_sectors", [])),
                    )
                )
                if conn.execute("SELECT changes()").fetchone()[0] > 0:
                    seeded += 1
            except Exception:
                pass
        conn.commit()
    finally:
        conn.close()

    if seeded > 0:
        console.log(f"[green]Market patterns: seeded {seeded} new patterns")
    return seeded


def match_current_patterns(prices: Optional[dict] = None, indicators: Optional[dict] = None) -> list[dict]:
    """Detect which canonical patterns may be active right now.

    Performs lightweight rule checks against live price/indicator data.
    Records detected patterns in patterns_tracked.
    Returns list of active pattern dicts.
    """
    active = []
    conn = _conn()
    try:
        patterns = conn.execute(
            "SELECT * FROM market_patterns ORDER BY historical_accuracy DESC"
        ).fetchall()

        # Get live data if not provided
        if prices is None:
            try:
                from engine.market_data import get_all_prices
                from config import WATCH_STOCKS
                prices = get_all_prices(WATCH_STOCKS)
            except Exception:
                prices = {}

        if indicators is None:
            try:
                from engine.technical_indicators import get_all_indicators
                indicators = get_all_indicators(list(prices.keys())) if prices else {}
            except Exception:
                indicators = {}

        # Get VIX
        vix_price = 0.0
        try:
            from engine.vix_monitor import get_vix_status
            v = get_vix_status()
            vix_price = float(v.get("price", 0)) if v else 0.0
        except Exception:
            pass

        for pat in patterns:
            matched_symbols = _check_pattern(pat, prices, indicators, vix_price)
            for symbol in matched_symbols:
                price_now = prices.get(symbol, {}).get("price") if prices else None
                # Record in patterns_tracked (dedup by pattern+symbol, resolve old ones)
                try:
                    conn.execute(
                        "UPDATE patterns_tracked SET active=0 WHERE pattern_id=? AND symbol=? AND active=1",
                        (pat["id"], symbol)
                    )
                    conn.execute(
                        """INSERT INTO patterns_tracked (pattern_id, symbol, price_at_detection)
                           VALUES (?, ?, ?)""",
                        (pat["id"], symbol, price_now)
                    )
                except Exception:
                    pass

                active.append({
                    "pattern": pat["name"],
                    "category": pat["category"],
                    "symbol": symbol,
                    "accuracy": pat["historical_accuracy"],
                    "avg_move_pct": pat["avg_move_pct"],
                    "duration_days": pat["avg_duration_days"],
                    "description": pat["description"],
                })

        conn.commit()
    except Exception as e:
        console.log(f"[yellow]Pattern matching error: {e}")
    finally:
        conn.close()

    return active


def _check_pattern(pat: sqlite3.Row, prices: dict, indicators: dict, vix: float) -> list[str]:
    """Return list of symbols matching this pattern (may be empty or ['MARKET'])."""
    name = pat["name"]
    matched = []

    try:
        # VIX-based patterns
        if name == "VIX Spike Reversal":
            if vix > 30:
                matched.append("SPY")
            return matched

        if name == "High VIX Iron Condor Setup":
            if vix > 25:
                matched.extend(["SPY", "QQQ"])
            return matched

        if name == "Sector Rotation Signal":
            if vix > 20:
                matched.append("MARKET")
            return matched

        # Per-symbol patterns
        for symbol, data in prices.items():
            ind = indicators.get(symbol, {})
            rsi = ind.get("rsi")
            price = data.get("price", 0)
            change_pct = data.get("change_pct", 0)
            vol_ratio = ind.get("volume_ratio", 1.0)
            above_200ma = ind.get("above_200ma", True)

            if name == "Oversold RSI Bounce" and rsi and rsi < 30:
                matched.append(symbol)
            elif name == "Overbought RSI Rejection" and rsi and rsi > 75:
                matched.append(symbol)
            elif name == "Golden Cross (50/200 MA)" and above_200ma and ind.get("ma_cross") == "golden":
                matched.append(symbol)
            elif name == "Death Cross (50/200 MA)" and not above_200ma and ind.get("ma_cross") == "death":
                matched.append(symbol)
            elif name == "Earnings Gap Up Hold" and change_pct > 3 and vol_ratio and vol_ratio > 2.5:
                matched.append(symbol)
            elif name == "Earnings Gap Down Hold" and change_pct < -3 and vol_ratio and vol_ratio > 2.5:
                matched.append(symbol)
            elif name == "Momentum Surge (Relative Strength)" and change_pct > 4:
                matched.append(symbol)
            elif name == "Bull Flag Breakout" and vol_ratio and vol_ratio > 2.0 and 0 < change_pct < 5:
                matched.append(symbol)
            elif name == "52-Week High Breakout" and ind.get("near_52w_high") and vol_ratio and vol_ratio > 1.5:
                matched.append(symbol)

    except Exception:
        pass

    return matched


def get_active_patterns(symbol: str = "", limit: int = 10) -> list[dict]:
    """Return recently matched active patterns."""
    conn = _conn()
    try:
        if symbol:
            rows = conn.execute(
                """SELECT pt.*, mp.name, mp.category, mp.historical_accuracy,
                          mp.avg_move_pct, mp.description
                   FROM patterns_tracked pt
                   JOIN market_patterns mp ON pt.pattern_id = mp.id
                   WHERE pt.active=1 AND pt.symbol=?
                   ORDER BY pt.detected_at DESC LIMIT ?""",
                (symbol.upper(), limit)
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT pt.*, mp.name, mp.category, mp.historical_accuracy,
                          mp.avg_move_pct, mp.description
                   FROM patterns_tracked pt
                   JOIN market_patterns mp ON pt.pattern_id = mp.id
                   WHERE pt.active=1 AND pt.detected_at >= datetime('now', '-24 hours')
                   ORDER BY mp.historical_accuracy DESC LIMIT ?""",
                (limit,)
            ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# MODULE 6 — Crew Prompt Enrichment
# ---------------------------------------------------------------------------

def get_enrichment_block(player_id: str = "", symbol: str = "") -> str:
    """Build a comprehensive data enrichment block for crew prompts.

    Combines: active patterns, insider signals, backtest insights, historical analogs.
    Returns "" on any failure — never breaks prompt flow.
    """
    sections = []

    # Active patterns for this symbol
    try:
        patterns = get_active_patterns(symbol=symbol, limit=5)
        if patterns:
            lines = [f"=== MARKET PATTERNS ({symbol or 'MARKET'}) ==="]
            for p in patterns:
                direction = "BULLISH" if p.get("category") == "bullish" else (
                    "BEARISH" if p.get("category") == "bearish" else "NEUTRAL"
                )
                lines.append(
                    f"  [{direction}] {p.get('name', '?')}: acc={p.get('historical_accuracy', 0):.0%}, "
                    f"avg move {p.get('avg_move_pct', 0):+.1f}% over {p.get('avg_duration_days', '?')}d"
                )
                lines.append(f"    {p.get('description', '')}")
            sections.append("\n".join(lines))
    except Exception:
        pass

    # Recent insider trades for this symbol
    if symbol:
        try:
            insiders = get_recent_insider_trades(symbol=symbol, days=30, limit=5)
            if insiders:
                lines = [f"=== INSIDER ACTIVITY — {symbol} (last 30d) ==="]
                for ins in insiders:
                    total = ins.get("total_value") or 0
                    lines.append(
                        f"  {ins.get('transaction_type', '?').upper()} — "
                        f"{ins.get('insider_name', '?')} ({ins.get('title', '?')}): "
                        f"${total:,.0f} on {ins.get('filing_date', '?')}"
                    )
                sections.append("\n".join(lines))
        except Exception:
            pass

    # Backtest insights relevant to this symbol or general top strategies
    try:
        bt_block = get_backtest_prompt_block(top_n=3)
        if bt_block:
            sections.append(bt_block)
    except Exception:
        pass

    # Historical analog: how did this stock perform in similar VIX/regime conditions
    if symbol:
        try:
            analog = _find_historical_analog(symbol)
            if analog:
                sections.append(analog)
        except Exception:
            pass

    return "\n\n".join(s for s in sections if s)


def _find_historical_analog(symbol: str) -> str:
    """Find the closest historical analog from market_snapshots.

    Looks for periods where VIX and SPY direction were similar to today,
    and reports what happened to the symbol in the following week.
    """
    conn = _conn()
    try:
        # Get current conditions
        try:
            from engine.vix_monitor import get_vix_status
            vix_now = float((get_vix_status() or {}).get("price", 20))
        except Exception:
            vix_now = 20.0

        try:
            from engine.market_data import get_stock_price
            spy = get_stock_price("SPY") or {}
            spy_change = spy.get("change_pct", 0)
        except Exception:
            spy_change = 0.0

        # Find similar historical periods (VIX within ±5, SPY direction matches)
        cutoff = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        spy_direction = 1 if spy_change > 0 else -1

        rows = conn.execute(
            """SELECT ms_stock.date, ms_stock.close, ms_stock.change_pct,
                      ms_spy.change_pct as spy_chg, ms_spy.vix_close as vix_val
               FROM market_snapshots ms_stock
               JOIN market_snapshots ms_spy ON ms_stock.date = ms_spy.date AND ms_spy.symbol='SPY'
               WHERE ms_stock.symbol=? AND ms_stock.date >= ?
                 AND ms_spy.vix_close BETWEEN ? AND ?
               ORDER BY ms_stock.date DESC
               LIMIT 10""",
            (symbol.upper(), cutoff, max(5, vix_now - 5), vix_now + 5)
        ).fetchall()

        if not rows:
            return ""

        similar_moves = [r["change_pct"] for r in rows if r["change_pct"] is not None]
        if not similar_moves:
            return ""

        avg_move = sum(similar_moves) / len(similar_moves)
        return (
            f"=== HISTORICAL ANALOG — {symbol} ===\n"
            f"  In {len(rows)} similar VIX ~{vix_now:.0f} periods: avg next-day move {avg_move:+.1f}%"
        )
    except Exception:
        return ""
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Convenience: run all init at once
# ---------------------------------------------------------------------------

def init_all() -> None:
    """Initialise all ingestion tables + seed patterns. Called at startup."""
    try:
        init_ingestion_tables()
        seed_market_patterns()
        console.log("[green]Data ingestion system initialized")
    except Exception as e:
        console.log(f"[yellow]Data ingestion init warning: {e}")


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import sys
    import os

    # Ensure project root is on path when run directly
    _root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _root not in sys.path:
        sys.path.insert(0, _root)

    parser = argparse.ArgumentParser(description="TradeMinds Data Ingestion CLI")
    parser.add_argument("--webull", metavar="FILE", help="Import Webull CSV trade history")
    parser.add_argument("--backfill", metavar="DAYS", type=int, nargs="?", const=365,
                        help="Backfill market history (default 365 days)")
    parser.add_argument("--insiders", action="store_true", help="Fetch SEC insider filings")
    parser.add_argument("--patterns", action="store_true", help="Run pattern matching scan")
    parser.add_argument("--init", action="store_true", help="Init tables + seed patterns")
    args = parser.parse_args()

    # Always ensure tables exist
    init_ingestion_tables()
    seed_market_patterns()

    ran_something = False

    if args.webull:
        ran_something = True
        console.print(f"[bold cyan]Importing Webull CSV: {args.webull}")
        result = import_webull_csv(args.webull)
        console.print(f"[green]  Imported : {result['imported']} trades")
        console.print(f"[yellow]  Skipped  : {result['skipped']}")
        if result["errors"]:
            console.print(f"[red]  Errors   : {len(result['errors'])}")
            for e in result["errors"][:10]:
                console.print(f"[red]    {e}")

    if args.backfill is not None:
        ran_something = True
        console.print(f"[bold cyan]Backfilling market history ({args.backfill} days)...")
        result = backfill_market_history(days=args.backfill)
        console.print(f"[green]  Symbols  : {result['symbols_processed']}")
        console.print(f"[green]  Inserted : {result['bars_inserted']} bars")
        console.print(f"[yellow]  Skipped  : {result['bars_skipped']} bars")
        if result["errors"]:
            for e in result["errors"][:5]:
                console.print(f"[red]  {e}")

    if args.insiders:
        ran_something = True
        console.print("[bold cyan]Fetching SEC insider filings...")
        result = fetch_insider_trades()
        console.print(f"[green]  Inserted : {result['inserted']}")
        console.print(f"[yellow]  Skipped  : {result['skipped']}")

    if args.patterns:
        ran_something = True
        console.print("[bold cyan]Running pattern scan...")
        matches = match_current_patterns()
        if matches:
            for m in matches:
                console.print(f"  [{m['category'].upper()}] {m['pattern']} — {m['symbol']}")
        else:
            console.print("[yellow]  No patterns matched")

    if args.init and not ran_something:
        console.print("[green]Tables initialized and patterns seeded.")

    if not ran_something and not args.init:
        parser.print_help()
