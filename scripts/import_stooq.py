#!/usr/bin/env python3
"""Import Stooq bulk US daily OHLCV data into TradeMinds.

Downloads and parses the d_us_txt.zip daily US equities file from Stooq,
then loads it into a local SQLite table for backtesting with Strategy Lab
and VectorBT Holodeck.

Usage:
    python3 scripts/import_stooq.py                    # download + import
    python3 scripts/import_stooq.py --file data.zip    # import from local file
    python3 scripts/import_stooq.py --symbols SPY,NVDA # import only specific tickers
"""
from __future__ import annotations
import os
import sys
import csv
import sqlite3
import zipfile
import argparse
import time
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB = "data/trader.db"
STOOQ_URL = "https://stooq.com/db/d/?b=d_us_txt"
DOWNLOAD_DIR = Path("data/stooq")


def ensure_table(conn: sqlite3.Connection):
    """Create the historical_prices table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS historical_prices (
            symbol TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            source TEXT DEFAULT 'stooq',
            UNIQUE(symbol, date)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_hp_symbol ON historical_prices(symbol)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_hp_date ON historical_prices(date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_hp_sym_date ON historical_prices(symbol, date)")
    conn.commit()


def download_stooq(dest: Path) -> Path:
    """Download the Stooq daily US equities zip file."""
    import requests

    dest.parent.mkdir(parents=True, exist_ok=True)
    zip_path = dest / "d_us_txt.zip"

    if zip_path.exists():
        age_hours = (time.time() - zip_path.stat().st_mtime) / 3600
        if age_hours < 24:
            print(f"Using cached download ({age_hours:.1f}h old)")
            return zip_path

    print(f"Downloading Stooq US daily data from {STOOQ_URL}...")
    print("This is a ~200MB file, may take a few minutes...")

    r = requests.get(STOOQ_URL, stream=True, timeout=300)
    r.raise_for_status()
    total = int(r.headers.get("content-length", 0))
    downloaded = 0

    with open(zip_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
            downloaded += len(chunk)
            if total > 0:
                pct = downloaded / total * 100
                print(f"\r  {downloaded / 1024 / 1024:.1f} MB / {total / 1024 / 1024:.1f} MB ({pct:.0f}%)", end="", flush=True)
    print(f"\nDownloaded to {zip_path}")
    return zip_path


def parse_stooq_csv(filepath: str, symbols_filter: set | None = None):
    """Parse a single Stooq CSV file. Yields (symbol, date, o, h, l, c, vol)."""
    try:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                return

            # Normalize headers
            header = [h.strip().upper().replace("<", "").replace(">", "") for h in header]

            # Find column indices
            col_map = {}
            for i, h in enumerate(header):
                if h in ("TICKER", "SYMBOL"):
                    col_map["ticker"] = i
                elif h in ("DATE", "DTYYYYMMDD"):
                    col_map["date"] = i
                elif h == "OPEN":
                    col_map["open"] = i
                elif h == "HIGH":
                    col_map["high"] = i
                elif h == "LOW":
                    col_map["low"] = i
                elif h == "CLOSE":
                    col_map["close"] = i
                elif h in ("VOL", "VOLUME"):
                    col_map["volume"] = i

            if "ticker" not in col_map or "close" not in col_map:
                return

            for row in reader:
                try:
                    symbol = row[col_map["ticker"]].strip().upper()
                    # Clean up Stooq symbol format (remove .US suffix)
                    if symbol.endswith(".US"):
                        symbol = symbol[:-3]

                    if symbols_filter and symbol not in symbols_filter:
                        continue

                    # Parse date (YYYYMMDD format)
                    raw_date = row[col_map["date"]].strip()
                    if len(raw_date) == 8:
                        date_str = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
                    else:
                        date_str = raw_date

                    o = float(row[col_map.get("open", col_map["close"])]) if col_map.get("open") else 0
                    h = float(row[col_map.get("high", col_map["close"])]) if col_map.get("high") else 0
                    l = float(row[col_map.get("low", col_map["close"])]) if col_map.get("low") else 0
                    c = float(row[col_map["close"]])
                    v = int(float(row[col_map.get("volume", 0)])) if col_map.get("volume") and row[col_map["volume"]].strip() else 0

                    if c > 0:
                        yield (symbol, date_str, o, h, l, c, v)
                except (ValueError, IndexError):
                    continue
    except Exception as e:
        print(f"  Error parsing {filepath}: {e}")


def import_from_zip(zip_path: Path, conn: sqlite3.Connection, symbols_filter: set | None = None):
    """Extract and import all CSV files from the Stooq zip."""
    total_rows = 0
    total_files = 0
    batch = []
    batch_size = 10000

    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_files = [f for f in zf.namelist() if f.endswith(".txt") or f.endswith(".csv")]
        print(f"Found {len(csv_files)} data files in archive")

        for i, name in enumerate(csv_files):
            zf.extract(name, DOWNLOAD_DIR / "extracted")
            extracted_path = DOWNLOAD_DIR / "extracted" / name

            for row in parse_stooq_csv(str(extracted_path), symbols_filter):
                batch.append(row)
                if len(batch) >= batch_size:
                    conn.executemany(
                        "INSERT OR IGNORE INTO historical_prices (symbol, date, open, high, low, close, volume) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        batch,
                    )
                    total_rows += len(batch)
                    batch = []

            total_files += 1
            if total_files % 100 == 0:
                conn.commit()
                print(f"\r  Processed {total_files}/{len(csv_files)} files, {total_rows:,} rows...", end="", flush=True)

            # Clean up extracted file
            try:
                os.unlink(extracted_path)
            except Exception:
                pass

    # Final batch
    if batch:
        conn.executemany(
            "INSERT OR IGNORE INTO historical_prices (symbol, date, open, high, low, close, volume) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            batch,
        )
        total_rows += len(batch)

    conn.commit()
    print(f"\nImported {total_rows:,} rows from {total_files} files")
    return total_rows


def show_stats(conn: sqlite3.Connection):
    """Show import statistics."""
    row = conn.execute("SELECT COUNT(*) FROM historical_prices").fetchone()
    print(f"\nTotal rows in historical_prices: {row[0]:,}")

    row = conn.execute("SELECT COUNT(DISTINCT symbol) FROM historical_prices").fetchone()
    print(f"Unique symbols: {row[0]:,}")

    row = conn.execute("SELECT MIN(date), MAX(date) FROM historical_prices").fetchone()
    print(f"Date range: {row[0]} to {row[1]}")

    # Show watchlist coverage
    try:
        from config import WATCH_STOCKS
        for sym in WATCH_STOCKS[:5]:
            r = conn.execute(
                "SELECT COUNT(*), MIN(date), MAX(date) FROM historical_prices WHERE symbol=?",
                (sym,),
            ).fetchone()
            print(f"  {sym}: {r[0]:,} days ({r[1]} to {r[2]})")
    except Exception:
        pass


def main():
    parser = argparse.ArgumentParser(description="Import Stooq bulk US daily OHLCV data")
    parser.add_argument("--file", help="Path to local d_us_txt.zip (skip download)")
    parser.add_argument("--symbols", help="Comma-separated list of symbols to import (default: all)")
    parser.add_argument("--stats", action="store_true", help="Show stats only, don't import")
    args = parser.parse_args()

    conn = sqlite3.connect(DB, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    ensure_table(conn)

    if args.stats:
        show_stats(conn)
        conn.close()
        return

    symbols_filter = None
    if args.symbols:
        symbols_filter = {s.strip().upper() for s in args.symbols.split(",")}
        print(f"Filtering to {len(symbols_filter)} symbols: {', '.join(sorted(symbols_filter)[:10])}")

    if args.file:
        zip_path = Path(args.file)
    else:
        zip_path = download_stooq(DOWNLOAD_DIR)

    if not zip_path.exists():
        print(f"Error: {zip_path} not found")
        sys.exit(1)

    print(f"\nImporting from {zip_path}...")
    start = time.time()
    rows = import_from_zip(zip_path, conn, symbols_filter)
    elapsed = time.time() - start
    print(f"Import completed in {elapsed:.1f} seconds ({rows / elapsed:.0f} rows/sec)")

    show_stats(conn)
    conn.close()


if __name__ == "__main__":
    main()
