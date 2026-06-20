#!/usr/bin/env python3
"""
UHURA Step 1 — News Ingest
Pull historical Alpaca news for the liquid options universe.
Idempotent: re-runs skip already-stored IDs.
"""
import os
import time
import sqlite3
import requests
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

from db import DB_PATH, get_conn, init_db  # noqa: E402

APCA_KEY = os.environ["APCA_API_KEY_ID"]
APCA_SECRET = os.environ["APCA_API_SECRET_KEY"]
HEADERS = {"APCA-API-KEY-ID": APCA_KEY, "APCA-API-SECRET-KEY": APCA_SECRET}
NEWS_URL = "https://data.alpaca.markets/v1beta1/news"

# Research window: 6-month spike (expand by re-setting INGEST_START env var)
INGEST_START = os.environ.get("UHURA_START", "2025-01-01T00:00:00Z")
INGEST_END = os.environ.get("UHURA_END", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))

# Liquidity floor for ticker selection
MIN_AVG_VOLUME = 1_000_000
TICKER_LIMIT = int(os.environ.get("UHURA_TICKERS", "25"))

TRADER_DB = Path(__file__).resolve().parents[2] / "data" / "trader.db"


SPIKE_TICKERS = [
    # Large-cap equity (high news flow, options-eligible)
    "NVDA", "AAPL", "MSFT", "AMZN", "GOOGL", "META", "TSLA", "NFLX",
    "AMD", "MU", "INTC", "AVGO",
    # Financial / macro proxy
    "JPM", "GS", "MS", "BAC",
    # Energy / commodities
    "XOM", "CVX",
    # ETF proxies
    "SPY", "QQQ", "IWM",
    # Options universe from CSP engine
    "COST", "ORCL", "CRM",
]


def get_universe() -> list[str]:
    """Curated large-cap news-rich tickers for spike (overrides scan_universe)."""
    limit = int(os.environ.get("UHURA_TICKERS", str(len(SPIKE_TICKERS))))
    return SPIKE_TICKERS[:limit]


def fetch_news_page(symbols: list[str], start: str, end: str,
                    page_token: str | None = None) -> dict:
    params: dict = {
        "symbols": ",".join(symbols),
        "start": start,
        "end": end,
        "limit": 50,
        "sort": "asc",
        "include_content": "false",
    }
    if page_token:
        params["page_token"] = page_token

    for attempt in range(5):
        r = requests.get(NEWS_URL, headers=HEADERS, params=params, timeout=15)
        if r.status_code == 429:
            wait = 2 ** attempt * 5  # 5, 10, 20, 40, 80s
            print(f"  429 rate limit, waiting {wait}s...")
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError("Exceeded retry limit on 429")


def ingest_batch(tickers: list[str], conn: sqlite3.Connection) -> int:
    """Pull all pages for a batch of tickers. Returns count inserted."""
    inserted = 0
    page_token = None
    batch_label = ",".join(tickers[:3]) + ("..." if len(tickers) > 3 else "")

    while True:
        data = fetch_news_page(tickers, INGEST_START, INGEST_END, page_token)
        articles = data.get("news", [])

        for art in articles:
            news_id = str(art["id"])
            symbols = art.get("symbols", [])
            if not symbols:
                symbols = tickers[:1]
            for sym in symbols:
                if sym not in tickers:
                    continue
                try:
                    conn.execute(
                        """INSERT OR IGNORE INTO uhura_raw_news
                           (id, ticker, headline, source, created_at, updated_at, summary)
                           VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        (
                            f"{news_id}:{sym}",
                            sym,
                            art.get("headline", ""),
                            art.get("source", ""),
                            art.get("created_at", ""),
                            art.get("updated_at", ""),
                            art.get("summary", ""),
                        ),
                    )
                    if conn.execute("SELECT changes()").fetchone()[0]:
                        inserted += 1
                except sqlite3.Error as e:
                    print(f"  DB err {news_id}:{sym}: {e}")

        conn.commit()
        page_token = data.get("next_page_token")
        if not page_token:
            break
        time.sleep(0.5)

    print(f"  [{batch_label}] done, +{inserted} rows")
    return inserted


def main() -> None:
    init_db()
    tickers = get_universe()
    print(f"Universe: {len(tickers)} tickers, window {INGEST_START[:10]} → {INGEST_END[:10]}")

    conn = get_conn()
    total = 0
    # Alpaca news: batch 5 symbols per request to stay under rate limits
    batch_size = 5
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i : i + batch_size]
        total += ingest_batch(batch, conn)
        time.sleep(1.5)

    # Summary
    rows = conn.execute("SELECT COUNT(*) FROM uhura_raw_news").fetchone()[0]
    tickers_covered = conn.execute(
        "SELECT COUNT(DISTINCT ticker) FROM uhura_raw_news"
    ).fetchone()[0]
    conn.close()
    print(f"\nIngest complete: {rows} total rows, {tickers_covered} tickers, +{total} new this run")
    print(f"DB: {DB_PATH}")


if __name__ == "__main__":
    main()
