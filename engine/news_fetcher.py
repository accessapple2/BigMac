"""Fetch market news from Yahoo Finance RSS (free, no API key needed)."""
from __future__ import annotations
import sqlite3
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from rich.console import Console

console = Console()
DB = "data/trader.db"
YAHOO_RSS = "https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=US&lang=en-US"


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    return c


def fetch_news(symbols: list[str], max_per_symbol: int = 5) -> list[dict]:
    """Fetch latest news for each symbol from Yahoo Finance RSS."""
    all_news = []
    for symbol in symbols:
        try:
            resp = requests.get(
                YAHOO_RSS.format(symbol=symbol),
                timeout=10,
                headers={"User-Agent": "TradeMinds/1.0"}
            )
            if resp.status_code != 200:
                continue
            root = ET.fromstring(resp.content)
            items = root.findall(".//item")[:max_per_symbol]
            for item in items:
                title = item.findtext("title", "")
                link = item.findtext("link", "")
                desc = item.findtext("description", "")
                source = item.findtext("source", "Yahoo Finance")
                pub_date = item.findtext("pubDate", "")
                news_item = {
                    "symbol": symbol,
                    "headline": title,
                    "summary": desc[:500] if desc else "",
                    "source": source,
                    "url": link,
                    "pub_date": pub_date,
                }
                all_news.append(news_item)
        except Exception as e:
            console.log(f"[red]News fetch error for {symbol}: {e}")

    # Save to DB
    if all_news:
        _save_news(all_news)

    return all_news


def _save_news(news_items: list[dict]):
    """Save news to DB, deduplicating by headline."""
    conn = _conn()
    for item in news_items:
        # Skip if headline already exists
        existing = conn.execute(
            "SELECT 1 FROM market_news WHERE headline=? LIMIT 1",
            (item["headline"],)
        ).fetchone()
        if existing:
            continue
        conn.execute(
            "INSERT INTO market_news (symbol, headline, summary, source, url) VALUES (?,?,?,?,?)",
            (item["symbol"], item["headline"], item["summary"], item["source"], item["url"])
        )
    conn.commit()
    conn.close()


def get_recent_news(limit: int = 30) -> list[dict]:
    """Get recent news from DB."""
    conn = _conn()
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM market_news ORDER BY fetched_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_news_for_symbol(symbol: str, limit: int = 10) -> list[dict]:
    """Get news for a specific symbol."""
    conn = _conn()
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM market_news WHERE symbol=? ORDER BY fetched_at DESC LIMIT ?",
        (symbol, limit)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]
