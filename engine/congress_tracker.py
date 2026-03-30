"""Congressional Trades Tracker — monitors stock trades by US Congress members.

Uses Finnhub API (already configured) for congressional trading data.
Checks for overlap with our portfolio/watchlist tickers.
"""
from __future__ import annotations
import os
import time
import threading
import requests
from datetime import datetime, timedelta
from collections import Counter
from rich.console import Console

console = Console()

_cache = {"trades": [], "ts": 0}
_lock = threading.Lock()
_TTL = 3600  # 1 hour


def get_congressional_trades() -> dict:
    """Pull recent congressional stock trades — Capitol Trades + Quiver Quant."""
    try:
        from engine.congress_scraper import get_all_congress_trades
        scraped = get_all_congress_trades()

        # Normalize to the format the rest of the codebase expects
        trades = []
        for t in scraped:
            trade = {
                "politician": t.get("politician", "Unknown"),
                "party": t.get("party", ""),
                "chamber": "",
                "ticker": t.get("ticker", ""),
                "transaction": t.get("type", ""),
                "amount_range": t.get("size", ""),
                "transaction_date": t.get("trade_date", ""),
                "filing_date": t.get("filed_date", ""),
                "source": t.get("source", ""),
                "source_url": t.get("source_url", ""),
                "excess_return": t.get("excess_return", ""),
                "days_since_trade": 0,
            }
            trades.append(trade)

        return {"source": "scraper", "trades": trades}

    except Exception as e:
        console.log(f"[red]Congress tracker error: {e}")
        return {"source": "scraper", "trades": [], "error": str(e)}


def get_congress_overlap(our_tickers: list) -> list:
    """Check if any portfolio/watchlist tickers overlap with congressional trades."""
    data = get_congressional_trades()
    congress_tickers = set(t["ticker"] for t in data.get("trades", []) if t.get("ticker"))

    overlaps = []
    for ticker in our_tickers:
        if ticker in congress_tickers:
            matching = [t for t in data["trades"] if t["ticker"] == ticker]
            def _is_buy(t):
                tx = t.get("transaction", "").lower()
                return "purchase" in tx or tx == "buy"
            def _is_sell(t):
                tx = t.get("transaction", "").lower()
                return "sale" in tx or tx == "sell"
            overlaps.append({
                "ticker": ticker,
                "congress_trades": matching[:10],
                "buy_count": sum(1 for t in matching if _is_buy(t)),
                "sell_count": sum(1 for t in matching if _is_sell(t)),
                "total_trades": len(matching),
            })

    overlaps.sort(key=lambda x: x["total_trades"], reverse=True)
    return overlaps


def get_top_congress_buys(days_back: int = 30) -> list:
    """Get the most-bought tickers by Congress in the last N days."""
    data = get_congressional_trades()
    trades = [
        t for t in data.get("trades", [])
        if "purchase" in t.get("transaction", "").lower() or t.get("transaction", "").lower() == "buy"
    ]

    ticker_counts = Counter(t["ticker"] for t in trades if t.get("ticker"))

    top_buys = []
    for ticker, count in ticker_counts.most_common(10):
        politicians = list(set(
            t["politician"] for t in trades if t["ticker"] == ticker
        ))
        top_buys.append({
            "ticker": ticker,
            "buy_count": count,
            "politicians": politicians[:5],
            "signal_strength": (
                "STRONG" if count >= 3 else "MODERATE" if count >= 2 else "WEAK"
            ),
        })

    return top_buys
