"""Finnhub market data — insider transactions, earnings, news sentiment, SEC filings."""
from __future__ import annotations
import requests
import time
import json
from pathlib import Path
from datetime import datetime, timedelta
from rich.console import Console

import config

console = Console()

_FINNHUB_BASE = "https://finnhub.io/api/v1"
_cache = {}
_CACHE_TTL = 300  # 5 minutes

def _fh_get(endpoint: str, params: dict = None) -> dict | None:
    """Make authenticated Finnhub API call with rate limiting."""
    key = config.FINNHUB_API_KEY
    if not key:
        return None
    if params is None:
        params = {}
    params["token"] = key
    try:
        r = requests.get(f"{_FINNHUB_BASE}{endpoint}", params=params, timeout=10)
        if r.status_code == 429:
            console.log("[yellow]Finnhub rate limited")
            return None
        if r.status_code != 200:
            return None
        return r.json()
    except Exception as e:
        console.log(f"[red]Finnhub error: {e}")
        return None


def get_insider_transactions(symbol: str) -> list:
    """Get insider transactions for a symbol.
    Returns list of {name, share, change, transactionDate, transactionCode, transactionPrice}
    transactionCode: P=Purchase, S=Sale, A=Grant, M=Exercise
    """
    data = _fh_get("/stock/insider-transactions", {"symbol": symbol.upper()})
    if not data or "data" not in data:
        return []
    txns = []
    for t in data["data"][:20]:
        code = t.get("transactionCode", "")
        txn_type = {"P": "Purchase", "S": "Sale", "A": "Grant", "M": "Exercise", "G": "Gift"}.get(code, code)
        shares = t.get("change", 0)
        price = t.get("transactionPrice", 0) or 0
        value = abs(shares * price) if price else 0
        txns.append({
            "name": t.get("name", "Unknown"),
            "title": "",  # Finnhub doesn't always include title
            "shares": abs(shares),
            "value": round(value, 2),
            "transaction_type": txn_type,
            "date": t.get("transactionDate", ""),
            "symbol": symbol.upper(),
            "filing_date": t.get("filingDate", ""),
        })
    return txns


def get_insider_sentiment(symbol: str) -> dict:
    """Get aggregated insider sentiment for a symbol.
    Summarizes recent insider buying/selling as a signal string for AI prompts.
    """
    txns = get_insider_transactions(symbol)
    if not txns:
        return {"symbol": symbol, "signal": "no_data", "summary": f"No insider transaction data for {symbol}"}

    recent = [t for t in txns if t.get("date", "") >= (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")]
    buys = [t for t in recent if t["transaction_type"] == "Purchase"]
    sells = [t for t in recent if t["transaction_type"] == "Sale"]
    buy_value = sum(t["value"] for t in buys)
    sell_value = sum(t["value"] for t in sells)

    if buy_value > sell_value * 2:
        signal = "bullish"
    elif sell_value > buy_value * 2:
        signal = "bearish"
    else:
        signal = "neutral"

    summary = f"{symbol} insider activity (90d): {len(buys)} buys (${buy_value/1000:.0f}k), {len(sells)} sells (${sell_value/1000:.0f}k) — {signal}"
    return {"symbol": symbol, "signal": signal, "summary": summary, "buy_value": buy_value, "sell_value": sell_value, "buy_count": len(buys), "sell_count": len(sells)}


def get_earnings_calendar(from_date: str = None, to_date: str = None) -> list:
    """Get upcoming earnings calendar.
    from_date/to_date format: YYYY-MM-DD. Defaults to next 14 days.
    """
    if not from_date:
        from_date = datetime.now().strftime("%Y-%m-%d")
    if not to_date:
        to_date = (datetime.now() + timedelta(days=14)).strftime("%Y-%m-%d")

    data = _fh_get("/calendar/earnings", {"from": from_date, "to": to_date})
    if not data or "earningsCalendar" not in data:
        return []

    # Filter to watchlist stocks
    watchlist = set(s.upper() for s in config.WATCH_STOCKS)
    results = []
    for e in data["earningsCalendar"]:
        sym = e.get("symbol", "")
        if sym in watchlist:
            results.append({
                "symbol": sym,
                "date": e.get("date", ""),
                "hour": e.get("hour", ""),  # bmo=before market open, amc=after market close
                "eps_estimate": e.get("epsEstimate"),
                "eps_actual": e.get("epsActual"),
                "revenue_estimate": e.get("revenueEstimate"),
                "revenue_actual": e.get("revenueActual"),
                "quarter": e.get("quarter"),
                "year": e.get("year"),
            })

    results.sort(key=lambda x: x["date"])
    return results


def get_news_sentiment(symbol: str) -> dict:
    """Get Finnhub news sentiment for a symbol.
    Returns sentiment score (-1 to +1) and buzz metrics.
    """
    data = _fh_get("/news-sentiment", {"symbol": symbol.upper()})
    if not data:
        return {"symbol": symbol, "sentiment_score": 0, "buzz": 0, "summary": "No data"}

    sentiment = data.get("sentiment", {})
    buzz = data.get("buzz", {})
    score = sentiment.get("score", 0)  # -1 to +1 composite

    if score > 0.3:
        label = "bullish"
    elif score < -0.3:
        label = "bearish"
    else:
        label = "neutral"

    summary = f"{symbol} news sentiment: {label} (score: {score:.2f}), buzz: {buzz.get('buzz', 0):.1f}x, {buzz.get('articlesInLastWeek', 0)} articles this week"
    return {
        "symbol": symbol,
        "sentiment_score": round(score, 3),
        "bullish_pct": round(sentiment.get("bullishPercent", 0) * 100, 1),
        "bearish_pct": round(sentiment.get("bearishPercent", 0) * 100, 1),
        "label": label,
        "buzz": round(buzz.get("buzz", 0), 2),
        "articles_week": buzz.get("articlesInLastWeek", 0),
        "summary": summary,
    }


def get_sec_filings(symbol: str, form_type: str = None) -> list:
    """Get SEC filings for a symbol.
    form_type: '10-K', '10-Q', '8-K', etc. None = all types.
    """
    params = {"symbol": symbol.upper()}
    if form_type:
        params["form"] = form_type
    data = _fh_get("/stock/filings", params)
    if not data or not isinstance(data, list):
        return []

    results = []
    for f in data[:15]:
        results.append({
            "symbol": symbol.upper(),
            "form": f.get("form", ""),
            "filed_date": f.get("filedDate", ""),
            "accepted_date": f.get("acceptedDate", ""),
            "report_url": f.get("reportUrl", ""),
            "filing_url": f.get("filingUrl", ""),
        })
    return results


def get_quote(symbol: str) -> dict | None:
    """Get real-time quote from Finnhub as Yahoo fallback.
    Returns {price, change_pct, high, low, volume} or None.
    """
    data = _fh_get("/quote", {"symbol": symbol.upper()})
    if not data or data.get("c", 0) == 0:
        return None

    price = data["c"]  # current price
    prev_close = data.get("pc", price)
    change_pct = round((price - prev_close) / prev_close * 100, 2) if prev_close else 0

    return {
        "symbol": symbol.upper(),
        "price": round(price, 2),
        "change_pct": change_pct,
        "high": round(data.get("h", price), 2),
        "low": round(data.get("l", price), 2),
        "volume": 0,  # Finnhub quote doesn't include volume
        "timestamp": datetime.now().isoformat(),
        "source": "finnhub",
    }


def build_ai_context(symbol: str) -> str:
    """Build a context string for AI model prompts with Finnhub intelligence."""
    parts = []

    # Insider sentiment
    insider = get_insider_sentiment(symbol)
    if insider.get("signal") != "no_data":
        parts.append(insider["summary"])

    # News sentiment
    news = get_news_sentiment(symbol)
    if news.get("sentiment_score", 0) != 0:
        parts.append(news["summary"])

    # Earnings proximity
    earnings = get_earnings_calendar()
    for e in earnings:
        if e["symbol"] == symbol.upper():
            days_until = (datetime.strptime(e["date"], "%Y-%m-%d") - datetime.now()).days
            if 0 <= days_until <= 14:
                timing = "before market" if e.get("hour") == "bmo" else "after close" if e.get("hour") == "amc" else ""
                est = f", EPS estimate: ${e['eps_estimate']}" if e.get("eps_estimate") else ""
                parts.append(f"{symbol} earnings in {days_until} days ({e['date']} {timing}){est}")

    # Recent SEC filings
    filings = get_sec_filings(symbol)
    for f in filings[:2]:
        filed = f.get("filed_date", "")
        if filed >= (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"):
            parts.append(f"{symbol} filed {f['form']} on {filed}")

    return " | ".join(parts) if parts else ""
