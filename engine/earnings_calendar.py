"""Earnings calendar — check which watchlist stocks have earnings in the next 7 days."""
from __future__ import annotations
import json
import os
from datetime import datetime, timedelta
from rich.console import Console

console = Console()

CACHE_FILE = "data/earnings_cache.json"
_cache = {}
_last_fetch = None


def fetch_earnings(symbols: list) -> list:
    """Fetch earnings dates for symbols using Yahoo Finance direct HTTP.
    Returns list of {symbol, date, days_until, eps_estimate} sorted by date.
    Caches results for 6 hours to avoid repeated API calls.
    """
    global _cache, _last_fetch

    # Check cache freshness (6 hour TTL)
    now = datetime.now()
    if _last_fetch and (now - _last_fetch).total_seconds() < 21600 and _cache:
        return _get_upcoming(_cache)

    # Try loading from disk cache first
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r") as f:
                disk = json.load(f)
            cached_at = datetime.fromisoformat(disk.get("cached_at", "2000-01-01"))
            if (now - cached_at).total_seconds() < 21600:
                _cache = disk.get("data", {})
                _last_fetch = cached_at
                return _get_upcoming(_cache)
        except Exception:
            pass

    # Fetch fresh data from Yahoo Finance direct HTTP (crumb auth)
    from engine.market_data import yahoo_quote_summary
    earnings_data = {}
    for sym in symbols:
        try:
            summary = yahoo_quote_summary(sym, modules="calendarEvents")
            if not summary:
                continue
            cal = summary.get("calendarEvents", {}).get("earnings", {})
            dates = cal.get("earningsDate", [])
            if dates:
                raw_date = dates[0].get("raw") or dates[0].get("fmt")
                if raw_date:
                    if isinstance(raw_date, int):
                        date_str = datetime.fromtimestamp(raw_date).strftime("%Y-%m-%d")
                    else:
                        date_str = str(raw_date)[:10]

                    eps = None
                    avg = cal.get("earningsAverage", {})
                    if avg and avg.get("raw") is not None:
                        eps = round(float(avg["raw"]), 2)

                    earnings_data[sym] = {
                        "date": date_str,
                        "eps_estimate": eps,
                    }
        except Exception as e:
            console.log(f"[dim]Earnings fetch skip {sym}: {e}")

    _cache = earnings_data
    _last_fetch = now

    # Save to disk cache
    try:
        os.makedirs("data", exist_ok=True)
        with open(CACHE_FILE, "w") as f:
            json.dump({"cached_at": now.isoformat(), "data": earnings_data}, f)
    except Exception:
        pass

    return _get_upcoming(earnings_data)


def _get_upcoming(data: dict, days: int = 7) -> list:
    """Filter earnings within next N days and sort by date."""
    today = datetime.now().date()
    cutoff = today + timedelta(days=days)
    result = []
    for sym, info in data.items():
        try:
            earn_date = datetime.strptime(info["date"], "%Y-%m-%d").date()
            days_until = (earn_date - today).days
            if -1 <= days_until <= days:  # include yesterday (reported) through next week
                result.append({
                    "symbol": sym,
                    "date": info["date"],
                    "days_until": days_until,
                    "eps_estimate": info.get("eps_estimate"),
                })
        except (ValueError, KeyError):
            continue
    result.sort(key=lambda x: x["days_until"])
    return result


def get_earnings_warnings(symbols: list) -> list:
    """Get earnings within next 7 days for dashboard banner."""
    return fetch_earnings(symbols)
