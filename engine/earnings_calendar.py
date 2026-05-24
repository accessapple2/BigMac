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


# HM-CHART-DATA-EARNINGS-DATES-POPULATE 2026-05-24 — per-symbol historical
# earnings dates for chart overlay rendering. Returns past 2 years + next
# scheduled earnings as ISO date strings keyed by type. 24h disk cache.

_CHART_EARNINGS_CACHE_FILE = "data/earnings_dates_cache.json"
_CHART_EARNINGS_TTL_HOURS = 24
_chart_earnings_mem: dict = {}     # symbol → {fetched_at, dates}


def get_chart_earnings_dates(symbol: str) -> list[dict]:
    """Return earnings dates for chart overlay rendering.

    Output: list of ``{date: 'YYYY-MM-DD', type: 'past' | 'next'}`` covering
    the trailing ~2 years (up to 8 past quarters) plus the next scheduled
    earnings date if known. Empty list on lookup failure (no exception).

    24h two-tier cache:
      - in-process ``_chart_earnings_mem`` per-symbol
      - disk ``data/earnings_dates_cache.json`` {symbol: {fetched_at, dates}}
    """
    if not symbol:
        return []
    sym = str(symbol).strip().upper()
    if not sym:
        return []

    now = datetime.now()

    # Tier 1: in-memory
    mem = _chart_earnings_mem.get(sym)
    if mem:
        try:
            fetched_at = datetime.fromisoformat(mem.get("fetched_at", "2000-01-01"))
            if (now - fetched_at).total_seconds() < _CHART_EARNINGS_TTL_HOURS * 3600:
                return mem.get("dates", [])
        except Exception:
            pass

    # Tier 2: disk
    disk_entry = None
    if os.path.exists(_CHART_EARNINGS_CACHE_FILE):
        try:
            with open(_CHART_EARNINGS_CACHE_FILE, "r") as f:
                disk_all = json.load(f)
            disk_entry = disk_all.get(sym)
            if disk_entry:
                fetched_at = datetime.fromisoformat(disk_entry.get("fetched_at", "2000-01-01"))
                if (now - fetched_at).total_seconds() < _CHART_EARNINGS_TTL_HOURS * 3600:
                    _chart_earnings_mem[sym] = disk_entry
                    return disk_entry.get("dates", [])
        except Exception:
            disk_entry = None

    # Tier 3: Yahoo direct HTTP via yahoo_quote_summary (already used by
    # stock_fundamentals + the existing fetch_earnings path). Avoids the
    # yfinance lxml dependency required by Ticker.earnings_dates.
    dates: list[dict] = []
    try:
        from engine.market_data import yahoo_quote_summary

        cutoff_past = now - timedelta(days=730)
        past_set: set[str] = set()
        future_set: set[str] = set()

        # 1. Past earnings via earningsHistory module
        summary = yahoo_quote_summary(sym, modules="earningsHistory")
        if summary:
            history = summary.get("earningsHistory", {}).get("history", [])
            for row in history:
                ts = row.get("quarter", {}).get("raw")
                if ts is None:
                    continue
                try:
                    d = datetime.fromtimestamp(int(ts))
                except Exception:
                    continue
                if d >= cutoff_past and d < now:
                    past_set.add(d.strftime("%Y-%m-%d"))

        # 2. Next earnings via calendarEvents module (same path fetch_earnings uses)
        summary2 = yahoo_quote_summary(sym, modules="calendarEvents")
        if summary2:
            cal = summary2.get("calendarEvents", {}).get("earnings", {})
            for entry in cal.get("earningsDate", []):
                raw = entry.get("raw") or entry.get("fmt")
                if raw is None:
                    continue
                try:
                    if isinstance(raw, (int, float)):
                        d = datetime.fromtimestamp(int(raw))
                        ds = d.strftime("%Y-%m-%d")
                    else:
                        ds = str(raw)[:10]
                        d = datetime.strptime(ds, "%Y-%m-%d")
                except Exception:
                    continue
                if d >= now:
                    future_set.add(ds)

        # past = oldest→newest; cap at 8 most recent
        past_list = sorted(past_set)[-8:]
        for ds in past_list:
            dates.append({"date": ds, "type": "past"})
        if future_set:
            next_ds = sorted(future_set)[0]
            dates.append({"date": next_ds, "type": "next"})
    except Exception as e:
        console.log(f"[dim]get_chart_earnings_dates {sym} failed: {type(e).__name__}: {e!r}")
        dates = []

    # Persist to both tiers
    entry = {"fetched_at": now.isoformat(), "dates": dates}
    _chart_earnings_mem[sym] = entry
    try:
        disk_all = {}
        if os.path.exists(_CHART_EARNINGS_CACHE_FILE):
            with open(_CHART_EARNINGS_CACHE_FILE, "r") as f:
                disk_all = json.load(f) or {}
        disk_all[sym] = entry
        os.makedirs(os.path.dirname(_CHART_EARNINGS_CACHE_FILE), exist_ok=True)
        with open(_CHART_EARNINGS_CACHE_FILE, "w") as f:
            json.dump(disk_all, f)
    except Exception:
        pass

    return dates
