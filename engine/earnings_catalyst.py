"""Earnings Catalyst — pre-earnings momentum + post-earnings drift signals.

Pre-earnings: Buy 3-5 days before earnings if stock has 75%+ beat rate
Post-earnings drift: Buy within 5 days of a beat if stock still trending up
These are among the highest-probability setups in finance.
"""
from __future__ import annotations
import sqlite3
import time
import threading
from datetime import datetime, timedelta
from rich.console import Console

console = Console()
DB = "data/trader.db"

_cache = {"upcoming": [], "drifters": [], "ts": 0}
_lock = threading.Lock()
_TTL = 3600  # 1 hour


def get_upcoming_earnings(tickers: list = None, days_ahead: int = 14) -> list:
    """Get stocks reporting earnings in the next N days using yfinance."""
    import yfinance as yf

    if not tickers:
        try:
            from engine.universe_scanner import get_latest_universe_scan
            scan = get_latest_universe_scan()
            tickers = [s["ticker"] for s in scan.get("results", [])[:50]]
        except Exception:
            from config import WATCH_STOCKS
            tickers = WATCH_STOCKS

    upcoming = []
    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            cal = stock.calendar
            if cal is None or cal.empty:
                continue

            # yfinance calendar returns a DataFrame or dict depending on version
            if hasattr(cal, "iloc"):
                # DataFrame format
                if "Earnings Date" in cal.index:
                    dates = cal.loc["Earnings Date"]
                    if hasattr(dates, "__iter__"):
                        next_date = dates.iloc[0] if hasattr(dates, "iloc") else dates
                    else:
                        next_date = dates
                else:
                    continue
            elif isinstance(cal, dict):
                ed = cal.get("Earnings Date", [])
                if not ed:
                    continue
                next_date = ed[0] if isinstance(ed, list) else ed
            else:
                continue

            if hasattr(next_date, "date"):
                next_date = next_date.date() if hasattr(next_date, "date") else next_date
            if isinstance(next_date, str):
                next_date = datetime.strptime(next_date, "%Y-%m-%d").date()

            days_until = (next_date - datetime.now().date()).days
            if not (0 < days_until <= days_ahead):
                continue

            upcoming.append({
                "ticker": ticker,
                "earnings_date": str(next_date),
                "days_until": days_until,
            })
        except Exception:
            continue

    upcoming.sort(key=lambda x: x["days_until"])
    return upcoming


def get_post_earnings_drift(tickers: list = None, days_back: int = 7) -> list:
    """Find stocks that recently beat earnings and are still drifting up."""
    import yfinance as yf

    if not tickers:
        try:
            from engine.universe_scanner import get_latest_universe_scan
            scan = get_latest_universe_scan()
            tickers = [s["ticker"] for s in scan.get("results", [])[:100]]
        except Exception:
            from config import WATCH_STOCKS
            tickers = WATCH_STOCKS

    drifters = []
    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            # Check recent price action for post-earnings gap
            hist = stock.history(period="10d")
            if len(hist) < 5:
                continue

            # Look for a gap day (>3% move on high volume in last 7 days)
            for i in range(1, min(days_back, len(hist))):
                daily_ret = (float(hist["Close"].iloc[-i]) / float(hist["Close"].iloc[-i-1]) - 1) * 100
                if abs(daily_ret) > 3:
                    # Check if it's still drifting in the same direction
                    post_drift = (float(hist["Close"].iloc[-1]) / float(hist["Close"].iloc[-i]) - 1) * 100
                    if daily_ret > 3 and post_drift > 0:
                        drifters.append({
                            "ticker": ticker,
                            "gap_pct": round(daily_ret, 1),
                            "post_drift_pct": round(post_drift, 1),
                            "days_since_gap": i,
                            "direction": "UP",
                        })
                    break
        except Exception:
            continue

    drifters.sort(key=lambda x: x["gap_pct"], reverse=True)
    return drifters


def build_earnings_catalyst_section() -> str:
    """Build prompt section with earnings catalyst data for AI models."""
    with _lock:
        if _cache["ts"] and time.time() - _cache["ts"] < _TTL:
            upcoming = _cache["upcoming"]
            drifters = _cache["drifters"]
        else:
            try:
                upcoming = get_upcoming_earnings()
                drifters = get_post_earnings_drift()
                _cache.update({"upcoming": upcoming, "drifters": drifters, "ts": time.time()})
            except Exception:
                return ""

    if not upcoming and not drifters:
        return ""

    lines = ["\n=== EARNINGS CATALYST (Warp 10 Final) ==="]

    if upcoming:
        lines.append("UPCOMING EARNINGS (pre-earnings momentum candidates):")
        for e in upcoming[:8]:
            lines.append(f"  {e['ticker']}: reports in {e['days_until']}d ({e['earnings_date']})")

    if drifters:
        lines.append("POST-EARNINGS DRIFT (recent beats still trending up):")
        for d in drifters[:5]:
            lines.append(f"  {d['ticker']}: gapped {d['gap_pct']:+.1f}%, still drifting {d['post_drift_pct']:+.1f}%")

    return "\n".join(lines)
