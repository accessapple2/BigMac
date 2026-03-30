"""Earnings Hub — countdown cards, estimates, and guidance for upcoming earnings."""
from __future__ import annotations
import json
from pathlib import Path
from datetime import datetime, timedelta
from rich.console import Console

import config

console = Console()


def get_earnings_countdown(days_ahead: int = 7) -> list:
    """Get earnings countdown for watchlist stocks reporting within N days."""
    # Try Finnhub first (more reliable)
    earnings = []
    try:
        from engine.finnhub_data import get_earnings_calendar
        earnings = get_earnings_calendar(
            from_date=datetime.now().strftime("%Y-%m-%d"),
            to_date=(datetime.now() + timedelta(days=days_ahead)).strftime("%Y-%m-%d"),
        )
    except Exception:
        pass

    # Enrich with Alpha Vantage earnings surprises (past 4Q)
    results = []
    for e in earnings:
        sym = e["symbol"]
        days_until = (datetime.strptime(e["date"], "%Y-%m-%d") - datetime.now()).days

        entry = {
            "symbol": sym,
            "date": e["date"],
            "days_until": max(0, days_until),
            "hour": e.get("hour", ""),  # bmo/amc
            "timing": (
                "Before Market Open" if e.get("hour") == "bmo"
                else "After Market Close" if e.get("hour") == "amc"
                else "TBD"
            ),
            "eps_estimate": e.get("eps_estimate"),
            "revenue_estimate": e.get("revenue_estimate"),
        }

        # Add past earnings history
        try:
            from engine.alphavantage_data import get_earnings_surprises
            surprises = get_earnings_surprises(sym)
            if surprises:
                beats = sum(1 for s in surprises if s.get("beat"))
                entry["past_beats"] = beats
                entry["past_quarters"] = len(surprises)
                entry["last_surprise_pct"] = surprises[0].get("surprise_pct")
        except Exception:
            pass

        # Get current price
        try:
            from engine.market_data import get_stock_price
            price = get_stock_price(sym)
            if "error" not in price:
                entry["current_price"] = price["price"]
                entry["change_pct"] = price.get("change_pct", 0)
        except Exception:
            pass

        results.append(entry)

    results.sort(key=lambda x: x["days_until"])
    return results


def build_ai_context(symbol: str) -> str:
    """Build earnings context for AI model prompts."""
    countdown = get_earnings_countdown(days_ahead=14)
    for e in countdown:
        if e["symbol"] == symbol.upper():
            parts = [f"{symbol} earnings in {e['days_until']} days ({e['date']} {e['timing']})"]
            if e.get("eps_estimate"):
                parts.append(f"EPS estimate: ${e['eps_estimate']}")
            if e.get("past_beats") is not None:
                parts.append(f"Beat {e['past_beats']}/{e['past_quarters']} recent quarters")
            if e.get("last_surprise_pct") is not None:
                parts.append(f"Last surprise: {e['last_surprise_pct']}%")
            return " | ".join(parts)
    return ""
