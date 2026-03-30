"""
shared/finviz_scanner.py — Finviz Elite scanner shared between both servers.

Works on Python 3.9 (arena/port 8080) and Python 3.12 (crew/port 8000).
No crewai dependency.

Usage:
    from shared.finviz_scanner import scan_finviz, finviz_login
"""
from __future__ import annotations

import json
import logging
import os

log = logging.getLogger("finviz_scanner")

_logged_in = False


def finviz_login() -> bool:
    """Authenticate to Finviz Elite once per process.

    Reads FINVIZ_EMAIL and FINVIZ_PASSWORD from environment.
    Uses the shared requests.Session inside finvizfinance.util so all
    subsequent scrapes automatically carry the Elite session cookie.
    Returns True if login succeeded, False if skipped or failed.
    """
    global _logged_in
    if _logged_in:
        return True
    email = os.environ.get("FINVIZ_EMAIL", "").strip()
    password = os.environ.get("FINVIZ_PASSWORD", "").strip()
    if not email or not password:
        return False
    try:
        from finvizfinance.util import session, headers
        resp = session.post(
            "https://finviz.com/login_submit.ashx",
            data={"email": email, "password": password, "remember": "on"},
            headers=headers,
            timeout=15,
        )
        resp.raise_for_status()
        _logged_in = True
        log.info("Finviz Elite login succeeded")
        return True
    except Exception as e:
        log.warning(f"Finviz Elite login failed (falling back to free tier): {e}")
        return False


# ---------------------------------------------------------------------------
# Individual scan functions — each returns a list[dict]
# ---------------------------------------------------------------------------

def _safe_df_to_records(df, cols: list[str]) -> list[dict]:
    """Select available columns and convert DataFrame to JSON-safe records."""
    import math
    cols = [c for c in cols if c in df.columns]
    records = df[cols].to_dict("records")
    # Replace NaN/inf with None so json.dumps won't choke
    cleaned = []
    for row in records:
        cleaned.append({
            k: (None if isinstance(v, float) and (math.isnan(v) or math.isinf(v)) else v)
            for k, v in row.items()
        })
    return cleaned


def finviz_gainers(limit: int = 20) -> list[dict]:
    """Top % gainers today — USA, price > $1, avg vol > 100K."""
    finviz_login()
    from finvizfinance.screener.overview import Overview
    s = Overview()
    s.set_filter(filters_dict={
        "Country": "USA", "Price": "Over $1", "Average Volume": "Over 100K",
    })
    df = s.screener_view(order="Change", ascend=False, limit=limit)
    return _safe_df_to_records(df, ["Ticker", "Company", "Sector", "Price", "Change", "Volume", "Market Cap"])


def finviz_losers(limit: int = 20) -> list[dict]:
    """Top % losers today — USA, price > $1, avg vol > 100K."""
    finviz_login()
    from finvizfinance.screener.overview import Overview
    s = Overview()
    s.set_filter(filters_dict={
        "Country": "USA", "Price": "Over $1", "Average Volume": "Over 100K",
    })
    df = s.screener_view(order="Change", ascend=True, limit=limit)
    return _safe_df_to_records(df, ["Ticker", "Company", "Sector", "Price", "Change", "Volume", "Market Cap"])


def finviz_unusual_volume(limit: int = 20) -> list[dict]:
    """Stocks trading > 2x their average volume."""
    finviz_login()
    from finvizfinance.screener.overview import Overview
    s = Overview()
    s.set_filter(filters_dict={
        "Country": "USA", "Average Volume": "Over 200K", "Relative Volume": "Over 2",
    })
    df = s.screener_view(order="Relative Volume", ascend=False, limit=limit)
    return _safe_df_to_records(df, ["Ticker", "Company", "Sector", "Price", "Change", "Volume", "Market Cap"])


def finviz_oversold_rsi(limit: int = 20) -> list[dict]:
    """Stocks with RSI(14) below 30 — deeply oversold bounce candidates."""
    finviz_login()
    from finvizfinance.screener.technical import Technical
    s = Technical()
    s.set_filter(filters_dict={
        "Country": "USA", "RSI (14)": "Oversold (30)", "Average Volume": "Over 200K",
    })
    df = s.screener_view(order="RSI", ascend=True, limit=limit)
    return _safe_df_to_records(df, ["Ticker", "RSI", "Price", "Change", "Volume", "SMA20", "SMA50", "SMA200"])


def finviz_golden_cross(limit: int = 20) -> list[dict]:
    """Stocks with price above 20/50/200-day SMAs — trend confirmation."""
    finviz_login()
    from finvizfinance.screener.technical import Technical
    s = Technical()
    s.set_filter(filters_dict={
        "Country": "USA",
        "20-Day Simple Moving Average": "Price above SMA20",
        "50-Day Simple Moving Average": "Price above SMA50",
        "200-Day Simple Moving Average": "Price above SMA200",
        "Average Volume": "Over 200K",
    })
    df = s.screener_view(order="Change", ascend=False, limit=limit)
    return _safe_df_to_records(df, ["Ticker", "Price", "Change", "Volume", "RSI", "SMA20", "SMA50", "SMA200"])


def finviz_insider_buys(limit: int = 25) -> list[dict]:
    """Latest insider purchase transactions."""
    finviz_login()
    from finvizfinance.insider import Insider
    df = Insider(option="latest buys").get_insider()
    if df is None or len(df) == 0:
        return []
    return _safe_df_to_records(df.head(limit), [
        "Ticker", "Owner", "Relationship", "Date", "Transaction",
        "Cost", "#Shares", "Value ($)", "#Shares Total",
    ])


def finviz_sector_heat() -> list[dict]:
    """Sector performance heat map — week/month/YTD change for all 11 sectors."""
    finviz_login()
    from finvizfinance.group.performance import Performance as GPerf
    df = GPerf().screener_view(group="Sector", order="Change")
    return _safe_df_to_records(df, ["Name", "Change", "Perf Week", "Perf Month", "Perf YTD", "Volume", "Rel Volume"])


def finviz_news(limit: int = 30) -> list[dict]:
    """Latest market-moving headlines from Finviz."""
    finviz_login()
    from finvizfinance.news import News
    data = News().get_news()
    df = data["news"].head(limit)
    return _safe_df_to_records(df, ["Date", "Title", "Source"])


def finviz_earnings_today(limit: int = 20) -> list[dict]:
    """Stocks reporting earnings today with high volume."""
    finviz_login()
    from finvizfinance.screener.overview import Overview
    s = Overview()
    s.set_filter(filters_dict={
        "Country": "USA", "Earnings Date": "Today", "Average Volume": "Over 200K",
    })
    df = s.screener_view(order="Change", ascend=False, limit=limit)
    return _safe_df_to_records(df, ["Ticker", "Company", "Sector", "Price", "Change", "Volume", "Market Cap"])


# ---------------------------------------------------------------------------
# Dispatch function (used by CrewAI @tool wrapper in crew/agents.py)
# ---------------------------------------------------------------------------

_SCAN_MAP = {
    "gainers": finviz_gainers,
    "losers": finviz_losers,
    "unusual_volume": finviz_unusual_volume,
    "oversold_rsi": finviz_oversold_rsi,
    "golden_cross": finviz_golden_cross,
    "insider_buys": finviz_insider_buys,
    "sector_heat": finviz_sector_heat,
    "news": finviz_news,
    "earnings_today": finviz_earnings_today,
}

VALID_SCAN_TYPES = list(_SCAN_MAP.keys())


def scan_finviz(scan_type: str) -> str:
    """Run a named Finviz scan and return a JSON string result.

    scan_type must be one of: gainers, losers, unusual_volume, oversold_rsi,
    golden_cross, insider_buys, sector_heat, news, earnings_today.
    """
    st = scan_type.strip().lower()
    fn = _SCAN_MAP.get(st)
    if fn is None:
        return json.dumps({
            "error": f"Unknown scan_type '{scan_type}'.",
            "valid_options": VALID_SCAN_TYPES,
        })
    try:
        data = fn()
        return json.dumps({"scan": st, "count": len(data), "data": data}, default=str)
    except Exception as e:
        log.warning(f"Finviz scan '{scan_type}' error: {e}")
        return json.dumps({"error": str(e), "scan_type": st})


# ---------------------------------------------------------------------------
# Discovery helper — returns discoveries pre-formatted for discovery_scanner.py
# ---------------------------------------------------------------------------

def get_finviz_discoveries() -> list[dict]:
    """Pull Finviz gainers, unusual volume, and insider buys and return them
    as a list of discovery dicts compatible with discovery_scanner.record_discoveries().

    Each dict has: symbol, trigger_type, price, change_pct, volume,
                   rel_volume, short_float, triggers, score, name.
    """
    finviz_login()
    discoveries: list[dict] = []
    seen: set[str] = set()

    # 1. Top gainers — large moves surface momentum / catalyst plays
    try:
        for row in finviz_gainers(limit=20):
            sym = row.get("Ticker", "")
            if not sym or sym in seen:
                continue
            change = float(row.get("Change") or 0)
            if abs(change) < 3.0:
                continue  # Same threshold as existing scanner
            seen.add(sym)
            direction = "gapping_up" if change > 0 else "gapping_down"
            trigger = f"{direction} ({change:+.1f}%) [finviz]"
            discoveries.append({
                "symbol": sym,
                "name": row.get("Company", ""),
                "price": float(row.get("Price") or 0),
                "change_pct": round(change, 2),
                "volume": float(row.get("Volume") or 0),
                "rel_volume": 0.0,
                "short_float": 0.0,
                "trigger_type": "finviz_gainer",
                "triggers": [trigger],
                "score": abs(change) * 5 + 25,
                "source": "finviz",
            })
    except Exception as e:
        log.warning(f"finviz_gainers error: {e}")

    # 2. Unusual volume — stocks trading > 2x average
    try:
        for row in finviz_unusual_volume(limit=20):
            sym = row.get("Ticker", "")
            if not sym or sym in seen:
                continue
            seen.add(sym)
            change = float(row.get("Change") or 0)
            vol = float(row.get("Volume") or 0)
            trigger = f"unusual_volume [finviz], change {change:+.1f}%"
            discoveries.append({
                "symbol": sym,
                "name": row.get("Company", ""),
                "price": float(row.get("Price") or 0),
                "change_pct": round(change, 2),
                "volume": vol,
                "rel_volume": 2.0,  # screener filter guarantees >= 2x
                "short_float": 0.0,
                "trigger_type": "unusual_volume",
                "triggers": [trigger],
                "score": 25 + abs(change) * 5 + 20,  # vol bonus
                "source": "finviz",
            })
    except Exception as e:
        log.warning(f"finviz_unusual_volume error: {e}")

    # 3. Insider buys — smart money signal
    try:
        seen_insider: set[str] = set()
        for row in finviz_insider_buys(limit=25):
            sym = row.get("Ticker", "")
            if not sym or sym in seen_insider:
                continue
            seen_insider.add(sym)
            owner = row.get("Owner", "Unknown")
            relationship = row.get("Relationship", "")
            value = row.get("Value ($)", "")
            trigger = f"insider_buy: {owner} ({relationship}) ${value} [finviz]"
            # Add to discoveries only if not already seen from other scans
            if sym not in seen:
                seen.add(sym)
                discoveries.append({
                    "symbol": sym,
                    "name": "",
                    "price": float(row.get("Cost") or 0),
                    "change_pct": 0.0,
                    "volume": 0.0,
                    "rel_volume": 0.0,
                    "short_float": 0.0,
                    "trigger_type": "insider_buy",
                    "triggers": [trigger],
                    "score": 30,
                    "source": "finviz",
                })
            else:
                # Enrich an existing discovery with insider signal
                for d in discoveries:
                    if d["symbol"] == sym:
                        d["triggers"].append(trigger)
                        d["score"] += 30
                        break
    except Exception as e:
        log.warning(f"finviz_insider_buys error: {e}")

    return discoveries


# ---------------------------------------------------------------------------
# Quality screen — Dalio/Buffett fundamental filter
# ---------------------------------------------------------------------------

_quality_cache: dict = {"tickers": [], "updated": 0.0}
_QUALITY_TTL = 4 * 3600  # 4-hour cache — fundamentals don't move intraday


def finviz_quality_screen() -> list[str]:
    """Return tickers passing the Dalio/Buffett quality screen.

    Finviz filters applied:
        fa_grossmargin_high   — Gross Margin > 50%
        fa_ltdebteq_u0.4     — LT Debt/Equity < 0.4
        fa_opermargin_high    — Operating Margin > 25%
        fa_roe_o15            — Return on Equity > 15%

    Results are cached for 4 hours. Returns a list of ticker strings.
    """
    import time
    global _quality_cache

    now = time.time()
    if now - _quality_cache["updated"] < _QUALITY_TTL and _quality_cache["tickers"]:
        return _quality_cache["tickers"]

    finviz_login()
    try:
        from finvizfinance.screener.overview import Overview
        s = Overview()
        s.set_filter(filters_dict={
            "Gross Margin":     "High (>50%)",
            "LT Debt/Equity":   "Under 0.4",
            "Operating Margin": "High (>25%)",
            "Return on Equity": "Over +15%",
            "Country":          "USA",
        })
        df = s.screener_view(limit=300)
        tickers: list[str] = []
        if df is not None and len(df) > 0 and "Ticker" in df.columns:
            tickers = [str(t) for t in df["Ticker"].tolist() if t]
        _quality_cache = {"tickers": tickers, "updated": now}
        log.info(f"Quality screen refreshed: {len(tickers)} tickers")
        return tickers
    except Exception as e:
        log.warning(f"finviz_quality_screen error: {e}")
        return _quality_cache.get("tickers", [])  # return stale on error


def is_quality_stock(ticker: str) -> bool | None:
    """Return True/False if ticker is/isn't in the quality screen.

    Returns None if the quality cache is empty (not yet loaded).
    """
    cache = _quality_cache.get("tickers")
    if cache is None:
        return None
    return ticker.upper() in {t.upper() for t in cache}
