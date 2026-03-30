"""Finviz sector performance — fast primary source for broad sector % changes.

Uses finvizfinance library to pull sector-level performance in one request
instead of fetching 11 ETFs individually from Yahoo Finance.
Falls back to empty dict on any error so caller can use Yahoo as backup.
"""
from __future__ import annotations
import time
import threading

# Finviz sector name → our display names
_NAME_MAP = {
    "Basic Materials":        "Materials",
    "Communication Services": "Communication",
    "Consumer Cyclical":      "Consumer Disc",
    "Consumer Defensive":     "Consumer Staples",
    "Energy":                 "Energy",
    "Financial":              "Financials",
    "Healthcare":             "Healthcare",
    "Industrials":            "Industrials",
    "Real Estate":            "Real Estate",
    "Technology":             "Technology",
    "Utilities":              "Utilities",
}

_cache: dict = {"data": None, "ts": 0}
_lock = threading.Lock()
CACHE_TTL = 300  # 5 minutes


def get_finviz_sector_performance() -> dict[str, float]:
    """Return {our_sector_name: change_pct_float} from Finviz.
    change_pct values are already in percent (e.g. 2.23 means +2.23%).
    Returns {} on error — caller should fall back to Yahoo ETF prices."""
    now = time.time()
    if _cache["data"] is not None and now - _cache["ts"] < CACHE_TTL:
        return _cache["data"]

    with _lock:
        # Double-check inside lock
        if _cache["data"] is not None and time.time() - _cache["ts"] < CACHE_TTL:
            return _cache["data"]
        try:
            import concurrent.futures
            def _fetch():
                from finvizfinance.group.overview import Overview
                ov = Overview()
                return ov.screener_view(group="Sector")

            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as _ex:
                future = _ex.submit(_fetch)
                try:
                    df = future.result(timeout=8)  # 8s hard timeout on Finviz
                except concurrent.futures.TimeoutError:
                    return _cache["data"] or {}

            result: dict[str, float] = {}
            for _, row in df.iterrows():
                name = str(row.get("Name", "")).strip()
                change = row.get("Change", 0)
                mapped = _NAME_MAP.get(name)
                if mapped:
                    try:
                        # finvizfinance returns decimal (0.0223 = 2.23%)
                        result[mapped] = round(float(change) * 100, 2)
                    except (TypeError, ValueError):
                        pass
            _cache["data"] = result
            _cache["ts"] = time.time()
            return result
        except Exception:
            return _cache["data"] or {}
