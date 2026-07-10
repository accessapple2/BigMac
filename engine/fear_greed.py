"""Fear & Greed Index — custom composite sentiment gauge from free data.

Combines VIX, SPY RSI, sector breadth, safe haven demand, and momentum.
"""
from __future__ import annotations
import math
import time
import threading
import concurrent.futures
from rich.console import Console

console = Console()

_cache = {"data": None, "ts": 0}
_lock = threading.Lock()
_TTL = 600  # 10 minutes
_TIMEOUT = 15  # seconds for the entire computation

_FALLBACK = {"score": None, "label": "Unavailable", "error": "data source timeout", "signals": {}}

# HM-BUG-BATCH-2026-07-09: single source of truth for score->label
# classification. Every consumer (v2 gauge, /classic sections, CTO advisory
# text, Troi's read) should call classify_fear_greed() instead of
# re-deriving its own boundaries -- found 7+ independently hardcoded
# ladders disagreeing on the greed/extreme-greed cutoff (75 vs 80) and
# other boundaries, so the same score (76) rendered as "EXTREME GREED",
# "GREED", and "neutral/greed" in three different places. Bounds are
# upper-exclusive (score < bound picks that label).
FEAR_GREED_THRESHOLDS = (
    (15, "EXTREME FEAR"),
    (35, "FEAR"),
    (50, "MILD FEAR"),
    (65, "NEUTRAL"),
    (80, "GREED"),
    (None, "EXTREME GREED"),
)


def classify_fear_greed(score: float | None) -> str:
    """Score (0-100) -> canonical label per FEAR_GREED_THRESHOLDS."""
    if score is None or not math.isfinite(score):
        return "NEUTRAL"
    for bound, label in FEAR_GREED_THRESHOLDS:
        if bound is None or score < bound:
            return label
    return "EXTREME GREED"


def _sanitize(obj):
    """Recursively replace nan/inf with None so JSON serialization never fails."""
    if isinstance(obj, float):
        return None if not math.isfinite(obj) else obj
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    return obj


def _safe_close(df, col="Close"):
    """Extract a clean Series from yfinance data, handling MultiIndex columns.

    yfinance >= 0.2.31 returns MultiIndex columns like ('Close', 'SPY').
    This helper normalizes to a flat Series regardless of yfinance version.
    """
    if df is None or df.empty:
        return None
    close = df[col]
    # If MultiIndex produced a DataFrame with one column, squeeze to Series
    if hasattr(close, "columns"):
        close = close.iloc[:, 0] if len(close.columns) == 1 else close
    # If it's still a DataFrame (multi-ticker), take first column
    if hasattr(close, "columns"):
        close = close.iloc[:, 0]
    return close.dropna()


def _compute_fear_greed() -> dict:
    """Inner computation — runs in a thread with timeout protection."""
    # 2026-04-27: migrated VIX to get_vix() (direct Yahoo HTTP, 5min cache, separate rate bucket)
    #              migrated sector ETF breadth to get_alpaca_bars() (Alpaca, no Yahoo rate limit)
    from engine.market_data import get_alpaca_bars, get_vix
    import pandas as pd
    from strategies.polygon_client import fetch_daily_bars

    signals = {}
    score = 50.0

    # Pre-fetch SPY bars once (252d covers RSI last-30, safe-haven last-30, momentum SMA-125)
    _spy_bars: list[dict] = []
    try:
        _spy_bars = fetch_daily_bars("SPY", days=252)
    except Exception:
        pass

    # 1. VIX — migrated 2026-04-27 from yf.download to get_vix() (direct HTTP, cached, safer)
    try:
        vix_val = get_vix()
        if vix_val and vix_val > 0:
            vix_score = max(0, min(100, 100 - (vix_val - 12) * 3))
            signals["vix"] = {
                "value": round(vix_val, 1),
                "signal": "EXTREME_FEAR" if vix_val > 30 else "FEAR" if vix_val > 20 else "NEUTRAL" if vix_val > 15 else "GREED",
                "score": round(vix_score),
            }
            score += (vix_score - 50) * 0.25
    except Exception:
        pass

    # 2. SPY RSI — Polygon aggregates (reuses _spy_bars pre-fetched above)
    try:
        spy30 = _spy_bars[-30:] if len(_spy_bars) >= 30 else _spy_bars
        if len(spy30) >= 15:
            close = pd.Series([b["close"] for b in spy30])
            delta = close.diff()
            gain = delta.where(delta > 0, 0).rolling(14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
            rs = gain / loss
            rsi = float((100 - (100 / (1 + rs))).iloc[-1])
            signals["rsi"] = {
                "value": round(rsi, 1),
                "signal": "EXTREME_FEAR" if rsi < 25 else "FEAR" if rsi < 40 else "NEUTRAL" if rsi < 60 else "GREED" if rsi < 75 else "EXTREME_GREED",
                "score": round(rsi),
            }
            score += (rsi - 50) * 0.25
    except Exception:
        pass

    # 3. Sector breadth — migrated 2026-04-27 from yf.download to get_alpaca_bars()
    #    Returns dict[symbol -> DataFrame] instead of multi-index DataFrame.
    try:
        etfs = ["XLK", "XLV", "XLF", "XLE", "XLY", "XLP", "XLI", "XLB", "XLU", "XLRE", "XLC"]
        data = get_alpaca_bars(etfs, timeframe="1Day", days=5)
        up_count = 0
        for etf in etfs:
            try:
                etf_close = None
                # Handle both MultiIndex and flat column layouts
                try:
                    etf_close = _safe_close(data[etf])
                except (KeyError, TypeError):
                    pass
                if etf_close is not None and len(etf_close) >= 2:
                    if float(etf_close.iloc[-1]) > float(etf_close.iloc[-2]):
                        up_count += 1
            except Exception:
                pass
        breadth = up_count / len(etfs) * 100
        signals["breadth"] = {
            "value": round(breadth),
            "signal": "EXTREME_GREED" if breadth > 80 else "GREED" if breadth > 60 else "NEUTRAL" if breadth > 40 else "FEAR",
            "score": round(breadth),
        }
        score += (breadth - 50) * 0.20
    except Exception:
        pass

    # 4. Safe haven demand (Gold vs SPY 30d) — Polygon aggregates
    try:
        gld_bars = fetch_daily_bars("GLD", days=30)
        spy_bars_30 = _spy_bars[-30:] if len(_spy_bars) >= 2 else []
        if len(gld_bars) > 1 and len(spy_bars_30) > 1:
            gold_ret = (float(gld_bars[-1]["close"]) / float(gld_bars[0]["close"]) - 1) * 100
            spy_ret = (float(spy_bars_30[-1]["close"]) / float(spy_bars_30[0]["close"]) - 1) * 100
            haven = gold_ret - spy_ret
            haven_score = max(0, min(100, 50 - haven * 5))
            signals["safe_haven"] = {
                "value": round(haven, 2),
                "signal": "EXTREME_FEAR" if haven > 5 else "FEAR" if haven > 2 else "NEUTRAL" if haven > -2 else "GREED",
                "score": round(haven_score),
            }
            score += (haven_score - 50) * 0.15
    except Exception:
        pass

    # 5. Momentum (SPY vs 125-day SMA) — Polygon aggregates (reuses _spy_bars pre-fetched above)
    try:
        if len(_spy_bars) >= 126:
            close = pd.Series([b["close"] for b in _spy_bars])
            current = float(close.iloc[-1])
            sma125 = float(close.rolling(125).mean().iloc[-1])
            momentum = ((current - sma125) / sma125) * 100
            mom_score = max(0, min(100, 50 + momentum * 5))
            signals["momentum"] = {
                "value": round(momentum, 2),
                "signal": "EXTREME_GREED" if momentum > 10 else "GREED" if momentum > 3 else "NEUTRAL" if momentum > -3 else "FEAR" if momentum > -10 else "EXTREME_FEAR",
                "score": round(mom_score),
            }
            score += (mom_score - 50) * 0.15
    except Exception:
        pass

    final = max(0, min(100, score))

    return _sanitize({
        "score": int(round(final)) if math.isfinite(final) else 50,
        "label": classify_fear_greed(final),  # handles non-finite -> "NEUTRAL" itself
        "signals": signals,
    })


def get_fear_greed_index() -> dict:
    """Custom fear & greed index from free data sources.

    Returns cached data when fresh, otherwise computes with a hard timeout.
    Never raises — always returns a dict the frontend can display.
    """
    try:
        with _lock:
            if _cache["data"] and time.time() - _cache["ts"] < _TTL:
                return _cache["data"]

        pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        try:
            future = pool.submit(_compute_fear_greed)
            result = future.result(timeout=_TIMEOUT)
        finally:
            pool.shutdown(wait=False, cancel_futures=True)

        with _lock:
            _cache["data"] = result
            _cache["ts"] = time.time()

        return result
    except concurrent.futures.TimeoutError:
        console.print("[yellow]Fear & Greed: timed out after {}s[/yellow]".format(_TIMEOUT))
        with _lock:
            if _cache["data"]:
                return {**_cache["data"], "stale": True}
        return _FALLBACK
    except Exception as e:
        console.print("[red]Fear & Greed error: {}[/red]".format(e))
        with _lock:
            if _cache["data"]:
                return {**_cache["data"], "stale": True}
        return _FALLBACK
