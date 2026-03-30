"""Relative Strength Scanner — rank watchlist stocks vs SPY (0-100 score)."""
from __future__ import annotations
import threading
import time
from engine.market_data import _is_yf_limited, _set_yf_limited
import pandas as pd
import numpy as np
from rich.console import Console

console = Console()

# Thread-safe cache
_cache: dict = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 120  # 2 minutes


def _calc_rsi(series: pd.Series, period: int = 14) -> float:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1]) if not rsi.empty and pd.notna(rsi.iloc[-1]) else 50.0


def compute_strength_score(symbol: str, spy_perf_5d: float = 0.0) -> dict | None:
    """Compute a 0-100 relative strength score for a symbol.

    Components (each 0-25):
    1. Price vs 50/200 MA (0-25)
    2. RSI momentum (0-25)
    3. Volume ratio (0-25)
    4. 5-day performance vs SPY (0-25)
    """
    if _is_yf_limited():
        return None
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="1y", interval="1d")
        if hist.empty or len(hist) < 10:
            return None

        close = hist["Close"]
        volume = hist["Volume"]
        current = float(close.iloc[-1])

        # 1. Price vs MAs (0-25)
        ma_score = 0
        if len(close) >= 50:
            sma50 = float(close.rolling(50).mean().iloc[-1])
            if current > sma50:
                ma_score += 10
                # Bonus for how far above
                pct_above = (current - sma50) / sma50 * 100
                ma_score += min(5, pct_above / 2)
        if len(close) >= 200:
            sma200 = float(close.rolling(200).mean().iloc[-1])
            if current > sma200:
                ma_score += 8
                pct_above = (current - sma200) / sma200 * 100
                ma_score += min(2, pct_above / 5)
        else:
            ma_score += 5  # Partial credit if not enough data for 200
        ma_score = min(25, ma_score)

        # 2. RSI momentum (0-25)
        rsi = _calc_rsi(close)
        # RSI 50-70 is strongest momentum (20-25), 30-50 is moderate (10-20), 70+ is overbought (15-20)
        if 55 <= rsi <= 70:
            rsi_score = 20 + (rsi - 55) / 3  # 20-25
        elif 45 <= rsi < 55:
            rsi_score = 15 + (rsi - 45) / 2  # 15-20
        elif 70 < rsi <= 80:
            rsi_score = 18  # Still strong but overbought risk
        elif rsi > 80:
            rsi_score = 12  # Very overbought
        elif 30 <= rsi < 45:
            rsi_score = 5 + (rsi - 30) / 3  # 5-10
        else:  # < 30
            rsi_score = 3  # Oversold — weak
        rsi_score = min(25, max(0, rsi_score))

        # 3. Volume ratio (0-25)
        avg_vol_20 = float(volume.rolling(20).mean().iloc[-1]) if len(volume) >= 20 else float(volume.mean())
        vol_ratio = float(volume.iloc[-1] / avg_vol_20) if avg_vol_20 > 0 else 1.0
        # Higher volume = more conviction
        if vol_ratio >= 2.0:
            vol_score = 25
        elif vol_ratio >= 1.5:
            vol_score = 20
        elif vol_ratio >= 1.0:
            vol_score = 15
        elif vol_ratio >= 0.7:
            vol_score = 10
        else:
            vol_score = 5

        # 4. 5-day performance vs SPY (0-25)
        if len(close) >= 6:
            perf_5d = (float(close.iloc[-1]) / float(close.iloc[-6]) - 1) * 100
        else:
            perf_5d = 0.0
        relative_perf = perf_5d - spy_perf_5d
        # Scale: +5% relative = 25, 0% = 12.5, -5% = 0
        perf_score = max(0, min(25, 12.5 + relative_perf * 2.5))

        total_score = round(ma_score + rsi_score + vol_score + perf_score, 1)
        total_score = max(0, min(100, total_score))

        # Get SMA values for display
        sma50_val = round(float(close.rolling(50).mean().iloc[-1]), 2) if len(close) >= 50 else None
        sma200_val = round(float(close.rolling(200).mean().iloc[-1]), 2) if len(close) >= 200 else None

        return {
            "symbol": symbol,
            "score": round(total_score, 1),
            "price": round(current, 2),
            "rsi": round(rsi, 1),
            "vol_ratio": round(vol_ratio, 2),
            "perf_5d": round(perf_5d, 2),
            "relative_perf": round(relative_perf, 2),
            "sma50": sma50_val,
            "sma200": sma200_val,
            "above_sma50": current > sma50_val if sma50_val else None,
            "above_sma200": current > sma200_val if sma200_val else None,
            "components": {
                "ma_score": round(ma_score, 1),
                "rsi_score": round(rsi_score, 1),
                "vol_score": round(vol_score, 1),
                "perf_score": round(perf_score, 1),
            },
        }
    except Exception as e:
        err = str(e)
        if "Too Many Requests" in err or "Rate" in err:
            _set_yf_limited()
        console.log(f"[red]Strength scanner error for {symbol}: {e}")
        return None


def scan_relative_strength(symbols: list) -> list:
    """Rank all symbols by relative strength score vs SPY."""
    with _cache_lock:
        if _cache.get("data") and time.time() - _cache.get("ts", 0) < _CACHE_TTL:
            return _cache["data"]

    if _is_yf_limited():
        return []

    # Get SPY's 5-day performance as benchmark
    spy_perf_5d = 0.0
    try:
        spy = yf.Ticker("SPY")
        spy_hist = spy.history(period="10d", interval="1d")
        if len(spy_hist) >= 6:
            spy_perf_5d = (float(spy_hist["Close"].iloc[-1]) / float(spy_hist["Close"].iloc[-6]) - 1) * 100
    except Exception as e:
        err = str(e)
        if "Too Many Requests" in err or "Rate" in err:
            _set_yf_limited()


    results = []
    for sym in symbols:
        data = compute_strength_score(sym, spy_perf_5d)
        if data:
            results.append(data)

    results.sort(key=lambda x: x["score"], reverse=True)

    # Add rank
    for i, r in enumerate(results):
        r["rank"] = i + 1

    with _cache_lock:
        _cache["data"] = results
        _cache["ts"] = time.time()

    return results


def get_strength_rankings() -> list:
    """Get cached strength rankings."""
    with _cache_lock:
        return _cache.get("data", [])


def build_strength_prompt_note(symbol: str) -> str:
    """Build a short note for the AI prompt about this symbol's strength score."""
    rankings = get_strength_rankings()
    if not rankings:
        return ""
    match = next((r for r in rankings if r["symbol"] == symbol), None)
    if not match:
        return ""
    score = match["score"]
    rank = match["rank"]
    total = len(rankings)
    label = "STRONG" if score >= 70 else "WEAK" if score < 30 else "NEUTRAL"
    return (
        f"\n--- Relative Strength ---\n"
        f"Strength Score: {score}/100 [{label}] (Rank {rank}/{total})\n"
        f"5d Performance vs SPY: {match['relative_perf']:+.2f}%\n"
        f"{'** HIGH STRENGTH — favorable for entry **' if score >= 70 else '** LOW STRENGTH — avoid new entries **' if score < 30 else ''}"
    )
