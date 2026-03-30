"""Strategy Presets — Momentum, Mean Reversion, and Breakout strategies.

The AI chooses which strategy fits each situation. Each trade is tagged with strategy used.
"""
from __future__ import annotations
import threading
import time
from engine.market_data import _is_yf_limited, _set_yf_limited
import numpy as np
from datetime import datetime
from rich.console import Console

console = Console()

_cache: dict = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 120  # 2 minutes


def _get_data(symbol: str) -> dict | None:
    """Fetch price/volume data needed for strategy evaluation."""
    cache_key = f"strat_data_{symbol}"
    with _cache_lock:
        if cache_key in _cache and time.time() - _cache[cache_key]["ts"] < _CACHE_TTL:
            return _cache[cache_key]["data"]

    if _is_yf_limited():
        return None
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="2mo", interval="1d")
        if hist.empty or len(hist) < 21:
            return None

        close = hist["Close"].values
        volume = hist["Volume"].values
        high = hist["High"].values
        low = hist["Low"].values

        # RSI
        deltas = np.diff(close[-15:])
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        avg_gain = np.mean(gains) if len(gains) > 0 else 0
        avg_loss = np.mean(losses) if len(losses) > 0 else 0.001
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        # RSI direction (rising from oversold?)
        prev_deltas = np.diff(close[-20:-5])
        prev_gains = np.where(prev_deltas > 0, prev_deltas, 0)
        prev_losses = np.where(prev_deltas < 0, -prev_deltas, 0)
        prev_avg_gain = np.mean(prev_gains) if len(prev_gains) > 0 else 0
        prev_avg_loss = np.mean(prev_losses) if len(prev_losses) > 0 else 0.001
        prev_rs = prev_avg_gain / prev_avg_loss
        prev_rsi = 100 - (100 / (1 + prev_rs))

        vol_avg_20 = np.mean(volume[-20:])
        vol_ratio = volume[-1] / vol_avg_20 if vol_avg_20 > 0 else 1.0
        daily_change = (close[-1] - close[-2]) / close[-2] * 100 if len(close) > 1 else 0
        high_20 = float(np.max(high[-20:]))

        result = {
            "close": close,
            "volume": volume,
            "high": high,
            "low": low,
            "rsi": float(rsi),
            "prev_rsi": float(prev_rsi),
            "vol_ratio": float(vol_ratio),
            "daily_change": float(daily_change),
            "high_20": high_20,
            "current": float(close[-1]),
        }

        with _cache_lock:
            _cache[cache_key] = {"data": result, "ts": time.time()}

        return result
    except Exception as e:
        err = str(e)
        if "Too Many Requests" in err or "Rate" in err:
            _set_yf_limited()
        console.log(f"[red]Strategy data error for {symbol}: {e}")
        return None


def evaluate_momentum(symbol: str, data: dict = None) -> dict | None:
    """Momentum: RSI rising from oversold + volume spike."""
    if data is None:
        data = _get_data(symbol)
    if not data:
        return None

    score = 0
    signals = []

    # RSI rising from oversold (was <35, now >35 and trending up)
    if data["prev_rsi"] < 35 and data["rsi"] > data["prev_rsi"]:
        score += 40
        signals.append(f"RSI rising from oversold ({data['prev_rsi']:.0f}→{data['rsi']:.0f})")
    elif data["rsi"] < 40 and data["rsi"] > data["prev_rsi"]:
        score += 25
        signals.append(f"RSI recovering ({data['rsi']:.0f})")

    # Volume spike
    if data["vol_ratio"] > 2.0:
        score += 30
        signals.append(f"Volume spike ({data['vol_ratio']:.1f}x avg)")
    elif data["vol_ratio"] > 1.5:
        score += 15
        signals.append(f"Elevated volume ({data['vol_ratio']:.1f}x)")

    # Price up today
    if data["daily_change"] > 0:
        score += 15
        signals.append(f"Price up {data['daily_change']:+.1f}%")

    # Positive momentum
    close = data["close"]
    if len(close) >= 5 and close[-1] > close[-5]:
        score += 15
        signals.append("5-day positive momentum")

    if score < 40:
        return None

    return {
        "strategy": "momentum",
        "label": "Momentum",
        "symbol": symbol,
        "score": min(100, score),
        "signals": signals,
        "entry": round(data["current"], 2),
        "stop": round(data["current"] * 0.92, 2),  # 8% stop
        "target": round(data["current"] * 1.12, 2),  # 12% target
    }


def evaluate_mean_reversion(symbol: str, data: dict = None) -> dict | None:
    """Mean Reversion: stock dropped >3% in a day with high volume (bounce play)."""
    if data is None:
        data = _get_data(symbol)
    if not data:
        return None

    score = 0
    signals = []

    # Big drop
    if data["daily_change"] < -3:
        score += 40
        signals.append(f"Sharp drop {data['daily_change']:.1f}%")
    elif data["daily_change"] < -2:
        score += 25
        signals.append(f"Significant drop {data['daily_change']:.1f}%")

    # High volume on the drop (capitulation)
    if data["vol_ratio"] > 2.0:
        score += 30
        signals.append(f"Capitulation volume ({data['vol_ratio']:.1f}x)")
    elif data["vol_ratio"] > 1.5:
        score += 15
        signals.append(f"Elevated volume ({data['vol_ratio']:.1f}x)")

    # RSI oversold
    if data["rsi"] < 30:
        score += 20
        signals.append(f"RSI oversold ({data['rsi']:.0f})")
    elif data["rsi"] < 40:
        score += 10
        signals.append(f"RSI low ({data['rsi']:.0f})")

    # Still above longer-term support (SMA 50)
    close = data["close"]
    if len(close) >= 50:
        sma50 = np.mean(close[-50:])
        if close[-1] > sma50 * 0.95:
            score += 10
            signals.append("Above SMA50 support zone")

    if score < 40:
        return None

    return {
        "strategy": "mean_reversion",
        "label": "Mean Reversion",
        "symbol": symbol,
        "score": min(100, score),
        "signals": signals,
        "entry": round(data["current"], 2),
        "stop": round(data["current"] * 0.95, 2),  # 5% stop (tight for bounce)
        "target": round(data["current"] * 1.06, 2),  # 6% target (mean revert)
    }


def evaluate_breakout(symbol: str, data: dict = None) -> dict | None:
    """Breakout: price breaking above 20-day high on >2x volume."""
    if data is None:
        data = _get_data(symbol)
    if not data:
        return None

    score = 0
    signals = []

    # Breaking 20-day high
    prior_20_high = float(np.max(data["high"][-21:-1])) if len(data["high"]) > 21 else data["high_20"]
    if data["current"] > prior_20_high:
        score += 40
        signals.append(f"New 20-day high (broke ${prior_20_high:.2f})")
    elif data["current"] > prior_20_high * 0.99:
        score += 20
        signals.append(f"Testing 20-day high (${prior_20_high:.2f})")

    # Volume confirmation
    if data["vol_ratio"] > 2.0:
        score += 30
        signals.append(f"Breakout volume ({data['vol_ratio']:.1f}x avg)")
    elif data["vol_ratio"] > 1.5:
        score += 15
        signals.append(f"Above-average volume ({data['vol_ratio']:.1f}x)")

    # Trend support
    close = data["close"]
    sma_20 = np.mean(close[-20:])
    if data["current"] > sma_20:
        score += 15
        signals.append("Above SMA20")

    # RSI not overbought (room to run)
    if 50 < data["rsi"] < 70:
        score += 15
        signals.append(f"RSI healthy ({data['rsi']:.0f})")

    if score < 40:
        return None

    return {
        "strategy": "breakout",
        "label": "Breakout",
        "symbol": symbol,
        "score": min(100, score),
        "signals": signals,
        "entry": round(data["current"], 2),
        "stop": round(prior_20_high * 0.97, 2),  # 3% below breakout level
        "target": round(data["current"] * 1.10, 2),  # 10% target
    }


def scan_strategies(symbols: list = None) -> list:
    """Scan all symbols for all strategy fits, return sorted by score."""
    if symbols is None:
        from config import WATCH_STOCKS
        symbols = WATCH_STOCKS

    results = []
    for sym in symbols:
        data = _get_data(sym)
        if not data:
            continue

        for evaluator in [evaluate_momentum, evaluate_mean_reversion, evaluate_breakout]:
            try:
                result = evaluator(sym, data)
                if result:
                    results.append(result)
            except Exception:
                continue

    results.sort(key=lambda x: x["score"], reverse=True)
    return results


def get_best_strategy(symbol: str) -> dict | None:
    """Get the highest-scoring strategy for a symbol."""
    data = _get_data(symbol)
    if not data:
        return None

    best = None
    for evaluator in [evaluate_momentum, evaluate_mean_reversion, evaluate_breakout]:
        try:
            result = evaluator(symbol, data)
            if result and (best is None or result["score"] > best["score"]):
                best = result
        except Exception:
            continue
    return best


def build_strategy_prompt_section(symbol: str) -> str:
    """Build prompt section showing available strategies for this symbol."""
    strategies = []
    data = _get_data(symbol)
    if not data:
        return ""

    for evaluator in [evaluate_momentum, evaluate_mean_reversion, evaluate_breakout]:
        try:
            result = evaluator(symbol, data)
            if result:
                strategies.append(result)
        except Exception:
            continue

    if not strategies:
        return ""

    strategies.sort(key=lambda x: x["score"], reverse=True)
    lines = ["\n--- Strategy Presets ---"]
    for s in strategies:
        signals_str = ", ".join(s["signals"][:3])
        lines.append(f"  {s['label']} (score {s['score']}): {signals_str}")
        lines.append(f"    Entry ${s['entry']} | Stop ${s['stop']} | Target ${s['target']}")
    lines.append("  Choose the strategy that best fits the current setup.")

    return "\n".join(lines)
