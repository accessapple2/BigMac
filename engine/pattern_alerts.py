"""Pattern Alert Tiles — enriched pattern cards with breakout/target/stop/win-rate."""
from __future__ import annotations
import threading
import time
from datetime import datetime
from rich.console import Console

console = Console()

_cache: dict = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 300

# Historical win rates for common patterns (approximate from academic studies)
PATTERN_WIN_RATES = {
    "double_top": {"win_rate": 65, "avg_move_pct": -8.5, "direction": "bearish"},
    "double_bottom": {"win_rate": 67, "avg_move_pct": 10.2, "direction": "bullish"},
    "head_and_shoulders": {"win_rate": 70, "avg_move_pct": -12.0, "direction": "bearish"},
    "ascending_triangle": {"win_rate": 72, "avg_move_pct": 9.8, "direction": "bullish"},
    "descending_triangle": {"win_rate": 68, "avg_move_pct": -10.5, "direction": "bearish"},
    "rising_wedge": {"win_rate": 60, "avg_move_pct": -7.2, "direction": "bearish"},
    "falling_wedge": {"win_rate": 62, "avg_move_pct": 8.0, "direction": "bullish"},
}


def _calc_breakout_stop(pattern: dict) -> dict:
    """Calculate breakout price, target, and stop-loss for a pattern."""
    p_type = pattern.get("pattern", "")
    current = pattern.get("current_price", 0)
    target = pattern.get("target", 0)

    if p_type == "double_top":
        neckline = pattern.get("neckline", current)
        breakout = neckline  # Breakdown below neckline
        stop = pattern.get("peak", current * 1.02)
        return {"breakout": round(breakout, 2), "target": round(target, 2),
                "stop_loss": round(stop, 2), "trigger": "break below neckline"}

    elif p_type == "double_bottom":
        neckline = pattern.get("neckline", current)
        breakout = neckline  # Breakout above neckline
        stop = pattern.get("trough", current * 0.98)
        return {"breakout": round(breakout, 2), "target": round(target, 2),
                "stop_loss": round(stop, 2), "trigger": "break above neckline"}

    elif p_type == "head_and_shoulders":
        neckline = pattern.get("neckline", current)
        breakout = neckline
        stop = pattern.get("right_shoulder", current * 1.03)
        return {"breakout": round(breakout, 2), "target": round(target, 2),
                "stop_loss": round(stop, 2), "trigger": "break below neckline"}

    elif p_type == "ascending_triangle":
        resistance = pattern.get("resistance", current)
        breakout = resistance
        support = pattern.get("support_trend", current * 0.97)
        return {"breakout": round(breakout, 2), "target": round(target, 2),
                "stop_loss": round(support, 2), "trigger": "break above resistance"}

    elif p_type == "descending_triangle":
        support = pattern.get("support", current)
        breakout = support
        resistance = pattern.get("resistance_trend", current * 1.03)
        return {"breakout": round(breakout, 2), "target": round(target, 2),
                "stop_loss": round(resistance, 2), "trigger": "break below support"}

    elif p_type in ("rising_wedge", "falling_wedge"):
        breakout = current  # Already in the wedge
        stop = current * 1.03 if p_type == "rising_wedge" else current * 0.97
        return {"breakout": round(breakout, 2), "target": round(target, 2),
                "stop_loss": round(stop, 2), "trigger": "wedge breakdown" if p_type == "rising_wedge" else "wedge breakout"}

    # Default
    return {"breakout": round(current, 2), "target": round(target, 2),
            "stop_loss": round(current * 0.97, 2), "trigger": "pattern completion"}


def get_pattern_alert_tiles(symbols: list = None) -> list:
    """Get enriched pattern alert tiles for all detected patterns."""
    cache_key = "pattern_tiles"
    with _cache_lock:
        if cache_key in _cache and time.time() - _cache[cache_key]["ts"] < _CACHE_TTL:
            return _cache[cache_key]["data"]

    from engine.chart_patterns import detect_all_patterns
    if symbols is None:
        from config import WATCH_STOCKS
        symbols = WATCH_STOCKS

    raw_patterns = detect_all_patterns(symbols)
    tiles = []

    for p in raw_patterns:
        p_type = p.get("pattern", "unknown")
        meta = PATTERN_WIN_RATES.get(p_type, {"win_rate": 50, "avg_move_pct": 0, "direction": "neutral"})
        levels = _calc_breakout_stop(p)

        tile = {
            "symbol": p.get("symbol", ""),
            "pattern": p_type,
            "label": p.get("label", p_type.replace("_", " ").title()),
            "direction": p.get("direction", "neutral"),
            "current_price": p.get("current_price", 0),
            "breakout_price": levels["breakout"],
            "target_price": levels["target"],
            "stop_loss": levels["stop_loss"],
            "trigger": levels["trigger"],
            "win_rate": meta["win_rate"],
            "avg_move_pct": meta["avg_move_pct"],
            "confidence": _calc_pattern_confidence(p, meta),
            "risk_reward": _calc_rr(p.get("current_price", 0), levels["target"], levels["stop_loss"]),
            "updated": datetime.now().isoformat(),
        }
        tiles.append(tile)

    # Sort by confidence descending
    tiles.sort(key=lambda x: x["confidence"], reverse=True)

    with _cache_lock:
        _cache[cache_key] = {"data": tiles, "ts": time.time()}

    return tiles


def _calc_pattern_confidence(pattern: dict, meta: dict) -> int:
    """Calculate confidence % for a pattern based on win rate and proximity to breakout."""
    base = meta["win_rate"]
    current = pattern.get("current_price", 0)

    # Bonus for being near breakout level
    neckline = pattern.get("neckline") or pattern.get("resistance") or pattern.get("support")
    if neckline and current > 0:
        proximity = abs(current - neckline) / current * 100
        if proximity < 1:
            base += 10  # Very close to breakout
        elif proximity < 3:
            base += 5

    return min(95, base)


def _calc_rr(current: float, target: float, stop: float) -> float:
    """Calculate risk:reward ratio."""
    if current <= 0 or stop <= 0:
        return 0
    risk = abs(current - stop)
    reward = abs(target - current)
    if risk == 0:
        return 0
    return round(reward / risk, 2)
