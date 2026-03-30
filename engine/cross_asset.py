"""Cross-Asset Monitor — track SPY, VIX, DXY, Oil together for macro signals."""
from __future__ import annotations
import threading
import time
from engine.market_data import _is_yf_limited, _set_yf_limited
import pandas as pd
from datetime import datetime
from rich.console import Console

console = Console()

_cache: dict = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 300  # 5 minutes

# Alert cooldowns
_vix_spike_alerted = False
_bearish_combo_alerted = False

CROSS_ASSETS = {
    "SPY": {"label": "S&P 500", "ticker": "SPY"},
    "VIX": {"label": "VIX", "ticker": "^VIX"},
    "DXY": {"label": "US Dollar", "ticker": "DX-Y.NYB"},
    "OIL": {"label": "Crude Oil", "ticker": "CL=F"},
}


def _fetch_asset(ticker_symbol: str) -> dict | None:
    """Fetch current price and daily change for an asset."""
    if _is_yf_limited():
        return None
    try:
        ticker = yf.Ticker(ticker_symbol)
        hist = ticker.history(period="5d", interval="1d")
        if hist.empty:
            return None

        current = float(hist["Close"].iloc[-1])
        prev = float(hist["Close"].iloc[-2]) if len(hist) > 1 else current
        change = current - prev
        change_pct = round((change / prev) * 100, 2) if prev > 0 else 0

        # 5-day trend
        if len(hist) >= 5:
            five_ago = float(hist["Close"].iloc[0])
            trend_5d = round((current / five_ago - 1) * 100, 2)
        else:
            trend_5d = change_pct

        return {
            "price": round(current, 2),
            "change": round(change, 2),
            "change_pct": change_pct,
            "trend_5d": trend_5d,
            "high": round(float(hist["High"].iloc[-1]), 2),
            "low": round(float(hist["Low"].iloc[-1]), 2),
        }
    except Exception as e:
        err = str(e)
        if "Too Many Requests" in err or "Rate" in err:
            _set_yf_limited()
        console.log(f"[red]Cross-asset error for {ticker_symbol}: {e}")
        return None


def get_cross_asset_monitor() -> dict:
    """Get cross-asset dashboard data with correlation signals."""
    with _cache_lock:
        if _cache.get("data") and time.time() - _cache.get("ts", 0) < _CACHE_TTL:
            return _cache["data"]

    assets = {}
    for key, info in CROSS_ASSETS.items():
        data = _fetch_asset(info["ticker"])
        if data:
            assets[key] = {**data, "label": info["label"], "key": key}

    # Generate correlation signals
    signals = []

    spy = assets.get("SPY", {})
    vix = assets.get("VIX", {})
    dxy = assets.get("DXY", {})
    oil = assets.get("OIL", {})

    # Signal 1: Oil AND Dollar both rising = bearish for equities
    if oil.get("change_pct", 0) > 0.5 and dxy.get("change_pct", 0) > 0.3:
        signals.append({
            "signal": "BEARISH MACRO",
            "description": f"Oil (+{oil['change_pct']:.1f}%) AND Dollar (+{dxy['change_pct']:.1f}%) both rising — bearish for equities",
            "severity": "high",
            "action": "Reduce equity exposure, consider defensive positions",
            "color": "#f85149",
        })

    # Signal 2: VIX spike > 20% intraday
    if vix.get("change_pct", 0) >= 20:
        signals.append({
            "signal": "VIX SPIKE ALERT",
            "description": f"VIX surged {vix['change_pct']:+.1f}% — auto-reduce position sizes by 50%",
            "severity": "critical",
            "action": "All position sizes halved, tighten stops",
            "color": "#f85149",
        })

    # Signal 3: VIX elevated (>25) but declining = risk-on
    if vix.get("price", 0) > 25 and vix.get("change_pct", 0) < -2:
        signals.append({
            "signal": "VIX RETREAT",
            "description": f"VIX at {vix['price']:.1f} but declining ({vix['change_pct']:+.1f}%) — volatility cooling",
            "severity": "medium",
            "action": "Cautious risk-on, consider adding to winners",
            "color": "#f0883e",
        })

    # Signal 4: Dollar dropping + SPY rising = risk-on tailwind
    if dxy.get("change_pct", 0) < -0.3 and spy.get("change_pct", 0) > 0.3:
        signals.append({
            "signal": "RISK-ON TAILWIND",
            "description": f"Dollar weakening ({dxy['change_pct']:+.1f}%) while SPY rising ({spy['change_pct']:+.1f}%)",
            "severity": "low",
            "action": "Favorable for equity longs, especially multinationals",
            "color": "#3fb950",
        })

    # Signal 5: Oil crashing = deflationary, watch growth stocks
    if oil.get("change_pct", 0) < -3:
        signals.append({
            "signal": "OIL CRASH",
            "description": f"Oil down {oil['change_pct']:.1f}% — deflationary signal",
            "severity": "medium",
            "action": "Bullish for tech/growth, bearish for energy stocks",
            "color": "#58a6ff",
        })

    # Signal 6: All-clear (low VIX, stable dollar, stable oil)
    if (vix.get("price", 30) < 18 and abs(dxy.get("change_pct", 0)) < 0.3
            and abs(oil.get("change_pct", 0)) < 1):
        signals.append({
            "signal": "ALL CLEAR",
            "description": "Low VIX, stable dollar and oil — calm macro environment",
            "severity": "low",
            "action": "Normal position sizing, trade your setups",
            "color": "#3fb950",
        })

    # Correlation arrows between assets
    correlations = []
    if spy and vix:
        # SPY and VIX are typically inversely correlated
        direction = "inverse" if spy.get("change_pct", 0) * vix.get("change_pct", 0) < 0 else "aligned"
        normal = direction == "inverse"
        correlations.append({
            "asset1": "SPY", "asset2": "VIX",
            "direction": direction, "normal": normal,
            "note": "Normal" if normal else "ABNORMAL — both moving same direction",
        })
    if spy and dxy:
        direction = "inverse" if spy.get("change_pct", 0) * dxy.get("change_pct", 0) < 0 else "aligned"
        correlations.append({
            "asset1": "SPY", "asset2": "DXY",
            "direction": direction, "normal": True,  # Can go either way
            "note": "Dollar strength → headwind" if dxy.get("change_pct", 0) > 0.5 else "Neutral",
        })
    if spy and oil:
        direction = "aligned" if spy.get("change_pct", 0) * oil.get("change_pct", 0) > 0 else "inverse"
        correlations.append({
            "asset1": "SPY", "asset2": "OIL",
            "direction": direction, "normal": True,
            "note": "Growth sensitive" if direction == "aligned" else "Diverging",
        })

    # Position sizing multiplier
    sizing_multiplier = 1.0
    if vix.get("change_pct", 0) >= 20:
        sizing_multiplier = 0.5
    elif vix.get("price", 0) >= 30:
        sizing_multiplier = 0.6
    elif vix.get("price", 0) >= 25:
        sizing_multiplier = 0.75

    result = {
        "assets": assets,
        "signals": signals,
        "correlations": correlations,
        "sizing_multiplier": sizing_multiplier,
        "macro_bias": _determine_macro_bias(signals),
        "updated": datetime.now().isoformat(),
    }

    with _cache_lock:
        _cache["data"] = result
        _cache["ts"] = time.time()

    return result


def _determine_macro_bias(signals: list) -> dict:
    """Determine overall macro bias from signals."""
    if any(s["severity"] == "critical" for s in signals):
        return {"bias": "RISK-OFF", "color": "#f85149"}
    bearish = sum(1 for s in signals if "bearish" in s["signal"].lower() or s["severity"] == "high")
    bullish = sum(1 for s in signals if "risk-on" in s["signal"].lower() or "clear" in s["signal"].lower())
    if bearish > bullish:
        return {"bias": "BEARISH", "color": "#f85149"}
    if bullish > bearish:
        return {"bias": "BULLISH", "color": "#3fb950"}
    return {"bias": "NEUTRAL", "color": "#8b949e"}


def get_vix_sizing_factor() -> float:
    """Get position sizing factor based on VIX conditions."""
    data = get_cross_asset_monitor()
    return data.get("sizing_multiplier", 1.0)


def check_vix_auto_reduce() -> bool:
    """Check if VIX spiked >20% and we need to auto-reduce positions."""
    global _vix_spike_alerted
    data = get_cross_asset_monitor()
    vix = data.get("assets", {}).get("VIX", {})
    if vix.get("change_pct", 0) >= 20 and not _vix_spike_alerted:
        _vix_spike_alerted = True
        try:
            from engine.telegram_alerts import send_alert
            send_alert(
                f"<b>VIX SPIKE {vix['change_pct']:+.1f}%</b>\n"
                f"VIX at {vix['price']:.2f}\n"
                f"AUTO-REDUCING all position sizes by 50%"
            )
        except Exception:
            pass
        console.log(f"[bold red]VIX SPIKE: {vix['change_pct']:+.1f}% — auto-reducing position sizes")
        return True
    if vix.get("change_pct", 0) < 10:
        _vix_spike_alerted = False
    return False
