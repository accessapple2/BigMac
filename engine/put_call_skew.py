"""Put/Call Skew Monitor — ratio of put premium to call premium."""
from __future__ import annotations
import threading
import time
from engine.market_data import yahoo_options_chain, _yahoo_chart
import pandas as pd
import numpy as np
from datetime import datetime
from rich.console import Console

console = Console()

_cache: dict = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 300  # 5 minutes

# Alert cooldown
_skew_alerted: dict = {}
ALERT_COOLDOWN = 1800  # 30 min


def compute_put_call_skew(symbol: str) -> dict | None:
    """Calculate put/call skew for a symbol using Yahoo options chain.

    Skew = total put premium / total call premium * 100
    >200% = EXTREME FEAR
    >150% = ELEVATED FEAR
    100% = NEUTRAL
    <80% = COMPLACENT
    <50% = EXTREME GREED
    """
    cache_key = f"skew_{symbol}"
    with _cache_lock:
        if cache_key in _cache and time.time() - _cache[cache_key]["ts"] < _CACHE_TTL:
            return _cache[cache_key]["data"]

    try:
        chain = yahoo_options_chain(symbol)
        if not chain:
            return None

        # Get spot price from chain quote
        quote = chain.get("quote", {})
        spot = quote.get("regularMarketPrice", 0)
        if not spot:
            # Fallback to chart
            chart = _yahoo_chart(symbol, interval="1m", range_="1d")
            if chart:
                closes = chart.get("indicators", {}).get("quote", [{}])[0].get("close", [])
                closes = [c for c in closes if c is not None]
                spot = closes[-1] if closes else 0
        if not spot:
            return None

        # Get nearest expiration options
        options = chain.get("options", [])
        if not options:
            return None

        first_exp = options[0]
        calls = first_exp.get("calls", [])
        puts = first_exp.get("puts", [])

        if not calls or not puts:
            return None

        # Get expiration date
        exp_ts = chain.get("expirationDates", [])
        nearest = datetime.utcfromtimestamp(exp_ts[0]).strftime("%Y-%m-%d") if exp_ts else "unknown"

        # Filter to ATM range (+-10% from spot)
        atm_calls = [c for c in calls if spot * 0.9 <= c.get("strike", 0) <= spot * 1.1]
        atm_puts = [p for p in puts if spot * 0.9 <= p.get("strike", 0) <= spot * 1.1]

        # Calculate total premium (last price * OI as weight)
        def _safe_premium(options_list):
            total = 0.0
            for opt in options_list:
                price = opt.get("lastPrice", 0) or 0
                oi = opt.get("openInterest", 0) or 0
                vol = opt.get("volume", 0) or 0
                weight = max(oi, vol)
                total += price * weight
            return total

        call_premium = _safe_premium(atm_calls)
        put_premium = _safe_premium(atm_puts)

        if call_premium <= 0:
            skew_pct = 200.0
        else:
            skew_pct = round(put_premium / call_premium * 100, 1)

        # Put/Call ratio by OI
        total_call_oi = sum(c.get("openInterest", 0) or 0 for c in atm_calls)
        total_put_oi = sum(p.get("openInterest", 0) or 0 for p in atm_puts)
        pc_ratio = round(total_put_oi / total_call_oi, 2) if total_call_oi > 0 else 0

        # Label
        if skew_pct >= 200:
            label = "EXTREME FEAR"
            color = "#f85149"
        elif skew_pct >= 150:
            label = "ELEVATED FEAR"
            color = "#f0883e"
        elif skew_pct >= 80:
            label = "NEUTRAL"
            color = "#8b949e"
        elif skew_pct >= 50:
            label = "COMPLACENT"
            color = "#58a6ff"
        else:
            label = "EXTREME GREED"
            color = "#3fb950"

        result = {
            "symbol": symbol,
            "skew_pct": skew_pct,
            "label": label,
            "color": color,
            "put_premium": round(put_premium, 2),
            "call_premium": round(call_premium, 2),
            "pc_ratio": pc_ratio,
            "total_call_oi": total_call_oi,
            "total_put_oi": total_put_oi,
            "spot": round(spot, 2),
            "expiration": nearest,
        }

        with _cache_lock:
            _cache[cache_key] = {"data": result, "ts": time.time()}

        return result

    except Exception as e:
        console.log(f"[red]Put/Call skew error for {symbol}: {e}")
        return None


def get_all_skew(symbols: list = None) -> list:
    """Get put/call skew for multiple symbols."""
    if symbols is None:
        symbols = ["SPY", "QQQ"]
    results = []
    for sym in symbols:
        data = compute_put_call_skew(sym)
        if data:
            results.append(data)
    return results


def check_extreme_skew():
    """Check for extreme fear skew and send Telegram alert."""
    now = time.time()
    for sym in ["SPY", "QQQ"]:
        data = compute_put_call_skew(sym)
        if not data:
            continue
        if data["skew_pct"] >= 200:
            key = f"extreme_skew_{sym}"
            if key not in _skew_alerted or now - _skew_alerted[key] > ALERT_COOLDOWN:
                _skew_alerted[key] = now
                try:
                    from engine.telegram_alerts import send_alert
                    send_alert(
                        f"<b>EXTREME FEAR</b> {sym} put/call skew at {data['skew_pct']:.0f}%\n"
                        f"Put premium is {data['skew_pct']/100:.1f}x call premium\n"
                        f"P/C Ratio: {data['pc_ratio']:.2f}"
                    )
                    console.log(f"[bold red]EXTREME SKEW: {sym} at {data['skew_pct']:.0f}%")
                except Exception:
                    pass
