"""
Multi-Timeframe Gamma Map
--------------------------
Computes GEX per timeframe (0DTE, weekly, monthly) from SPY option chain
and identifies confluence zones where multiple expiries cluster.

GEX formula: gamma × OI × 100 × spot² × 0.01
  Calls add positive GEX, puts subtract (dealers short puts → long gamma on puts too,
  but convention: call GEX positive, put GEX negative).

Endpoint: GET /api/ready-room/gamma-map
"""
from __future__ import annotations

import os
import time
from collections import defaultdict
from datetime import date, datetime, timezone, timedelta
from typing import Any

_cache: dict[str, Any] = {}
_cache_ts: float = 0.0
CACHE_TTL = 300


def _get_snaps(symbol: str = "SPY") -> list[Any]:
    try:
        from alpaca.data import OptionHistoricalDataClient
        from alpaca.data.requests import OptionSnapshotRequest

        api_key = os.environ.get("ALPACA_API_KEY", "")
        secret  = os.environ.get("ALPACA_SECRET_KEY", "")
        client  = OptionHistoricalDataClient(api_key, secret)

        req = OptionSnapshotRequest(underlying_symbols=[symbol])
        snaps = client.get_option_snapshot(req)
        return list(snaps.values()) if snaps else []
    except Exception:
        return []


def _parse_expiry(snap: Any) -> date | None:
    """Extract expiry date from the option symbol or snapshot."""
    try:
        sym = getattr(snap, "symbol", "") or ""
        # Standard OCC format: SPY250401C00550000
        if len(sym) >= 15:
            date_part = sym[3:9]  # YYMMDD
            return date(2000 + int(date_part[:2]), int(date_part[2:4]), int(date_part[4:6]))
    except Exception:
        pass
    return None


def _classify_expiry(exp: date, today: date) -> str:
    days = (exp - today).days
    if days == 0:
        return "0DTE"
    # Find nearest Friday
    days_to_friday = (4 - today.weekday()) % 7
    next_friday = today + timedelta(days=days_to_friday if days_to_friday > 0 else 7)
    if exp <= next_friday:
        return "weekly"
    # Monthly = 3rd Friday of expiry month
    return "monthly"


def _compute_gex_by_timeframe(snaps: list[Any], spot: float) -> dict[str, Any]:
    today = date.today()
    timeframes: dict[str, float] = defaultdict(float)
    strike_gex: dict[float, float] = defaultdict(float)
    timeframe_strikes: dict[str, dict[float, float]] = {
        "0DTE": defaultdict(float),
        "weekly": defaultdict(float),
        "monthly": defaultdict(float),
    }

    for snap in snaps:
        greeks = getattr(snap, "greeks", None)
        if not greeks:
            continue
        gamma = getattr(greeks, "gamma", None)
        if gamma is None:
            continue

        details = getattr(snap, "details", None)
        oi = getattr(details, "open_interest", None) if details else None
        strike = getattr(details, "strike_price", None) if details else None
        opt_type = getattr(details, "type", "") if details else ""
        if oi is None or strike is None:
            continue

        exp = _parse_expiry(snap)
        if exp is None:
            continue

        tf = _classify_expiry(exp, today)
        if tf not in timeframes:
            continue

        gex = float(gamma) * float(oi) * 100 * (spot ** 2) * 0.01
        if "put" in str(opt_type).lower():
            gex = -gex

        timeframes[tf] += gex
        strike_gex[float(strike)] += gex
        timeframe_strikes[tf][float(strike)] += gex

    # Confluence zones: strikes where |GEX| is top 5 across all timeframes
    sorted_strikes = sorted(strike_gex.items(), key=lambda x: abs(x[1]), reverse=True)
    confluence = [
        {"strike": s, "gex": round(g / 1e9, 3)}
        for s, g in sorted_strikes[:5]
    ]

    return {
        "gex_by_timeframe": {
            tf: round(gex / 1e9, 3) for tf, gex in timeframes.items()
        },
        "confluence_zones": confluence,
        "total_gex": round(sum(timeframes.values()) / 1e9, 3),
        "snap_count": len(snaps),
    }


def get_gamma_map(symbol: str = "SPY", force: bool = False) -> dict[str, Any]:
    global _cache, _cache_ts
    now = time.time()
    if not force and _cache and (now - _cache_ts) < CACHE_TTL:
        return _cache

    snaps = _get_snaps(symbol)

    # Get spot price from snaps (use underlying_price if available)
    spot = 0.0
    for snap in snaps:
        up = getattr(snap, "underlying_price", None) or getattr(snap, "latest_trade", None)
        if up:
            try:
                spot = float(up) if not hasattr(up, "price") else float(up.price)
                break
            except Exception:
                pass

    if spot <= 0 or not snaps:
        result = {
            "symbol": symbol,
            "error": "No option data or spot price unavailable.",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
        _cache = result
        _cache_ts = now
        return result

    gex_data = _compute_gex_by_timeframe(snaps, spot)
    total = gex_data.get("total_gex", 0)
    if total > 0:
        signal = f"🟢 Net positive GEX ({total:.2f}B) — dealers long gamma, expect mean-reversion / range."
    elif total < -1:
        signal = f"🔴 Net negative GEX ({total:.2f}B) — dealers short gamma, expect amplified moves."
    else:
        signal = f"⚪ Near-zero GEX ({total:.2f}B) — gamma neutral. Direction unclear."

    result = {
        "symbol": symbol,
        "spot":   spot,
        "signal": signal,
        **gex_data,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }

    _cache = result
    _cache_ts = now
    return result
