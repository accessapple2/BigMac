"""
IV Skew Monitor
---------------
Computes 25-delta put/call IV skew from the SPY option chain.
Positive skew = fear premium (puts more expensive).
Negative skew = greed (calls more expensive).

No new DB table needed — data is ephemeral / cached.
Endpoint: GET /api/ready-room/skew
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any

_cache: dict[str, Any] = {}
_cache_ts: float = 0.0
CACHE_TTL = 300  # 5 minutes


def _nearest_expiry_snaps(symbol: str = "SPY") -> list[Any]:
    """Return option snapshots for the nearest expiry of SPY."""
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


def _compute_skew(snaps: list[Any]) -> dict[str, Any]:
    """
    Find the 25-delta put and 25-delta call closest to |delta| = 0.25,
    then return IV(put) - IV(call) as skew score.
    """
    puts_25: list[tuple[float, float]] = []   # (|delta - 0.25|, iv)
    calls_25: list[tuple[float, float]] = []

    for snap in snaps:
        greeks = getattr(snap, "greeks", None)
        iv = getattr(snap, "implied_volatility", None)
        if not greeks or iv is None:
            continue
        delta = getattr(greeks, "delta", None)
        if delta is None:
            continue

        # Puts have negative delta; calls positive
        if delta < 0:
            distance = abs(abs(delta) - 0.25)
            puts_25.append((distance, float(iv)))
        else:
            distance = abs(delta - 0.25)
            calls_25.append((distance, float(iv)))

    if not puts_25 or not calls_25:
        return {
            "put_iv_25d":  None,
            "call_iv_25d": None,
            "skew_score":  None,
            "signal":      "Insufficient option data for skew calculation.",
        }

    puts_25.sort(key=lambda x: x[0])
    calls_25.sort(key=lambda x: x[0])

    put_iv  = round(puts_25[0][1]  * 100, 2)
    call_iv = round(calls_25[0][1] * 100, 2)
    skew    = round(put_iv - call_iv, 2)

    if skew > 5:
        signal = f"🐻 High fear skew ({skew:.1f}pp). Put buyers are loading up — expect defensive tape."
    elif skew > 2:
        signal = f"⚠️ Moderate put skew ({skew:.1f}pp). Mild protective positioning."
    elif skew < -2:
        signal = f"🐂 Negative skew ({skew:.1f}pp) — call demand > puts. Greed premium present."
    else:
        signal = f"⚖️ Neutral skew ({skew:.1f}pp). No strong directional options bias."

    return {
        "put_iv_25d":  put_iv,
        "call_iv_25d": call_iv,
        "skew_score":  skew,
        "signal":      signal,
    }


def get_iv_skew(symbol: str = "SPY", force: bool = False) -> dict[str, Any]:
    global _cache, _cache_ts
    now = time.time()
    if not force and _cache and (now - _cache_ts) < CACHE_TTL:
        return _cache

    snaps = _nearest_expiry_snaps(symbol)
    skew  = _compute_skew(snaps)

    result = {
        "symbol": symbol,
        "snap_count": len(snaps),
        **skew,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }

    _cache = result
    _cache_ts = now
    return result
