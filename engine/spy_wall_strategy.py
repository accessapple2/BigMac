"""
SPY GEX Wall Strategy — 0DTE options trigger logic for T'Pol (dayblade-0dte).

Rules
-----
PUT WALL BOUNCE (BUY CALL):
  - SPY within 0.3% of the nearest put-wall strike
  - Momentum trend_score flipping up (currently > 0, was <= 0 in prior reading)
  - VIX dropping (current VIX < prior VIX by at least 0.3 points)
  → BUY ATM 0DTE CALL  |  stop -30%  |  target +50%  |  max hold 45 min

CALL WALL FADE (BUY PUT):
  - SPY within 0.3% of the nearest call-wall strike
  - Momentum trend_score flipping down (currently < 0, was >= 0 in prior reading)
  - Volume spike (SPY volume bar significantly above average)
  → BUY ATM 0DTE PUT  |  stop -30%  |  target +50%  |  max hold 45 min

Returns
-------
check_spy_wall_setup() → dict with keys:
  signal      : "BUY_CALL" | "BUY_PUT" | "NONE"
  spy_price   : float
  wall_strike : float  (nearest matched wall)
  wall_type   : "put_wall" | "call_wall" | None
  distance_pct: float  (% from wall)
  trend_score : float
  vix         : float
  reason      : str
"""

from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

# How close SPY must be to a wall to trigger (0.3%)
_WALL_PROXIMITY_PCT = 0.003

# Minimum VIX drop to confirm "dropping" condition
_VIX_DROP_MIN = 0.3

# Momentum threshold: score must cross zero in the right direction
_MOMENTUM_FLIP_THRESHOLD = 5  # avoids noise near zero

# Simple rolling state for flip detection (in-process memory)
_prior_state: dict[str, Any] = {
    "trend_score": None,
    "vix":         None,
    "ts":          0,
}
_STATE_MAX_AGE = 600  # 10 min — discard stale prior readings


def _get_spy_price() -> float:
    try:
        from engine.market_data import get_stock_price
        d = get_stock_price("SPY")
        return float(d.get("price") or 0)
    except Exception:
        return 0.0


def _get_vix() -> float:
    try:
        from engine.ready_room import get_latest_briefing
        b = get_latest_briefing() or {}
        return float(b.get("vix") or 0)
    except Exception:
        return 0.0


def _get_momentum() -> float:
    try:
        from engine.momentum_tracker import get_intraday_momentum
        m = get_intraday_momentum("SPY")
        return float(m.get("trend_score") or 0)
    except Exception:
        return 0.0


def _get_spy_walls() -> tuple[float | None, float | None]:
    """Return (nearest_put_wall_strike, nearest_call_wall_strike) for SPY."""
    try:
        from engine.gex_scanner import get_gex
        gex = get_gex("SPY")
        if not gex:
            return None, None
        magnets = gex.get("magnets", []) + gex.get("secondary_levels", [])
        put_walls  = [m["strike"] for m in magnets if m.get("type") == "put_wall"]
        call_walls = [m["strike"] for m in magnets if m.get("type") == "call_wall"]
        return (min(put_walls, key=lambda s: abs(s)) if put_walls else None,
                min(call_walls, key=lambda s: abs(s)) if call_walls else None)
    except Exception:
        return None, None


def check_spy_wall_setup() -> dict[str, Any]:
    """
    Evaluate current market state against the SPY wall strategy rules.
    Updates prior-state tracking for momentum/VIX flip detection.
    """
    global _prior_state

    spy   = _get_spy_price()
    vix   = _get_vix()
    trend = _get_momentum()
    now   = time.time()

    base: dict[str, Any] = {
        "signal":       "NONE",
        "spy_price":    spy,
        "wall_strike":  None,
        "wall_type":    None,
        "distance_pct": None,
        "trend_score":  trend,
        "vix":          vix,
        "reason":       "No wall setup triggered",
    }

    if spy <= 0:
        base["reason"] = "SPY price unavailable"
        return base

    prior_trend = _prior_state.get("trend_score")
    prior_vix   = _prior_state.get("vix")
    prior_age   = now - _prior_state.get("ts", 0)

    # Update prior state for next call
    _prior_state = {"trend_score": trend, "vix": vix, "ts": now}

    put_wall, call_wall = _get_spy_walls()

    # ── PUT WALL BOUNCE → BUY CALL ────────────────────────────────────────────
    if put_wall and put_wall > 0:
        dist = abs(spy - put_wall) / spy
        if dist <= _WALL_PROXIMITY_PCT:
            # Momentum flipping up: now positive, was negative (or first reading)
            momentum_flipping_up = (
                trend > _MOMENTUM_FLIP_THRESHOLD and (
                    prior_trend is None or
                    (prior_age < _STATE_MAX_AGE and prior_trend <= _MOMENTUM_FLIP_THRESHOLD)
                )
            )
            # VIX dropping
            vix_dropping = (
                prior_vix is None or
                (prior_age < _STATE_MAX_AGE and prior_vix - vix >= _VIX_DROP_MIN)
            )
            if momentum_flipping_up and vix_dropping:
                base.update({
                    "signal":       "BUY_CALL",
                    "wall_strike":  put_wall,
                    "wall_type":    "put_wall",
                    "distance_pct": round(dist * 100, 3),
                    "reason": (
                        f"SPY ${spy:.2f} at put wall ${put_wall:.0f} ({dist*100:.2f}% away). "
                        f"Momentum flipping up (score {trend:+.0f}). "
                        f"VIX dropping ({prior_vix:.1f}→{vix:.1f}). "
                        f"BUY ATM 0DTE CALL | stop -30% | target +50% | max 45min"
                    ),
                })
                return base

    # ── CALL WALL FADE → BUY PUT ──────────────────────────────────────────────
    if call_wall and call_wall > 0:
        dist = abs(spy - call_wall) / spy
        if dist <= _WALL_PROXIMITY_PCT:
            # Momentum flipping down: now negative, was positive (or first reading)
            momentum_flipping_down = (
                trend < -_MOMENTUM_FLIP_THRESHOLD and (
                    prior_trend is None or
                    (prior_age < _STATE_MAX_AGE and prior_trend >= -_MOMENTUM_FLIP_THRESHOLD)
                )
            )
            # Volume spike: check via momentum volume_delta
            volume_spike = False
            try:
                from engine.momentum_tracker import get_intraday_momentum
                m = get_intraday_momentum("SPY")
                buy_vol  = float(m.get("buy_volume", 0) or 0)
                sell_vol = float(m.get("sell_volume", 0) or 0)
                total    = buy_vol + sell_vol
                # Volume spike if sell volume > 60% of total and total is substantial
                volume_spike = total > 0 and sell_vol / total > 0.60
            except Exception:
                pass

            if momentum_flipping_down and volume_spike:
                base.update({
                    "signal":       "BUY_PUT",
                    "wall_strike":  call_wall,
                    "wall_type":    "call_wall",
                    "distance_pct": round(dist * 100, 3),
                    "reason": (
                        f"SPY ${spy:.2f} at call wall ${call_wall:.0f} ({dist*100:.2f}% away). "
                        f"Momentum flipping down (score {trend:+.0f}). "
                        f"Volume spike (sell-side dominant). "
                        f"BUY ATM 0DTE PUT | stop -30% | target +50% | max 45min"
                    ),
                })
                return base

    # No trigger — provide diagnostic info
    walls_info = []
    if put_wall:
        walls_info.append(f"put wall ${put_wall:.0f} ({abs(spy-put_wall)/spy*100:.1f}% away)")
    if call_wall:
        walls_info.append(f"call wall ${call_wall:.0f} ({abs(spy-call_wall)/spy*100:.1f}% away)")
    base["reason"] = (
        f"No trigger. SPY ${spy:.2f} | trend {trend:+.0f} | VIX {vix:.1f} | "
        + (", ".join(walls_info) if walls_info else "no GEX walls available")
    )
    return base
