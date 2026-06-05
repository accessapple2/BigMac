"""
SUPER_MAX Wave 3 — GEX Level Derivation + Strategy Tag Mapper
Observation-only. Never called from any execution path.

Two public functions:
  derive_levels()    — gives stop/target to signals that have none (unusual_oi)
  tag_strategy()     — maps GEX regime → strategy tag for all shadow signals
"""

from __future__ import annotations
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ── Strategy tag vocabulary ───────────────────────────────────────────────────
# Format: {GAMMA_REGIME}_{LEAN}_{SUGGESTED_STRUCTURE}
# Gamma regime: LONG_GAMMA (spot > flip) | SHORT_GAMMA (spot < flip) | NEUTRAL (no flip)
# Lean: BULL | BEAR | NEUTRAL
# Structure: suggestion only — W4 routing makes the final call

TAG_LONG_GAMMA_BULL   = "LONG_GAMMA_BULL_CALL_SPREAD"
TAG_LONG_GAMMA_BEAR   = "LONG_GAMMA_BEAR_PUT_SPREAD"
TAG_LONG_GAMMA_NEUTRAL = "LONG_GAMMA_NEUTRAL"
TAG_SHORT_GAMMA_BULL  = "SHORT_GAMMA_BULL_CALL"      # outright — wider swings expected
TAG_SHORT_GAMMA_BEAR  = "SHORT_GAMMA_BEAR_PUT"
TAG_SHORT_GAMMA_NEUTRAL = "SHORT_GAMMA_NEUTRAL"
TAG_UNKNOWN           = "UNKNOWN"

# ── Level derivation constants ────────────────────────────────────────────────
STOP_BUFFER_PCT   = 0.005   # 0.5% beyond the derived wall (breathing room)
TARGET_RR_RATIO   = 2.0     # minimum R:R for derived target (2× risk)
MIN_STOP_DIST_PCT = 0.003   # if derived stop is tighter than this → skip


def derive_levels(
    entry_price: float,
    action: str,           # 'BUY' | 'SELL' | 'BUY_CALL' etc.
    gamma_flip: Optional[float],
    call_wall: Optional[float],
    put_wall: Optional[float],
    king_node: Optional[float],
    lean: str = "neutral",  # 'bullish' | 'bearish' | 'neutral'
) -> dict:
    """
    Derive stop + target for signals that had stop=None / target=None.
    Uses GEX structural levels as anchors.

    Returns dict with w3_derived_stop, w3_derived_target, w3_level_source,
    w3_level_note. All None if derivation fails.
    """
    result = {
        "w3_derived_stop":   None,
        "w3_derived_target": None,
        "w3_level_source":   "none",
        "w3_level_note":     "",
    }

    if not (entry_price and entry_price > 0):
        result["w3_level_note"] = "SKIP: invalid entry"
        return result

    is_long = action.upper() in ("BUY", "BUY_CALL", "WATCH")

    # ── Choose stop anchor ────────────────────────────────────────────────────
    # For longs: stop below entry → use put_wall or gamma_flip (whichever closer below)
    # For shorts: stop above entry → use call_wall or gamma_flip (whichever closer above)
    candidates = []

    if is_long:
        for level, source in [(put_wall, "put_wall"), (gamma_flip, "gamma_flip")]:
            if level and level < entry_price:
                candidates.append((level, source))
        candidates.sort(key=lambda x: x[0], reverse=True)  # closest below first
    else:
        for level, source in [(call_wall, "call_wall"), (gamma_flip, "gamma_flip")]:
            if level and level > entry_price:
                candidates.append((level, source))
        candidates.sort(key=lambda x: x[0])  # closest above first

    if not candidates:
        result["w3_level_note"] = "SKIP: no GEX level below/above entry for stop anchor"
        return result

    anchor_level, anchor_source = candidates[0]

    # Apply buffer
    if is_long:
        stop = anchor_level * (1 - STOP_BUFFER_PCT)
    else:
        stop = anchor_level * (1 + STOP_BUFFER_PCT)

    stop_dist_pct = abs(entry_price - stop) / entry_price
    if stop_dist_pct < MIN_STOP_DIST_PCT:
        result["w3_level_note"] = (
            f"SKIP: derived stop distance {stop_dist_pct:.3%} < min {MIN_STOP_DIST_PCT:.3%}"
        )
        return result

    # ── Derive target at TARGET_RR_RATIO × risk ───────────────────────────────
    risk = abs(entry_price - stop)
    if is_long:
        target = entry_price + (risk * TARGET_RR_RATIO)
    else:
        target = entry_price - (risk * TARGET_RR_RATIO)

    result["w3_derived_stop"]   = round(stop, 4)
    result["w3_derived_target"] = round(target, 4)
    result["w3_level_source"]   = anchor_source
    result["w3_level_note"]     = (
        f"anchor={anchor_source}@{anchor_level:.2f} "
        f"stop={stop:.2f}({stop_dist_pct:.2%}) "
        f"target={target:.2f}(RR={TARGET_RR_RATIO}x)"
    )

    logger.debug("[W3 levels] %s", result["w3_level_note"])
    return result


def tag_strategy(
    gex_regime: str,       # raw regime label from compute_gex e.g. "LONG GAMMA · stable (spot above flip)"
    lean: str,             # 'bullish' | 'bearish' | 'neutral'
    gamma_flip: Optional[float],
    call_wall: Optional[float],
    put_wall: Optional[float],
) -> dict:
    """
    Map GEX regime + flow lean → strategy tag.
    Returns dict with w3_strategy_tag, w3_gex_regime, w3_gamma_flip, w3_nearest_wall.
    """
    regime_upper = (gex_regime or "").upper()
    lean_lower   = (lean or "neutral").lower()

    # Parse regime
    if "LONG GAMMA" in regime_upper:
        gamma_regime = "LONG_GAMMA"
    elif "SHORT GAMMA" in regime_upper:
        gamma_regime = "SHORT_GAMMA"
    else:
        gamma_regime = "NEUTRAL"

    # Map lean
    if lean_lower == "bullish":
        lean_tag = "BULL"
    elif lean_lower == "bearish":
        lean_tag = "BEAR"
    else:
        lean_tag = "NEUTRAL"

    # Build tag
    tag_map = {
        ("LONG_GAMMA",  "BULL"):    TAG_LONG_GAMMA_BULL,
        ("LONG_GAMMA",  "BEAR"):    TAG_LONG_GAMMA_BEAR,
        ("LONG_GAMMA",  "NEUTRAL"): TAG_LONG_GAMMA_NEUTRAL,
        ("SHORT_GAMMA", "BULL"):    TAG_SHORT_GAMMA_BULL,
        ("SHORT_GAMMA", "BEAR"):    TAG_SHORT_GAMMA_BEAR,
        ("SHORT_GAMMA", "NEUTRAL"): TAG_SHORT_GAMMA_NEUTRAL,
        ("NEUTRAL",     "BULL"):    TAG_LONG_GAMMA_BULL,
        ("NEUTRAL",     "BEAR"):    TAG_LONG_GAMMA_BEAR,
        ("NEUTRAL",     "NEUTRAL"): TAG_UNKNOWN,
    }
    tag = tag_map.get((gamma_regime, lean_tag), TAG_UNKNOWN)

    # Nearest wall
    walls = [w for w in [call_wall, put_wall] if w]
    nearest_wall = min(walls) if walls else None

    return {
        "w3_strategy_tag": tag,
        "w3_gex_regime":   gex_regime or "UNKNOWN",
        "w3_gamma_flip":   gamma_flip,
        "w3_nearest_wall": nearest_wall,
    }
