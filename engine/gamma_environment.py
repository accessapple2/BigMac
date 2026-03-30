"""Gamma Environment Detector — determine if SPY is in positive or negative gamma territory.

Positive gamma: dealers are long gamma → they sell rallies and buy dips → dampens moves (mean-reverting)
Negative gamma: dealers are short gamma → they chase moves → amplifies volatility (trending)
"""
from __future__ import annotations
import threading
import time
from rich.console import Console

console = Console()

_cache: dict = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 300  # 5 minutes


def detect_gamma_environment() -> dict:
    """Detect whether SPY is in positive or negative gamma territory.

    Uses the GEX scanner's total net GEX for SPY.
    Positive total GEX → positive gamma → dampened moves
    Negative total GEX → negative gamma → amplified moves
    """
    with _cache_lock:
        if _cache.get("data") and time.time() - _cache.get("ts", 0) < _CACHE_TTL:
            return _cache["data"]

    try:
        from engine.gex_scanner import get_gex

        spy_gex = get_gex("SPY")
        if not spy_gex:
            return {"environment": "unknown", "total_gex": 0, "sizing_factor": 1.0}

        total_gex = spy_gex.get("total_gex", 0)
        magnets = spy_gex.get("magnets", [])
        spot = spy_gex.get("spot", 0)

        # Count call walls vs put walls
        call_walls = sum(1 for m in magnets if m["type"] == "call_wall")
        put_walls = sum(1 for m in magnets if m["type"] == "put_wall")

        # Determine environment
        if total_gex > 0:
            environment = "positive"
            label = "POSITIVE GAMMA"
            description = "Dealers long gamma — moves dampened, mean-reverting. DayBlade can size up."
            sizing_factor = 1.5  # DayBlade can be more aggressive
            color = "#3fb950"
        else:
            environment = "negative"
            label = "NEGATIVE GAMMA"
            description = "Dealers short gamma — moves amplified, trending. DayBlade sizes down 50%."
            sizing_factor = 0.5  # DayBlade should be cautious
            color = "#f85149"

        # Intensity based on magnitude
        abs_gex = abs(total_gex)
        if abs_gex > 1_000_000:
            intensity = "extreme"
        elif abs_gex > 500_000:
            intensity = "strong"
        elif abs_gex > 100_000:
            intensity = "moderate"
        else:
            intensity = "weak"

        # Find the gamma flip level (where net GEX changes sign)
        gamma_flip = None
        strikes = spy_gex.get("strikes", [])
        if strikes and spot:
            # Walk strikes near spot, find where net_gex crosses zero
            near_strikes = [s for s in strikes if abs(s["strike"] - spot) / spot < 0.03]
            for i in range(len(near_strikes) - 1):
                if near_strikes[i]["net_gex"] * near_strikes[i + 1]["net_gex"] < 0:
                    gamma_flip = round(
                        (near_strikes[i]["strike"] + near_strikes[i + 1]["strike"]) / 2, 2
                    )
                    break

        result = {
            "environment": environment,
            "label": label,
            "description": description,
            "total_gex": total_gex,
            "sizing_factor": sizing_factor,
            "intensity": intensity,
            "color": color,
            "spot": spot,
            "gamma_flip": gamma_flip,
            "call_walls": call_walls,
            "put_walls": put_walls,
            "magnets": magnets,
        }

        with _cache_lock:
            _cache["data"] = result
            _cache["ts"] = time.time()

        return result

    except Exception as e:
        console.log(f"[red]Gamma environment error: {e}")
        return {"environment": "unknown", "total_gex": 0, "sizing_factor": 1.0}


def get_dayblade_sizing_factor() -> float:
    """Get the DayBlade position sizing multiplier based on gamma environment."""
    env = detect_gamma_environment()
    return env.get("sizing_factor", 1.0)
