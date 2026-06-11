"""
SWINGDESK-W2 — saved manual-spread blueprints + runtime resolver.

A blueprint stores the *shape* of a spread (structure, leg roles, DTE band,
strike rules) but NOT concrete strikes/expiry — those are resolved at submit
time from live spot, so a blueprint never goes stale (the CEG hedge's original
Jul-18 strikes are the cautionary tale).

resolve_blueprint() turns a blueprint into the leg list spread_executor expects:
  [{underlying, expiration, option_type, strike, side}, ...]
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

_DIR = Path(__file__).resolve().parent
_FIXTURES = _DIR / "fixtures"
sys.path.insert(0, str(_DIR))  # options_engine is a sibling module


def load_blueprint(name: str) -> dict:
    """Load a saved blueprint JSON by name (e.g. 'ceg_hedge')."""
    path = _FIXTURES / f"{name}.json"
    if not path.exists():
        raise FileNotFoundError(f"no blueprint: {path}")
    return json.loads(path.read_text())


def _round_to_increment(price: float, inc: float) -> float:
    return round(round(price / inc) * inc, 2)


def resolve_blueprint(bp: dict, spot: Optional[float] = None,
                      expiration: Optional[str] = None) -> dict:
    """Resolve a blueprint to concrete legs using live spot + a 30-45 DTE expiry.

    Returns {underlying, structure, qty, expiration, spot, legs:[...]}.
    spot/expiration may be injected (tests); otherwise pulled from options_engine.
    """
    from options_engine import get_spot, find_target_expiration  # sibling
    underlying = bp["underlying"].upper()
    if spot is None:
        spot = get_spot(underlying)
    if not spot:
        raise ValueError(f"could not resolve spot for {underlying}")
    if expiration is None:
        expiration = find_target_expiration(underlying)
    if not expiration:
        raise ValueError(f"could not resolve a 30-45 DTE expiry for {underlying}")

    inc = float(bp.get("strike_increment", 5))
    atm = _round_to_increment(spot, inc)

    def _strike(leg_spec: dict) -> float:
        if leg_spec.get("moneyness") == "atm":
            return atm
        off = leg_spec.get("offset_dollars")
        if off is not None:
            return _round_to_increment(atm + float(off), inc)
        if leg_spec.get("strike") is not None:
            return float(leg_spec["strike"])
        raise ValueError(f"leg spec needs moneyness/offset_dollars/strike: {leg_spec}")

    legs = []
    for role in ("long_leg", "short_leg"):
        ls = bp[role]
        legs.append({
            "underlying": underlying,
            "expiration": expiration,
            "option_type": ls.get("option_type", "put"),
            "strike": _strike(ls),
            "side": ls["side"],
        })
    return {
        "underlying": underlying, "structure": bp.get("structure", "vertical_spread"),
        "qty": int(bp.get("qty", 1)), "expiration": expiration, "spot": spot,
        "legs": legs,
    }
