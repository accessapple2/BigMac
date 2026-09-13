"""tests/test_gex_dex.py -- HM-DEX-2026-09-12.

DEX (delta exposure) computed alongside GEX from the same Alpaca options
snapshot -- delta is on the same `greeks` object gamma comes from, so this
is a same-loop, zero-new-fetch addition. Covers the pure per-contract
formula and that GEXLevel/GEXProfile carry it with safe defaults (no
existing caller that doesn't know about DEX breaks).
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from gex_calculator import GEXCalculator, GEXLevel, GEXProfile  # noqa: E402


def test_dex_contrib_call_is_positive():
    # delta=0.5, oi=100, spot=200 -> 0.5*100*100*200
    assert GEXCalculator._dex_contrib(0.5, 100, 200.0) == 1_000_000.0


def test_dex_contrib_put_is_negative():
    """Puts carry negative delta naturally -- no separate sign flip needed,
    unlike _gex_contrib which explicitly negates for puts."""
    assert GEXCalculator._dex_contrib(-0.3, 100, 200.0) == -600_000.0


def test_gex_level_defaults_dex_fields_to_zero():
    """A GEXLevel built the old way (positional GEX fields only) must not
    break -- DEX fields default safely."""
    lv = GEXLevel(strike=100.0, net_gex=1.0, call_gex=2.0, put_gex=-1.0)
    assert lv.call_dex == 0.0
    assert lv.put_dex == 0.0
    assert lv.net_dex == 0.0


def test_gex_profile_defaults_total_dex_to_zero():
    profile = GEXProfile(
        symbol="SPY", spot_price=100.0, timestamp="t", levels=[],
        max_gamma_strike=100.0, zero_gamma_level=100.0,
        put_wall=95.0, call_wall=105.0, gamma_flip=100.0, total_gex=0.0,
    )
    assert profile.total_dex == 0.0
