"""tests/test_gex_scanner_wall_labels.py -- HM-GEX-WALL-LABEL-FIX-2026-09-12.

engine/gex_scanner.py (CBOE) used to label a magnet strike call_wall/put_wall
by the SIGN of its net GEX, with no constraint on which side of spot it was
on -- a strike below spot with positive net GEX got called "call_wall" even
though every other consumer in this codebase (ready_room.py, gex_calculator.py,
options_flow_gex.py) requires call_wall to mean "above spot, resistance" and
put_wall "below spot, support". This is the exact defect named in
dashboard/app.py:6725's 2026-06-09 comment (HM-DRYDOCK A1) as the reason CBOE
was routed around once already -- it was never actually fixed, just avoided.

Fixed: label by position relative to spot instead of sign. This test proves
the fix with the exact failure shape the old code got wrong -- a strike below
spot carrying POSITIVE net GEX -- and would fail against the old
`"call_wall" if net_gex > 0 else "put_wall"` rule.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import engine.gex_scanner as gex_scanner  # noqa: E402


def _cboe_raw(spot: float, options: list) -> dict:
    return {"data": {"current_price": spot, "options": options}}


def _occ(ticker: str, is_call: bool, strike: float) -> str:
    cp = "C" if is_call else "P"
    return f"{ticker}260101{cp}{int(strike * 1000):08d}"


def test_below_spot_strike_with_positive_net_gex_is_put_wall_not_call_wall():
    """The exact HM-DRYDOCK A1 failure shape: a strike below spot whose net
    GEX happens to be positive. Old rule (sign-based) would call this
    call_wall -- wrong side of spot, backwards from what every consumer
    expects. Fixed rule (position-based) must call it put_wall."""
    spot = 100.0
    options = [
        # Below spot (95), heavy call OI -> large POSITIVE net GEX at a
        # strike that is nonetheless below spot.
        {"option": _occ("SPY", True, 95.0), "gamma": 0.05, "open_interest": 10000},
        {"option": _occ("SPY", False, 95.0), "gamma": 0.01, "open_interest": 100},
        # Above spot (105), a smaller genuine call-side magnet for contrast.
        {"option": _occ("SPY", True, 105.0), "gamma": 0.02, "open_interest": 2000},
        {"option": _occ("SPY", False, 105.0), "gamma": 0.01, "open_interest": 100},
    ]
    raw = _cboe_raw(spot, options)
    parsed = gex_scanner._parse_gex(raw, "SPY")
    assert parsed is not None

    below_spot_magnet = next(m for m in parsed["magnets"] if m["strike"] == 95.0)
    assert below_spot_magnet["net_gex"] > 0, "test setup: this strike must have positive net GEX"
    assert below_spot_magnet["type"] == "put_wall", (
        "a strike below spot must be labeled put_wall regardless of net-GEX sign"
    )

    above_spot_magnet = next(m for m in parsed["magnets"] if m["strike"] == 105.0)
    assert above_spot_magnet["type"] == "call_wall"


def test_prompt_section_labels_match_position_not_backwards_text():
    """build_gex_prompt_section()'s labels used to say the opposite of every
    other consumer's convention (call wall = 'support/pin', put wall =
    'resistance/accelerator'). Must now say resistance/support, matching
    ready_room.py and gex_calculator.py."""
    fake_gex = {
        "spot": 100.0, "total_gex": 500.0,
        "magnets": [{"strike": 105.0, "net_gex": 500.0, "type": "call_wall", "tier": "primary"}],
        "secondary_levels": [{"strike": 95.0, "net_gex": -300.0, "type": "put_wall", "tier": "secondary"}],
    }
    with patch.object(gex_scanner, "get_gex", return_value=fake_gex):
        text = gex_scanner.build_gex_prompt_section("SPY")
    assert "CALL WALL (resistance)" in text
    assert "put wall (support)" in text
    assert "support/pin" not in text
    assert "resistance/accelerator" not in text
