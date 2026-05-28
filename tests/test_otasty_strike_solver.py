"""SC-7 #2 — regression test for the strike-solver inverted put-branch (97b18d4).

The bug (shipped once): find_strike_for_delta's put branch had the binary-search
comparison inverted, driving the search to the S*1.5 upper bound — producing
put strikes FAR ABOVE spot and negative iron-condor max_loss. The fix anchored
the put strike below spot at the target delta. This pins it so it can't recur.
"""
import sys
from pathlib import Path

_SWINGDESK = Path(__file__).resolve().parent.parent / "swingdesk"
if str(_SWINGDESK) not in sys.path:
    sys.path.insert(0, str(_SWINGDESK))

from options_engine import find_strike_for_delta  # noqa: E402

_S = 100.0          # spot
_T = 30 / 365       # 30 DTE
_SIGMA = 0.30       # 30% IV
_DELTA = 0.20       # 20-delta target


def test_put_strike_lands_below_spot():
    """A 20-delta OTM put must be BELOW spot — never pinned to the S*1.5 bound."""
    k = find_strike_for_delta(_S, _T, _SIGMA, _DELTA, opt_type="put")
    assert k < _S, f"20Δ put strike {k} should be < spot {_S} (regression: was ~S*1.5)"
    assert k > _S * 0.50, f"put strike {k} pinned to lower bound — solver not converging"
    assert k < _S * 1.40, f"put strike {k} near S*1.5 upper bound — the inverted-branch bug"


def test_call_strike_lands_above_spot():
    """Symmetric sanity: a 20-delta OTM call must be ABOVE spot."""
    k = find_strike_for_delta(_S, _T, _SIGMA, _DELTA, opt_type="call")
    assert k > _S, f"20Δ call strike {k} should be > spot {_S}"
    assert k < _S * 1.50, f"call strike {k} pinned to upper bound — solver not converging"


def test_put_strike_monotonic_in_delta():
    """Higher target delta (closer to ATM) → put strike closer to spot."""
    k_low = find_strike_for_delta(_S, _T, _SIGMA, 0.10, opt_type="put")   # far OTM
    k_high = find_strike_for_delta(_S, _T, _SIGMA, 0.35, opt_type="put")  # nearer ATM
    assert k_low < k_high < _S, (
        f"expected far-OTM {k_low} < near-ATM {k_high} < spot {_S} "
        "(put strike should rise toward spot as delta rises)"
    )
