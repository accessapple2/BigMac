"""tests/test_gamma_context_rate_limiter_wiring.py — HM-POLYGON-LIMITER-SHADOW-2026-09-01.

engine/gamma_context.py::get_gamma_context() is the first live call site
wired through engine/polygon_rate_limiter.py (previously a design-only
module, not imported anywhere). Two guarantees this file protects:

1. Default (POLYGON_LIMITER_MODE unset/"off") behavior is unchanged --
   deploying the wiring alone must not alter what get_gamma_context()
   returns or how many times the underlying fetch runs.
2. BudgetExhausted (only possible in "enforce" mode, not enabled by this
   change) is caught at the call site and degrades to the same
   available=False path the function already uses for "chain unavailable" --
   per engine/tiered_rate_limiter.py's own contract, callers MUST catch
   this specifically rather than let it propagate.

Mocks engine.gamma_context._polygon_snapshot directly -- no real Polygon
HTTP calls in this file.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import engine.gamma_context as gamma_context  # noqa: E402
from engine.polygon_rate_limiter import BudgetExhausted  # noqa: E402


def _fresh_cache():
    """get_gamma_context has its own module-level in-memory _CACHE (separate
    from the rate limiter's cache) -- clear it so each test starts cold."""
    gamma_context._CACHE.clear()


def test_off_mode_behavior_unchanged():
    """Default mode (no POLYGON_LIMITER_MODE set) must be a byte-for-byte
    passthrough -- same result, fetch called exactly once."""
    _fresh_cache()
    calls = {"n": 0}

    def fake_snapshot(ticker):
        calls["n"] += 1
        return [{"strike": 100.0, "type": "call", "expiry": "2026-12-19",
                  "oi": 10, "gamma": 0.01, "iv": 0.3, "_spot": 100.0}]

    with patch.object(gamma_context, "_polygon_snapshot", side_effect=fake_snapshot):
        ctx = gamma_context.get_gamma_context("TEST", use_cache=False)

    assert calls["n"] == 1
    assert ctx.available is True


def test_budget_exhausted_degrades_to_unavailable_not_raised():
    """A BudgetExhausted from the limiter (enforce mode only -- not enabled
    today, but the call site must be forward-correct) must be caught here,
    never propagate out of get_gamma_context()."""
    _fresh_cache()

    def raise_exhausted(caller_name, cache_key, fetch_fn):
        raise BudgetExhausted("test: budget exhausted")

    with patch("engine.polygon_rate_limiter.gated_call", side_effect=raise_exhausted):
        ctx = gamma_context.get_gamma_context("TEST", use_cache=False)

    assert ctx.available is False
    assert ctx.note == "chain unavailable"


def test_limiter_import_failure_falls_back_to_direct_call():
    """If the rate limiter module itself is ever broken, gamma grounding
    must not go down with it -- falls back to the pre-wiring direct call."""
    _fresh_cache()
    calls = {"n": 0}

    def fake_snapshot(ticker):
        calls["n"] += 1
        return [{"strike": 100.0, "type": "call", "expiry": "2026-12-19",
                  "oi": 10, "gamma": 0.01, "iv": 0.3, "_spot": 100.0}]

    def raise_import_error(*a, **kw):
        raise RuntimeError("simulated limiter failure")

    with patch.object(gamma_context, "_polygon_snapshot", side_effect=fake_snapshot), \
         patch("engine.polygon_rate_limiter.gated_call", side_effect=raise_import_error):
        ctx = gamma_context.get_gamma_context("TEST", use_cache=False)

    assert calls["n"] == 1, "must fall back to a direct _polygon_snapshot call"
    assert ctx.available is True
