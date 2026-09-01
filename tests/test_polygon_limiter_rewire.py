"""tests/test_polygon_limiter_rewire.py — HM-POLYGON-LIMITER-REWIRE-2026-09-01.

The Polygon rate limiter's shadow wiring first landed in
engine/gamma_context.py (bda14e5) -- the wrong chokepoint. Moved to
engine/market_data.py::get_intraday_candles, the function actually
generating the 429 storm (37,174 Polygon 429s in one day, 15+
uncoordinated callers). gamma_context.py's wiring was reverted to a plain
direct call (see tests/test_gamma_context_rate_limiter_wiring.py, deleted
-- its subject no longer exists there).

These tests cover the new wiring's guarantees:
1. POLYGON_LIMITER_MODE unset (default "off") is still a byte-for-byte
   passthrough through get_intraday_candles -- deploying this rewiring
   alone changes zero live behavior, same guarantee as before.
2. BudgetExhausted (enforce mode only, not enabled) is caught at the call
   site and degrades to the existing Alpaca-fallback path, never
   propagates.
3. Shadow mode, when explicitly forced on for a test, actually observes
   calls through get_intraday_candles now -- proving the "wrong function"
   problem is fixed (gamma_context.py's version, if it had stayed wired,
   would show a clean shadow report while this real storm ran untouched).

Never makes a real network call -- Polygon and Alpaca are both mocked/
disabled per test.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import engine.market_data as market_data  # noqa: E402
from engine.market_data import get_intraday_candles  # noqa: E402
from engine.polygon_rate_limiter import MODE_ENV_VAR, _limiter  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_state(monkeypatch):
    """Reset the market_data cooldown, the shared Polygon limiter's
    internal state, and force mode back to 'off' after every test --
    these are all module-level/singleton state that would otherwise leak
    between tests."""
    market_data._polygon_limited_until = 0
    market_data._polygon_cooldown_logged = False
    monkeypatch.delenv(MODE_ENV_VAR, raising=False)
    with _limiter._lock:
        _limiter._live_tokens = _limiter.live_reserved_per_min
        _limiter._shared_tokens = _limiter.shared_per_min
        _limiter._cache = {}
        _limiter._shadow_stats = {
            "total": 0, "would_throttle": 0, "would_fail_loud": 0,
            "would_serve_stale": 0, "by_caller_fail_loud": {},
        }
    yield
    market_data._polygon_limited_until = 0
    market_data._polygon_cooldown_logged = False


def _no_alpaca_creds(monkeypatch):
    monkeypatch.delenv("APCA_API_KEY_ID", raising=False)
    monkeypatch.delenv("APCA_API_SECRET_KEY", raising=False)


def _polygon_bar(dt: datetime, price: float):
    return {"t": int(dt.timestamp() * 1000), "o": price, "h": price + 1,
            "l": price - 1, "c": price, "v": 1000}


@pytest.fixture(autouse=True)
def _no_real_yahoo_call():
    with patch.object(market_data, "_yahoo_chart", return_value=None):
        yield


def test_off_mode_passthrough_unchanged(monkeypatch):
    """Default mode (MODE_ENV_VAR unset) must be byte-for-byte identical
    to calling Polygon directly -- fetch runs exactly once, real data
    returned, no limiter side effects."""
    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    now = datetime.now(timezone.utc)
    calls = {"n": 0}

    def _fake_get(url, timeout=None):
        calls["n"] += 1
        resp = Mock()
        resp.status_code = 200
        resp.json.return_value = {"results": [_polygon_bar(now, 100.0)]}
        return resp

    assert os.environ.get(MODE_ENV_VAR, "off") == "off"
    with patch("requests.get", side_effect=_fake_get):
        candles = get_intraday_candles("SPY", interval="1m", range_="1d")

    assert calls["n"] == 1
    assert len(candles) == 1
    assert candles[0]["close"] == 100.0
    report = _limiter.shadow_report()
    assert report["total"] == 0, "off mode must not touch shadow accounting at all"


def test_budget_exhausted_caught_falls_back_to_alpaca(monkeypatch):
    """Simulates enforce mode (not live today, but the call site must be
    forward-correct) exhausting the budget -- must be caught here and
    degrade to the Alpaca fallback, never propagate out of
    get_intraday_candles."""
    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    monkeypatch.setenv(MODE_ENV_VAR, "enforce")
    _no_alpaca_creds(monkeypatch)  # forces a clean, fast fallback failure

    # Drain the budget entirely so gated_call raises BudgetExhausted.
    with _limiter._lock:
        _limiter._live_tokens = 0
        _limiter._shared_tokens = 0

    with patch.object(market_data, "_is_polygon_limited", return_value=False), \
         patch("engine.polygon_rate_limiter._limiter._market_hours_fn", return_value=True):
        result = get_intraday_candles("SPY", interval="1m", range_="1d")

    assert result == []  # exhausted everywhere -- honest empty, not fabricated


def test_shadow_mode_observes_calls_through_get_intraday_candles(monkeypatch):
    """The actual point of the rewire: with shadow mode forced on, calls
    through get_intraday_candles must now show up in the limiter's shadow
    report. Before this fix, this function was invisible to the limiter
    entirely (it was only wired into gamma_context.py)."""
    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    monkeypatch.setenv(MODE_ENV_VAR, "shadow")
    now = datetime.now(timezone.utc)

    def _fake_get(url, timeout=None):
        resp = Mock()
        resp.status_code = 200
        resp.json.return_value = {"results": [_polygon_bar(now, 100.0)]}
        return resp

    with patch("requests.get", side_effect=_fake_get):
        get_intraday_candles("SPY", interval="1m", range_="1d")

    report = _limiter.shadow_report()
    assert report["total"] == 1
    assert report["mode"] == "shadow"


def test_get_intraday_candles_is_in_live_callers():
    """The caller_name used at the call site must actually be registered
    as a live-tier caller -- otherwise shadow mode would silently treat
    this as cached-tier and never report would_fail_loud, understating
    the real overshoot the Captain asked to be able to represent."""
    from engine.polygon_rate_limiter import LIVE_CALLERS
    assert "get_intraday_candles" in LIVE_CALLERS


def test_cooldown_check_runs_before_offering_call_to_limiter(monkeypatch):
    """A call already known to be doomed (HM-429-BACKOFF cooldown active)
    must never even reach the limiter -- both mechanisms must compose,
    not double up or conflict."""
    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    monkeypatch.setenv(MODE_ENV_VAR, "shadow")
    market_data._set_polygon_limited()

    with patch("requests.get") as _mock_get:
        get_intraday_candles("SPY", interval="1m", range_="1d")
        _mock_get.assert_not_called()

    report = _limiter.shadow_report()
    assert report["total"] == 0, "cooldown-skipped calls must never reach the limiter"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
