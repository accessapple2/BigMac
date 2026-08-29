"""HM-429-REMEDIATION-C/B — tests for engine/tiered_rate_limiter.py's four
required behaviors: kill switch (off = passthrough), reserved live budget,
fail-loud on budget+cache exhaustion during market hours, and shadow mode
changing no real behavior while still logging what enforcement would do.

This module is a design, not wired into any live caller yet -- these tests
exercise the limiter in isolation, not any real gamma_context/paper_trader
call site.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.tiered_rate_limiter import (  # noqa: E402
    BudgetExhausted,
    LimiterMode,
    TieredRateLimiter,
)


class _Limiter(TieredRateLimiter):
    """Test subclass that overrides `mode` to a fixed value instead of
    reading an env var, so tests don't need to mutate process environment."""
    def __init__(self, fixed_mode, **kw):
        self._fixed_mode = fixed_mode
        self.alerts = []
        kw.setdefault("alert_fn", lambda t, m: self.alerts.append((t, m)))
        super().__init__(**kw)

    @property
    def mode(self):
        return self._fixed_mode


def _limiter(tmp_path, mode, market_hours=True, cap=4, live_reserved=2,
             live_max_stale=30):
    return _Limiter(
        mode,
        name="test",
        live_callers={"live_caller"},
        cap_per_min=cap,
        live_reserved_per_min=live_reserved,
        live_max_stale_secs=live_max_stale,
        cache_path=str(tmp_path / "cache.json"),
        mode_env_var="TEST_LIMITER_MODE_XYZ",
        market_hours_fn=lambda: market_hours,
    )


# ── (3) kill switch: off = pure passthrough ─────────────────────────────────

def test_off_mode_is_pure_passthrough(tmp_path):
    lim = _limiter(tmp_path, LimiterMode.OFF, cap=1, live_reserved=0)
    calls = {"n": 0}
    def fetch():
        calls["n"] += 1
        return {"ok": True}
    for _ in range(10):  # far exceeds cap=1 -- must never throttle in OFF
        result = lim.gated_call("live_caller", "k", fetch)
        assert result == {"ok": True}
    assert calls["n"] == 10, "OFF mode must call fetch_fn every time, no caching, no budget"
    assert lim._cache == {}, "OFF mode must not populate the cache either"


# ── (1) reserved live budget + shared pool ──────────────────────────────────

def test_live_tier_has_reserved_budget_shared_tier_does_not(tmp_path):
    lim = _limiter(tmp_path, LimiterMode.ENFORCE, cap=4, live_reserved=2)
    # Exhaust the shared pool (2 tokens) with cached-tier calls.
    for i in range(2):
        lim.gated_call("cached_caller", f"shared{i}", lambda: {"v": 1})
    # Shared pool now empty. A 3rd cached-tier call has no cache and no
    # budget -- must degrade to None, not raise (cached tier never fail-louds).
    assert lim.gated_call("cached_caller", "shared_new", lambda: {"v": 1}) is None
    # Live tier still has its OWN reserved 2 tokens, untouched by the above.
    r1 = lim.gated_call("live_caller", "live0", lambda: {"v": "a"})
    r2 = lim.gated_call("live_caller", "live1", lambda: {"v": "b"})
    assert r1 == {"v": "a"} and r2 == {"v": "b"}


def test_live_tier_can_overflow_into_shared_pool(tmp_path):
    lim = _limiter(tmp_path, LimiterMode.ENFORCE, cap=4, live_reserved=1)
    # 1 reserved for live + 3 shared = live can get up to 4 total if shared
    # is untouched by anyone else.
    results = [lim.gated_call("live_caller", f"k{i}", lambda: i) for i in range(4)]
    assert results == [0, 1, 2, 3]


# ── (2) fail-loud: budget exhausted + cache stale/missing during market hours ─

def test_live_tier_fails_loud_when_exhausted_and_uncached_in_market_hours(tmp_path):
    lim = _limiter(tmp_path, LimiterMode.ENFORCE, cap=0, live_reserved=0,
                    market_hours=True)
    with pytest.raises(BudgetExhausted):
        lim.gated_call("live_caller", "no_cache_key", lambda: {"should": "not run"})
    assert len(lim.alerts) == 1, "fail-loud must alert exactly once per occurrence"
    assert "live_caller" in lim.alerts[0][1]


def test_live_tier_serves_cache_within_freshness_window(tmp_path):
    lim = _limiter(tmp_path, LimiterMode.ENFORCE, cap=1, live_reserved=1,
                    live_max_stale=30)
    lim.gated_call("live_caller", "k", lambda: {"fresh": True})  # populates cache, uses the 1 token
    # Budget now exhausted, but cache is fresh (age ~0s < 30s) -- must serve it.
    result = lim.gated_call("live_caller", "k", lambda: {"should": "not run"})
    assert result == {"fresh": True}


def test_live_tier_fails_loud_on_stale_cache_in_market_hours(tmp_path):
    lim = _limiter(tmp_path, LimiterMode.ENFORCE, cap=0, live_reserved=0,
                    live_max_stale=0.05, market_hours=True)
    lim._cache["k"] = {"ts": time.time() - 1.0, "data": {"stale": True}}  # 1s old, max is 0.05s
    with pytest.raises(BudgetExhausted):
        lim.gated_call("live_caller", "k", lambda: {"should": "not run"})


def test_stale_cache_outside_market_hours_is_served_not_fail_loud(tmp_path):
    lim = _limiter(tmp_path, LimiterMode.ENFORCE, cap=0, live_reserved=0,
                    live_max_stale=0.05, market_hours=False)
    lim._cache["k"] = {"ts": time.time() - 999, "data": {"very_stale": True}}
    result = lim.gated_call("live_caller", "k", lambda: {"should": "not run"})
    assert result == {"very_stale": True}, \
        "outside market hours, even very stale cache is fine -- no live decision is being made"


def test_cached_tier_never_fails_loud(tmp_path):
    lim = _limiter(tmp_path, LimiterMode.ENFORCE, cap=0, live_reserved=0,
                    market_hours=True)
    # Cached tier with zero budget and no cache degrades to None, never raises.
    result = lim.gated_call("cached_caller", "k", lambda: {"should": "not run"})
    assert result is None
    assert lim.alerts == [], "cached tier must never trigger the fail-loud alert"


def test_failed_fetch_is_not_cached(tmp_path):
    """A None result (the existing PolygonData._get() failure convention)
    must not poison the cache as if it were a good value."""
    lim = _limiter(tmp_path, LimiterMode.ENFORCE, cap=5, live_reserved=5)
    lim.gated_call("live_caller", "k", lambda: None)
    assert "k" not in lim._cache


# ── (4) shadow mode: real behavior unchanged, decisions logged/counted ──────

def test_shadow_mode_never_changes_real_behavior(tmp_path):
    lim = _limiter(tmp_path, LimiterMode.SHADOW, cap=0, live_reserved=0,
                    market_hours=True)
    calls = {"n": 0}
    def fetch():
        calls["n"] += 1
        return {"real": "data"}
    # Would fail-loud under ENFORCE (cap=0) -- but SHADOW must still call
    # fetch_fn for real, every time, and return its real result.
    for _ in range(5):
        result = lim.gated_call("live_caller", "k", fetch)
        assert result == {"real": "data"}
    assert calls["n"] == 5
    assert lim.alerts == [], "shadow mode must never actually fire the fail-loud alert"


def test_shadow_mode_reports_what_enforcement_would_have_done(tmp_path):
    lim = _limiter(tmp_path, LimiterMode.SHADOW, cap=0, live_reserved=0,
                    market_hours=True)
    for i in range(3):
        lim.gated_call("live_caller", f"k{i}", lambda: {"v": i})
    report = lim.shadow_report()
    assert report["mode"] == "shadow"
    assert report["total"] == 3
    assert report["would_throttle"] == 3        # cap=0 -- every call is over budget
    assert report["would_fail_loud"] == 3        # and every one is live+market-hours+no-cache
    assert report["by_caller_fail_loud"]["live_caller"] == 3


def test_shadow_mode_distinguishes_would_serve_stale_from_would_fail_loud(tmp_path):
    lim = _limiter(tmp_path, LimiterMode.SHADOW, cap=0, live_reserved=0,
                    live_max_stale=999, market_hours=True)
    lim._cache["k"] = {"ts": time.time(), "data": {"cached": True}}
    lim.gated_call("live_caller", "k", lambda: {"real": True})
    report = lim.shadow_report()
    assert report["would_throttle"] == 1
    assert report["would_serve_stale"] == 1     # fresh-enough cache exists -- would NOT fail loud
    assert report["would_fail_loud"] == 0
