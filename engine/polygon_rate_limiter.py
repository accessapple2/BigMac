"""engine/polygon_rate_limiter.py — Polygon rate limiter, HM-429-REMEDIATION-C design.

NAMING NOTE: the original 429-remediation directive named this file
"engine/rate_limiter.py" -- that path was already taken by a real, existing,
partially-adopted Alpaca limiter (engine/rate_limiter.py::AlpacaRateLimiter,
imported by engine/deep_scan.py and engine/strategy_rotator.py). Caught this
only after briefly overwriting that file with an early draft of this one
and restoring it from git before anything was staged/committed -- no data
was lost, but flagging the near-miss plainly rather than pretending the
directive's suggested filename was simply "corrected" here.

DESIGN ONLY -- NOT WIRED IN. No caller (in engine/, scripts/, or anywhere
else) imports this module yet. Deploying this file changes nothing by
itself; wiring gamma_context.py etc. through it, and actually setting
POLYGON_LIMITER_MODE to anything other than "off", are both separate,
later decisions.

Scope (per the Admiral-approved engine/ triage,
QUESTION_polygon-rate-limiter-bench-gate-list.md): this covers the 15
engine/ callers only. scripts/ and the other 19 files (swingdesk/, etc.)
are NOT accounted for by this limiter's budget -- they remain unmanaged
load on the same underlying 5-calls/min Polygon free-tier quota. That is
exactly why the managed cap here is 4/min, not 5: one call/min of slack is
deliberately left for what this limiter doesn't yet control. Migrating
those files onto this limiter (raising its effective coverage, not its cap)
is future work, scoped separately per the same review.

Live tier (can't tolerate stale GEX -- Admiral-approved list):
  gamma_context, options_pricing, paper_trader, bk_orb_scanner,
  squeeze_scanner, ollie_machine_universe

Everything else that would eventually route through this limiter is the
cached tier: it can be served a stale on-disk cache value freely, never
raises BudgetExhausted, and degrades to None (matching
PolygonData._get()'s existing failure behavior) rather than skip-loud when
there's truly nothing cached.

Usage (once actually wired into a call site -- not done yet):

    from engine.polygon_rate_limiter import gated_call, BudgetExhausted
    try:
        data = gated_call("gamma_context", f"snapshot:{symbol}",
                           lambda: polygon.get_snapshot(symbol))
    except BudgetExhausted:
        log.warning(f"{symbol}: GEX unavailable this cycle, skipping")
        return None  # the caller's OWN skip-this-cycle behavior, not ours
"""
from __future__ import annotations

from pathlib import Path

from engine.tiered_rate_limiter import TieredRateLimiter, BudgetExhausted  # noqa: F401 (re-exported)

LIVE_CALLERS = {
    "gamma_context",
    "options_pricing",
    "paper_trader",
    "bk_orb_scanner",
    "squeeze_scanner",
    "ollie_machine_universe",
}

# 4/min managed cap (deliberately under Polygon's real 5/min free-tier limit
# -- see module docstring). 2 reserved exclusively for the live tier, 2
# shared. Tune here, not by editing tiered_rate_limiter.py.
CAP_PER_MIN = 4
LIVE_RESERVED_PER_MIN = 2

# Options/GEX data older than this during market hours is treated as unusable
# for the live tier -- fail loud rather than trade on it. A single blanket
# threshold for a first pass; if options_pricing.py (tight, per-fill
# freshness) and ollie_machine_universe.py (a filter, changes more slowly)
# turn out to need different thresholds, split this into a per-caller dict
# then -- not speculatively now.
LIVE_MAX_STALE_SECS = 30

CACHE_PATH = str(Path(__file__).resolve().parent.parent / "data" / "polygon_limiter_cache.json")
MODE_ENV_VAR = "POLYGON_LIMITER_MODE"  # off (default) | shadow | enforce

_limiter = TieredRateLimiter(
    name="polygon",
    live_callers=LIVE_CALLERS,
    cap_per_min=CAP_PER_MIN,
    live_reserved_per_min=LIVE_RESERVED_PER_MIN,
    live_max_stale_secs=LIVE_MAX_STALE_SECS,
    cache_path=CACHE_PATH,
    mode_env_var=MODE_ENV_VAR,
)


def gated_call(caller_name: str, cache_key: str, fetch_fn):
    return _limiter.gated_call(caller_name, cache_key, fetch_fn)


def shadow_report() -> dict:
    return _limiter.shadow_report()
