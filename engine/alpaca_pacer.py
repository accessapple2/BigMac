"""engine/alpaca_pacer.py — shared Alpaca data-API pacer, HM-429-REMEDIATION-B
design (companion to engine/polygon_rate_limiter.py's design, same review).

DESIGN ONLY -- NOT WIRED IN. Same posture as the Polygon design: no caller
imports this yet, default mode is "off" (pure passthrough), enabling is a
separate later decision.

IMPORTANT: a real Alpaca rate limiter ALREADY EXISTS --
engine/rate_limiter.py::AlpacaRateLimiter (the "Warp Core Governor"),
imported today by exactly two modules (engine/deep_scan.py,
engine/strategy_rotator.py) out of the ~20 files that actually call
Alpaca's data API. It documents Alpaca's real published limit -- 200
req/min, conservatively capped at 150 -- which is the number this design
uses below, replacing an earlier draft's ungrounded guess of 30/min. That
existing limiter is a blocking token bucket (`acquire()` waits up to 30s for
a token, then proceeds anyway) with NO tiering, NO fail-loud semantics, NO
cache, NO shadow mode, and NO kill switch -- none of the four requirements
this design implements. It also does nothing for the ~18 callers that don't
import it, which is exactly why red_alert_check() and friends still 429.

This design does NOT touch or replace that file. Recommending, not doing:
once this design is approved, `engine/rate_limiter.py`'s two callers should
probably migrate to this interface and the old blocking-wait class should
be retired, rather than running two different Alpaca-pacing philosophies
side by side indefinitely. That migration is a distinct, separate change
from standing up this design -- not attempted here.

Managed budget is 20/min (10 live-reserved + 10 shared), not the full
150/min the existing limiter targets -- mirroring the Polygon design's
logic: only migrating a handful of callers to start with, so most of the
real 200/min (150 conservative) ceiling stays available to the ~16
still-unmigrated callers making their own uncoordinated calls. Raise this
as more callers migrate onto this pacer, not before.

Tier list below is a FIRST-PASS PROPOSAL based on module docstrings only --
it has NOT been through the same review the Polygon engine/ list got
(QUESTION_polygon-rate-limiter-bench-gate-list.md). Treat it as a starting
point for that review, not an approved scope.

Preliminary live tier (execution-adjacent / real-time, by docstring):
  crew_scanner ("Master Signal Pipeline"), battle_station ("0DTE real-time
  tactical intelligence"), premium_tracker ("live SPY 0DTE options chain"),
  tick_recorder (real-time IEX tick recording feeding the event tape).

Preliminary cached tier: long_range_sensors (whale detection -- its own
alerts are self-labeled "shadow"/observation), volume_baselines (slow-
changing 20d rolling average, already properly chunked+paced),
volume_scanner (already has its own 429 retry now -- see
HM-429-REMEDIATION-B commit), data_ingestion (historical/ingestion),
total_portfolio (read-only external-account reporting), signal_evaluator
("observe-first" -- retrospective, not gating a live decision).

Not yet placed either way, needs its own look: market_data.py (shared
primitive many other modules call into -- may not belong in a per-caller
tier scheme the same way; more investigation needed before this list is
final), strategies/alpaca_chain_client.py (bull-spread construction --
plausibly execution-adjacent, not reviewed in depth), and
engine/deep_scan.py / engine/strategy_rotator.py (today's two adopters of
the OLD limiter -- need a tier assignment as part of their migration).
"""
from __future__ import annotations

from pathlib import Path

from engine.tiered_rate_limiter import TieredRateLimiter, BudgetExhausted  # noqa: F401 (re-exported)

LIVE_CALLERS = {
    "crew_scanner",
    "battle_station",
    "premium_tracker",
    "tick_recorder",
}

# Managed slice of Alpaca's real 200/min (150 conservative) limit -- see
# module docstring for why this starts small rather than claiming the whole
# budget while most callers are still unmigrated and uncoordinated.
CAP_PER_MIN = 20
LIVE_RESERVED_PER_MIN = 10

# Alpaca snapshot data older than this during market hours is unusable for
# the live tier. Snapshots move faster than options/GEX data, hence a
# tighter default than the Polygon limiter's 30s -- also a starting guess,
# to be tuned in shadow mode before enforcement.
LIVE_MAX_STALE_SECS = 10

CACHE_PATH = str(Path(__file__).resolve().parent.parent / "data" / "alpaca_pacer_cache.json")
MODE_ENV_VAR = "ALPACA_PACER_MODE"  # off (default) | shadow | enforce

_limiter = TieredRateLimiter(
    name="alpaca",
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
