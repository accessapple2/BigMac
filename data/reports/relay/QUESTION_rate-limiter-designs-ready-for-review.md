# Question: rate limiter designs ready for review (Polygon + Alpaca)

**Date:** 2026-08-28
**Context:** Per the Part C review verdict — `engine/` triage approved,
proceed to DESIGN (not enable) with four requirements, bring the Alpaca
pacer design back together with it. Both done. **Nothing is wired into any
live caller and nothing is enabled** — this is the review checkpoint before
either of those next steps.

## A naming collision, caught before anything was staged or committed

The original directive named the Polygon design file `engine/rate_limiter.py`.
That path was already taken by a **real, existing, partially-adopted Alpaca
limiter** (`AlpacaRateLimiter`, the "Warp Core Governor" — imported today by
`engine/deep_scan.py` and `engine/strategy_rotator.py`). I wrote the Polygon
design there without reading the existing file first, briefly overwriting
it. Caught it while running the fuller test suite (a diff showed
`engine/rate_limiter.py` as *modified*, not new) and restored it from git
immediately — confirmed byte-for-byte identical to `HEAD` before staging or
committing anything, so nothing was lost. The Polygon design now lives at
`engine/polygon_rate_limiter.py` instead. Flagging this plainly rather than
quietly renaming and moving on: it's a real near-miss, and it also turned
out to matter for the actual design (see below).

## What's built (design-only, dormant)

- `engine/tiered_rate_limiter.py` — shared base implementing all four
  requirements once, so Polygon and Alpaca can't silently drift apart on
  behavior.
- `engine/polygon_rate_limiter.py` — Polygon, scoped to the Admiral-approved
  `engine/` triage (6 live-tier callers, `engine/` only — `scripts/` and the
  other 19 files stay unmanaged for now, which is why the cap is 4/min, not
  Polygon's real 5/min: one call/min of deliberate slack for what this
  doesn't yet control).
- `engine/alpaca_pacer.py` — same base. The naming collision above turned up
  something directly useful here: the existing `AlpacaRateLimiter` documents
  Alpaca's real published limit (200 req/min, conservatively capped at
  150) — replacing an earlier draft's ungrounded guess of 30/min with a real
  number. That existing limiter has none of the four requirements here
  (no tiers, no fail-loud, no cache, no shadow mode, no kill switch) and is
  only imported by 2 of the ~20 real Alpaca callers — it doesn't solve this
  problem, but it's a real, calibrated data point this design should build
  on rather than ignore. Managed budget here is a deliberately small slice
  (20/min: 10 live-reserved + 10 shared) of that real 150-200/min ceiling,
  same "leave room for unmigrated callers" logic as the Polygon design.
  **Recommendation, not done here:** the two existing `AlpacaRateLimiter`
  callers should eventually migrate to this interface and the old
  blocking-wait class retired, rather than running two Alpaca-pacing
  philosophies side by side indefinitely — a separate, later change.
  Tier list (crew_scanner, battle_station, premium_tracker, tick_recorder
  as live; long_range_sensors, volume_baselines, volume_scanner,
  data_ingestion, total_portfolio, signal_evaluator as cached) is a
  **first-pass proposal**, not reviewed to the depth Polygon's list got.
  Alpaca also has no single chokepoint class like `polygon_provider.py` —
  ~20 files each make their own `requests.get()` calls directly, so wiring
  this in later means editing each call site, not one provider class.
- `tests/test_tiered_rate_limiter.py` — 12 tests, one or more per
  requirement, all passing:

```
test_off_mode_is_pure_passthrough
test_live_tier_has_reserved_budget_shared_tier_does_not
test_live_tier_can_overflow_into_shared_pool
test_live_tier_fails_loud_when_exhausted_and_uncached_in_market_hours
test_live_tier_serves_cache_within_freshness_window
test_live_tier_fails_loud_on_stale_cache_in_market_hours
test_stale_cache_outside_market_hours_is_served_not_fail_loud
test_cached_tier_never_fails_loud
test_failed_fetch_is_not_cached
test_shadow_mode_never_changes_real_behavior
test_shadow_mode_reports_what_enforcement_would_have_done
test_shadow_mode_distinguishes_would_serve_stale_from_would_fail_loud
12 passed
```

## How each requirement is met

1. **Freshness tiers + reserved live budget within the managed cap.**
   Two token buckets: `live_reserved_per_min` (Polygon: 2, exclusively for
   the 6 live callers) and `shared_per_min` (Polygon: 2, either tier can
   draw from it — live tier overflows into it once its reserve is spent,
   cached tier only ever draws from here). Total stays at the managed cap
   (4/min for Polygon). Freshness itself is a per-cache-entry age check
   against `live_max_stale_secs` (Polygon: 30s, Alpaca: 10s — both starting
   guesses, not calibrated), consulted only for the live tier; cached-tier
   entries have no staleness ceiling at all.

2. **Fail loud, never silent-stale, during market hours.** When a live-tier
   call has no token AND no cache fresh enough (or none at all) AND
   `RiskManager.is_market_hours()` says the core session is active: raises
   `BudgetExhausted` (the caller MUST catch this specifically and skip its
   cycle — this is by design not something the limiter can enforce on the
   caller, since Python can't force a caller to handle an exception any
   particular way) and fires a RED_ALERT via `engine.alert_channels.
   send_alert()`. Chose RED_ALERT specifically, not WARNING/INFO — with
   ntfy currently silenced by DECOM-SILENCE (see the flag below), a
   WARNING-level alert would never actually reach a phone, and "never
   silently serve stale GEX" demands this be impossible to miss. Outside
   market hours, or with a usable cache, no alert and no exception — cached
   tier NEVER fail-louds under any condition, by design (that's the whole
   point of being in that tier).

3. **Env-flag kill switch.** `POLYGON_LIMITER_MODE` / `ALPACA_PACER_MODE`,
   default `"off"` if unset. `off` is a byte-for-byte passthrough — calls
   `fetch_fn()` directly, no cache read/write, no budget tracking, no
   alerting. Deploying this code today changes nothing anywhere; someone
   has to explicitly export the env var to change behavior at all.

4. **Shadow mode.** `mode="shadow"` always calls `fetch_fn()` for real (byte
   -for-byte the same real outcome as `off`) but computes, in parallel,
   what `enforce` would have done — logs a WARNING line per would-be
   fail-loud event, and `shadow_report()` returns running counts (`total`,
   `would_throttle`, `would_fail_loud`, `would_serve_stale`, plus a
   per-caller breakdown of would-be fail-louds) so "run it for one full
   session, see what it would have throttled" is a real, checkable report,
   not just log noise to read by hand.

## A finding, unrelated to this design but surfaced while testing it

Running the full test suite to make sure nothing regressed turned up that
tonight's earlier commit (`87e88c5`, the DECOM-SILENCE + Pushover RED_ALERT
activation) breaks two real tests: `test_squeeze_writer.py::
test_ntfy_individual_under_throttle` and `::test_ntfy_rollup_over_throttle`.
Confirmed via A/B against the pre-DECOM-SILENCE version of
`alert_channels.py` — these two pass there, fail now. Root cause:
`engine/squeeze_scanner.py::_ntfy_priority_candidates()` calls
`alert_channels._send_ntfy()` directly and uses **its return value** to
decide whether to mark `squeeze_watch` rows `ntfy_sent=1` in the DB. Now
that `_send_ntfy()` always returns `False` (the DECOM-SILENCE guard),
those rows presumably never get marked sent — a bookkeeping side effect
beyond "the phone stays quiet," which I didn't check when I verified and
committed that pre-existing code earlier tonight. Not fixed here — flagging
it since it's a real gap in what "DECOM-SILENCE" actually does versus what
"Admiral wants phone quiet" plausibly intended, and it's your call whether
squeeze_scanner's bookkeeping should be decoupled from the send-success
boolean, or whether this is acceptable collateral until Gate 2. (The other
~30 test failures found during this same full-suite run are pre-existing
and unrelated — several are literal `ImportError`s for `riker_synthesis.py`
and other files deleted in the 2026-07-22 stand-down, not something either
tonight's commits or this design touched.)

## Options

- **Approve both designs as-is, proceed to wiring** — pick a first call
  site (e.g. `gamma_context.py`) to actually route through `rate_limiter.
  gated_call()`, deploy in shadow mode first per requirement 4, review the
  shadow report before ever setting `POLYGON_LIMITER_MODE=enforce`.
- **Approve the Polygon design, hold the Alpaca one** until its tier list
  gets the same review depth Polygon's did (its own question, separate from
  this one).
- **Send back for changes** — on the tier lists, the cap numbers, the
  30s/10s staleness thresholds, or the RED_ALERT choice for fail-loud.
- Separately: **decide on the squeeze_scanner/DECOM-SILENCE bookkeeping
  gap** above — independent of whether the rate limiter designs get
  approved.
