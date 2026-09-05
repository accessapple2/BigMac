# Scope: routing bk_orb_scanner through get_intraday_candles (not implemented — Monday)

**Date:** 2026-09-04
**Status:** Scoping only. No code changed. Implementation deferred to Monday
per Admiral instruction.

---

## Question asked

Can `engine/bk_orb_scanner.py` drop its own direct Polygon path
(`_fetch_minutes_polygon`) and call the shared `engine.market_data
.get_intraday_candles` like the other 15 callers, inheriting the 2026-09-04
freshness check and the (not-yet-enabled) tiered Polygon rate limiter for
free?

## Finding: no, not as a drop-in — two coordinated changes needed

### 1. Why bk_orb can't just call it today

`get_intraday_candles`'s Polygon path (`engine/market_data.py:927-984`) is a
single, unpaginated request (`limit=500&sort=desc`, no `next_url` follow-up).
Whatever `range_` is requested, it returns at most the newest 500 bars.

bk_orb needs `TRAIL_SESSIONS=20` full RTH sessions of 1-minute bars to compute
its median first-window volume baseline (≥7,800 bars), and actually pulls
`FETCH_DAYS=40` calendar days via its own `next_url` pagination loop
specifically because of this gap. Pointing it at the shared function
unmodified would silently collapse the trailing baseline to ~1.3 days —
`_median_first_vol` would almost never see `TRAIL_SESSIONS` worth of history,
and the scanner would stop firing. Not a style regression — a correctness
one.

### 2. Change size to `get_intraday_candles`

Small and additive: thread an opt-in parameter (e.g. `max_bars`) through the
Polygon block that makes it follow `next_url` in a bounded loop (same shape
as bk_orb's existing `pages < 10` guard), only when the caller asks for more
than 500 bars. Absent the parameter, behavior is byte-for-byte identical to
today.

The staleness check (`_POLYGON_STALENESS_THRESHOLD_HOURS`, `HM-POLYGON-
FRESHNESS-2026-09-04`) already keys off `_rows[0]` — the newest row of the
first (most-recent, since `sort=desc`) page — so it needs no changes to keep
working once pagination is added.

### 3. Callers affected: none

Enumerated all 15 live (non-test/backup) call sites of `get_intraday_candles`.
Every one besides bk_orb's own dead fallback branch and `get_session_vwap`
uses interval/range combinations that stay well under 500 bars today (`5m`/
`1d` ≈ 78 bars, `1h`/`5d` ≈ 35, `1d`/`3mo` ≈ 63, etc.). `get_session_vwap`
(`interval="1m", range_="1d"`) is ~390 bars, already inside the cap. None of
the 15 would take a different code path with the new parameter absent — this
is additive with zero regression risk to existing callers, confirmed by
direct enumeration, not assumed.

### 4. The real risk: the rate limiter's budget shape doesn't fit bk_orb's data shape

Read `engine/tiered_rate_limiter.py::TieredRateLimiter.gated_call` end to
end. It is **fail-fast by design** — `_try_acquire` grants or denies a token
immediately; there is no sleep/backoff/retry anywhere in the class. So the
specific failure mode raised as a concern ("30 minutes per cycle instead of
failing fast") **cannot happen** with this limiter's current implementation —
every call resolves in microseconds, one way or the other.

But a different, worse-shaped problem exists:

- `CAP_PER_MIN=4` (2 live-reserved + 2 shared) is a budget **shared across
  all seven `LIVE_CALLERS`** (`gamma_context`, `options_pricing`,
  `paper_trader`, `bk_orb_scanner`, `squeeze_scanner`,
  `ollie_machine_universe`, and the `get_intraday_candles` alias covering the
  other 14 collapsed callers) — bk_orb doesn't get its own lane.
- bk_orb needs up to 150 `gated_call` invocations per scan cycle (one per
  universe symbol — pagination happens inside a single `fetch_fn`, so it
  doesn't cost extra tokens, which is the one piece of good news here).
- Per the 09-04 relay, bk_orb runs ~44 cycles/day over a ~134-minute window
  — roughly one cycle every 3 minutes, needing ~150 fresh-or-cached
  resolutions in that window against a pool that refills 4 tokens/min
  **total**, shared with six other live callers.
- `LIVE_MAX_STALE_SECS=30` is a single global threshold, tuned for
  live-options/GEX data that genuinely changes every few seconds. bk_orb's
  data is the opposite shape: ~40 days of mostly-static minute history plus
  one live tail-minute. Applied unchanged, almost every cache entry older
  than 30 seconds is treated as unusably stale, so on any given cycle only
  the handful of symbols that happen to win one of the ~4 tokens/min
  succeed — the rest raise `BudgetExhausted` (market hours) and get skipped
  for that cycle.
- Net effect if wired in as-is: not slow, but **starved** — a
  near-total throughput collapse that *looks* clean in the logs (no 429s,
  no errors, just skipped cycles) and could easily be misread as "fixed"
  by anyone glancing at `shadow_report()` without checking realized scan
  counts. That's arguably worse than today's loud, countable 429 failures.

**Conclusion:** shipping this needs two coordinated pieces, not one:
(a) the opt-in pagination change to `get_intraday_candles` (cheap, zero-risk
to existing callers), and (b) either a per-caller staleness-TTL override
(the module's own docstring already flags this as deferred, not built:
*"if options_pricing.py and ollie_machine_universe.py turn out to need
different thresholds, split this into a per-caller dict then"*) or a
separate token allocation so bk_orb's mostly-static historical fetch isn't
judged by a 30-second freshness bar built for live options quotes. Neither
is a large change individually, but doing (a) without (b) would ship
something that passes review and then quietly stops scanning most of the
universe once the limiter mode flips from `off`/`shadow` to `enforce`.

---

## Retroactive correction: bk_orb is the largest *quantified* uncovered Polygon caller

`engine/polygon_rate_limiter.py`'s own module docstring already documents
this (HM-POLYGON-LIMITER-REWIRE-2026-09-01 note), but it's worth stating
plainly here because it changes how the shadow report should be read:

bk_orb's `_fetch_minutes` only falls back to the shared
`get_intraday_candles` cascade when `POLYGON_API_KEY` is unset. In
production the key **is** set, so that fallback branch is dead code — bk_orb
always takes its own direct, unmanaged `_fetch_minutes_polygon` path. That
path took **2,471 429s** on 2026-09-01, entirely separate from the **37,174**
429s traced to the 15+ callers that share the `get_intraday_candles` budget
attribution.

Because bk_orb never touches the shared chokepoint, **none of its real-world
Polygon load has ever been attributed to any of `shadow_report()`'s
per-caller stats.** The shadow report isn't missing a small slice — it's
systematically blind to bk_orb's entire volume, which (2,471 429s/day) is
larger than any other *individually quantified* caller besides the 15-caller
aggregate itself. (`scripts/` and 12 other files outside `engine/` remain
unquantified per `QUESTION_polygon-rate-limiter-bench-gate-list.md` — this
claim is "largest known number," not "largest possible," since those are
still untriaged.)

Anyone reading a clean `shadow_report()` between now and whenever bk_orb
gets wired in should not conclude it represents total fleet-wide Polygon
load — it structurally cannot, by construction, while bk_orb sits outside
it.

---

## Not done in this pass

No code changed. Both the pagination change and the per-caller TTL/budget
design are scoped but not built — Monday, per instruction.
