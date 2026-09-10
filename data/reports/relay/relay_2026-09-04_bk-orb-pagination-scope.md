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

## Re-scoped 2026-09-10 PM — cap raised 4→100/min, does the original
## "don't ship (a) without (b)" recommendation still hold?

Context: `CAP_PER_MIN` 4→100, `LIVE_RESERVED_PER_MIN` 2→50 shipped and
deployed today (`ef321ff`/`c9c1731`, restart `13:08:20`) — see
`relay_2026-09-10_polygon-limiter-and-ollie-stale-socket.md`. Live-verified
today: `run_bk_orb_scan` is enabled (`ORB_CONFIRMATORY_VOTE_ENABLED=True`),
scheduled every 3 min, self-gated to the 09:46–12:00 ET window
(`main.py:4764`). `UNIVERSE_SIZE=150`, `FETCH_DAYS=40`,
`TRAIL_SESSIONS=20` — unchanged from the 09-04 read.

**(a) Pagination gap — completely unaffected by the cap raise.** The
500-bar-per-request ceiling on `get_intraday_candles`'s Polygon path has
nothing to do with token budget; it's a correctness gap regardless of cap
size. Still a hard prerequisite before bk_orb can be pointed at the shared
function at all — unchanged conclusion from 09-04.

**(b) Budget starvation — substantially de-risked, not eliminated.**
Redone the math with the new cap: bk_orb needs up to 150 `gated_call`
invocations per 3-min cycle. Old cap (4/min, 2 reserved): a 3-min window
supplied only ~6 reserved tokens against a 150-token need — nightmare-case
correctly diagnosed 09-04. New cap (100/min, 50 reserved): a 3-min window
now supplies ~150 reserved tokens — matches bk_orb's own per-cycle need
almost exactly *if it had the reserved lane to itself*, but it's one of 7
`LIVE_CALLERS` sharing that lane (the other six include the
`get_intraday_candles` alias covering 14+ additional real callers). 25x
more headroom than 09-04's math, genuinely plausible now rather than a
near-certain collapse — but "plausible, needs a live trial" is different
from "safe," and this wasn't re-verified against real concurrent
production contention today (out of scope for this pass).

**The staleness-TTL mismatch is architecturally unchanged.** A single
global `LIVE_MAX_STALE_SECS=30` still can't distinguish bk_orb's
mostly-static 40-day history from live options quotes. The cap raise
means *fewer* calls fall through to the cache-serve path (more succeed on
a direct token grant instead), so this failure mode now affects a smaller
slice of bk_orb's traffic than 09-04's worst case — but it's not fixed,
and nothing here fixes it. Per-caller TTL override is still the correct
fix, still not built.

**New fact that changes the actual urgency, independent of the cap
math: `POLYGON_LIMITER_MODE` is still `shadow` today** (confirmed live,
`[POLYGON-LIMITER-CONFIG] ... mode=shadow`). Shadow mode never blocks a
real call — the starvation and staleness risks above only bite once/if
someone flips the mode to `enforce`, which is not on today's bundle and
not proposed here. That means: **(a) alone could ship now with zero live
risk** — wiring bk_orb through the paginated `get_intraday_candles` today
would only improve shadow-report telemetry accuracy (closing the
"structurally blind to bk_orb's entire volume" gap flagged 09-04), not
create any real starvation, because shadow mode can't starve anything.

**Verdict:** build (a) now if there's a session for it — it's safe today
regardless of (b), and it closes a real telemetry blind spot. Do **not**
flip `POLYGON_LIMITER_MODE` to `enforce` with bk_orb wired in until (b)
(per-caller TTL override) ships too — the cap raise makes that transition
far less likely to be catastrophic than 09-04 feared, but "far less
likely" isn't "verified safe," and there's no live trial data to point to
yet either way.

## Not done in this pass (09-04 original)

No code changed. Both the pagination change and the per-caller TTL/budget
design are scoped but not built — Monday, per instruction.

## Not done in this pass (09-10 re-scope)

No code changed here either — this is a re-analysis of the existing
scope against today's new cap, not an implementation session. (a) is
still buildable in a future session per the verdict above.
