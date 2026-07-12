# Relay: HM-SIGNALS-V2-FIFO-STARVATION closed (already-fixed, doc catch-up) + HM-SENTINEL-ACK shipped + new watch item filed

**Date:** 2026-07-12
**Commit:** `3f47e5c`

## What was asked

XO directive: (1) apply the propose-first fix for HM-SIGNALS-V2-FIFO-STARVATION
and verify oldest-pending drops under 48h, (2) add an ack/ceiling mechanism to
HM-OPS-SENTINEL so a known condition can be acknowledged without going
permanently silent, prompted by a real 5:20 PM alert firing.

## Part 1 finding: nothing to fix — already resolved, just never marked closed

Read the backlog entry cold before touching anything, per doctrine (docs go
stale). Both the original recommendations were already live:
- **2026-07-06, `aa55f1d`:** expired halted-source pending rows + switched
  the consumer to `ORDER BY created_at DESC` (newest-first).
- **2026-07-09, `b3e9ade`:** archived 630 active-source rows predating the
  reorder (the exact residual gap this ticket's own 07-09 note flagged).

Live-verified 2026-07-12 (no writes): pending=140, oldest-pending age=45.9h
wall-clock (<48h), zero active-source rows predating the 07-06 reorder.
Marked `HM-SIGNALS-V2-FIFO-STARVATION` 🟢 RESOLVED in `docs/XO_BACKLOG.md`
with the verification numbers and commit trail. Took a fresh `db_snapshot.sh`
run first per doctrine (no-op — today's snapshot already existed).

## Drain-check: the alert *will* re-fire this afternoon, and that's expected

Investigated why the queue wasn't draining: today is Sunday, market closed
(`RiskManager.is_market_hours()` → `False`), and
`main.py::run_events_bus_consumer()` no-ops entirely when
`is_us_market_open()` is false ("NYSE-hours only" by design). Nothing has
moved in `signals_v2` (any status) since 2026-07-11 02:59:23 — the whole
pipeline is dormant for the weekend, not stuck.

Math: oldest-pending row created `2026-07-10 20:01:52` UTC → crosses the
48h wall-clock WARNING threshold at **~13:02 MST today**, and keeps
climbing (nothing drains) until the consumer resumes Monday ~06:30 MST,
by which point it'll read ~65-66h. Whether it then actually drains or gets
permanently newest-first-outranked by Monday's fresh signal volume is
genuinely unverified — see the new watch-item ticket below.

## Part 2 shipped: HM-SENTINEL-ACK

- `scripts/hm_sentinel_ack.py` (new) — `ack <alert_type> [--ceiling N] [--note] [--by]`,
  `unack`, `list`. Writes `data/.hm_ops_sentinel_acks.json` only;
  `hm_ops_sentinel.py` only ever reads it (stays read-only against
  production data, per the XO constraint).
- `scripts/hm_ops_sentinel.py` — alert tuples gain a metric value;
  `_is_suppressed()` checks acks before dispatch (no ack → fires; acked, no
  ceiling → suppressed until unacked; acked with a ceiling → suppressed
  until the metric breaches it, then re-fires automatically).
- **Market-hours-aware ceiling for the signals_v2 check specifically**
  (requested if cheap — added `engine.market_calendar.market_hours_elapsed()`,
  a new pure function summing actual NYSE-session overlap between two
  timestamps, holiday/early-close/weekend aware). The queue check's ceiling
  metric is elapsed *market* hours, not wall-clock, so a Friday-evening
  backlog sitting through a closed weekend doesn't burn an ack's escalation
  budget for time nothing could have drained during. Confirmed live against
  production data: oldest-pending reads 46.2h wall-clock but **0.0
  market-hours elapsed** right now.
- Verified end-to-end against real production data (WARN threshold
  temporarily lowered in-process, no writes) that: unacked → fires;
  acked with ceiling=13 market-hours → suppressed; simulated post-Monday
  breach (14.0) → re-fires despite the ack. This is the literal "confirm
  the ~1:02 PM re-fire gets suppressed" check the Admiral asked for.
- 53 tests passing (7 new ack tests, 8 new `market_hours_elapsed` tests,
  full existing `market_calendar`/`dynamic_alerts` suites re-run clean, no
  regressions). `py_compile` clean on all touched/new files.

Admiral still needs to run `scripts/hm_sentinel_ack.py ack sentinel_signals_v2_queue
--ceiling <N> --by Admiral --note "..."` themselves to actually suppress
today's alert — nothing was pre-acked on their behalf.

## New watch item filed: HM-SIGNALS-V2-STARVATION-RECURRENCE

Documented in `docs/XO_BACKLOG.md`, 🔵 propose-first, **no code changes**:
the newest-first + fixed-drain-cap combination that required two prior
one-time cleanups (07-06, 07-09) could recur with today's 140-row batch if
Monday's volume is high enough to permanently outrank them. Two candidate
fixes recorded for later evaluation, not built: (a) TTL auto-expiry on
market-hours age, (b) hybrid ordering reserving drain-cap headroom for the
oldest row. Needs Admiral sign-off before either is implemented.

## Open items

- **Monday verification task (queued):** after 2026-07-13 market open,
  check whether the 140 rows are draining or being outranked. Decides
  whether `HM-SIGNALS-V2-STARVATION-RECURRENCE` becomes active work or
  closes as a non-recurrence. Not yet scheduled as an automated check —
  needs either a follow-up session or an explicit ask to set up a
  one-time scheduled check.
- Admiral to run the ack CLI if they want today's alert suppressed before
  the ~13:02 MST crossing.
