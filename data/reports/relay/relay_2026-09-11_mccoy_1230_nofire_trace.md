# Relay — McCoy's 12:30 PM ET slot: traced, root cause is a global scheduler stall. 2026-09-11

## Checklist, per the Admiral's exact trace request

1. **Timezone bug (ET spec vs MST box) — RULED OUT.**
   `run_mccoy_screened_scan()` (`main.py:3176`) uses `datetime.now(pytz.
   timezone("US/Eastern"))` — a real timezone-aware conversion, not a
   naive box-local read. The box's own MST/AZ system time is irrelevant
   to this comparison; only the underlying UTC clock matters, and that's
   already confirmed accurate (no clock skew, established earlier this
   session). Not the bug.

2. **Was the job registered? Yes, confirmed on every restart today.**
   `job=run_mccoy_screened_scan` appears in `WR-DEBUG-INIT` logging 6
   times today, once per restart, including the most recent restarts
   before the 12:30 window opened.

3. **Did a guard reject it (market-hours, halt_mode, regime)? No —
   it never got that far.** The function's own guards (`now.hour < 1`,
   `now.weekday() >= 5`) don't apply at 12:3x PM ET on a Friday. McCoy
   (`ollama-plutus`) is `halt_mode='active'`. There's no halt/regime
   check inside this function at all — that logic lives downstream in
   `arena.run_scan()`/`paper_trader.buy()`'s real gate chain, which the
   function never reached.

4. **Did the scheduler tick at all during the window? No — and this
   is the real finding.** Checked `logs/trader.log` for the exact
   12:30–12:50 ET window (= 09:30–09:50 AZ box-local, where the log
   timestamps actually live): **zero log lines from ANY `schedule.
   every(1/2/5).minutes` job** — not just `run_mccoy_screened_scan`, but
   also `run_scanner`, `run_crew_scanner_job`, `run_events_bus_consumer`,
   and every other schedule-registered job checked. The dashboard was
   still serving requests and a separate War Room daemon thread
   (`WR-DAEMON-HB`, its own background thread) ticked normally at
   09:30:33 and 09:45:33 — so the process wasn't frozen — but the
   single-threaded `schedule.run_pending()` queue that every 1/2/5-minute
   job (including McCoy's) runs through produced no evidence of running
   anywhere in this 20-minute span.

## Root cause: a known, pre-existing architecture issue, not a new bug

`main.py` already carries its own comments documenting this exact failure
class: *"single-thread schedule.run_pending() loop-blocker: avg 831s /
max 1194s"* (~14–20 minutes) for at least one job type, and a near-
identical incident already root-caused for `run_kirk_advisory_job`
(missed slots because a single-threaded scheduler stalled past a narrow
window). **1194s (≈19.9 min) very nearly equals the 20-minute width of
McCoy's own firing window** — consistent with one long-running scheduled
job blocking the entire queue for close to the full window, not
McCoy's function specifically misbehaving.

**Not identified in this pass**: which specific job was the blocker.
That would need a deeper log trace than context budget allows for right
now — flagged to the backlog below as the next concrete step, not left
unrecorded.

## Does the second slot (9:35 AM) tell us anything more?

No new information — already explained by a different, non-recurring
cause (the dry-dock incident occupied that entire window). The 12:30
no-fire is the first clean data point on this specific failure mode for
this specific job.

## What this means for Phase 1.2 (the cadence cut)

The mechanism is currently unreliable, not broken by design: on a day
with heavy scheduler contention, McCoy can silently get zero scans for
an entire session even though the code, registration, and guards are all
correct. This directly undermines Phase 1.2's core promise (twice-daily,
reliable scans instead of continuous noise) — a twice-daily job that can
silently no-op is worse than a frequent one, since there's no next tick
in the same day to recover on.

## Filed to backlog

Two items: (1) find the actual blocking job for today's 09:30–09:50
stall, (2) consider whether `run_mccoy_screened_scan` (and any other
narrow-window twice-daily job) needs a dedicated thread/process instead
of sharing the single-threaded `schedule.run_pending()` queue with
everything else — the same fix class already applied elsewhere in this
codebase for jobs that can't tolerate being starved (e.g. the WR daemon
itself, `HM-EQ`'s snapshot pass).
