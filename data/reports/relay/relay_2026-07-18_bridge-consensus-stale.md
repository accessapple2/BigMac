# Relay: HM-BRIDGE-CONSENSUS-STALE

**Date:** 2026-07-18 (Saturday), directive issued by XO 2026-07-18, Admiral-approved.
**Priority:** HIGH (live_decision-criticality source) — resolved same session.

## What was asked

`bridge_consensus` source-health sat AMBER, `as_of` 2026-07-16 13:02 (~30h
stale), having missed Friday 2026-07-17's market-hours update. Diagnose
root cause (scheduler-never-fired / job-errored / write-didn't-land),
fix, verify GREEN, and add the source to the Monday pre-market
verification scope.

## Investigation

- **No cron or launchd job named "consensus" exists on this box.**
  `bridge_consensus` is written by `engine/bridge_vote.py::run_morning_vote()`,
  invoked by `engine/bridge_vote.py::run_bridge_vote_job()`, which main.py
  registers via `schedule.every(5).minutes.do(run_bridge_vote_job)`
  (`main.py:5148`) — an **in-process** job inside the always-running
  `main.py` trader process, not an OS-level scheduler. HM-CRONTAB-EINTR is
  not implicated; ruled out early.
- `main.py` had not restarted since 2026-07-10 20:05:13 (confirmed via
  `logs/trader_keepalive_cron.log`) — the same continuous process handled
  both the successful 07-13 through 07-16 votes and the failed 07-17 one,
  ruling out "process was down."
- `bridge_votes`/`bridge_consensus` had zero rows for session_date
  2026-07-17 (confirmed directly against `data/trader.db`) — a clean full
  miss, not a partial/errored run.
- `run_bridge_vote_job()` only emits a log line when it either (a) skips
  for weekend, or (b) actually fires ("9 AM ET gate — firing morning
  vote"). On every other weekday tick outside its 9:00–9:10 AM ET fire
  window it returns silently. `logs/trader_error.log` (bridge_vote logs
  to stderr, not stdout — a real gotcha, cost real time to find) has
  **zero** "firing"/"failed" lines anywhere across all of Friday
  2026-07-17 — the fire branch was never reached that day, with no
  exception anywhere (`run_morning_vote failed` never appears either).
- `main.py` was demonstrably alive and cycling normally through the
  06:00–06:12 AZ (09:00–09:12 ET) window that morning (`logs/trader.log`,
  full ISO timestamps) — strategy scans, deep scans, war room cycles all
  firing. This was **not** a hung process.
- However: a war room cycle in that exact window logged
  `[WR-DUR] cycle wall=299.9s` (nearly 5 minutes) at 06:01:13, and another
  at `wall=331.2s` at 06:06:44. `run_bridge_vote_job` is registered in
  main.py's **single-threaded** `schedule.every(5).minutes` queue behind
  ~15 other same-cadence jobs (`run_dayblade`, `run_earnings_day_scan`,
  `run_cto_advisory`, `run_kirk_advisory_job`, `run_portfolio_monitor`,
  `run_oi_morning_snapshot`, three spread-exit jobs, `run_eod_scorecard_job`,
  `run_volume_red_alert`, then `run_bridge_vote_job`). All become "due"
  simultaneously each 5-minute tick and run **sequentially, synchronously**
  via `schedule.run_pending()`. The three spread-exit jobs registered just
  ahead of bridge_vote were observed firing at 06:08:56/06:09:02/06:09:08
  that same morning — i.e. that sweep landed *after* the 9:00–9:10 ET
  window had already closed.
- This single-threaded-queue bottleneck is **not a new finding** — it's
  already flagged repeatedly in main.py's own comments (≈lines 1416, 1687,
  1724, 2003, 4184, 4216, 4242, 4255, 4312) as a known architectural
  weakness. Per the directive's 30-minute non-refactor constraint, no fix
  was attempted here — only documented.

## Root cause classification

**(a) scheduler issue** — but the in-process `schedule` library's queue,
not cron/launchd. The likely (not 100%-certain) mechanism: cumulative
delay from long-running jobs registered ahead of `run_bridge_vote_job` in
the same 5-minute batch pushed execution past the 9:00–9:10 AM ET gate
before the job's own window check ever ran, on a day when nearby jobs
(war room cycles) happened to run long. No exception, no crash — a
silent skip by design (the function returns early with no log call when
outside its window).

## Fix applied

1. Ran `engine.bridge_vote.run_morning_vote(force=True)` manually
   (2026-07-18 20:44:21 UTC / 13:44 MST) — 6/6 voters responded, consensus
   SELL (HIGH conviction). Confirmed via `engine.source_gate.source_freshness
   ("bridge_consensus")`: **state=GREEN**, `as_of=2026-07-18 20:44:21`,
   `age=0s`. (`cadence_class=daily` → GREEN ≤24h, AMBER ≤48h, RED beyond,
   per `source_registry` in `signal-center/signals.db`.)
2. Added `scripts/hm_bridge_consensus_monday_check.py` — read-only
   freshness check against `source_gate.source_freshness("bridge_consensus")`,
   NTFYs `ollietrades-admin` only if not GREEN at check time. Scheduled as
   a one-shot launchd job, `com.ollietrades.hm-bridge-consensus-monday-check`
   (`~/Library/LaunchAgents/`), **Monday 2026-07-20 07:00 MST**, bootstrapped
   live this session (`launchctl bootstrap gui/501 ...` succeeded, confirmed
   via `launchctl print`). Mirrors the existing
   `com.ollietrades.hm-signals-v2-monday-check` one-shot pattern already in
   use in this repo.

## Not done (out of scope per directive)

- No refactor of the single-threaded `schedule.run_pending()` queue —
  explicitly out of scope ("do not refactor the producer speculatively").
  If Monday's check also comes back non-GREEN, that would be strong
  evidence this is a recurring pattern worth a real fix (candidate:
  moving `run_bridge_vote_job` onto a dedicated thread/timer the way
  `[WR-DAEMON-HB]`/event-tape/tick-recorder already are, rather than
  sharing the jammed `schedule.every(5).minutes` queue).
- No sacred-DB writes beyond the producer's own normal append
  (`bridge_votes`/`bridge_consensus` INSERTs via `run_morning_vote`).

## Verification

```
$ source_gate.source_freshness('bridge_consensus')
state=GREEN as_of=2026-07-18 20:44:21 age=0s
```
