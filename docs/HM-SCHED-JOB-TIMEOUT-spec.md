# SPEC — per-job timeout for the shared scheduler queue (HM-SCHED-JOB-TIMEOUT)

**Status: SPEC ONLY. Not built. The Admiral reads this before anything ships.**
Written 2026-09-14 after the second whole-scheduler stall (DOCTRINE.md, "One slow job takes out the
whole shared scheduler — twice now").

## 1. Problem

`main.py`'s main loop calls `schedule.run_pending()` on one thread. **168 `schedule.every(...)` jobs**
share it; 137 distinct jobs have logged runs since 2026-09-09. A job runs inline, so while it runs
nothing else on the queue runs. That includes `run_spread_exit_cycle` (exits),
`run_alpaca_gex_refresh` (canonical GEX), and `check_scan_liveness`, the watchdog meant to notice
stalls. The watchdog shares the failure mode of what it watches.

Incidents:
- **2026-09-11:** an unidentified job blocked ~20 min of RTH; McCoy's 12:30 ET slot never fired.
- **2026-09-14:** `run_volume_red_alert` blocked from 06:41 to the 07:18 restart. Its self-inflicted
  SQLite lock waited ~169 s per alerted symbol (HM-RED-ALERT-SELF-LOCK, fixed `989ed38`). Nothing
  alerted; it was found by hand.

### Evidence: `[SCHED-JOB] done … wall=` 2026-09-09 → 2026-09-14 (32,041 runs)

| job | interval | median | p99 | max |
|---|---|---|---|---|
| run_strategy_lab_auto | 30 min check | 0.00 s | 0.00 s | **8,390 s** (Sun 00:05) |
| run_weekly_tuning | 30 min check | 0.00 | 0.14 | 1,985 (Sun 21:14) |
| run_ollie_extended_scan | 10 min | 0.00 | 521 | 529; ~520 s back to back all Sunday night |
| run_strategy_scan | 30 min | 0.00 | 431 | 470 |
| run_uoa_premarket | 15 min | 0.00 | 0.00 | 469 |
| run_scanner | 2 min | 0.00 | 76 | 278 |
| run_pattern_match | 15 min | 0.00 | 202 | 216 |
| run_imbalance_scan | 2 h | 180 | 180 | 180 |
| run_volume_red_alert | 5 min | 0.00 | 1.35 | 170 (+ the 37-min run killed today, not in this data) |
| run_spread_exit_cycle | — | 1.30 | 23 | 130 |

- 37 jobs have exceeded 10 s at least once, 21 exceeded 60 s, 5 exceeded 300 s.
- 272 runs exceeded 60 s; only 6 of them fell inside weekday RTH.
- **This data undercounts the worst case.** A run that never finishes never logs `done`.
  Today's 37-min run and the 9/11 stall are both absent.

## 2. Hard constraints (these decide the answer)

1. **CPython cannot safely kill a thread.** There is no API.
   - `ctypes.pythonapi.PyThreadState_SetAsyncExc` only raises at the next bytecode boundary. It does
     not interrupt a blocking C call.
   - Today's stall was exactly that: `sqlite3_step → sqliteDefaultBusyCallback → unixSleep`.
   - Where it does land, it can land inside a `finally:`, a commit or a lock release, leaving
     half-written state.
   - The only real kill is the process (a trader restart).
2. **A timeout cannot preempt a job that runs on the loop thread.** Whatever thread would enforce
   the timeout is the thread that's blocked. Any timeout needs job bodies to run off the loop thread.
3. **Jobs have always run serially with each other.** Some of the 168 almost certainly share module
   globals, in-memory caches or "once per day" flags that were never made thread-safe. Moving
   everything to concurrent threads introduces races that don't exist today.
4. **Restarts have costs.** A restart re-arms in-memory state: today it re-fired McCoy's pre-open
   slot because the done-today set lives in memory. It also kills unrelated in-flight work. Automatic
   restarts during RTH are not free recovery.
5. **A timeout does not release a SQLite lock.** A stuck job holding a write transaction still blocks
   every writer in the process, whichever thread it's on. A timeout keeps the scheduler running and
   names the culprit. Lock-holding bugs still need DOCTRINE rule 1 (no write transaction across a loop,
   network call or cross-module write).

## 3. The question: what does a timeout do when it fires?

| action | feasible? | verdict |
|---|---|---|
| **Kill the thread** | No (constraint 1); only a process restart kills | **Rejected in-process.** Process restart stays a human decision (§4.4). |
| **Skip the job** | Only as "don't start another instance while one is in flight, and don't pile up missed ticks" | **Necessary, not sufficient.** It can't unblock anything alone. |
| **Alert and continue** | Yes, once job bodies run off the loop thread | **Recommended**, combined with skip-re-entry. |

**Recommended policy when a job exceeds its budget:**
1. **Alert once per stuck run.** A `[SCHED-STALL]` log line plus a WARNING NTFY via
   `engine.alert_channels`, naming job, start time, elapsed and budget.
2. **Continue.** Every other job keeps its cadence.
3. **Skip re-entry.** That job's later ticks are skipped and counted while its run is in flight; no
   second instance, no pile-up.
4. **Never kill.** The stuck thread runs until it returns. If it never returns, one worker leaks.
5. **Escalate, don't act.** Past a hard ceiling, a RED alert says the job has been stuck N min and a
   restart is needed to reclaim the worker. A human decides.

## 4. Design

### 4.0 Phase 0 — detection only, no behavior change (can ship alone, first)

- **In-process watchdog thread** (`sched_watchdog`, its own daemon thread, not a scheduled job).
  - `_instrumented_schedule_job_run` already brackets every job. Add module state
    `current_job = (name, start_monotonic)`, set on start and cleared in `finally`.
  - The watchdog polls it every 15 s. Over budget → the §3 alert, once per run.
  - Today it would have fired ~06:43 naming `run_volume_red_alert`.
- **Out-of-process check** (different mechanism, per "alarms must not share a failure mode").
  - The loop writes `data/sched_loop_heartbeat.json` (`last_loop_utc`, `current_job`,
    `job_started_utc`) every iteration, atomic replace.
  - `scripts/hm_ops_sentinel.py` (cron `*/5`) raises RED if it's > 5 min stale during RTH, naming
    `current_job`.
  - This also catches the in-process watchdog dying.
- Phase 0 changes no job's execution. Risk ≈ logging only.

### 4.1 Phase 1 — get job bodies off the loop thread, without new concurrency

- **Lanes.** The loop thread only decides which jobs are due and hands each to a lane.
  - **Legacy lane (default):** one worker thread that runs every non-allowlisted job serially,
    exactly as today. Mutual exclusion between legacy jobs is preserved (constraint 3).
  - **Isolated lanes:** one single-worker thread per allowlisted job. Serial with itself,
    concurrent with everything else.
- **Initial isolated allowlist** (proposed; each needs a review that it touches no shared mutable
  state, or is made thread-safe first):
  - known long-runners: `run_strategy_lab_auto`, `run_weekly_tuning`, `run_ollie_extended_scan`,
    `run_strategy_scan`;
  - known blocker: `run_volume_red_alert`;
  - must-not-starve: `run_spread_exit_cycle`, `run_alpaca_gex_refresh`, `check_scan_liveness`.
- **Skip re-entry / no pile-up.** A job due while its previous run is still in flight is not
  submitted again. Log `[SCHED-SKIP] name=… reason=in_flight elapsed=…` and count it in the heartbeat.
  - On the legacy lane, jobs queued behind a stuck job are dropped for that tick, not queued. The
    alert names the blocker, which becomes a candidate for the allowlist.
- The existing `_bg_*` wrappers (`_bg_autopilot`, `_bg_ollie_machine_enter`, …) are the in-repo
  precedent for "dispatch and return".

### 4.2 Budgets

- **Per job, derived from `[SCHED-JOB]` history:** `budget = max(60 s, 3 × p99 wall)`.
  - Recomputed by a script from logs, then reviewed and pinned in one table (e.g.
    `config.SCHED_JOB_BUDGETS`) with a default for jobs without history.
  - Not hardcoded at call sites.
- **Hard ceiling (RED escalation):** `max(4 × budget, 30 min)`; 10 min for must-not-starve jobs
  during RTH.
- **Interval check at load:** warn if a job's budget exceeds its schedule interval (it will skip ticks
  by construction).

### 4.3 What a timeout does NOT do

- Doesn't kill or interrupt the job (constraint 1).
- Doesn't release any SQLite lock the job holds (constraint 5).
- Doesn't restart the process.
- Doesn't mark the job's own work failed. If it eventually returns, its result stands, and the
  `done` line logs the true wall.

### 4.4 Restart escalation

Recommended: **never automatic.** The RED alert says "restart required to reclaim worker for job X,
stuck N min". A human runs `scripts/trader_restart.sh` after checking what else is in flight.

## 5. Tests (write first; each must fail on today's code)

1. **Loop keeps ticking.** Fake job A sleeps past its budget and fake job B is due every second:
   B keeps running while A is stuck. Today B never runs.
2. **Exactly one alert per stuck run,** containing name, elapsed and budget.
3. **Skip re-entry.** A stays in flight across 3 of its ticks: one instance runs, 2 `[SCHED-SKIP]`,
   no second thread.
4. **Legacy lane serial.** Two legacy jobs never overlap (probe on shared state).
5. **Exceptions contained.** A job that raises is logged; the lane continues.
6. **Heartbeat file** is written every loop iteration; the sentinel check goes RED when it's stale.
7. **Phase 0 watchdog.** A job stuck in a C-level sleep (e.g. a SQLite busy wait on a held lock) is
   still detected and alerted; detection must not depend on the job returning.

## 6. Decisions for the Admiral

1. **Timeout action:** alert + continue + skip re-entry, never kill (recommended), vs. skip-only vs.
   process restart.
2. **Auto-restart escalation:** never (recommended) vs. after N min stuck in RTH.
3. **Rollout:** Phase 0 alone first (recommended; detection with no execution change), then Phase 1.
4. **Lane model:** legacy serial lane + reviewed isolated allowlist (recommended) vs. fully concurrent.
5. **Budgets:** data-derived `max(60 s, 3 × p99)` per job (recommended) vs. one flat budget.
