# HM-AS-β SCHEDULER OVERLOAD — TOP PRIORITY for next ALL OUT session

Banked 2026-05-28 (end of the WAVE 0-6 ALL OUT run). This is the **#1 carry-forward**:
it's both a systemic performance issue AND the root of the TIER3 tail (5.9).

## Problem
`main.py` registers **145 `schedule.every()` jobs on a single-thread `schedule`
library loop**. When a job runs long (WR cycle, scans, fundamentals fanouts),
every other job's cadence drifts — jobs queue behind the running one. Observability
already exists (`[HM-AS-β]` cadence-drift warnings in `logs/trader_error.log`).

## Evidence it's biting (TIER3 5.9 tail)
Three genuine equity agents — **energy-arnold, ollama-qwen3, qwen3-8b-flash** —
are active + rostered + on installed models, but **hard-stopped emitting on
2026-05-07** (last `signal_emit` in `decision_audit`). Not halted, not a 404, not
HOLD. The date lines up with the scheduler-overload era. Hypothesis: their scan
jobs are starved by single-thread contention (the rule-based control
deepseek-7b-grok4 still writes because it's cheap/fast; the LLM agents' slower
jobs lose the queue).

## What's already shipped (partial)
- Cadence-drift observability (`[HM-AS-β]` warnings).
- **HM-AS-β.2 Option A pilot (2026-05-08):** `_bg()` fire-and-forget thread
  wrapper on ONE job (`run_squeeze_watcher`). "Broader rollout = HM-AS-β.3 after
  1-2 weeks soak" — never done.

## Proposed approach (HM-AS-β.3) — for next session
1. **Instrument first** (verify-before-fix): dump `schedule.jobs` + per-job last-run
   wall time; confirm energy-arnold/ollama-qwen3/qwen3-8b-flash scan jobs are
   actually firing vs starved. (This IS the 5.9 trace.)
2. **Move hot/long jobs off the single thread** — either:
   - (a) extend the `_bg()` wrapper to the heavy jobs (scans, WR, fundamentals),
     bounded by a small ThreadPoolExecutor so we don't spawn unbounded threads, OR
   - (b) replace the `schedule` loop with an APScheduler `BackgroundScheduler`
     (thread-pool executor, misfire grace) — cleaner but a bigger refactor.
   Recommend (a) first (surgical, matches the shipped pilot), (b) only if (a)
   doesn't clear the drift.
3. **Soak + validate**: confirm the 3 agents resume emitting + cadence drift
   warnings drop.

## Risk
Changes the timing/concurrency of 145 jobs — races, double-fires, DB contention.
Needs careful design + a soak window. **Do NOT rush at a session tail.** This is
why it was deferred from WAVE 6 to its own focused session.

## Also fold in here
- WR-cycle-RCA Phase 2 instrumentation (debug branch was deleted) — subsumed by #1.
- ASGI exception (1,273 non-fatal anyio worker-thread occurrences) — likely related
  to threadpool/middleware contention; investigate alongside the scheduler work.
