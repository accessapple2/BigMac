# HM-BQ — HM-AS-β Cadence Drift Root Cause

**Status:** DISCOVERY → HALT for Captain decision
**Origin:** 2026-05-12 logs/trader_error.log shows HM-AS-β cadence drift firing 12+ times today.
- battle_station_monitor target 120s, observed 930s–6202s (8x–50x late) at 04:40, 06:24, 11:00, 12:26, 13:06, 14:07, 14:22, 14:48
- squeeze_watcher target 1800s, observed 3970s and 3728s (~2x late) at 13:17 and 14:19
Total HM-AS-β line count across the error log: 146.

## Phase 1 — Discovery (next session, NO code changes)

1. Read the HM-AS-β cadence drift detector code — confirm exactly what "drift" measures (time-since-last-fire vs target, or queue-wait, or scheduler-skew)
2. Identify how battle_station_monitor and squeeze_watcher are scheduled (launchd, internal asyncio loop, threaded scheduler)
3. Check if drift events correlate temporally with heavy CPU/IO operations (backtest runs, large LLM inferences, screener snapshots)
4. Pull CPU/load samples around the drift times if available (e.g., system.log, vm_stat snapshots)
5. Determine if any single task is starving the scheduler queue (long-running blocking call inside the main loop)

## Possible root causes to triage

- A: Backtest CPU starvation — daily backtest or model evals consuming the worker thread
- B: Ollama inference blocking — LLM calls running on bigmac CPU instead of routing to Ollie Box, blocking the loop
- C: Scheduler queue design — single-threaded asyncio loop, slow handlers serialize and back up the queue
- D: External I/O blocking — yfinance fetches, screener snapshots, or DB writes holding the loop

## HALT condition

Phase 1 discovery only. No code changes. Captain decision required on remediation strategy after root cause identified.

## Cross-references

- logs/trader_error.log (146 HM-AS-β occurrences)
- HM-AS-β detector code location (TBD by Phase 1 grep)
- Memory: bigmac runs trader on launchd; Ollie Box (.166) is the GPU/inference workhorse
