# HM-BN — Proving Ground Window Enforcement

**Status:** DISCOVERY → HALT for Captain decision
**Origin:** trader.log shows "Proving Ground Day 33/30 | Trades: 272 | WR: 76.5%" on 2026-05-12. The /30 suffix indicates a 30-day planned window, but Day counter is at 33 — three days past nominal end. No graduation NTFY observed on Day 30. Cohort continues to run.

## Phase 1 — Discovery (next session, NO code changes)

1. Read engine/proving_ground.py — locate the Day counter increment and the /30 cap (or absence thereof)
2. Identify where the running_scorecard row is written daily (main.py:3904 emits the log line — find the underlying writer)
3. Determine whether the 30-day boundary is enforced anywhere (graduation event, halt, cohort rollover) or whether it is purely cosmetic
4. Check if today total_trades=272 was supposed to trigger a "graduation complete" event
5. Write up the two options below with recommendation.

## Two options for Captain decision

- Option A: Hard HALT at Day 30. Emit graduation NTFY summarizing all 6 benchmark statuses. Lock further appends to running_scorecard until Captain starts a new cohort.
- Option B: Relabel to "rolling 30-day window". Day counter keeps incrementing; /30 indicates the metric window length, not a cap. Bench statuses become rolling-window evaluations.

## HALT condition

Phase 1 only this session. No code changes. Captain decision required before Phase 2 ship.

## Cross-references

- main.py:3904 (emission point)
- data/proving_ground.db running_scorecard table (today as_of_date=2026-05-12, total_trades=272)
- HM-BP (Max DD validation, dependent on the same scorecard semantics)
