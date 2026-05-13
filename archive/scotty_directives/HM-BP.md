# HM-BP — Max Drawdown Calculation Validation

**Status:** DISCOVERY → HALT for Captain decision
**Origin:** running_scorecard 2026-05-12 row shows max_drawdown = -87.557 and dd_bench_status = FAIL. If stored as percentage (matching rolling_win_rate convention which stores 76.5 not 0.765), the fleet drew down to roughly 12% of peak equity at some point — catastrophic. If stored as cumulative pp or basis points or dollars, far less alarming.

## Phase 1 — Discovery (next session, NO code changes)

1. Read engine/proving_ground.py — locate the max_drawdown computation
2. Identify the unit: fraction, percentage, basis points, dollars
3. Identify whether DD resets at each new peak or accumulates monotonically
4. Pull data/equity_curve.json and visually inspect the trough — confirm if -87.557 reflects a real equity collapse
5. Correlate the value with the Day 14/30 and Day 15/30 collapse points (trader.log shows both days at WR 0.0%, 3 trades each)
6. Verify dd_bench_threshold value — what level triggers FAIL?

## Two possible findings

- A: DD is real (-87.5% drawdown of peak equity). Action: tighten risk gates, investigate Day 14-15 root cause, possibly invalidate the graduation.
- B: Metric bug — wrong unit, no peak reset, or accumulation flaw. Action: fix calc, recompute historical scorecard, re-evaluate dd_bench_status across all cohort days.

## HALT condition

Phase 1 discovery only. No code changes. Captain decision required to choose remediation path.

## Cross-references

- data/proving_ground.db running_scorecard (max_drawdown column)
- data/equity_curve.json (visual confirmation)
- trader.log Day 14/30 and 15/30 entries (WR 0.0%)
