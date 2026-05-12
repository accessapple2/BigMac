# HM-BM — Recon Aggregator Semantic Fix

**Status:** DISCOVERY → HALT for Captain decision
**Origin:** 2026-05-12 recon drift NTFY. data/reconciliation/2026-05-12.json shows 6 fractional positions (LLY/MA/SPGI/UNH/WMT/XOM) reporting ~2x drift between internal and Alpaca. Root cause confirmed: both alpaca-mirror AND ollie-auto hold the same 6 symbols. Recon sums them = 2x of Alpaca actual. Same divergence the failing gap_bench_status has been flagging since cohort start.

## Phase 1 — Discovery (next session, NO code changes)

1. Read engine/reconciliation.py end to end. Document how internal_qty is computed for routed positions.
2. grep for usages of player_id="alpaca-mirror" — verify it is the fill-truth reflection of Alpaca
3. grep for usages of player_id="ollie-auto" — verify it holds signal-intent quantities
4. Read engine/paper_trader.py around the buy/sell paths that touch both players, identify why both write positions for the same symbol
5. Write up the three options below with implications and a recommendation.

## Three options for Captain decision

- Option A: Exclude alpaca-mirror from the internal book sum in recon. Mirror equals Alpaca by definition; comparing mirror+ollie-auto to Alpaca is circular and inflates drift.
- Option B: Compare ollie-auto only to Alpaca. Treats ollie-auto as canonical intent book. Mirror becomes purely observational.
- Option C: Re-architect — eliminate the dual-write pattern entirely. Single canonical position record per (player, symbol) with no mirror.

## HALT condition

Phase 1 only this session. No code changes. Captain decision required before Phase 2 ship.

## Cross-references

- data/reconciliation/2026-05-12.json (today payload)
- HM-AS-β gap_bench_status FAIL since 2026-04-28 (same divergence, surfaced differently)
- engine/reconciliation.py
