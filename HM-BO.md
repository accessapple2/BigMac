# HM-BO — direction_correct Column Audit

**Status:** DISCOVERY → likely SHIP in same session if trivial
**Origin:** logs/trader_error.log shows 4 occurrences today of "_fetch_scorecards failed: no such column: direction_correct" cascading into symbol_scorecard timeouts at dashboard/app.py:3919. The column is referenced but does not exist in trader.db.

## Phase 1 — Discovery

1. grep -rn "direction_correct" engine/ dashboard/ scripts/ — list all references
2. sqlite3 data/trader.db ".schema" | grep -i "direction" — identify if the column was renamed
3. git log -S "direction_correct" — find when it was added/removed in history
4. Identify the canonical replacement column (likely something like signal_correct or direction_aligned)

## Phase 2 — Ship (if trivial)

If a simple rename: update dashboard query, anchor # === HM-BO ===
- Self-verify: tail -f logs/trader_error.log, hit /api/symbol/SPY/scorecard, confirm no direction_correct error appears
- Commit, push, close

## HALT condition

If the column was genuinely dropped without replacement and downstream logic depends on direction-correctness data, HALT and document. Do not synthesize the column or stub it without Captain decision.

## Done-when

- direction_correct error count goes from 4/day to 0
- symbol_scorecard timeout incidents stop
- Anchor # === HM-BO === present in dashboard query that was fixed
