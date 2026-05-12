# HM-BR — intraday_snapshots.trade_date Column Audit

**Status:** DISCOVERY → likely SHIP in same session if trivial
**Origin:** HM-BE-historic Phase 1 (4714181) surfaced an adjacent issue: _fetch_intraday_snapshots references a non-existent trade_date column on the intraday_snapshots table. Same shape as HM-BO (5560cbb).

## Phase 1 — Discovery

1. grep -rn "trade_date" engine/ dashboard/ scripts/ | grep -i "intraday" — list all references
2. sqlite3 data/trader.db ".schema intraday_snapshots" — confirm actual columns
3. Identify the canonical replacement (likely snapshot_date or as_of_date or timestamp)
4. Check git log for rename history: git log -S "trade_date" -- '*.py' | head -20
5. grep _fetch_intraday_snapshots logs/trader_error.log — confirm current failure rate (if any). Note: HM-BO origin was stale (4 errors were all from 2026-04-12); same may be true here.

## Phase 2 — Ship (if trivial)

If simple rename: update query, anchor # === HM-BR === / # === /HM-BR ===
- Self-verify: trigger one _fetch_intraday_snapshots call (via dashboard endpoint or direct call), confirm no trade_date error in tail -50 logs/trader_error.log
- Commit, push, close

## HALT condition

If trade_date column was dropped without canonical replacement AND downstream logic depends on per-day intraday snapshot grouping, HALT and document. Do not synthesize the column or stub it without Captain decision.

## Done-when

- _fetch_intraday_snapshots no longer raises trade_date errors
- Anchor # === HM-BR === present in fixed query
- Closure note in commit message

## Cross-references

- HM-BO (5560cbb) — parallel direction_correct fix in adaptive_tuner.py
- HM-BE-HISTORIC-PHASE1.md (4714181) — origin
