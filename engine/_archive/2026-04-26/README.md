# Archived 2026-04-26 — Sunday Drydock Audit

These engine/ tools were identified in OT_AUDIT_2026-04-26 as having
no real Python imports from active code, no routes, and no recent log activity.

Moved here (not deleted) per the sacred-data rule and archive convention.

## To restore any tool

```bash
mv ~/autonomous-trader/engine/_archive/2026-04-26/<tool>.py ~/autonomous-trader/engine/
```

## Expiry

If none of these have been retrieved by **2026-07-25** (90 days), they
can be permanently deleted.

## Triggered by

Admiral "approved — ship Recommendations #2 and #3" decision, 2026-04-26.
Audit document: ~/autonomous-trader/docs/OT_AUDIT_2026-04-26.md

## What's here and why

| File | Reason |
|------|--------|
| super_backtest_v3.py | Superseded by v3b → v5. Only self-references in header comments; no active imports. |
| super_backtest_v3b.py | Superseded by v5. Only self-references in header comments; no active imports. |
| momentum.py | Distinct from momentum_tracker.py. No imports found outside own file. |
| backfill_regime_history.py | One-shot diagnostic script. No active callers. |
| money_machine.py | Dead code preserved by sacred-data rule (see comment in app.py:12092). No active imports. |
| triple_threat.py | Standalone backtest script. No active callers. |
| deep_dive_report.py | Standalone report generator. No active callers. |
| agent_coaching_report.py | Standalone script (run manually). No active imports. |
| agent_comparison_6month.py | Standalone script (run manually). No active imports. |
| mega_backtest_6month.py | One-shot 6-month backtest runner. No active callers. |
| full_wiring_check.py | Diagnostic script. No active callers. |
| neo_matrix_diagnostic.py | Diagnostic script. Only caller was full_wiring_check (also archived). |

## What was NOT archived (paranoia check rescued)

The following audit candidates turned out to have real callers and were left in place:

- super_backtest_v2 — imported by v5 and oos
- super_backtest_v4 — imported by v5
- fast_scanner — `is_market_hours` used in app.py:416
- regime_detector — used in app.py (3 call sites)
- warp10_engine — used in app.py + crew_scanner
- spy_wall_strategy — used in crew_scanner
- daily_enrichment — lazy import in main.py
- war_room_feed — used in red_alert
- sr_heatmap — serves /api/volume-profile/{symbol} (first live registration)
- event_shield — used in phase4_routes + ready_room_routes + app.py
- recovery_protocol — lazy import in main.py
- super_trader — used in app.py + crew_scanner
- openai_text — used in app.py (violates Free Models First but active)
- openbb_data — used in app.py
- finmem_memory — used in brain_context
- thetadata_spx — used in phase4_routes
- gemini_free_tier — used in app.py + crew scripts
- generated_assets — used in app.py
