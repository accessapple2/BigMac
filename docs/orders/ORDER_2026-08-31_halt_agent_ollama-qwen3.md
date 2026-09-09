# ORDER — HALTED: ollama-qwen3 (agent)

**Date:** 2026-08-31
**Action:** halt
**Reason:** HM-SEAT-CONSOLIDATION 2026-08-31: parked to cut Ollama model-swap thrash on a 16GB box -- the swap probe logged constant LOADED/EVICTED churn between plutus-v1, ministral-3:3b and qwen3:4b. Fleet reduced to 3 live ollama seats across 2 distinct models (plutus-v1, qwen3:8b). State was applied by direct SQLite UPDATE at 09:35 and this order backfills the ledger row that bypass skipped.

## Reversal checklist
- Resume-by: (not set)
- Review-by: 2026-09-30
- Reverse with: `scripts/fleet_lifecycle.py revive ollama-qwen3 --reason "..."`
- Until reversed, this state is intentional — a sentinel finding against this target before its review-by date is a false alarm; after it, it is a legitimate 'this pause was forgotten' alert.
