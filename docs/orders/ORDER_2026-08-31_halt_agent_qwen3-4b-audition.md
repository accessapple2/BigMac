# ORDER — HALTED: qwen3-4b-audition (agent)

**Date:** 2026-08-31
**Action:** halt
**Reason:** HM-SEAT-CONSOLIDATION 2026-08-31: audition suspended, not concluded -- qwen3:4b was a fourth distinct model on a 16GB box already thrashing between plutus-v1, ministral-3:3b and qwen3:8b. No verdict reached on the model itself; resume the audition when the workstation (9900X + 2x 2080 Ti) has the VRAM to run it without evicting the trader's advisory seat. State applied by direct SQLite UPDATE at 09:35; this order backfills the ledger row.

## Reversal checklist
- Resume-by: (not set)
- Review-by: 2026-09-30
- Reverse with: `scripts/fleet_lifecycle.py revive qwen3-4b-audition --reason "..."`
- Until reversed, this state is intentional — a sentinel finding against this target before its review-by date is a false alarm; after it, it is a legitimate 'this pause was forgotten' alert.
