# ORDER — HALTED: morning-cd-instr (job)

**Date:** 2026-08-29
**Action:** halt
**Reason:** Already dead since 2026-05-22, well before the 07-22 stand-down — unrelated failure, deferred for separate investigation, not bundled into the stand-down reversal.

## Reversal checklist
- Resume-by: (not set)
- Review-by: 2026-09-28
- Reverse with: `scripts/fleet_lifecycle.py revive morning-cd-instr --reason "..."`
- Until reversed, this state is intentional — a sentinel finding against this target before its review-by date is a false alarm; after it, it is a legitimate 'this pause was forgotten' alert.
