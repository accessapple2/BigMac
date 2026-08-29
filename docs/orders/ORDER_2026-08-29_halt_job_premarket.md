# ORDER — HALTED: premarket (job)

**Date:** 2026-08-29
**Action:** halt
**Reason:** Older, independent scanner (not part of the Kirk-briefing pipeline). QUESTION_fleet-standdown-reversal.md flagged it as needing its own explicit call, not bundled into the 2026-08-29 successor order. Deferred, not decided.

## Reversal checklist
- Resume-by: (not set)
- Review-by: 2026-09-15
- Reverse with: `scripts/fleet_lifecycle.py revive premarket --reason "..."`
- Until reversed, this state is intentional — a sentinel finding against this target before its review-by date is a false alarm; after it, it is a legitimate 'this pause was forgotten' alert.
