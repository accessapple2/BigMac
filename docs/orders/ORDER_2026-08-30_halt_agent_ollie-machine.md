# ORDER — HALTED: ollie-machine (agent)

**Date:** 2026-08-30
**Action:** halt
**Reason:** ollie-machine kill-gate FAIL, verdict rendered 2026-08-30 for the original 2026-07-24 window (5-week overrun -- the 2026-07-22 quietdown stand-down disabled ollie_machine_kill_gate_check.py two days before it could fire even once at the gate date). Zero trades in trades+options_trades as of 2026-07-24 (creation 2026-06-01); still zero as of 2026-08-30, so the intervening 5 weeks do not change the verdict. Per Admiral's pre-committed 2026-07-05 decision (docs/XO_BACKLOG.md HM-OLLIE-MACHINE-KILLGATE): zero trades by gate date = halt proposal to Admiral. Applied directly per 2026-08-30 Admiral authorization to render this pass's verdicts and apply gate consequences on failure.

## Reversal checklist
- Resume-by: (not set)
- Review-by: 2026-09-29
- Reverse with: `scripts/fleet_lifecycle.py revive ollie-machine --reason "..."`
- Until reversed, this state is intentional — a sentinel finding against this target before its review-by date is a false alarm; after it, it is a legitimate 'this pause was forgotten' alert.
