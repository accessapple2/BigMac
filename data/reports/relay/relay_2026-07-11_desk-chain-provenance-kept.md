# Relay: HM-DESK-CHAIN-PROVENANCE decided — KEEP, not FIX (and a real correction)

**Date:** 2026-07-11
**Commit:** `4a9f7dd`

## What was asked

"decide keep vs fix on the desk chain provenance."

## Decision: KEEP. No code touched.

Before implementing the "proposed fix" from the earlier scoping pass, I
re-verified both pieces directly against live code — and both turned out
to be less urgent/less broken than that pass concluded.

**1. `execution_status` "never EXECUTED" is not a bug.**
`engine/paper_trader.py`'s own module docstring documents a deliberate
three-tier routing model: `trading` (real Alpaca order → `EXECUTED`),
`paper` (simulated, DB-only, **"No external broker calls" by design** →
`SIMULATED`), `tracking` (log-only). Traced the full write path live
(`buy()` → `ai_brain.py:1653/1657` → `update_signal_status`) and
confirmed it's working exactly as documented. The fix the first scoping
pass proposed — swap the whitelist gate for a check against
`alpaca_status='filled'` — would have been a no-op at best (paper-mode
trades never call Alpaca, so they'd never satisfy that check either) and
actively wrong at worst (it would relabel genuinely-internal-simulation
trades as real broker fills). **No fix needed.**

**2. The claimed kill-gate urgency doesn't hold — verified false, corrected
in the backlog.** The original ticket (filed 2026-07-05) and the first
scoping pass (2026-07-11, earlier today) both said the upcoming
2026-07-24 kill-gates "read exactly this kind of chain data." I read
both actual gate scripts directly
(`scripts/door1_kill_gate_check.py`, `scripts/ollie_machine_kill_gate_check.py`
— built 2026-07-10, i.e. *after* this ticket was filed): **neither
references `signal_id` or `execution_status` at all.** Both just do raw
`COUNT(*)`/aggregate queries against `trades`/`options_trades`. The
urgency claim was a reasonable anticipation at filing time that never
got re-checked once the gate scripts actually shipped — corrected in the
backlog rather than left standing.

**The 72% `signal_id` mislink remains real and open**, but deprioritized:
not gate-critical (per #2), its write-site is still genuinely unknown
(the corrupted rows don't match any currently-visible code path), and
the Desk's own display already guards against ever showing a wrong fill.
Fixing it blind — without knowing the actual cause — risks papering over
a real bug rather than catching it. Left as its own dedicated future
trace, not rushed.

## Why this matters as a process note

This is the second ticket handled today where the first-pass scoping
conclusion needed a second look before acting on it — the shadow-pipeline
ticket's initial framing held up under a "keep vs kill" decision, this
one didn't fully hold up under "keep vs fix." Worth flagging: scoping
passes (including my own) can inherit stale assumptions from a ticket's
original filing date without re-verifying against what actually shipped
since. Re-checking against live code before implementing a "proposed fix"
caught this before any code changed.

## Verification

- `engine/paper_trader.py:1-35` (module docstring), `:1732-1746`
  (`route_mode=="trading"` gate around `_forward_to_alpaca`/
  `_persist_alpaca_fill`), `:1759` (return dict).
- `engine/ai_brain.py:1653,1657` (`update_signal_status` call site).
- `signals` schema: `execution_status TEXT DEFAULT 'PENDING'`.
- `scripts/door1_kill_gate_check.py`, `scripts/ollie_machine_kill_gate_check.py`
  — grepped both for `execution_status`/`signal_id`/`broker_executed`,
  zero matches; read their actual SQL directly.

## Open items

`HM-ERROR-FILTER-CONSOLIDATION` is the one remaining item from the
original "1 through 4" batch still awaiting a decision. Everything else
from today's departure-hardening pass is closed or decided.
