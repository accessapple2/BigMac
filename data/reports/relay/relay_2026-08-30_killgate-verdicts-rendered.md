# Relay — 2026-08-30 — door1 + ollie-machine kill-gate verdicts rendered (5-week-late)

## Context

Follow-on to tonight's `HM-QUIETDOWN-STALE-JOBS-CLASSIFICATION` report,
which flagged that `door1_kill_gate_check.py` and
`ollie_machine_kill_gate_check.py` were both disabled by the 2026-07-22
quietdown two days before their pre-committed **2026-07-24** verdict
date — meaning neither gate ever actually fired. Admiral directive
tonight: render both verdicts against the original as-designed window
using historical data (not today's live state), record honestly-dated
ledger entries, apply the gate's own consequence on any failure, and
note whether the intervening 5 weeks would change either verdict.

## What was done

- Replicated `compute_g1`/`g2`/`g3` from `door1_kill_gate_check.py`
  directly against `data/trader.db` + read-only Alpaca `GET`s (portfolio
  history + SPY bars), scoped to DAY_0=2026-06-24 → DAY_30=2026-07-24
  rather than "today." **Door-1: KEEP-eligible, G1/G2/G3 all PASS, G4
  N/A** (unchanged — no parallel-benchmark tracking exists). Full numbers
  in `docs/XO_BACKLOG.md` under `HM-DOOR1-OLLIE-MACHINE-KILLGATE-VERDICT`.
  **Did not apply the KEEP branch's fleet-wide consequence** ("scale CSP
  book, halt everything else permanently") — that's a separate, much
  bigger call than "apply consequence on failure" authorized, and this
  gate didn't fail. Flagged for the Admiral as its own decision.
- Checked `ollie-machine`'s `trades`/`options_trades` row counts as of
  2026-07-24: **zero, both tables** — the pre-committed 2026-07-05
  trigger fires (zero trades by gate date → halt proposal). **Applied**
  via `scripts/fleet_lifecycle.py halt ollie-machine --type agent`,
  ledger id 114, order doc
  `docs/orders/ORDER_2026-08-30_halt_agent_ollie-machine.md`, reason
  dated honestly ("verdict rendered 2026-08-30 for the original 2026-07-24
  window ... 5-week overrun"), review-by 2026-09-29.
- Both gates: checked whether the 5-week gap would change anything.
  **No for either** — the CSP book made zero trades after 2026-06-29 and
  still has zero open positions today; `ollie-machine` still has zero
  trades in either table as of 2026-08-30. Today's data is numerically
  identical to what 07-24 would have shown.

## Not done this pass

Door-1's KEEP-branch consequence (fleet-wide strategy halt) — explicitly
out of scope, flagged for a separate Admiral decision. No other
fleet_lifecycle actions taken beyond the single `ollie-machine` halt.
