# Relay — 2026-08-30 — Door-1 KEEP consequence waived + systemic staleness check

## Context

Follow-on to tonight's `HM-DOOR1-OLLIE-MACHINE-KILLGATE-VERDICT` and the
case memo (`docs/HM-DOOR1-KEEP-CONSEQUENCE-MEMO-2026-08-30.md`). Admiral
reviewed the memo and ruled: **WAIVE-WITH-NOTE** on the KEEP branch's
"halt all other strategies permanently" consequence. Verdict KEEP itself
is unaffected. Also ordered a systemic check: search the rest of
`docs/XO_BACKLOG.md` for any other pending pre-committed consequences
that could fire stale the same way Door-1/ollie-machine almost did, and
add an expiry/data-freshness condition to any found.

## What was done

- Replaced the "PENDING RE-ARGUMENT" ledger text with the Admiral's exact
  waiver wording (grounds: present-reason test fails, no data shows any
  book competing with Door-1's dormant CSP edge; 06-19 precedent never
  enforced crowd-out in practice; waiver scoped to this instance only,
  future gates need an explicit expiry clause).
- Audited the rest of the backlog for the same bug class. Found three
  candidates, confirmed none need a patch:
  - `options-sosnoff`/`qwen3-8b-flash` incumbent-audition gate (both the
    Aug 15/16 dated version and the 2026-07-05 "6 weeks" version under
    `HM-ROSTER-RECONCILE-8` — same underlying mechanism). Already safe:
    the suspension is a hardcoded code flag in
    `engine/crew/audition_tracking.py`, gated on `HM-ROUTE-TO-BROKER`
    shipping (confirmed still unstarted, still 🔴) — not a wall-clock
    check, so it structurally cannot fire on a stale date.
  - Sniper Mode / `ollie-auto` proving-ground Day-60 `kill_warning`
    (2026-06-09). Different architecture (live daily evaluator with its
    own state machine, not a doc-based standing order) — `ollie-auto` is
    currently `shadow`, resolved through general fleet reclassification
    rather than the evaluator's own terminal states. Flagged for its own
    look, not patched here — a doc-level expiry clause doesn't apply to
    live code, and building one would be exactly the "new architecture"
    scope the memo was told to avoid.
  - No other date-triggered consequence pattern found in the rest of the
    file.

## Not done this pass

`ollie-machine` (ledger 114, review-by 09-29) untouched, as ordered.
S7/options roster untouched. No code changes — this was a ledger-wording
and documentation-audit task only.
