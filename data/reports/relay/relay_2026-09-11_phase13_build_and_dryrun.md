# Relay — Phase 1.3 alpha-scaled sizing: built behind a flag, NOT enabled. 2026-09-11

Per XO Priority 3, item 8: built to the spec (`docs/XO_PLAN_2026-09.md` lines
146-339, verbatim copy also at `relay_2026-09-11_phase13_spec_for_review.md`).
**Flag is OFF. Nothing sizes anything real.** This doc is the diff summary +
first dry-run output, for review before any decision to enable.

## Scope decision — what's built vs. explicitly held

The full "Phase 1.3" section of the plan doc has three parts. Only the first
is built here, matching the directive's own wording ("Alpha-scaled sizing on
buy()'s existing sizing_multiplier; fail CLOSED where a confidence bucket has
no calibration data") — the other two are explicitly out of scope, per the
spec's own text:

1. **Alpha-scaled sizing tier — BUILT, behind the flag.** See below.
2. **The confidence_modifier / calibration_map coordination unification** —
   NOT built. The spec's own text is explicit: "Do not build any of this
   until calibration_map has materially more live data than it has today...
   not a 'wait a few days' condition." Confirmed still true tonight (see
   "Live data reality check" below).
3. **`UNIVERSAL_MIN_CONVICTION` 0.65->0.70** — NOT built. The spec calls this
   "the actual restoration" and cites supporting measurement, but the
   directive didn't name it and it's a real live-gate threshold change
   (affects every player, not just McCoy) — flagging it as a distinct,
   separate decision rather than bundling it into this sizing build.

## What's built

**New flag** (`config.py`): `PHASE_1_3_ALPHA_SIZING_ENABLED = False`,
`PHASE_1_3_ALPHA_SIZING_PLAYER_IDS = ("ollama-plutus",)`. Deliberately a plain
constant, not a `live_flag()` (DB-flippable without restart) — flipping this
should require a code review + restart, not a one-line DB write, given how
new the build is.

**Scope: McCoy only, stock BUYs only.** Matches the spec's entire measured
basis (the 30-day funnel, the 42-trade/71.4% calibration finding are all
McCoy-specific). Every other player and every non-stock action is completely
unaffected — same code path as before this change, zero behavior difference.

**`engine/phase13_sizing.py`** (new) — `compute_alpha_sizing_multiplier
(symbol, regime, stated_confidence) -> (multiplier, reason)`:
- Reads the symbol's latest `composite_alpha.composite_score`
  (`data/alpha_signals.db`).
- `>= 0.6` -> 1.0 (full) **only if** `calibration_map.has_calibration_
  evidence(regime, stated_confidence)` is True; otherwise fails closed to 0.5.
- `0.3-0.6` -> 0.5 (base).
- `< 0.3` or no alpha data -> 1.0 (unaffected — matches `buy()`'s existing
  default, i.e. Phase 1.3 doesn't engage at all).
- Never raises — any internal failure falls back to 1.0 (unaffected), same
  fail-safe posture as the rest of this codebase's signal-lookup helpers.

**`engine/calibration_map.py`** — added `has_calibration_evidence(regime,
stated_confidence) -> bool` (new function, additive). Does **not** touch
`get_calibrated_confidence()`'s own fail-open behavior or signature, per the
spec's explicit instruction ("the gate still needs it... apply the
fail-closed rule in the sizing layer instead").

**`engine/paper_trader.py::execute_signal()`** — BUY branch now computes
`sizing_multiplier` via the above (gated on the flag + player scope) before
calling `buy()`, and logs `[PHASE-1.3-SIZING] {player} {symbol}
multiplier=X — {reason}` either way, so a live run's decisions are
auditable line-by-line, not just a number. Wrapped in try/except with the
same 1.0 fallback — a Phase 1.3 error can never block or alter a trade
beyond its own sizing contribution.

**`scripts/phase13_sizing_dry_run.py`** (new) — read-only demonstration
against live data. Does not touch the flag, does not call `buy()`/
`execute_signal()`, does not write anything. Full output below.

## Verification

- `py_compile` clean on all 4 modified/new files.
- `tests/pre_restart.py`: still 29/32 (same 3 pre-existing failures as this
  morning's session — `DalioFallbackProvider` import, Anderson provider
  instantiation, power-hour convergence gate — reproduced identically,
  **zero new failures** from this change).
- Confirmed zero live behavior change while the flag is False: the BUY
  branch's new block is entirely inside `if PHASE_1_3_ALPHA_SIZING_ENABLED`,
  falls through to the exact same `buy(..., sizing_multiplier=1.0, ...)`
  call as before this change when the flag is off.

## First dry-run output (live data, 2026-09-11 ~15:00 MST)

Live regime at run time: **BEAR_CROSS**. 20 symbols with real
`composite_alpha` data, 3 sample confidence levels each:

```
symbol      alpha   conf multiplier  reason
----------------------------------------------------------------------------------------------------
AMZN        0.594   0.65       0.50  composite_alpha=0.594 in [0.3, 0.6) -- base tier
GOOGL       0.302   0.65       0.50  composite_alpha=0.302 in [0.3, 0.6) -- base tier
INTC        0.302   0.65       0.50  composite_alpha=0.302 in [0.3, 0.6) -- base tier
MU          0.302   0.65       0.50  composite_alpha=0.302 in [0.3, 0.6) -- base tier
(16 other symbols: alpha ranged -0.62 to 0.25, all below the 0.3 floor -> 1.0 unaffected)
```

(Full 20-symbol x 3-confidence table — 60 lines — in
`scripts/phase13_sizing_dry_run.py`'s own output; trimmed here to the
non-trivial rows. Every confidence level produces the same tier for a given
symbol today, since none of today's live alpha scores are anywhere near
`MIN_BUCKET_N`-gated territory — see the honest note below for why.)

## Honest note — the fail-closed rule is effectively ALWAYS active today

Checked directly (`fit_binned_calibration()` against live `decision_audit`):
**every single (regime, confidence-bucket) in the database right now has
fewer than `MIN_BUCKET_N=8` observations.** The entire `calibration_map` pool
is ~3 fleet-wide `trade_fire`-linked rows total — this is the exact blocker
the spec's own "Coordination question" section describes, and it applies
here too, not just to the (separately-held) confidence-coordination piece.

**Practical consequence:** if this flag were enabled today, it could
currently only ever produce 1.0 (alpha < 0.3, unaffected) or 0.5 (base tier —
either alpha in [0.3, 0.6), or alpha >= 0.6 but fails closed). **No live
trade can currently reach the 1.0 full tier via real calibration evidence** —
the evidence simply doesn't exist yet anywhere in the DB. This is a genuinely
conservative starting state, not a loophole: the feature can only ever size
DOWN from the current unconditional-1.0 baseline until real calibration data
accrues, never up.

Since no live data could demonstrate the "evidence present -> full tier"
branch, it's demonstrated with a clearly-labeled synthetic example instead
(mocked inputs, not from any table):

```
Synthetic example (fabricated inputs, NOT from any live table):
  alpha=0.75 (fabricated), evidence=True (fabricated)  -> multiplier=1.0  reason=composite_alpha=0.750 >= 0.6, calibration evidence present for regime=BULL_CROSS stated_confidence=0.90 -- full tier
  alpha=0.75 (fabricated), evidence=False (fabricated) -> multiplier=0.5  reason=composite_alpha=0.750 >= 0.6 but NO calibration evidence for regime=BULL_CROSS stated_confidence=0.90 -- fail-closed to base tier
```

## Status

**Not enabled. `PHASE_1_3_ALPHA_SIZING_ENABLED` stays `False`** until the
Admiral reviews this doc + the diff and makes an explicit go/no-go call. No
restart was done specifically for this (the change is provably a no-op while
the flag is off — confirmed above — so there's no urgency; it'll ride along
naturally on whatever this session's next restart is for other work, or wait
for an explicit decision).

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01PZs3iBLLgQUffpHn8yfJzi
