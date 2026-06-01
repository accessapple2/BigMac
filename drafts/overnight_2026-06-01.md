# Overnight Queue — 2026-06-01 (Scotty, autonomous)

Read-only / safe-to-fail. No execution, no order path, no agent/producer turned on,
shadow feed + W0 scoring left to accrue untouched, no bare launchd, sacred-data
(archive/rename only), frontend = diagnose/scope only. Decisions/restarts HELD for
Admiral morning review. Worked in phase order; committed incrementally.

## PHASE 1 — Validation readiness (read-only)

### DSR/PBO readiness vs graduation gate (DSR≥0.95 ∧ PBO≤0.30)
Computed from existing W0 `scored_predictions` (IN-SAMPLE historical) for the two proven
edges the shadow bridge re-emits:

| setup | n | DSR 1d | DSR 3d | DSR 5d | DSR 10d |
|---|--|--|--|--|--|
| **relative_strength** | 444 | 1.00 ✓ | 1.00 ✓ | 1.00 ✓ | 1.00 ✓ |
| bull_flag | 38 | 0.49 ✗ | 1.00 ✓ | 0.997 ✓ | 0.95 ✗(marginal) |

expectancy_R: relative_strength +0.41/+0.71/+0.52/+0.34 (1/3/5/10d); bull_flag +0.08/+0.73/+0.56/+0.33.

**relative_strength clears the DSR leg at every horizon (n=444).** bull_flag clears at 3d/5d
(thin n=38; fails 1d, marginal 10d).

### ⚠️ PBO leg is NOT honestly evaluable yet
CSCV PBO across only the 2 setups = **0.55 (flagged "fragile")** — but that's a **degenerate-N
artifact**: with n_strategies=2 the IS-best generalizes to OOS at ~coin-flip → PBO ~0.5 by
construction (median_logit −0.69). PBO is only meaningful over a LARGE config universe (cf. the
345-sweep's 36 strategies). **The PBO gate cannot be cleared/failed on a 2-setup matrix.**
GAP to surface: a meaningful PBO needs either (a) parameter-variant configs per setup, or
(b) per-symbol sub-strategies, accrued forward. Until then, DSR is the operative read and PBO
is "not yet computable."

### Forward shadow accrual
Shadow signals emitted **2026-05-31 20:09** (22 rows). **0 closed outcomes yet** — they need
forward trading-day bars; the 1d horizon first matures ~2026-06-02 (06-01 is Sunday). The
graduation gate's *forward* (true OOS) confirmation will populate over the coming sessions;
today's DSR is in-sample only.

### Distance to clearing (the gate I'm waiting on)
- **relative_strength:** in-sample DSR ✓ at all horizons. Remaining: (1) FORWARD shadow sample
  to confirm OOS (accruing), (2) a non-degenerate PBO. **Closest to graduation.**
- **bull_flag:** in-sample DSR ✓ at 3d/5d only; thin (n=38). Needs more sample + forward.
- **unusual_oi:** no closed history (new aggregate-flow setup); accruing from zero.

### W0 forward-scoring health
- All **22 shadow signals tracked** in `signal_outcomes` (0 untracked / stuck). Outcome-tracker
  daemon healthy; last_update = emit time (market closed Sunday → no price re-poll yet; will
  update tracked_high/low/current on the next RTH cycle). No stuck-untracked signals.
- Shadow boundary intact: 0 shadow-era trades; executor chokepoint + consumer skip both live.

**HOLD items for Admiral:** none blocking — relative_strength is the lead graduation candidate
(in-sample DSR ✓); awaiting forward shadow accrual + a non-degenerate PBO before any go.
