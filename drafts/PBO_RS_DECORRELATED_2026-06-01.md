# PBO — relative_strength, DECORRELATED grid — 2026-06-01

Answers: was the first PBO=0.48 a collinearity artifact, or is relative_strength genuinely
fragile? Harness `strategies/pbo_rs_decorrelated.py` (observation-only, .venv-backtest).
Numbers: `PBO_RS_DECORRELATED_2026-06-01.json`. **Nothing changed — no execution, no graduation.**

## Grid design (genuinely independent configs)
Varies the real decision axes, with **entry-trigger** and **universe-slice** as the primary
decorrelators (lookback/threshold/hold alone just re-tweak one signal → collinear):
| axis | values | why it decorrelates |
|---|---|---|
| lookback L | 40, 60, 90 | RS measurement window |
| RS-rank threshold P | 70, 85 | selectivity |
| **entry trigger E** | level · breakout · trend · accel | **different signal MATH** — level (rank≥P) vs onset (crossing up through P) vs trend-confirmed (price>L-SMA) vs acceleration (RS excess rising) → different names/timing |
| holding horizon H | 3, 10 | rebalance/hold |
| **universe slice U** | all · liquid · illiquid | **different NAMES** — RS ranked *within* each trailing-$volume half → independent portfolios |
= **144 configs** (dropped any >95% cash). T=115 daily returns, 2025-11-07→2026-04-24, 494 symbols
(liquid/illiquid = 247 each). 9330 CSCV splits (16 blocks, purge/embargo 10).

## Decorrelation check (the validity gate for this whole exercise)
| grid | mean pairwise &#124;corr&#124; |
|---|---|
| old (36, lookback×pct×hold only) | **0.8941** |
| new (144, decorrelated) | **0.6308** |
Drop of **−0.26** → the grid genuinely decorrelated (residual 0.63 is the irreducible shared RS
factor — variants of one setup can't go to zero). So the new PBO is a trustworthy read, not a
collinearity artifact.

## Result
| metric | value |
|---|---|
| **PBO** | **0.6348 → FRAGILE** |
| median logit | **−0.7348** (negative) |
| gate leg (≤0.30) | **FAIL** |
| Sharpe (ann.) | median 1.91; top ~3.2 |

## Verdict — GENUINELY FRAGILE. DO NOT graduate.
The 0.48 was **not** masking a clean strategy. On a genuinely decorrelated grid PBO went **up to
0.63** (further from the 0.5 coin-flip, toward fragile), and **median_logit is negative (−0.73)** —
i.e. the in-sample-best config lands **below** the out-of-sample median on 63% of splits, worse than
chance. Translation: **you cannot reliably pick which RS parameterization will work out-of-sample.**
If collinearity had been inflating the first read, decorrelating would have pulled PBO toward/under
0.30; it did the opposite.

**This blocks graduation** (gate = DSR≥0.95 ∧ PBO≤0.30; PBO leg fails clearly). The earlier
"PBO<0.30 → forward OOS becomes the arbiter" branch does NOT apply — PBO≥0.30, so the gate holds.

### Honest scope of the claim
PBO tests **config-selection robustness**, and it fails. DSR (aggregate edge vs a null, deflated)
still passes — so the RS *factor* may carry some edge in aggregate, but no *specific parameterization*
is reliably selectable OOS, which is what you'd need to deploy one. Caveat: T=115d (1y minus lookback)
is a moderate window; a longer history would firm the read, but PBO-up + negative-median-logit on a
decorrelated grid is an unambiguous fragility signature. **Recommendation: keep relative_strength
shadow/observation; do not graduate; let forward OOS accrue, but treat graduation as PBO-blocked,
not pending-forward-confirmation.**
