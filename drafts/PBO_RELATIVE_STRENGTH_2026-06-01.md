# PBO — relative_strength (graduation gate, leg 2) — 2026-06-01

Harness: `strategies/pbo_relative_strength.py` (observation-only). Data: 1y daily Close
for the live `rs_rank` universe (523 fetched → 494 full-history) + SPY, via
`get_bulk_daily_ohlcv`. Compute under isolated `.venv-backtest` (numpy/pandas +
`strategies.validation.cscv_pbo` only). Full numbers: `PBO_RELATIVE_STRENGTH_2026-06-01.json`.

## Result
| metric | value |
|---|---|
| configs (N) | **36** (lookback {20,40,60,90} × pct {70,80,90} × hold {3,5,10}) |
| period (T) | 115 daily returns, 2025-11-07 → 2026-04-24 |
| CSCV splits | 9330 (16 blocks, purge/embargo = 10) |
| **PBO** | **0.4787 → FRAGILE** |
| gate leg (≤0.30) | **FAIL** |
| median logit | 0.0541 |
| Sharpe (ann.) | median 2.05; top L90_P70_H3 = 2.87; bottom L20_P90_H3 = 1.45 |

## Read
This is the **non-degenerate** PBO the gate needs (vs the earlier N=2 0.55 coin-flip
artifact). It is a **real FAIL of the ≤0.30 leg**: the in-sample-best config lands
below the OOS median on ~48% of splits — i.e. you cannot robustly *select* a winning
parameterization out-of-sample.

**Caveat (honest):** all 36 configs are highly collinear variants of the same RS
signal (uniformly high Sharpe 1.4–2.9). When the config field is near-duplicate, the
IS-best is nearly interchangeable with the pack, which pushes PBO toward ~0.5 — partly
a *config-redundancy* signature, not proof RS is a fluke. The DSR leg (deflated vs a
null, trial-count-penalized) is the stronger evidence of a real directional edge and
it clears. PBO here tests **config-selection robustness**, and that is what's fragile.

## Implication for the gate (Admiral decision, not built)
The gate as written (DSR≥0.95 **AND** PBO≤0.30) is **NOT cleared** for relative_strength.
Three honest paths:
1. **Hold the gate** → relative_strength is NOT graduated; keep it shadow/observation.
2. **Decorrelate the config space** (e.g. RS vs different benchmarks/sectors, vol-scaled
   entries, distinct exit rules) so PBO measures real selection risk rather than
   near-duplicate noise, then re-run.
3. **Re-weight the gate** → treat DSR as primary edge evidence and PBO as a
   config-selection caution (document the collinearity), with forward OOS shadow
   accrual as the tie-breaker. The forward shadow sample (1d matures ~2026-06-02)
   is the cleanest true-OOS confirmation and is accruing.

Recommend **path 1 + accrue forward** until a decorrelated grid (path 2) is built —
do not graduate on a fragile PBO.
