# SPEC — Wave 2: Bracket Sizing (DESIGN ONLY, observation-first)
Status: design draft 2026-06-01 (overnight). No build. Gated AFTER graduation (W0 DSR≥0.95 ∧ PBO≤0.3).

## Goal
Attach a risk-sized bracket (entry/stop/target + position size) to each signal, so a graduated
setup can be sized sanely. Phase 1 is OBSERVATION-ONLY: compute + log the size on shadow signals
for W0 (does sizing improve realized R?), NEVER route it to execution.

## Sizing ladder (simplest first)
1. **Fixed-fractional (default, pre-graduation):** risk = 0.5–1.0% of book equity per trade.
   shares = floor(risk$ / |entry − stop|). Stop from the signal (deep_scan stop_price). This is the
   baseline; no edge estimate needed.
2. **Fractional-Kelly (post-graduation only):** once a setup clears the validation gate, size at
   **≤0.25× Kelly** off its W0 expectancy: f* = expectancy_R / avg_win_R² (per-setup, per-horizon);
   cap at 0.25·f* AND at the 1% fixed-fractional ceiling (whichever smaller). Kelly is an UPPER bound,
   never the floor. Undeflated/thin setups (n<50) stay fixed-fractional.

## Guards (all hard, pre-size)
- **Correlation/exposure:** cap aggregate risk per sector + per correlated cluster (e.g. mega-cap
  tech); cap total open risk at N%. Reject if adding the position breaches.
- **Earnings/IV blackout:** no new entry within X days of earnings; skip if IV-rank > threshold
  (premium too rich / event risk). Reuse existing earnings calendar + IV-rank.
- **P95 drawdown sizing tie-in:** size so a P95 historical drawdown of the setup stays within the
  book's max-DD budget (from W0 equity_curve max_drawdown_R).

## Observation-only hook
`size_bracket(signal) -> {shares, risk$, stop, target, kelly_f, guards_passed}` — computed and
LOGGED beside each shadow signal (a new column / sidecar table), fed into W0 so we can score
"sized R" vs "raw R". It does NOT call buy(). Execution wiring is a separate, post-graduation,
Admiral-gated step.

## Acceptance (when built)
- Fixed-fractional sizes reproduce hand-calc on fixtures.
- Kelly capped at 0.25× and at the 1% ceiling; thin setups never Kelly-sized.
- Guards reject correlated/earnings/IV-rich entries.
- Observation-only: zero order-path calls (assert no paper_trader import).
