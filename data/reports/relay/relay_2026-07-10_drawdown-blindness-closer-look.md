# Relay: closer look at drawdown/options-P&L blindness (S6, work block 9)

**Date:** 2026-07-10
**Commit:** `7973601` (pushed to `exec-pipeline`) — docs only, no code
**Prior work block:** `HM-DRAWDOWN-BLIND-TO-OPTIONS-PNL` filed (relay
`51e37f5`) in the second CSP/MLEG sweep block.

## Ask

"check the drawdown blindness gap more closely."

## What was found

**Blast radius: exactly one agent, today.** `check_drawdown()` only runs
for `ai_players` roster members (`ai_brain.py::_run_player()` iterates
that table). Checked every options/CSP/spread-trading `agent_id` in
`options_trades` against `ai_players` directly:

- `options-sosnoff` — the only actual `ai_players` row.
- `shadow-qwen35-csp`, `strategy:bull_spread_v1`, `swingdesk-manual`,
  `test-door1-regression` — **none of these are `ai_players` rows.**
  They run through entirely separate scheduling systems (shadow-CSP
  audition, `strategies/executor.py`'s own scheduler, SwingDesk's manual
  API). `check_drawdown()` structurally cannot apply to them.

**Current practical impact: zero.** `options-sosnoff`'s real-quotes-era
CSP P&L is exactly $0 — no real-quotes-era closes exist yet, so there's
nothing for the gap to be hiding right now. (`shadow-qwen35-csp`, outside
the gate anyway, is actually +$2,511.39 real — not a loss.) The gap is
real and will matter the first time a CSP-trading roster member posts an
actual loss, but it isn't masking anything today.

**Fix design, and a correction to the original filing:** the "double-
counting risk against the CSP notional cap" flagged in the original
entry doesn't hold up under closer inspection. `get_csp_exposure()`
(the notional/margin cap) reads the shared `options_books.current_cash`
pool for sizing; a fixed `check_drawdown()` would use
`get_portfolio_with_pnl()`'s `total_value_restated` — agent-scoped via
`options_trades.agent_id`, fully independent computation path, no
overlap. Traced the actual formula
(`portfolio["cash"] + positions_value + csp_pnl_real_quotes`) and
confirmed `csp_pnl_real_quotes` is purely additive against
`ai_players.cash` (per the existing `HM-W1F4` decoupling doctrine
already documented in the code) — folding it in would not double-count.

**The real obstacle is missing peak-tracking infrastructure, not
double-counting.** `check_drawdown()` needs a historical peak to compare
the current value against. Checked all three equity-tracking surfaces in
this codebase — none persist a restated history:
- `portfolio_history` (SQL) — raw cash+stock only, what `check_drawdown`
  reads today.
- The JSON equity-curve file (`save_equity_snapshot()`) — also raw, used
  for charting, a totally separate mechanism not read by
  `check_drawdown` either.
- `get_portfolio_with_pnl()` itself — has the restated figure, but
  live/on-demand only, no history.

Comparing a *restated* current value against the *existing raw* peak
would be apples-to-oranges and could false-trigger a halt purely from
the two figures being computed on different bases, not from any real
loss. A correct fix needs new persisted infrastructure (a new table or
column tracking restated-equity peaks over time), not a one-line field
swap — genuinely a small design/build project.

## What changed

`docs/XO_BACKLOG.md`'s `HM-DRAWDOWN-BLIND-TO-OPTIONS-PNL` entry updated
in place with all of the above. No code touched — this was investigation
only, as asked.

## Bottom line

The gap is real, well-understood now, and has a clear (if non-trivial)
path to a fix — but it's genuinely not urgent. Nobody's drawdown is
currently being hidden by it. Safe to leave filed for a dedicated future
session rather than rushed into today's mechanical-fix pattern.

## Open items (unchanged, carried forward)

1. `HM-STRATEGIES-EXECUTOR-STATUS-NEVER-SET`/`HM-SWINGDESK-CLOSE-
   PHANTOM-ROW` pnl gap — sign convention resolved, on hold pending a
   live-fire confirmation.
2. `HM-ARMED-DORMANT-SPREAD-STRATEGIES` — still needs an Admiral
   decision on whether the three spread strategies stay armed.
3. `HM-DRAWDOWN-BLIND-TO-OPTIONS-PNL` — now well-scoped (this block),
   still needs a dedicated build session when prioritized; zero urgency.
4. The `options_books` stored-counter drift remains unreconciled — still
   harmless, still out of scope.
