# Relay: second-round CSP/MLEG sweep — 2 real bugs fixed, 2 agent claims refuted (S6, work block 8)

**Date:** 2026-07-10
**Commit:** `f260398` (pushed to `exec-pipeline`)
**Ask:** "sweep for any other unfiltered CSP or MLEG surfaces."

## Method

Dispatched two parallel Explore agents — one re-sweeping for CSP-era-
filter gaps beyond today's earlier fixes, one sweeping for the "status
never synced on close" bug family found across the last three work
blocks (`strategies/executor.py`, `swingdesk/spread_executor.py` ×2).
**Every claim from both agents was independently verified against source
and the live DB before being reported or fixed** — this surfaced a real
correction: two of the four total findings did not hold up.

## Confirmed real, shipped

**1. `HM-OPTIONS-EXEC-CLOSE-EXEC-STATUS-NEVER-SET`.**
`engine/options_exec.py::close_options_trade()` updates `status`, `pnl`,
`exit_date` etc. on close but never set `exec_status`. Three call sites
gate on `exec_status='open'` alone: `strategies/exit_manager.py::
fetch_open_strategy_positions()`, and the dedup checks in
`bull_spread_v1.py`/`bull_call_spread_v1.py`. Verified live in the DB:
**19 rows** had `status='closed' AND exec_status='open'` — one is
`options_trades` id 28 (`bull_spread_v1`, SPY, closed 2026-05-22,
`expired_otm`), which **silently blocked every new SPY entry for both
`bull_spread_v1` and `bull_call_spread_v1` for over 7 weeks** (confirmed
by direct call to `_already_open('SPY')` returning `True` before the
fix). The other 18 are `options-sosnoff`'s CSP closes — same data
defect, verified NOT to cause a live blocking effect since
`paper_trader.py`'s CSP gating doesn't check `exec_status`.

Fixed: added `exec_status='closed'` to the same UPDATE. Hand-corrected
all 19 existing rows directly. Verified `_already_open('SPY')` now
returns `False`.

**2. `HM-EXECUTOR-STRUCTURE-WHITELIST-GAP`.**
`strategies/executor.py::_execute_live()`'s structure whitelist was
hardcoded to `("bull_call_spread", "bull_put_spread")`. `bear_put_
spread_v1.py` — scheduled every 15 minutes — emits `"bear_put_spread"`/
`"bear_call_spread"`, neither on the list. **100% of that strategy's
signals were rejected before submission since it was ever wired up**
(confirmed: zero `options_trades` rows for that `strategy_id`, ever).
Verified via `submit_vertical_spread()`'s own docstring (already
documents bear-put-spread support by name) and payload-shape comparison
that this was purely a missing tuple entry, not a missing feature — the
whole rest of the pipeline already supports it.

Fixed: whitelist expanded to include both bear-spread structures. No
data to correct (zero rows ever existed).

## Agent claims checked and NOT confirmed

The CSP-era sweep flagged `engine/risk_manager.py::check_drawdown()` and
`engine/rallies_intel.py::compare_crew_vs_rallies()` as reading
unrestated `portfolio_history.total_value`, with an estimate that
options-sosnoff's "true restated equity" was around -$16,988. Traced how
`portfolio_history` actually gets written
(`paper_trader.py::record_portfolio_snapshot()` → `get_portfolio()` →
`ai_players.cash` + stock positions only) and found it **never included
CSP P&L at all, synthetic or real**, by an explicit architectural
decision documented in the code itself (CSP P&L books to `options_books.
<book_tag>.current_cash`, never `ai_players.cash`). Confirmed directly in
the live DB: options-sosnoff's `portfolio_history` peak equals her
current value exactly ($12,880.20 = $12,880.20) — there was never any
synthetic inflation in this specific table to restate. The agent's
arithmetic assumed a linkage between two structurally separate books
that doesn't exist. Nothing was shipped against either file.

## What that verification surfaced instead

**`HM-DRAWDOWN-BLIND-TO-OPTIONS-PNL`** (filed, not fixed): the 20%-
drawdown safety halt (`check_drawdown()`, gates new-trade scanning every
cycle) is structurally blind to ALL options/CSP P&L for any CSP-trading
agent, always — not an era-filtering bug, an architectural gap. A real,
large options loss would never trip the auto-halt via this mechanism.
This is a design question (should it fold in options P&L, from which
book, avoiding double-counting against the existing CSP notional/margin
draw against `options_books`) bigger than today's mechanical fixes —
filed for a dedicated future session.

## Testing

- New file `tests/test_options_exec_status_and_bear_spread_whitelist.py`,
  5 tests: `close_options_trade()` sets `exec_status`; the existing
  `status='open'` guard still applies (already-closed rows untouched);
  `bear_put_spread`/`bear_call_spread` no longer rejected; a genuinely
  unknown structure is still rejected (confirms the fix didn't open the
  door too wide).
- Full suite: 983 passed (978 + 5), same 14 pre-existing unrelated
  failures as every run this season.
- `py_compile` clean.
- Trader restarted, single-PID bind confirmed, zero orphans, corrected
  data confirmed persisted post-restart.

## Note on this pass's own process

Applying the two `docs/XO_BACKLOG.md` entries in the prior work block's
edit accidentally swallowed the `---`/heading boundary in front of the
pre-existing `HM-SIGNALS-V2-FIFO-STARVATION` entry (an artifact of a
find-replace that matched into the next section's heading). Caught and
fixed while filing this block's new entries — the file reads correctly
end-to-end now. Worth remembering: verify the surrounding structure after
any edit to a long, multi-section markdown file, not just the inserted
content.

## Open items (carried forward)

1. `HM-STRATEGIES-EXECUTOR-STATUS-NEVER-SET`/`HM-SWINGDESK-CLOSE-
   PHANTOM-ROW` pnl gap — sign convention resolved (doc citation), on
   hold pending a live-fire confirmation (Admiral decision, prior block).
2. `HM-ARMED-DORMANT-SPREAD-STRATEGIES` — still needs an Admiral
   decision on whether the three spread strategies stay armed.
3. `HM-DRAWDOWN-BLIND-TO-OPTIONS-PNL` (new) — needs a dedicated design
   session, not a quick fix.
4. The `options_books` stored-counter drift remains unreconciled — still
   harmless, still out of scope.
