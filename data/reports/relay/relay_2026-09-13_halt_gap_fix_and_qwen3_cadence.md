# Relay — halt-gate gap fixed, ollama-qwen3 cadence mirrored, Q1 verdict recorded. 2026-09-13

Follow-up to `relay_2026-09-13_gate_efficacy_and_halt_gap.md` (the
read-only investigation). This session builds and ships the three fixes
that investigation's findings called for.

## 1. Halt-gate gap — fixed

Added the same `halt_mode='full'` check `paper_trader.sell()` and
`paper_trader.short_sell()` already carry to the three functions that
were missing it:

- `engine/paper_trader.py::sell_partial()` — exact same gate block as
  `sell()`: queries `ai_players.halt_mode`, refuses and logs a gate
  reject (`_log_gate_reject(..., "HALT", ...)`) when `halt_mode='full'`.
  `exit_only` still permits the sell, matching `sell()`'s documented
  semantics for a position-close.
- `engine/crew_scanner.py::_check_scaled_exits()` — added a belt-and-
  braces skip per player at the top of the tier loop, before
  `get_portfolio()` is even called, so a halted player's positions
  aren't evaluated at all. Not a duplicate of the `sell_partial()` fix —
  this loop is the only current caller, but a future caller of
  `sell_partial()` gets the callee-side check regardless; this catches
  it earlier and cheaper for this specific loop.
- `engine/options_exec.py::close_options_trade()` — added the same check
  keyed off `options_trades.agent_id`, reusing the already-open
  connection. This function never calls a broker (paper bookkeeping
  only, confirmed by reading it — the module's "NO real money touched"
  docstring is accurate), but a halted agent's position/P&L shouldn't
  move either. Fails open (allows the close) if `agent_id` isn't a
  column or `ai_players` doesn't exist in whatever DB it's pointed at —
  two existing test fixtures for this function are `options_trades`-only
  with no `agent_id` column, and a missing column/table is a schema-
  absence case, not evidence of a halt.

**Not touched**: the market-closed gate that `sell()`/`short_sell()` also
carry (which `sell_partial()` and the option-close path still lack) —
out of scope for this instruction, flagged again here so it isn't
forgotten.

### Regression test

`tests/test_halt_gate_order_paths_regression.py`, same shape as
`test_trades_tz_gate_regression.py` (source-scan against named files/
functions, not a snapshot of the current line). Two tests:

1. `test_every_named_order_path_checks_halt_mode_full` — extracts each of
   six named functions' own source block (`buy`, `sell`, `sell_partial`,
   `short_sell` in `paper_trader.py`; `_check_scaled_exits` in
   `crew_scanner.py`; `close_options_trade` in `options_exec.py`) and
   asserts each references `halt_mode` AND compares it against one of
   the two legitimate literal shapes already in this codebase (`==
   'full'` for closes, `!= 'active'` for new-position entries — `buy()`/
   `short_sell()` correctly use the broader entry-gate form, so the test
   accepts either rather than wrongly demanding `== 'full'` everywhere).
   **Verified this actually catches the original bug**: stashing out the
   three fixed files and re-running this test fails it immediately.
2. `test_sell_partial_actually_blocks_a_fully_halted_player` — behavioral
   companion with a minimal in-memory DB (`ai_players` + `positions`),
   proves `sell_partial()` returns `None` for a `halt_mode='full'` player
   rather than just checking the source text says the right words.

Both new tests pass. Full suite: 1,310 passed (1,308 + these 2 new),
18 failed — **same 18, confirmed pre-existing via `git stash`** (missing
`vectorbt` module, a stale `engine.riker_synthesis` import, and four
unrelated test files' own pre-existing failures — none touch any file
this session modified). Zero regressions introduced.

## 2. ollama-qwen3 cadence — mirrored from McCoy's 2026-09-11 fix

`ollama-qwen3` (Scotty) was the last live member of `_SCAN_TIER2`
(`main.py`) after McCoy's 09-11 removal — same elapsed-time 2-hour
cadence, same full 600-900-symbol active universe, no market-hours
awareness. Confirmed by the read-only investigation as ~262 of the
30-day window's 4,594 market-closed rejects.

**Fix, identical shape to McCoy's**: `_SCAN_TIER2` is now empty (comment
left explaining why, not deleted — the tier's dispatch code is harmless
against an empty set, and a genuinely-live future "secondary signals,
every 2h, full universe" agent has a documented slot). New
`run_qwen3_screened_scan()` (`main.py`, right after
`run_mccoy_screened_scan()`) fires the same two deterministic slots
(9:35 AM / 12:30 PM ET, weekdays), reusing
`engine.mccoy_screen.get_mccoy_screened_symbols()` as-is — that screen
(Volume Radar + a liquidity floor + regime context) is genuinely
player-agnostic, no McCoy-specific logic, so this is reuse per RULE #1 /
additive doctrine rather than a duplicated qwen3-specific screen module.
Given the same `HM-SCHED-STALL-FIX` risk that motivated moving McCoy's
scan onto its own daemon thread (a shared `schedule.run_pending()` stall
silently no-fired McCoy's 12:30 PM slot once, 2026-09-11), qwen3's
version gets the identical dedicated-thread treatment
(`_qwen3_scheduler_thread`, 60s poll, independent of the shared queue)
rather than being left on the queue that already burned McCoy once.

## 3. Restart + live verification

Backed up both DBs first, integrity-checked:

```
data/backups/trader.pre_haltfix_20260912_205008.db   PRAGMA integrity_check: ok
data/backups/signals.pre_haltfix_20260912_205008.db  PRAGMA integrity_check: ok
```

Market confirmed closed (Saturday, `MarketStatus.CLOSED_WEEKEND`) before
restarting, per standing after-hours authorization.

[Restart + live verification results filled in below after the restart —
see the commit this doc ships with for the exact commands and output.]

## 4. Q1 verdict — recorded in the plan doc, not just this relay

Per instruction, the settled conclusion from the gate-efficacy funnel is
now in `docs/XO_PLAN_2026-09.md`'s Status log (2026-09-13 entry), not
just this relay doc, so it survives context loss and isn't relitigated
without new data: **at 90-day scale, no gate rejects a systematically
profitable population; REGIME-ROUTER's apparent 1-day edge reverses by
day 5 and is a bear-regime bounce, not suppressed alpha. The constraint
is the model, not the chain.**

## RULE #1 statement

Nothing deleted or rewritten in place. Every change is either an
additive gate check (refuses an action, never alters a historical row),
a new function + new scheduler thread (net-new code, no existing
function's unrelated behavior changed), or documentation. `_SCAN_TIER2`
was emptied, not deleted, with its full removal history preserved in
comments. One restart, backup-first, integrity-checked, market-closed,
under standing after-hours authorization.
