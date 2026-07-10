# Relay: SwingDesk close creates phantom row instead of closing original (S6, work block 5)

**Date:** 2026-07-10
**Commit:** `188715f` (pushed to `exec-pipeline`)
**Prior work block:** `strategies/executor.py` status-never-set fix
(`3a5c66b` + relay `c012d48`), which flagged `swingdesk/spread_executor.py`'s
close path as unverified — this block is that follow-up, per Captain
request.

## Ask

"go check swingdesk/spread_executor.py's close path."

## What was found

`swingdesk/spread_executor.py` has no dedicated close-position logic.
`submit_spread(action='close', ...)` fell through to `_persist()` — the
same function that records a brand-new open position — unconditionally.
Closing a spread therefore **inserted a second row** instead of updating
the original: no link back to the position being closed, no
`status='closed'` written anywhere, a new row indistinguishable in shape
from a fresh open trade.

**Confirmed against real data, not a hypothetical.** `options_trades` ids
93/94/95: three near-identical CEG `bear_put_spread` rows, same strikes,
submitted ~36 seconds apart on 2026-06-11 — consistent with hitting this
exact gap during manual testing (retry after no visible effect). Row 93
shows `exec_status='expired'` (the underlying options actually expired)
but `status` is still `'open'` — a real zombie position sitting in the
ledger today, not a theoretical one. Rows 94/95 show `status='canceled'`,
which — same pattern as the `bull_spread_v1` legacy rows last block —
traces to a one-time manual fix, not any live code (grepped the whole
repo; nothing writes that value anywhere).

**Financial safety is not affected.** The real Alpaca paper order for a
close is built correctly (`build_mleg_order` already reverses to
SELL_TO_CLOSE/BUY_TO_CLOSE for `action='close'`) — the actual paper
position does get closed at the broker. This is purely a local-ledger
bookkeeping bug.

**Blast radius today is near-zero.** The only entry point,
`/api/swingdesk/spread/submit`, is documented in its own docstring as
"manual trigger only — no autonomous agent reaches this path." The
automated close path (`spread_exit_manager.py::_auto_close()`) is still
fully gated by `config.AUTO_SPREADS_ENABLED = False` (confirmed) and has
never fired. No SwingDesk activity of any kind since 2026-06-11.

## Decision point

Same category as the prior block — execution-adjacent code reachable by
a human action (an API call a person could make right now), not
read-only display code. Asked the Captain how to scope it: fix
linkage/status only, also clean up the 4 existing zombie rows, write-up
only, or something else. Answer: **fix the linkage/status only** — same
scope shape as the previous block's decision.

## What shipped

- `_find_open_position_id(underlying, strategy, occ_symbols)` — finds the
  existing `status='open'` row matching symbol + `strategy_id` + identical
  leg symbols. Unlike the existing `_find_open_duplicate()` (which only
  guards against duplicate *working* orders via `_LIVE_STATUSES`), this
  matches on any `status='open'` row regardless of `exec_status`, and
  picks the most-recent (`ORDER BY id DESC`) when duplicates exist —
  directly reproducing and correctly handling the real 93/94/95 scenario.
- `_close_original_position(...)` — `UPDATE`s the matched row:
  `status='closed'`, `exit_date`, appends to `exit_reason`
  (`"swingdesk_close order=<id>"`). Returns `None` if no match, in which
  case `submit_spread()` falls back to the old `_persist()` behavior
  (records the close event as a new row rather than silently dropping
  it) and flags the response with a `note` so the anomaly is visible to
  the caller.
- No schema change — this module's own docstring states "no schema
  change, no duplicate cols" as a hard constraint; the fix reuses
  existing `status`/`exit_date`/`exit_reason` columns.

## Deliberately NOT shipped

Same reasoning as the prior block: `pnl`/`exit_credit_debit` computation
on close needs the MLEG combo order's fill price, and there is no
precedent anywhere in this codebase for the sign convention of a
multi-leg close order's net `filled_avg_price`. Left as `NULL`
(unchanged from current behavior) rather than risk writing a
plausible-looking but backwards number into the ledger.

## Testing

- New file `tests/test_swingdesk_close_phantom_row.py`, 6 tests, all at
  the DB-mutation level (no Alpaca client needed — the bug is entirely in
  local bookkeeping, not order submission):
  - position matching by symbol + strategy + legs
  - ignores rows already `status='closed'`
  - returns `None` when no match exists
  - **most-recent-of-duplicates wins** — directly reproduces the real
    93/94/95 scenario with three `status='open'` duplicate rows
  - close updates in place, exactly one row survives (proves no phantom
    row gets created)
  - no-match-found path returns `None` cleanly
- Full suite: 973 passed (967 + 6 new), same 14 pre-existing unrelated
  failures as every run this season.
- `py_compile` clean.
- Trader restarted, single-PID bind confirmed, zero orphans.

## Live verification

`spread_exit_manager.run_spread_exit_cycle()` is scheduled every 5
minutes in `main.py` and imports `spread_executor` at call time — its
`[SPREAD-EXIT]` cycle log has a long clean history in `trader.log`
(confirmed back to 2026-07-01), and no error appeared post-restart.
Confirmed the new code is currently **inert in production**: none of the
4 existing SwingDesk rows have `exec_status='filled'`, so
`run_spread_exit_cycle()`'s own gate (`if _norm_status(exec_status) !=
'filled': continue`) skips all of them before reaching anything close
to a close action. No live end-to-end close event was available to
observe (same situation as the prior block) — correctness rests on the
6 unit tests exercising the exact SQL/matching logic directly.

## Open items (carried forward)

1. `HM-STRATEGIES-EXECUTOR-STATUS-NEVER-SET` pnl gap (prior block) —
   still needs Alpaca sign-convention confirmation before it's safe to
   wire up. This block's pnl gap is the same open question, same
   resolution path.
2. `HM-ARMED-DORMANT-SPREAD-STRATEGIES` (prior block) — still needs an
   Admiral decision on `bull_call_spread_v1`/`bear_put_spread_v1`/
   `bull_spread_v1`.
3. The 4 existing zombie/duplicate SwingDesk rows (93/94/95 + the
   `w2_selftest` row 89) were explicitly NOT hand-corrected this pass —
   the Captain's scope decision was code-fix-only. They remain in their
   current inconsistent state until a future pass addresses the data
   directly.
4. The `options_books` stored-counter drift (from two blocks ago) remains
   unreconciled — still out of scope, still harmless since nothing reads
   it.
