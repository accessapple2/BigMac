# Relay: total_trades bug root cause + status-never-set fix (S6, work block 4)

**Date:** 2026-07-10
**Commit:** `3a5c66b` (pushed to `exec-pipeline`)
**Prior work blocks:** Finding 4 (`8837da6`), CSP-era sweep (`5b0395b` +
relay `e33db80`), book-summary counts + Relay Doctrine (`79baae4` +
relay `9d8cd4b`). This block follows up on that block's flagged "open item
1" — the `total_trades` open-vs-close counting bug.

## Ask

Captain: "go check the total_trades open-vs-close counting bug." Mid-turn
follow-up: "note for the backlog: bull_call_spread_v1 and
bear_put_spread_v1 have never fired but stay armed and polled every
tick — evaluate whether they should be halted until wanted."

## What was found

**Root cause of the `total_trades` drift:** two live execution paths write
directly to `options_trades` and never call into `engine/options_exec.py`
or touch `options_books` at all:

1. `strategies/executor.py` — the framework behind `bull_spread_v1`,
   `bull_call_spread_v1`, `bear_put_spread_v1`, all three imported and
   scheduled every tick in `main.py`. Only `bull_spread_v1` has ever
   fired (25 rows, 2026-04-22 through 2026-05-14 — dormant 2 months but
   still armed).
2. `swingdesk/spread_executor.py` — manual/self-test spreads (4 rows, all
   currently open).

The 21 `bull_spread_v1` rows currently showing `status='closed'` in
`options_trades` got that way from a **one-time manual SQL migration on
2026-05-17** (`HM-BULL-SPREAD-V1-SCHEMA-CANONICALIZE`), not from the live
code — `options_books`'s counters were never reconciled against that
migration, which is the drift measured last block (84 stored vs. 105
actual closed trades for the fleet book).

**More serious finding, discovered while tracing the above:**
`strategies/executor.py::_increment_closed()` — the *only* live close
path for all three scheduled strategies — sets `exec_status='closed'` on
full close but **never sets `status='closed'`, and never writes `pnl`**.
Every P&L/win-rate query in the system filters on `status='closed'`
(`options_book_summary()`, `strategy_pnl()`, etc.). Proof this has never
worked correctly even once: all 21 `status='closed'` legacy rows trace to
one-time manual reconciliation scripts (`exit_reason` =
`HM-OPTIONS-TRADES-ZOMBIE-CLEANUP-reconcile-2026-05-18` or
`HM-AE-Option-B-reconcile-2026-05-05`), none to `_increment_closed()`
itself. Any future close through this path — for any of the three armed
strategies — would be permanently invisible to every reporting surface,
which is a more consequential bug than the counter drift itself.

## Decision point — asked before touching live-armed execution code

Two findings both touch code that actively routes real (paper) broker
orders, not read-only display code like every other fix this season.
Asked the Captain how to scope it (fix both counter-drift + status/pnl,
fix status/pnl only, write up only, or something else). Answer: **fix
status/pnl only** — leave the `options_books` stored-counter drift alone
since nothing reads those columns anymore (neutralized by the prior
block's fix to `options_book_summary()`, which already computes live from
`options_trades`).

## What shipped

`strategies/executor.py::_increment_closed()` now also sets
`status='closed'` in the same `CASE WHEN contracts_closed_so_far + ? >=
contracts` branch that already flips `exec_status` — unambiguous, no
broker-fill data required.

## What was deliberately NOT shipped

`pnl`/`exit_credit_debit` computation on close. Investigated the path:
`close_vertical_spread()` (`engine/alpaca_options.py`) submits a market
order and returns immediately, before fill — real P&L needs the MLEG
combo order's `filled_avg_price` via `client.get_order_by_id()` (the same
mechanism `swingdesk/spread_executor.py::poll_fill()` already uses for
single-leg fills). But **no example anywhere in this codebase captures a
real multi-leg combo order's fill this way** — grepped every
`filled_avg_price` usage in the repo; all are single-instrument fills.
The one pnl-populated legacy row (id=28) is a degenerate expired-OTM case
(close cost is trivially $0 by definition, no fill involved) and proves
nothing about the sign convention for an active MLEG close. Checked
Alpaca's public docs directly — they don't spell out the net-price sign
convention for multi-leg closes either. Writing arithmetic against an
unverified sign convention risks a plausible-looking but **backwards**
P&L number landing in the trading ledger — strictly worse than the
current `pnl IS NULL` state (already true for 20 of these 21 rows).
Filed as `HM-STRATEGIES-EXECUTOR-STATUS-NEVER-SET` in
`docs/XO_BACKLOG.md` with the specific unblock condition: Alpaca
docs/support confirmation, or an Admiral-authorized live test-fire to
observe the real sign empirically.

## Backlog item filed per mid-turn request

`HM-ARMED-DORMANT-SPREAD-STRATEGIES` in `docs/XO_BACKLOG.md`:
`bull_call_spread_v1` and `bear_put_spread_v1` have never produced a
single trade despite being imported and polled every scan cycle;
`bull_spread_v1` itself has been dormant since 2026-05-14 despite staying
armed. Flagged for the Admiral to decide: legitimately picky entry
conditions, or silently broken? Not investigated further this pass — no
signal-generation logs were read to distinguish the two.

## Testing

- New file `tests/test_strategies_executor_status_fix.py`, 3 tests:
  full close sets `status='closed'` + `exit_date` (pnl stays NULL,
  confirmed not a regression); partial close leaves `status='open'`;
  a second partial close that completes the position transitions to
  `status='closed'`.
- Full suite: 967 passed (964 + 3 new), same 14 pre-existing unrelated
  failures as every prior run this season.
- `py_compile` clean.
- Trader restarted, single-PID bind confirmed, zero orphans.

## Live verification

No live full-close event was observable tonight — `bull_spread_v1` has
been dormant since 2026-05-14 and no signals fired for any of the three
strategies in the post-restart window. `logs/trader.log` /
`logs/trader_error.log` checked for errors tied to
`strategies/executor.py` or `_increment_closed` since restart — none
found (only pre-existing, unrelated Ollama-host-unreachable noise). Given
no live event to observe end-to-end, correctness rests on the unit tests
exercising the SQL logic directly, which is the same bar used whenever a
live-fire test isn't available in this session.

## Open items (carried forward)

1. `HM-STRATEGIES-EXECUTOR-STATUS-NEVER-SET` pnl/exit_credit_debit gap —
   needs Alpaca sign-convention confirmation or an authorized live test
   before it's safe to wire up.
2. `HM-ARMED-DORMANT-SPREAD-STRATEGIES` — Admiral decision on whether to
   keep `bull_call_spread_v1`/`bear_put_spread_v1`/`bull_spread_v1` armed.
3. The `options_books` stored-counter drift itself (total_trades/wins/
   losses columns on the table) remains unreconciled — explicitly out of
   scope per this block's decision, since nothing reads it anymore.
4. `swingdesk/spread_executor.py`'s close path was not traced this pass
   (its 4 rows are all still open) — unverified whether it has the same
   `status`-never-set gap; worth a look if/when one of those positions
   closes.
