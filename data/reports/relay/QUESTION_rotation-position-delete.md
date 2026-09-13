# QUESTION — rotation position delete: which shape? (2026-09-13)

Asked before any code change, per the task's "STOP and ask if a step seems to need a delete".

## STEP 1 findings (read-only)
- a) `engine/season_manager.py:322` (`rotate_season`) and `:449` (`start_season`) both run
  `DELETE FROM positions WHERE player_id NOT IN ('webull','alpaca-mirror') AND player_id != 'neo-matrix'`
  — no halt-state or broker-backing check.
- b) Today's would-be-deleted rows are **not** broker-backed: all `execution_type='simulated'`,
  no `alpaca_order_id`, symbols absent from the Alpaca paper account. The broker's 15 open
  positions exist only under `alpaca-mirror` (already excluded). **Correction:** an earlier
  report called tonight's would-be deletion "five new orphans" — wrong; it would have wiped 5
  simulated book rows, not orphaned broker positions.
- c) 5 rows: `m5-allocator` AGG, SPY; `ollama-plutus` NU, P, TSLL.
- d) `positions` is current state: normal full closes already DELETE rows
  (`paper_trader.py:242/2053/2058/2337/2342`); 59 files / 139 SELECT sites read every row as an
  open holding; history lives in `trades`. Not a RULE #1 protected table.

All options below add the broker-backed guard: if any row the rotation would clear is backed
by an open broker position (agent's BUY carries an `alpaca_order_id` / non-simulated execution
AND the symbol is open at the broker), rotation **aborts before any write**, loudly, same shape
as the margin guard. All options fill the ending season's `end_date` and backfill S7's from
`season_history.ended_at` (config row only).

## Options
**A — Archive, then clear (recommended).** Copy each non-broker-backed row into a new
`positions_season_archive` table (all columns + season, archived_at, reason) in the same
transaction, then remove it from `positions`. History survives; `positions` stays current
state; the 139 readers are untouched. Still contains a DELETE on `positions`, of rows already
archived — needs your explicit OK under this task's banner. ~40 lines + additive migration + tests.

**B — No delete at all; carry positions over.** Rotation leaves every AI position row owned
into the new season and only resets cash/season. Zero delete, zero reader changes. Cost: holders
start the new season with carried holdings on top of a $7,000 cash reset, so their season
equity/P&L is overstated (today: 5 rows, ~$1.4k cost basis).

**C — Flag rows retired in place.** Add `retired_at`/`retired_reason`, no delete. Every
open-holdings reader (59 files / 139 SELECT sites) must filter retired rows, or they keep being
counted in equity and swept for stops. Largest blast radius; not safe as one pass.
