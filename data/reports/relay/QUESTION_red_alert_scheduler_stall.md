# QUESTION — live scheduler stall: run_volume_red_alert has held schedule.run_pending() since 06:41 MST

Asked 2026-09-14 07:13 MST, market open. **Nothing has been changed or restarted.**

## What is happening (verified live)

- The last `[SCHED-JOB]` line in trader.log is `06:41:27 start name=run_volume_red_alert`, with no `done`
  (31+ min). The `[WR-DEBUG-HB]` main-loop heartbeat stopped at 06:41:26. None of the 168 shared-queue
  jobs have run since; that includes `run_alpaca_gex_refresh`, which is why canonical GEX is stale.
- Main-thread native stack (`sample 1670`):
  `pysqlite_connection_execute → sqlite3_step → btreeBeginTrans → sqliteDefaultBusyCallback → unixSleep`.
  PID 1670 is the only process with trader.db open, so the lock holder is inside the trader.
- **Mechanism: `engine/volume_scanner.py::red_alert_check` (lines 452-496) locks itself.**
  - It opens one connection `c` for the whole loop over today's 529 hot symbols and commits only after the loop.
  - The first red-alert `INSERT` (RMAX, 06:47:06) starts a write transaction on `c`.
  - Every later red/critical symbol calls `_post_to_war_room → war_room.save_hot_take`, which writes on its
    own connection and waits on `c`'s lock until it times out.
  - Log: `07:12:29 War Room post failed: database is locked`.
  - Alerts land every ~169 s (06:47:06, 06:49:55 … 07:12:29).
- Collateral: other writers in the process hit `database is locked` (HM-EQ snapshots, breadth_scanner).
  No `red_alert` rows have committed (latest volume_alerts row is still 13:36:15 UTC).
- Unaffected (own daemon threads): McCoy/qwen3 screened scans, War Room, battle_station.
- Estimate: 52 of the 529 symbols have hit ≥ 50x relative volume today, 10 have alerted so far. If the rest
  alert at ~169 s each, the scheduler stays blocked until roughly 09:10 MST.
- History: the only earlier long run in the archive (9/9 → 9/14) was one 171 s run. The 9/11 unidentified
  14-20 min blocker (relay_2026-09-11_mccoy_1230_nofire_trace.md) fits this job, but that isn't proven.

## Why a plain restart doesn't fix it

The uncommitted red_alert rows roll back. After a restart `existing_red` is empty, so the first
`run_volume_red_alert` (≤ 5 min after start) re-alerts all ~52 hot symbols and recreates the same stall,
likely longer.

## Question

How should the live stall be handled?

## Options

- **A — Hotfix `red_alert_check`, then restart. (Recommended)**
  Collect red_alert rows and War Room posts during the loop, write the rows in one short transaction, then
  post to the War Room only after that connection is closed. Add a regression test (second-connection write
  must not block). py_compile + tests, commit, restart via `scripts/trader_restart.sh`.
  The restart also activates HM-SCREENED-SCAN-HB (679444d). ~15-20 min to restart, plus the pre-commit
  suite (~8 min) unless told to restart before the commit finishes.
- **B — Wait it out, no restart.**
  No change to the live process. The shared scheduler stays dark until the loop finishes (est. ~09:10 MST).
  All red_alert rows commit at the end. Fix after close.
- **C — Plain restart now, no code change.**
  Clears the block immediately, but the same stall is expected to start again within ~5 min of startup
  (see above).
