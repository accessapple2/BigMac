# Relay — 2026-09-13. Rotation no longer orphans positions (`34451e2`) + X9 retention report

RULE #1 held: no trade/signal/decision/rating/history row deleted or rewritten. Live DB
operations this block: two backups, one config-row fill (Season 7 `end_date`), nothing else.
No rotation ran. `SEASON_AUTOROTATE_ENABLED` stays off; seasons are manual until the Admiral
says go.

## STEP 1 findings (asked before code — `QUESTION_rotation-position-delete.md`)
- `rotate_season()` / `start_season()` each ran `DELETE FROM positions WHERE player_id NOT IN
  ('webull','alpaca-mirror') AND player_id != 'neo-matrix'`, with no halt or broker check.
- Today's would-be-deleted rows (5: m5-allocator AGG/SPY, ollama-plutus NU/P/TSLL) are all
  `execution_type='simulated'` and absent from the broker. **Correction:** the earlier
  "five new orphans" claim was wrong — they are simulated book rows, not broker positions.
- `positions` is current state (normal sells already remove rows; 59 files / 139 read sites).
- Admiral chose **A — archive then clear**, with six conditions, then amended it to reuse the
  `scripts/signal_center_archive_rotate.py` house pattern.

## neo-matrix exclusion (condition 4, reported before touching)
In place since the Season 5 commit `859a4f0`. `neo-matrix` is an independent participant
(`shared/matrix_bridge.INDEPENDENT_PLAYER_IDS`) whose position rows the Matrix bridge owns and
rewrites. `is_auto_tradeable('neo-matrix')` is **True**, so "non-auto-tradeable accounts" alone
would have silently dropped the protection — it is kept explicitly. The keep predicate is:
humans, passive broker mirrors, independent players, holders not in `ai_players`.
`can_trade_live` is deliberately excluded (it only enforces inside the trader process). Human
rows (trade-desk, desk-manual, enterprise-computer) were previously deleted; now kept.

## What shipped
- Broker guard on actual broker presence: live Alpaca, else `alpaca-mirror` snapshot ≤6h old;
  neither → fail closed. A clearable row open at the broker aborts before any write.
- House archive-then-clear into append-only `positions_season_archive`: INSERT, read back,
  verify count AND every field, then DELETE the batch; any failure rolls back the rotation.
  `trg_rule1_no_delete_*` + no-update trigger, ensured at apply time; one-time integrity-checked
  first-run snapshot recorded in `archive_metadata`; dry-run by default (`apply=True`).
- Ending season `end_date` fill; `backfill_season_end_dates()` for past seasons.
- Divergences from the house script (and why) are in the commit message: one rotation
  transaction, archive inside trader.db, provenance columns + no-update trigger, uncompressed
  `trader_*.db` snapshot so the X9 sweep carries it, `apply=True` kwarg, plain INSERT.
- XO_BACKLOG: hold lifted with what now makes rotation safe; ledger items recorded (sentinel
  should make the DB check primary; Riker restated recommended stops as existing orders).

## Verification
- Backups first: `data/backups/{trader,signals}_pre-rotation-archive_2026-09-13.db`, integrity
  ok locally and on the X9.
- Tests: `test_rotation_positions_archive.py` 18 (in pre-commit) + season/RULE #1 suites = 57
  pass; full suite failure set identical to baseline.
- Live: Season 7 `end_date` NULL → `2026-09-11` (from `season_history.ended_at`
  2026-09-11T06:13:15); Seasons 6/8 unchanged.
- Live dry run (`rotate_season(caller="verify-dry-run")`, broker `alpaca-live`): Season 9, not
  blocked, 5 rows would archive, 15 mirror + 2 neo-matrix rows kept; positions, season, history,
  cash and table state identical before/after — zero writes.
- Restart 10:44:55 (PID 1670, after the 10:44:06 commit) so the running process carries the
  guarded `start_season()` behind `/api/seasons/start`; no new errors; Season 8, 3 config rows,
  22 positions. The archive table is not created until the first `apply=True` rotation.

## X9 retention report (report only, measured ~10:25 MST)
- **Free space:** 931 GiB volume, 510 GiB used, **422 GiB free** (55%).
- **`OLLIETRADES_BACKUPS/`:** `backups/` 36 GiB (110 files: 21 `trader_prerestart_*` 8.0 GB,
  14 dated dailies 16.3 GB, 75 other ad-hoc 14.2 GB); `ollie_db_backups_archive_2026-05-28`
  4.5 GiB; `signal-center` 2.1 GiB; `data` 1.2 GiB; `pre_rotation_fix_20260829_184712.db` 1.0 GiB.
- **Retention covering the X9: none.** `offhost_backup.sh` is rsync copy-only (no `--delete`); it
  only picks the latest 14 local dailies to sync but never removes older ones from the X9, and
  its ad-hoc sweep has no cap by design. The 21:30 TTL cron deletes only local
  `data/backups/_archive/{trader_*.db.gz,*.db-shm,*.db-wal}` older than 30 days.
  `docs/runbooks/backup-retention-policy.md` is scoped to local `data/backups/`; its only X9 rule
  is that a copy must exist there before anything is removed locally. `backup_freshness_check.sh`
  checks X9 freshness, not size.
- The `trader_prerestart_*` copies have no producer in this repo's scripts, `main.py` or
  dashboard — their origin is outside what was searched.
- Rough runway: at the 9/11–9/12 pace (~5 GB/day) about 80 days; at a quiet nightly baseline
  (~1.5 GB/day, the in-place `signal-center` rsync doesn't add files) several months. Estimates,
  not measurements.
