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

## Correction to an earlier statement in this doc
"The archive table is not created until the first `apply=True` rotation" is correct. An
earlier draft (pre-amendment) had rotation abort until `setup_db` created the table; that
behaviour was replaced by the house pattern and `setup_db.py` was never changed. Rotation
does NOT wait on a trader restart.

## Tonight's window watcher (session monitor) — cannot rotate
Read-only by construction: `sleep` loops, `lsof`, `grep`/`awk` on `logs/trader.log`, and
`sqlite3.connect("file:data/trader.db?mode=ro", uri=True)` SELECTs. It imports no engine
code and cannot reach `rotate_season()`. The only scheduled rotation path is `main.py`'s job,
gated by `SEASON_AUTOROTATE_ENABLED` (off).

## UNTESTED until a real rotation runs (inherit this)
Covered only by fixture DBs and one live dry run:
- Creating `positions_season_archive`, `archive_metadata` and both triggers on the live
  `trader.db` (possibly while the trader is running).
- The first-run snapshot: a ~1.3 GB sqlite backup inside the apply path — duration, disk use,
  lock contention with a running trader are unmeasured.
- The archive INSERT / read-back / DELETE on real rows inside one transaction against live writes.
- Rollback leftovers: `save_season_summary()` commits the ending season's `season_history` rows
  BEFORE the main transaction, so a rolled-back rotation leaves them (retry skips them — safe,
  but a leftover).
- Alert delivery on abort (every send is blocked in tests).
- The 6h `alpaca-mirror` snapshot fallback against the real sync cadence (live dry run used
  `alpaca-live`).
- Option-row matching by underlying symbol (no option positions today; code only).
- `/api/seasons/start` with `"apply": true` (never called).
- `test_live_db_archive_if_present_is_protected` is vacuous until the table exists.

## RUNBOOK — first real rotation (DO NOT RUN without the Admiral's go)
**Preconditions**
1. Explicit Admiral go. `SEASON_AUTOROTATE_ENABLED` stays unset/false — confirm:
   `.venv/bin/python3 -c "import config; print(config.SEASON_AUTOROTATE_ENABLED)"` → `False`.
2. Market closed (after close or weekend), no restart or deploy in progress.
3. Trader state: stopped is preferred (the S8 rotation ran with the trader stopped during
   dry-dock) to avoid lock contention during the snapshot. If left running, expect
   busy_timeout waits. Confirm the stop method before using it; the verified restart after is
   `zsh scripts/trader_restart.sh`.
4. Local disk: ≥3 GB free in `data/` for the first-run snapshot plus WAL (`df -h data`).
5. Backups, integrity-checked, on the X9 (use `.backup`, not `cp`):
   ```
   cd /Users/bigmac/autonomous-trader; TS=$(date +%Y-%m-%d_%H%M)
   for pair in "trader:data/trader.db" "signals:signal-center/signals.db"; do
     n=${pair%%:*}; s=${pair#*:}; d="data/backups/${n}_pre-first-rotation_${TS}.db"
     sqlite3 "$s" "PRAGMA integrity_check;"; sqlite3 "$s" ".backup '$d'"; sqlite3 "$d" "PRAGMA integrity_check;"
   done
   # rsync both to "/Volumes/Crucial X9/OLLIETRADES_BACKUPS/backups/" (mount-guarded, as in
   # scripts/offhost_backup.sh) and integrity_check the X9 copies
   ```
6. Record the pre-state (positions count, current_season, season_history count).

**Dry run (writes nothing) — must show `blocked: None` and an expected `would_archive`**
```
cd /Users/bigmac/autonomous-trader
.venv/bin/python3 -c "import engine.season_manager as sm; print(sm.rotate_season(caller='first-archive-rotation'))"
```
Expect `broker_source: 'alpaca-live'`. Any `blocked` reason → stop and investigate.

**Apply**
```
.venv/bin/python3 -c "import engine.season_manager as sm; print(sm.rotate_season(caller='first-archive-rotation', apply=True))"
```
Returns the new season number on success, `None` on any abort (reason printed; RED_ALERT sent).

**Watch during the snapshot** (first-ever apply only)
- `ls -la data/backups/trader_pre-positions-archive-first-run_*.db` growing toward the
  `trader.db` size (~1.3 GB); `df -h data`.
- The run prints `positions archive ready; first-run snapshot taken: <path>` before any
  rotation write. If it hangs on locks, a running trader is the likely cause.

**Post-checks**
- `current_season` = new season; `season_config` has the new row; the ending season's `end_date` is set.
- `SELECT COUNT(*) FROM positions_season_archive` = dry-run `would_archive`; positions count =
  before − `would_archive`; spot-compare one archived row against the pre-rotation backup.
- `archive_metadata` has `positions_season_archive.backup_taken_at` and `.last_run`; both
  `trg_rule1_no_*_positions_season_archive` triggers exist.
- Run or wait for `offhost_backup.sh` so the first-run snapshot reaches the X9.

**Abort / rollback**
- `None` before writes (margin guard, broker check unavailable, broker-backed row, archive prep
  or snapshot failure): nothing rotated. Possible leftovers: the archive tables/triggers and a
  first-run snapshot file + key. Fix the cause and re-run the dry run.
- `None` inside the transaction (verify count/field mismatch, missing archive column): the whole
  rotation is rolled back. Leftover: the ending season's `season_history` rows (idempotent on
  retry).
- Process killed mid-apply: the uncommitted transaction rolls back automatically; same leftovers.
- Wrong result after a successful commit: do NOT delete anything. Restoring the pre-rotation
  backup requires the Admiral's approval; the live file is renamed aside (e.g.
  `trader.db.zombie.<date>`), never removed.

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
