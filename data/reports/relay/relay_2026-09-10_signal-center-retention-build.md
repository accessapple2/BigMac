# Relay — signal-center retention job built + validated, 2026-09-10

## What shipped

`scripts/signal_center_archive_rotate.py` — finishes the 2026-04-26
scaffold (`signal-center/signals_archive.db` + `archive_metadata`,
`docs/SUNDAY_DRYDOCK_2026-04-26_FINAL.md` item E1) that declared a 30-day
rolling archive policy but never built the mover job. Moves
`signal_history` + `intelligence_feed` rows older than 30 days from
`signal-center/signals.db` into `signals_archive.db`.

RULE #1 conditions from the Admiral, all built and tested:
- **Verify-then-remove per batch, never a bare DELETE.** Each 5,000-row
  batch runs INSERT → verify (row count AND full content match, id-for-id)
  → DELETE, all inside one transaction. A failed verify rolls back the
  whole batch (insert included) and aborts the script — nothing partial
  is ever left standing.
- **Backup before the first backfill.** First-ever `--apply` run takes a
  consistent `sqlite3.backup()` snapshot and gzips it
  (`signals.db.pre-retention-backfill-<ts>.gz`) before touching anything;
  tracked via an `archive_metadata` key so it only fires once.
- **Archive gets the same never-delete protection as the primaries.**
  `trg_rule1_no_delete_signal_history` / `trg_rule1_no_delete_intelligence_feed`
  triggers, same `RAISE(ABORT, ...)` pattern as `setup_db.py`'s
  `RULE1_TABLES`, created in the `signals_archive.db` schema.
- **Archive DB is now in the offhost backup set** — `scripts/offhost_backup.sh`
  updated to rsync `signal-center/signals_archive.db` (+ -shm/-wal)
  alongside the live DB, and added to its integrity-check file list.

## Consumer-safety check — answering "does anything read past 30 days"

Confirmed **no live consumer reads `signal_history` older than 30 days:**
- Signal Center UI's history view: dropdown tops out at "Last 30 days"
  (`index.html` `#hist-days`, options are 1/7/30 only).
- `/api/signals/history` defaults to `days=7`; `/api/export/<fmt>`
  defaults to `days=30`. Both bounded by caller-supplied `days`, no
  unbounded path.
- Every `intelligence_feed` read is `ORDER BY created_at DESC LIMIT n`
  (most-recent-N), never a date-range scan — archiving older rows can't
  affect it.
- **One unbounded query found:** `engine/super_backtest_v2.py::analyze_signal_center()`
  does `COUNT(*)` / `GROUP BY signal_name` over all of `signal_history`
  with no date filter. Traced every caller — it's imported and invoked
  only from `engine/_archive/2026-04-26/super_backtest_v3.py` and
  `v3b.py`, both retired (not imported by v4, v5, oos, or oos_c, and not
  in crontab). **Dead code path today — 30 days is safe.** If this
  function is ever revived by a future backtest version, its "total
  records" stat would need to read across both `signals.db` and
  `signals_archive.db` (a two-DB UNION) to stay accurate — noted here so
  a future session doesn't have to re-derive it.

## Validated end-to-end before touching production

Copied the real 2.19 GB `signals.db` + `signals_archive.db` to an
isolated test `$HOME` (scratchpad, not the repo) and ran the full
sequence there — nothing below touched production:

- Dry-run matched the numbers from this morning's manual measurement
  exactly: 196,559 `signal_history` rows / 44,634 `intelligence_feed`
  rows eligible.
- `--apply`: moved all of it in 62s, every batch verified before delete,
  zero verify failures.
- `--apply` re-run immediately after: 0 rows moved (idempotent), backup
  step correctly skipped ("already taken"), confirms safe to run
  repeatedly/resumably.
- `--vacuum`: live DB **2,209.0 MB → 159.9 MB** (better than the ~350-400
  MB estimated in the earlier proposal — index overhead came off too).
- `PRAGMA integrity_check` on the archive: **ok**.
- Manually attempted `DELETE FROM signal_history WHERE id=1` directly
  against the test archive DB — **blocked**, `RULE #1: signal_history
  rows in signals_archive.db are never deleted -- this is the cold copy
  (19)`, row confirmed still present after.
- Live table post-run correctly scoped to the last ~30 days
  (`signal_history` 2026-08-27 → 2026-09-09 in the test run).

## Also done — cron wired, offhost backup wired

- `scripts/offhost_backup.sh`: added the archive DB to both the rsync
  set and the local integrity-check list. Syntax-checked (`bash -n`),
  clean.
- Crontab: added a weekly line, `45 19 * * 0` (Sunday, ahead of the
  20:15/20:30/20:45 daily backup chain), running `--apply` without
  `--vacuum` (freed pages get reused by SQLite's own free-list on
  subsequent inserts at this cadence — VACUUM is a one-time job for the
  initial backfill, not a recurring cost; can be run manually later if
  size ever creeps). Followed the repo's Cron Edit Safety Rule exactly:
  dumped live crontab to a file, edited the file (not a pipe), diffed
  (exactly one line added), count-guarded (171→172), then installed via
  `crontab <file>`. Backup at `/tmp/xo_cron_backup_20260910.txt`.

## NOT done — holding for the Admiral's queue order

**The actual `--apply` run against the real, live
`signal-center/signals.db` has not happened.** The Admiral's approval
specified an execution order — "Phase 1.1 acceptance read → signals.db
retention → Polygon limiter cap → key rotation → bk_orb → Bridge
cosmetics → offhost-backup check" — "after close." Phase 1.1's
acceptance read hasn't been run yet this session, and this environment
has no clock/market-hours signal to confirm "after close" has arrived.
Rather than jump the given order or guess at market timing, holding here
for an explicit go — the tool is validated and ready to run for real the
moment it's time.

**First real production run will need `--apply` (and a one-time
`--vacuum` after, to actually reclaim the ~2GB), not the weekly cron's
plain `--apply`** — the recurring cron only handles the ongoing trickle
once the backlog is already cleared.
