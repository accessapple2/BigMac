# signals.db retention — RESOLVED number, proposal below

**Date filed:** 2026-09-10 AM. **Updated:** 2026-09-10, same morning —
number confirmed, proposal drafted per Admiral's follow-up.

## The number

Wrong guess corrected: not `trader.db`'s `signals`/`signals_v2` tables.
The 2.19 GB is **`signal-center/signals.db`** — the standalone SQLite DB
behind the :9000 Signal Center service (`signal-center/server.py`),
separate from `data/trader.db` entirely.

- **Path:** `/Users/bigmac/autonomous-trader/signal-center/signals.db`
- **Size:** 2,208,968,704 bytes = **2.19 GB** (matches the figure exactly)

**Not moot — confirmed real and worth fixing.**

## Where the size actually is

99.6% of the file is one table:

| Table | Rows | Size | Date range |
|---|---|---|---|
| `signal_history` | 207,178 | **2,039.6 MB** | 2026-04-05 → 2026-09-10 (live) |
| `base_rate_features` | 219,618 | 22.2 MB | (date column has a bad 2006 row — data-quality note, not touched here) |
| `intelligence_feed` | 47,255 | 10.9 MB | 2026-04-08 → 2026-09-10 (live) |
| everything else (9 tables) | — | ~5 MB combined | — |

`signal_history.raw_data` (a JSON blob per row, avg 9.7 KB, max 184 KB) is
the actual bulk driver — 207K rows at ~10KB average is the whole 2 GB.

## The retention policy already exists — it was scaffolded 2026-04-26 and never run

Found `signal-center/signals_archive.db` already sitting next to the live
DB, with a matching schema for `signal_history` + `intelligence_feed` and
an `archive_metadata` table stating:

```
policy: 30-day rolling
created: 2026-04-26
cutoff: 2026-03-27
note: Archival begins producing rows on 2026-05-05 (day 30 of data collection)
```

(Documented at the time in `docs/SUNDAY_DRYDOCK_2026-04-26_FINAL.md`, item
E1.) **The archive DB has zero rows in it.** The policy was designed and
the destination was built; the job that actually moves rows was never
written. `signal_history` has grown unbounded for the four-plus months
since — this is why it's 2 GB today instead of a rolling ~30-day window.

## Proposal — finish what E1 started, don't design something new

**1. Scope:** `signal_history` + `intelligence_feed` only, matching the
existing `archive_metadata` declaration. The other tables are small
(≤22 MB) and don't need this.

**2. Mechanism:** one new script (e.g.
`scripts/signal_center_archive_rotate.py`), cron-run weekly:
- `ATTACH 'signals_archive.db' AS a` (same pattern already written as the
  E1 rollback command in the Drydock doc).
- For each table: `INSERT INTO a.<table> SELECT * FROM <table> WHERE
  <ts_col> < date('now','-30 days')`.
- **Verify row count inserted into the archive matches row count selected
  before deleting anything** (insert-then-verify-then-delete, never
  delete-first — same discipline as this repo's other archive/expire
  scripts).
- `DELETE FROM <table> WHERE <ts_col> < date('now','-30 days')` on the
  live DB only after the archive insert is verified.
- Update `archive_metadata` (`last_run`, row counts) each pass.
- **Archive DB itself is never pruned** — cold storage, permanent, same
  spirit as RULE #1 even though these two tables aren't on the official
  sacred-table list.
- `VACUUM` on `signals.db` monthly, not every run (it needs roughly the
  DB's own size again in free disk space and briefly locks the file —
  schedule after close, same window as tonight's other after-close work,
  not during live :9000 traffic).

**3. Numbers — what this actually does:**
- One-time backfill run today: moves **196,559 `signal_history` rows
  (1.81 GB)** + **44,634 `intelligence_feed` rows (8.2 MB)** into the
  archive. Live `signal_history` drops to 10,619 rows / **103 MB**; live
  `intelligence_feed` drops to 2,621 rows / 0.5 MB.
- Live `signals.db` total after backfill + VACUUM: **~2.19 GB → ~350-400
  MB** (a ~5.5x shrink).
- Steady state going forward: `signal_history` growth has actually
  slowed a lot since the 07-22 stand-down (Aug: 2,682 rows/25 MB; Sept
  partial: 7,937 rows/78 MB — vs. 40-80K rows/mo April-July), so a 30-day
  rolling window should hold live `signal_history` in roughly the
  50-150 MB range going forward, not re-grow toward 2 GB.

**4. Bonus — likely explains tonight's other open item.** Checked
`scripts/offhost_backup.sh`: it rsyncs `signal-center/signals.db` (`+`
-shm/-wal) directly, full file, no pruning — the single largest DB in
that backup set by a wide margin (everything else in `data/` is well
under 100 MB). This is a strong candidate for **both** the 9,638s runtime
(vs ~1,065s a week ago) and the 9-of-10-DBs gap filed as a separate
low-priority item this morning — a 2.2 GB rsync under any contention is
exactly where a slow/dropped transfer would show up. Shrinking the live
file to ~350-400 MB should improve both, though this is inference, not
confirmed — worth checking after the retention job runs and the next
offhost backup completes, rather than assuming.

## Options

- **Build and ship it (Recommended)** — one script + one weekly cron
  line, reusing the already-designed schema and policy; no new design
  decisions needed, the 04-26 session already made them.
- **Adjust the window** — 30 days was the original design's number, not
  re-argued here; say if you want a different cutoff (e.g. 14 or 60
  days) before I build against it.
- **Hold** — flag it as known and sized, don't build yet.

Nothing built yet pending which of these you want.
