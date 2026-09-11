# Backup retention policy — data/backups/

Written down 2026-09-11, per Admiral directive, after `hm_ops_sentinel.py`'s
boot-volume warning (87.7% full) traced `data/backups/` to 12.8 GiB across
13 `trader*.db`/`signals*.db` files. RULE #1 (`CLAUDE.md`) names backup
retention as the one thing permitted to expire — this document is that
policy, written once rather than decided ad hoc each time disk pressure
comes up.

**This document defines the policy. It does not apply it.** Applying it
(deleting or archiving any file) requires a separate, explicit approval —
see "Status" at the bottom.

## The four rules

1. **Daily retention window** — keep every dated snapshot
   (`trader_YYYY-MM-DD.db`, the `db_snapshot.sh`-produced kind) whose date
   is **7 days old or less**. A snapshot becomes eligible for demotion the
   day it turns *more than* 7 days old (i.e. a snapshot exactly 7 days old
   is still kept; day 8 is the first eligible day) — chosen so "keep for
   7 days" reads the way it's usually meant, not as an off-by-one trap.

2. **Weekly tier, 8 weeks** — once a daily ages out of rule 1, it isn't
   deleted outright. One representative per calendar week is kept for the
   next 8 weeks (the **oldest surviving snapshot in each week**, so the
   kept date is stable and doesn't drift as files get removed — matches
   the convention `scripts/signal_center_archive_rotate.py` already uses
   for its own weekly Sunday rotation). Everything else in that week is
   removed once its weekly representative is confirmed.

3. **Indefinite keep — pre-migration / pre-restatement / incident
   checkpoints** — any snapshot taken deliberately ahead of a schema
   change, data migration, or an active incident (identifiable by name,
   not by date pattern — e.g. `*_pre-*`, `*_dry-dock*`, `*-restart_*`) is
   exempt from rules 1 and 2 and kept with no automatic expiry. These are
   qualitatively different from routine dailies: they exist specifically
   so a migration or incident can be unwound, and their value doesn't
   decay on a calendar schedule.

4. **Dedup — supersede within N hours** (new, this pass). A snapshot taken
   within **N hours** of an existing one, where nothing of note changed
   the DB in between, supersedes it rather than adding a second copy.
   Proposed **N = 4 hours**. **This rule and rule 3 can conflict** — two
   incident checkpoints 40 minutes apart are both "indefinite" under rule
   3 and both "redundant" under rule 4. Resolution: **rule 3 wins by
   default** (an incident checkpoint is never silently collapsed); dedup
   only ever applies to rule-3-protected snapshots on an explicit,
   named decision at the time the incident is closed out, not
   automatically. Rule 4 without this carve-out would have auto-collapsed
   today's own dry-dock checkpoints while the incident was still being
   actively referenced — exactly the failure mode a data-retention policy
   for this codebase should not have.

## Before removing anything: the off-host X9 copy must hold it

`scripts/offhost_backup.sh`'s nightly sync only matches the strict pattern
`trader_YYYY-MM-DD.db` (`find ... -name 'trader_20[0-9][0-9]-[0-9][0-9]-[0-9][0-9].db'`)
— **any ad-hoc-named snapshot (the rule-3 indefinite category, by
definition) is never synced to X9 by the existing cron, regardless of how
long it's kept locally.** This is a real gap, not a hypothetical one —
verified live 2026-09-11: zero of today's 4 ad-hoc `trader_*.db` files and
2 `signals_*.db` files exist anywhere on the mounted X9
(`/Volumes/Crucial X9/OLLIETRADES_BACKUPS/backups/`), which holds only the
standard-named dailies.

**Policy addition**: a snapshot may only be removed locally once a copy
(any copy — the standard sync or a one-off manual `rsync`) is confirmed
present on X9 **and** passes its own `PRAGMA integrity_check` there, not
just a byte-size match. For rule-3 indefinite-keep files specifically,
this means: since they're never auto-synced, either (a) copy them to X9
manually once (they don't need to move locally, just also exist there),
or (b) accept they're single-point-of-failure on the local disk for as
long as they're kept — which defeats the point of calling them
"indefinite." **Recommendation: manually rsync the current 6 ad-hoc files
to X9 now**, independent of whatever the Admiral decides about rule 4,
since local-only indefinite retention is a real risk regardless of the
dedup question.

## Simulated against the current inventory (2026-09-11, nothing applied)

| File | Date | Size | Category | Disposition under rules 1–3 |
|---|---|---|---|---|
| `trader_2026-09-04.db` | 09-04 | 1.061 GiB | daily | keep (exactly 7 days old — boundary, see rule 1) |
| `trader_2026-09-05.db` | 09-05 | 1.062 GiB | daily | keep |
| `trader_2026-09-06.db` | 09-06 | 1.062 GiB | daily | keep |
| `trader_2026-09-07.db` | 09-07 | 1.064 GiB | daily | keep |
| `trader_2026-09-08.db` | 09-08 | 1.086 GiB | daily | keep |
| `trader_2026-09-09.db` | 09-09 | 1.113 GiB | daily | keep |
| `trader_2026-09-10.db` | 09-10 13:07 | 1.170 GiB | daily (early — `db_snapshot.sh` ran manually ahead of the key-rotation restart; the 20:15 cron correctly skipped as a dup, this is the real daily for that date) | keep |
| `trader_pre-key-rotation-restart_20260910_134530.db` | 09-10 13:45 | 1.173 GiB | pre-migration | keep indefinitely (rule 3) |
| `trader_20260911_dry-dock.db` | 09-11 06:11 | 1.227 GiB | incident checkpoint (dry-dock start) | keep indefinitely (rule 3) |
| `trader_drydock_20260911_065430.db` | 09-11 06:54 | 1.229 GiB | incident checkpoint (breach discovered) | keep indefinitely (rule 3) |
| `trader_20260911_074244_pre_b6_restatement.db` | 09-11 07:42 | 1.230 GiB | pre-migration (options_trades restatement) | keep indefinitely (rule 3) |
| `signals_20260911_dry-dock.db` | 09-11 06:11 | 0.168 GiB | incident checkpoint | keep indefinitely (rule 3) |
| `signals_drydock_20260911_065430.db` | 09-11 06:54 | 0.168 GiB | incident checkpoint | keep indefinitely (rule 3) |

**Rules 1–3 applied literally today free 0 bytes.** This is the honest
result, not a shortfall in the policy: every current daily is within its
7-day window (none have reached the weekly tier yet), and every ad-hoc
file is a genuine, named migration/incident checkpoint from the last 24
hours, not stale cruft. The policy is sound and will start freeing space
as dailies age past day 7 and as incidents close out — it just has
nothing to do yet against *this* inventory.

**Rule 4 (dedup) is the only lever with real savings today**, and only
against the 3 trader.db + 2 signals.db dry-dock-era files, since they're
the only ones within any plausible N of each other. Three options,
Admiral's call:

| Option | What it keeps | What it frees |
|---|---|---|
| **A — no dedup** (rule 3 wins, nothing collapsed) | All 5 ad-hoc files | 0 GiB |
| **B — drop the middle checkpoint** (keep the true before/after bookends: 06:11 pre-dry-dock baseline + 07:42 pre-restatement; drop 06:54, the breach-discovery moment, now superseded since D16 confirms the incident is closed) | 4 of 5 | 1.229 GiB (`trader_drydock_20260911_065430.db` only) |
| **C — keep only the newest** (collapse all dry-dock-era snapshots to the single latest state) | 2 of 5 (renamed/latest trader + latest signals) | 2.456 + 0.168 = **2.624 GiB** |

None of these three options are applied — this table exists so a decision
can be made by picking a row, not by re-deriving the numbers each time.

## Off-host verification performed (2026-09-11)

All 7 of the current daily snapshots (`trader_2026-09-04.db` through
`trader_2026-09-10.db`) confirmed present on the mounted Crucial X9
(`/Volumes/Crucial X9/OLLIETRADES_BACKUPS/backups/`), byte-size exact
match to the local copy, **and** `PRAGMA integrity_check` = `ok` run
directly against each X9 copy (not inferred from the size match alone).
These 7 are the only files in the current inventory that rules 1–3 would
ever consider removing (once they age out) — and per the policy section
above, none of the rule-3 indefinite files have any X9 copy yet, which is
the real, actionable gap this pass surfaced.

## Incidental finding, not part of this policy

`data/backups/_archive/` is a separate 3.2 GiB of old `.db-shm`/`.db-wal`
sidecar debris (dated back to July) plus an `_orphaned_sidecars/`
subfolder. The existing `archive_ttl_cron`
(`find data/backups/_archive -name "trader_*.db.gz" -mtime +30 -delete`)
only matches `.db.gz`, so it has never touched these sidecar files —
they've been accumulating with no retention mechanism at all. Out of
scope for this pass (not what was asked), flagged to `docs/XO_BACKLOG.md`
as a smaller, separate cleanup.

## Status

**Proposed, not applied.** Nothing in `data/backups/` has been moved or
deleted. Waiting on:
1. Confirmation of rules 1–4 as written (or adjustments).
2. A choice among options A/B/C for today's dry-dock-era files.
3. Go-ahead to actually rsync the 6 currently-unsynced ad-hoc files to X9
   (recommended regardless of the A/B/C choice).

Once approved, implementation is a new script
(`scripts/backup_retention_apply.py`, not built yet) that applies these
rules on the same schedule as `db_snapshot.sh`/`offhost_backup.sh`
(after the nightly 20:30 offhost sync confirms X9 has the day's copy,
never before) — not a manual one-off cleanup, so the policy stays a
policy rather than reverting to ad hoc the next time disk pressure comes
up.
