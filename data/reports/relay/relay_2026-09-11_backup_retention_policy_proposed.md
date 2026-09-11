# Relay — data/backups/ retention policy proposed, nothing applied. 2026-09-11

## Trigger

The `hm_ops_sentinel.py` disk-space WARNING that showed up in your
Pushover screenshot earlier was real: boot volume 87.7% full. Traced two
sources — `~/.ollama/models` (37GB, dead since olliemax took over model
serving 2026-09-09, you're removing it directly) and `data/backups/`
(12.8 GiB across 13 `trader*.db`/`signals*.db` files).

## What shipped: the policy document, not a cleanup

`docs/runbooks/backup-retention-policy.md` — four rules, written down per
your sketch:
1. Keep dated dailies 7 days.
2. One per week for 8 weeks after that.
3. Pre-migration/pre-restatement/incident checkpoints kept indefinitely.
4. New: a snapshot within N hours (proposed 4) of an existing one
   supersedes rather than adds — **with an explicit carve-out that rule 3
   wins by default**, since without it this rule would have auto-collapsed
   today's own dry-dock checkpoints while the incident was still being
   actively referenced in this very conversation.

## The honest number: 0 bytes freed today

Simulated the full policy against the current 13-file inventory. Every
daily is within its 7-day window (oldest is exactly 7 days old — a
boundary case, resolved in the doc). Every ad-hoc file (4 trader.db + 2
signals.db from today and yesterday's key-rotation) is a genuine, named
incident or migration checkpoint, not stale cruft. Rules 1–3, applied
literally, don't touch any of it. That's not a shortfall in the policy —
it's an accurate reflection of what's actually in there right now.

**The only real lever today is rule 4**, and only against the 5
dry-dock-era files (06:11, 06:54, 07:42 trader.db + the two signals.db
pairs). Three options laid out in the policy doc with exact byte counts:
keep all five (0 freed), drop just the 06:54 breach-discovery checkpoint
since the incident is closed per D16 (frees 1.229 GiB), or collapse down
to only the newest state (frees 2.624 GiB). Your call — none applied.

## A real gap found along the way, not part of what was asked

Checked the actual mounted X9 drive rather than trusting the sync
script's intent: **none of today's 6 ad-hoc-named backups have any
off-host copy.** `offhost_backup.sh`'s nightly sync only matches the
strict `trader_YYYY-MM-DD.db` pattern — anything named `*_dry-dock*` or
`*_pre-*` (i.e. every rule-3 "keep indefinitely" file, by definition)
silently never gets synced. Verified live: the 7 standard dailies
(09-04 through 09-10) are all on X9, byte-exact, **and** pass a real
`PRAGMA integrity_check` run directly against the X9 copy (not inferred
from size alone). The 6 ad-hoc files are local-only. Recommend a manual
one-off rsync of those 6 to X9 regardless of what you decide on rule 4 —
"kept indefinitely" and "single copy on one local disk" shouldn't
coexist for the same files.

## Also found, not acted on

`data/backups/_archive/` — 3.2 GiB of old `.db-shm`/`.db-wal` sidecar
files (July-dated), no retention mechanism at all (the existing TTL cron
only matches `.db.gz`). Filed to `docs/XO_BACKLOG.md` as its own smaller
cleanup, separate from the main policy.

## Status

Nothing in `data/backups/` was moved or deleted. Waiting on: the rules as
written (or adjustments), a choice among the three dry-dock-file options,
and a go-ahead on the X9 backfill for the 6 ad-hoc files. Implementation
(a real `scripts/backup_retention_apply.py`, applied on the standard
schedule after each night's offhost sync confirms X9 has the day's copy)
is not built — this pass is the policy, not the automation.
