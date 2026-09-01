#!/bin/bash
# scripts/db_snapshot.sh
# HM-BACKUP-SPINE-2026-07-01 Phase B — local daily snapshot of trader.db.
# Replacement for the healthcheck.py snapshot function (healthcheck.py stays OFF
# per HM-WATCHDOG-SUPERVISOR 2026-06-10 — this is a standalone script, not a
# resurrection of that daemon).
#
# NEVER deletes: retention prunes by MOVING older snapshots into
# data/backups/_archive/ and gzip-ing them in place there, never rm.
#
# HM-DISK-EMERGENCY-2026-08-25: archive step used to leave snapshots
# uncompressed, so _archive/ grew unbounded (38GB, ~78% reclaimable via
# gzip) and nearly filled the disk out from under the live trader.db.
# Fixed by gzipping each snapshot immediately after the mv.
#
# Schedule: cron 15 20 * * * (20:15 MST)
# NTFY topic: ollietrades-admin (failure/integrity-fail only, mirrors offhost_backup.sh doctrine)

set -euo pipefail

REPO="$HOME/autonomous-trader"
cd "$REPO"

DB="data/trader.db"
BACKUP_DIR="data/backups"
ARCHIVE_DIR="$BACKUP_DIR/_archive"
# HM-BACKUP-RETENTION-HARDEN-2026-09-01: raised 7->14. offhost_backup.sh has
# wanted a 14-day uncompressed daily-backup window since HM-HARDEN A1
# (2026-06-10, its own DAILIES glob does `tail -14`) but KEEP=7 here meant
# a plain .db file never survived past day 7 before being archived+gzip'd --
# offhost's 14-day target could never actually be met. At ~1GB/night
# uncompressed, 14 days is ~14GB against 38.6GiB free on this volume (well
# clear of the disk-full errors logged 2026-08-12/08-15/08-16/08-21).
KEEP=14
NTFY_TOPIC="ollietrades-admin"
NTFY_URL="https://ntfy.sh/$NTFY_TOPIC"
LOG="$REPO/logs/db_snapshot.log"

mkdir -p "$BACKUP_DIR" "$ARCHIVE_DIR" "$(dirname "$LOG")"
exec >>"$LOG" 2>&1
echo "=== $(date -Iseconds) db_snapshot START ==="

ntfy_post() {
    # DECOM-SILENCE 2026-07-19 — suppressed ahead of Gate 2 full removal.
    return 0
    local prio="$1"; shift
    curl -s -H "Priority: $prio" -d "$*" "$NTFY_URL" >/dev/null 2>&1 || true
}

DATE=$(date +%F)
DEST="$BACKUP_DIR/trader_$DATE.db"

if [ -f "$DEST" ]; then
    echo "  [SKIP] snapshot for $DATE already exists: $DEST"
    exit 0
fi

# HM-BACKUP-FREESPACE-PRECHECK-2026-09-01: `sqlite3 .backup` on a full volume
# previously failed mid-write ("Error: database or disk is full", logged
# repeatedly 2026-08-12/08-15/08-16/08-21) -- set -e caught the sqlite3
# failure so the script did abort, but only AFTER attempting the write, and
# a partial/truncated $DEST could be left on disk with no [FAIL] line
# distinguishing it from a clean run. Check free space BEFORE attempting,
# fail loudly with a clear reason, and never create $DEST at all if there
# isn't room. Require 1.3x the live DB's size as margin (.backup needs a
# full page-by-page copy plus WAL checkpoint headroom).
db_size_kb=$(du -k "$DB" | cut -f1)
free_kb=$(df -k "$BACKUP_DIR" | tail -1 | awk '{print $4}')
required_kb=$(( db_size_kb * 13 / 10 ))
if [ "$free_kb" -lt "$required_kb" ]; then
    echo "  [FAIL] insufficient free space for snapshot: have ${free_kb}KB, need ~${required_kb}KB (1.3x live DB ${db_size_kb}KB) — refusing to attempt .backup"
    ntfy_post high "DB snapshot ABORTED (free space): have ${free_kb}KB need ~${required_kb}KB"
    exit 1
fi
echo "  [OK] free-space precheck: ${free_kb}KB available, need ~${required_kb}KB"

sqlite3 "$DB" ".backup '$DEST'"

result=$(sqlite3 "$DEST" "PRAGMA integrity_check;")
if [ "$result" != "ok" ]; then
    echo "  [FAIL] integrity_check for $DEST: $result"
    ntfy_post high "DB snapshot integrity_check FAILED for $DEST: $result"
    exit 1
fi
size=$(du -h "$DEST" | cut -f1)
echo "  [OK] snapshot $DEST ($size, integrity_check=ok)"

# Retention: keep newest $KEEP, archive (never delete) the rest.
# HM-BACKUP-RETENTION-HARDEN-2026-09-01: was `ls -1 <glob> 2>/dev/null | sort`
# assigned inside `all=( $(...) )` under `set -euo pipefail`. Two fragility
# points, either of which can silently zero out $count without tripping
# errexit (a failed command inside a $(...) used in an assignment does NOT
# trigger set -e in bash): (1) if the glob matched nothing, `shopt -s
# nullglob` makes the unquoted pattern expand to zero arguments, so a bare
# `ls -1` (no args) would list $PWD (the repo root, post the `cd "$REPO"`
# above) instead of failing -- garbage in, not a clean empty count; (2) any
# transient `ls`/`sort` hiccup (the same near-full-disk conditions that
# produced "database or disk is full" on 08-12/08-15/08-16/08-21 in this
# same log) inside the pipeline is swallowed by the assignment context.
# Empirically confirmed 2026-09-01: logs/db_snapshot.log shows [OK] snapshot
# every night 08-25->08-31 (7 straight) with ZERO [ARCHIVED] lines across
# that whole span, despite dated .db files necessarily accumulating past
# the then-KEEP=7 -- the retention trigger silently never fired. Could not
# reproduce ".gz being double-counted" specifically by direct test (a
# sample dir with mixed .db/.db.gz correctly counted only the .db files);
# switched to `find -maxdepth 1` instead either way, matching the already-
# more-robust pattern offhost_backup.sh's own DAILIES glob already uses --
# no subshell-pipe assignment, no nullglob dependency, no ambiguity if
# nothing matches (empty array, not $PWD).
all=()
while IFS= read -r f; do
    all+=("$f")
done < <(find "$BACKUP_DIR" -maxdepth 1 -type f -name 'trader_20[0-9][0-9]-[0-9][0-9]-[0-9][0-9].db' 2>/dev/null | sort)
count=${#all[@]}
echo "  [RETENTION] $count dated snapshot(s) on disk, KEEP=$KEEP"
if [ "$count" -gt "$KEEP" ]; then
    to_archive=$((count - KEEP))
    for ((i=0; i<to_archive; i++)); do
        old="${all[$i]}"
        base=$(basename "$old")
        mv "$old" "$ARCHIVE_DIR/"
        gzip "$ARCHIVE_DIR/$base"
        echo "  [ARCHIVED] $old -> $ARCHIVE_DIR/${base}.gz"
    done
fi

echo "=== $(date -Iseconds) db_snapshot DONE ==="
