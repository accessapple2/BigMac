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
KEEP=7
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
shopt -s nullglob
all=( $(ls -1 "$BACKUP_DIR"/trader_20[0-9][0-9]-[0-9][0-9]-[0-9][0-9].db 2>/dev/null | sort) )
count=${#all[@]}
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
