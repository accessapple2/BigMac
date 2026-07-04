#!/bin/bash
# scripts/rotate_logs.sh
# HM-BACKUP-SPINE-2026-07-01 Phase E — logs/trader.log rotation (no sudo needed).
#
# Supersedes the 2026-05-10 B24 proposal (generic all-*.log rotator, 50MB
# threshold, numbered .1/.2/.3 copies, never cron'd/installed — see
# docs/proposals/log_rotation_plist.md). Prior version archived, not deleted:
# _archive/bak-2026-07-01/rotate_logs.sh.b24-proposal.pre-2026-07-01
#
# trader.log had NO rotation mechanism and grew 107MB (2026-06-12) -> 345MB
# (2026-07-01) unbounded. The live trader process holds an open file handle on
# this path and never reopens it, so rotation MUST truncate in place
# (`: > logs/trader.log`), never rename/rm -- renaming would orphan the
# process's fd on the old inode (still growing, invisibly) while nothing
# writes to the new path.
#
# Schedule: cron weekly Sun 05:00 MST.

set -euo pipefail

REPO="$HOME/autonomous-trader"
cd "$REPO"

TARGET="logs/trader.log"
ARCHIVE_DIR="logs/_archive"
THRESHOLD_BYTES=$((100 * 1024 * 1024))
LOG="$REPO/logs/rotate_logs.log"

mkdir -p "$ARCHIVE_DIR" "$(dirname "$LOG")"
exec >>"$LOG" 2>&1
echo "=== $(date -Iseconds) rotate_logs START ==="

if [ ! -f "$TARGET" ]; then
    echo "  [SKIP] $TARGET does not exist"
    exit 0
fi

size=$(stat -f %z "$TARGET" 2>/dev/null || stat -c %s "$TARGET")
if [ "$size" -le "$THRESHOLD_BYTES" ]; then
    echo "  [SKIP] $TARGET is ${size} bytes, under 100MB threshold"
    exit 0
fi

date_str=$(date +%F)
dest="$ARCHIVE_DIR/trader_$date_str.log.gz"
suffix=2
while [ -e "$dest" ]; do
    dest="$ARCHIVE_DIR/trader_${date_str}_$suffix.log.gz"
    suffix=$((suffix + 1))
done

# Copy (not move) the live content out, THEN truncate in place -- truncation
# must hit the SAME inode the trader process already has open.
gzip -c "$TARGET" > "$dest"
gzip_size=$(stat -f %z "$dest" 2>/dev/null || stat -c %s "$dest")
if [ "$gzip_size" -lt 1024 ]; then
    echo "  [FAIL] gzip output suspiciously small (${gzip_size} bytes) -- aborting, NOT truncating source"
    exit 1
fi
echo "  [OK] archived ${size} bytes -> $dest (${gzip_size} bytes gzipped)"

: > "$TARGET"
echo "  [OK] truncated $TARGET in place (fd preserved for live writer)"

echo "=== $(date -Iseconds) rotate_logs DONE ==="
