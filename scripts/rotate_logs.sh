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
# writes to the new path. Every target below is rotated the same
# copy-then-truncate-in-place way, even the cron-invoked ones (which
# reopen their file fresh each tick and would tolerate rename just fine) —
# one safe pattern for every entry beats reasoning about two.
#
# HM-OPS-SENTINEL formalization (2026-07-07): added logs/hm_ops_sentinel_cron.log
# -- every-5-min cron with no prior rotation mechanism, would otherwise grow
# unbounded forever like trader.log did before this script existed. Smaller
# 10MB threshold (vs trader.log's 100MB) since it grows much slower
# (~58KB/day observed) and there's no reason to let a slow-growing cron log
# sit around for months before its first rotation.
#
# HM-NTFY-IPV6-NOROUTE-SWEEP followup (2026-07-11): added logs/watchdog_cron.log
# -- watchdog.py is a long-running process (60s heartbeat loop) launched via
# `nohup ... >> logs/watchdog_cron.log 2>&1 &` by watchdog_supervisor.sh, so it
# holds this path open the same way main.py holds trader.log open -- same
# truncate-in-place requirement, already the default behavior of rotate_one
# below. Found unrotated at 5.6MB/42 days (~133KB/day, since 2026-05-30) while
# checking log health after the ntfy sweep. Same 10MB threshold tier as
# hm_ops_sentinel_cron.log (comparable growth rate, both frequent low-volume
# heartbeat-style logs).
#
# Schedule: cron weekly Sun 05:00 MST.

set -euo pipefail

REPO="$HOME/autonomous-trader"
cd "$REPO"

ARCHIVE_DIR="logs/_archive"
LOG="$REPO/logs/rotate_logs.log"

mkdir -p "$ARCHIVE_DIR" "$(dirname "$LOG")"
exec >>"$LOG" 2>&1
echo "=== $(date -Iseconds) rotate_logs START ==="

rotate_one() {
    local target="$1" threshold_bytes="$2" archive_prefix="$3"

    if [ ! -f "$target" ]; then
        echo "  [SKIP] $target does not exist"
        return 0
    fi

    local size
    size=$(stat -f %z "$target" 2>/dev/null || stat -c %s "$target")
    if [ "$size" -le "$threshold_bytes" ]; then
        echo "  [SKIP] $target is ${size} bytes, under ${threshold_bytes}-byte threshold"
        return 0
    fi

    local date_str dest suffix
    date_str=$(date +%F)
    dest="$ARCHIVE_DIR/${archive_prefix}_$date_str.log.gz"
    suffix=2
    while [ -e "$dest" ]; do
        dest="$ARCHIVE_DIR/${archive_prefix}_${date_str}_$suffix.log.gz"
        suffix=$((suffix + 1))
    done

    # Copy (not move) the live content out, THEN truncate in place -- truncation
    # must hit the SAME inode any live writer already has open.
    gzip -c "$target" > "$dest"
    local gzip_size
    gzip_size=$(stat -f %z "$dest" 2>/dev/null || stat -c %s "$dest")
    if [ "$gzip_size" -lt 1024 ]; then
        echo "  [FAIL] gzip output suspiciously small (${gzip_size} bytes) -- aborting, NOT truncating $target"
        return 1
    fi
    echo "  [OK] archived ${size} bytes -> $dest (${gzip_size} bytes gzipped)"

    : > "$target"
    echo "  [OK] truncated $target in place (fd preserved for live writer)"
}

rotate_one "logs/trader.log" $((100 * 1024 * 1024)) "trader"
rotate_one "logs/hm_ops_sentinel_cron.log" $((10 * 1024 * 1024)) "hm_ops_sentinel_cron"
rotate_one "logs/watchdog_cron.log" $((10 * 1024 * 1024)) "watchdog_cron"

echo "=== $(date -Iseconds) rotate_logs DONE ==="
