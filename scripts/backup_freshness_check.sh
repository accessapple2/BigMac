#!/bin/bash
# scripts/backup_freshness_check.sh
# HM-BACKUP-SPINE-2026-07-01 Phase C3 — freshness alarm ("never again" guard).
# Independent of offhost_backup.sh / db_snapshot.sh success paths by construction
# (Doctrine: "an alarm must not share a failure mode with what it watches" — see
# CLAUDE.md "Alarms must not share a failure mode with what they watch"). If either
# the newest local snapshot or the newest off-host snapshot is >48h old, NTFY WARN
# regardless of what the producing cron jobs last reported about themselves.
#
# Schedule: cron daily 20:45 MST (after db_snapshot 20:15 and offhost_backup 20:30).

set -uo pipefail  # no -e: we want to always reach the freshness comparisons even if ssh fails

REPO="$HOME/autonomous-trader"
cd "$REPO"

LOCAL_DIR="data/backups"
REMOTE_HOST="192.168.1.168"
REMOTE_DIR="bigmac-backups/backups"
STALE_HOURS=48
NTFY_TOPIC="ollietrades-admin"
NTFY_URL="https://ntfy.sh/$NTFY_TOPIC"
LOG="$REPO/logs/backup_freshness_check.log"

mkdir -p "$(dirname "$LOG")"
exec >>"$LOG" 2>&1
echo "=== $(date -Iseconds) backup_freshness_check START ==="

ntfy_post() {
    local prio="$1"; shift
    curl -s -H "Priority: $prio" -d "$*" "$NTFY_URL" >/dev/null 2>&1 || true
}

# age_hours_of_newest <dir-glob-path> -> prints age in hours of the newest
# trader_*.db file in the given local directory, or "NONE" if none exist.
age_hours_of_newest_local() {
    local dir="$1"
    local newest
    newest=$(ls -1t "$dir"/trader_20[0-9][0-9]-[0-9][0-9]-[0-9][0-9].db 2>/dev/null | head -1)
    if [ -z "$newest" ]; then
        echo "NONE"
        return
    fi
    local mtime now
    mtime=$(stat -f %m "$newest" 2>/dev/null || stat -c %Y "$newest")
    now=$(date +%s)
    echo $(( (now - mtime) / 3600 ))
}

# Same check but for a remote host over ssh (used for the real off-host check,
# and reused directly by the test harness against a local stale file, since
# stat-and-subtract logic is identical — no ssh needed for the correctness test).
check_and_alarm() {
    local label="$1" age="$2"
    if [ "$age" = "NONE" ]; then
        echo "  [ALARM] $label: no snapshot found at all"
        ntfy_post high "Backup freshness ALARM: $label has NO snapshot file present"
        return 1
    fi
    if [ "$age" -gt "$STALE_HOURS" ]; then
        echo "  [WARN] $label: newest snapshot is ${age}h old (>${STALE_HOURS}h threshold)"
        ntfy_post default "Backup freshness WARN: $label newest snapshot is ${age}h old (threshold ${STALE_HOURS}h)"
        return 1
    fi
    echo "  [OK] $label: newest snapshot is ${age}h old"
    return 0
}

if [ "${1:-}" = "--test" ]; then
    # Read-only correctness test: point the local-dir checker at the OLD, known-stale
    # legacy backups/ directory (frozen at trader_2026-06-06.db since healthcheck.py
    # was disabled) instead of the live data/backups/ directory. No files are written,
    # moved, or touched -- this only proves the age-comparison + WARN branch fires
    # correctly against real stale data before relying on it for the real check.
    echo "--- TEST MODE: checking known-stale legacy backups/ dir (read-only) ---"
    test_age=$(age_hours_of_newest_local "backups")
    echo "  computed age of newest legacy snapshot: ${test_age}h"
    if check_and_alarm "TEST-legacy-backups" "$test_age"; then
        echo "  [TEST FAIL] expected WARN branch to fire for known-stale data, but it reported OK"
        exit 1
    else
        echo "  [TEST OK] WARN branch correctly fired for known-stale (>48h) legacy snapshot"
        exit 0
    fi
fi

overall=0

local_age=$(age_hours_of_newest_local "$LOCAL_DIR")
check_and_alarm "local snapshot ($LOCAL_DIR)" "$local_age" || overall=1

remote_newest=$(ssh -o ConnectTimeout=10 "$REMOTE_HOST" \
    "ls -1t ~/$REMOTE_DIR/trader_20[0-9][0-9]-[0-9][0-9]-[0-9][0-9].db 2>/dev/null | head -1" 2>/dev/null)
if [ -z "$remote_newest" ]; then
    remote_age="NONE"
else
    remote_mtime=$(ssh -o ConnectTimeout=10 "$REMOTE_HOST" "stat -c %Y '$remote_newest' 2>/dev/null")
    if [ -z "$remote_mtime" ]; then
        remote_age="NONE"
    else
        now=$(date +%s)
        remote_age=$(( (now - remote_mtime) / 3600 ))
    fi
fi
check_and_alarm "off-host snapshot ($REMOTE_HOST:~/$REMOTE_DIR)" "$remote_age" || overall=1

echo "=== $(date -Iseconds) backup_freshness_check DONE (overall=$overall) ==="
exit $overall
