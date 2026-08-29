#!/bin/bash
# PROPOSED — HM-DEPARTURE-HARDENING Phase 1 item 3 (disk-space alert gap).
# Not yet in crontab; standalone until approved.
#
# scripts/vitals.sh already knows how to read disk usage but is manual-only
# (never scheduled, no alerting) — this is the missing automated half.
# Independent of backup_freshness_check.sh / db_snapshot.sh by construction
# (Doctrine: "an alarm must not share a failure mode with what it watches" —
# CLAUDE.md). A full disk would itself make backups start silently failing,
# so this can't rely on the same mechanism to notice.
#
# Threshold: 85% used (WARN) / 95% used (ALARM), configurable via env.
# Rate-limited to one push per severity level per day (state file), same
# spirit as watchdog.py's cooldown — a full disk doesn't need re-paging every
# 15 minutes once you've been told once.
#
# Proposed schedule: cron 3x/day, e.g. `0 6,13,20 * * *` (matches the
# existing morning/midday/evening cadence other ops scripts use).

set -uo pipefail

REPO="$HOME/autonomous-trader"
cd "$REPO"

WARN_PCT="${DISK_WARN_PCT:-85}"
ALARM_PCT="${DISK_ALARM_PCT:-95}"
NTFY_TOPIC="ollietrades-admin"
NTFY_URL="https://ntfy.sh/$NTFY_TOPIC"
LOG="$REPO/logs/disk_space_alert.log"
STATE_FILE="$REPO/data/.disk_space_alert_state"

mkdir -p "$(dirname "$LOG")" "$(dirname "$STATE_FILE")"
exec >>"$LOG" 2>&1
echo "=== $(date -Iseconds) disk_space_alert START ==="

ntfy_post() {
    # DECOM-SILENCE 2026-07-19 — suppressed ahead of Gate 2 full removal.
    return 0
    local prio="$1"; shift
    curl -s -H "Priority: $prio" -d "$*" "$NTFY_URL" >/dev/null 2>&1 || true
}

# already_alerted_today <level> -> 0 if we've already pushed this level today
already_alerted_today() {
    local level="$1" today
    today=$(date +%F)
    [ -f "$STATE_FILE" ] && grep -q "^${today} ${level}\$" "$STATE_FILE" 2>/dev/null
}

mark_alerted() {
    local level="$1" today
    today=$(date +%F)
    echo "${today} ${level}" >> "$STATE_FILE"
    # keep the state file small — only today's entries matter
    tail -n 20 "$STATE_FILE" > "${STATE_FILE}.tmp" && mv "${STATE_FILE}.tmp" "$STATE_FILE"
}

USED_PCT=$(df -h / 2>/dev/null | awk 'NR==2 {gsub("%","",$5); print $5}')
FREE_HUMAN=$(df -h / 2>/dev/null | awk 'NR==2 {print $4}')

if [ -z "$USED_PCT" ]; then
    echo "  [ERROR] could not read disk usage via df"
    ntfy_post high "disk_space_alert: could not read disk usage on bigmac (df failed)"
    exit 1
fi

echo "  disk / : ${USED_PCT}% used, ${FREE_HUMAN} free (warn=${WARN_PCT}%, alarm=${ALARM_PCT}%)"

if [ "$USED_PCT" -ge "$ALARM_PCT" ]; then
    if already_alerted_today "ALARM"; then
        echo "  [ALARM] ${USED_PCT}% used — already alerted today, suppressing repeat"
    else
        echo "  [ALARM] ${USED_PCT}% used (>= ${ALARM_PCT}% alarm threshold)"
        ntfy_post urgent "DISK ALARM: bigmac / is ${USED_PCT}% full (${FREE_HUMAN} free) — action needed soon"
        mark_alerted "ALARM"
    fi
elif [ "$USED_PCT" -ge "$WARN_PCT" ]; then
    if already_alerted_today "WARN"; then
        echo "  [WARN] ${USED_PCT}% used — already alerted today, suppressing repeat"
    else
        echo "  [WARN] ${USED_PCT}% used (>= ${WARN_PCT}% warn threshold)"
        ntfy_post default "Disk space WARN: bigmac / is ${USED_PCT}% full (${FREE_HUMAN} free)"
        mark_alerted "WARN"
    fi
else
    echo "  [OK] ${USED_PCT}% used, below ${WARN_PCT}% threshold"
fi

echo "=== $(date -Iseconds) disk_space_alert DONE ==="
