#!/bin/bash
# scripts/backup_freshness_check.sh
# HM-BACKUP-SPINE-2026-07-01 Phase C3 — freshness alarm ("never again" guard).
# Independent of offhost_backup.sh / db_snapshot.sh success paths by construction
# (Doctrine: "an alarm must not share a failure mode with what it watches" — see
# CLAUDE.md "Alarms must not share a failure mode with what they watch"). If either
# the newest local snapshot or the newest off-host snapshot is >48h old, ALARM
# regardless of what the producing cron jobs last reported about themselves.
#
# HM-BACKUP-FRESHNESS-FIX-2026-09-02: two independent breaks fixed here, found
# during the offhost_backup.sh TCC outage diagnosis (relay_2026-09-02_offhost-
# backup-tcc-diagnosis.md / -option-b-scoping.md):
#   1. The off-host check was hardcoded to SSH `192.168.1.168` (olliemax) --
#      decommissioned, dead since before this alarm even existed. It has been
#      firing "[ALARM] ... no snapshot found at all" every night since at
#      least 2026-08-23, for the wrong reason, and would keep doing so even
#      after a real off-host target existed at a different address.
#   2. Its ntfy delivery has been a stubbed no-op since DECOM-SILENCE
#      (2026-07-19) -- so even a correct alarm delivered nothing, anywhere,
#      visible only by reading this log file directly.
# Fixed: (1) the off-host check now stats the Crucial X9 local mount directly
# (no SSH, no dead host) -- genuinely reflects the one real off-host copy
# that exists today. This is an interim measure: X9 is locally-mounted, not
# truly off-site, so it stays a real gap until Option B (a genuine network
# target) is built -- repoint this back to an SSH check against that target
# once it exists. (2) alerts now go through the same live Pushover/RED_ALERT
# channel `scripts/origin_healthcheck.sh` already proved out (DECOM-SILENCE
# explicitly carves out an exception for RED_ALERT -- no need to lift it).
#
# HM-BACKUP-FRESHNESS-FIX-2026-09-04: the 09-02 fix above stopped the wrong-
# host/silent-ntfy bugs, but left a THIRD one live: `ls "$X9_BACKUPS_DIR"`
# from cron hits the exact same TCC Removable-Volume block that
# offhost_backup.sh had (relay_2026-09-02_offhost-backup-tcc-diagnosis.md) --
# cron, not sshd, was still the responsible process for this script's own
# reads. Confirmed live: every run since 09-02 20:45 MST logged "[ALARM] ...
# directory unreadable from cron" and fired a RED_ALERT regardless of
# whether the real off-host backup that night succeeded or failed -- a
# freshness alarm that can't tell fresh from stale provides no more signal
# than no alarm at all, just louder. Fixed the same way offhost_backup.sh's
# write side was fixed (relay_2026-09-03_offhost-backup-ssh-loopback-fix.md):
# repointed the CRONTAB line (not this file) to invoke via
# `ssh -tt -o BatchMode=yes localhost 'bash .../backup_freshness_check.sh'`
# so sshd (which holds the TCC grant) is the reading process, not cron.
# Live-verified same night: identical script, invoked via the loopback,
# read the X9 dir successfully and reported real freshness (`overall=0`)
# instead of the permanent unreadable-alarm. The `ls ... || alarm` fallback
# a few lines below stays as defense in depth (honest degraded-mode message)
# in case the loopback itself ever breaks -- not removed, just no longer
# the normal path.
#
# Schedule: cron daily 20:45 MST (after db_snapshot 20:15 and offhost_backup 20:30),
# invoked via the SSH-loopback wrapper above, not a bare `/bin/bash` line.

set -uo pipefail  # no -e: we want to always reach the freshness comparisons even if a check fails

REPO="$HOME/autonomous-trader"
cd "$REPO"

LOCAL_DIR="data/backups"
X9_MOUNT="/Volumes/Crucial X9"
X9_BACKUPS_DIR="$X9_MOUNT/OLLIETRADES_BACKUPS/backups"
STALE_HOURS=48
LOG="$REPO/logs/backup_freshness_check.log"

mkdir -p "$(dirname "$LOG")"
exec >>"$LOG" 2>&1
echo "=== $(date -Iseconds) backup_freshness_check START ==="

alert_post() {
    # HM-BACKUP-FRESHNESS-FIX-2026-09-02: replaces the DECOM-SILENCE-stubbed
    # ntfy-only poster. Same bash-shells-out-to-python pattern already
    # live-verified in scripts/origin_healthcheck.sh -- routes through
    # engine.alert_channels.send_alert() at RED_ALERT severity, which fires
    # Pushover (creds at /usr/local/etc/pushover.env, confirmed present)
    # regardless of DECOM-SILENCE, plus the DB-backed browser/dashboard
    # channel. A backup-freshness alarm is a real DR-severity condition,
    # not routine noise -- RED_ALERT, not WARNING, on purpose.
    local title="$1"; shift
    local msg="$*"
    ALERT_TITLE="$title" ALERT_MSG="$msg" \
      "$REPO/.venv/bin/python3" -c "
import os, sys
sys.path.insert(0, '.')
from engine.alert_channels import send_alert, AlertLevel
r = send_alert(
    os.environ['ALERT_MSG'],
    AlertLevel.RED_ALERT,
    'backup_freshness_check',
    title=os.environ['ALERT_TITLE'],
)
print(f'  [ALERT_RESULT] {r}')
"
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

# Shared by both the local-dir check and the X9 off-host check below (same
# stat-and-subtract age logic either way), and reused directly by the test
# harness against a known-stale local dir.
check_and_alarm() {
    local label="$1" age="$2"
    if [ "$age" = "NONE" ]; then
        echo "  [ALARM] $label: no snapshot found at all"
        alert_post "Backup freshness ALARM: $label" "$label has NO snapshot file present"
        return 1
    fi
    if [ "$age" -gt "$STALE_HOURS" ]; then
        echo "  [WARN] $label: newest snapshot is ${age}h old (>${STALE_HOURS}h threshold)"
        alert_post "Backup freshness WARN: $label" "$label newest snapshot is ${age}h old (threshold ${STALE_HOURS}h)"
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

# HM-BACKUP-FRESHNESS-FIX-2026-09-02: local stat against the X9 mount, no
# SSH, no dead host. Interim until a real network Option B target exists --
# see header comment.
#
# CAUGHT DURING LIVE VERIFICATION, same night: cron cannot even *read* the
# X9 mount, not just write to it -- the identical TCC Removable-Volume
# denial that blocks offhost_backup.sh's rsync, confirmed live by firing
# this exact check via a real temporary crontab entry while the X9 was
# provably mounted (`diskutil info` showing Mounted: Yes) in this same
# interactive shell at the same time. A plain `ls .../trader_*.db
# 2>/dev/null | head -1` can't distinguish "TCC silently blocked the read"
# from "genuinely no matching files" -- both produce empty output. Test
# directory-readability separately first so the alarm text stays honest
# instead of falsely claiming "no snapshot found" when snapshots exist and
# cron simply can't see them.
if ! ls "$X9_BACKUPS_DIR" >/dev/null 2>&1; then
    echo "  [ALARM] off-host snapshot (X9 local: $X9_BACKUPS_DIR): directory unreadable from cron (expected -- same TCC Removable-Volume block documented in relay_2026-09-02_offhost-backup-tcc-diagnosis.md; this does NOT mean no snapshot exists, only that cron can't see it -- verify manually from an interactive shell)"
    alert_post "Backup freshness ALARM: off-host (X9)" "X9 backups directory unreadable from cron (known TCC limitation) -- cannot verify off-host freshness automatically; check manually or wait for Option B"
    overall=1
else
    x9_age=$(age_hours_of_newest_local "$X9_BACKUPS_DIR")
    check_and_alarm "off-host snapshot (X9 local: $X9_BACKUPS_DIR)" "$x9_age" || overall=1
fi

echo "=== $(date -Iseconds) backup_freshness_check DONE (overall=$overall) ==="
exit $overall
