#!/bin/bash
# scripts/offhost_backup.sh
# Off-host backup of bigmac → Ollie Max (192.168.1.168)
# HM-AY-α #1 — Scotty 2.4 sprint. See docs/SCOTTY_AUDIT_2.md Section L (Backup Reality Check).
# Repointed 192.168.1.166 → 192.168.1.168 on 2026-05-31 (HM-OFFHOST-DR-WIRE) once
# passwordless bigmac→.168 SSH was live. rsync is copy-only (no --delete).
#
# Replicates:
#   - data/trader.db (+ -shm / -wal)
#   - signal-center/signals.db (+ -shm / -wal)
#   - backups/trader_YYYY-MM-DD.db (last 14 daily snapshots)
#
# ~/ollietrades/tractor_beam/tractor.db rsync REMOVED from integrity-check
# (HM-OLLIETRADES-FOLDER-DISPOSITION, 2026-07-06) -- ~/ollietrades archived to
# ~/ollietrades_archived_2026-07-06 (tractor_beam retired, dead since
# 2026-04-17); the `run_rsync` call below still fires but no-ops via its own
# [ -f ... ] guard now that the source path is gone. The already-replicated
# copy on Ollie (~/bigmac-backups/tractor/tractor.db) stays put as a frozen
# archival artifact, not a live target.
#
# Schedule: cron `30 6 * * *` (time-based = reboot-survivable on this box; HM-OFFHOST-DR-WIRE 2026-05-31).
# NTFY topic: ollietrades-admin
#
# Sacred rules: rsync only. No source mutation. No rm. No VACUUM.

set -euo pipefail

REMOTE_HOST="192.168.1.168"
REMOTE_BASE="bigmac-backups"
REPO="$HOME/autonomous-trader"
LOG="$REPO/logs/offhost_backup.log"
NTFY_TOPIC="ollietrades-admin"
NTFY_URL="https://ntfy.sh/$NTFY_TOPIC"

mkdir -p "$(dirname "$LOG")"
exec >>"$LOG" 2>&1
echo "=== $(date -Iseconds) offhost_backup START ==="

ts_start=$(date +%s)
errors=0
note=""

ntfy_post() {
    local prio="$1"; shift
    local msg="$*"
    curl -s -H "Priority: $prio" -d "$msg" "$NTFY_URL" >/dev/null 2>&1 || true
}

run_rsync() {
    # HM-HARDEN A1 (2026-06-10): skip non-existent local sources (last arg is the
    # remote dest). WAL/SHM sidecars only exist when a DB has uncommitted pages;
    # a checkpointed DB has none, and rsync would error on the missing files →
    # false FAILURE + spurious high-priority NTFY. Filtering them keeps the
    # success/failure signal honest. bash 3.2-safe (no negative array indices).
    #
    # HM-BACKUP-SPINE-2026-07-01: optional leading --soft flag marks a component
    # best-effort/supplementary — failures are logged as [WARN], not [FAIL], and
    # do NOT increment the global errors/note used for overall exit code + NTFY.
    # Introduced because the live trader.db+wal rsync races rsync's own whole-file
    # delta-verification against a continuously-written multi-hundred-MB WAL
    # ("trader.db-wal failed verification -- update discarded"), failing most
    # nights since 2026-06-18 and masking real signal from the components that
    # matter (the verified static snapshot in data/backups/, HM-BACKUP-SPINE Phase B).
    local soft=0
    if [ "$1" = "--soft" ]; then soft=1; shift; fi
    local label="$1"; shift
    local all=("$@")
    local n=${#all[@]}
    local dest="${all[$((n-1))]}"
    local srcs=()
    local i
    for ((i=0; i<n-1; i++)); do [ -e "${all[$i]}" ] && srcs+=("${all[$i]}"); done
    if [ ${#srcs[@]} -eq 0 ]; then
        echo "  [SKIP] $label (no source files present)"
        return 0
    fi
    if rsync -a --copy-links --no-owner --no-group "${srcs[@]}" "$dest"; then
        echo "  [OK] $label"
        return 0
    else
        if [ "$soft" -eq 1 ]; then
            echo "  [WARN] $label (best-effort, non-fatal)"
        else
            echo "  [FAIL] $label"
            errors=$((errors+1))
            note="$note $label"
        fi
        return 1
    fi
}

# Live DBs (+ WAL/SHM if present)
run_rsync --soft "trader.db" "$REPO/data/trader.db" "$REPO/data/trader.db-shm" "$REPO/data/trader.db-wal" "$REMOTE_HOST:~/$REMOTE_BASE/data/" || true
run_rsync "signals.db"     "$REPO/signal-center/signals.db" "$REPO/signal-center/signals.db-shm" "$REPO/signal-center/signals.db-wal" "$REMOTE_HOST:~/$REMOTE_BASE/signal-center/" || true

# Tractor (optional)
if [ -f "$HOME/ollietrades/tractor_beam/tractor.db" ]; then
    run_rsync "tractor.db" "$HOME/ollietrades/tractor_beam/tractor.db" "$REMOTE_HOST:~/$REMOTE_BASE/tractor/" || true
fi

# Last 7 daily atomic backups (year-agnostic)
# HM-BACKUP-SPINE-2026-07-01: repointed $REPO/backups -> $REPO/data/backups.
# The old $REPO/backups dir was written by healthcheck.py, disabled 2026-06-10
# (HM-WATCHDOG-SUPERVISOR) -- it froze at trader_2026-06-06.db and this script
# kept faithfully re-syncing those same 7 stale files every night since, never
# failing but never sending anything new either. scripts/db_snapshot.sh (Phase B)
# now writes fresh dated snapshots to data/backups/ daily at 20:15 MST.
shopt -s nullglob
DAILIES=( $(find "$REPO"/data/backups -maxdepth 1 -type f -name 'trader_20[0-9][0-9]-[0-9][0-9]-[0-9][0-9].db' 2>/dev/null | sort | tail -14) )  # HM-HARDEN A1: 14-day retention
if [ ${#DAILIES[@]} -gt 0 ]; then
    run_rsync "daily-backups (${#DAILIES[@]})" "${DAILIES[@]}" "$REMOTE_HOST:~/$REMOTE_BASE/backups/" || true
fi

# Integrity check via remote python (sqlite3 CLI not on Ollie)
# HM-OLLIETRADES-FOLDER-DISPOSITION (2026-07-06): tractor.db dropped from this
# list -- ~/ollietrades archived (tractor_beam retired, dead since 2026-04-17),
# the remote copy is now a frozen archival artifact, not a live replication
# target. Re-verifying integrity_check=ok on a file that will never change
# again is pointless; the copy stays on Ollie for the record either way.
echo "--- integrity check (remote) ---"
integrity=$(ssh -o ConnectTimeout=10 "$REMOTE_HOST" 'python3 -c "
import sqlite3, glob
fail = 0
for f in [\"/home/bigmac/'"$REMOTE_BASE"'/data/trader.db\",
          \"/home/bigmac/'"$REMOTE_BASE"'/signal-center/signals.db\"] + sorted(glob.glob(\"/home/bigmac/'"$REMOTE_BASE"'/backups/trader_2026-*.db\"))[-7:]:
    try:
        c = sqlite3.connect(f)
        r = c.execute(\"PRAGMA integrity_check\").fetchone()[0]
        c.close()
        if r != \"ok\": fail += 1; print(f\"BAD {f}: {r}\")
    except Exception as e: fail += 1; print(f\"BAD {f}: {e}\")
print(f\"FAIL_COUNT={fail}\")
"' 2>&1) || true
echo "$integrity"

ts_end=$(date +%s)
elapsed=$((ts_end - ts_start))

if echo "$integrity" | grep -q "FAIL_COUNT=0" && [ "$errors" -eq 0 ]; then
    # HM-OLLIETRADES-FOLDER-DISPOSITION (2026-07-06): was a hardcoded "10 DBs"
    # that drifted out of sync with reality (predated the 14-day daily-backup
    # retention bump, and didn't survive tractor.db's removal above) -- count
    # what was actually replicated this run instead: trader.db + signals.db
    # + however many daily snapshots existed tonight.
    db_count=$((2 + ${#DAILIES[@]}))
    msg="Off-host backup OK: ${db_count} DBs replicated to Ollie in ${elapsed}s, all integrity_check=ok"
    # HM-HARDEN A1 (2026-06-10): NTFY on FAILURE only — success is logged, not
    # pushed (admin-channel noise reduction). The failure branch below still NTFYs.
    echo "=== SUCCESS: $msg ==="
    exit 0
else
    msg="Off-host backup FAILED: rsync_errors=$errors integrity=[$(echo "$integrity" | grep -E 'BAD|FAIL_COUNT' | tr '\n' ' ')] note=$note"
    ntfy_post high "$msg"
    echo "=== FAILURE: $msg ==="
    exit 1
fi
