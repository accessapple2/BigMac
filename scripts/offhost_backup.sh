#!/bin/bash
# scripts/offhost_backup.sh
# Off-host backup of bigmac -> Crucial X9 (external drive, mounted locally)
# HM-AY-α #1 — Scotty 2.4 sprint. See docs/SCOTTY_AUDIT_2.md Section L (Backup Reality Check).
# Repointed 192.168.1.168 (olliemax, RETIRED) -> local Crucial X9 volume on
# 2026-08-27. Olliemax was decommissioned; every SSH-based run since had been
# failing silently against a dead host. rsync is copy-only (no --delete).
#
# Replicates:
#   - data/trader.db (checkpointed daily snapshot, see HM-TRADER-SNAPSHOT-HARDEN below)
#   - signal-center/signals.db (+ -shm / -wal)
#   - backups/trader_YYYY-MM-DD.db (last 14 daily snapshots)
#
# HM-TRADER-SNAPSHOT-HARDEN 2026-08-27: trader.db used to rsync the LIVE file
# (+ -shm/-wal) with --soft, because copying a continuously-written multi-
# hundred-MB WAL races rsync's own delta-verify and fails most nights (see
# run_rsync's --soft comment below). Fixed at the source instead of tolerating
# the race: now copies scripts/db_snapshot.sh's checkpointed `.backup` snapshot
# for TODAY (data/backups/trader_YYYY-MM-DD.db) into data/trader.db on the X9 --
# a static, already-integrity-checked file with an empty -wal (fully
# checkpointed, verified via PRAGMA journal_mode/-wal size at design time), so
# no sidecars need copying. db_snapshot.sh runs 20:15 MST, 15 min before this
# script's real 20:30 MST cron slot (verified live via `crontab -l` -- NOT the
# stale "30 6 * * *" this file used to claim below); today's snapshot is
# therefore always <15min old here. Gets the same hard [FAIL]-on-error/missing
# guarantee as signals.db and the daily backups (no --soft).
# ~/ollietrades/tractor_beam/tractor.db rsync REMOVED from integrity-check
# (HM-OLLIETRADES-FOLDER-DISPOSITION, 2026-07-06) -- ~/ollietrades archived to
# ~/ollietrades_archived_2026-07-06 (tractor_beam retired, dead since
# 2026-04-17); the `run_rsync` call below still fires but no-ops via its own
# [ -f ... ] guard now that the source path is gone.
#
# Schedule: cron `30 20 * * *` (20:30 MST; time-based = reboot-survivable on
# this box; HM-OFFHOST-DR-WIRE 2026-05-31). Corrected 2026-08-27 -- the prior
# "30 6 * * *" here was stale doctrine; live `crontab -l` has always shown
# 20:30, 15 min after db_snapshot.sh's 20:15 slot (see HM-TRADER-SNAPSHOT-
# HARDEN above, which depends on that ordering).
# NTFY topic: ollietrades-admin
#
# Sacred rules: rsync only. No source mutation. No rm. No VACUUM.
# DO NOT touch OLLIETRADES_ARCHIVE or any other folder on the X9 -- it holds
# the decommission archive and personal files. This script only ever reads/
# writes inside $DEST_BASE (OLLIETRADES_BACKUPS).

set -euo pipefail

MOUNT_POINT="/Volumes/Crucial X9"
DEST_BASE="$MOUNT_POINT/OLLIETRADES_BACKUPS"
REPO="$HOME/autonomous-trader"
LOG="$REPO/logs/offhost_backup.log"
NTFY_TOPIC="ollietrades-admin"
NTFY_URL="https://ntfy.sh/$NTFY_TOPIC"

mkdir -p "$(dirname "$LOG")"
exec >>"$LOG" 2>&1
echo "=== $(date -Iseconds) offhost_backup START ==="

# HM-X9-MOUNT-GUARD (2026-08-27): if the X9 is unplugged, "/Volumes/Crucial X9"
# is just a path under /Volumes -- a real directory that lives on the BOOT
# DRIVE, not the external volume. mkdir -p / rsync would happily create it
# there and silently fill the boot disk instead of failing. Directory
# EXISTENCE proves nothing; only the device-id check below proves it's a
# genuinely separate mounted filesystem.
assert_x9_mounted() {
    if [ ! -d "$MOUNT_POINT" ]; then
        echo "!!! ABORT: '$MOUNT_POINT' does not exist -- Crucial X9 is not connected." >&2
        exit 1
    fi
    local dev_mount dev_parent
    dev_mount=$(stat -f "%d" "$MOUNT_POINT")
    dev_parent=$(stat -f "%d" "/Volumes")
    if [ "$dev_mount" = "$dev_parent" ]; then
        echo "!!! ABORT: '$MOUNT_POINT' is NOT a real mount (same device id $dev_mount as /Volumes) -- the Crucial X9 is unplugged and this is a plain directory on the boot drive. Refusing to back up here." >&2
        exit 1
    fi
    echo "  [OK] Crucial X9 confirmed mounted (device id $dev_mount, distinct from /Volumes' $dev_parent)"
}
assert_x9_mounted

ts_start=$(date +%s)
errors=0
note=""

ntfy_post() {
    # DECOM-SILENCE 2026-07-19 — suppressed ahead of Gate 2 full removal.
    return 0
    local prio="$1"; shift
    local msg="$*"
    curl -s -H "Priority: $prio" -d "$msg" "$NTFY_URL" >/dev/null 2>&1 || true
}

run_rsync() {
    # HM-HARDEN A1 (2026-06-10): skip non-existent local sources (last arg is the
    # dest). WAL/SHM sidecars only exist when a DB has uncommitted pages;
    # a checkpointed DB has none, and rsync would error on the missing files →
    # false FAILURE + spurious high-priority NTFY. Filtering them keeps the
    # success/failure signal honest. bash 3.2-safe (no negative array indices).
    #
    # HM-BACKUP-SPINE-2026-07-01: optional leading --soft flag marks a component
    # best-effort/supplementary — failures are logged as [WARN], not [FAIL], and
    # do NOT increment the global errors/note used for overall exit code + NTFY.
    # Introduced because the live trader.db+wal rsync raced rsync's own whole-file
    # delta-verification against a continuously-written multi-hundred-MB WAL
    # ("trader.db-wal failed verification -- update discarded"), failing most
    # nights since 2026-06-18 and masking real signal from the components that
    # matter (the verified static snapshot in data/backups/, HM-BACKUP-SPINE Phase B).
    # HM-TRADER-SNAPSHOT-HARDEN 2026-08-27: trader.db no longer calls run_rsync
    # at all (it copies+renames db_snapshot.sh's checkpointed file directly,
    # below) so it no longer needs --soft. The flag stays available on this
    # helper for any future best-effort component.
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
        # HM-OFFHOST-SKIP-AS-FAILURE-2026-09-01: this used to be an
        # unconditional `return 0` (success) regardless of soft/hard --
        # every ORIGINAL source vanishing is never the same situation
        # HM-HARDEN A1 was built for (a checkpointed DB's WAL/SHM sidecars
        # legitimately absent while the DB itself is present -- that case
        # never reaches here at all, since srcs is only empty when ALL
        # passed paths are gone, sidecars included). Silently succeeding on
        # "nothing replicated" hid a full week of missing offhost dailies
        # with zero signal in the log or NTFY. Now follows the same
        # soft/hard policy real rsync failures already use below.
        if [ "$soft" -eq 1 ]; then
            echo "  [WARN-SKIP] $label (no source files present, best-effort, non-fatal)"
            return 0
        fi
        echo "  [FAIL-SKIP] $label (no source files present — nothing replicated)"
        errors=$((errors+1))
        note="$note $label(skip)"
        return 1
    fi
    mkdir -p "$dest"
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

# trader.db: copy today's checkpointed db_snapshot.sh backup, not the live file
# (HM-TRADER-SNAPSHOT-HARDEN 2026-08-27 -- see header comment for why/ordering).
TRADER_SNAPSHOT="$REPO/data/backups/trader_$(date +%F).db"
if [ -f "$TRADER_SNAPSHOT" ]; then
    mkdir -p "$DEST_BASE/data"
    if rsync -a --copy-links --no-owner --no-group "$TRADER_SNAPSHOT" "$DEST_BASE/data/trader.db"; then
        echo "  [OK] trader.db (checkpointed snapshot $(basename "$TRADER_SNAPSHOT"))"
    else
        echo "  [FAIL] trader.db"
        errors=$((errors+1))
        note="$note trader.db"
    fi
else
    echo "  [FAIL] trader.db (no snapshot for today: $TRADER_SNAPSHOT -- did db_snapshot.sh run? check logs/db_snapshot.log)"
    errors=$((errors+1))
    note="$note trader.db"
fi

# Live DB (+ WAL/SHM if present)
run_rsync "signals.db"     "$REPO/signal-center/signals.db" "$REPO/signal-center/signals.db-shm" "$REPO/signal-center/signals.db-wal" "$DEST_BASE/signal-center/" || true

# Tractor (optional)
if [ -f "$HOME/ollietrades/tractor_beam/tractor.db" ]; then
    run_rsync "tractor.db" "$HOME/ollietrades/tractor_beam/tractor.db" "$DEST_BASE/tractor/" || true
fi

# Last 14 daily atomic backups (year-agnostic)
# HM-BACKUP-SPINE-2026-07-01: repointed $REPO/backups -> $REPO/data/backups.
# The old $REPO/backups dir was written by healthcheck.py, disabled 2026-06-10
# (HM-WATCHDOG-SUPERVISOR) -- it froze at trader_2026-06-06.db and this script
# kept faithfully re-syncing those same 7 stale files every night since, never
# failing but never sending anything new either. scripts/db_snapshot.sh (Phase B)
# now writes fresh dated snapshots to data/backups/ daily at 20:15 MST.
# HM-BACKUP-RETENTION-HARDEN-2026-09-01: header comment corrected 7->14 to
# match the code below, which has said `tail -14` since HM-HARDEN A1
# (2026-06-10) -- db_snapshot.sh's own KEEP was still 7 until today, so this
# 14-day target could never actually be met (a plain .db never survived
# past day 7 before being archived+gzip'd there); raised in lockstep.
shopt -s nullglob
DAILIES=( $(find "$REPO"/data/backups -maxdepth 1 -type f -name 'trader_20[0-9][0-9]-[0-9][0-9]-[0-9][0-9].db' 2>/dev/null | sort | tail -14) )  # HM-HARDEN A1: 14-day retention
if [ ${#DAILIES[@]} -gt 0 ]; then
    run_rsync "daily-backups (${#DAILIES[@]})" "${DAILIES[@]}" "$DEST_BASE/backups/" || true
else
    # HM-OFFHOST-SKIP-AS-FAILURE-2026-09-01: previously silent -- no [SKIP],
    # no [FAIL], nothing at all, since run_rsync was never even called. If
    # db_snapshot.sh ever stops producing dated snapshots (its own failure,
    # a path change, anything), this branch is what would have caught it --
    # instead a full week of missing offhost dailies produced zero signal
    # in this log or NTFY. Treat "no dailies found at all" as a hard
    # failure, same severity as every other real component here.
    echo "  [FAIL] daily-backups (0 found in $REPO/data/backups — did db_snapshot.sh run? check logs/db_snapshot.log)"
    errors=$((errors+1))
    note="$note daily-backups(none-found)"
fi

# Integrity check via local sqlite3 CLI against the just-copied files on the X9.
# HM-X9-LOCAL-INTEGRITY (2026-08-27): replaces the old ssh+remote-python check
# now that the destination is a local mount, not olliemax over the network.
# HM-OLLIETRADES-FOLDER-DISPOSITION (2026-07-06): tractor.db stays out of this
# list -- ~/ollietrades archived (tractor_beam retired, dead since
# 2026-04-17), the copy is a frozen archival artifact, not a live replication
# target. Re-verifying integrity_check=ok on a file that will never change
# again is pointless.
echo "--- integrity check (local) ---"
CHECK_FILES=("$DEST_BASE/data/trader.db" "$DEST_BASE/signal-center/signals.db")
while IFS= read -r f; do
    CHECK_FILES+=("$f")
done < <(find "$DEST_BASE/backups" -maxdepth 1 -type f -name 'trader_20[0-9][0-9]-[0-9][0-9]-[0-9][0-9].db' 2>/dev/null | sort | tail -7)

fail=0
integrity=""
# HM-OFFHOST-GZ-INTEGRITY-2026-09-01: sqlite3 cannot PRAGMA integrity_check a
# .db.gz directly -- it would just report an open failure, which the || r=
# fallback below already catches, but as a misleading "BAD ... ERROR: unable
# to open" for what may be a perfectly good compressed file. None of today's
# CHECK_FILES globs match .gz (both are .db-only), so this is currently
# unreached defensively -- guards the loop if a future change ever adds an
# archived/compressed source to CHECK_FILES instead of failing confusingly.
for f in "${CHECK_FILES[@]}"; do
    [ -f "$f" ] || continue
    if [[ "$f" == *.gz ]]; then
        tmp_check=$(mktemp)
        if gunzip -c "$f" > "$tmp_check" 2>/dev/null; then
            r=$(sqlite3 "$tmp_check" "PRAGMA integrity_check;" 2>&1) || r="ERROR: $r"
        else
            r="ERROR: gunzip failed to decompress $f"
        fi
        rm -f "$tmp_check"
    else
        r=$(sqlite3 "$f" "PRAGMA integrity_check;" 2>&1) || r="ERROR: $r"
    fi
    if [ "$r" != "ok" ]; then
        fail=$((fail+1))
        integrity="$integrity
BAD $f: $r"
    fi
done
integrity="$integrity
FAIL_COUNT=$fail"
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
    msg="Off-host backup OK: ${db_count} DBs replicated to Crucial X9 in ${elapsed}s, all integrity_check=ok"
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
