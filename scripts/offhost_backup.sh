#!/bin/bash
# scripts/offhost_backup.sh
# Off-host backup of bigmac → Ollie Box (192.168.1.166)
# HM-AY-α #1 — Scotty 2.4 sprint. See docs/SCOTTY_AUDIT_2.md Section L (Backup Reality Check).
#
# Replicates:
#   - data/trader.db (+ -shm / -wal)
#   - signal-center/signals.db (+ -shm / -wal)
#   - ~/ollietrades/tractor_beam/tractor.db
#   - backups/trader_YYYY-MM-DD.db (last 7 daily snapshots)
#
# Schedule: launchd com.ollietrades.offhost-backup at 06:30 daily (after the 06:00 local backup).
# NTFY topic: ollietrades-admin
#
# Sacred rules: rsync only. No source mutation. No rm. No VACUUM.

set -euo pipefail

REMOTE_HOST="192.168.1.166"
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
    local label="$1"; shift
    local args=("$@")
    if rsync -a --copy-links --no-owner --no-group "${args[@]}"; then
        echo "  [OK] $label"
        return 0
    else
        echo "  [FAIL] $label"
        errors=$((errors+1))
        note="$note $label"
        return 1
    fi
}

# Live DBs (+ WAL/SHM if present)
run_rsync "trader.db"      "$REPO/data/trader.db"      "$REPO/data/trader.db-shm"      "$REPO/data/trader.db-wal"      "$REMOTE_HOST:~/$REMOTE_BASE/data/" || true
run_rsync "signals.db"     "$REPO/signal-center/signals.db" "$REPO/signal-center/signals.db-shm" "$REPO/signal-center/signals.db-wal" "$REMOTE_HOST:~/$REMOTE_BASE/signal-center/" || true

# Tractor (optional)
if [ -f "$HOME/ollietrades/tractor_beam/tractor.db" ]; then
    run_rsync "tractor.db" "$HOME/ollietrades/tractor_beam/tractor.db" "$REMOTE_HOST:~/$REMOTE_BASE/tractor/" || true
fi

# Last 7 daily atomic backups (year-agnostic)
shopt -s nullglob
DAILIES=( $(find "$REPO"/backups -maxdepth 1 -type f -name 'trader_20[0-9][0-9]-[0-9][0-9]-[0-9][0-9].db' 2>/dev/null | sort | tail -7) )
if [ ${#DAILIES[@]} -gt 0 ]; then
    run_rsync "daily-backups (${#DAILIES[@]})" "${DAILIES[@]}" "$REMOTE_HOST:~/$REMOTE_BASE/backups/" || true
fi

# Integrity check via remote python (sqlite3 CLI not on Ollie)
echo "--- integrity check (remote) ---"
integrity=$(ssh -o ConnectTimeout=10 "$REMOTE_HOST" 'python3 -c "
import sqlite3, glob
fail = 0
for f in [\"/home/bigmac/'"$REMOTE_BASE"'/data/trader.db\",
          \"/home/bigmac/'"$REMOTE_BASE"'/signal-center/signals.db\",
          \"/home/bigmac/'"$REMOTE_BASE"'/tractor/tractor.db\"] + sorted(glob.glob(\"/home/bigmac/'"$REMOTE_BASE"'/backups/trader_2026-*.db\"))[-7:]:
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
    msg="Off-host backup OK: 10 DBs replicated to Ollie in ${elapsed}s, all integrity_check=ok"
    ntfy_post default "$msg"
    echo "=== SUCCESS: $msg ==="
    exit 0
else
    msg="Off-host backup FAILED: rsync_errors=$errors integrity=[$(echo "$integrity" | grep -E 'BAD|FAIL_COUNT' | tr '\n' ' ')] note=$note"
    ntfy_post high "$msg"
    echo "=== FAILURE: $msg ==="
    exit 1
fi
