#!/bin/bash
# rotate_logs.sh — USS TradeMinds log rotation (B24)
# Rotates any *.log in ~/autonomous-trader/logs/ over THRESHOLD_MB.
# Keeps last KEEP rotations (.1, .2, .3). Truncates in place to preserve
# any held file handles (launchd-managed services).
#
# Safe to run at any cadence. Idempotent. No DB touches.
#
# Manual run:   bash ~/autonomous-trader/scripts/rotate_logs.sh
# Cron candidate: daily at 00:30 MST (see docs/proposals/log_rotation_plist.md)

set -euo pipefail

LOG_DIR="${LOG_DIR:-$HOME/autonomous-trader/logs}"
THRESHOLD_MB="${THRESHOLD_MB:-50}"
KEEP="${KEEP:-3}"

if [ ! -d "$LOG_DIR" ]; then
  echo "rotate_logs: LOG_DIR not found: $LOG_DIR" >&2
  exit 1
fi

rotated=0
inspected=0

# find logs over threshold (BSD find: -size +NM)
while IFS= read -r -d '' f; do
  inspected=$((inspected + 1))
  base="${f%.log}"
  # shift .2 -> .3, .1 -> .2 (newest at .1)
  for i in $(seq $((KEEP - 1)) -1 1); do
    src="${base}.log.${i}"
    dst="${base}.log.$((i + 1))"
    [ -f "$src" ] && mv "$src" "$dst"
  done
  cp "$f" "${base}.log.1"
  : > "$f"   # truncate in place, preserve handle
  rotated=$((rotated + 1))
  echo "rotated: $f -> ${base}.log.1 (truncated in place)"
done < <(find "$LOG_DIR" -maxdepth 2 -type f -name "*.log" -size "+${THRESHOLD_MB}M" -print0)

echo "rotate_logs: inspected=${inspected} rotated=${rotated} threshold=${THRESHOLD_MB}MB keep=${KEEP}"
