#!/bin/zsh
# finmem_promote_weekly.sh — Sunday promotion job for HM-FINMEM
# agent_memory. Consolidates frequently-referenced SHORT→MID and
# MID→LONG entries based on cumulative score over the lookback window.
# Cron: Sunday 00:30 AZ via crontab `30 0 * * 0` — offset 30 min after
# daily decay to avoid race.

set -u

ROOT_DIR="/Users/bigmac/autonomous-trader"
PYTHON="$ROOT_DIR/.venv/bin/python3"
LOG_DIR="$ROOT_DIR/logs"
LOG="$LOG_DIR/finmem_promote.log"

mkdir -p "$LOG_DIR"

if [[ ! -x "$PYTHON" ]]; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] FATAL: $PYTHON not executable" >> "$LOG"
  exit 1
fi

cd "$ROOT_DIR" || {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] FATAL: cannot cd to $ROOT_DIR" >> "$LOG"
  exit 1
}

"$PYTHON" -c "
import sys
sys.path.insert(0, '$ROOT_DIR')
from engine.finmem_writers import promote_weekly
import json
result = promote_weekly()
print(json.dumps(result))
" >> "$LOG" 2>&1
echo "[$(date '+%Y-%m-%d %H:%M:%S')] promote_weekly done" >> "$LOG"
