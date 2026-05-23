#!/bin/zsh
# finmem_decay_daily.sh — daily decay scorer for HM-FINMEM agent_memory.
# Multiplies score by per-layer decay_rate; prunes rows with score < 0.05.
# Cron: @daily 00:00 AZ via crontab `0 0 * * *`.

set -u

ROOT_DIR="/Users/bigmac/autonomous-trader"
PYTHON="$ROOT_DIR/.venv/bin/python3"
LOG_DIR="$ROOT_DIR/logs"
LOG="$LOG_DIR/finmem_decay.log"

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
from engine.finmem_writers import decay_daily
import json
result = decay_daily()
print(json.dumps(result))
" >> "$LOG" 2>&1
echo "[$(date '+%Y-%m-%d %H:%M:%S')] decay_daily done" >> "$LOG"
