#!/bin/zsh
set -u

ROOT_DIR="/Users/bigmac/autonomous-trader"
LAUNCHER="$ROOT_DIR/launch-trademinds.sh"
LOG_DIR="$ROOT_DIR/logs"
WATCHDOG_LOG="$LOG_DIR/watchdog.log"

mkdir -p "$LOG_DIR"
cd "$ROOT_DIR" || exit 1

echo "[$(date '+%Y-%m-%d %H:%M:%S')] TradeMinds watchdog starting" >> "$WATCHDOG_LOG"

while true; do
  if ! lsof -nP -iTCP:8080 -sTCP:LISTEN >/dev/null 2>&1 || \
     ! lsof -nP -iTCP:8000 -sTCP:LISTEN >/dev/null 2>&1; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Listener missing, launching servers" >> "$WATCHDOG_LOG"
    "$LAUNCHER" --servers >> "$WATCHDOG_LOG" 2>&1
  fi
  sleep 15
done
