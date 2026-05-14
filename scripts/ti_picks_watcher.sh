#!/bin/bash
# HM-CA: TI .eml watcher. Polls inbox/trade_ideas/ for new .eml files,
# invokes the parser on each, moves to processed/ on success or failed/
# on error. Mirrors HM-AT-β Schwab pattern (incl. sleep 11 anti-throttle).
#
# Usage:
#   bash scripts/ti_picks_watcher.sh          # service mode (sleep 11 at end)
#   bash scripts/ti_picks_watcher.sh --once   # one-shot, no trailing sleep
set -uo pipefail

WATCH_DIR="$HOME/autonomous-trader/inbox/trade_ideas"
PROCESSED_DIR="$WATCH_DIR/processed"
FAILED_DIR="$WATCH_DIR/failed"
LOG="$HOME/autonomous-trader/logs/ti_picks_watcher.log"
VENV="$HOME/autonomous-trader/venv/bin/python3"

mkdir -p "$PROCESSED_DIR" "$FAILED_DIR"
mkdir -p "$(dirname "$LOG")"

cd "$HOME/autonomous-trader"

shopt -s nullglob
PROCESSED=0
FAILED=0
for EML in "$WATCH_DIR"/*.eml; do
    [ -f "$EML" ] || continue
    BASENAME=$(basename "$EML")
    TS=$(date +%Y%m%d_%H%M%S)
    echo "=== $(date -Iseconds) processing: $BASENAME ===" >> "$LOG"
    if "$VENV" scripts/ti_picks_parser.py "$EML" >> "$LOG" 2>&1; then
        mv "$EML" "$PROCESSED_DIR/${TS}_${BASENAME}"
        echo "=== $(date -Iseconds) SUCCESS: $BASENAME -> processed/ ===" >> "$LOG"
        PROCESSED=$((PROCESSED+1))
    else
        RC=$?
        mv "$EML" "$FAILED_DIR/${TS}_${BASENAME}"
        echo "=== $(date -Iseconds) FAILED rc=$RC: $BASENAME -> failed/ ===" >> "$LOG"
        FAILED=$((FAILED+1))
    fi
done
echo "=== $(date -Iseconds) batch done: processed=$PROCESSED failed=$FAILED ===" >> "$LOG"

# HM-AT-β anti-throttle: launchd kills jobs that exit <~10s repeatedly.
if [ "${1:-}" != "--once" ]; then
    sleep 11
fi
