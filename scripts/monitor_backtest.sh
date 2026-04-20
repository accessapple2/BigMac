#!/bin/bash
# monitor_backtest.sh — live progress dashboard for OllieTrades backtests
# Usage: ./scripts/monitor_backtest.sh [PID] [LOG_PATH]
#
# Defaults: reads PID from /tmp/backtest_180d.pid, log from path stored there.
# Ctrl+C stops watching WITHOUT killing the backtest.

PID=${1:-$(grep -oE "PID=[0-9]+" /tmp/backtest_180d.pid 2>/dev/null | cut -d= -f2)}
LOG=${2:-$(grep "^Log:" /tmp/backtest_180d.pid 2>/dev/null | cut -d' ' -f2)}

# Expand ~ manually since it may not expand inside variables
LOG="${LOG/#\~/$HOME}"

# Fallback log path
if [ -z "$LOG" ]; then
  LOG="$HOME/autonomous-trader/logs/ollie_backtest_v6_180d_2026-04-19.log"
fi

if [ -z "$PID" ]; then
  echo "ERROR: No PID found."
  echo "Usage: $0 [PID] [LOG_PATH]"
  echo "Or ensure /tmp/backtest_180d.pid contains PID=<number>"
  exit 1
fi

if [ ! -f "$LOG" ]; then
  echo "ERROR: Log file not found: $LOG"
  echo "Usage: $0 [PID] [LOG_PATH]"
  exit 1
fi

trap 'printf "\n\n  Monitor stopped. Backtest still running if PID %s alive.\n\n" "$PID"; exit 0' INT

while true; do
  clear
  echo "════════════════════════════════════════════════════════════"
  printf "  OllieTrades Backtest Monitor\n"
  printf "  PID %-8s  %s\n" "$PID" "$(date '+%Y-%m-%d %H:%M:%S')"
  echo "════════════════════════════════════════════════════════════"
  echo ""

  if ps -p "$PID" > /dev/null 2>&1; then
    ELAPSED=$(ps -p "$PID" -o etime= 2>/dev/null | tr -d ' ')
    CPU=$(ps -p "$PID" -o %cpu= 2>/dev/null | tr -d ' ')
    MEM=$(ps -p "$PID" -o %mem= 2>/dev/null | tr -d ' ')
    printf "  ✓ RUNNING\n"
    printf "    Elapsed:  %s\n" "$ELAPSED"
    printf "    CPU:      %s%%\n" "$CPU"
    printf "    MEM:      %s%%\n" "$MEM"
  else
    printf "  ✗ PROCESS ENDED\n"
    echo ""
    echo "══ Final 40 log lines ══════════════════════════════════════"
    tail -40 "$LOG" | sed 's/^/  /'
    echo ""
    echo "  Monitor exiting. Check backtest_runs in trader.db for results."
    echo ""
    exit 0
  fi

  echo ""
  echo "── PROGRESS ─────────────────────────────────────────────────"

  TOTAL_LINES=$(wc -l < "$LOG" 2>/dev/null | tr -d ' ')
  OLLIE_CALLS=$(grep -c "\[OLLAMA\]" "$LOG" 2>/dev/null || echo 0)
  ERRORS=$(grep -cE "\[ollama-err\]|Traceback|^ERROR" "$LOG" 2>/dev/null || echo 0)

  # Most recent date being simulated
  LAST_DATE=$(grep -oE "\[20[0-9]{2}-[0-9]{2}-[0-9]{2}\]" "$LOG" 2>/dev/null | tail -1 | tr -d '[]')
  if [ -z "$LAST_DATE" ]; then
    LAST_DATE=$(grep -oE "20[0-9]{2}-[0-9]{2}-[0-9]{2}" "$LOG" 2>/dev/null | tail -1)
  fi

  # Current agent and version from last day-header line
  LAST_HEADER=$(grep -E "^\s+\[20[0-9]{2}-" "$LOG" 2>/dev/null | tail -1)
  CURRENT_AGENT=$(echo "$LAST_HEADER" | grep -oE "\[([a-z0-9_-]+)/" | tr -d '[/' | head -1)
  CURRENT_VER=$(echo "$LAST_HEADER" | grep -oE "/(BASELINE|V2_ALPHA|V3_CONC)\]" | tr -d '/]' | head -1)

  # Completed agent summary lines
  COMPLETED=$(grep -c "^  → return=" "$LOG" 2>/dev/null || echo 0)

  printf "  Log lines:         %s\n"   "$TOTAL_LINES"
  printf "  Ollama calls OK:   %s\n"   "$OLLIE_CALLS"
  printf "  Errors:            %s\n"   "$ERRORS"
  printf "  Sim date:          %s\n"   "${LAST_DATE:-unknown}"
  printf "  Agent/version:     %s / %s\n" "${CURRENT_AGENT:-unknown}" "${CURRENT_VER:-unknown}"
  printf "  Agent runs done:   %s / 15\n" "$COMPLETED"

  # ETA estimate: extrapolate from elapsed time and agent runs completed
  if [ "$COMPLETED" -gt 0 ] 2>/dev/null; then
    START_EPOCH=$(stat -f "%m" /tmp/backtest_180d.pid 2>/dev/null || echo 0)
    NOW_EPOCH=$(date +%s)
    ELAPSED_SECS=$(( NOW_EPOCH - START_EPOCH ))
    SECS_PER_RUN=$(( ELAPSED_SECS / COMPLETED ))
    REMAINING_RUNS=$(( 15 - COMPLETED ))
    ETA_SECS=$(( SECS_PER_RUN * REMAINING_RUNS ))
    ETA_MIN=$(( ETA_SECS / 60 ))
    ETA_HR=$(( ETA_MIN / 60 ))
    ETA_MIN_REM=$(( ETA_MIN % 60 ))
    printf "  ETA (estimate):    ~%dh %02dm remaining\n" "$ETA_HR" "$ETA_MIN_REM"
  fi

  echo ""
  echo "── OLLIE VRAM ───────────────────────────────────────────────"
  VRAM=$(ssh -o ConnectTimeout=3 -o BatchMode=yes -o StrictHostKeyChecking=no \
    bigmac@192.168.1.166 "ollama ps 2>/dev/null" 2>/dev/null)
  if [ -n "$VRAM" ]; then
    echo "$VRAM" | head -5 | sed 's/^/  /'
  else
    echo "  (idle — no model loaded, or SSH timeout)"
  fi

  echo ""
  echo "── LAST 8 LOG LINES ─────────────────────────────────────────"
  tail -8 "$LOG" | sed 's/^/  /'

  echo ""
  echo "─────────────────────────────────────────────────────────────"
  printf "  Refreshing in 30s... Ctrl+C to stop watching (backtest keeps running)\n"
  sleep 30
done
