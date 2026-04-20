#!/bin/bash
echo "════════════════════════════════════════════════════════"
echo "  FLEET STATUS  |  $(date '+%Y-%m-%d %H:%M:%S')"
echo "════════════════════════════════════════════════════════"

echo ""
echo "── BIGMAC ──────────────────────────────────────────────"
FREE_PCT=$(memory_pressure 2>/dev/null | grep "System-wide" | grep -oE "[0-9]+%")
echo "  Memory free: ${FREE_PCT:-unknown}"

MAIN_PID=$(pgrep -f "python3.*main.py" | head -1)
if [ -n "$MAIN_PID" ]; then
  echo "  main.py:"
  ps -p $MAIN_PID -o pid=,pcpu=,pmem=,etime= 2>/dev/null | sed 's/^/    /'
else
  echo "  main.py: NOT RUNNING"
fi

echo "  Local Ollama:"
if pgrep -f "ollama serve" > /dev/null 2>&1; then
  LOADED=$(ollama ps 2>/dev/null | tail -n +2)
  if [ -n "$LOADED" ]; then
    ollama ps 2>/dev/null | sed 's/^/    /'
  else
    echo "    running (no model loaded)"
  fi
else
  echo "    not running"
fi

echo "  Top Python by MEM:"
PS_OUT=$(ps aux | grep python3 | grep -v grep | sort -rn -k 4 | head -5)
if [ -n "$PS_OUT" ]; then
  echo "$PS_OUT" | awk '{printf "    %-8s %5s%% CPU  %5s%% MEM  %s\n", $2, $3, $4, $11}'
else
  echo "    (none)"
fi

echo ""
echo "── OLLIE (192.168.1.166) ───────────────────────────────"
OLLIE_OUT=$(ssh -o ConnectTimeout=3 -o BatchMode=yes -o StrictHostKeyChecking=no \
  bigmac@192.168.1.166 '
echo "GPU:"
nvidia-smi --query-gpu=utilization.gpu,memory.used,memory.total,temperature.gpu \
  --format=csv,noheader 2>/dev/null | sed "s/^/  /" || echo "  (nvidia-smi unavailable)"
echo "Active models:"
LOADED=$(ollama ps 2>/dev/null | tail -n +2)
if [ -n "$LOADED" ]; then
  ollama ps 2>/dev/null | sed "s/^/  /"
else
  echo "  (no model loaded)"
fi
echo "RAM:"
free -h 2>/dev/null | head -3 | sed "s/^/  /" || echo "  (free unavailable)"
' 2>&1)

if [ -z "$OLLIE_OUT" ] || echo "$OLLIE_OUT" | grep -qE "refused|Unreachable|timed out|Permission denied"; then
  echo "  UNREACHABLE"
else
  echo "$OLLIE_OUT" | sed 's/^/  /'
fi

echo ""
echo "── BACKTEST ────────────────────────────────────────────"
if [ -f /tmp/backtest_180d.pid ]; then
  BT_PID=$(grep -oE "PID=[0-9]+" /tmp/backtest_180d.pid | cut -d= -f2)
  BT_LOG=$(grep "^Log:" /tmp/backtest_180d.pid 2>/dev/null | cut -d' ' -f2)
  BT_LOG="${BT_LOG/#\~/$HOME}"
  if ps -p "$BT_PID" > /dev/null 2>&1; then
    echo "  180-day backtest RUNNING (PID $BT_PID):"
    ps -p "$BT_PID" -o etime=,%cpu=,%mem= 2>/dev/null | sed 's/^/    Elapsed\/CPU\/MEM: /'
    if [ -n "$BT_LOG" ] && [ -f "$BT_LOG" ]; then
      CALLS=$(grep -c "\[OLLAMA\]" "$BT_LOG" 2>/dev/null || echo 0)
      DONE=$(grep -c "^  → return=" "$BT_LOG" 2>/dev/null || echo 0)
      ERRS=$(grep -cE "\[ollama-err\]|Traceback|^ERROR" "$BT_LOG" 2>/dev/null || echo 0)
      LAST_DATE=$(grep -oE "\[20[0-9]{2}-[0-9]{2}-[0-9]{2}\]" "$BT_LOG" 2>/dev/null | tail -1 | tr -d '[]')
      printf "    Ollama calls: %s  |  Agent runs: %s/15  |  Errors: %s  |  Sim date: %s\n" \
        "$CALLS" "$DONE" "$ERRS" "${LAST_DATE:-unknown}"
    fi
  else
    echo "  180-day backtest COMPLETE (PID $BT_PID exited)"
    if [ -n "$BT_LOG" ] && [ -f "$BT_LOG" ]; then
      echo "  Log: $BT_LOG"
      CALLS=$(grep -c "\[OLLAMA\]" "$BT_LOG" 2>/dev/null || echo 0)
      ERRS=$(grep -cE "\[ollama-err\]|Traceback|^ERROR" "$BT_LOG" 2>/dev/null || echo 0)
      printf "    Total Ollama calls: %s  |  Errors: %s\n" "$CALLS" "$ERRS"
    fi
  fi
else
  echo "  No active backtest (/tmp/backtest_180d.pid not found)"
fi
echo "════════════════════════════════════════════════════════"
