#!/bin/zsh
# trader_restart.sh — SAFE manual restart with ORPHAN PREVENTION.
# HM-RESTART-ORPHAN-PREVENTION (2026-05-30).
#
# WHY: the naive "kill $(lsof -tiTCP:8080 -sTCP:LISTEN) + relaunch" only kills the
# LISTENER-holder. A process that freed the listener but kept running its scan loop
# survives as an ORPHAN — it double-scans and pollutes the shared trader.log with stale
# phase lines. On 2026-05-29 a 2.6h orphan corrupted every §C-close verification and
# triggered a multi-restart phantom chase. "Port freed" != "process dead".
#
# HOW: a trader instance is DEFINITIVELY any process holding trader.log open for WRITE
# (its stdout fd, mode 'w'). That set catches orphans and CANNOT catch other projects'
# main.py — and the write-mode filter means we never kill a human's `tail -f`/grep reader.
# After relaunch we GATE on single-writer: exactly one writer, or fail loudly.

set -u
ROOT_DIR="/Users/bigmac/autonomous-trader"
PYTHON="$ROOT_DIR/.venv/bin/python3"
ENTRYPOINT="$ROOT_DIR/main.py"
LOG="$ROOT_DIR/logs/trader.log"
ERR="$ROOT_DIR/logs/trader_error.log"
ts() { date '+%Y-%m-%d %H:%M:%S'; }

# PIDs holding trader.log open for WRITE ('w' in the lsof FD/mode column). Readers (tail
# -f / grep, FD mode 'r') are excluded so we never kill a human's monitor.
writers() { /usr/sbin/lsof "$LOG" 2>/dev/null | awk 'NR>1 && $4 ~ /w/ {print $2}' | sort -u; }

# 1. Kill ALL trader instances (orphans included).
PIDS="$(writers)"
if [[ -n "$PIDS" ]]; then
  echo "[$(ts)] killing trader instance(s): $(echo $PIDS | tr '\n' ' ')"
  echo "$PIDS" | xargs kill 2>/dev/null
  for i in {1..15}; do [[ -z "$(writers)" ]] && break; sleep 1; done
  REMAIN="$(writers)"
  if [[ -n "$REMAIN" ]]; then
    echo "[$(ts)] SIGTERM didn't clear; SIGKILL: $(echo $REMAIN | tr '\n' ' ')"
    echo "$REMAIN" | xargs kill -9 2>/dev/null
    sleep 2
  fi
fi
if [[ -n "$(writers)" ]]; then
  echo "[$(ts)] FATAL: trader.log still has writers after kill — aborting (manual cleanup)" >&2
  exit 1
fi
echo "[$(ts)] all trader instances dead (zero trader.log writers)"

# 2. Relaunch, detached.
[[ -x "$PYTHON" ]] || { echo "[$(ts)] FATAL: $PYTHON not executable" >&2; exit 1; }
cd "$ROOT_DIR" || { echo "[$(ts)] FATAL: cannot cd $ROOT_DIR" >&2; exit 1; }
echo "[$(ts)] starting trader: $PYTHON $ENTRYPOINT"
nohup "$PYTHON" "$ENTRYPOINT" >> "$LOG" 2>> "$ERR" &!

# 3. Wait for bind, then the SINGLE-WRITER gate.
for i in {1..45}; do
  [[ -n "$(/usr/sbin/lsof -tiTCP:8080 -sTCP:LISTEN 2>/dev/null | head -1)" ]] && break
  sleep 2
done
sleep 2
LISTENER="$(/usr/sbin/lsof -tiTCP:8080 -sTCP:LISTEN 2>/dev/null | head -1)"
WC="$(writers | grep -c .)"
echo "[$(ts)] listener PID=${LISTENER:-NONE} | trader.log writers=$WC"
if [[ "$WC" != "1" ]]; then
  echo "[$(ts)] FATAL: single-writer gate FAILED — $WC writers (orphan survived?). Writers: $(writers | tr '\n' ' ')" >&2
  exit 2
fi
if [[ -z "$LISTENER" ]]; then
  echo "[$(ts)] FATAL: no listener on :8080 after restart" >&2
  exit 3
fi
echo "[$(ts)] RESTART OK — single trader pid=$LISTENER bound :8080 (orphan-free)"
exit 0
