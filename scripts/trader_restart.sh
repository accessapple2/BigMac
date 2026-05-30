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

# ── MUTEX — HM-TRADER-RESTART-FLOCK (2026-05-30) ────────────────────────────
# WHY: the single-writer gate below is a DETECTOR, not a MUTEX. Two concurrent
# invocations both kill writers, both relaunch, both spawn a trader, then both
# fail the gate — a transient double-spawn whose resolution depends on main.py's
# UNVERIFIED :8080 bind-conflict behavior. Serializing restarts makes "admit one"
# ENFORCED. Required before a 2nd actor (watchdog repoint) routes through here.
# HOW: flock is NOT on this macOS box (no native BSD flock, none via brew), so we
# use a portable mkdir-atomic lock (mkdir is atomic: exactly one concurrent caller
# creates the dir). Staleness: reclaim only if the recorded PID is dead, OR the
# lock is >5min old with no live PID — never destroy a lock created microseconds
# ago (pid-not-yet-written race). Second live caller ABORTS LOUD (exit 4).
LOCKDIR="/tmp/uss_trader_restart.lock"
_acquire_lock() {
  if mkdir "$LOCKDIR" 2>/dev/null; then echo $$ > "$LOCKDIR/pid"; return 0; fi
  local holder; holder="$(cat "$LOCKDIR/pid" 2>/dev/null || true)"
  if [[ -n "$holder" ]] && kill -0 "$holder" 2>/dev/null; then
    return 1   # live holder → caller aborts
  fi
  # no live PID: reclaim ONLY if genuinely old (>5min), else treat as a just-born
  # lock mid-creation and abort — never steal a lock whose pid isn't written yet.
  if [[ -n "$(find "$LOCKDIR" -maxdepth 0 -mmin +5 2>/dev/null)" ]]; then
    rm -rf "$LOCKDIR"
    if mkdir "$LOCKDIR" 2>/dev/null; then echo $$ > "$LOCKDIR/pid"; return 0; fi
  fi
  return 1
}
if ! _acquire_lock; then
  echo "[$(ts)] ABORT: another trader_restart already in progress (pid=$(cat "$LOCKDIR/pid" 2>/dev/null)) — refusing to double-spawn" >&2
  exit 4
fi
trap 'rm -rf "$LOCKDIR"' EXIT INT TERM
echo "[$(ts)] restart lock acquired (pid $$)"

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
