#!/bin/zsh
# signal_center_reboot_start.sh — @reboot wrapper for the Flask signal
# center on port 9000. Sibling of scripts/trader_reboot_start.sh; same
# rationale (gui/$UID LaunchAgent domain doesn't bootstrap from SSH on
# this macOS box; RunAtLoad doesn't fire at boot without a logged-in
# Aqua session — see CLAUDE.md "LaunchAgent Reboot Lifecycle" 2026-05-23).
#
# Tradeoff vs LaunchDaemon: no KeepAlive respawn. Acceptable because
# the signal-center has run for weeks without crash; a crash is a paging
# event anyway.
#
# Note: this wrapper points at the signal-center's separate Python 3.9
# venv at /Users/bigmac/autonomous-trader/venv (NOT the trader's .venv
# Python 3.14). The split is intentional — server.py depends on older
# Flask/urllib3/etc. that haven't fully validated against Py3.14 yet.

set -u

ROOT_DIR="/Users/bigmac/autonomous-trader"
SC_DIR="$ROOT_DIR/signal-center"
PYTHON="$ROOT_DIR/venv/bin/python3"
ENTRYPOINT="$SC_DIR/server.py"
LOG_DIR="$ROOT_DIR/logs"
LOG="$LOG_DIR/signal-center.log"
REBOOT_LOG="$LOG_DIR/signal_center_reboot_start.log"

mkdir -p "$LOG_DIR"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] @reboot fired — waiting 35s (trader may need to bind 8080 first)" >> "$REBOOT_LOG"
sleep 35

# Guard: don't double-fire if an operator beat the wrapper to it.
if pgrep -fl "$ENTRYPOINT" >/dev/null 2>&1; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] signal-center already running, skipping start" >> "$REBOOT_LOG"
  exit 0
fi

if [[ ! -x "$PYTHON" ]]; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] FATAL: $PYTHON not executable" >> "$REBOOT_LOG"
  exit 1
fi

cd "$SC_DIR" || {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] FATAL: cannot cd to $SC_DIR" >> "$REBOOT_LOG"
  exit 1
}

# Guard: port 9000 free?
if /usr/sbin/lsof -ti :9000 >/dev/null 2>&1; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] FATAL: port 9000 already bound — refusing to start" >> "$REBOOT_LOG"
  exit 1
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] starting signal-center: $PYTHON $ENTRYPOINT" >> "$REBOOT_LOG"

nohup "$PYTHON" "$ENTRYPOINT" >> "$LOG" 2>> "$LOG" &!

sleep 4
if pgrep -fl "$ENTRYPOINT" >/dev/null 2>&1; then
  PID=$(pgrep -f "$ENTRYPOINT" | head -1)
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] signal-center started, pid=$PID" >> "$REBOOT_LOG"
  exit 0
else
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] FATAL: signal-center failed to start (see $LOG)" >> "$REBOOT_LOG"
  exit 1
fi
