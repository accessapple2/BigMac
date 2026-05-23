#!/bin/zsh
# trader_reboot_start.sh — invoked from @reboot cron entry to start the
# TradeMinds trader after host reboot without requiring a GUI login.
#
# Rationale: macOS LaunchAgents (gui/ domain) only bootstrap when an Aqua
# session attaches, which never happens on an SSH-only restart. A
# LaunchDaemon would be the apple-canonical fix but requires sudo and
# Full Disk Access; cron @reboot is the no-privilege fallback.
#
# Tradeoff vs LaunchDaemon: no KeepAlive. If main.py crashes, it stays
# dead until the next reboot. Acceptable because the trader has been
# stable for weeks at a time; a crash is a paging event anyway.

set -u

ROOT_DIR="/Users/bigmac/autonomous-trader"
PYTHON="$ROOT_DIR/.venv/bin/python3"
ENTRYPOINT="$ROOT_DIR/main.py"
LOG_DIR="$ROOT_DIR/logs"
STDOUT="$LOG_DIR/trader.log"
STDERR="$LOG_DIR/trader_error.log"
REBOOT_LOG="$LOG_DIR/reboot_start.log"

mkdir -p "$LOG_DIR"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] @reboot fired — waiting 30s for network/services" >> "$REBOOT_LOG"
sleep 30

# Guard: if a trader is already running (e.g. someone started it manually
# before the 30s sleep elapsed), don't double-fire.
if pgrep -fl "$ENTRYPOINT" >/dev/null 2>&1; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] trader already running, skipping start" >> "$REBOOT_LOG"
  exit 0
fi

if [[ ! -x "$PYTHON" ]]; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] FATAL: $PYTHON not executable" >> "$REBOOT_LOG"
  exit 1
fi

cd "$ROOT_DIR" || {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] FATAL: cannot cd to $ROOT_DIR" >> "$REBOOT_LOG"
  exit 1
}

echo "[$(date '+%Y-%m-%d %H:%M:%S')] starting trader: $PYTHON $ENTRYPOINT" >> "$REBOOT_LOG"

# nohup + setsid-equivalent (zsh's `&!`) detaches from cron's controlling
# session so the process survives after this wrapper exits.
nohup "$PYTHON" "$ENTRYPOINT" >> "$STDOUT" 2>> "$STDERR" &!

sleep 3
if pgrep -fl "$ENTRYPOINT" >/dev/null 2>&1; then
  PID=$(pgrep -f "$ENTRYPOINT" | head -1)
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] trader started, pid=$PID" >> "$REBOOT_LOG"
  exit 0
else
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] FATAL: trader failed to start (see $STDERR)" >> "$REBOOT_LOG"
  exit 1
fi
