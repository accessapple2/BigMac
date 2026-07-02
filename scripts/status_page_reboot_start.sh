#!/bin/bash
# @reboot wrapper for status.ollietrades.com (:8090). Same pattern as
# signal_center_reboot_start.sh / cloudflared_reboot_start.sh -- launchd
# bootstrap doesn't fire reliably over SSH-only sessions on this box.
set -u
ROOT_DIR="/Users/bigmac/autonomous-trader"
PYTHON="$ROOT_DIR/.venv/bin/python3"
ENTRYPOINT="$ROOT_DIR/scripts/status_page.py"
LOG="$ROOT_DIR/logs/status_page.log"
REBOOT_LOG="$ROOT_DIR/logs/status_page_reboot_start.log"

mkdir -p "$ROOT_DIR/logs"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] @reboot fired — waiting 35s" >> "$REBOOT_LOG"
sleep 35

if pgrep -fl "$ENTRYPOINT" >/dev/null 2>&1; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] status_page already running, skipping" >> "$REBOOT_LOG"
  exit 0
fi
if /usr/sbin/lsof -ti :8090 >/dev/null 2>&1; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] FATAL: port 8090 already bound — refusing to start" >> "$REBOOT_LOG"
  exit 1
fi

nohup "$PYTHON" "$ENTRYPOINT" >> "$LOG" 2>> "$LOG" &!
sleep 3
if pgrep -fl "$ENTRYPOINT" >/dev/null 2>&1; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] status_page started, pid=$(pgrep -f "$ENTRYPOINT" | head -1)" >> "$REBOOT_LOG"
else
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] FATAL: status_page failed to start" >> "$REBOOT_LOG"
fi
