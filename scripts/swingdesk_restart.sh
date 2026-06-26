#!/bin/bash
# Manual restart for the SwingDesk (O-Tasty) backend on :8889.
# Identical kill+relaunch logic as swingdesk_reboot_start.sh.
cd /Users/bigmac/autonomous-trader
pkill -f "swingdesk/backend.py" 2>/dev/null
PORT_PIDS=$(lsof -ti :8889 2>/dev/null); [ -n "$PORT_PIDS" ] && kill $PORT_PIDS 2>/dev/null
sleep 2
nohup ./.venv/bin/python3 swingdesk/backend.py >> logs/otasty.log 2>> logs/otasty_error.log &
echo "$(date): SwingDesk restarted PID $!" | tee -a logs/otasty_reboot.log
