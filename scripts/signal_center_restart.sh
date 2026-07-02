#!/bin/bash
# Manual/healthcheck restart for signal-center on :9000.
# Identical kill+relaunch shape as swingdesk_restart.sh.
cd /Users/bigmac/autonomous-trader || exit 1
pkill -f "signal-center/server.py" 2>/dev/null
PORT_PIDS=$(lsof -ti :9000 2>/dev/null); [ -n "$PORT_PIDS" ] && kill $PORT_PIDS 2>/dev/null
sleep 2
nohup ./venv/bin/python3 signal-center/server.py >> logs/signal-center.log 2>> logs/signal-center.log &
echo "$(date): signal-center restarted PID $!" | tee -a logs/signal_center_reboot.log
