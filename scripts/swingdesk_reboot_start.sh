#!/bin/bash
# Starts the O-Tasty (SwingDesk) backend on :8889 after reboot.
# Isolated paper account PA3YVDTUH5CB. Reads swingdesk/.env only.
cd /Users/bigmac/autonomous-trader
# kill any stale instance — match by script name AND by whatever holds :8889.
# The prior manual instance ran as `uvicorn backend:app`, which a name-only
# pkill misses (leaves a duplicate); the port-based kill is invocation-agnostic.
pkill -f "swingdesk/backend.py" 2>/dev/null
PORT_PIDS=$(lsof -ti :8889 2>/dev/null); [ -n "$PORT_PIDS" ] && kill $PORT_PIDS 2>/dev/null
sleep 2
nohup ./.venv/bin/python3 swingdesk/backend.py >> logs/otasty.log 2>> logs/otasty_error.log &
echo "$(date): O-Tasty backend started PID $!" >> logs/otasty_reboot.log
