#!/bin/bash
# Healthcheck restart for status_page (status.ollietrades.com) on :8090.
# Unlike swingdesk_restart.sh / signal_center_restart.sh, this does NOT
# relaunch the process itself — status_page runs under the
# com.trademinds.statuspage LaunchDaemon (KeepAlive=true), and the process
# executes as UserName=bigmac, so a plain same-UID kill is enough: launchd
# (root-owned supervisor) notices the exit and respawns it within its
# ThrottleInterval. This exists to catch the wedged-but-alive failure mode
# (process running + port LISTENing but not answering requests) that
# KeepAlive alone can never detect, since KeepAlive only fires on process
# exit.
pkill -f "scripts/status_page.py" 2>/dev/null
PORT_PIDS=$(lsof -ti :8090 2>/dev/null); [ -n "$PORT_PIDS" ] && kill $PORT_PIDS 2>/dev/null
echo "$(date): status_page killed for healthcheck failure, awaiting LaunchDaemon KeepAlive respawn"
