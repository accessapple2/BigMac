#!/bin/bash
# PROPOSED — HM-DEPARTURE-HARDENING Phase 1 item 2 (tour_api healthcheck gap).
# Not yet wired into origin_healthcheck.sh or cron; standalone until approved.
#
# Healthcheck restart for tour_api (:8088). Unlike swingdesk/signal-center,
# tour_api has its own respawn loop (scripts/tour_api_start.sh, a `while true`
# wrapper around tour_api.py, launched via cron @reboot — not launchd). A
# plain `pkill -f tour_api.py` would just get instantly respawned by that
# loop with the same wedge, so both the wrapper AND the python process must
# be killed before relaunching, same discipline as the @reboot cron line's
# own kill step.
cd /Users/bigmac/autonomous-trader || exit 1
pkill -f "scripts/tour_api_start.sh" 2>/dev/null
pkill -f "tour_api.py" 2>/dev/null
PORT_PIDS=$(lsof -ti :8088 2>/dev/null); [ -n "$PORT_PIDS" ] && kill $PORT_PIDS 2>/dev/null
sleep 2
nohup zsh scripts/tour_api_start.sh >> logs/tour_api.log 2>&1 &
echo "$(date): tour_api restarted (wrapper PID $!), awaiting :8088 to come up"
