#!/bin/zsh
# TOUR-API launcher — respawn loop gives crash auto-restart without launchd
# (this box's `launchctl bootstrap gui/$UID` fails over SSH; cron @reboot +
# nohup is the established reboot-survival pattern — see CLAUDE.md).
#
# Single-instance: callers kill prior tour_api procs BEFORE invoking this
# (see the @reboot cron / restart command). This wrapper must NOT self-pkill.
cd "$HOME/autonomous-trader" || exit 1
mkdir -p logs

while true; do
  echo "[tour-api] starting uvicorn :8088 $(date '+%Y-%m-%d %H:%M:%S')" >> logs/tour_api.log
  .venv/bin/python3 tour_api.py >> logs/tour_api.log 2>&1
  echo "[tour-api] exited rc=$? $(date '+%Y-%m-%d %H:%M:%S'), respawning in 2s" >> logs/tour_api.log
  sleep 2
done
