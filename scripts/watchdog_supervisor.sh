#!/bin/bash
# Phase-1 watchdog supervisor — replaces launchd KeepAlive.
# The com.trademinds.watchdog gui/501 LaunchAgent does NOT survive an SSH-only
# reboot (reboot-survival-gap, ALL-OUT-AUDIT-2026-05-30). Cron fires this every
# */5; if watchdog.py is absent, (re)launch it detached. Cron — always alive —
# is the supervisor-of-the-supervisor, so this survives watchdog's own crash.
# Idempotent: pgrep-guarded, so the every-5-min cron is a no-op while up.
PY=/Users/bigmac/autonomous-trader/venv/bin/python3
WD=/Users/bigmac/autonomous-trader/watchdog.py
LOG=/Users/bigmac/autonomous-trader/logs/watchdog_cron.log
# Guard on the SCRIPT PATH, not "python3 ..." — venv/bin/python3 is a symlink to
# CLT python3.9, so the live process shows argv "python3.9 /…/watchdog.py"; a
# "python3 " pattern would never match → supervisor would relaunch every */5 and
# pile up duplicates. The script path is the reliable, self-match-free discriminator
# (the supervisor itself is watchdog_supervisor.sh, which this pattern won't match).
if ! /usr/bin/pgrep -f "autonomous-trader/watchdog\.py" >/dev/null 2>&1; then
  echo "$(date '+%F %T') supervisor: watchdog absent — launching" >> "$LOG"
  /usr/bin/nohup "$PY" "$WD" >> "$LOG" 2>&1 &
fi
