#!/bin/bash
# Manual/healthcheck restart for signal-center on :9000.
#
# HM-SIGNAL-CENTER-MUTEX 2026-08-31. WHY: this script had no serialization,
# and origin_healthcheck.sh fires it from cron every 5 min. On 2026-08-31 at
# 06:46:26/06:46:31/06:46:31 three healthchecks failed within five seconds
# and all three spawned: PIDs 75347, 75440 and 75445 all bound (or fought
# over) :9000. Two further hazards showed up in the same window:
#   - concurrent `pkill -f "signal-center/server.py"` processes kill EACH
#     OTHER, because a pkill's own argv contains the pattern it searches for
#     (logged: 'line 5: 75402 Terminated: 15'). pgrep with the [s] bracket
#     trick + explicit kill has no such self-match.
#   - a mutex alone is not enough: the 2nd caller's curl already failed
#     BEFORE the 1st restart finished, so it would still spawn a redundant
#     server. Re-check health inside the lock and skip if it recovered.
# Lock mechanism copied from scripts/trader_restart.sh (mkdir is atomic;
# flock is unavailable on this macOS box). Second live caller exits 4.
cd /Users/bigmac/autonomous-trader || exit 1

ts() { date '+%Y-%m-%d %H:%M:%S'; }
LOG=logs/signal_center_reboot.log
HEALTH_URL="http://localhost:9000/api/health"

LOCKDIR="/tmp/uss_signal_center_restart.lock"
_acquire_lock() {
  if mkdir "$LOCKDIR" 2>/dev/null; then echo $$ > "$LOCKDIR/pid"; return 0; fi
  local holder; holder="$(cat "$LOCKDIR/pid" 2>/dev/null || true)"
  if [[ -n "$holder" ]] && kill -0 "$holder" 2>/dev/null; then
    return 1
  fi
  if [[ -n "$(find "$LOCKDIR" -maxdepth 0 -mmin +5 2>/dev/null)" ]]; then
    rm -rf "$LOCKDIR"
    if mkdir "$LOCKDIR" 2>/dev/null; then echo $$ > "$LOCKDIR/pid"; return 0; fi
  fi
  return 1
}
if ! _acquire_lock; then
  echo "$(ts): ABORT: another signal_center_restart in progress (pid=$(cat "$LOCKDIR/pid" 2>/dev/null)) — refusing to double-spawn" | tee -a "$LOG" >&2
  exit 4
fi
trap 'rm -rf "$LOCKDIR"' EXIT INT TERM
echo "$(ts): signal-center restart lock acquired (pid $$)" >> "$LOG"

# Re-check inside the lock: a queued caller's failure may pre-date the
# restart that already fixed it.
if curl -sf --max-time 8 "$HEALTH_URL" >/dev/null 2>&1; then
  echo "$(ts): signal-center healthy on re-check — skipping restart" | tee -a "$LOG"
  exit 0
fi

PIDS="$(pgrep -f '[s]ignal-center/server.py' || true)"
PORT_PIDS="$(lsof -ti :9000 2>/dev/null || true)"
ALL="$(printf '%s\n%s\n' "$PIDS" "$PORT_PIDS" | sort -u | grep -v '^$' || true)"
if [[ -n "$ALL" ]]; then
  echo "$(ts): killing signal-center instance(s): $(echo $ALL | tr '\n' ' ')" >> "$LOG"
  echo "$ALL" | xargs kill 2>/dev/null
  for i in {1..10}; do
    [[ -z "$(pgrep -f '[s]ignal-center/server.py' || true)" ]] && break
    sleep 1
  done
  REMAIN="$(pgrep -f '[s]ignal-center/server.py' || true)"
  if [[ -n "$REMAIN" ]]; then
    echo "$(ts): SIGTERM didn't clear; SIGKILL: $(echo $REMAIN | tr '\n' ' ')" >> "$LOG"
    echo "$REMAIN" | xargs kill -9 2>/dev/null
    sleep 1
  fi
fi

nohup ./venv/bin/python3 signal-center/server.py >> logs/signal-center.log 2>> logs/signal-center.log &
NEW_PID=$!
echo "$(ts): signal-center restarted PID $NEW_PID" | tee -a "$LOG"

# Hold the lock through startup so a caller arriving seconds later sees a
# live holder rather than an empty port.
sleep 5
LIVE="$(pgrep -f '[s]ignal-center/server.py' | wc -l | tr -d ' ')"
echo "$(ts): RESTART OK — $LIVE signal-center process(es) after respawn" >> "$LOG"
