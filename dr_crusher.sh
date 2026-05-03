#!/usr/bin/env bash
# ============================================================
# Dr. Crusher — BACKUP ALERTING ONLY (as of 2026-04-26)
# ============================================================
# Watchdog.py is the PRIMARY healthcheck + auto-restarter.
# This script runs every 6 min as a passive failsafe — it
# alerts (but does NOT restart) so we get notified if both
# the bridge AND watchdog are down. Restart races eliminated.
# All alerts prefixed with [BACKUP] for source identification.
# ============================================================
LOG="$HOME/autonomous-trader/logs/crusher.log"
mkdir -p "$(dirname "$LOG")"
echo "$(date): Crusher healthcheck starting" >> "$LOG"

cd "$HOME/autonomous-trader"

check_port() {
    curl -s --max-time 5 -o /dev/null -w "%{http_code}" "http://127.0.0.1:$1" 2>/dev/null
}

# Retry check_port 3x with 2s gap before declaring down.
# Accepts any HTTP 2xx/3xx (< 400) as OK — dashboards return 303, Signal Center 302.
# This matches watchdog.py's http_ok() behaviour (r.status < 400).
check_port_retry() {
    local port="$1"
    local attempt
    for attempt in 1 2 3; do
        local code; code=$(check_port "$port")
        if [ "$code" -lt 400 ] 2>/dev/null; then echo "ok"; return; fi
        [ "$attempt" -lt 3 ] && sleep 2
    done
    echo "$code"  # return last non-ok code
}

# Check Ollama first
if ! pgrep -q ollama; then
    echo "$(date): Ollama NOT running — starting" >> "$LOG"
    open -a Ollama
    sleep 10
fi

# Check port 8080 (USS TradeMinds — all trading + dashboard)
# BACKUP ONLY: alert fired here means watchdog.py also failed to restart the bridge.
STATUS_8080=$(check_port_retry 8080)
if [ "$STATUS_8080" != "ok" ]; then
    echo "$(date): Port 8080 DOWN (got $STATUS_8080) — alerting (no restart — watchdog.py is primary)" >> "$LOG"
    curl -s -o /dev/null \
        -H "Title: [BACKUP] Bridge DOWN" \
        -H "Priority: urgent" \
        -H "Tags: warning" \
        -d "Port 8080 down (got $STATUS_8080) — Crusher backup alert at $(date '+%H:%M'). Watchdog.py should restart." \
        https://ntfy.sh/ollietrades-admin 2>/dev/null || true
else
    echo "$(date): Port 8080 OK" >> "$LOG"
fi

# Check port 9000 (Signal Center)
# BACKUP ONLY: alert fired here means watchdog.py also failed to restart Signal Center.
STATUS_9000=$(check_port_retry 9000)
if [ "$STATUS_9000" != "ok" ]; then
    echo "$(date): Port 9000 DOWN (got $STATUS_9000) — alerting (no restart — watchdog.py is primary)" >> "$LOG"
    curl -s -o /dev/null \
        -H "Title: [BACKUP] Signal Center DOWN" \
        -H "Priority: high" \
        -H "Tags: warning" \
        -d "Port 9000 down (got $STATUS_9000) — Crusher backup alert at $(date '+%H:%M'). Watchdog.py should restart." \
        https://ntfy.sh/ollietrades-admin 2>/dev/null || true
else
    echo "$(date): Port 9000 OK" >> "$LOG"
fi

echo "$(date): Crusher check complete" >> "$LOG"
