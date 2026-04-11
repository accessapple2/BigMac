#!/usr/bin/env bash
# Dr. Crusher — TradeMinds Healthcheck & Auto-Restart
# Port 8000 decommissioned 2026-04-07. Only 8080 monitored.
LOG="$HOME/autonomous-trader/logs/crusher.log"
mkdir -p "$(dirname "$LOG")"
echo "$(date): Crusher healthcheck starting" >> "$LOG"

cd "$HOME/autonomous-trader"

check_port() {
    curl -s --max-time 5 -o /dev/null -w "%{http_code}" "http://127.0.0.1:$1" 2>/dev/null
}

NEED_RESTART=0

# Check Ollama first
if ! pgrep -q ollama; then
    echo "$(date): Ollama NOT running — starting" >> "$LOG"
    open -a Ollama
    sleep 10
fi

# Check port 8080 (USS TradeMinds — all trading + dashboard)
STATUS_8080=$(check_port 8080)
if [ "$STATUS_8080" != "200" ]; then
    echo "$(date): Port 8080 DOWN (got $STATUS_8080) — killing stale" >> "$LOG"
    lsof -ti :8080 | xargs kill -9 2>/dev/null
    sleep 2
    NEED_RESTART=1
else
    echo "$(date): Port 8080 OK" >> "$LOG"
fi

# Check port 9000 (Signal Center)
STATUS_9000=$(check_port 9000)
if [ "$STATUS_9000" != "200" ]; then
    echo "$(date): Port 9000 DOWN (got $STATUS_9000) — restarting Signal Center" >> "$LOG"
    lsof -ti :9000 | xargs kill -9 2>/dev/null
    sleep 2
    cd "$HOME/autonomous-trader" && venv/bin/python3 signal-center/server.py >> "$LOG" 2>&1 &
    echo "$(date): Signal Center restart triggered (PID $!)" >> "$LOG"
    curl -s -o /dev/null \
        -H "Title: Signal Center RESTARTING" \
        -H "Priority: high" \
        -H "Tags: warning" \
        -d "Port 9000 down — Crusher restarted Signal Center at $(date '+%H:%M')" \
        https://ntfy.sh/ollietrades-admin 2>/dev/null || true
else
    echo "$(date): Port 9000 OK" >> "$LOG"
fi

if [ "$NEED_RESTART" -eq 1 ]; then
    echo "$(date): RESTARTING TradeMinds..." >> "$LOG"
    curl -s -o /dev/null \
        -H "Title: TradeMinds RESTARTING" \
        -H "Priority: urgent" \
        -H "Tags: warning" \
        -d "Port 8080 down — Crusher triggered restart at $(date '+%H:%M')" \
        https://ntfy.sh/ollietrades-admin 2>/dev/null || true
    "$HOME/autonomous-trader/restart.sh" >> "$LOG" 2>&1 &
    echo "$(date): Restart triggered" >> "$LOG"
else
    echo "$(date): All systems nominal" >> "$LOG"
fi
