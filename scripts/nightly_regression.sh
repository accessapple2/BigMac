#!/usr/bin/env bash
# Nightly Endurance Regression Test — runs at 2 AM AZ (09:00 UTC) via launchd
# Fires a 30-day OOS backtest for all Active 4 + Bench 4 agents.
# Sends ntfy summary on completion. Writes report to logs/nightly_regression_YYYYMMDD.log

set -euo pipefail

BASE="$HOME/autonomous-trader"
LOG_DIR="$BASE/logs"
DATE=$(date +%Y%m%d)
LOG="$LOG_DIR/nightly_regression_${DATE}.log"
NTFY_TOPIC="ollietrades-admin"

mkdir -p "$LOG_DIR"

echo "$(date): ── Nightly Regression START ──" | tee -a "$LOG"

# Only run on weekdays (Mon–Fri); skip weekends
DOW=$(date +%u)  # 1=Mon ... 7=Sun
if [ "$DOW" -ge 6 ]; then
    echo "$(date): Weekend — skipping regression run" | tee -a "$LOG"
    exit 0
fi

# Run the 30-day OOS backtest (all players, no subset)
cd "$BASE"
echo "$(date): Starting 30-day OOS backtest..." | tee -a "$LOG"

if venv/bin/python3 scripts/ollie_backtest_30d.py >> "$LOG" 2>&1; then
    STATUS="✅ PASS"
    PRIORITY="default"
    echo "$(date): Backtest completed successfully" | tee -a "$LOG"
else
    STATUS="❌ FAIL"
    PRIORITY="high"
    echo "$(date): Backtest FAILED — check $LOG" | tee -a "$LOG"
fi

# Tail the last 20 lines for the ntfy summary
TAIL=$(tail -20 "$LOG" 2>/dev/null | head -c 1000)

# Send ntfy notification
curl -s -o /dev/null \
    -H "Title: Nightly Regression ${STATUS} — ${DATE}" \
    -H "Priority: ${PRIORITY}" \
    -H "Tags: chart_with_upwards_trend,regression,nightly" \
    -d "$(printf '%s\n\nLog: %s' "$TAIL" "$LOG")" \
    "https://ntfy.sh/${NTFY_TOPIC}" 2>/dev/null || true

echo "$(date): ── Nightly Regression END ──" | tee -a "$LOG"

# Purge logs older than 14 days
find "$LOG_DIR" -name "nightly_regression_*.log" -mtime +14 -delete 2>/dev/null || true
