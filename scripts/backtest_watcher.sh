#!/bin/bash
# backtest_watcher.sh — time + progress-based report card scheduler
# Usage: ./scripts/backtest_watcher.sh <PID> <LOG> [--ntfy]
#
# Firing schedule:
#   Minute 30: always fire (early detection — fires immediately if already past 30min)
#   After that: whichever comes first —
#     - 60 minutes since last report, OR
#     - 33% / 66% / 100% progress milestone
#   Never duplicate; always fire at 100%

PID=${1:-$(grep -oE "PID=[0-9]+" /tmp/backtest_180d.pid 2>/dev/null | cut -d= -f2)}
LOG=${2:-$(ls -t ~/autonomous-trader/logs/ollie_backtest_v6_*.log 2>/dev/null | head -1)}
NTFY=0
[[ "$*" == *"--ntfy"* ]] && NTFY=1

if [ -z "$PID" ] || [ -z "$LOG" ] || [ ! -f "$LOG" ]; then
  echo "Usage: $0 <PID> <LOG> [--ntfy]"
  echo "  PID: backtest process ID"
  echo "  LOG: path to backtest log file"
  exit 1
fi

REPORT_CARD=~/autonomous-trader/scripts/backtest_report_card.sh
if [ ! -x "$REPORT_CARD" ]; then
  echo "ERROR: $REPORT_CARD not found or not executable"
  exit 1
fi

# ── FUNCTION DEFINITIONS (must precede any calls) ─────────────────────────

get_progress_pct() {
  # Use Neo's sim-day coverage as progress proxy (Neo is the bottleneck)
  local total_days neo_days
  total_days=$(grep -oE '\[202[0-9]-[0-9]{2}-[0-9]{2}\]' "$LOG" | sort -u | wc -l | tr -d ' ')
  [ "${total_days:-0}" -eq 0 ] && { echo 0; return; }
  neo_days=$(grep "\[OLLAMA\] Neo/" "$LOG" 2>/dev/null | \
    grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}:' | sort -u | wc -l | tr -d ' ')
  echo $(( neo_days * 100 / total_days ))
}

send_card() {
  local label="$1"
  local bt_elapsed=$(( ($(date +%s) - START_TIME) / 60 ))

  echo ""
  echo "╔══════════════════════════════════════════════════════════════════════╗"
  printf "║  %-68s║\n" "▶ MILESTONE: $label"
  printf "║  %-68s║\n" "  Backtest running ${bt_elapsed}min  |  $(date '+%Y-%m-%d %H:%M:%S')"
  echo "╚══════════════════════════════════════════════════════════════════════╝"

  "$REPORT_CARD" "$LOG"

  LAST_REPORT=$(date +%s)

  if [ "$NTFY" -eq 1 ]; then
    local BODY
    BODY=$(
      echo "Milestone: $label (+${bt_elapsed}min)"
      "$REPORT_CARD" "$LOG" | \
        grep -E "Sim window|Neo progress|Total OLLAMA|Errors|✅|🔄|McCoy active|Total:|DB RESULTS" | head -14
    )
    curl -s \
      -H "Title: OllieTrades $label" \
      -H "Tags: chart_increasing" \
      -d "$BODY" \
      ntfy.sh/ollietrades-admin > /dev/null 2>&1 \
      && echo "[ntfy sent]" || echo "[ntfy failed — continuing]"
  fi
}

# ── START TIME (backtest's, not watcher's) ────────────────────────────────
BT_START=""
if [ -f /tmp/backtest_180d.pid ]; then
  BT_STARTED_STR=$(grep "^Started:" /tmp/backtest_180d.pid 2>/dev/null | cut -d' ' -f2-)
  if [ -n "$BT_STARTED_STR" ]; then
    BT_START=$(date -j -f "%a %b %d %T %Z %Y" "$BT_STARTED_STR" +%s 2>/dev/null || \
               date -d "$BT_STARTED_STR" +%s 2>/dev/null)
  fi
fi
# Fallback: log file creation time
[ -z "$BT_START" ] && BT_START=$(stat -f %m "$LOG" 2>/dev/null || stat -c %Y "$LOG" 2>/dev/null)
START_TIME=${BT_START:-$(date +%s)}

LAST_REPORT=0
FIRED_30MIN=0
FIRED_33=0
FIRED_66=0
FIRED_100=0

# ── HEADER ────────────────────────────────────────────────────────────────
BT_ELAPSED_NOW=$(( ($(date +%s) - START_TIME) / 60 ))
echo "╔══════════════════════════════════════════════════════════════════════╗"
printf "║  BACKTEST WATCHER — PID %-47s║\n" "$PID  attached $(date '+%H:%M:%S')"
printf "║  LOG: %-64s║\n" "$(basename "$LOG")"
printf "║  Backtest already running: %-43s║\n" "${BT_ELAPSED_NOW}min"
echo "╠══════════════════════════════════════════════════════════════════════╣"
echo "║  Schedule: 30min → then 60min intervals OR 33/66/100% milestones   ║"
[ "$NTFY" -eq 1 ] && \
  echo "║  ntfy: ENABLED → ollietrades-admin                                  ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""

# ── LATE-ATTACH: fire 30min report immediately if already past threshold ──
if [ "$BT_ELAPSED_NOW" -ge 30 ]; then
  PCT=$(get_progress_pct)
  send_card "30-MIN CHECKPOINT [late-attach +${BT_ELAPSED_NOW}min] (${PCT}% complete)"
  FIRED_30MIN=1
fi

echo "Watching PID $PID... (Ctrl+C to detach — backtest keeps running)"
echo ""

# ── MAIN WATCH LOOP ───────────────────────────────────────────────────────
while ps -p "$PID" > /dev/null 2>&1; do
  ELAPSED_MIN=$(( ($(date +%s) - START_TIME) / 60 ))
  SINCE_LAST=$(( $(date +%s) - LAST_REPORT ))
  PCT=$(get_progress_pct)

  # Minute-30: always fire once (if not already fired in late-attach)
  if [ "$ELAPSED_MIN" -ge 30 ] && [ "$FIRED_30MIN" -eq 0 ]; then
    send_card "30-MIN CHECKPOINT (${PCT}% complete)"
    FIRED_30MIN=1
    continue
  fi

  # 33% milestone
  if [ "$PCT" -ge 33 ] && [ "$FIRED_33" -eq 0 ]; then
    send_card "33% PROGRESS (+${ELAPSED_MIN}min)"
    FIRED_33=1
    continue
  fi

  # 66% milestone
  if [ "$PCT" -ge 66 ] && [ "$FIRED_66" -eq 0 ]; then
    send_card "66% PROGRESS (+${ELAPSED_MIN}min)"
    FIRED_66=1
    continue
  fi

  # 60-min heartbeat (only after 30-min checkpoint fired)
  if [ "$FIRED_30MIN" -eq 1 ] && [ "$SINCE_LAST" -ge 3600 ]; then
    send_card "HOURLY CHECKPOINT (${PCT}% complete, +${ELAPSED_MIN}min)"
    continue
  fi

  sleep 60
done

# ── Process exited — always fire final card ───────────────────────────────
FIRED_100=1
send_card "COMPLETE — process $PID exited"

echo ""
echo "Watcher done."
