#!/bin/zsh
# One-shot post-check for com.ollietrades.hm-signals-v2-monday-check.
# Captures fire status + logs + relay report + backlog append into a single
# file for the next live session to read -- no judgment calls, just capture.

OUT="/Users/bigmac/autonomous-trader/logs/monday_check_verification_2026-07-13.txt"

{
  echo "=== HM-SIGNALS-V2-STARVATION-RECURRENCE Monday check verification ==="
  echo "generated: $(date)"
  echo

  echo "--- launchctl print (fire status) ---"
  launchctl print gui/501/com.ollietrades.hm-signals-v2-monday-check 2>&1
  echo

  echo "--- console session (who) ---"
  who
  echo

  echo "--- stdout log tail ---"
  if [ -f /Users/bigmac/autonomous-trader/logs/hm_signals_v2_monday_check_stdout.log ]; then
    tail -100 /Users/bigmac/autonomous-trader/logs/hm_signals_v2_monday_check_stdout.log
  else
    echo "NOT FOUND"
  fi
  echo

  echo "--- stderr log tail ---"
  if [ -f /Users/bigmac/autonomous-trader/logs/hm_signals_v2_monday_check_stderr.log ]; then
    tail -100 /Users/bigmac/autonomous-trader/logs/hm_signals_v2_monday_check_stderr.log
  else
    echo "NOT FOUND"
  fi
  echo

  echo "--- relay report ---"
  RELAY=/Users/bigmac/autonomous-trader/data/reports/relay/relay_2026-07-13_signals-v2-monday-check.md
  if [ -f "$RELAY" ]; then
    cat "$RELAY"
  else
    echo "NOT FOUND: $RELAY"
  fi
  echo

  echo "--- XO_BACKLOG.md append check ---"
  if grep -q '\*\*Monday check result (2026-07-13' /Users/bigmac/autonomous-trader/docs/XO_BACKLOG.md 2>/dev/null; then
    echo "FOUND -- backlog was appended:"
    grep -A2 '\*\*Monday check result (2026-07-13' /Users/bigmac/autonomous-trader/docs/XO_BACKLOG.md
  else
    echo "NOT FOUND -- backlog append missing"
  fi
} > "$OUT" 2>&1

echo "wrote $OUT"
