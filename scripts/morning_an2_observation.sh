#!/bin/bash
# Morning HM-AN2.C observation — fires 11:00 AZ Mon-Fri
# (90 min after US market open; HM-AN2.C should have evaluated several
#  Signal Center signal cycles by now with neo-matrix.halt_mode='exit_only').
#
# Summarizes neo-matrix consume behavior and HALTS for Captain SQL flip
# decision (per HM-CLOSE-GAP Q4 — never auto-flip halt_mode).

set -uo pipefail
cd ~/autonomous-trader || exit 1

LOG=~/autonomous-trader/logs/trader.log
DB=~/autonomous-trader/data/trader.db
TODAY=$(date +%Y-%m-%d)

# Gather observation data
AN2_FIRED=$(grep "HM-AN2" "$LOG" 2>/dev/null | grep "$TODAY" | wc -l | tr -d ' ')
AN2_CANDIDATES=$(grep "HM-AN2.*CANDIDATE" "$LOG" 2>/dev/null | grep "$TODAY" | wc -l | tr -d ' ')
AN2_HALTED=$(grep "HALTED: neo-matrix" "$LOG" 2>/dev/null | grep "$TODAY" | wc -l | tr -d ' ')
AN2_BLOCKED=$(grep "HM-AN2.*BLOCKED" "$LOG" 2>/dev/null | grep "$TODAY" | wc -l | tr -d ' ')
AN2_EXECUTED=$(grep "HM-AN2.*EXECUTED" "$LOG" 2>/dev/null | grep "$TODAY" | wc -l | tr -d ' ')
AN2_RED=$(grep -E "\[red\].*neo-matrix|neo-matrix.*Traceback" "$LOG" 2>/dev/null | grep "$TODAY" | wc -l | tr -d ' ')

# Current halt_mode state
NEO_HALT=$(sqlite3 "$DB" "SELECT halt_mode FROM ai_players WHERE id='neo-matrix';" 2>/dev/null || echo unknown)
NEO_REASON=$(sqlite3 "$DB" "SELECT COALESCE(halt_reason,'') FROM ai_players WHERE id='neo-matrix';" 2>/dev/null || echo '')

# Sample of recent decision lines
DECISIONS=$(grep -E "HM-AN2|HALTED: neo-matrix" "$LOG" 2>/dev/null | grep "$TODAY" | tail -15)

# Compose summary
SUMMARY="📊 HM-AN2.C Observation — $TODAY 11:00 AZ
━━━━━━━━━━━━━━━━━━━━━━
Signal consume fires:  $AN2_FIRED
  ├ CANDIDATE log:     $AN2_CANDIDATES
  ├ EXECUTED:          $AN2_EXECUTED
  ├ BLOCKED by gates:  $AN2_BLOCKED
  └ HALTED gate fires: $AN2_HALTED  (proves halt_mode='exit_only' is working)

[red] errors today:    $AN2_RED  (expect 0)

neo-matrix.halt_mode:  $NEO_HALT
neo-matrix.halt_reason: $NEO_REASON

Recent decisions (last 15):
$DECISIONS

Captain action (per HM-CLOSE-GAP Q4):
If behavior looks acceptable (CANDIDATEs > 0, EXECUTED == 0,
HALTED gate fires correctly, [red] == 0), flip neo-matrix to live with:

  cp data/trader.db backups/trader.db.pre-an2-flip-\$(date +%Y%m%d_%H%M).bak
  sqlite3 data/trader.db \"UPDATE ai_players SET halt_mode='active', halt_reason=NULL WHERE id='neo-matrix';\"
  sqlite3 data/trader.db \"SELECT halt_mode FROM ai_players WHERE id='neo-matrix';\"  -- expect 'active'

If behavior shows anomalies (EXECUTED > 0 despite exit_only — gate bypass
bug — or [red] > 0), keep exit_only and investigate."

PRIORITY="default"
[ "$AN2_RED" -gt 0 ] && PRIORITY="high"
[ "$AN2_EXECUTED" -gt 0 ] && [ "$NEO_HALT" != "active" ] && PRIORITY="high"  # gate bypass = urgent

curl -fsS \
  -d "$SUMMARY" \
  -H "Title: HM-AN2.C Observation $TODAY" \
  -H "Priority: $PRIORITY" \
  -H "Tags: rocket" \
  https://ntfy.sh/ollietrades-admin > /dev/null 2>&1 || echo "ntfy POST failed"

mkdir -p ~/autonomous-trader/logs
echo "$SUMMARY" >> ~/autonomous-trader/logs/morning_observations.log
echo "---" >> ~/autonomous-trader/logs/morning_observations.log
echo "[morning_an2_observation] dispatched at $(date '+%Y-%m-%d %H:%M:%S')"
