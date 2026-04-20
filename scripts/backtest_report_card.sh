#!/bin/bash
# backtest_report_card.sh — in-flight summary for ollie_backtest_v6 runs
# Usage: ./scripts/backtest_report_card.sh [LOG]
# Compatible with bash 3.2 (macOS default)

LOG="${1:-$(ls -t ~/autonomous-trader/logs/ollie_backtest_v6_*.log 2>/dev/null | head -1)}"

if [ ! -f "$LOG" ]; then
  echo "ERROR: log not found: $LOG"
  exit 1
fi

NOW=$(date '+%Y-%m-%d %H:%M:%S')

# grep_count: safe count — grep -c exits 1 on no matches (double-outputs with || echo)
grep_count() { grep -c "$1" "$2" 2>/dev/null; }

# ── DATE RANGE & PROGRESS ──────────────────────────────────────────────────
FIRST_DATE=$(grep -oE '\[202[0-9]-[0-9]{2}-[0-9]{2}\]' "$LOG" | head -1 | tr -d '[]')
LAST_DATE=$(grep -oE '\[202[0-9]-[0-9]{2}-[0-9]{2}\]' "$LOG" | sort -u | tail -1 | tr -d '[]')
TOTAL_DAYS=$(grep -oE '\[202[0-9]-[0-9]{2}-[0-9]{2}\]' "$LOG" | sort -u | wc -l | tr -d ' ')

# Latest sim date seen for each agent (from OLLAMA call lines)
agent_last() { grep "\[OLLAMA\] ${1}/" "$LOG" 2>/dev/null | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}:' | tail -1 | tr -d ':'; }
agent_calls() { grep -c "\[OLLAMA\] ${1}/" "$LOG" 2>/dev/null; }
agent_buys()  { grep -c "\[OLLAMA\] ${1}/.*BUY("  "$LOG" 2>/dev/null; }
agent_sells() { grep -c "\[OLLAMA\] ${1}/.*SELL(" "$LOG" 2>/dev/null; }
agent_holds() { grep -c "\[OLLAMA\] ${1}/.*HOLD(" "$LOG" 2>/dev/null; }

DATA_LAST=$(agent_last "Data");  DATA_CALLS=$(agent_calls "Data")
NEO_LAST=$(agent_last "Neo");    NEO_CALLS=$(agent_calls "Neo")
CHEK_LAST=$(agent_last "Chekov"); CHEK_CALLS=$(agent_calls "Chekov")
DAX_LAST=$(agent_last "Dax");    DAX_CALLS=$(agent_calls "Dax")
MCCY_LAST=$(agent_last "McCoy"); MCCY_CALLS=$(agent_calls "McCoy")

DATA_B=$(agent_buys "Data"); DATA_S=$(agent_sells "Data"); DATA_H=$(agent_holds "Data")
NEO_B=$(agent_buys "Neo");   NEO_S=$(agent_sells "Neo");   NEO_H=$(agent_holds "Neo")
CHEK_B=$(agent_buys "Chekov"); CHEK_S=$(agent_sells "Chekov"); CHEK_H=$(agent_holds "Chekov")
DAX_B=$(agent_buys "Dax");   DAX_S=$(agent_sells "Dax");   DAX_H=$(agent_holds "Dax")
MCCY_B=$(agent_buys "McCoy"); MCCY_S=$(agent_sells "McCoy"); MCCY_H=$(agent_holds "McCoy")

# Progress: Neo's unique dates vs total unique dates (Neo = bottleneck, daily caller)
NEO_DAYS=$(grep "\[OLLAMA\] Neo/" "$LOG" 2>/dev/null | \
  grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}:' | sort -u | wc -l | tr -d ' ')
PCT=0
if [ "${TOTAL_DAYS:-0}" -gt 0 ]; then
  PCT=$(( NEO_DAYS * 100 / TOTAL_DAYS ))
fi

# Agent status: done if last date == overall last date
# Conditional agents (McCoy) only fire on specific regimes — check if Data is done as proxy
agent_status() {
  local last="$1" is_conditional="${3:-0}"
  if [ -z "$last" ]; then
    echo "— no calls"
  elif [ "$last" = "$LAST_DATE" ]; then
    echo "✅ DONE"
  elif [ "$is_conditional" = "1" ] && [ "$DATA_LAST" = "$LAST_DATE" ]; then
    echo "✅ DONE (regime-gated)"
  else
    echo "🔄 ${last}"
  fi
}

DATA_ST=$(agent_status "$DATA_LAST" "$DATA_CALLS")
NEO_ST=$(agent_status "$NEO_LAST"  "$NEO_CALLS")
CHEK_ST=$(agent_status "$CHEK_LAST" "$CHEK_CALLS" 1)
DAX_ST=$(agent_status "$DAX_LAST"  "$DAX_CALLS"  1)
MCCY_ST=$(agent_status "$MCCY_LAST" "$MCCY_CALLS" 1)

# ── REGIME DISTRIBUTION ────────────────────────────────────────────────────
REGIME_NEUTRAL=$(grep_count 'Regime=NEUTRAL' "$LOG")
REGIME_CAUTIOUS=$(grep_count 'Regime=CAUTIOUS' "$LOG")
REGIME_BULL=$(grep_count 'Regime=BULL_CALM' "$LOG")
REGIME_BEAR=$(grep_count 'Regime=BEAR' "$LOG")
REGIME_CRISIS=$(grep_count 'Regime=CRISIS' "$LOG")
MCCOY_ACTIVE=$(grep_count 'McCoy=active' "$LOG")

# ── HIGH CONVICTION ────────────────────────────────────────────────────────
HC_COUNT=$(grep -cE '\(8/10\)|\(9/10\)|\(10/10\)' "$LOG" 2>/dev/null)
HC_BUYS=$(grep -E '\(8/10\)|\(9/10\)|\(10/10\)' "$LOG" 2>/dev/null | grep "BUY(" | \
  sed 's/.*\[OLLAMA\] //' | sed 's/ [0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}: / /' | tail -5)

# ── TOTAL CALLS & ERRORS ───────────────────────────────────────────────────
TOTAL_CALLS=$(grep_count '\[OLLAMA\]' "$LOG")
ERR_COUNT=$(grep -cE '\[ollama-err\]|Traceback|^ERROR' "$LOG" 2>/dev/null)

# ── DB RESULTS (written at completion) ────────────────────────────────────
DB=~/autonomous-trader/data/trader.db
DB_RESULTS=""
if [ -f "$DB" ]; then
  DB_RESULTS=$(sqlite3 "$DB" "
    SELECT printf('  %-12s %-10s %+7.2f%%  Sharpe %5.2f  WR %4.1f%%  %2d trades',
      br.display_name, r.version_tag,
      br.total_return_pct*100, br.sharpe_ratio, br.win_rate*100, br.num_trades)
    FROM backtest_results br
    JOIN backtest_runs r ON r.id = br.run_id
    WHERE r.days = 180
    ORDER BY r.id DESC, br.sharpe_ratio DESC
    LIMIT 15;" 2>/dev/null)
fi

# ── PRINT REPORT ──────────────────────────────────────────────────────────
W=70  # box width (chars between │ delimiters)
fmt() { printf "│ %-${W}s│\n" "$1"; }

echo "┌─ BACKTEST REPORT CARD $(printf '─%.0s' {1..47})┐"
fmt "Log:  $(basename "$LOG")"
fmt "Time: $NOW"
echo "├─ PROGRESS $(printf '─%.0s' {1..59})┤"
fmt "Sim window:  ${FIRST_DATE} → ${LAST_DATE}  ($TOTAL_DAYS trading days)"
fmt "Neo progress: ${NEO_DAYS}/${TOTAL_DAYS} days  (${PCT}%  — Neo is bottleneck)"
fmt "Total OLLAMA calls: ${TOTAL_CALLS}  |  Errors: ${ERR_COUNT}"
echo "├─ AGENT STATUS $(printf '─%.0s' {1..55})┤"
printf "│  %-8s  %-16s  calls=%-5s  B=%-4s S=%-4s H=%s\n" \
  "Data"   "$DATA_ST" "$DATA_CALLS" "$DATA_B" "$DATA_S" "$DATA_H"
printf "│  %-8s  %-16s  calls=%-5s  B=%-4s S=%-4s H=%s\n" \
  "Neo"    "$NEO_ST"  "$NEO_CALLS"  "$NEO_B"  "$NEO_S"  "$NEO_H"
printf "│  %-8s  %-16s  calls=%-5s  B=%-4s S=%-4s H=%s\n" \
  "Chekov" "$CHEK_ST" "$CHEK_CALLS" "$CHEK_B" "$CHEK_S" "$CHEK_H"
printf "│  %-8s  %-16s  calls=%-5s  B=%-4s S=%-4s H=%s\n" \
  "Dax"    "$DAX_ST"  "$DAX_CALLS"  "$DAX_B"  "$DAX_S"  "$DAX_H"
printf "│  %-8s  %-16s  calls=%-5s  B=%-4s S=%-4s H=%s\n" \
  "McCoy"  "$MCCY_ST" "$MCCY_CALLS" "$MCCY_B" "$MCCY_S" "$MCCY_H"
echo "├─ REGIMES $(printf '─%.0s' {1..60})┤"
printf "│  NEUTRAL=%-4s  CAUTIOUS=%-4s  BULL_CALM=%-4s  BEAR=%-4s  CRISIS=%-4s │\n" \
  "$REGIME_NEUTRAL" "$REGIME_CAUTIOUS" "$REGIME_BULL" "$REGIME_BEAR" "$REGIME_CRISIS"
fmt "McCoy active days: $MCCOY_ACTIVE"
echo "├─ HIGH-CONVICTION BUYs (8-10/10) $(printf '─%.0s' {1..36})┤"
fmt "Total: $HC_COUNT signals"
if [ -n "$HC_BUYS" ]; then
  while IFS= read -r line; do
    fmt "  $line"
  done <<< "$HC_BUYS"
fi
if [ -n "$DB_RESULTS" ]; then
  echo "├─ DB RESULTS (180-day final) $(printf '─%.0s' {1..41})┤"
  while IFS= read -r line; do
    fmt "$line"
  done <<< "$DB_RESULTS"
fi
echo "└$(printf '─%.0s' {1..71})┘"
