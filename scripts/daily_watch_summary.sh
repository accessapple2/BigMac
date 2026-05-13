#!/bin/bash
# Daily watch summary — fires 13:30 AZ (right after US market close)
# Built: HM-CLOSE-GAP W3 (2026-05-12)
# Doctrine: pipe-through ntfy with traffic-light summary; never block trader.

set -uo pipefail
cd ~/autonomous-trader || exit 1

TODAY=$(date +%Y-%m-%d)
DB=~/autonomous-trader/data/trader.db
LOG=~/autonomous-trader/logs/trader_error.log
ERR=~/autonomous-trader/logs/trader_error.log

# === Check 1: HM-EQ daemon — equity snapshot count last 24h ===
HM_EQ_24H=$(grep -E "HM-EQ|equity_snapshot" "$LOG" 2>/dev/null \
  | grep -E "$(date +%Y-%m-%d)|$(date -v-1d +%Y-%m-%d 2>/dev/null || date -d 'yesterday' +%Y-%m-%d)" \
  | wc -l | tr -d ' ')

# === Check 2: Polygon fallback count today ===
POLYGON_FALLBACK=$(grep -iE "polygon.*fallback|fallback.*polygon|polygon.*timed.out|polygon.*fail" "$LOG" 2>/dev/null \
  | grep "$TODAY" | wc -l | tr -d ' ')

# === Check 3: $ATH delisted-ticker errors (HM-BL territory) ===
ATH_ERRORS=$(grep -E "ATH.*delist|ATH.*no data|delisted.*ATH|\\\$ATH" "$ERR" 2>/dev/null \
  | grep "$TODAY" | wc -l | tr -d ' ')

# === Check 4: Adaptive tuning weights (latest run snapshot) ===
WEIGHTS=$(sqlite3 "$DB" "SELECT signal_name || ':' || ROUND(new_weight, 3) FROM adaptive_weights WHERE run_date=(SELECT MAX(run_date) FROM adaptive_weights) ORDER BY signal_name;" 2>/dev/null | tr '\n' ',' | sed 's/,$//' || echo 'no_table')
[ -z "$WEIGHTS" ] && WEIGHTS="defaults (no tuning row)"

# === Check 5: HM-AS-β cadence drift count today ===
BSM_DRIFT=$(grep "HM-AS" "$ERR" 2>/dev/null | grep "$TODAY" | wc -l | tr -d ' ')

# === Check 6: HM-BB writer end-to-end (new positions written today) ===
BB_NEW_ROWS=$(sqlite3 "$DB" "SELECT COUNT(*) FROM positions WHERE date(opened_at) = '$TODAY' AND avg_price IS NOT NULL;" 2>/dev/null || echo 0)

# === Check 7: HM-AN2.C dedup behavior + neo-matrix halt state ===
AN2_FIRED=$(grep "HM-AN2" "$LOG" 2>/dev/null | grep "$TODAY" | wc -l | tr -d ' ')
AN2_HALTED=$(grep "HALTED: neo-matrix" "$LOG" 2>/dev/null | grep "$TODAY" | wc -l | tr -d ' ')
NEO_HALT=$(sqlite3 "$DB" "SELECT halt_mode FROM ai_players WHERE id = 'neo-matrix';" 2>/dev/null || echo 'unknown')

# === Check 8: Trader process health ===
TRADER_PID=$(launchctl list 2>/dev/null | grep com.trademinds.trader | awk '{print $1}')
PORT_BOUND=$(lsof -ti :8080 2>/dev/null | head -1)

# === Check 9: Squeeze candidates surface ===
SQ_CANDIDATES=$(sqlite3 "$DB" "SELECT COUNT(*) FROM squeeze_candidates WHERE date(scan_ts)='$TODAY' AND dismissed=0;" 2>/dev/null || echo 0)
SQ_WATCH=$(sqlite3 "$DB" "SELECT COUNT(*) FROM squeeze_watch WHERE date(scan_ts)='$TODAY' AND dismissed=0;" 2>/dev/null || echo 0)

# === Check 10: Recent [red]/Traceback count ===
RED_COUNT=$(grep -cE "\[red\]|Traceback" "$LOG" 2>/dev/null | head -1 || echo 0)

# === Traffic-light tags ===
TAG_EQ="🟢"; [ "$HM_EQ_24H" -lt 200 ] && TAG_EQ="🟡"; [ "$HM_EQ_24H" -lt 100 ] && TAG_EQ="🔴"
TAG_ATH="🟢"; [ "$ATH_ERRORS" -gt 0 ] && TAG_ATH="🔴"
TAG_DRIFT="🟢"; [ "$BSM_DRIFT" -gt 5 ] && TAG_DRIFT="🟡"; [ "$BSM_DRIFT" -gt 20 ] && TAG_DRIFT="🔴"
TAG_NEO="🟢"; [ "$NEO_HALT" != "active" ] && TAG_NEO="🟡"
TAG_PORT="🟢"; [ -z "$PORT_BOUND" ] && TAG_PORT="🔴"

# === Compose summary ===
SUMMARY="📊 Daily Watch — $TODAY
━━━━━━━━━━━━━━━━━━━━━━
$TAG_EQ HM-EQ snaps 24h:   $HM_EQ_24H  (expect ~288)
🟢 Polygon fallbacks: $POLYGON_FALLBACK  (>500 = degraded)
$TAG_ATH \$ATH errors:      $ATH_ERRORS  (expect 0)
🟢 Adaptive weights:  $WEIGHTS
$TAG_DRIFT BSM drift today:  $BSM_DRIFT  (>20 = scheduler stalling)
🟢 HM-BB new positions: $BB_NEW_ROWS
🟢 HM-AN2.C fired:    $AN2_FIRED (HALTED gate: $AN2_HALTED)
$TAG_NEO neo-matrix halt:  $NEO_HALT
🟢 Squeeze surface:   watch=$SQ_WATCH cand=$SQ_CANDIDATES
$TAG_PORT Trader PID/Port:  $TRADER_PID / $PORT_BOUND
🟢 [red]/Traceback:   $RED_COUNT (lifetime; new today via grep $TODAY for detail)"

# === Dispatch ntfy ===
PRIORITY="default"
echo "$SUMMARY" | grep -qE "🔴" && PRIORITY="high"

curl -fsS \
  -d "$SUMMARY" \
  -H "Title: 📊 Daily Watch $TODAY" \
  -H "Priority: $PRIORITY" \
  -H "Tags: chart_with_upwards_trend" \
  https://ntfy.sh/ollietrades-admin > /dev/null 2>&1 || echo "ntfy POST failed"

# === Log locally ===
mkdir -p ~/autonomous-trader/logs
echo "$SUMMARY" >> ~/autonomous-trader/logs/daily_watch.log
echo "---" >> ~/autonomous-trader/logs/daily_watch.log
echo "[daily_watch] dispatched at $(date '+%Y-%m-%d %H:%M:%S')"

# === HM-CLUSTER-PNL === per-cluster P&L (Vol Breakout 4-sym clusters as 1 bet)
CLUSTER_PNL=$(sqlite3 "$DB" "
WITH entries AS (
  SELECT
    executed_at,
    symbol,
    reasoning,
    price,
    qty,
    -- bucket entries by 5-minute windows
    strftime('%Y-%m-%d %H:%M', datetime((strftime('%s', executed_at) / 300) * 300, 'unixepoch')) AS bucket_5m,
    CASE
      WHEN reasoning LIKE '%VOLATILITY_BREAKOUT%' THEN 'VolBreakout'
      WHEN reasoning LIKE '%CONVERGENCE%'         THEN 'Convergence'
      WHEN reasoning LIKE '%TRIPLE ALIGNED%'      THEN 'TripleAligned'
      ELSE NULL
    END AS cluster_type
  FROM trades
  WHERE action='BUY'
    AND date(executed_at, 'localtime') = date('now', 'localtime')
)
SELECT
  cluster_type || ' ' || bucket_5m AS cluster,
  COUNT(*)                          AS n_entries,
  GROUP_CONCAT(symbol, ',')         AS symbols
FROM entries
WHERE cluster_type IS NOT NULL
GROUP BY cluster_type, bucket_5m
HAVING n_entries >= 2
ORDER BY bucket_5m DESC;
" 2>/dev/null)
# === /HM-CLUSTER-PNL ===

