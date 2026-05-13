#!/usr/bin/env bash
# HM-STALE-TRIM observation — log positions that would be trimmed under
# the "held >10 days, no TP2, current price < entry" rule.
# OBSERVATION-ONLY — no actual trades fired. Captain reviews log output for ~1 week
# before any code-level activation.

DB=~/autonomous-trader/data/trader.db
LOG=~/autonomous-trader/logs/trader_error.log
TODAY=$(date '+%Y-%m-%d')

sqlite3 "$DB" "
SELECT
  '[HM-STALE-TRIM-OBS] ' || player_id || ' ' || symbol ||
  ' held=' || CAST(julianday('now') - julianday(executed_at) AS INT) || 'd' ||
  ' entry=$' || ROUND(price, 2) ||
  ' qty=' || ROUND(qty, 3) ||
  ' WOULD trim 50% (observation only)'
FROM trades t1
WHERE t1.action='BUY'
  -- Exclude real-money tracking accounts + liquidated webull (HM-WEBULL-LIQUIDATED 2026-05-13)
  AND t1.player_id NOT IN ('webull','alpaca-mirror','dalio-metals','ibkr-real','kirk-real','schwab','schwab-real')
  AND julianday('now') - julianday(t1.executed_at) > 10
  AND NOT EXISTS (
    -- Position must still be open (no later SELL that fully exited)
    SELECT 1 FROM trades t2
    WHERE t2.player_id = t1.player_id
      AND t2.symbol = t1.symbol
      AND t2.action = 'SELL'
      AND t2.executed_at > t1.executed_at
      AND ABS(t2.qty - t1.qty) < 0.001  -- close to full exit (tolerate rounding)
  )
ORDER BY t1.executed_at ASC
LIMIT 20;
" 2>/dev/null | while read LINE; do
  echo "$(date '+%H:%M:%S') [LRS] $LINE" >> "$LOG"
done

echo "[stale_trim] obs run complete $(date '+%H:%M %Z')"
