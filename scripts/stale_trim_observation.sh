#!/usr/bin/env bash
# HM-STALE-TRIM observation — log positions that would be trimmed under
# the "held >10 days, no TP2, current price < entry" rule.
# OBSERVATION-ONLY — no actual trades fired. Captain reviews log output for ~1 week
# before any code-level activation.

DB=~/autonomous-trader/data/trader.db
LOG=~/autonomous-trader/logs/trader_error.log
TODAY=$(date '+%Y-%m-%d')

sqlite3 "$DB" "
-- HM-STALE-TRIM-OBS-V2 (2026-05-13) — positions table is source of truth
-- (V1 used trades arithmetic which produced false-positives on partial exits)
SELECT
  '[HM-STALE-TRIM-OBS] ' || p.player_id || ' ' || p.symbol ||
  ' held=' || CAST(julianday('now') - julianday(MAX(t.executed_at)) AS INT) || 'd' ||
  ' entry=\$' || ROUND(MAX(CASE WHEN t.action='BUY' THEN t.price ELSE NULL END), 2) ||
  ' qty=' || ROUND(p.qty, 3) ||
  ' WOULD trim 50% (observation only)'
FROM positions p
JOIN trades t ON t.player_id = p.player_id AND t.symbol = p.symbol AND t.action = 'BUY'
WHERE p.qty > 0
  AND p.player_id NOT IN ('webull','alpaca-mirror','dalio-metals','ibkr-real','kirk-real','schwab','schwab-real')
  AND p.player_id IN (SELECT id FROM ai_players WHERE halt_mode='active')
GROUP BY p.player_id, p.symbol
HAVING CAST(julianday('now') - julianday(MAX(t.executed_at)) AS INT) > 10
ORDER BY MAX(t.executed_at) ASC
LIMIT 20;
" 2>/dev/null | while read LINE; do
  echo "$(date '+%H:%M:%S') [LRS] $LINE" >> "$LOG"
done

echo "[stale_trim] obs run complete $(date '+%H:%M %Z')"
