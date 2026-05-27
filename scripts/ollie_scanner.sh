#!/bin/bash
# Ollie Live Scanner — Holly-style multi-strategy convergence
# Usage: ./ollie_scanner.sh          (one-shot)
#        watch -n 30 ./ollie_scanner.sh   (live refresh every 30s)

DB="$HOME/autonomous-trader/data/trader.db"
MINS="${1:-90}"

clear
echo "╔══════════════════════════════════════════════════════════════════════════════════════════╗"
echo "║  🔭  OLLIE LIVE SCANNER — last ${MINS} min                                $(date '+%H:%M:%S %Z')  ║"
echo "╚══════════════════════════════════════════════════════════════════════════════════════════╝"
echo ""

echo "── TIER 1: 5+ strategy convergence (highest conviction) ──"
sqlite3 -header -column "$DB" "
SELECT ticker, COUNT(DISTINCT strategy_name) as n,
       ROUND(MAX(confidence)*100,0) as conf,
       ROUND(AVG(entry_price),2) as entry,
       ROUND(AVG(stop_price),2) as stop,
       ROUND(AVG(target_price),2) as target,
       ROUND((AVG(target_price)-AVG(entry_price))/(AVG(entry_price)-AVG(stop_price)),2) as rr,
       GROUP_CONCAT(DISTINCT strategy_name) as strategies
FROM strategy_signals
WHERE date(scan_date)=date('now')
  AND created_at > datetime('now','-${MINS} minutes')
GROUP BY ticker
HAVING n >= 5
ORDER BY n DESC, conf DESC
LIMIT 10;"

echo ""
echo "── TIER 2: 4-strategy convergence ──"
sqlite3 -header -column "$DB" "
SELECT ticker, COUNT(DISTINCT strategy_name) as n,
       ROUND(MAX(confidence)*100,0) as conf,
       ROUND(AVG(entry_price),2) as entry,
       ROUND(AVG(stop_price),2) as stop,
       ROUND(AVG(target_price),2) as target,
       ROUND((AVG(target_price)-AVG(entry_price))/(AVG(entry_price)-AVG(stop_price)),2) as rr
FROM strategy_signals
WHERE date(scan_date)=date('now')
  AND created_at > datetime('now','-${MINS} minutes')
GROUP BY ticker
HAVING n = 4
ORDER BY conf DESC
LIMIT 15;"

echo ""
echo "── TIER 3: 3-strategy convergence (early stage) ──"
sqlite3 -header -column "$DB" "
SELECT ticker, COUNT(DISTINCT strategy_name) as n,
       ROUND(MAX(confidence)*100,0) as conf,
       ROUND(AVG(entry_price),2) as entry,
       ROUND(AVG(stop_price),2) as stop,
       ROUND(AVG(target_price),2) as target
FROM strategy_signals
WHERE date(scan_date)=date('now')
  AND created_at > datetime('now','-${MINS} minutes')
GROUP BY ticker
HAVING n = 3
ORDER BY conf DESC
LIMIT 10;"

echo ""
echo "── ALREADY IN POSITION (avoid double-entry) ──"
sqlite3 "$DB" "SELECT GROUP_CONCAT(DISTINCT symbol) FROM positions WHERE qty != 0 AND asset_type='stock';"

echo ""
echo "── LEGEND ──"
echo "  n      = number of strategies agreeing"
echo "  conf   = max confidence among strategies (%)"
echo "  rr     = reward/risk ratio (target-entry)/(entry-stop)"
echo "  Tier 1 = 5+ strategies (rare, act fast)"
echo "  Tier 2 = 4 strategies (strong)"
echo "  Tier 3 = 3 strategies (developing)"
