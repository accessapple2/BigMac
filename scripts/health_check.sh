#!/usr/bin/env bash
# === HM-HC ===
# OllieTrades trader health check — canonical version.
#
# Usage: ./scripts/health_check.sh
# Idempotent: read-only. No restarts, no SQL writes, no broker calls
# that mutate state. Safe to run any time, any frequency.
#
# Schema corrections vs Captain's 2026-05-12 morning ad-hoc script:
#   positions:    avg_entry_price → avg_price; current_price / unrealized_pl
#                 / unrealized_plpc do NOT exist as columns (computed at read)
#   trades:       timestamp → executed_at; side → action; agent → player_id
#   ghost_trades: timestamp → ts; agent → advisor
#   alpaca_account TABLE doesn't exist — live equity via /api/alpaca/status
#   memory:       Swap % is misleading on macOS; use `memory_pressure` Free %
#                 per HM-BHBI calibration
#
# Anchored # === HM-HC === at top of file.

set -u
cd "$(dirname "$0")/.." || exit 1
DB=data/trader.db
LOG=logs/trader.log
ERR=logs/trader_error.log
WDOG=logs/watchdog.log

# Helper: SQLite query with header + column output
sq() { sqlite3 -header -column "$DB" "$1" 2>/dev/null; }

echo "── ⏰ Market clock ──"
TZ=America/New_York date "+  %Y-%m-%d %H:%M %Z (%A)"

echo ""
echo "── 🟢 Service health ──"
PID=$(launchctl list 2>/dev/null | grep com.trademinds.trader | awk '{print $1}')
if [ -z "$PID" ] || [ "$PID" = "-" ]; then
  PID=$(pgrep -af main.py 2>/dev/null | head -1 | awk '{print $1}')
fi
if [ -n "$PID" ]; then
  echo "  trader PID: $PID"
  ps -o etime,%cpu,%mem,stat -p "$PID" 2>/dev/null | tail -1 | awk '{printf "  elapsed=%s  cpu=%s%%  mem=%s%%  stat=%s\n", $1, $2, $3, $4}'
  PORT_PID=$(lsof -ti :8080 2>/dev/null | head -1)
  echo "  port 8080 owner: ${PORT_PID:-<none>}"
else
  echo "  ❌ trader process not found"
fi

echo ""
echo "── 📊 Endpoint smoke ──"
for line in \
  "/                          " \
  "/api/symbol/SPY/scorecard  " \
  "/api/market/candles/IBM    " \
  "/api/ghost-trades/stats    "; do
  path=$(echo "$line" | awk '{print $1}')
  label=$(printf "%-28s" "$path")
  curl -s -o /dev/null -w "  $label HTTP %{http_code}  %{time_total}s\n" --max-time 6 "http://localhost:8080${path}"
done

echo ""
echo "── 💰 Live Alpaca paper equity (via /api/alpaca/status) ──"
curl -s -o /tmp/hmhc_alpaca.json --max-time 5 "http://localhost:8080/api/alpaca/status"
if [ -s /tmp/hmhc_alpaca.json ]; then
  python3 -c "
import json
with open('/tmp/hmhc_alpaca.json') as f: d = json.load(f)
if not d.get('connected'):
    print(f'  ❌ alpaca not connected: {d}')
else:
    print(f\"  equity:          \${d.get('equity', 0):,.2f}\")
    print(f\"  portfolio_value: \${d.get('portfolio_value', 0):,.2f}\")
    print(f\"  cash:            \${d.get('cash', 0):,.2f}\")
    print(f\"  buying_power:    \${d.get('buying_power', 0):,.2f}\")
"
else
  echo "  ❌ /api/alpaca/status returned empty"
fi

echo ""
echo "── 📜 DoD baseline (last alpaca-mirror snapshot in portfolio_history) ──"
sq "
  SELECT
    ROUND(total_value, 2) AS value,
    ROUND(cash, 2)        AS cash,
    datetime(recorded_at, 'localtime') AS recorded,
    CAST((julianday('now') - julianday(recorded_at)) * 24 AS INTEGER) || 'h ago' AS age
  FROM portfolio_history
  WHERE player_id = 'alpaca-mirror'
  ORDER BY recorded_at DESC
  LIMIT 1;
"
echo "  ⚠  if age > 24h, record_portfolio_snapshot('alpaca-mirror', ...) hasn't fired recently — check ai_brain.py L711 HM-BD.F-audit wrap"

echo ""
echo "── 📈 Open positions (most recent first) ──"
sq "
  SELECT
    player_id, symbol, qty,
    ROUND(avg_price, 2) AS avg_px,
    asset_type,
    datetime(opened_at, 'localtime') AS opened
  FROM positions
  WHERE qty != 0
  ORDER BY opened_at DESC
  LIMIT 10;
"

echo ""
echo "── 🚨 Recent fills (last 2 hours) ──"
sq "
  SELECT
    datetime(executed_at, 'localtime') AS t,
    symbol, action, qty,
    ROUND(price, 2) AS price,
    player_id
  FROM trades
  WHERE executed_at > datetime('now', '-2 hours')
  ORDER BY executed_at DESC
  LIMIT 10;
"

echo ""
echo "── 🎯 Signals by hour today (canonical 'signals' table only) ──"
echo "    note: fleet-specific activity also lives in 15+ specialized *_signals tables"
sq "
  SELECT
    strftime('%H:00', created_at, 'localtime') AS hour,
    COUNT(*) AS signals
  FROM signals
  WHERE date(created_at, 'localtime') = date('now', 'localtime')
  GROUP BY hour
  ORDER BY hour;
"

echo ""
echo "── 👻 Ghost writer end-to-end (HM-BB validation) ──"
sq "
  SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN id > 16 THEN 1 ELSE 0 END) AS new_today,
    SUM(CASE WHEN entry_price IS NOT NULL THEN 1 ELSE 0 END) AS with_entry,
    SUM(CASE WHEN exit_price  IS NOT NULL THEN 1 ELSE 0 END) AS with_exit
  FROM ghost_trades;
"
echo ""
echo "  most recent 3 ghost rows:"
sq "
  SELECT
    id, symbol, side,
    ROUND(entry_price, 2) AS entry,
    ROUND(exit_price,  2) AS exit,
    advisor,
    datetime(ts, 'localtime') AS ts
  FROM ghost_trades
  ORDER BY id DESC
  LIMIT 3;
"

echo ""
echo "── ⚠️  Anomaly scan ──"

# Helper: grep+count that always returns a single integer (grep -c returns
# exit-code 1 on zero matches, which breaks `var=$(... || echo 0)` pipelines
# by appending "0" to a non-empty result. Use wc -l after grep instead.)
gc() { grep "$@" 2>/dev/null | wc -l | tr -d ' '; }

# Bridge banner — HM-BK + HM-BK-residual validation
# Search whole log; show count + most recent timestamp.
BANNER_TOTAL=$(gc "Alpaca Paper Trading bridge initialized" "$LOG")
BANNER_LAST=$(grep "Alpaca Paper Trading bridge initialized" "$LOG" 2>/dev/null | tail -1 | grep -oE '\[[0-9]{2}:[0-9]{2}:[0-9]{2}\]' | head -1)
echo "  bridge banners total in log: $BANNER_TOTAL  — most recent: ${BANNER_LAST:-<none>}"
echo "    expect 1 per process restart (HM-BK + HM-BK-residual). Many in succession = regression."

# $ATH error count today — HM-BL validation (delisted symbol memoization)
ATH_TOTAL=$(gc '\$ATH' "$ERR")
echo "  \$ATH errors total in error log: $ATH_TOTAL  — was 180 at HM-BL ship; should stay flat"

# HM-BD.F-audit Tier-1 fire indicators (rare — silent when healthy)
TIER1_HITS=$(tail -500 "$ERR" 2>/dev/null | grep -ciE "Ollama unload [a-z0-9_-]+:|signal-center post failed|alpaca-mirror snapshot failed|record_signal " | tr -d ' ')
TIER1_HITS=${TIER1_HITS:-0}
echo "  HM-BD.F-audit Tier-1 fires (tail -500): $TIER1_HITS  — 0 is typical (silent until first real failure)"
if [ "$TIER1_HITS" -gt 0 ] 2>/dev/null; then
  tail -500 "$ERR" 2>/dev/null | grep -iE "Ollama unload [a-z0-9_-]+:|signal-center post failed|alpaca-mirror snapshot failed|record_signal " | tail -3 | sed 's/^/    /'
fi

# Polygon fallback count today — HM-CB validation
POLY_FB_TOTAL=$(gc "HM-CB Polygon candles fallback" "$LOG")
echo "  Polygon fallbacks in log: $POLY_FB_TOTAL  — 0 ideal; <50 acceptable; >100 investigate"

echo ""
echo "── 🧠 Memory (HM-BHBI metric: Free %, not Swap %) ──"
if command -v memory_pressure >/dev/null 2>&1; then
  MP_OUT=$(memory_pressure 2>/dev/null)
  # macOS memory_pressure: "System-wide memory free percentage: 78%"
  FREE_PCT=$(echo "$MP_OUT" | grep -oE "free percentage: [0-9]+%" | grep -oE "[0-9]+" | head -1)
  if [ -n "$FREE_PCT" ]; then
    echo "  free: ${FREE_PCT}%  (healthy >=20%, warn 10-20%, critical <10%)"
    if [ "$FREE_PCT" -lt 10 ]; then
      echo "  🔴 CRITICAL — investigate process RAM usage"
    elif [ "$FREE_PCT" -lt 20 ]; then
      echo "  🟡 WARN — check Ollama models / dashboard worker / scanners"
    fi
  else
    echo "  $MP_OUT" | tail -3
  fi
else
  echo "  memory_pressure not found (non-macOS?) — fallback: vm_stat"
  vm_stat 2>/dev/null | head -5
fi

echo ""
echo "── 📜 trader.log freshness + last 5 ──"
if [ -f "$LOG" ]; then
  LAST_MOD=$(stat -f "%Sm" -t "%Y-%m-%d %H:%M:%S" "$LOG" 2>/dev/null)
  echo "  last modified: $LAST_MOD"
  echo "  last 5 lines:"
  tail -5 "$LOG" | sed 's/^/    /'
else
  echo "  ❌ $LOG not found"
fi

echo ""
echo "── 🐶 Watchdog (last 2 cycles) ──"
if [ -f "$WDOG" ]; then
  tail -2 "$WDOG" | sed 's/^/  /'
else
  echo "  $WDOG not found"
fi

echo ""
echo "── Done. ──"
