#!/usr/bin/env bash
# ============================================================================
# USS TradeMinds — Pre-Market Scout Scan
#
# Runs every weekday at 6:00 AM MST (before 7:30 AM MST / 9:30 AM ET open).
# Scans for opportunities, runs full pipeline on high-conviction finds.
#
# Usage: ./premarket-scan.sh
# Managed by: com.trademinds.premarket launchd agent
# ============================================================================

set -euo pipefail

export TRADEMINDS_DIR="${TRADEMINDS_DIR:-$HOME/autonomous-trader}"
export TRADEMINDS_DB="${TRADEMINDS_DB:-$TRADEMINDS_DIR/data/trader.db}"
VENV="$TRADEMINDS_DIR/venv/bin/python3"
LOG="$TRADEMINDS_DIR/logs/premarket.log"
LOCK="/tmp/trademinds-ollama.lock"

mkdir -p "$TRADEMINDS_DIR/logs"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "$LOG"; }

log "========================================"
log "PRE-MARKET SCAN STARTING"
log "========================================"

# Clean stale Ollama lock
if [ -f "$LOCK" ]; then
    log "Removing stale Ollama lock"
    rm -f "$LOCK"
fi

# Verify Ollama is running
if ! curl -s http://localhost:11434/api/tags > /dev/null 2>&1; then
    log "[FAIL] Ollama not running. Aborting."
    exit 1
fi

# Verify crew server is up — if not, attempt start and wait; exit cleanly (not error) if unavailable
_crew_up=false
if curl -s --max-time 5 http://localhost:8000/health > /dev/null 2>&1; then
    _crew_up=true
else
    log "[WARN] Crew server not responding on :8000, attempting start..."
    launchctl start com.trademinds.crew 2>/dev/null || true
    sleep 10
    if curl -s --max-time 5 http://localhost:8000/health > /dev/null 2>&1; then
        _crew_up=true
    fi
fi

if [ "$_crew_up" = "false" ]; then
    log "[SKIP] Crew server unavailable after start attempt — running DB-only conviction check, skipping Scout API"
fi

log "Running Scout scan: pre-market movers, earnings, overnight news"

# Run the Scout via API — skip gracefully (exit 0) if crew is down
SCOUT_RESULT=""
SCOUT_STATUS="skipped"
if [ "$_crew_up" = "true" ]; then
    SCOUT_RESULT=$(curl -s -X POST "http://localhost:8000/api/crew/scout" \
        -H "Content-Type: application/json" \
        -d '{"focus_area": "pre-market movers, earnings, overnight news, congress trades, gap up/down stocks, sector rotation signals"}' \
        --max-time 300 2>&1) || { log "[WARN] Scout curl failed (crew may have died) — continuing with DB scan"; SCOUT_RESULT=""; }
fi

if [ -n "$SCOUT_RESULT" ]; then
    SCOUT_STATUS=$(echo "$SCOUT_RESULT" | "$VENV" -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(d.get('status', 'unknown'))
except:
    print('error')
" 2>/dev/null)

    SCOUT_DURATION=$(echo "$SCOUT_RESULT" | "$VENV" -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(d.get('duration', 0))
except:
    print(0)
" 2>/dev/null)

    log "Scout finished: status=$SCOUT_STATUS duration=${SCOUT_DURATION}s"

    if [ "$SCOUT_STATUS" != "completed" ]; then
        log "[WARN] Scout API returned non-completed status ($SCOUT_STATUS) — continuing with DB scan"
    fi
else
    log "Scout API skipped (crew unavailable) — proceeding with DB-only conviction check"
fi

# Save scout result to log
log "Scout result:"
echo "$SCOUT_RESULT" | "$VENV" -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(json.dumps(d, indent=2))
except:
    print(sys.stdin.read())
" >> "$LOG" 2>/dev/null

# Check for high-conviction opportunities and run full pipeline
log "Checking for high-conviction opportunities..."

"$VENV" << 'PYEOF' >> "$LOG" 2>&1
import json
import os
import sys
import sqlite3
from datetime import datetime

DB = os.environ.get("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))

# Read latest scout run result
conn = sqlite3.connect(DB, timeout=30)
conn.row_factory = sqlite3.Row

# Check if scout found anything worth running the full pipeline on
# Look at recent high-confidence signals in the last hour
cutoff = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
rows = conn.execute("""
    SELECT symbol, signal, confidence, reasoning
    FROM signals
    WHERE confidence >= 0.7
    AND created_at > datetime('now', '-1 hour')
    AND signal IN ('BUY', 'BUY_CALL', 'BUY_PUT', 'STRONG_BUY')
    ORDER BY confidence DESC
    LIMIT 10
""").fetchall()

# Also check universe scan for high-score pre-market movers
universe = conn.execute("""
    SELECT ticker, score, signals, gap_pct
    FROM universe_scan
    WHERE score >= 60
    AND scan_date = date('now')
    ORDER BY score DESC
    LIMIT 5
""").fetchall()

# Check discoveries for fresh catalysts
discoveries = conn.execute("""
    SELECT symbol, trigger_type, change_pct, short_float
    FROM discoveries
    WHERE detected_at > datetime('now', '-6 hours')
    AND (trigger_type IN ('short_squeeze', 'unusual_volume', 'momentum_breakout')
         OR abs(change_pct) > 3)
    ORDER BY abs(change_pct) DESC
    LIMIT 5
""").fetchall()

high_conviction = []

for r in rows:
    if r["confidence"] >= 0.7:
        high_conviction.append({
            "symbol": r["symbol"],
            "signal": r["signal"],
            "confidence": r["confidence"],
            "source": "ai_signals",
        })

for u in universe:
    if u["score"] >= 70:
        high_conviction.append({
            "symbol": u["ticker"],
            "score": u["score"],
            "gap_pct": u["gap_pct"],
            "source": "universe_scan",
        })

for d in discoveries:
    if d["short_float"] and d["short_float"] > 20:
        high_conviction.append({
            "symbol": d["symbol"],
            "trigger": d["trigger_type"],
            "change_pct": d["change_pct"],
            "source": "discoveries",
        })

conn.close()

print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Found {len(high_conviction)} high-conviction opportunities")
for h in high_conviction[:5]:
    print(f"  -> {h}")

# Write marker file so the bash script knows whether to run full pipeline
with open("/tmp/trademinds-premarket-conviction.json", "w") as f:
    json.dump({"count": len(high_conviction), "opportunities": high_conviction[:5]}, f)
PYEOF

# Read conviction check result
CONVICTION_COUNT=$("$VENV" -c "
import json
try:
    with open('/tmp/trademinds-premarket-conviction.json') as f:
        d = json.load(f)
    print(d.get('count', 0))
except:
    print(0)
" 2>/dev/null)

log "High-conviction opportunities found: $CONVICTION_COUNT"

if [ "$CONVICTION_COUNT" -gt 0 ]; then
    log "========================================"
    log "RUNNING FULL PIPELINE (high conviction detected)"
    log "========================================"

    if [ "$_crew_up" = "true" ]; then
        # Run full pipeline for stocks
        log "Pipeline 1/2: Stock opportunities..."
        PIPE_RESULT=$(curl -s -X POST "http://localhost:8000/api/crew/run" \
            -H "Content-Type: application/json" \
            -d '{"focus_area": "pre-market high-conviction stock opportunities from overnight scan", "target_asset_class": "stock", "target_portfolio_id": 1, "trigger": "premarket_auto"}' \
            --max-time 600 2>&1) || { log "[WARN] Stock pipeline curl failed"; PIPE_RESULT=""; }
        log "Stock pipeline launched: $(echo "$PIPE_RESULT" | head -c 200)"

        # Run full pipeline for options if enough conviction
        if [ "$CONVICTION_COUNT" -ge 3 ]; then
            log "Pipeline 2/2: Options opportunities (3+ high conviction)..."
            PIPE_RESULT2=$(curl -s -X POST "http://localhost:8000/api/crew/run" \
                -H "Content-Type: application/json" \
                -d '{"focus_area": "pre-market high-conviction options plays from overnight catalysts", "target_asset_class": "option", "target_portfolio_id": 1, "trigger": "premarket_auto"}' \
                --max-time 600 2>&1) || { log "[WARN] Options pipeline curl failed"; PIPE_RESULT2=""; }
            log "Options pipeline launched: $(echo "$PIPE_RESULT2" | head -c 200)"
        fi
    else
        log "Pipeline skipped — crew server unavailable (DB conviction check still ran)"
    fi
else
    log "No high-conviction opportunities. Scout-only run complete."
fi

# Cleanup
rm -f /tmp/trademinds-premarket-conviction.json

log "========================================"
log "PRE-MARKET SCAN COMPLETE"
log "========================================"
