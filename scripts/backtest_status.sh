#!/bin/bash
# USS TradeMinds — Backtest Status Monitor
# Usage: bash scripts/backtest_status.sh [--resume-arena]

LOG=$(ls -t ~/autonomous-trader/logs/holodeck_backtest_*.log 2>/dev/null | head -1)
if [ -z "$LOG" ]; then echo "No backtest log found."; exit 1; fi

echo "=== BACKTEST STATUS: $(basename $LOG) ==="
echo ""

PID=$(pgrep -f "ollama_bulk_backtest" 2>/dev/null)
PAUSED=$(sqlite3 ~/autonomous-trader/data/trader.db "SELECT value FROM settings WHERE key='pause_all';" 2>/dev/null)

if [ -n "$PID" ]; then
    echo "STATUS:      🟢 RUNNING (PID $PID)"
    echo "ARENA:       ⏸  PAUSED (will resume when backtest completes)"
else
    echo "STATUS:      ✅ COMPLETED (or stopped)"
    if [ "$PAUSED" = "1" ]; then
        echo "ARENA:       ⚠️  Still paused — run with --resume-arena to restore"
        if [ "$1" = "--resume-arena" ]; then
            sqlite3 ~/autonomous-trader/data/trader.db "UPDATE settings SET value='0' WHERE key='pause_all';"
            echo "ARENA:       ▶️  RESUMED"
        fi
    else
        echo "ARENA:       ▶️  Running"
    fi
fi

echo ""
echo "--- LAST 30 LOG LINES ---"
tail -30 "$LOG" | grep -v "NotOpenSSL\|Warning\|urllib3"

echo ""
echo "--- DB: Run #7 progress ---"
sqlite3 ~/autonomous-trader/data/trader.db "
SELECT 
    res.player_id,
    COUNT(*) as tickers_done,
    ROUND(AVG(res.total_return_pct),1) || '%' as avg_return,
    ROUND(AVG(res.sharpe_ratio),2) as avg_sharpe
FROM backtest_results res
WHERE res.run_id = 7
GROUP BY res.player_id
ORDER BY avg_return DESC;
" 2>/dev/null
