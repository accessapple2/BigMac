#!/bin/bash
# show_error_log.sh — Open TradeMinds error log in Terminal
# Called by the Dr. Crusher health check "Show" notification button

LOG="$HOME/autonomous-trader/logs/trader_error.log"

if [ -f "$LOG" ]; then
    open -a Terminal "$LOG"
else
    # Fallback: open the logs directory
    open "$HOME/autonomous-trader/logs/"
fi
