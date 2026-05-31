#!/bin/zsh
# HM-HOLLY-NIGHTLY-CRON (2026-05-30) — run Holly's nightly engines via cron, NOT the
# in-process schedule.every() in main.py (which dies on every trader restart → the
# 8-day stall: last good run 2026-05-22, missed every restart/midnight since).
#
# Runs BOTH engines under .venv-backtest (vectorbt installed there, not the live .venv):
#   1. run_holly_nightly  (DAILY swing "Holly-lite")   → holly_winning_strategies
#   2. run_holly_intraday (INTRADAY-flat 5min, faithful)→ holly_intraday_winners
# Both self-contained, both fail LOUD (NTFY + status=error) if vectorbt is missing —
# no more silent 8-day death. The intraday module load_dotenv's .env itself so the
# Polygon key is present under cron's standalone environment.
#
# Reboot-survival pattern (schwab_csv_watcher, git_push_health_check): cron survives
# restarts; the in-process scheduler does not. KEEPS ALL DATA (append-only per run_date).
#
# Installed crontab: 0 0 * * 1-5 /Users/bigmac/autonomous-trader/scripts/holly_nightly_cron.sh
# (midnight AZ, weekdays)
cd /Users/bigmac/autonomous-trader || exit 1
LOG=logs/holly_nightly_cron.log
PY=./.venv-backtest/bin/python3

echo "[$(date '+%Y-%m-%d %H:%M:%S')] holly_nightly_cron start (.venv-backtest)" >> "$LOG"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] -- DAILY (Holly-lite) --" >> "$LOG"
"$PY" -c "
from engine.holly_nightly_backtest import run_holly_nightly
r = run_holly_nightly()
print('daily status=%s runs=%s top10=%s' % (r.get('status'), r.get('total_runs'), len(r.get('top_10', []))))
" >> "$LOG" 2>&1

echo "[$(date '+%Y-%m-%d %H:%M:%S')] -- INTRADAY (faithful, 5min EOD-flat) --" >> "$LOG"
"$PY" -m engine.holly_intraday >> "$LOG" 2>&1

echo "[$(date '+%Y-%m-%d %H:%M:%S')] holly_nightly_cron done" >> "$LOG"
