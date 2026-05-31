#!/bin/zsh
# HM-HOLLY-NIGHTLY-CRON (2026-05-30) — run Holly's nightly backtest+optimize via cron,
# NOT the in-process schedule.every() in main.py (which dies on every trader restart →
# the 8-day stall: last good run 2026-05-22, missed every restart/midnight since).
#
# RUNS UNDER .venv-backtest — vectorbt (the backtest engine) is installed ONLY there,
# NOT in the live trader's .venv (heavy numba/numpy dep, conflicts on py3.14). The
# nightly is a self-contained backtest, so running it under .venv-backtest is correct
# and avoids polluting the live runtime. run_holly_nightly() itself fails LOUD (NTFY +
# status=error) if vectorbt is ever missing — no more silent 8-day death.
#
# Mirrors the established reboot-survival pattern (schwab_csv_watcher, git_push_health_check):
# cron survives restarts; the in-process scheduler does not. KEEPS ALL DATA (append-only
# per run_date in data/backtest.db holly_winning_strategies + holly_backtests).
#
# Install (after Admiral approval):
#   crontab entry: 0 0 * * 1-5 /Users/bigmac/autonomous-trader/scripts/holly_nightly_cron.sh
#   (midnight AZ, weekdays — matches the original intended cadence)
cd /Users/bigmac/autonomous-trader || exit 1
LOG=logs/holly_nightly_cron.log
echo "[$(date '+%Y-%m-%d %H:%M:%S')] holly_nightly_cron start (.venv-backtest)" >> "$LOG"
./.venv-backtest/bin/python3 -c "
from engine.holly_nightly_backtest import run_holly_nightly
r = run_holly_nightly()
print('status=%s runs=%s top10=%s' % (r.get('status'), r.get('total_runs'), len(r.get('top_10', []))))
" >> "$LOG" 2>&1
RC=$?
echo "[$(date '+%Y-%m-%d %H:%M:%S')] holly_nightly_cron done rc=$RC" >> "$LOG"
