#!/bin/zsh
# HM-HOLLY-WORKS Stage 5 — live loop for holly-scanner (the_continuation + count_de_monet).
# STAGED, NOT YET INSTALLED — awaits Admiral eyes-on before going live-scheduled.
#
# Runs the validated works-set live: scan small-cap movers for fresh setups → buy →
# manage swing exits (per-strategy stop/target/max-hold). Internal $10k book, paper.
#
# LIVE venv (venv/, NOT .venv-backtest) — uses paper_trader/market_data. The loop trades
# EXACTLY as validated (same entries, selectivity gate, swing exits). buy() has its own
# market-closed gate, so off-hours runs no-op safely; the schedule restricts to market hrs.
#
# Reboot-survival: cron, NOT launchd (the banked lesson — launchd gui/$UID dies on
# SSH-only reboot; cron survives). Proposed crontab (install ONLY after eyes-on GO):
#   */15 13-20 * * 1-5 /Users/bigmac/autonomous-trader/scripts/holly_live_cron.sh
#   (every 15min, ~9:30-16:00 ET in UTC, weekdays)
#
# Run with --live to actually trade; default (no arg) is DRY-RUN (logs intent only).
cd /Users/bigmac/autonomous-trader || exit 1
LOG=logs/holly_live.log
PY=./venv/bin/python3
MODE="${1:---dry}"   # default dry-run; pass --live to trade

echo "[$(date '+%Y-%m-%d %H:%M:%S')] holly_live start ($MODE)" >> "$LOG"
if [ "$MODE" = "--live" ]; then
  "$PY" -m engine.holly_live --live >> "$LOG" 2>&1
else
  "$PY" -m engine.holly_live >> "$LOG" 2>&1
fi
echo "[$(date '+%Y-%m-%d %H:%M:%S')] holly_live done" >> "$LOG"
