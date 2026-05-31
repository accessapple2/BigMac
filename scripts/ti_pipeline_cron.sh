#!/bin/zsh
# HM-EXTERNAL-INTEL — TI email pipeline (revived 2026-05-31). Poll Gmail → fetch new .eml →
# parse → external_picks (clean store) + intelligence_feed (raw archive). Reboot-survivable
# cron so it doesn't go dormant again (it last ran 2026-05-23 then died — the recurring disease).
#
# Poller marks messages \Seen + writes .eml; watcher parses each → external_picks via the
# repointed ti_picks_parser. LIVE venv. Idempotent. Creds come from .env (never logged).
cd /Users/bigmac/autonomous-trader || exit 1
LOG=logs/ti_pipeline_cron.log
PY=./venv/bin/python3
echo "[$(date '+%Y-%m-%d %H:%M:%S')] ti_pipeline start" >> "$LOG"
"$PY" scripts/ti_email_poller.py --once >> "$LOG" 2>&1
/bin/bash scripts/ti_picks_watcher.sh --once >> "$LOG" 2>&1
echo "[$(date '+%Y-%m-%d %H:%M:%S')] ti_pipeline done" >> "$LOG"
