#!/bin/zsh
# HM-EXTERNAL-INTEL — daily follow-TI shadow snapshot (2026-05-31). Tracks whether following
# Andy Lindloff's TI swing picks makes money, over time as picks accumulate. Reboot-survivable
# cron (the banked lesson). TRACKED, NOT auto-traded (parallel ghost tracker). LIVE venv.
cd /Users/bigmac/autonomous-trader || exit 1
LOG=logs/ti_shadow_snapshot.log
echo "[$(date '+%Y-%m-%d %H:%M:%S')] ti_shadow_snapshot start" >> "$LOG"
./venv/bin/python3 -c "from engine.external_intel import ti_shadow_snapshot; print(ti_shadow_snapshot())" >> "$LOG" 2>&1
echo "[$(date '+%Y-%m-%d %H:%M:%S')] done" >> "$LOG"
