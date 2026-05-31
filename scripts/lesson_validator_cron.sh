#!/bin/zsh
# HM-LESSON-VALIDATION — daily shadow run (2026-05-31). Re-scans decision_audit forward so
# lesson verdicts mature (provisional → harmful/helpful) as conditions recur. SHADOW-ONLY:
# logs would-be salience to lesson_validation_shadow, NEVER touches agent_memory. Reboot-
# survivable cron (the banked lesson). cd here because cron's cwd is $HOME.
cd /Users/bigmac/autonomous-trader || exit 1
LOG=logs/lesson_validator.log
echo "[$(date '+%Y-%m-%d %H:%M:%S')] lesson_validator shadow run" >> "$LOG"
./venv/bin/python3 -c "from engine.lesson_validator import run_validation; r=run_validation(); print('verdicts', r['verdicts'])" >> "$LOG" 2>&1
