#!/bin/zsh
# HM-DEJAVU — incremental recall-corpus refresh wrapper.
# Embeds only closed trades new since the last run (cheap), keeping recall_corpus / vec_trades_bge
# from rotting. Runs in .venv-recall (the ONLY venv with sqlite_vec; the live .venv must not get it).
# NTFYs ollietrades-admin on failure so a silent rot is caught — an independent rot-catcher, since
# "no alarm == healthy" is false (per reboot-survival doctrine).
set -u
ROOT="/Users/bigmac/autonomous-trader"
LOG="$ROOT/logs/recall_refresh.log"
cd "$ROOT" || exit 1

echo "[$(date '+%Y-%m-%dT%H:%M:%S%z')] recall_refresh start" >> "$LOG"
"$ROOT/.venv-recall/bin/python" "$ROOT/scripts/recall_refresh.py" >> "$LOG" 2>&1
rc=$?
echo "[$(date '+%Y-%m-%dT%H:%M:%S%z')] recall_refresh exit=$rc" >> "$LOG"

if [ $rc -ne 0 ]; then
    # HM-NTFY-MIGRATE-2026-08-30: was a raw curl straight to ntfy.sh, bypassing
    # the hardened engine.alert_channels sender (DECOM-SILENCE guard, Pushover
    # RED_ALERT lane, per-alert-type rate limit, 429 backoff) -- pre-dates the
    # 2026-08-28 429-remediation pass. Uses the main .venv (not .venv-recall --
    # engine.alert_channels needs no sqlite_vec) purely for the notification.
    RECALL_RC="$rc" RECALL_TAIL="$(/usr/bin/tail -n 8 "$LOG" 2>/dev/null)" \
        "$ROOT/.venv/bin/python3" -c "
import os, sys
sys.path.insert(0, '.')
from engine.alert_channels import send_alert, AlertLevel
rc = os.environ['RECALL_RC']
tail = os.environ.get('RECALL_TAIL', '')
send_alert(tail, AlertLevel.WARNING, 'recall_refresh_failed',
           title=f'HM-DEJAVU recall_refresh FAILED (rc={rc})')
" >> "$LOG" 2>&1
fi
exit $rc
