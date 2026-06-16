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
    /usr/bin/curl -s -m 10 \
        -H "Title: HM-DEJAVU recall_refresh FAILED (rc=$rc)" \
        -d "$(/usr/bin/tail -n 8 "$LOG" 2>/dev/null)" \
        https://ntfy.sh/ollietrades-admin >/dev/null 2>&1
fi
exit $rc
