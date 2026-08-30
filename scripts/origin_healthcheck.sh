#!/bin/bash
# Origin HTTP healthcheck — cron-based, checks the actual HTTP response,
# not just process liveness (a wedged-but-alive process passes pgrep but
# fails this — exactly today's SwingDesk incident: process alive and
# LISTENing the whole time, just not completing requests). Restarts +
# NTFYs on failure. Mirrors HM-TRADER-KEEPALIVE's NTFY-on-revive pattern
# for the two services that don't have an equivalent yet.
cd /Users/bigmac/autonomous-trader || exit 1
LOG=logs/origin_healthcheck.log
NTFY_TOPIC="${NTFY_ADMIN_TOPIC:-ollietrades-admin}"

check_and_restart() {
  local name="$1" url="$2" restart_script="$3"
  if ! curl -sf --max-time 8 "$url" >/dev/null 2>&1; then
    echo "$(date): $name failed healthcheck ($url) — restarting" >> "$LOG"
    bash "$restart_script" >> "$LOG" 2>&1
    # HM-NTFY-MIGRATE-2026-08-30: was a raw curl straight to ntfy.sh, bypassing
    # the hardened engine.alert_channels sender (DECOM-SILENCE guard, Pushover
    # RED_ALERT lane, per-alert-type rate limit, 429 backoff) -- pre-dates the
    # 2026-08-28 429-remediation pass. Shells out to the real sender instead.
    ORIGIN_HC_NAME="$name" ORIGIN_HC_URL="$url" ORIGIN_HC_SCRIPT="$restart_script" \
      .venv/bin/python3 -c "
import os, sys
sys.path.insert(0, '.')
from engine.alert_channels import send_alert, AlertLevel
name = os.environ['ORIGIN_HC_NAME']
url = os.environ['ORIGIN_HC_URL']
script = os.environ['ORIGIN_HC_SCRIPT']
send_alert(
    f'Healthcheck to {url} failed. Restart script {script} fired.',
    AlertLevel.WARNING,
    'origin_healthcheck_restart',
    title=f'{name} restarted (failed healthcheck)',
)
" >> "$LOG" 2>&1
  fi
}

check_and_restart "main.py (bridge)" "http://localhost:8080/api/status" "scripts/trader_restart.sh"
check_and_restart "signal-center"    "http://localhost:9000/api/health" "scripts/signal_center_restart.sh"
check_and_restart "swingdesk"        "http://localhost:8889/api/health" "scripts/swingdesk_restart.sh"
check_and_restart "status_page"      "http://localhost:8090/"           "scripts/status_page_restart.sh"
# HM-DEPARTURE-HARDENING-P1-ITEM-2 2026-07-10 — tour_api had no healthcheck
# coverage at all (confirmed via docs/XO_BACKLOG.md XO-DEPARTURE-HARDENING
# status check). tour_api_restart.sh handles its self-respawn-loop
# architecture correctly (kills the wrapper too, not just the process).
check_and_restart "tour-api"         "http://localhost:8088/api/tour/health" "scripts/tour_api_restart.sh"

# HM-SENTINEL-HEARTBEAT-2026-08-30: silent-on-success meant a healthy run
# left the crontab-redirect log (origin_healthcheck_cron.log) completely
# untouched -- hm_ops_sentinel's check_cron_missing_scripts reads that
# file's last line and had no way to tell "silently fine" from "still
# broken with stale pre-fix error content." Unconditional one-line
# heartbeat to stdout (captured by the crontab's own >> redirect, not the
# internal $LOG) so every real tick leaves fresh, unambiguous evidence.
echo "$(date '+%Y-%m-%dT%H:%M:%S%z') origin_healthcheck tick OK"
