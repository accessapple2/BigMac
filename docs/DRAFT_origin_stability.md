# DRAFT — Origin Stability (cron-based, per repo doctrine) — FOR SIGN-OFF, NOT APPLIED

Per Admiral direction 2026-07-02: cron-based, explicitly **not** systemd/launchd naming or
mechanism (CLAUDE.md's own "LaunchAgent Reboot Lifecycle" section documents why —
launchd/LaunchDaemon bootstrap fails on this box over SSH-only sessions).

## Gap this closes

`main.py` (:8080) already has a real cron-based keepalive
(`HM-TRADER-KEEPALIVE`, `*/5 * * * *`, `pgrep -f main.py` + NTFY on revive).
**`signal-center` (:9000) and `swingdesk` (:8889) have no equivalent** — only
`@reboot` starts, nothing that catches a mid-session failure.

More importantly: today's actual SwingDesk incident (502, fd exhaustion)
would **not** have been caught by a `pgrep`-style liveness check — the
process was still alive and still `LISTEN`ing the whole time, just wedged
(TCP accepts, then resets on the actual HTTP request). A process-liveness
check is the wrong instrument for that failure mode. This draft uses an
HTTP-level healthcheck instead, which would have caught it.

## Component A — HTTP healthcheck + auto-restart (new script + cron)

New file `scripts/origin_healthcheck.sh`:

```bash
#!/bin/bash
# Origin HTTP healthcheck — cron-based, checks the actual HTTP response,
# not just process liveness (a wedged-but-alive process passes pgrep but
# fails this). Restarts + NTFYs on failure. Mirrors HM-TRADER-KEEPALIVE's
# NTFY-on-revive pattern for the two services that don't have one yet.
cd /Users/bigmac/autonomous-trader
LOG=logs/origin_healthcheck.log
NTFY_TOPIC="${NTFY_ADMIN_TOPIC:-ollietrades-admin}"

check_and_restart() {
  local name="$1" url="$2" restart_script="$3"
  if ! curl -sf --max-time 8 "$url" >/dev/null 2>&1; then
    echo "$(date): $name failed healthcheck ($url) — restarting" >> "$LOG"
    bash "$restart_script" >> "$LOG" 2>&1
    curl -s -m 10 -H "Title: $name restarted (failed healthcheck)" \
      -d "Healthcheck to $url failed. Restart script $restart_script fired at $(date)." \
      "https://ntfy.sh/$NTFY_TOPIC" >/dev/null 2>&1
  fi
}

check_and_restart "main.py (bridge)" "http://localhost:8080/api/status" "scripts/trader_restart.sh"
check_and_restart "signal-center"    "http://localhost:9000/api/health" "scripts/signal_center_restart.sh"
check_and_restart "swingdesk"        "http://localhost:8889/api/health" "scripts/swingdesk_restart.sh"
```

**New file needed:** `scripts/signal_center_restart.sh` (doesn't exist yet —
only `signal_center_reboot_start.sh`, which is `@reboot`-only and has a 35s
sleep + "already running" guard, wrong shape for a periodic healthcheck
restart). Mirrors `swingdesk_restart.sh`'s kill+relaunch shape exactly:

```bash
#!/bin/bash
# Manual/healthcheck restart for signal-center on :9000.
cd /Users/bigmac/autonomous-trader
pkill -f "signal-center/server.py" 2>/dev/null
PORT_PIDS=$(lsof -ti :9000 2>/dev/null); [ -n "$PORT_PIDS" ] && kill $PORT_PIDS 2>/dev/null
sleep 2
nohup ./venv/bin/python3 signal-center/server.py >> logs/signal-center.log 2>> logs/signal-center.log &
echo "$(date): signal-center restarted PID $!" | tee -a logs/signal_center_reboot.log
```

**Cron entry** (goes in the existing "Daemon Graveyard" / HM-HARDEN block style):
```
# HM-DIRECTIVE-2026-07-02 — origin HTTP healthcheck (bridge/signal/swingdesk), cron per
# repo doctrine (launchd/systemd bootstrap fails over SSH-only sessions on this box).
*/5 * * * * /bin/bash /Users/bigmac/autonomous-trader/scripts/origin_healthcheck.sh >> /Users/bigmac/autonomous-trader/logs/origin_healthcheck_cron.log 2>&1
```

Note: this does NOT replace `HM-TRADER-KEEPALIVE` (the existing `pgrep`-based
one for main.py) — keeping both is fine and gives two independent detection
angles (dead process vs. wedged-but-alive process) for the same service.

## Component B — external uptime monitoring (needs an account only the Admiral can create)

Per the "Alarms must not share a failure mode with what they watch" doctrine
already in CLAUDE.md (2026-05-28 entry) — a cron job on *this box* checking
*this box*'s services shares a failure mode with the thing it's watching
(if the box is down, network is down, or cron itself is wedged, the alarm
is silent too). Real external monitoring needs to run somewhere else.

**Recommended (free tier, no card required):** [UptimeRobot](https://uptimerobot.com)
free plan — 50 monitors, 5-minute interval, supports webhook alerts (can
point at ntfy.sh directly, no email needed). Three HTTP(s) monitors:
- `https://bridge.ollietrades.com/` (expect 200 or the CF Access 302, not 5xx)
- `https://signal.ollietrades.com/`
- `https://swingdesk.ollietrades.com/`

This requires creating a third-party account — not something I'll do
myself (falls under "Creating accounts" in my prohibited-actions list).
Admiral sets it up; I can help wire the webhook → ntfy bridge once the
account exists if useful.

## Verification plan (once applied)
1. `bash scripts/origin_healthcheck.sh` manually once — confirm all 3 pass silently (no NTFY, no restart) against currently-healthy services.
2. Kill one service's port manually (e.g. `kill $(lsof -ti :8889)`), wait for the next 5-min cron tick, confirm it auto-restarts + NTFYs.
3. Confirm the existing `HM-TRADER-KEEPALIVE` cron line is untouched (both mechanisms coexist).
