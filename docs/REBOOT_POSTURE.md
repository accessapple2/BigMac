# Reboot Posture

> Established 2026-07-05 (HM-REBOOT-INCIDENT-CLOSEOUT), following a real
> planned power-down/reboot of bigmac that surfaced a gap: this is the box's
> auto-start posture as *proven by an actual reboot*, not as documented intent.

## Boot-inventory table (verified 2026-07-05 reboot, 10:20:56 boot)

| Service | Port | Mechanism | Result this boot |
|---|---|---|---|
| Trader (`main.py`) | 8080 | `@reboot` cron → `trader_restart.sh` | ✅ up |
| Signal Center | 9000 | `@reboot` cron → `signal_center_reboot_start.sh` (zsh) | ✅ up |
| SwingDesk/O-Tasty | 8889 | **LaunchDaemon** `com.trademinds.swingdesk.plist` (system domain, RunAtLoad+KeepAlive) | ✅ up |
| cloudflared tunnel | — | **LaunchDaemon** `com.trademinds.cloudflared.plist` (system domain, RunAtLoad+KeepAlive) | ✅ up (redundant `@reboot` cron wrapper no-op'd "already running") |
| Tour API | 8088 | `@reboot` cron → `tour_api_start.sh` | ✅ up |
| Status Page | 8090 | `@reboot` cron → `status_page_reboot_start.sh` (bash) | ❌ died mid-sleep, never started — manually recovered same session; **promoted to LaunchDaemon same day, see below** |
| riker-synthesis | — | cron `*/10 * * * *` (a `~/Library/LaunchAgents/com.ollietrades.riker-synthesis.plist` also exists on disk but is confirmed NOT bootstrappable — see below) | ✅ cron-only, no double-fire |

## Key posture facts

1. **Only two services had crash-respawn protection going into this reboot:**
   cloudflared and SwingDesk, both via LaunchDaemon (system domain) with
   `RunAtLoad` + `KeepAlive`. Everything else was fire-once-at-boot cron with
   no supervisor except the trader's separate `watchdog_supervisor.sh` +
   `*/5` keepalive cron.
2. **Status Page had zero monitoring going into this reboot** —
   `origin_healthcheck.sh` (`*/5`) only checked bridge/signal/swingdesk, and
   there was no keepalive cron for it either. That's why the gap was silent
   until it was noticed externally: nothing in the fleet would have caught it.
3. **Any `gui/501` LaunchAgent plist on this box is dead weight** under this
   reboot pattern (SSH-only management, no logged-in Aqua session).
   `launchctl print gui/501/<label>` fails with `125: Domain does not support
   specified action` for every such plist probed. Confirmed 3-for-3 across
   `com.trademinds.trader` / `com.trademinds.tunnel` (2026-05-23, see
   `docs/runbooks/reboot-lifecycle.md`) and `com.ollietrades.riker-synthesis`
   (2026-07-05, this incident). **Rule: a service that must survive reboot on
   this box needs either cron `@reboot` or a system-domain LaunchDaemon —
   never a bare `gui/$UID` LaunchAgent.**
4. **`status.ollietrades.com`'s DNS route lives in Cloudflare's
   dashboard-managed tunnel config, not `~/.cloudflared/config.yml`.** The
   local ingress file only lists bridge/signal/swingdesk/tour + a catch-all
   404 — status_page's hostname was added via the Zero Trust dashboard
   directly (see `docs/HANDOFF.md` #25) and is invisible if you only check
   the local config file.

## Status Page — LaunchDaemon promotion (2026-07-05, same-day closeout)

Following the incident, `status_page` was promoted to the same pattern as
SwingDesk/cloudflared:

- `com.trademinds.statuspage.plist` (system domain, `UserName=bigmac`,
  `RunAtLoad`+`KeepAlive`, `ThrottleInterval=10`) installed to
  `/Library/LaunchDaemons/`.
- The fragile `@reboot` cron line removed from crontab.
- `scripts/status_page_reboot_start.sh` retired to `scripts/_archive/` (not
  deleted — same convention as every other retirement on this box).
- `scripts/status_page_restart.sh` added and wired into
  `origin_healthcheck.sh`'s `*/5` HTTP check — this catches the
  wedged-but-alive failure mode that KeepAlive alone cannot (KeepAlive only
  fires on process exit, not on a hung-but-listening process). The restart
  action is kill-only (mirrors `swingdesk_restart.sh`'s process ownership
  model): killing a `UserName=bigmac` LaunchDaemon child is a normal
  same-UID `kill`, no sudo required, and the root-owned `launchd` supervisor
  respawns it via `KeepAlive` within the throttle window.

| Service | Port | Mechanism | Result |
|---|---|---|---|
| Status Page | 8090 | **LaunchDaemon** `com.trademinds.statuspage.plist` (system domain, RunAtLoad+KeepAlive) + `origin_healthcheck.sh` HTTP check every 5min | ✅ promoted; kill-test verified respawn (see below) |

**Kill-test result (2026-07-05, verified end-to-end):**
- **Plain `pkill` (crash simulation):** killed PID 4220 → LaunchDaemon KeepAlive
  respawned a fresh process (PID 4220 → new PID) within ~1 second, port 8090
  rebound, `200` confirmed. Too fast for the `*/5` healthcheck cron to ever
  race against — this layer alone handles ordinary crashes.
- **`SIGSTOP` (wedged-but-alive simulation — the actual SwingDesk-incident
  failure mode this healthcheck line exists for):** stopped the process
  (state `T`), port stayed bound/LISTENing, `curl --max-time 5` correctly
  timed out (`000`, full 5s) rather than erroring fast — a faithful
  reproduction of "alive but not answering." Manually invoked the real
  `scripts/origin_healthcheck.sh`. It detected the failure, logged
  `status_page failed healthcheck (http://localhost:8090/) — restarting` to
  `logs/origin_healthcheck.log`, and ran `scripts/status_page_restart.sh`,
  which killed the wedged process. LaunchDaemon `KeepAlive` then respawned it
  automatically (new PID, port 8090 rebound). Final `curl` → `200`.
- **Conclusion:** the two layers are properly independent — KeepAlive catches
  crashes (fast, no cron round-trip needed); the healthcheck catches hangs
  that KeepAlive structurally cannot see, and its restart action (kill-only)
  correctly hands the actual respawn back to KeepAlive rather than racing it
  with a duplicate launch.

Related: `docs/runbooks/reboot-lifecycle.md`, `docs/DOCTRINE.md` (Doctrine
Lessons — "Alarms must not share a failure mode with what they watch").
