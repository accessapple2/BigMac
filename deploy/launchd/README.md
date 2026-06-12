# deploy/launchd — system LaunchDaemons

## com.trademinds.cloudflared.plist
Canonical KeepAlive supervisor for the Cloudflare tunnel that fronts
`bridge.ollietrades.com → http://localhost:8080`. Installed 2026-06-12
(HM-CLOUDFLARED-CONSOLIDATE) to replace an **unsupervised orphan** connector
(a bare `cloudflared` process started once via the `@reboot` wrapper, with no
respawn path — a reboot or crash would have dropped the bridge with nothing to
restart it).

**Why a LaunchDaemon (not a LaunchAgent / @reboot wrapper):** gui LaunchAgents
die on an SSH-only reboot (no Aqua session) — the documented reason the old
`@reboot` cron wrapper existed. A *system-domain* LaunchDaemon is not
session-bound: it survives SSH-only reboots (RunAtLoad) **and** process crashes
(KeepAlive). Runs as `UserName=bigmac` so it can read the `~/.cloudflared`
credentials (mode 0400, owned by bigmac); config paths are absolute.

### Install / re-install (requires sudo)
```
sudo cp deploy/launchd/com.trademinds.cloudflared.plist /Library/LaunchDaemons/com.trademinds.cloudflared.plist
sudo chown root:wheel /Library/LaunchDaemons/com.trademinds.cloudflared.plist
sudo chmod 644 /Library/LaunchDaemons/com.trademinds.cloudflared.plist
sudo launchctl bootstrap system /Library/LaunchDaemons/com.trademinds.cloudflared.plist
```
Uninstall: `sudo launchctl bootout system/com.trademinds.cloudflared`

### Verify
```
pgrep -x cloudflared                              # exactly one process, PPID 1
# KeepAlive test: kill it, confirm respawn within ~2s
kill $(pgrep -x cloudflared); sleep 3; pgrep -x cloudflared
curl -s -o /dev/null -w '%{http_code}\n' https://bridge.ollietrades.com/api/status   # 401 = origin app healthy through the tunnel
```

### Notes
- Binary: `/opt/homebrew/bin/cloudflared` (brew; was 2026.6.0 at install).
- Retired by this migration: the dead `homebrew.mxcl.cloudflared` brew service
  (latent duplicate-connector / 502 risk) and the dormant
  `~/Library/LaunchAgents/com.trademinds.tunnel.plist`.
- **Cloudflare Access** on `bridge.ollietrades.com` is a separate edge-side
  concern (Zero Trust dashboard), independent of this connector. As of
  2026-06-12 Access was NOT active on the zone (`/cdn-cgi/access/*` → 404).
