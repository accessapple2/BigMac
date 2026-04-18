# OllieTrades — Systemd Units

Version-controlled systemd unit files for running OllieTrades on Ollie (G1 Pro, Ubuntu Linux). These replace the macOS launchd plists in `launchd/` and `~/Library/LaunchAgents/` on bigmac.

## Services (Phase 1 — 5 units)

| Unit | Purpose | Port | bigmac equivalent |
|------|---------|------|-------------------|
| `trademinds-dashboard.service` | Main FastAPI dashboard (`main.py`) — trader UI, API, scheduler | 8080 | `com.trademinds.trader.plist` |
| `trademinds-signal-center.service` | Signal aggregator (Flask, `signal-center/server.py`) | 9000 | `com.trademinds.signal-center.plist` |
| `trademinds-scanner.service` | Fast scanner daemon (`engine.fast_scanner --daemon`) | — | `com.trademinds.scanner.plist` |
| `trademinds-mcp.service` | HTTP/JSON-RPC bridge for Claude Code (`engine/mcp_server.py`) — 8 tools exposed | 8081 | `com.trademinds.mcp.plist` |
| `trademinds-watchdog.service` | HTTP/process health probes every 60s + ntfy.sh iPhone alerts (`watchdog.py`). Monitors dashboard, signal-center, ollama, cloudflared. **Does NOT monitor scanner/MCP** — follow-up refactor pending. | — | `com.trademinds.watchdog.plist` |

## Not yet ported (deferred to later phases)

- **cloudflared tunnel** — currently runs on bigmac, serves `bridge.ollietrades.com` → bigmac:8080. Deferred pending cloudflared install + config.yml copy on Ollie.
- **16 scheduled jobs** (premarket, caffeinate, healthcheck, crew, metals-sync, webull-sync, nightly-backtest, riker-synthesis, danelfin-update, ghost-trader, uhura, archer-briefing, etfregime, morningbriefing, optionsflow, ollama-keepalive) — will become systemd `.timer` + `.service` pairs in Phase 2. Three of these (etfregime, morningbriefing, optionsflow) point to an old `~/ollietrades/` path and need audit before porting.

## Install / Uninstall / Reinstall

All commands require sudo. Run from anywhere — scripts use absolute paths.

```bash
# Install (idempotent — safe to re-run after editing unit files)
sudo bash ~/autonomous-trader/systemd/install.sh

# Uninstall (stops, disables, and unlinks all trademinds-* units)
sudo bash ~/autonomous-trader/systemd/uninstall.sh

# Reinstall
sudo bash ~/autonomous-trader/systemd/uninstall.sh && \
sudo bash ~/autonomous-trader/systemd/install.sh
```

Install is a 2-step process: **(1)** symlinks `~/autonomous-trader/systemd/*.service` into `/etc/systemd/system/`, **(2)** runs `systemctl daemon-reload`. Services are NOT auto-started — enable them individually once you're ready.

## Service lifecycle commands

```bash
# Enable + start (equivalent to plist "load")
sudo systemctl enable --now trademinds-dashboard.service

# Stop / start / restart
sudo systemctl stop    trademinds-dashboard.service
sudo systemctl start   trademinds-dashboard.service
sudo systemctl restart trademinds-dashboard.service

# Disable (won't start on boot) vs enable (will)
sudo systemctl disable trademinds-dashboard.service
sudo systemctl enable  trademinds-dashboard.service

# Status (one-shot)
systemctl status trademinds-dashboard.service

# Is it running? Scripting-friendly exits
systemctl is-active  trademinds-dashboard.service    # active/inactive/failed
systemctl is-enabled trademinds-dashboard.service    # enabled/disabled
```

## Logs — `journalctl` cheatsheet

All stdout/stderr from services lands in the systemd journal. No more `tail -f logs/trader.log`.

```bash
# Follow live logs (the new `tail -f`)
journalctl -u trademinds-dashboard -f

# Last N lines
journalctl -u trademinds-dashboard -n 100

# Since a point in time
journalctl -u trademinds-dashboard --since "2026-04-18 06:00"
journalctl -u trademinds-dashboard --since "15 min ago"

# Between two times
journalctl -u trademinds-dashboard --since "06:00" --until "07:00"

# Across all trademinds-* services
journalctl -u 'trademinds-*' -f

# Filter by priority (emerg/alert/crit/err/warning/notice/info/debug)
journalctl -u trademinds-dashboard -p err

# Just today
journalctl -u trademinds-dashboard --since today

# With no pager (for piping / scripts)
journalctl -u trademinds-dashboard -n 100 --no-pager
```

## Editing a unit

Unit files live in `~/autonomous-trader/systemd/` (this directory) and are symlinked from `/etc/systemd/system/`. To edit:

1. Edit the file here: `vim ~/autonomous-trader/systemd/trademinds-dashboard.service`
2. Reload systemd's view: `sudo systemctl daemon-reload`
3. Restart the service to pick up changes: `sudo systemctl restart trademinds-dashboard.service`
4. Commit the change: `git add systemd/ && git commit -m "..."`

**Do NOT use `sudo systemctl edit <service>`** — that creates drop-in overrides in `/etc/systemd/system/<service>.service.d/` which won't be version-controlled here.

## Cross-references

- [docs/G1_MIGRATION_INVENTORY.md](../docs/G1_MIGRATION_INVENTORY.md) — full migration context, original launchd plist inventory, cloudflared tunnel notes
- [launchd/](../launchd/) — legacy macOS plists (preserved for reference; not executed on Linux)
- [ops/com.ollietrades.ollama-keepalive.plist](../ops/com.ollietrades.ollama-keepalive.plist) — macOS Ollama keep-alive env shim; Linux equivalent is a drop-in at `/etc/systemd/system/ollama.service.d/override.conf` (not yet created)
