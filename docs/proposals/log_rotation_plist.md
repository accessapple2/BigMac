# Log Rotation Plist Proposal (B24)

**Status:** Proposal only. Plist NOT installed.
**Owner:** Scotty (filed 2026-05-10)
**Linked backlog item:** B24 (XO_BACKLOG.md line 187) — "no log rotation policy"

## Current state

- `logs/` total: **78 MB** (10 May 2026)
- Largest: `crusher.log` 717 KB, `aladdin.log` 404 KB
- **No log currently exceeds the 50 MB threshold.** Trigger is preventive.

## What ships now (scripts/rotate_logs.sh)

- Bash script, no Python/venv deps. Safe to run any cadence.
- Inspects `~/autonomous-trader/logs/*.log` (maxdepth 2).
- Rotates only logs `>50 MB` (overridable via `$THRESHOLD_MB`).
- Keeps 3 rotations (`.log.1`, `.log.2`, `.log.3`). Overridable via `$KEEP`.
- Truncates in place (`: > f`) — preserves handles held by launchd-managed services. **No service restart needed.**

## Proposed launchd plist (NOT installed)

Path: `~/Library/LaunchAgents/com.trademinds.log-rotate.plist`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key><string>com.trademinds.log-rotate</string>

    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>/Users/bigmac/autonomous-trader/scripts/rotate_logs.sh</string>
    </array>

    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key><integer>0</integer>
        <key>Minute</key><integer>30</integer>
    </dict>

    <key>RunAtLoad</key><false/>
    <key>StandardOutPath</key>
    <string>/Users/bigmac/autonomous-trader/logs/log-rotate.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/bigmac/autonomous-trader/logs/log-rotate.log</string>
</dict>
</plist>
```

## Install procedure (Admiral-gated; do NOT run autonomously)

```bash
cp ~/autonomous-trader/docs/proposals/log_rotation_plist.md  # reference only
# Hand-author the plist at ~/Library/LaunchAgents/com.trademinds.log-rotate.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.trademinds.log-rotate.plist
launchctl enable gui/$(id -u)/com.trademinds.log-rotate
launchctl kickstart gui/$(id -u)/com.trademinds.log-rotate   # one-shot first-fire
```

## Rollback

```bash
launchctl bootout gui/$(id -u)/com.trademinds.log-rotate
rm ~/Library/LaunchAgents/com.trademinds.log-rotate.plist
# Keep scripts/rotate_logs.sh on disk — harmless idle.
```

## Risk assessment

- **Sacred DB:** none touched (script is FS-only, logs only).
- **Service impact:** none — truncate-in-place preserves the writer handle. Confirmed by `: > f` semantics on macOS/HFS+/APFS.
- **First-run blast radius:** 0 today (no logs over 50 MB).
- **Re-entry:** `set -euo pipefail` will exit non-zero if `LOG_DIR` missing; otherwise idempotent.

## Open follow-ups (do NOT block on)

1. Should rotation also cover `logs/*.err` / `logs/*.stderr.log` / `logs/*.stdout.log`? Currently `.log` only.
2. Should `.log.3` be gzipped to save disk? Not implemented in v1.
3. Backup integration: if rotation interferes with the 06:00 backup runner (`trader_<date>.db`), schedule should land before 06:00 — 00:30 chosen.
