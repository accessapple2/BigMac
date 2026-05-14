# Scotty: HM-CC LANE 2 Emergency Fixes — 2026-05-14

Three production launchd-plist fixes from the LANE 2 audit
(`data/scotty_endpoint_audit_2026-05-14.md`). All three landed cleanly
during market hours with sub-second downtime.

## Affected plists

All three at user-level `~/Library/LaunchAgents/` (no sudo needed):
- `com.trademinds.trader.plist`
- `com.trademinds.signal-center.plist`
- `com.ollietrades.morningbriefing.plist`

## HM-CC-α & β: FD limit raise (trader + signal-center)

**Problem:** trader and signal-center were OOM-killed twice this morning
(2026-05-14 07:57 and 08:00 AZ) with `OSError: [Errno 24] Too many open
files`. Default macOS soft FD limit is 256 — insufficient for the 25-agent
fleet plus broker/data-feed sockets plus SQLite handles.

**Fix:** add to both plists, before `</dict>`:
```xml
<key>SoftResourceLimits</key>
<dict>
    <key>NumberOfFiles</key>
    <integer>16384</integer>
</dict>
<key>HardResourceLimits</key>
<dict>
    <key>NumberOfFiles</key>
    <integer>32768</integer>
</dict>
```

**Verification:** `launchctl print gui/$(id -u)/<label>` reports for both:
```
maxfiles (soft) => 16384
maxfiles (hard) => 32768
```
Previously: 256 (macOS default).

**Reload procedure:** `launchctl unload <plist>` then `launchctl load <plist>`.
Trader and signal-center came back within 8 seconds each, /api/health
returns `server_up:true`.

## HM-CC-γ: morningbriefing WorkingDirectory

**Problem:** `com.ollietrades.morningbriefing` has been silently failing
for 10 days (since 2026-05-04) with `ModuleNotFoundError: No module named
'engine'`. Plist had no `WorkingDirectory`, so launchd ran the script with
cwd=$HOME, where the `engine/` package is not on the import path.

**Fix:** add to plist, before `</dict>`:
```xml
<key>WorkingDirectory</key>
<string>/Users/bigmac/autonomous-trader</string>
```

**Verification (import-only smoke test):**
```
cd /Users/bigmac/autonomous-trader
/opt/homebrew/bin/python3 -c "
import sys; sys.path.insert(0, '/Users/bigmac/autonomous-trader')
import engine.morning_briefing"
```
Result: imports cleanly, no `ModuleNotFoundError`.

Full briefing run deferred — next scheduled trigger is 2026-05-15 06:00 AZ.

## Backups

Originals saved to `/tmp/plist_backup_$(date +%Y%m%d_%H%M%S)/` before any
edit. Plistlib used for the edit (preserves XML formatting and ordering).
`plutil -lint` ran green on all three plists post-patch.

## Followups

- The trader hit the 256-FD wall under normal-ish load — worth profiling
  *what* opened so many handles to confirm there's no FD leak. The new
  16384 soft limit is generous but won't catch a true leak.
- Audit other launchd plists in the project for missing `WorkingDirectory`
  + missing `SoftResourceLimits` — same class of silent failure could exist
  elsewhere. Cross-link: [[silent-failure-pattern]].
- Consider checking other plists for `/opt/homebrew/bin/python3` vs
  `venv/bin/python3` consistency — morning_briefing uses homebrew python
  which may not have all project deps; this didn't bite us because
  the script's only failure point was the cwd, but it's brittle.

## Captain action

- [x] FD limits raised on trader + signal-center
- [x] morningbriefing imports resolve (full run will fire tomorrow 06:00 AZ)
- [ ] Optional: profile trader's FD usage to confirm no leak
- [ ] Optional: audit other plists for the same gaps
