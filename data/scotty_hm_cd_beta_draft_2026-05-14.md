# Scotty: HM-CD-β Draft — 2026-05-14

**Status:** DRAFT — not applied. Captain reviews and runs `--apply` when ready.

## Scope

Mechanical plist hygiene fixes identified by the HM-CD-α audit
(`data/scotty_hm_cd_plist_audit_2026-05-14.md`). 17 plists targeted; the
script auto-detects which keys are already present and only adds missing ones,
so re-running is idempotent.

Three HM-CC-fixed plists are NOT in this batch (already done): `com.trademinds.trader`,
`com.trademinds.signal-center`, `com.ollietrades.morningbriefing`.

## Script

`scripts/hm_cd_beta_draft.sh`

| Mode | Behavior |
|---|---|
| `--dry-run` (default) | Print what would change; no writes |
| `--apply` | Backup each plist to `.bak.YYYY-MM-DD`, write changes, run `plutil -lint`, revert if lint fails |
| `--revert` | Restore from `.bak.YYYY-MM-DD` backup |

Idempotent: re-applying skips already-present keys.

## Risk-rated apply order

### 🟢 LOW RISK BATCH (6 plists) — apply first

Trivial / non-critical services. If anything breaks, system stable.

| Plist | Change | Note |
|---|---|---|
| `com.trademinds.caffeinate` | + WorkingDirectory | trivial `caffeinate -di` |
| `com.ollietrades.crusher` | + WorkingDirectory | interval=360s; not in critical path |
| `com.ollietrades.etfregime` | + WorkingDirectory | calendar-based; not blocking |
| `com.ollietrades.morning-an2-observation` | + WorkingDirectory | shell script handles cd; hygiene only |
| `com.ollietrades.morning-cd-instr` | + WorkingDirectory | shell script handles cd; hygiene only |
| `com.ollietrades.stale-trim-obs` | + WorkingDirectory | calendar-based |

### 🟡 MED RISK BATCH (7 plists) — apply after LOW confirmed clean

Useful services but recoverable if config breaks.

| Plist | Change | Note |
|---|---|---|
| `com.ollietrades.optionsflow` | + WorkingDirectory | calendar-based data scraper |
| `com.ollietrades.schwab-watcher` | + WorkingDirectory | 60s interval, mirrors HM-AT-β |
| `com.trademinds.webull-sync` | + WorkingDirectory | calendar-based |
| `com.ollietrades.ollama-keepalive` | + WorkingDirectory + StandardErrorPath | also adds err log path (was undefined) |
| `com.ollietrades.danelfin-update` | **(no changes needed — see caveat)** | already has WorkingDirectory |
| `com.ollietrades.ti-email-poller` | + WorkingDirectory | hygiene; script uses `Path(__file__)` |
| `com.ollietrades.ti-picks-watcher` | + WorkingDirectory | hygiene; script handles cd internally |

### 🔴 HIGH RISK BATCH (4 plists) — apply LAST, separately

Critical long-running services. Reload one at a time, verify after each.

| Plist | Change | Note |
|---|---|---|
| `com.trademinds.mcp` | + Soft/HardResourceLimits | long-running daemon; FD headroom needed |
| `com.trademinds.scanner` | + Soft/HardResourceLimits | long-running daemon; FD headroom needed |
| `com.trademinds.tunnel` | + WorkingDirectory + Soft/HardResourceLimits | **see caveat below** |
| `com.trademinds.watchdog` | + Soft/HardResourceLimits | **CRITICAL — apply absolute last** |

**Watchdog last because:** if the watchdog plist breaks, it cannot respawn the trader/signal-center on the next OOM. Apply ONLY after all others verified clean.

## Caveats — investigate before / instead of mechanical apply

### `com.ollietrades.danelfin-update` (audit miscategorization)

The HM-CD-α audit flagged this 🔴 CRITICAL based on a `ModuleNotFoundError`
entry in the log on 2026-05-10. But the plist **already has WorkingDirectory**.
The audit's recommended fix (adding FD limits) is unrelated to the observed
error. So either:

- (a) the WorkingDirectory was added AFTER 2026-05-10 (the error is stale)
- (b) the ModuleNotFoundError has a different root cause (env var? import path?
  Python interpreter?)

Suggested before apply: `cat ~/ollietrades/logs/danelfin_update.log | tail -50`
and verify whether the error has recurred since the WD was added. If no recent
errors → no fix needed. If still erroring → investigate the root cause, do not
just stack FD limits.

### `com.trademinds.tunnel` (root-cause needed)

26 fresh cloudflared errors in `~/Library/Logs/cloudflared-trademinds.err`
within the last 7 days:

```
ERR failed to run the datagram handler error="Application error 0x0 (remote)"
ERR failed to serve tunnel connection error="accept stream listener encountered a failure"
ERR Serve tunnel error error="accept stream listener encountered a failure while serving"
```

These look like Cloudflare-side or network issues, not local config. Adding
WorkingDirectory + FD limits will not fix them. Captain should investigate the
tunnel separately. The mechanical config change is still **safe** to apply
(adds hygiene, doesn't make worse) but won't resolve the symptom.

## Smoke test plan

Before any `--apply`:

```bash
# 1. Confirm dry-run shows the expected changes
bash scripts/hm_cd_beta_draft.sh --dry-run

# 2. plutil -lint on every plist before AND after
for L in com.ollietrades.crusher com.ollietrades.etfregime ... ; do
  plutil -lint ~/Library/LaunchAgents/$L.plist
done
```

The script itself runs `plutil -lint` after each write and reverts from `.bak`
if lint fails. So a bad apply self-heals.

## Reload sequence (after `--apply`)

```bash
# LOW + MED: reload in any order (independent services)
for L in com.trademinds.caffeinate com.ollietrades.crusher \
         com.ollietrades.etfregime com.ollietrades.morning-an2-observation \
         com.ollietrades.morning-cd-instr com.ollietrades.stale-trim-obs \
         com.ollietrades.optionsflow com.ollietrades.schwab-watcher \
         com.trademinds.webull-sync com.ollietrades.ollama-keepalive \
         com.ollietrades.ti-email-poller com.ollietrades.ti-picks-watcher; do
  launchctl unload ~/Library/LaunchAgents/$L.plist 2>/dev/null
  launchctl load   ~/Library/LaunchAgents/$L.plist
done

# HIGH: reload one at a time, verify each, watchdog absolute last
launchctl unload ~/Library/LaunchAgents/com.trademinds.mcp.plist
launchctl load   ~/Library/LaunchAgents/com.trademinds.mcp.plist
sleep 5; launchctl list | grep com.trademinds.mcp   # expect STATUS=0, PID alive

launchctl unload ~/Library/LaunchAgents/com.trademinds.scanner.plist
launchctl load   ~/Library/LaunchAgents/com.trademinds.scanner.plist
sleep 5; launchctl list | grep com.trademinds.scanner

# tunnel: only if you've decided to apply (root-cause not addressed)
launchctl unload ~/Library/LaunchAgents/com.trademinds.tunnel.plist
launchctl load   ~/Library/LaunchAgents/com.trademinds.tunnel.plist

# watchdog ABSOLUTE LAST
launchctl unload ~/Library/LaunchAgents/com.trademinds.watchdog.plist
launchctl load   ~/Library/LaunchAgents/com.trademinds.watchdog.plist
sleep 10
launchctl list | grep com.trademinds.watchdog   # expect alive
launchctl print gui/$(id -u)/com.trademinds.watchdog | grep maxfiles
# expect: maxfiles (soft) => 16384 / (hard) => 32768
```

## Captain action

- [ ] Review this draft
- [ ] Run `bash scripts/hm_cd_beta_draft.sh --dry-run` and confirm output matches expectations
- [ ] Address the **danelfin-update** and **tunnel** caveats (separate from this batch)
- [ ] `bash scripts/hm_cd_beta_draft.sh --apply` (writes + lints + reverts on lint failure)
- [ ] Run reload sequence above
- [ ] Verify each batch before moving to next risk tier
- [ ] If anything goes sideways: `bash scripts/hm_cd_beta_draft.sh --revert`

## Files written

- `scripts/hm_cd_beta_draft.sh` (176 lines) — batch script, dry-run default
- `data/scotty_hm_cd_beta_draft_2026-05-14.md` (this file)

Neither committed. Neither applied. Untracked in working tree.
