# Scotty: HM-CD-β Apply Plan — 2026-05-15

**Status:** DRAFT — script ready, tests ready, awaiting Captain `--apply`.

**Branch:** `hm-cd-beta-plist-hygiene`  ·  **PR:** open at merge time.

## State refresh (2026-05-15 11:00 AZ)

State verified via `plistlib` (same parser the script uses), independent of
yesterday's draft. **No drift.** 16 of 17 plists still need hygiene fields;
`danelfin-update` already has WD (only it of the 17). State table:

| Plist | Risk | WD | SoftFD | HardFD | Stderr | Script will… |
|---|---|---|---|---|---|---|
| `com.trademinds.caffeinate` | LOW | — | — | — | ✓ | + WD |
| `com.ollietrades.crusher` | LOW | — | — | — | ✓ | + WD |
| `com.ollietrades.etfregime` | LOW | — | — | — | ✓ | + WD |
| `com.ollietrades.morning-an2-observation` | LOW | — | — | — | ✓ | + WD |
| `com.ollietrades.morning-cd-instr` | LOW | — | — | — | ✓ | + WD |
| `com.ollietrades.stale-trim-obs` | LOW | — | — | — | ✓ | + WD |
| `com.ollietrades.optionsflow` | MED | — | — | — | ✓ | + WD |
| `com.ollietrades.schwab-watcher` | MED | — | — | — | ✓ | + WD |
| `com.trademinds.webull-sync` | MED | — | — | — | ✓ | + WD |
| `com.ollietrades.ollama-keepalive` | MED | — | — | — | — | + WD + StderrPath |
| `com.ollietrades.danelfin-update` | MED | ✓ | — | — | ✓ | (no change) |
| `com.ollietrades.ti-email-poller` | MED | — | — | — | ✓ | + WD |
| `com.ollietrades.ti-picks-watcher` | MED | — | — | — | ✓ | + WD |
| `com.trademinds.mcp` | HIGH | ✓ | — | — | ✓ | + Soft + Hard FD |
| `com.trademinds.scanner` | HIGH | ✓ | — | — | ✓ | + Soft + Hard FD |
| `com.trademinds.watchdog` | HIGH | ✓ | — | — | ✓ | + Soft + Hard FD |
| `com.trademinds.tunnel` | HIGH | — | — | — | ✓ | + WD + Soft + Hard FD |

## New HIGH-risk gate (HM-CD-β-2026-05-15)

The script now requires per-plist invocation for HIGH-tier plists:

```bash
bash scripts/hm_cd_beta_draft.sh --apply             # LOW + MED only (12 plists)
bash scripts/hm_cd_beta_draft.sh --apply-high <LABEL>  # ONE HIGH plist
```

`--apply` SKIPS all 4 HIGH plists with an explicit message naming the
required `--apply-high LABEL` command. Each HIGH plist is its own Captain
re-approval boundary — bulk `--apply` cannot touch them.

## Risk-ordered apply sequence

### 🟢 LOW + MED (single `--apply`, 12 plists)

```bash
bash scripts/hm_cd_beta_draft.sh --apply
```

Expected behavior:
- 11 plists get `+ WorkingDirectory`
- `ollama-keepalive` gets `+ WorkingDirectory` AND `+ StandardErrorPath`
- `danelfin-update` skipped — no changes needed
- All 4 HIGH plists skipped with `--apply-high` instruction in output

Verification:
```bash
bash tests/test_plist_hygiene_post_apply.sh LOW    # expect all 6 PASS
bash tests/test_plist_hygiene_post_apply.sh MED    # expect all 7 PASS
```

Reload (Captain action, not auto):
```bash
for L in com.trademinds.caffeinate com.ollietrades.crusher \
         com.ollietrades.etfregime com.ollietrades.morning-an2-observation \
         com.ollietrades.morning-cd-instr com.ollietrades.stale-trim-obs \
         com.ollietrades.optionsflow com.ollietrades.schwab-watcher \
         com.trademinds.webull-sync com.ollietrades.ollama-keepalive \
         com.ollietrades.ti-email-poller com.ollietrades.ti-picks-watcher; do
  launchctl unload ~/Library/LaunchAgents/$L.plist 2>/dev/null
  launchctl load   ~/Library/LaunchAgents/$L.plist
done
```

### 🔴 HIGH (4 separate `--apply-high LABEL` invocations, watchdog last)

```bash
# 1. mcp
bash scripts/hm_cd_beta_draft.sh --apply-high com.trademinds.mcp
bash tests/test_plist_hygiene_post_apply.sh com.trademinds.mcp        # expect PASS
launchctl unload ~/Library/LaunchAgents/com.trademinds.mcp.plist
launchctl load   ~/Library/LaunchAgents/com.trademinds.mcp.plist
sleep 5 && launchctl list | grep com.trademinds.mcp                    # expect alive
launchctl print gui/$(id -u)/com.trademinds.mcp | grep maxfiles        # expect 16384/32768

# 2. scanner
bash scripts/hm_cd_beta_draft.sh --apply-high com.trademinds.scanner
bash tests/test_plist_hygiene_post_apply.sh com.trademinds.scanner
launchctl unload ~/Library/LaunchAgents/com.trademinds.scanner.plist
launchctl load   ~/Library/LaunchAgents/com.trademinds.scanner.plist
sleep 5 && launchctl list | grep com.trademinds.scanner

# 3. tunnel  (NB: tunnel has 34 ERR in last 100 cloudflared lines today;
#             config patch is safe but does NOT fix the underlying cloudflared issue)
bash scripts/hm_cd_beta_draft.sh --apply-high com.trademinds.tunnel
bash tests/test_plist_hygiene_post_apply.sh com.trademinds.tunnel
launchctl unload ~/Library/LaunchAgents/com.trademinds.tunnel.plist
launchctl load   ~/Library/LaunchAgents/com.trademinds.tunnel.plist
sleep 5 && launchctl list | grep com.trademinds.tunnel

# 4. watchdog — ABSOLUTE LAST
bash scripts/hm_cd_beta_draft.sh --apply-high com.trademinds.watchdog
bash tests/test_plist_hygiene_post_apply.sh com.trademinds.watchdog
launchctl unload ~/Library/LaunchAgents/com.trademinds.watchdog.plist
launchctl load   ~/Library/LaunchAgents/com.trademinds.watchdog.plist
sleep 10 && launchctl list | grep com.trademinds.watchdog
launchctl print gui/$(id -u)/com.trademinds.watchdog | grep maxfiles
```

## Verification commands

After each tier, run the appropriate test:
```bash
bash tests/test_plist_hygiene_post_apply.sh ALL    # full sweep
bash tests/test_plist_hygiene_post_apply.sh LOW    # one tier
bash tests/test_plist_hygiene_post_apply.sh HIGH   # one tier
bash tests/test_plist_hygiene_post_apply.sh <label>  # one plist
```

The test exits non-zero if any expected field is missing. Pre-apply
all 16 expected-FAIL plists fail with a clear field-by-field message.

## Caveats (still open, NOT addressed by this batch)

### `com.ollietrades.danelfin-update` — Module-not-found unresolved
The plist already has WD and uses `venv/bin/python3` (verified 2026-05-15).
The audit's reference to "ModuleNotFoundError on 2026-05-04" appears
**resolved** — no recent error log activity observed. If errors recur,
investigate the import path / env vars separately, not via FD limits.

### `com.trademinds.tunnel` — Cloudflared errors continue
34 ERR lines in last 100 of `~/Library/Logs/cloudflared-trademinds.err`
(checked 2026-05-15). These are Cloudflare-side connection failures
(`failed to serve tunnel connection`), unrelated to local plist
hygiene. The config patch is **safe** to apply but will **not** resolve
these errors. Tunnel root-cause investigation queued for separate work.

## Files in this batch

- `scripts/hm_cd_beta_draft.sh` — modified: added `--apply-high LABEL` mode + HIGH gate
- `tests/test_plist_hygiene_post_apply.sh` — new: TDD verification harness
- `data/scotty_hm_cd_beta_apply_plan_2026-05-15.md` — this file

## Captain action queue

1. Review PR (link in commit message)
2. Approve merge to main
3. After merge, on local main:
   ```bash
   git pull
   bash scripts/hm_cd_beta_draft.sh --dry-run     # final sanity check
   bash scripts/hm_cd_beta_draft.sh --apply       # LOW + MED only
   bash tests/test_plist_hygiene_post_apply.sh LOW   # verify
   bash tests/test_plist_hygiene_post_apply.sh MED   # verify
   # Reload LOW + MED (launchctl block above)
   ```
4. Per-HIGH approval cycle (one at a time):
   - mcp → test → reload → verify
   - scanner → test → reload → verify
   - tunnel → test → reload → verify (note: tunnel ERR symptom NOT fixed)
   - watchdog → test → reload → verify (absolute last)
5. Final sweep: `bash tests/test_plist_hygiene_post_apply.sh ALL` → all 17 PASS
6. If anything goes sideways: `bash scripts/hm_cd_beta_draft.sh --revert`
   restores from `/tmp/plist_backup_<TS>/` (created on apply).

## Rollback

The script writes backups to `/tmp/plist_backup_<TS>/` on every `--apply`
or `--apply-high` invocation, then runs `plutil -lint` and reverts on
lint failure. Manual rollback for any reason:

```bash
bash scripts/hm_cd_beta_draft.sh --revert
```

Followed by launchctl unload/load to re-pick the reverted file.
