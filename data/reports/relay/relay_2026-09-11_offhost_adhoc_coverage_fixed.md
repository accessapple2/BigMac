# Relay — Off-host sync gap fixed, 6 ad-hoc backups backfilled to X9. 2026-09-11

## Decision on the dry-dock files

Disk pressure resolved (66% used, 65GB free after the `~/.ollama` reclaim)
— **keeping all five dry-dock-era files.** 0 bytes freed is the correct
outcome when nothing in the inventory is actually stale; the rule-3-beats-
rule-4 carve-out did its job (no incident checkpoint got auto-collapsed
while the incident was still being referenced).

## The real fix: offhost_backup.sh's sync pattern

`scripts/offhost_backup.sh` only ever synced files matching
`trader_YYYY-MM-DD.db`. Every ad-hoc-named backup — pre-migration,
pre-restatement, dry-dock/incident checkpoints, exactly the files
`docs/runbooks/backup-retention-policy.md`'s rule 3 says to keep
**indefinitely** — was silently excluded, with zero signal anywhere that
this was happening (the script logs `[OK] daily-backups (7)` every night
and always has, truthfully, while the ad-hoc files it never touches sit
local-only).

**Fixed**: added a new `ADHOC` sync step matching every `data/backups/
*.db` file that isn't the dated-daily pattern — no 14-day cap (unlike the
routine dailies), since these are rare and deliberate by construction.
Every ad-hoc file also gets its own individual post-sync integrity check
now (not a `tail -N` sample, since there are few of them and each matters
on its own). Runs automatically as part of the existing 20:30 nightly
cron from tonight on — no separate step to remember.

## Backfill, done today

All 6 currently-existing ad-hoc files rsync'd to X9 by hand (same flags
the script itself uses: `rsync -a --copy-links --no-owner --no-group`),
then verified with the same rigor already used for the 7 dailies:

| File | Byte match | `PRAGMA integrity_check` |
|---|---|---|
| `signals_20260911_dry-dock.db` | exact | ok |
| `signals_drydock_20260911_065430.db` | exact | ok |
| `trader_20260911_074244_pre_b6_restatement.db` | exact | ok |
| `trader_20260911_dry-dock.db` | exact | ok |
| `trader_drydock_20260911_065430.db` | exact | ok |
| `trader_pre-key-rotation-restart_20260910_134530.db` | exact | ok |

**All 13 files in `data/backups/` now have a verified off-host copy** —
not just the 7 routine dailies.

## Named as a pattern

Added to `docs/DOCTRINE.md`'s "Doctrine Lessons" section: "Coverage that
looks complete but silently excludes what matters." Three real instances
from this single dry-dock session, same shape in each: `origin_
healthcheck.sh` restarted a deliberately-stopped trader because it had no
concept of intentional downtime; several `docs/XO_BACKLOG.md` `REVISIT-
BY` tags sat overdue for weeks with nothing forcing a re-check; and this
sync pattern excluded exactly the backups a retention policy most needs
protected. In every case the mechanism looked like coverage and logged
success honestly — the gap was in what it matched, not in whether it ran.
Rule stated for future audits: enumerate what a coverage mechanism
actually matches against a live inventory, not what it was intended to
match.

## Verification

`bash -n scripts/offhost_backup.sh` clean. The new `ADHOC` find pattern
tested standalone against the live `data/backups/` directory before any
sync ran — confirmed it matches exactly the 6 known ad-hoc files and
none of the 7 dailies. The full nightly script itself was not run
end-to-end today (it would report a false `[FAIL]` on the trader.db step
since today's routine daily snapshot doesn't exist until 20:15 tonight)
— the ad-hoc backfill was done as a standalone, targeted rsync instead,
using the identical flags and destination the script's own `run_rsync`
helper uses. Tonight's regular 20:30 run will be the first real
end-to-end exercise of the new `ADHOC` step; worth a glance at `logs/
offhost_backup.log` tomorrow to confirm it logs `[OK] adhoc-backups (6)`
(or more, if anything new gets created before then) alongside the usual
daily/trader.db/signals.db lines.
