# HM-BACKUP-REBOOT-SURVIVE — DR backup reconcile (2026-05-31)

## TL;DR
- **Local daily DB backup is SAFE and reboot-survivable** — it was never the launchd job; it's `healthcheck.py::backup_trader_db()` on cron `0 6-13 * * *`. Verified: `backups/trader_2026-05-31.db` (482 MB), `integrity_check=ok`, 2372 trades.
- **The two launchd "backup" plists were both DEAD** — `launchctl list` showed neither loaded (died on a past SSH-only reboot, never reloaded — the documented `bootstrap gui/$UID` failure mode). Archived both to `archive/launchagents_2026-05-31/`.
- **crusher** (turns out to be a *port-health alerter*, every 6 min — NOT a backup) migrated launchd→cron `*/6 * * * *` so the secondary alarm survives reboot.
- **off-host replication is DOWN and HELD** — target `.166` is retired (host down) and `.168` (Ollie Max) can't receive yet (`bigmac@` SSH = publickey denied, no dest dir). Not scheduled against a host that can't take it. See runbook below.

## What each piece actually is (corrected)
| Piece | Real role | Mechanism (now) | Reboot-survivable? |
|---|---|---|---|
| `healthcheck.py::backup_trader_db()` | **the local DR backup** — sqlite `.backup()` → `backups/trader_YYYY-MM-DD.db`, keep-last-7 | cron `0 6-13 * * *` | ✅ yes (cron) |
| `dr_crusher.sh` | port-health **alerter** (8080/9000), NTFY-only, no restart | cron `*/6 * * * *` (was launchd `StartInterval 360`, dead) | ✅ now (was ❌) |
| `scripts/offhost_backup.sh` | **off-host rsync** of DBs + last-7 dailies → Ollie Box | **HELD** (was launchd 06:30, dead) | ⛔ blocked on host |

## Changes made (2026-05-31)
1. crontab: added `*/6 * * * * dr_crusher.sh` (reboot-survivable alerter); added a **commented** offhost line + runbook note (held, see below).
2. Archived `com.ollietrades.crusher.plist` + `com.ollietrades.offhost-backup.plist` → `archive/launchagents_2026-05-31/` (per archive-not-delete doctrine; both were already unloaded).
3. No change to `healthcheck.py` (local backup already correct) and **no repoint of `offhost_backup.sh`** (REMOTE_HOST still `.166` — held).

> Note on "@reboot": periodic backups use **time-based cron** (`*/6`, `30 6`), NOT literal `@reboot` — cron reloads its crontab at boot so time-based entries inherently survive reboot, whereas `@reboot` fires once at boot (correct only for the long-running trader/tunnel daemons, wrong for a periodic backup).

## OFF-HOST BACKUP RUNBOOK (manual — to restore DR replication)
Off-host backup is the only remaining gap. It needs a reachable receiver. `.166` is retired; `.168` (Ollie Max, RTX 5080 host) pings but rejects `bigmac@` SSH.

To activate (one-time, manual — requires interactive SSH the agent can't do headless):
1. On bigmac: `ssh-copy-id bigmac@192.168.1.168` (or append `~/.ssh/id_*.pub` to `bigmac@192.168.1.168:~/.ssh/authorized_keys`). Confirm `ssh bigmac@192.168.1.168 hostname` works passwordlessly.
2. On Ollie Max: `mkdir -p ~/bigmac-backups/{data,signal-center,tractor,backups}`.
3. In `scripts/offhost_backup.sh`: repoint `REMOTE_HOST="192.168.1.166"` → `"192.168.1.168"`.
4. Uncomment the offhost line in crontab (`30 6 * * * scripts/offhost_backup.sh`).
5. Verify: run `scripts/offhost_backup.sh` once → expect "Off-host backup OK … integrity_check=ok" NTFY.

Until step 1-5, **off-host DR does not exist** — the local `backups/` snapshots (same disk as the live DB) are the only copies. A disk failure would lose both. This is the standing risk to clear.
