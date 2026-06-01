# SCOPE — #7 Daemon graveyard re-home: exact commands — HOLD for go (DO NOT RUN)

**Nothing applied.** Concrete commands for Admiral review. Pairs with
`docs/DAEMON-GRAVEYARD-REHOME-PLAN-2026-05-30.md`. Every cron-rehome MUST be paired with a
`launchctl disable`/plist-move (Caveat 1: double-fire) — gui/501 plists are unreachable from SSH,
so the `launchctl` steps may need the Admiral's logged-in Aqua session.

## State found 2026-06-01 (changes the plan)
- **signal-center @reboot cron ALREADY EXISTS** (`@reboot scripts/signal_center_reboot_start.sh`,
  added 2026-05-23). So signal-center is already cron-rehomed. The only gap is **Caveat 1**: the
  stray gui plist `com.trademinds.signal-center.plist` still exists + is loadable → double-fire if
  the Admiral logs into the GUI. Same for trader/cloudflared/swingdesk (all already @reboot-cron'd).
- **8 agents already retired** (`*.plist.retired-2026-05-30`): fleet-auditor, ghost-advisor,
  morningbriefing, ollie-scan, real-portfolio-snapshot, sitrep, squeeze-scan, uhura. No action.
- **33 plist files** in ~/Library/LaunchAgents (incl. .bak/.retired). Live (non-retired) ones still
  subject to the reboot-survival-gap.
- **Kirk producer is NOT scheduled anywhere** (no plist, no cron, no main.py schedule) —
  `engine/kirk_advisory.py::generate_kirk_advisory` only runs when `/api/kirk/advisory` is hit.
  That's why `kirk_advisory_log` last wrote 2026-05-18. Re-home = give it a schedule.

## 1. signal-center — close Caveat 1 (the @reboot cron already covers boot)
```bash
# backup, then disable + move the stray gui plist so it can't double-fire on GUI login
mkdir -p ~/Library/LaunchAgents/_disabled_2026-06-01
launchctl bootout gui/501/com.trademinds.signal-center 2>/dev/null   # may need Admiral GUI session
launchctl disable gui/501/com.trademinds.signal-center 2>/dev/null
mv ~/Library/LaunchAgents/com.trademinds.signal-center.plist ~/Library/LaunchAgents/_disabled_2026-06-01/
# verify the @reboot cron is still the sole start path:
crontab -l | grep signal_center_reboot_start    # expect the existing @reboot line
```
(Repeat the same pattern for trader / cloudflared / swingdesk plists — all already @reboot-cron'd.)

## 2. Kirk advisory producer — give it a schedule (currently unscheduled)
generate_kirk_advisory() writes kirk_advisory_log. Two options:

**Option A (cron, decoupled — recommended, survives reboot):**
```bash
# every 30 min during RTH (9:30–16:00 ET ≈ 6:30–13:00 MST/AZ), weekdays
crontab -e   # add:
*/30 6-13 * * 1-5 cd /Users/bigmac/autonomous-trader && PYTHONPATH=/Users/bigmac/autonomous-trader \
  /Users/bigmac/autonomous-trader/.venv/bin/python3 -c \
  "from engine.kirk_advisory import generate_kirk_advisory; generate_kirk_advisory()" \
  >> logs/kirk_advisory_cron.log 2>&1
```
**Option B (in-process):** add `schedule.every(30).minutes.do(run_kirk_advisory_job)` in main.py
(gated to RTH like battle_station) — but then it dies with the trader and isn't independently
monitored. Prefer A. After either, W1 source_gate `kirk_advisory` flips RED→GREEN once it writes.

## 3. The ~24 agents — restore template (per graveyard plan tiers)
Restore order = Tier 1 first (watchdog + healthcheck), then Tier 2 (morningbriefing — note: already
retired, decide cron-vs-in-process), then Tier 3 (verify no in-process overlap). Each restore:
```bash
# template — one managed cron block (so the next reboot-audit has ONE place to check)
# file: scripts/cron_daemons.sh  (NEW — holds all re-homed @reboot/interval lines)
# then for each restored job, DISABLE its gui plist to honor Caveat 1:
for label in com.trademinds.watchdog com.trademinds.healthcheck ; do
  launchctl bootout gui/501/$label 2>/dev/null
  launchctl disable gui/501/$label 2>/dev/null
  mv ~/Library/LaunchAgents/$label.plist ~/Library/LaunchAgents/_disabled_2026-06-01/ 2>/dev/null
done
# watchdog needs a */5 supervisor (Caveat 2 — @reboot alone loses KeepAlive respawn):
*/5 * * * * pgrep -f watchdog.py >/dev/null || \
  /Users/bigmac/autonomous-trader/venv/bin/python3 /Users/bigmac/autonomous-trader/watchdog.py &
```
Tier 3 (squeeze-scan/ghost-advisor/metals-sync/ollie-scan) — **verify no in-process overlap before
restoring** (ollie-scan likely doubles the in-process LRS arena scan; the graveyard doc flags it).
Tier 4 stays retired.

## Recommended sequence (for the go)
1. Close Caveat 1 on the 4 already-cron'd services (signal-center first — commands in §1).
2. Re-home Kirk via §2 Option A (one cron line; lowest risk; flips the W1 RED flag GREEN).
3. Build `scripts/cron_daemons.sh` as the single managed block, restore Tier 1 (watchdog +
   `*/5` supervisor, healthcheck) per §3, each paired with plist-disable.
4. Decide morningbriefing (cron vs in-process — pick one, it's currently retired).
5. Tier 3 only after in-process-overlap verification. Tier 4 stays dead.

**All gui/501 `launchctl` steps likely require the Admiral's logged-in session (SSH can't reach the
gui domain). Hold for go.**
