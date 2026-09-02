# Off-host backup — Option B scoping (Option A dead)

**Branch:** exec-pipeline. Follow-up to
`relay_2026-09-02_offhost-backup-tcc-diagnosis.md`. Option A (Full Disk
Access grant via Screen Sharing) is dead — VNC connects and authenticates
but the physical framebuffer is black (headless Mac mini, no active
console session driving a real display; `caffeinate` didn't help), so the
System Settings panel is unreachable regardless of network access to the
service. Per Captain directive: scope Option B (network-based off-host
target, bypassing the Removable-Volume TCC gate entirely), propose, do
not apply. Nothing in this pass changed cron, the X9 volume, or any
script — one temporary crontab test line was added and removed during
live TCC-premise verification (item 2 below), no artifact left behind.

---

## 1. Where does it go — what's actually reachable right now

Checked live rather than assumed. Short answer: **nothing is reachable
today.** This is a hardware/provisioning gap, not a script change.

**LAN (`arp -a` + direct probes):**
- `192.168.1.166` ("ollie", in `~/.ssh/config`) — 100% ping loss, dead.
- `192.168.1.168` (olliemax, already known decommissioned) — 100% ping
  loss, dead; also shown `(incomplete)` in the live ARP table.
- `192.168.1.80` (in `~/.ssh/known_hosts`, no config alias) — 100% ping
  loss, dead.
- `192.168.1.104` (`DESKTOP-G05A57H`, Windows, TTL 128) — **alive**, pings
  clean, SMB (445) open. No SSH (22) — closed/no response. Not in
  `~/.ssh/config` or `known_hosts`; no established access path.
  `smbclient` isn't installed on this box, so even SMB isn't currently
  actionable without adding tooling. More importantly: it's an unlabeled
  personal/family Windows PC, not a machine provisioned for this purpose
  — **not recommending it** without you confirming whose machine it is
  and that you want trading DB backups landing on it. Flagging that it
  exists rather than silently omitting it, per your instruction not to
  hide what's actually there.
- Everything else on the LAN (router, an Apple Watch, an iPhone, two
  Brother printers) — not credible backup targets, skipped.

**Tailscale** (`tailscale status`, installed and running —
`100.103.190.24 steves-mac-mini`, this box, online): every other node in
the tailnet is offline —
`desktop-dell17` (last seen 19d ago / 2026-08-13 per JSON detail),
`iphone-14-pro-max` (81d ago), `minimac` (9d ago per the short status
line), `olliemax` (43d ago). **Zero reachable Tailscale peers besides
this machine.**

**Cloud/network storage tooling:** none installed —
`rclone`/`aws`/`b2`/`restic`/`borg` all absent (`which` returns nothing
for all five); only plain `rsync` exists. No existing cloud remote
config found (`~/.aws`, `~/.rclone.conf`, `~/.config/rclone` — none
present). No SMB/AFP/NFS shares currently mounted. There is no
pre-existing cloud backup path being unused; it would need to be stood
up from nothing.

**What this means:** Option B isn't "point the script at X" — there is
currently no X. The real decision is which of these gets built:

- **(i) Bring a known machine back online.** `desktop-dell17` or
  `minimac` (both Tailscale-known, both currently offline) — if either is
  physically reachable/powerable, that restores a target with zero new
  infrastructure. Needs you to say whether either is realistically
  available (I don't know their physical location/owner-intent).
- **(ii) Provision a small always-on cloud target** — a cheap VPS
  (DigitalOcean/Linode/Hetzner-class, ~$5-6/mo) with SSH, used purely as
  an `rsync`-over-SSH destination the same way olliemax was. Clean,
  doesn't depend on any household device staying powered on, matches the
  design that already ran clean for months. Real, if small, recurring
  cost + one-time setup (SSH key, firewall) — needs your sign-off as a
  spend decision, not something to provision unilaterally.
- **(iii) Stopgap only:** keep relying on the interactive-shell path to
  the X9 (already proven working — see the prior diagnosis's live test)
  for manual/on-demand syncs until (i) or (ii) lands, rather than leaving
  the gap fully unaddressed. Not automated, not a real fix, but better
  than zero while (i)/(ii) is decided.

I'm not picking one — this is the "hardware decision instead of a script
change" you called out. Recommend (ii) if no existing machine in (i) is
realistically bringable back, since it fully replicates what already
worked for months with no new dependency on a household device's power
state.

---

## 2. TCC premise — reverified live, not just historically

Re-checked rather than assumed, two ways:

**Historical (the whole log, not just the summary from last time):**
scanned every line of `logs/offhost_backup.log` before the 2026-08-27
repoint — including the whole stretch where olliemax was already timing
out (`Operation timed out`, `Host is down`, `No route to host` — it had
been dead for a while before the "official" 08-27 decommission date, this
script's own header undersells how long) — **zero** occurrences of
`Operation not permitted` or `authorization denied` anywhere in that
entire SSH-era history. Every failure in that era was a network
reachability problem, never a TCC denial. The TCC signature appears for
the first time the instant the destination becomes a locally-mounted
volume (08-27 20:30, first cron run against X9) and on every run since.

**Live, today, via cron specifically (not an interactive shell standing
in for it):** added a temporary one-off crontab line
(`/usr/bin/ssh -o BatchMode=yes ... git@github.com`), waited for it to
fire under the real cron daemon, then removed the line. Actual output:

```
Pseudo-terminal will not be allocated because stdin is not a terminal.
Hi accessapple2! You've successfully authenticated, but GitHub does not provide shell access.
EXIT_CODE:1
```

A full SSH handshake + public-key auth, completed by cron, today. (Exit 1
is SSH's normal response to "authenticated but no shell" — not a
failure of the thing being tested.) No trace left — crontab restored,
temp log deleted, verified the four real jobs (`offhost_backup`,
`db_snapshot`, `archive_ttl`, `backup_freshness_check`) still present
afterward.

**Confirms the premise cleanly: cron's outbound network/SSH path is not
TCC-gated, today, for real.** One important boundary, worth stating
explicitly since you flagged it: **this only holds for actual network
transport (SSH/rsync-over-SSH, or an HTTPS API call to a cloud
provider) — not for anything that presents as a locally mounted
filesystem.** An SMB share, a WebDAV mount, an rclone FUSE mount, iCloud
Drive, a Dropbox-style synced folder — any of those would register with
macOS the same way the X9 does (a local mount point subject to the same
Removable/Network-Volume TCC category) and should be expected to fail
identically under cron. Whatever Option B target gets built, the
transport must be a raw protocol client (rsync+ssh, scp, an S3/B2-style
API client) that never mounts anything — that's the one hard constraint
carried forward from this diagnosis.

---

## 3. X9 stays secondary — untouched

No changes made to `offhost_backup.sh`, its crontab line, or the X9
volume itself. It still holds the real (if 6-nights-stale) archive and is
still fully usable from an interactive session, per the prior diagnosis's
live write/read/integrity-check test. Whatever Option B design gets
approved should be a **second**, independent destination — cron should
not be made to depend on the X9 succeeding or failing, and the X9 write
should stay as-is (still attempted nightly, still failing until a TCC fix
materializes some other way, still harmless to leave running since it's
non-destructive on failure).

---

## 4. `backup_freshness_check.sh` — fix scoped, not optional, doesn't need DECOM-SILENCE lifted

Confirmed two independent breaks, both real, both older than this
incident:

- **`REMOTE_HOST="192.168.1.168"`** — still hardcoded to dead olliemax,
  never repointed when `offhost_backup.sh` moved to the X9 on 08-27.
  `logs/backup_freshness_check.log` shows `[ALARM] off-host snapshot: no
  snapshot found at all` **every night back through at least 2026-08-23**
  — predating this whole incident. It's been alarming nightly for the
  wrong reason for over a week.
- **`ntfy_post()` is a stubbed no-op** (`# DECOM-SILENCE 2026-07-19 —
  suppressed ahead of Gate 2 full removal` / `return 0` before doing
  anything) — so even the correct alarm firing every night has delivered
  exactly zero notifications anywhere, visible only by reading the log
  file directly.

**What it'd take to get a working alert channel — nothing needs
building, one already exists and is live.** `engine/alert_channels.py`
carries a **Pushover** sender (`_send_pushover`, reads creds from
`/usr/local/etc/pushover.env` — confirmed present, 182 bytes, real
creds) that's explicitly carved out as an exception to DECOM-SILENCE://
its own comment says "silenced per DECOM-SILENCE 2026-07-19; this
restores delivery for critical alerts alone." `send_alert(...,
AlertLevel.RED_ALERT, ...)` routes to it. This is not theoretical —
`scripts/origin_healthcheck.sh` already uses exactly this pattern (a bash
script shelling out to a `.venv/bin/python3 -c "..."` one-liner that
imports `engine.alert_channels.send_alert`) for its own restart alerts,
confirmed live-verified per `docs/XO_BACKLOG.md` (`send_alert` returned
`{'ntfy': False, 'browser': True}` in its own test — correctly suppressed
on ntfy, delivering elsewhere).

**Proposed fix (not applied):**
1. Repoint the freshness check's remote target to whatever Option B
   destination gets approved (once it exists) — same SSH-based
   `age_hours_of_newest` pattern the script already has for the local
   side, pointed at the new host instead of dead olliemax. Until a
   target exists, have it check the **X9 path directly** instead (it's a
   local mount, no SSH needed — `stat` the X9's `data/trader.db` mtime
   the same way the local-dir check already works) so the alarm reflects
   the one real off-host copy that currently exists, instead of alarming
   on a dead host for the wrong reason. This part doesn't need to wait on
   the Option B hardware decision and could ship tonight.
2. Replace the neutered `ntfy_post()` calls with the same bash-to-python
   `engine.alert_channels.send_alert(..., AlertLevel.RED_ALERT, ...)`
   shellout `origin_healthcheck.sh` already uses. No DECOM-SILENCE change
   needed — Pushover already has the RED_ALERT carve-out and working
   creds.

This closes the "alarm shares a failure mode with what it watches"
problem (the doctrine in `CLAUDE.md`'s "Alarms must not share a failure
mode with what they watch") for real this time — it wasn't actually
following that doctrine before; it just looked like it was.

---

## 5. Verification bar

Unchanged: not done until a real cron-fired run produces a real artifact
at the actual new target, not "the code looks right." Concretely, once a
target and the freshness-check fix are both approved and applied:

1. **Freshness-check fix** can be verified tonight regardless of the
   Option B hardware decision — force a run (or wait for 20:45 MST),
   confirm the log shows the corrected target and a real Pushover
   delivery (or correct silence if genuinely fresh), not just "no more
   `192.168.1.168` string in the file."
2. **Option B backup target itself** — genuinely can't be verified
   tonight unless (1) an existing machine gets brought back online today,
   or (2) a stopgap manual sync is run in the meantime. If a target
   exists by tonight's 20:30, same bar as before: don't wait for the
   real slot — fire a temporary near-term crontab test first, confirm
   `SUCCESS` in the log and the file actually landing on the new host,
   remove the temp entry, then let the real 20:30 slot run normally and
   check it too. If no target exists yet, tomorrow — or whenever (i)/(ii)
   above gets decided — same verification standard applies then.
3. Either way: independently re-check the copied files (`sqlite3 ...
   PRAGMA integrity_check`) from a session other than the one that ran
   the backup, not trusting the script's own self-report alone.

**Exposure, unchanged since last report:** newest good offhost copies of
`trader.db`/`signals.db` are 2026-08-27 13:20; dated snapshots
2026-08-27 through 2026-09-01 never crossed. Six nights, still open.

## Waiting on you

- Is `desktop-dell17` or `minimac` realistically bringable back online,
  or should I scope option (ii) (small always-on VPS) in more detail
  (provider choice, monthly cost, one-time setup steps) as the default
  path?
- OK to ship the freshness-checker fix (item 4) tonight on its own,
  independent of the Option B target decision, since it doesn't need one?
