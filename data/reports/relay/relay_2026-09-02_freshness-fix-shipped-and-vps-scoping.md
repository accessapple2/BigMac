# Freshness-checker fix shipped + verified; VPS scoped; six-nights recovery confirmed

**Branch:** exec-pipeline. Follow-up to `relay_2026-09-02_offhost-backup-tcc-
diagnosis.md` and `-option-b-scoping.md`. Per Captain directive: ship the
freshness-checker fix tonight (applied, not just proposed), scope the VPS
option in detail (not dell17/minimac revival), and answer whether the six
missing nights are recoverable. `scripts/backup_freshness_check.sh` is
applied and live-verified. Nothing else was applied — Option B (the actual
off-host target) is still a decision pending your answer below.

---

## 1. Freshness-checker fix — shipped, verified under real cron, one bug found and fixed mid-verification

**What shipped** (`scripts/backup_freshness_check.sh`, staged for commit):
1. Off-host check repointed from dead `192.168.1.168` (olliemax) SSH to a
   direct stat of the Crucial X9 local mount — interim until Option B.
2. `ntfy_post()` (DECOM-SILENCE-stubbed no-op) replaced with `alert_post()`,
   shelling out to `engine.alert_channels.send_alert(..., AlertLevel.RED_ALERT,
   ...)` — the same live, credentialed Pushover path `origin_healthcheck.sh`
   already proved out. No DECOM-SILENCE change needed.

**Verification, in order, all via real temporary crontab entries (added,
fired, confirmed, removed each time — not terminal runs standing in for
cron):**

- `--test` mode (local-only, no X9 involved): fired cleanly,
  `[ALERT_RESULT] {'ntfy': False, 'pushover': True, 'browser': True, 'email':
  False}` — first live confirmation Pushover actually delivers. **Check your
  phone for a "RED ALERT: Backup freshness ALARM: TEST-legacy-backups"
  notification around 14:33 MST** — that's this test, not a new problem.
- First real cron-fired run (14:36 MST): correctly ALARMed — but at that
  moment the X9 had, unexpectedly, spontaneously unmounted itself (`ls
  /Volumes/` showed only Macintosh HD; `diskutil list` still showed the
  physical disk present as `disk6s2`, just not mounted). Remounted it
  (`diskutil mount disk6s2`) and confirmed readable again interactively.
  Noting this separately — worth knowing the X9 doesn't always stay mounted
  on its own, independent of everything else in this saga.
- **Second real cron-fired run (14:40 MST), X9 confirmed mounted at the
  time (`diskutil info` showing `Mounted: Yes`) in this same interactive
  shell: still reported "no snapshot found at all."** This is a genuine new
  finding, not a repeat of the unmount: **cron cannot *read* the X9 mount
  either, not just write to it** — the identical TCC Removable-Volume
  denial, extended to a plain `ls`/glob this time. I'd only verified writes
  (rsync) and one specific read (`sqlite3 PRAGMA integrity_check`, which
  *did* already show this in the original diagnosis — I underweighted that
  data point when designing this fix). The bug: `ls ... 2>/dev/null` can't
  distinguish "TCC silently blocked the read" from "genuinely no matching
  files" — both produce empty output, and the fix as first written claimed
  "no snapshot found at all," which is **false** when snapshots exist and
  cron simply can't see them.
- **Fixed on the spot**: the script now tests directory-readability
  separately (`ls "$X9_BACKUPS_DIR" >/dev/null 2>&1`) before attempting the
  age computation, and alarms with accurate wording when unreadable —
  "directory unreadable from cron (expected — same TCC block... this does
  NOT mean no snapshot exists, only that cron can't see it)."
- **Third real cron-fired run (14:44 MST) confirmed the corrected wording**:
  `[ALARM] off-host snapshot (X9 local: ...): directory unreadable from cron
  (expected -- same TCC Removable-Volume block documented in relay_2026-09-
  02_offhost-backup-tcc-diagnosis.md; this does NOT mean no snapshot exists,
  only that cron can't see it -- verify manually from an interactive shell)`,
  Pushover delivered again.

**You will have gotten three Pushover pings in the last ~15 minutes** (test,
pre-fix real run, post-fix real run) — all real, all expected, all from this
verification pass, not a malfunction or three separate problems.

**Known, permanent consequence, worth setting expectations on now:** because
cron structurally cannot read the X9 at all, this specific check will alarm
**every single night** at 20:45 until Option B replaces it with a genuine
SSH-reachable target — there's no scenario where it can ever report a real
"OK, N hours old" for the X9 leg from cron. That's honestly the correct
behavior (there genuinely is an unverifiable off-host gap right now), but
it means a nightly RED_ALERT Pushover ping is coming until Option B lands,
not a one-time notice. Flagging so it reads as "expected, still open" each
night rather than "new problem" or "the fix didn't work."

**Local snapshot side, for contrast:** correctly reports `[OK] local
snapshot (data/backups): newest snapshot is 18h old` every run — that leg
works fine, no TCC involved (plain local directory, not a Removable Volume).

---

## 2. VPS option, scoped in detail

Storage math first (measured directly, not estimated): a daily
`trader_YYYY-MM-DD.db` snapshot is **1.10 GB**; `signal-center/signals.db`
is currently **2.15 GB**. 14-day retention → **~17.6 GB steady-state**
footprint for what `offhost_backup.sh` actually copies each night (14
dailies + the live signals.db copy). With 2x headroom for `signals.db`
growth over the coming months: **~35 GB** is a comfortable safety margin,
not a hard requirement.

**Two shapes of "VPS," genuinely different fit:**

**(a) Hetzner Storage Box** — purpose-built backup storage, not a general
server. BX11 (1 TB) runs **~€3.20-3.81/mo (~$3.50-4.15 USD)**, no minimum
contract, cancel anytime. Natively speaks rsync-over-SSH, SFTP, SCP — the
exact transport this design needs. 1 TB is ~57x the steady-state footprint
computed above; storage was never going to be the constraint here.
**Catch:** it's not a general-purpose shell — SSH access is
transfer-restricted (rsync/sftp/scp only, no arbitrary remote command
execution, no `sqlite3` installed remotely). The *old* olliemax design's
"integrity check (remote)" step (`ssh $HOST "sqlite3 ... PRAGMA
integrity_check"`) can't be ported as-is — would need to drop the remote
check and rely on what already happens today: `db_snapshot.sh` already runs
`PRAGMA integrity_check` on the LOCAL snapshot the moment it's created,
before anything gets copied anywhere, so the source is always known-good
pre-transfer; rsync's own protocol-level checksums catch corruption in
transit. A separate remote re-verification is a nice-to-have on top of
that, not the only integrity guarantee, so losing it is a real but small
downgrade, not a gap.

**(b) Small general-purpose VPS** (Hetzner CPX11 ~€4.51/mo (~$4.90 USD): 2
vCPU/2GB/40GB NVMe/20TB transfer; or Vultr's $5/mo: 1 vCPU/1GB/25GB
NVMe/1TB transfer) — full shell, so the remote-integrity-check step ports
over unchanged from how olliemax worked. Costs about the same as (a) or a
little more, but is now "a Linux server you own" — OS patching, SSH
hardening, security updates become an ongoing (if light, `unattended-
upgrades`-automatable) responsibility that doesn't exist today anywhere in
this project. Checked: there's no existing pattern/tooling in this repo for
maintaining a remote Linux box (olliemax was physical hardware someone
managed directly, not a cloud instance with its own patch cadence) — this
would be new operational surface, not a revival of an existing habit.

**Recommendation: (a), the Storage Box.** It's purpose-built for exactly
this (rsync/SSH backup target), cheaper, and — critically — needs no ongoing
OS maintenance at all, which matters more here than the lost remote
integrity-check convenience (already backstopped by the local pre-transfer
check). Script changes needed either way are small and the same shape:
`offhost_backup.sh`'s `run_rsync()` already speaks plain rsync-over-SSH —
this is pointing `REMOTE_HOST`/`REMOTE_DIR` at the new target plus a fresh
SSH key (`~/.ssh/config` already has a per-host key convention —
`ollie_box`, `ollie_max` — a new dedicated key fits that pattern), and, for
(a) specifically, deleting the old remote-`sqlite3` block from the
integrity-check section. Rough total setup: sign-up + key exchange (~15
min), script edit (~20 min), first live-verified run (however long tonight's
20:30-equivalent test takes). Not scoping further or picking a specific
provider account without your go-ahead — this is a real signup + a real,
if small, recurring charge.

**S3-style object storage (Backblaze B2, etc.)** — considered, not
recommended for this. Genuinely cheaper per-GB at scale (irrelevant here,
we're talking single-digit GB) and zero server to maintain like (a), but
needs new tooling (`rclone`/`aws`-cli — confirmed neither is installed)
plus a real rewrite of `offhost_backup.sh`'s transfer logic (object storage
has no directories/mtimes the way rsync's delta-transfer and this script's
whole design assumes) for no benefit over (a) at this scale. Bigger lift,
same outcome.

---

## 3. The six missing nights — recoverable, verified, not lost

**`trader.db`: fully recoverable, all six nights, already integrity-verified
twice over.** Checked `data/backups/_archive/` directly rather than
assuming from the offhost gap: every one of 08-27 through 08-31 is sitting
there as a compressed dated snapshot (`db_snapshot.sh`'s own 14-day
retention moves anything beyond the KEEP window there — compressed, never
deleted, per the sacred-data "archive not delete" rule):

```
08-27: FOUND trader_2026-08-27.db.gz
08-28: FOUND trader_2026-08-28.db.gz
08-29: FOUND trader_2026-08-29.db.gz
08-30: FOUND trader_2026-08-30.db.gz
08-31: FOUND trader_2026-08-31.db.gz
09-01: not yet archived — still the live file at data/backups/trader_2026-09-01.db (within the 14-day KEEP window, hasn't rolled into _archive/ yet)
```

Every one of these was already integrity-checked at *creation* time by
`db_snapshot.sh` itself (`logs/db_snapshot.log` shows `integrity_check=ok`
for all six, back-to-back, no exceptions). Independently re-verified two of
them tonight rather than trusting that log alone: decompressed
`trader_2026-08-27.db.gz` (oldest of the six) and ran `PRAGMA
integrity_check` fresh — `ok`, 2,773 rows in `trades`, real data, not a
truncated/corrupt file. Also re-checked `trader_2026-09-01.db` directly
(the not-yet-archived one) — `ok`. **The exposure here was never "six
nights of trader.db lost" — it was "six nights without a SECOND (off-host)
copy," while the primary local copy stayed intact and verified the entire
time.** Smaller than the original framing suggested, exactly the outcome
you were checking for.

**`signal-center/signals.db`: genuinely no point-in-time history for this
window — or ever.** Different situation, not a gap specific to this
incident: `db_snapshot.sh` only ever snapshots `trader.db` — confirmed by
reading the script directly, it has no `signals.db` path at all. The *only*
backup mechanism `signals.db` has ever had is the live-file rsync copy
inside `offhost_backup.sh`, and that's exactly the copy that's been failing
— stuck at 2026-08-27 13:20, same as before. There is no local dated-
snapshot series for `signals.db` to fall back on, for these six nights or
any night before them. The live file itself is healthy right now (checked:
`PRAGMA integrity_check` → `ok`, actively being written, current) — so
there's no active data-loss risk today — but if the local disk were lost,
recovery for `signals.db` would jump straight back to 08-27 13:20 with zero
granularity in between, a materially different (and worse) risk profile
than `trader.db`'s fully-intact nightly series. Worth its own decision
eventually (a `db_snapshot.sh`-style dated-snapshot regime for
`signal-center/signals.db` doesn't exist and could be added cheaply,
independent of the off-host question) — not something this pass fixes,
flagging it because you asked to know the real state rather than discover
it later.

(Note: `trader.db` itself also holds ~20 internal `*_signals` tables —
`aladdin_signals`, `bk_avwap_signals`, etc. — those ARE fully covered by the
nightly snapshot regime; the exposure above is specifically about the
standalone `signal-center/signals.db` file, a different database.)

---

## Waiting on you

- OK to proceed with Hetzner Storage Box (option a) as the Option B target,
  or want the general-VPS route (b) instead for the intact remote-shell
  integrity check? Either way I'll scope the exact signup/setup steps once
  you say go — not creating an account or spending anything without that.
- Understood the nightly 20:45 RED_ALERT Pushover ping is now a standing,
  expected thing until Option B lands — say if you'd rather I mute/adjust
  its cadence in the meantime (e.g. weekly instead of nightly) rather than
  leave it as-is.
