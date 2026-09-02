# Off-host backup failure — diagnosis only, not fixed

**Branch:** exec-pipeline. Six consecutive nights (2026-08-27 through
2026-09-01) of `offhost_backup.sh` FAILURE, first flagged in the 2026-09-02
morning check-in. Per Captain directive: diagnose and report, do not apply
any fix. Nothing in this pass writes to crontab, System Settings, or the X9
volume beyond three throwaway test files created and immediately removed
during live verification (see section 1c).

---

## 1. What exactly is failing

### 1a. The verbatim error, six nights running

Every night since 2026-08-27T20:30:00, `logs/offhost_backup.log` shows the
identical failure (quoting 2026-09-01, the most recent):

```
=== 2026-09-01T20:30:00-07:00 offhost_backup START ===
  [OK] Crucial X9 confirmed mounted (device id 16777238, distinct from /Volumes' 16777233)
rsync(2004): error: /Volumes/Crucial X9/OLLIETRADES_BACKUPS/data/: open: Operation not permitted
rsync(2003): error: unexpected end of file
  [FAIL] trader.db
rsync(2007): error: /Volumes/Crucial X9/OLLIETRADES_BACKUPS/signal-center/: open: Operation not permitted
rsync(2006): error: unexpected end of file
  [FAIL] signals.db
rsync(2014): error: /Volumes/Crucial X9/OLLIETRADES_BACKUPS/backups/: open: Operation not permitted
rsync(2013): error: unexpected end of file
  [FAIL] daily-backups (1)
--- integrity check (local) ---

BAD /Volumes/Crucial X9/OLLIETRADES_BACKUPS/data/trader.db: ERROR: Error: unable to open database "/Volumes/Crucial X9/OLLIETRADES_BACKUPS/data/trader.db": authorization denied
BAD /Volumes/Crucial X9/OLLIETRADES_BACKUPS/signal-center/signals.db: ERROR: Error: unable to open database "/Volumes/Crucial X9/OLLIETRADES_BACKUPS/signal-center/signals.db": authorization denied
FAIL_COUNT=2
=== FAILURE: Off-host backup FAILED: rsync_errors=3 integrity=[BAD ... authorization denied BAD ... authorization denied FAIL_COUNT=2 ] note= trader.db signals.db daily-backups (1) ===
```

Byte-identical pattern on 08-27 (20:30 run only — see 1b), 08-28, 08-29,
08-30, 08-31, 09-01. Only the daily-backups count in parens changes (3, 3,
3, 3, 1, 1) — the number of local dated snapshots that existed to attempt
that night, none of which relates to the failure itself.

### 1b. Which component

**All of them, at two different layers:**
- `trader.db`, `signals.db`, `daily-backups` — fail at the **write** layer:
  `rsync` cannot `open()` the destination directories at all
  (`Operation not permitted`, i.e. `EPERM`, not `EACCES`/"Permission
  denied" — that distinction matters, see 1c).
- The **integrity check** — fails at the **read** layer: `sqlite3` cannot
  even open the already-existing `trader.db`/`signals.db` files that were
  copied over successfully back on 08-27 before this started. Apple's
  `sqlite3` on macOS returns the string `authorization denied` specifically
  for TCC-layer denials — this is not a generic SQLite error string, and it
  never appears in successful runs' output.

The `assert_x9_mounted` guard passes every night (`[OK] Crucial X9
confirmed mounted, device id ... distinct from /Volumes'`) — the drive is
genuinely mounted, this isn't the classic "disk unplugged, writing into a
phantom boot-disk directory" trap the guard exists for.

### 1c. Not a uid/gid/ACL mismatch — confirmed live

Checked the Captain's hypothesis directly rather than assuming it:

```
$ ls -la "/Volumes/Crucial X9/OLLIETRADES_BACKUPS/"
drwx------  1 bigmac  staff  262144 Sep  1 08:06 .
...
$ mount | grep -i crucial
/dev/disk6s2 on /Volumes/Crucial X9 (exfat, local, nodev, nosuid, noowners, noatime, fskit)
```

The volume is mounted `noowners` — macOS ignores on-disk permission bits
for exFAT volumes mounted this way and grants the mounting user effective
owner access regardless of the `drwx------` mode shown. That mode bit is
cosmetic here, not the enforcement mechanism.

Proved this live, from an interactive shell (same `bigmac` uid the cron job
runs as):

```
$ touch "/Volumes/Crucial X9/OLLIETRADES_BACKUPS/data/.claude_write_test"
WRITE OK
$ sqlite3 "/Volumes/Crucial X9/OLLIETRADES_BACKUPS/data/trader.db" "PRAGMA integrity_check;"
ok
$ rsync -a --copy-links --no-owner --no-group /tmp/test.txt "/Volumes/Crucial X9/OLLIETRADES_BACKUPS/data/"
[succeeded]
```

All three test artifacts were removed immediately after; verified clean.
**Same uid, same paths, same commands the script runs — succeed instantly
from an interactive session, fail every night from cron.** This rules out
a filesystem-level permission or ACL problem and points at something
specific to *how the process is invoked*, not *what it's invoked as*.

### 1d. Root cause: macOS TCC, not the filesystem — and there's exact precedent for this on this box

`Operation not permitted` on `open()` of a directory + `sqlite3` returning
`authorization denied` is the signature of macOS's TCC (Transparency,
Consent & Control) layer denying access to a **Removable Volume** (its own
grant category, separate from — but subsumed by — Full Disk Access),
combined with the fact that interactive Terminal/SSH sessions on this box
already carry a Full Disk Access grant that headless daemons never
inherit.

This is not a new failure mode here. `docs/OPS_LOG.md`, **2026-05-07,
HM-AT**, diagnosed the identical pattern for the Schwab CSV watcher
against `~/Downloads/`:

> "The real cause is macOS TCC denying the launchd audit session access to
> `~/Downloads/`. Manual runs (SSH/Terminal) inherit the Full Disk Access
> grant from the host app; launchd's audit session does not."

That entry also recorded, critically: **"GUI fix path unavailable —
bigmac is a headless Mac Mini M4 with SSH-only access."** The System
Settings → Privacy & Security → Full Disk Access grant was judged
infeasible to apply at the time, and the actual shipped fix (HM-AT-β) was
to move the watched directory *out of* a TCC-protected location instead of
granting access to it. That workaround doesn't transfer here — the
Crucial X9 is inherently external; there's no "un-protected" version of an
off-host backup target.

---

## 2. When it started, and what changed

**First failing night: 2026-08-27T20:30:00-07:00** (the very first
`30 20 * * *` cron-scheduled run after the repoint). Full timeline that
day, from the log:

| Time (MST) | Run type | Result |
|---|---|---|
| 13:15:50 | manual (off-schedule) | **SUCCESS** — 4 DBs replicated, 701s |
| 13:52:08 | manual (off-schedule) | **SUCCESS** — 4 DBs replicated, 398s |
| 18:55:26 | manual (off-schedule) | FAIL — but only `trader.db` (no snapshot for today yet); integrity check on existing X9 files still passed, `FAIL_COUNT=0` — read access was still fine at this point |
| **20:30:00** | **cron** (`30 20 * * *`) | **FAIL — first appearance of `Operation not permitted` / `authorization denied`, on every component** |

Every run before 20:30 that day was off the `30 20 * * *` cron slot — almost
certainly interactive verification of the just-edited script (see below).
The 20:30 run is the first time this specific job was ever *cron*-invoked
against the X9. Every failure since has been the identical signature.

**What changed:** the script itself. `scripts/offhost_backup.sh` was
edited to repoint from SSH-to-olliemax (`192.168.1.168`, decommissioned
2026-08-27) to the local Crucial X9 mount. The edit was live on disk
2026-08-27 (confirmed by the log timeline above) but not committed to git
until `e7c3e7d` on 2026-08-29 — it was "found sitting uncommitted during an
unpushed/unmerged-work audit," per that commit's own message.

This fully explains the timing without needing a coincidental OS update or
remount: **the SSH-to-olliemax path never triggered a Removable-Volume TCC
check at all** (network SSH access isn't gated that way) — six-plus months
of this job running fine over SSH never needed this permission. The
2026-08-27 repoint is the first time this specific cron job ever touched a
local external volume, and it has never had the grant a first-time access
like that requires. Nothing broke; a previously-unneeded permission became
needed and was never granted, because cron has no mechanism to prompt for
one.

(There was a reboot earlier the same day, 2026-08-27 10:41–10:42, and
further reboots 08-28 20:08, 08-31 02:32/04:15 since — noted for
completeness, but none of these are required to explain the failure and I
found nothing pointing at a TCC *reset* specifically; the simpler
explanation, a grant that never existed, fits every data point.)

### Freshness alarm cross-check — a confirmed second broken thing, predating this

`scripts/backup_freshness_check.sh` (cron 20:45 MST, 15 min after
offhost_backup) has fired `[ALARM] off-host snapshot: no snapshot found at
all` **every single night back through at least 2026-08-23** — i.e. before
the 08-27 repoint even happened. Its `REMOTE_HOST` variable is still
hardcoded to `192.168.1.168` (dead olliemax) and was never updated when
offhost_backup.sh was repointed to the X9; every night it SSHs a
10s-timeout connection to a decommissioned host, gets nothing back, and
correctly (for the wrong reason) alarms. Separately, its `ntfy_post()`
function has been a stubbed `return 0` no-op since 2026-07-19
(`DECOM-SILENCE`) — so even on a night this alarm would have caught the
*real* problem, nothing would have been pushed anywhere. Both bugs are
visible only by reading `logs/backup_freshness_check.log` directly; there
has been no live signal from this "never again" guard this entire time.
This needs its own fix (repoint `REMOTE_HOST` to the X9 path, and decide
whether to lift DECOM-SILENCE now that there's something worth alerting
on) — flagged, not touched here.

---

## 3. Actual exposure

Newest data that actually made it to the X9, by file mtime (rsync `-a`
preserves source mtimes, so these are genuine "as-of" dates, not copy
dates):

| File | Newest on X9 | Age as of 09-02 ~14:00 MST |
|---|---|---|
| `data/trader.db` | 2026-08-27 13:20 | ~6 days |
| `signal-center/signals.db` | 2026-08-27 13:20 | ~6 days |
| `backups/trader_2026-08-26.db` (newest dated snapshot) | source-dated 2026-08-26 20:15 | ~6 days, and **missing every dated snapshot from 08-27 through 09-01 entirely** — six full nights of dailies never replicated |

That last row is the real exposure: not just "the daily copy is 6 days
stale," but a complete 6-night gap in the dated-snapshot series on the
X9 — `trader_2026-08-27.db` through `trader_2026-09-01.db` do not exist
there at all. If the local box were lost right now, the off-host recovery
point is 2026-08-27 13:20 (data/trader.db) or 2026-08-26 (last dated
snapshot) — whichever is used for recovery, everything since is
unrecoverable from the X9.

**09-01's run, the first after the `[SKIP]-as-failure` fix (337407c,
`HM-OFFHOST-SKIP-AS-FAILURE-2026-09-01`): reported FAIL correctly**, same
as every night before it — `rsync_errors=3`, all three components flagged
`[FAIL]`, overall `FAILURE` line, exit 1. Worth being precise about what
that fix actually did and didn't touch here: it hardens the case where
*zero local source files exist to copy* (e.g. `db_snapshot.sh` itself
stopped producing dailies) so that scenario is no longer silently
swallowed as success. That branch was never actually exercised these six
nights — there was always ≥1 local daily present (1–5, per the counts in
the log). The failure this whole week is the TCC/permission denial
upstream of that logic, on a completely different code path
(`rsync`'s own exit code, and the direct hard-fail branch for
`trader.db`). The fix is real and correctly deployed, it's just not the
fix this particular problem needed.

---

## 4. Proposed fix — not applied

Ruled out by the 1c live test: no filesystem/ACL change needed on the X9
itself; it already grants full read/write to `bigmac`. The fix has to
happen in how cron's process gets TCC authorization, or by routing around
TCC's Removable-Volume gate entirely.

**Option A — Full Disk Access grant to `/bin/bash`, via Screen Sharing.
Confirmed reachable right now — this is the recommended option.** Direct
fix, matches the exact precedent (HM-AT granted FDA to `/bin/bash` for the
same class of problem). 2026-05-07's note called the GUI path infeasible
on this "headless" Mini; re-tested live rather than taking that as still
true:

- `com.apple.screensharing` (VNC/RFB, port 5900) is launchd
  socket-activated — `launchctl print` shows "not running" (0 active
  count, on-disk plist even says `Disabled => true`), but
  `launchctl print-disabled system` shows the runtime override is
  `"com.apple.screensharing" => enabled`, and launchd holds the listener
  socket itself, spawning `screensharingd` on the first real connection.
- Verified live: `nc -z 192.168.1.248 5900` (the box's real LAN IP, not
  just loopback) — **succeeded**. Screen Sharing is reachable on the LAN
  right now, no service needs starting first.
- `bigmac`'s account already carries the `com.apple.access_screensharing`
  group grant, so authentication won't be the blocker either.
- Could not check the Application Firewall's global on/off state
  specifically (`socketfilterfw --getglobalstate` needs `sudo`, and this
  session has no passworded sudo) — but a successful inbound TCP connect
  on the real interface is itself strong evidence nothing is blocking it
  for LAN-local traffic.

Net: the May "infeasible" note likely did mean "nobody was in front of
the machine," as suspected — the service itself isn't even the blocker,
it's live and waiting. Any VNC client (macOS's own Screen Sharing.app,
`vnc://192.168.1.248`, hostname `Steves-Mac-mini`) reaching this LAN
should connect. From there: System Settings → Privacy & Security → Full
Disk Access → **+** → navigate to `/bin/bash` → enable. No script or
crontab change needed. Risk: fragile the same way HM-AT flagged in
2026-05 — any future TCC reset (macOS update, migration) silently
re-breaks this with no alarm (see the freshness-checker gap above — get
that fixed regardless of which option is chosen, so a future recurrence
is at least visible).

**Option B — restore a network-reachable off-host target.** The SSH-based
design ran clean for 3+ months (2026-05-31 → 2026-08-27) with zero TCC
exposure, because network transfers aren't gated by the Removable-Volume
check. Olliemax itself is gone, but if there's any other always-on host
on the LAN reachable by SSH, repointing there sidesteps the problem
entirely rather than working around it. Needs the Captain to confirm
whether such a host currently exists — I'm not assuming one does.

**Option C — do nothing to cron; run the backup interactively/on-demand
instead**, e.g. as a step triggered from an already-TCC-authorized session
(this Claude Code shell already proved it can write and integrity-check
the X9 successfully). Weakest option — reintroduces exactly the kind of
manual dependency the cron automation was built to remove — but cheapest,
and closes the immediate gap tonight while A or B gets sorted.

Do **not** consider migrating cron → launchd a fix on its own: HM-AT's own
diagnosis explicitly found launchd's audit session subject to the same TCC
gate as cron. It would only help if paired with a plist correctly scoped
as a GUI-session LaunchAgent (not a LaunchDaemon) *and* Option A's grant —
strictly more moving parts than fixing cron directly, and cron was chosen
originally for `@reboot`-survival reasons that a LaunchAgent doesn't
share (see `feedback_reboot_survival_gap` in memory — launchd agents tied
to the GUI session die on an SSH-only reboot; cron survives it). Not
recommending this path unless A and B both prove impossible.

### Verification plan (for whichever option is chosen)

Last time's lesson applies directly: a committed fix isn't deployed, and a
deployed fix isn't verified until the actual job produces a real artifact
under its real invocation path — not "the code looks right," not a manual
terminal run (which would trivially succeed today regardless of whether
cron's TCC gap is closed, since interactive access already works).

1. After applying A or B, do not wait until tonight's 20:30 slot to find
   out. Add a **temporary** one-off crontab entry a few minutes out
   (e.g. `*/3 * * * * /bin/bash .../offhost_backup.sh`) to force a real
   cron-context invocation, confirm it in the log, then remove the
   temporary entry — leaves the real `30 20 * * *` line untouched.
2. Confirm the log shows `SUCCESS`, not just an absence of the old error
   string.
3. Confirm the X9 files actually moved: `data/trader.db` and
   `signal-center/signals.db` mtimes update to today; the missing
   `trader_2026-08-27.db` … `trader_2026-09-01.db` gap starts backfilling
   (only forward from tonight — this script has no backfill/replay of past
   dailies, so the 6-night gap itself stays a permanent hole in the
   series unless something explicitly copies those from local `_archive/`).
4. Re-run the integrity check independently (`sqlite3 ... PRAGMA
   integrity_check`) against the freshly-copied X9 files from an
   interactive session, not trusting the script's own self-report alone.
5. Fix `backup_freshness_check.sh`'s `REMOTE_HOST` in the same pass (it's
   pointed at a dead host regardless of which option above is chosen) so
   the alarm actually watches the real destination going forward.

---

## Not tonight, logged for tomorrow

Plutus/BEAR_CROSS regime-router gating — the D-rating deadlock is
provably cleared (zero `BENCH: rating D` rejects since 2026-09-01
19:58:00); today's blocks are `regime_mismatch` (681), `stale_signal`
(524), and `REGIME-ROUTER: long_equity not approved in BEAR_CROSS` (240).
Diagnostic, not urgent — deferred per Captain instruction.
