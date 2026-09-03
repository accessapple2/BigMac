# Off-host backup: SSH-loopback TCC workaround, shipped and live-verified

Follow-on to `relay_2026-09-02_offhost-backup-tcc-diagnosis.md` and
`DESIGN_2026-09-02_offhost-backup-option-b.md`. That diagnosis identified
macOS TCC (Removable-Volume gate) as the root cause of `offhost_backup.sh`
failing every night from cron since 2026-08-27, and proposed granting Full
Disk Access to `/bin/bash` via Screen Sharing as the primary fix. This
relay records what happened when that was tried, why it didn't work, the
SSH-loopback workaround built instead, and its live cron-fired
verification — including a real concurrency bug found and fixed along the
way.

---

## 1. The FDA-to-`/bin/bash` grant did not fix cron

Captain granted Full Disk Access to `/bin/bash` directly at the machine.
The next `20:30` cron-fired run failed with the byte-identical
`Operation not permitted` / `authorization denied` signature as all six
prior nights — no change.

**Why:** Captain independently read `TCC.db` directly (read-only) and
confirmed `kTCCServiceSystemPolicyAllFiles` has **no entry at all** for
`/usr/sbin/cron` — it was never granted anything, which is exactly why a
grant on `/bin/bash` had no effect. macOS TCC attributes a cron-launched
child's file access to the **responsible process**, which for a plain
`cron → bash → rsync` chain is `cron` itself, not the interpreter it
launches. Granting FDA to bash never touched the actual gap.

The same `TCC.db` read showed **exactly one** entry with
`auth_value=2` (allowed): `/usr/libexec/sshd-keygen-wrapper` — the
process macOS attributes responsibility to for a Remote Login (SSH)
session. This is why every "interactive" test in the original diagnosis
succeeded: those sessions were themselves SSH sessions, inheriting that
grant through the responsible-process chain. Bash run directly by cron
never touches sshd at all, so it never gets it.

**Physically granting FDA to `/usr/sbin/cron` instead** would be the
supported fix (same mechanism, correct target) — but bigmac is headless,
no display is attached, and getting one attached was judged not worth it
for this. That rules out the GUI path entirely for now.

---

## 2. SSH-loopback workaround — built and shipped

Have cron invoke the backup through an SSH call to `localhost` instead of
running it directly, so the process tree's responsible party becomes
sshd (which has the grant) instead of cron (which doesn't).

**What was added, all local to this box, nothing in the repo touches
secrets:**

1. **Dedicated keypair** — `~/.ssh/bigmac_loopback` (ed25519), matching
   the existing per-host convention (`ollie_box`, `ollie_max`), comment
   `bigmac->bigmac-loopback-20260903`.
2. **`~/.ssh/authorized_keys`** — new entry, restricted:
   `from="127.0.0.1",no-agent-forwarding,no-X11-forwarding,no-port-forwarding`.
   Loopback-only; forwarding disabled since the backup job needs none of
   it. (Considered also forcing `command=` to pin this key to only ever
   run `offhost_backup.sh` — left out to match the Captain's exact spec,
   which called out only the `from=` restriction; easy follow-up if
   tighter scoping is wanted later.)
3. **`~/.ssh/config`** — new `Host localhost` block, `HostName 127.0.0.1`
   (forced, so it can't accidentally resolve to `::1` and miss the
   `from=` restriction), `IdentityFile ~/.ssh/bigmac_loopback`,
   `IdentitiesOnly yes`.
4. **`~/.ssh/known_hosts`** — added the `127.0.0.1` host-key entry
   explicitly via `ssh-keyscan` (not blanket `StrictHostKeyChecking=
   accept-new`) and confirmed byte-for-byte it's the same key already
   trusted under the `localhost` name — same sshd, not a mismatch.
5. **Crontab, real `20:30` line** — was
   `/bin/bash .../offhost_backup.sh`, now
   `ssh -tt -o BatchMode=yes localhost 'bash .../offhost_backup.sh' >> .../offhost_backup.log 2>&1`.
   `-tt` (not single `-t`) per the ssh man page's own guidance for
   exactly this case — forces pty allocation even with no local tty,
   which cron never has. The outer redirect on the ssh call itself
   (rather than relying solely on the script's internal log redirect)
   is deliberate — see §5, detection.

---

## 3. A real concurrency bug, found and fixed the same day

Not part of the TCC work directly, but the same day's testing exposed
three unlocked concurrent runs on 2026-09-02 that interleaved their log
output and raced rsync writes against the same X9 destination files.
Added a lock to `offhost_backup.sh` (no `flock(1)` on this box — macOS
ships none — so an atomic `mkdir`-based lock instead, PID recorded
inside, `trap ... EXIT` cleanup).

**First version had a real TOCTOU race**, caught live during testing:
between a winner's `mkdir "$LOCKDIR"` succeeding and its very next line
writing its own pid into it, a second process arriving in that narrow
window would read an **empty** pid file, misclassify that the same as "a
dead process's stale pid," and reclaim (delete + recreate) the lock out
from under the still-running legitimate holder. Live symptom: the lock
directory was observed briefly absent while its recorded holder (pid
83839) was still genuinely running.

**Fixed:** empty/unreadable pid now means "ambiguous, holder mid-start —
treat as busy, skip" — reclaim only fires for a pid that is **present
and confirmed dead** via `kill -0`. A false skip costs nothing (the next
`*/5` cron tick, or tomorrow's `20:30` slot, picks it up); a false
reclaim causes exactly the double-run bug this guard exists to prevent.
Verified the fix directly: manufactured the exact race state (lock dir
present, empty `pid` file) and confirmed the script now logs `SKIPPED:
lock dir present, pid not yet readable` and leaves the manufactured dir
untouched, rather than deleting it.

---

## 4. Live verification — real, unattended cron ticks, not shell tests

Per explicit instruction: verified from cron, not from an interactive
shell. A temporary `*/5 * * * *` entry ran the exact SSH-loopback command
alongside the real `20:30` line; watched for a genuine tick rather than
launching one, then removed the temp entry once confirmed.

- **`06:00:00` tick (unattended, real cron fire):** `[OK] signals.db`,
  `[OK] daily-backups (2)` — no `Operation not permitted`, no
  `authorization denied`. `[FAIL] trader.db` is unrelated and expected —
  `db_snapshot.sh` (still scheduled `20:15 MST`, unchanged) hadn't run
  yet at 06:00 AM, so today's snapshot didn't exist to copy; this has
  nothing to do with TCC and will not recur at the real `20:30` slot.
- That same run's **integrity check completed `FAIL_COUNT=0`** —
  confirming the *read* side (`sqlite3 PRAGMA integrity_check`, denied
  with `authorization denied` in the original diagnosis) now succeeds
  too, not just the rsync *write* side.
- **`06:05:00` and `06:10:00` ticks** both correctly `SKIPPED`, citing
  the correct live holder pid, while the first run was still finishing —
  the lock held up under genuine cron-fired concurrency, not just manual
  testing.
- Temporary crontab entry removed after this confirmation; the real
  `20:30` line now carries the SSH-loopback form permanently (§2).

---

## 5. Fragility — read this before assuming it stays fixed

**This is a workaround for a headless box, relying on undocumented Apple
behavior — not a supported API.** SSH sessions bypassing TCC via the
sshd responsible-process attribution is a real, observed, but
Apple-unpublished behavior, and has reportedly been tightened across
macOS versions before. Nothing here guarantees it survives the next
macOS update on this box.

**The supported fix, if it's ever available:** grant Full Disk Access to
`/usr/sbin/cron` directly (same GUI mechanism already used for
`/bin/bash`, just aimed at the correct binary this time) via System
Settings → Privacy & Security, the moment a display is physically
attached to bigmac. **Revisit this then** — the SSH-loopback plumbing
(keypair, authorized_keys restriction, crontab line) can stay in place
harmlessly even after an FDA grant makes it unnecessary, or be rolled
back to the plain `/bin/bash .../offhost_backup.sh` form at that point.

**What "silently stopped working" would look like — two distinct
signatures, not one:**

1. **The TCC exemption itself gets revoked** (Apple tightens the sshd
   behavior in a future update): the nightly log block reappears
   bracketed exactly as before —
   `=== ... offhost_backup START ===` ... `Operation not permitted` /
   `authorization denied` ... `=== FAILURE: ... ===` — the identical
   signature from the original six-night outage. **This is only caught
   automatically once `backup_freshness_check.sh` is actually fixed** —
   its `REMOTE_HOST` is still hardcoded to the dead `192.168.1.168` and
   its `ntfy_post()` is still a stubbed no-op (`DECOM-SILENCE`,
   2026-07-19), both flagged as open in the original diagnosis and
   **still not fixed as of this relay**. Until that's done, a
   recurrence here produces zero live signal, same as the original
   six-night gap did.
2. **The SSH-loopback mechanism itself breaks** (host key change from a
   reimage, the loopback key or `authorized_keys` entry lost, Remote
   Login disabled by a future hardening pass, port 22/23 firewall
   change): the failure happens **before** the script ever starts, so
   there is **no bracketed `=== START ===` line at all** for that
   night's slot — instead a bare, unwrapped line like
   `Host key verification failed.` or
   `Permission denied (publickey,password,keyboard-interactive).` or
   `ssh: connect to host 127.0.0.1 port 22: Connection refused`,
   landing in `logs/offhost_backup.log` via the crontab line's own outer
   redirect (deliberately kept separate from the script's internal
   `exec >>"$LOG"` redirect specifically so an SSH-level failure is
   still visible at all, rather than vanishing before the script's own
   logging even engages). **This is the harder case for the existing
   freshness check to catch by pattern-matching for success/failure
   strings** — worth building `backup_freshness_check.sh` (once fixed)
   to also alarm on "no log line at all with today's date," not only on
   parsing a `FAILURE` block, so a fully-silent SSH-level break doesn't
   read as identical to a quiet successful night.

**Not a failure signature, for the record:** every run (success, skip,
or failure) now ends with a trailing `Connection to 127.0.0.1 closed.`
line — that's ssh's own normal pty-session close message, added by
wrapping the invocation in ssh at all. Expected noise, not a new
anomaly, going forward.

---

## Status

Shipped and live-verified via real unattended cron ticks. Not yet
committed/pushed as of this relay being written — that happens alongside
it. `backup_freshness_check.sh`'s two independent bugs (dead
`REMOTE_HOST`, stubbed `ntfy_post`) remain open from the original
diagnosis and are what stands between "this recurs" and "someone
actually finds out" — flagged again here since §5 depends on it, not
re-scoped in this pass.
