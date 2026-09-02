# DESIGN — Off-host backup Option B (Hetzner Storage Box)

**Status: PROPOSAL — not implemented.** Per Captain directive: design doc
first, implementation after explicit approval. Answers the three
pre-questions below, then the concrete design. Verification standard
carried forward unchanged from the freshness-checker fix: not done until
the real job produces one real artifact on the real target, restored and
checked — not code that looks right.

---

## 1. Was the X9 failure fixable? Stated plainly.

**No permission/ACL bug on the drive. Re-verified again just now, not
assumed from the earlier session:**

```
$ df -h "/Volumes/Crucial X9"
/dev/disk6s2   931Gi   475Gi   456Gi    52%   /Volumes/Crucial X9
$ ls "/Volumes/Crucial X9/OLLIETRADES_BACKUPS/" >/dev/null 2>&1 && echo OK
READ: OK
$ touch "/Volumes/Crucial X9/OLLIETRADES_BACKUPS/.recheck" && echo OK
WRITE: OK
```

**456 GiB free** (your 461 GiB figure — same ballpark, minor measurement-
timing drift, not a discrepancy worth chasing), mounted `noowners` (macOS
ignores on-disk permission bits on this volume entirely), and both read and
write succeed instantly from an interactive shell, same user, same paths
`offhost_backup.sh`'s cron invocation fails on every night. **The drive,
its filesystem, its permissions, and its ACLs are not the problem — this
was proven directly, not inferred.**

**What's actually blocking it is macOS TCC** (Transparency, Consent &
Control) — a distinct, OS-level security policy layer that sits *above*
and independent of Unix file permissions, specifically gating a background
process's (cron's) access to Removable Volumes. It denies both writes
(`rsync`: `Operation not permitted`) and reads (`sqlite3`/`ls`: confirmed
this session too — `authorization denied`, and a plain directory listing
that came back empty while the drive was provably mounted and readable
from this same interactive shell at the same moment).

**Is it fixable?** Yes, in principle — Apple provides exactly one supported
unlock: granting Full Disk Access in System Settings → Privacy & Security,
a GUI-only action. **We don't have a working GUI on this box right now.**
Screen Sharing connects and authenticates (confirmed reachable, confirmed
socket-activated and live), but the physical framebuffer is black — no
active console session to mirror, `caffeinate` didn't wake one into
existence. That's an access-tooling limitation, not a drive or software
fault.

**Deliberately not attempted, and why:** editing `TCC.db` directly (via
`sqlite3` as root) to force-grant the permission without the GUI. The
system-level `TCC.db` is SIP-protected — even root can't write it without
disabling System Integrity Protection, which itself requires physical
Recovery Mode access (the same "no working display" problem, one level
deeper, plus a much bigger, harder-to-reverse decision). The user-level
`TCC.db` is technically writable but explicitly unsupported by Apple and
actively hardened against exactly this kind of external modification on
current macOS — attempting it risks an unreliable result or a corrupted
consent database for no reliable gain. Not something to try without your
explicit sign-off given the risk/reversibility profile, and not needed —
Option B avoids the whole category of problem by construction (network
transport isn't TCC-gated at all, confirmed live via the cron-fired SSH
test to github.com in the prior session).

**Bottom line for the record: going remote is a choice, not a forced move
from a broken local mount.** The X9 works fine by hand today, and would
work fine from cron too the moment either (a) someone gets physical hands
on the machine, or (b) the Screen-Sharing-black-screen problem gets solved
separately. Both remain open, standing options — not foreclosed by this
design, just not what unblocks tonight.

**Does X9 stay or get retired?** **Stays**, per your standing instruction
from the prior session — nothing here changes that. It holds real archive
data (still the deepest local history via `data/backups/_archive/`),
still works perfectly from an interactive shell, and stays available for
manual/on-demand syncs. What changes is only that cron stops depending on
it as the automated nightly off-host leg — Option B takes that role.
`offhost_backup.sh`'s existing X9 write attempt is left exactly as-is
(harmless nightly failure, already understood, not touched by this
design) unless you want it removed for log cleanliness — your call,
didn't assume either way.

---

## 2. signals.db addition, scoped concretely — space and time, measured live

**Correction to the reference figure first:** the "~155 MB gzipped" number
traces to `signal-center/signals.db.pre-tz-2026-06-02.gz` — confirmed that
file is real and exactly **162,529,865 bytes (155.0 MiB)**, so the figure
itself wasn't wrong. But it's three months stale: that backup predates
significant growth. Ran a fresh compression test against **today's** live
`signals.db` (2.15 GB) rather than trust a 3-month-old ratio:

```
$ time gzip -c signal-center/signals.db > /tmp/test.gz
gzip ... 23.05s user 0.31s system 99% cpu 23.461 total
-rw-r--r--  399,588,863 bytes
```

**Today's real numbers: 2.15 GB live → 399.6 MB compressed (5.4:1, an 81%
reduction), 23.5 seconds to compress on this machine.** Not 155 MB — that
ratio held when the DB was smaller; at today's size the compressible
fraction is proportionally lower. This will keep growing as `signals.db`
grows (it's a live, continuously-written DB with no local point-in-time
snapshot regime today — flagged as its own separate gap in the last relay
doc, not solved by this design).

**Nightly transfer payload, this design:** `trader.db`'s existing daily
snapshot (1.05 GB, uncompressed, unchanged from today's design) +
`signals.db` compressed fresh each night (399.6 MB today, will grow) ≈
**~1.45 GB total per night.**

**Upload bandwidth — measured live, not assumed:**

```
$ networkQuality -v
Uplink capacity: 248.242 Mbps
Downlink capacity: 482.869 Mbps
```

248 Mbps ≈ 31 MB/s theoretical ceiling on this connection specifically (not
a generic estimate). Real-world sustained throughput over a single
SSH-encrypted rsync stream typically runs 60-85% of that depending on
cipher and remote-side variance — call it a working range of **15-25
MB/s**, with the one genuinely unverified variable being the *remote
target's* inbound rate, which can't be measured until an account exists
(Hetzner's Storage Box product page advertises "unlimited traffic," which
speaks to no overage billing, not a throughput guarantee).

**Time budget:**

| Step | Duration |
|---|---|
| Compress `signals.db` (measured) | 23.5s today, scales with DB growth |
| Transfer ~1.45 GB @ 15-25 MB/s (this connection's realistic range) | 58s-97s |
| Transfer ~1.45 GB @ a pessimistic 5 MB/s (remote-side bottleneck, untested) | ~290s (4.8 min) |
| **Total, realistic case** | **~1.5-2 min** |
| **Total, pessimistic case** | **~5-6 min** |

**Does this push past the 20:30 cron boundary or into market hours? No,
by a wide margin either way.** `backup_freshness_check.sh` fires at 20:45
— a 15-minute gap after `offhost_backup.sh`'s 20:30 start. Even the
pessimistic 5-6 minute estimate leaves ~9-10 minutes of slack. Market open
is ~10 hours away (06:30-07:00 MST) regardless of which estimate holds —
there's no scenario at this payload size and this connection's measured
bandwidth where this threatens either boundary. If the real remote-side
rate turns out far worse than the pessimistic case above once tested
against the live target, that would be new information worth another look
— but nothing in tonight's numbers suggests it.

**Retention decision, presented for your call, not assumed:**

- **(A) Mirror-only (recommended for this pass)** — compress and transfer
  the single latest `signals.db` each night, overwriting the same remote
  filename, matching today's design exactly (live-file-copy, no dated
  series) just compressed and pointed at the new target. Minimal script
  change, no new local retention logic, no new storage growth beyond ~400
  MB (today) staying roughly flat on the remote side.
- **(B) Dated series, matching `trader.db`'s pattern** — a new
  `signals_YYYY-MM-DD.db.gz` per night, 14-day retention. Closes the
  point-in-time gap flagged last time (right now `signals.db` has *zero*
  history anywhere, only "current" or nothing). Costs ~14 × ~400 MB ≈ 5.6
  GB steady-state (trivially small against 1 TB) and a new local
  snapshotting step (a `signals`-flavored sibling to `db_snapshot.sh`,
  which doesn't exist today) — more script surface, a real but small
  addition.
- **(C) — CHOSEN. Fully scoped below, not incidental.** Hetzner Storage Box
  server-side automated snapshots, confirmed against Hetzner's live docs
  (not the earlier "up to 10, unverified" placeholder):

  - **Slot count is plan-tied, not universal:** BX11 = **10 slots**, BX21 =
    20, BX31 = 30, BX41 = 40. On BX11 (the plan recommended in §3), that's
    **10 rolling snapshots**. Manual and automatic snapshots draw from the
    same slot pool per-plan; oldest is auto-deleted the moment a new one is
    taken past the limit. Snapshot storage counts against the box's normal
    capacity — no separate billing line.
  - **Schedule is configurable, not fixed:** Console → Storage Box →
    Snapshots tab → "Automatic" → set frequency (daily/weekly/monthly) and
    time-of-day, plus the slot count to use (up to the plan max). **Set
    this to fire daily at a time comfortably after the 20:30-20:45 rsync
    window closes** — e.g. 21:00 MST — so each snapshot captures that
    night's fully-written mirror, never a mid-transfer file. Getting this
    ordering wrong (snapshot before rsync completes) would silently
    snapshot yesterday's data twice and cost a day of real history — worth
    double-checking once the box exists, before trusting the rotation.
  - Sources: [Snapshots](https://docs.hetzner.com/storage/storage-box/snapshots/),
    [Creating snapshots](https://docs.hetzner.com/storage/storage-box/getting-started/creating-snapshots/).

  **Restore procedure — documented now, not left for the night it's needed:**

  1. One-time setup: Console → Storage Box → Snapshots → toggle **"Display
     snapshot directory"** on (off by default). No effect on normal
     operation.
  2. Snapshots then appear as plain, browsable, **read-only** subfolders at
     `/home/.zfs/snapshot/<snapshot-name>/` when connected over SSH/SFTP on
     port 23 (the `/home/` prefix is specific to the SSH/rsync protocol
     path this design already uses; `.zfs/snapshot` alone is the path over
     other protocols). Each subfolder is a full point-in-time mirror of the
     box's directory tree at that snapshot's timestamp.
  3. **To restore `signals.db` (or `trader.db`) from N nights ago:** connect
     normally (`sftp -P 23` or `rsync` over the existing `hetzner-storage`
     SSH config entry), `cd` or path into
     `/home/.zfs/snapshot/<snapshot-name>/`, and copy the file out — a plain
     `get`/`scp`/`rsync` pull, exactly like fetching any other file on the
     box. **No console click, no full-box restore, no downtime**, because
     this is a normal read against a read-only directory. Decompress and
     `PRAGMA integrity_check` locally after pulling, per the verification
     standard below.
  4. **Do not use the Console's whole-box "restore snapshot" action for
     this.** That's a separate, destructive operation — Hetzner's own
     warning: *"When you restore a snapshot, any data created after the
     snapshot was taken — including data from newer snapshots! — will be
     deleted."* It resets the entire box to time X and wipes everything
     newer, snapshots included. **Never needed for our case** — per-file
     pull from the read-only directory in step 3 is the only restore path
     this design relies on. The whole-box action exists only as a
     last-resort disaster path (e.g. box-level corruption), not a routine
     recovery tool, and should not be reached for casually.
  - Source: [Snapshots](https://docs.hetzner.com/storage/storage-box/snapshots/)
    (confirmed against the **Storage Box** product docs specifically — an
    earlier pass in this research briefly pulled from Hetzner's *Storage
    Share* product docs instead, a different Nextcloud-based offering with
    no per-file restore at all; that page's info does not apply here and is
    not used in this design).

  **Does (C) satisfy the original signals.db point-in-time requirement?**
  **Yes, with one honest caveat on granularity.** Combined with (A)'s
  nightly mirror-and-overwrite, a correctly-sequenced daily snapshot turns
  every night's transfer into a durable, independently-restorable
  checkpoint — `signals.db` goes from *zero* history (today's real gap) to
  **10 rolling daily checkpoints** on BX11, for no added local script
  surface. The caveat: this is coarser than (B)'s purpose-built 14-day
  dated series — 10 days vs. 14, and the retention knob lives in Hetzner's
  Console rather than this repo's scripts, so it's not visible to
  `git log` or local tooling the way (B) would be. That trade — less
  configurability and shorter depth, in exchange for zero new local code —
  is what makes this "(C) on top of (A)" rather than a full substitute for
  (B); if 10 days ever proves too shallow, BX21 (20 slots, still cheap)
  or a future move to (B) both remain open.

**Decision: (C) on top of (A).** Nightly mirror-and-overwrite (A) stays the
transfer mechanism; Hetzner's server-side automatic snapshots (C) supply
the point-in-time recovery layer on top of it, per the restore procedure
above. (B)'s purpose-built dated series remains a real, separate option if
10-day depth ever proves insufficient — not pursued now given (C) closes
the immediate gap at effectively zero implementation cost.

---

## 3. Exact account/payment setup — you handle this, in order

Recommending **Hetzner Storage Box, plan BX11 (1 TB)** — unchanged
recommendation, storage math above still leaves ~2,500x headroom on a 1 TB
box even with (B)'s dated series. Confirmed current pricing directly
rather than citing from memory: **€3.20/month + VAT** (VAT typically
doesn't apply to non-EU customers — expect close to flat €3.20 ≈ $3.45-4
USD depending on exchange rate/card FX), no setup fee, no minimum
contract, hourly billing capped at the monthly max (cancel any time,
prorated), unlimited traffic (no bandwidth-overage billing risk).

**Steps, in order** (sourced from Hetzner's current docs; I have not
personally walked the live signup form this session — treat exact
button/field wording as approximate, the sequence and requirements are
accurate):

1. **Create a Hetzner account** at hetzner.com / console.hetzner.com —
   email + password, standard account creation.
2. **Add a payment method** — credit card or PayPal are Hetzner's standard
   options.
3. **Order a Storage Box, plan BX11** via the Hetzner Console. You'll pick
   a datacenter location — Storage Box locations are EU-based (Germany or
   Finland); this adds some latency versus a US host, irrelevant for a
   once-nightly batch transfer where bandwidth, not latency, is what
   matters (measured above).
4. **Supply an SSH public key at creation time, if possible** — the docs
   are explicit that this is easiest done *during* order creation; adding
   one after the box exists requires a different, SSH-based install
   command rather than the Console UI. I'll generate a new dedicated
   keypair on this machine first (matching the existing per-host
   convention already in `~/.ssh/config` — `ollie_box`, `ollie_max`) and
   hand you the **public** key to paste in at this step — the private key
   never leaves this machine, nothing for you to transmit.
5. **Enable SSH support** for the box (a setting in the Console — may be
   on by default when an SSH key is supplied at creation, or may need an
   explicit toggle via "Change settings"; confirm at signup). Note for
   later: Storage Box uses **SSH port 23, not the usual 22** — this
   affects the script edit, not your signup steps, flagging now so it
   doesn't surprise either of us later.

**What I'd need from you once it exists:** just the Storage Box's assigned
hostname (`uXXXXX.your-storagebox.de` format) and username (`uXXXXX`) —
both shown in the Console after creation, no password needed if the SSH
key was supplied at step 4. **I don't need, and won't ask for, any
payment or account credential.**

---

## Design summary (pending your approval — nothing below is implemented)

1. `~/.ssh/config` gets a new `Host` entry (e.g. `hetzner-storage`) once
   you hand me the hostname/username, using a freshly generated dedicated
   key.
2. `scripts/offhost_backup.sh`: `REMOTE_HOST`/`REMOTE_DIR` repointed from
   dead olliemax to the new target, port 23 added to the ssh/rsync
   invocations, a `gzip` step added ahead of the `signals.db` transfer
   (mirror-only, per (A) — no dated-series logic needed since (C) supplies
   history server-side), the old remote-`sqlite3`-integrity-check block
   removed (Storage Box's SSH is transfer-only — no remote shell) since
   the source is already integrity-checked locally pre-transfer by
   `db_snapshot.sh`.
3. `scripts/backup_freshness_check.sh`: `X9_BACKUPS_DIR` stat replaced
   with an SSH-based check against the new target (this one *can* use
   real SSH commands, unlike the X9 leg, since it's a normal reachable
   host) — X9's local stat either removed or kept as a secondary/best-
   effort check, your call.
4. **New step, added by the (C) decision:** once the box exists, enable
   automatic snapshots in the Console (Snapshots tab → Automatic → daily,
   10 slots, time set comfortably after the 20:30-20:45 rsync window —
   e.g. 21:00 MST) and toggle **"Display snapshot directory"** on. Both
   are one-time Console settings, not script changes — noted here so this
   step doesn't get silently dropped when the script diff lands.
5. Verification, unchanged standard: a real cron-fired run (temporary
   near-term crontab entry, same protocol as every prior verification this
   week — not a terminal run standing in), confirmed `SUCCESS` in the log,
   the actual file present and correct size on the remote target, then an
   independent restore-and-check — pull the file back down, decompress,
   `PRAGMA integrity_check` — before calling it done. **Extended for (C):**
   after the first automatic snapshot has fired (i.e. not the same night
   the box is created — wait for the first scheduled snapshot to land),
   also verify the restore path itself end-to-end: browse
   `/home/.zfs/snapshot/<name>/`, pull a file back out, confirm it matches.
   A snapshot nobody has confirmed how to retrieve isn't a backup — this
   gets checked once, not assumed from the docs above.

Waiting on: (1) account created and public key pasted in per §3, then
I'll draft the exact script diff for review before applying anything.
Retention question (§2) is now resolved — (C), documented above — no
longer blocking.
