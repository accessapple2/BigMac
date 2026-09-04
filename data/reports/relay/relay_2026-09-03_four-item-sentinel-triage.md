# Four-item sentinel triage — 2026-09-03 night

All four items diagnosed. Only item 4 (alert spam) was fixed tonight per
the Admiral's scope; items 1-3 are investigate-and-report only, not
reconciled, per explicit instruction not to touch lifecycle drift until
the cause was known and not to spend the night on the probe/disk digs.

---

## 1. mlx-qwen3 probe dead (RED ALERT) — investigated, not fixed

**The probe script itself is not the problem.** `scripts/mlx_qwen3_probe.py`
wraps its work in try/except and unconditionally writes its heartbeat JSON
before exiting — a genuine crash would still leave a heartbeat. Both
`logs/mlx_qwen3_probe.log` (clean, unbroken `healthy=True` lines ending
exactly at 2026-09-02T21:18:34Z, matching the sentinel's reported last-run)
and `logs/mlx_qwen3_probe.err.log` (0 bytes, always has been) rule out a
script-level crash.

**Most likely cause: the launchd job itself stopped being scheduled**, not
that it ran and failed. `com.ollietrades.mlx-qwen3-probe.plist` has
`StartInterval=300` + `RunAtLoad=true`, no `KeepAlive` — if the job were
still loaded it would refire every 5 minutes regardless of the previous
run's outcome, so 30+ hours of silence with a clean prior stop points at
the job being unloaded/booted-out sometime after 21:18:34Z, not a crash
loop.

**Could not confirm live launchd state from here.** Every
`launchctl print gui/501/...` — even for the whole `gui/501` domain —
returns `"Domain does not support specified action"` from this shell, and
`launchctl list` shows zero `com.ollietrades.*` jobs at all (only 74 Apple
system jobs), despite the plist existing on disk and no reboot having
occurred. This matches CLAUDE.md's own documented "LaunchAgent Reboot
Lifecycle" caveat about querying gui/501 from a non-Aqua-session shell —
this Bash tool's session apparently can't see the user's GUI launchd
domain at all, for any job. **This needs a real Terminal.app/interactive
session to check** `launchctl print gui/501/com.ollietrades.mlx-qwen3-probe`
directly and re-`bootstrap`/`load` it if it's genuinely gone.

**Second bug confirmed as suspected.**
`hm_ops_sentinel.py::check_mlx_qwen3_heartbeat()` returns
`{"mlx_qwen3_heartbeat_age_min": ..., "healthy": raw.get("healthy")}` —
`healthy` is read straight from the heartbeat JSON's last-written value,
never gated on the computed staleness. The RED_ALERT itself correctly
fires off `age_min` (that's why tonight's alert exists at all), but the
`healthy` field in the status dict will echo whatever the last successful
probe wrote (`True`, from 30+ hours ago) forever, independent of
freshness — a dead probe reports healthy in the status summary until
someone reads the age_min next to it. Latent since the check was built,
not touched by recent commits (`e9cdd43`, `1be7e15`, `24e0dba`). **Not
fixed tonight** — flagged for a future pass: the fix is straightforward
(`healthy = raw.get("healthy") and age_min <= HEARTBEAT threshold`), just
out of scope for tonight's single approved fix.

---

## 2. Disk 89.6% / 23.8 GiB free (was 83.1% / 38.6 GiB on 09-01) — investigated, not fixed

**None of the three named hypotheses is the main driver:**
- **Concurrent offhost_backup.sh runs — refuted as a local-disk cause.**
  The 09-02 concurrency bug (`relay_2026-09-03_offhost-backup-ssh-loopback-
  fix.md`) raced writes against the remote Crucial X9 volume via rsync,
  not local disk. The lock file (`.offhost_backup.lock`) is tiny; no
  orphaned local temp/partial files found.
- **`.ollama` growth — refuted for this window.** `du -sh ~/.ollama` = 37GB
  total (large, but `ollama list` shows nothing pulled more recently than
  ~4 days ago — nothing landed inside the 09-01→09-03 window).
- **New heavy logging — a real gap, but not the main driver.**
  `logs/trader_error.log` is 109MB and actively growing (`[options_flow_
  gex] HTTP 429 throttled` spam from the known Polygon/Alpaca rate-limit
  storm) and is **not covered by `scripts/rotate_logs.sh` at all** — it
  only rotates `trader.log`, `hm_ops_sentinel_cron.log`, and
  `watchdog_cron.log`. Worth fixing, but 109MB is far short of 15 GiB.

**Actual biggest driver: `db_snapshot.sh`'s retention bump, working as
intended.** The 09-01 `KEEP=7`→`KEEP=14` change (`relay_2026-09-01_four-
item-followup.md`, fixing a silently-broken `ls|sort` retention counter
that had never actually archived anything) is now accumulating toward its
new ~14GiB steady state that didn't exist before. `logs/db_snapshot.log`'s
own free-space precheck shows the decline directly: 09-01 20:15 = 28.9GiB
avail, 09-02 20:15 = 27.0GiB, 09-03 20:15 = 24.9GiB — a steady ~1-2GiB/
night decline as `trader_2026-09-01/02/03.db` (~1.05-1.07GB each,
uncompressed) accumulate. `data/backups/` is 8.3GB, `_archive/` a separate
4.2GB of older compressed snapshots.

**Unresolved gap:** the Admiral's 38.6GiB reading was from earlier in the
day on 09-01, but that same night's 20:15 snapshot log already showed only
28.9GiB avail — roughly 10GiB vanished within 09-01 daytime itself, before
any of the above mechanisms fired. Not explained by anything checked
tonight; a full `du -sh -d1 $HOME` sweep is the natural next step if this
matters precisely, not done tonight (scope was investigate-only).

---

## 3. Lifecycle drift 8 targets (was 2, was `[]` yesterday) — investigated, not reconciled

**Confirmed: a real re-enable event, not a detector bug and not newly-
scoped targets.** Reading `hm_ops_sentinel.py`'s own historical output
line-by-line in `logs/hm_ops_sentinel_cron.log`: the 2026-09-02 14:20:02
tick logged `lifecycle_drift={'job_drift': [], ...}` — clean. The very
next tick, **14:25:03**, logged all 8 targets simultaneously (`crusher`,
`hm-signals-v2-monday-check`, `hm-signals-v2-monday-check-verify`,
`hm-wr-dur-monday-check`, `morning-cd-instr`, `premarket`,
`riker-synthesis`, `ti-picks-watcher`), each `live_disabled: False`. Held
at exactly 8 on every 5-min tick since (361 consecutive occurrences
through tonight 20:25). No `launchctl print-disabled` error appears near
the transition, so the check didn't fail/timeout — it genuinely read
different live content before and after.

**Ruled out:**
- **Detector code change** — no. `git log --since="3 days ago"` shows the
  last commit touching `hm_ops_sentinel.py`/`fleet_lifecycle.py` is
  `ce5d3c3` (2026-09-01 17:37, the unrelated agent-rating fix from that
  day's session). Nothing touched `check_fleet_lifecycle_drift()` around
  09-02 14:20-25.
- **Newly-scoped targets** — no. All 8 already existed in the ledger with
  legitimate `halt`/`retire` entries dated 2026-08-29/08-30
  (`created_by`/action pattern matching real `fleet_lifecycle.py` runs,
  not raw SQL) — the check was already evaluating them and finding zero
  drift for ~2 days before the flip.

**Unresolved: the actual mechanism/actor.** `MAX(created_at)` on the whole
ledger table is 2026-08-31 18:26 — nothing wrote a new ledger row since,
so whatever flipped these 8 jobs live did **not** go through
`scripts/fleet_lifecycle.py` (a manual/scripted `launchctl enable` bypass,
per CLAUDE.md's own warning about what that produces: no record). Found
plist files for all 8 (dated May-July, untouched), no matching commit, no
sweep script in `scripts/*.sh` targeting this exact set, and no reboot in
that window (`last reboot` shows nothing between 08-31 04:15 and now). No
day-of-week correlation despite 3 of the 8 names containing "monday"
(09-02 was a Wednesday). **Best-supported conclusion: something ran a
manual/scripted `launchctl enable` against all 8 labels around 14:20-14:25
MST on 09-02, outside the lifecycle tool, leaving no audit trail** — a
real gap in the "every state change goes through the tool" doctrine.
**Per instruction, not reconciled tonight** — needs the who/what resolved
(or at minimum, Admiral sign-off to reconcile blind) before touching any
of the 8 targets.

---

## 4. Alert spam — FIXED tonight, shipped and confirmed live

**Root cause, confirmed directly against the notifications table.** IDs
30052-30063 were `sentinel_disk_space` and `sentinel_lifecycle_drift`,
both WARNING-level, firing on **every single 5-minute cron tick** with
zero suppression (36 rows each in a 3-hour window, exact 5-min cadence).
`sentinel_mlx_qwen3_heartbeat_stale` (RED_ALERT), by contrast, was already
firing on a real ~30-35 min cadence — the existing mechanism happened to
work for RED_ALERT only.

The reason: `engine/alert_channels.py::send_alert()`'s own rate limiter
(`_rate_ok`/`_mark_rate_limit_sent`) only consumes its cooldown window on
a **confirmed external delivery** (ntfy, email, or pushover — see
`_mark_rate_limit_sent`'s own docstring, HM-ALERT-RATE-ON-FAILURE
2026-07-07). But `_send_ntfy` has been globally stubbed to always return
`False` since DECOM-SILENCE (2026-07-19), and WARNING-level alerts never
touch pushover or email (those are RED_ALERT-only channels in
`send_alert`). So for every WARNING-level alert, `results.get("ntfy") or
results.get("email") or results.get("pushover")` is always `False` →
`_mark_rate_limit_sent` is **never called** → `_rate_ok()` always returns
`True` → the 1800s cooldown passed to `_dispatch` never actually engages.
RED_ALERT alerts do call `_send_pushover` (a real, non-stubbed channel),
so their cooldown window has been working by coincidence this whole time.

**Fix (scripts/hm_ops_sentinel.py only, not `engine/alert_channels.py`):**
a new per-alert-type cooldown tracked in the sentinel's own state file
(`data/.hm_ops_sentinel_state.json`, new `alert_cooldown` key), independent
of `send_alert`'s broken/delivery-gated limiter:
- Fires immediately on first occurrence of an alert_type.
- RED_ALERT: 30 min cooldown while it persists (unchanged cadence, now
  guaranteed rather than incidental).
- WARNING (and INFO): 60 min cooldown while it persists, per instruction.
- Re-arms (treated as a fresh first occurrence) the moment a tick's raw
  check results no longer include that alert_type at all — i.e. the
  underlying condition actually cleared. Ack status is deliberately
  irrelevant to re-arming; only the real condition clearing does.
- Exit code 2 still fires for any real unacked alert, cooldown-suppressed
  or not — cooldown only throttles the *notification*, not the health
  signal a wrapper script or human might poll.
- Deliberately **not** the ack mechanism (`scripts/hm_sentinel_ack.py`) —
  an ack hides the alert entirely from the dashboard/log; this only
  throttles repeat pushes while the condition keeps being visible.

Scoped to the sentinel's own dispatch loop, not `engine/alert_channels.py`
— that module is shared by many other live callers whose own
delivery-confirmed rate-limit semantics exist for a different, still-valid
reason (a genuine transient network failure shouldn't burn a retry budget
for those callers) and were out of scope for tonight's fix.

**Verified:**
- New isolated smoke test (fire → suppress-within-window-but-still-exit-2
  → re-fire-after-window → re-arm-on-clear → immediate-refire-after-rearm)
  passes all 5 assertions deterministically against mocked state/ack
  paths.
- Full existing sentinel test suite (44 tests across 7 files) passes.
  One pre-existing test (`test_main_dispatches_only_unsuppressed_alerts`)
  needed `check_disk_space` and `STATE_PATH` patched — it was already
  leaking the box's real (89%+) disk reading into a strict alert-set
  assertion before tonight, plus now needed isolation from the new
  state-file writes; fixed alongside the feature since the feature
  exposed it.
- **Live-confirmed on the real box**, no restart needed — this is a plain
  cron-invoked script (`*/5 * * * *`, no daemon to kickstart), so the
  fix took effect on the very next real tick after saving. The
  2026-09-03 20:30 cron tick already shows `sentinel_disk_space` correctly
  marked `[sentinel] ON COOLDOWN (3600s per warning)` in
  `logs/hm_ops_sentinel_cron.log`, with the underlying alert still
  present and exit code still 2.

**Not addressed:** the *reason* the underlying WARNING conditions
(disk space, lifecycle drift) are firing at all — those are items 2 and 3
above, still open. This fix only stops them from spamming while true.

---

## Also checked: `fd_count: 0` on main.py PID 72190 (was 47 yesterday)

Real, not a sentinel-lost-visibility artifact in the way suspected —
confirmed the actual live cron output across the whole session:
`fd_count` bounced 0/0/3/0/0 across consecutive ticks tonight (20:15
through 20:35), i.e. genuinely low and noisy, not a flat/stuck zero that
would indicate `lsof` silently failing. `check_fd_count()`'s own
`lsof -p <pid>` + `"trader.db" in line` count is unchanged and has no
known regression. Not independently deep-dived beyond this cross-check
tonight (out of the four scoped items); flagging that the noisy low
readings themselves (0-3 vs. yesterday's 47) may be worth a look
independent of a "sentinel broke" theory, since the mechanism reading it
is confirmed still working.

---

## Summary of what shipped

| # | Item | Status |
|---|------|--------|
| 1 | mlx-qwen3 probe dead + healthy:True bug | Diagnosed both; probe needs a real GUI-session `launchctl` check; healthy-flag bug is a known, unfixed one-liner |
| 2 | Disk 89.6%/23.8GiB | Diagnosed: db_snapshot.sh's own 09-01 retention bump is the main driver, not the three named suspects; ~10GiB same-day-09-01 gap still unexplained |
| 3 | Lifecycle drift 8 targets | Diagnosed: real out-of-band `launchctl enable` at 2026-09-02 14:20-25 MST, not a detector bug; who/what unresolved; **not reconciled**, awaiting cause or Admiral sign-off |
| 4 | Alert spam | **FIXED, tested, live-confirmed** — per-alert-type cooldown (30min RED_ALERT / 60min WARNING) in `scripts/hm_ops_sentinel.py`, independent of the ack mechanism |

Files changed: `scripts/hm_ops_sentinel.py`, `tests/test_hm_ops_sentinel_acks.py`.
