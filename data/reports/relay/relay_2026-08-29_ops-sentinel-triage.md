# Relay — HM-OPS-SENTINEL 3-banner triage, 2026-08-29 (evening)

**CORRECTION appended 2026-08-30 (same session, later that night):** the
"signals_v2 FIFO starvation" section below claims Recommendation #2
(priority-lane/age-ordering) "was never built" and "deserves its own
scoped session." That was wrong — I hadn't checked `git log` on
`engine/events_bus_consumer.py` before writing it. Commit `61126bb`
(2026-08-29, 13:33 — six hours before this relay was written) already
built, tested (4 pinned tests), and shipped exactly that fix;
`docs/XO_BACKLOG.md`'s `HM-SIGNALS-V2-STARVATION-RECURRENCE` entry already
marked it RESOLVED the same afternoon. The 1,127-pending figure quoted
below is that fix's own deliberate, documented remainder, not a fresh
recurrence. Full corrected review, the fairness/attribution question this
surfaced, and the interim-mitigation decision are in
`docs/XO_BACKLOG.md` under the new
"HM-SIGNALS-V2-FIFO-STARVATION — post-fix review, 2026-08-29 evening"
entry, appended right after the RESOLVED section it corrects. Leaving the
rest of this file's section 3 unedited below rather than rewriting
history — read it as superseded by that entry, not as current fact.

Triage of tonight's three TradeMinds Warning banners (14 failing cron entries,
17 stale launchd jobs, signals_v2 FIFO starvation), prompted by the
Admiral's own cross-referencing of the deleted-file working tree against
the sentinel's lists.

## 1. 14 failing cron entries — FIXED, crontab edited live

All 14 (13 distinct crontab lines, since `situation_report.py` was
double-scheduled) invoke scripts renamed away in the 2026-07-22
stand-down (`*.quietdown-disabled-2026-07-22` siblings on disk) — the
crontab was never cleaned up when those scripts were retired. Found 2
more of the identical class the sentinel doesn't watch at all
(`ollama_prewarm.sh` → `ollama_warmup_cron.log`, `recall_refresh_run.sh`
→ `recall_refresh_cron.log`) — same root cause, just not on its list.

**Action taken:** commented out all 15 crontab lines in place (dated
`# DISABLED 2026-08-29 (HM-CRON-STANDDOWN-CLEANUP)` marker + the original
line preserved below it, matching this crontab's existing convention).
Did **not** restore any files — per the Admiral's explicit instruction,
and because `data/reports/relay/QUESTION_fleet-standdown-reversal.md`
(2026-08-28) is still the open decision point for whether any of these
come back. Verified byte-for-byte before installing (reconstructed
original state from the edited file must match the pre-edit backup
exactly outside the 15 target lines) and confirmed zero active lines
reference a missing script afterward. Backup:
`backups/crontab/crontab_pre_HM-CRON-STANDDOWN-CLEANUP_20260829.bak`.

`gex_collector.log`'s failure (also on the sentinel's 14-list) needed no
crontab change — `scripts/hm_gex_daily_collect.py` was already restored
today per a separate ticket (HM-GEX-COLLECTOR-DEAD remediation, see the
crontab's own comment above that line); its cron is weekday-only and
hasn't fired since the restore (last log write Fri Aug 28 13:05), so the
failing lines in its log are stale and will clear on Monday's run.

## 2. 17 stale launchd jobs — already resolved earlier today, one correction, one real gap found

**Already handled, not a new problem tonight.** Commit `7e3a42c` (today,
14:59 MST) + `docs/HM-STANDDOWN-SUCCESSOR-2026-08-29.md` show all 17 of
tonight's stale-list jobs are inside an 18-job batch the Admiral already
reviewed job-by-job and revived this afternoon (enable + bootstrap
confirmed live via `launchctl list` — all loaded). Tonight's persisting
staleness is expected lag: revived ~15:00–22:00 today, but the affected
jobs are mostly AM-scheduled and haven't had their first post-revival
fire yet. No action needed on the other 15 of these 17 — they should
clear naturally as each job crosses its next calendar window (mostly
tomorrow morning).

**Correction to my own initial read:** I first suspected `archer-briefing`
was mis-revived (checked `~/ollietrades_archived_2026-07-06/logs/
archer_briefing.log` — 0 bytes, stale since April — and read that as
"job's been dead 5 months, superseded by main.py's own
`run_archer_morning_briefing()`"). That was the **wrong file** — the
plist's real configured path is the separate, still-live `~/ollietrades/
logs/` tree (recreated post-archive, distinct directory), where
`archer_briefing_err.log` shows genuine "Briefing sent and saved" output
through **2026-07-22 06:25**, matching the stand-down date exactly. The
revival call was correct; my first pass wasn't. One small, real,
non-blocking bug found in passing: every run logs `ntfy failed: No module
named 'engine'` (missing repo-root `sys.path` insertion when launchd
invokes the script standalone — same class of bug commit `224292b` fixed
for `riker_synthesis.py`). Not fixed here — flagging only, since the
core briefing function works and this wasn't asked for.

**Real gap found:** `hm-signals-v2-monday-check` and its `-verify` twin
are **not** general-purpose recurring watchdogs — both plists have a
`StartCalendarInterval` with a hardcoded `Year:2026 Month:7 Day:13`, and
the script's own docstring says "meant to be fired once... not run
repeatedly." They were one-shot forensic scripts built to answer a single
question about a specific July 12-13 backlog (hardcoded row IDs
67350-67489). That calendar date is 47 days gone; launchd will never
fire either job again no matter how many times `revive` re-enables +
re-bootstraps them (confirmed both are loaded per `launchctl list`, which
proves the revive mechanics worked — it just can't matter here). The
sentinel's own registry (`scripts/hm_ops_sentinel.py:643-644`) gives both
a 192h staleness ceiling they can now never satisfy — they'll read as
permanently stale from here forward unless retired out of that registry.

**Done, same session, Admiral-approved:** both retired via
`scripts/fleet_lifecycle.py retire` (reason: "superseded — recurring
HM-OPS-SENTINEL queue-age monitoring now covers what these one-shots
watched; retiring reverses the earlier revive entry deliberately,
Admiral-approved"). Confirmed via `launchctl list` — both fully unloaded.
Order docs: `docs/orders/ORDER_2026-08-29_retire_job_hm-signals-v2-monday-check.md`
and the `-verify` twin. No other ledger row touched (verified via
`fleet_lifecycle.py list --type job` before/after).

**Correcting the premise under this one:** the real, general watchdog for
signals_v2 queue health was never these two — it's
`check_signals_v2_queue()` inside `hm_ops_sentinel.py` itself, running
every 5 minutes via cron (`HM-OPS-SENTINEL-2026-07-06` line, confirmed
active and unaffected by anything above). It caught tonight's real
starvation correctly; it was never silenced. Recommend retiring the two
one-shots properly via `scripts/fleet_lifecycle.py retire` (reason: date
expired, was never recurring, superseded in practice by
`check_signals_v2_queue()`) rather than leaving them in the "revive"
state, since that state can never be satisfied again — did **not** do
this yet, since it reverses a ledger entry an Admiral-reviewed process
wrote a few hours ago and deserves explicit confirmation first.

Also noted, not chased further: `scripts/fleet_lifecycle.py`'s own
docstring says the sentinel reads its launchd registry "live from this
ledger, not a hardcoded dict" — `LAUNCHD_JOB_REGISTRY` in
`hm_ops_sentinel.py` is in fact a hardcoded dict. Doc/implementation
mismatch, not chased tonight.

## 3. signals_v2 FIFO starvation — confirmed structural, unfixed, recurring

Live query tonight: 1,127 pending rows, oldest is id `67352`
(`ollama-plutus` / Dr. McCoy, an **active**, non-halted agent),
`created_at` 2026-07-10 20:02:04 — ~50 days old. This is a **new** oldest
row, not the `2026-06-24` row `docs/XO_BACKLOG.md`'s most recent
FIFO-starvation entry described — that one is gone (consumed or expired),
confirming the backlog's own prediction: "nothing structural stops it
from recurring with a third batch." This is that recurrence.

Per `docs/XO_BACKLOG.md` (`HM-SIGNALS-V2-FIFO-STARVATION`):
Recommendation #1 (expire pending rows from halted/non-active sources)
already shipped as a one-time cleanup and worked as designed. Recommendation
#2 (priority-lane / age-ordering fix in the consumer so an old active-source
row can't block fresh ones forever under strict `ORDER BY created_at DESC`)
was never built — this is why the same shape of stuck-oldest-row keeps
recurring with each new batch. `ollama-plutus` being active (not halted)
means recommendation #1's expiry approach doesn't apply here without a
judgment call about discarding a live agent's aged signal.

**Not started tonight** — this is a real engineering change to
`engine/events_bus_consumer.py::consume_pending_signals()`'s ordering/cap
logic, not a config toggle, and deserves its own scoped session rather
than a rushed tail-end build. Flagging as the one item from tonight's
three banners still genuinely open and actionable.

## Summary of state after this pass

| Item | State |
|---|---|
| 14 dead-cron entries | Fixed live tonight (crontab edited, backed up) |
| 17 stale launchd jobs | Already fixed this afternoon (commit `7e3a42c`); expected to self-clear by tomorrow AM |
| archer-briefing | Correctly revived; my first-pass doubt was a wrong-file error, retracted above |
| 2 signals-v2 "monday-check" one-shots | Retired (Admiral-approved), same session — see correction above |
| signals_v2 FIFO starvation | **Already fixed** at 13:33 today (commit `61126bb`), before this relay was written — see the 2026-08-30 correction at the top of this file and the review entry in `docs/XO_BACKLOG.md`. My original claim below that it was "still open... fix not started" is wrong; left unedited for the record. |
