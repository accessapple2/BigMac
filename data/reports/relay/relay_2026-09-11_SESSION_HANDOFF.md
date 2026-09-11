# Session handoff — 2026-09-11. Next session starts fresh from here.

Marathon dry-dock session: A1-D16 undock, four Admiral follow-ups
(Pushover/ntfy, calibration correction, orphaned stash, backup
retention), two final autonomous tasks (olliemax delivery, Phase 1.3
spec). Everything below is committed and pushed to `exec-pipeline`
(latest: `5ac0ad0`). Fleet is live. This document is the state of the
world at handoff — read this before anything else.

## Fleet state right now

- `main.py` live, undocked, running normally. Last restart 08:45:42 AZ
  today (pushover fallback-removal fix). PID will differ by the time
  you read this — check `pgrep -af "[m]ain.py"`, don't assume 51732 is
  still current.
- Season 8, current.
- All dry-dock-disabled cron vectors restored (`origin_healthcheck.sh`,
  `watchdog_supervisor.sh`, `HM-TRADER-KEEPALIVE`, `@reboot` trader line)
  — confirmed live in `crontab -l`, zero `DRY-DOCK-2026-09-11 DISABLED`
  markers remain.
- `data/DRY_DOCK` flag file removed (`engine.dry_dock.is_docked()` is
  `False`). `hm_ops_sentinel.py`'s dock-mode heartbeat should be silent.

## The one open, unresolved item: McCoy's scheduler may be unreliable

**This is the most important thing to pick up.** Traced today: McCoy's
12:30 PM ET screened-scan slot didn't fire, root-caused to a ~20-minute
global stall of `main.py`'s single-threaded `schedule.run_pending()`
queue (zero evidence of ANY scheduled job firing in that window, not
just McCoy's). Matches a documented pre-existing pattern in this
codebase, not a new bug. Full trace: `relay_2026-09-11_mccoy_
1230_nofire_trace.md`. **Not done**: identifying which specific job
blocked the queue, and deciding whether `run_mccoy_screened_scan` needs
its own thread (like the WR daemon already has) instead of sharing the
queue. Both filed to `docs/XO_BACKLOG.md`.

**Check first thing next session**: did the 9:35 AM ET slot fire
tomorrow? If it silently no-fires again, this is a recurring, live
problem with Phase 1.2's entire premise (twice-daily reliable scans),
not a one-off — escalate priority accordingly. `grep -i "McCoy screened
scan" logs/trader.log` for today's date is the check.

## Everything else, by topic

**Pushover/ntfy** — fully resolved. Real OllieTrades app token in `.env`,
fallback code removed, both tiers verified delivering under the correct
app identity. ntfy DECOM-SILENCE lifted per Admiral decision. Nothing
open here.

**Calibration finding** — withdrawn and corrected. 169/78.6% (McCoy
overconfidence) is dead; **42 trades / 71.4% hit rate is the number now
on record** everywhere (Phase 1.3 spec, backlog). The underlying finding
(McCoy overconfident at the top of his stated range) got *stronger* after
correction (~22pt gap vs the withdrawn ~15pt), not weaker.

**Orphaned git stash** — dropped, per Admiral decision. Gone from `git
stash list`. Don't go looking for it.

**Backup retention** — policy written (`docs/runbooks/backup-retention-
policy.md`), decision made (keep all 5 dry-dock files, disk pressure
resolved), and the real bug found along the way — `offhost_backup.sh`'s
sync pattern excluding ad-hoc backups — is fixed and backfilled. All 13
files in `data/backups/` now have a verified X9 copy. Named as a general
pattern in `docs/DOCTRINE.md`: "Coverage that looks complete but
silently excludes what matters." `data/backups/_archive/` (3.2 GiB, old
sidecar debris) stays on the backlog, no urgency.

**Phase 1.3 spec** — posted verbatim to `relay_2026-09-11_phase13_
spec_for_review.md` for the Admiral to read. **NOT BUILT.** Two open
details flagged inline (the `confidence_calibration` reduction formula,
the `learning_engine` multiplier transform) need one more confirmation
pass before coding. The whole confidence-coordination piece stays held
until `calibration_map` has materially more data — and per the spec's
own text, that's a real blocker, not a "wait a few days" one (the
`trade_fire`→`trade_id` join it depends on sees 3 rows fleet-wide,
total, ever, as designed today).

**olliemax / modelworks** — Plutus v8 retrain **cancelled**, on
task-shape grounds (corpus teaches post-trade critique, seat needs
scan-time decisions — confirmed live against 16 real Arena calls, not
assumed). Decision delivered to `~/modelworks/fleet_checks/mccoy/
RETRAIN_DECISION.md`. Fresh `arena_calls.jsonl`/`parse_decision.py`
delivered and verified (16/16 exact match against real captures, runs
on olliemax's own Python). This should be a closed loop unless
modelworks comes back with a follow-up question.

## Verify-before-trusting reminders for next session

- The `git stash`/`git reset --hard` near-miss earlier today (recovered
  cleanly, no data lost) is a reminder: **check `git stash list` before
  ever stashing**, even when the tree looks clean.
- `CLEAN_TRADES_WHERE` is an exclusion list (`player_id NOT IN (...)`),
  not an inclusion filter for any one player — the calibration
  correction happened because an earlier query forgot this. Any future
  "just McCoy" query needs its own explicit `player_id='ollama-plutus'`
  clause.
- Three real instances this session of "coverage that looks complete but
  silently excludes what matters" (`origin_healthcheck.sh`, expired
  `REVISIT-BY` tags, the offhost sync gap) are now named as a pattern in
  `docs/DOCTRINE.md` — worth a skim before building or auditing any new
  monitoring/retention/sync mechanism.

## Full commit trail today (exec-pipeline)

Roughly 40 commits from dry-dock start through this handoff — `git log
--oneline` on `exec-pipeline` has the complete, honest trail, including
two corrections (the watchdog-only root-cause declared prematurely, the
calibration finding withdrawal) made openly rather than quietly folded
in. Nothing was force-pushed or rewritten.
