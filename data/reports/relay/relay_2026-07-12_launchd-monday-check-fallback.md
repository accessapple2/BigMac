# Relay: launchd fallback for HM-SIGNALS-V2-STARVATION-RECURRENCE Monday check

**Date:** 2026-07-12 (Sunday, evening)
**Directive:** XO — crontab is broken machine-wide (`crontab: tmp/tmp.49301:
Interrupted system call`, reproduced twice from two different sessions).
Stop troubleshooting crontab, switch to a launchd LaunchAgent for the Monday
2026-07-13 07:00 MST run of `scripts/hm_signals_v2_monday_check.py`, install
and verify it, file a backlog note on the crontab issue.

## What shipped

1. **LaunchAgent installed:**
   `~/Library/LaunchAgents/com.ollietrades.hm-signals-v2-monday-check.plist`
   - `ProgramArguments`: `.venv/bin/python3 scripts/hm_signals_v2_monday_check.py`
     (repo's Python 3.14 venv, matches the script's `sqlite3`/`engine.market_calendar`
     imports — verified with a `--dry-run` execution before install, ran clean).
   - `StartCalendarInterval` pinned to **Year 2026 / Month 7 / Day 13 / Hour 7 /
     Minute 0** (not a bare Weekday/Hour/Minute recurrence) so it fires exactly
     once on Monday 2026-07-13 07:00 MST and never again — the script's own
     baseline/report paths are hardcoded to that date, so a recurring trigger
     would just re-run pointlessly on future Mondays.
   - `RunAtLoad = false` (don't fire immediately on bootstrap).
   - stdout/stderr routed to `logs/hm_signals_v2_monday_check_{stdout,stderr}.log`.
2. **Bootstrapped:** `launchctl bootstrap gui/501 <plist>` — exit 0. The
   `docs/CLAUDE.md` "LaunchAgent Reboot Lifecycle" note warns this command can
   fail with "Domain does not support specified action" over SSH; it did NOT
   fail here (confirmed a real Aqua console session has been logged in on
   this box since 2026-07-11 12:55, which is the condition that note says
   the failure depends on).
3. **Verified loaded + scheduled** via `launchctl print
   gui/501/com.ollietrades.hm-signals-v2-monday-check`: job present, `runs =
   0`, `last exit code = (never exited)`, event trigger descriptor shows
   `Year=2026 Month=7 Day=13 Hour=7 Minute=0` under
   `com.apple.launchd.calendarinterval` / `monitor =
   com.apple.UserEventAgent-Aqua`. Also confirmed present in `launchctl
   list`.
4. **Backlog note filed:** `docs/XO_BACKLOG.md`, new ticket
   `HM-CRONTAB-EINTR` (🔵 not urgent) — records the EINTR symptom, the
   launchd workaround, and next diagnostic steps (isolate read vs write
   path, check `$EDITOR`/`$VISUAL`, check for a stale lockfile under
   `/usr/lib/cron/tabs/`).

## Caveat worth flagging (not a blocker, just a dependency to know about)

The `com.apple.UserEventAgent-Aqua` monitor on the calendar-interval trigger
means this job's fire time depends on the GUI (Aqua) session staying logged
in through Monday 07:00 MST, per the same reboot-lifecycle doctrine already
in `docs/CLAUDE.md`. If the box reboots or the console session gets logged
out between now and then, the trigger won't fire and there's no alarm for
that — same "silent gap" failure mode `docs/CLAUDE.md` already documents for
other launchd-only jobs. Given this is a single one-shot check with idempotent,
safe-to-rerun logic (guarded XO_BACKLOG append, re-runnable dry-run), the
blast radius of a missed fire is low — but it's honest to say "scheduled" ≠
"guaranteed to fire" here.

## Not done / left for next session

- `docs/XO_BACKLOG.md` and this relay report are **uncommitted** on disk
  (no-auto-push doctrine — same convention the Monday-check script itself
  follows). Needs review + commit + push in the next live session.
- Root-causing the crontab EINTR error itself — explicitly deferred per
  XO's directive, not urgent.
- No cleanup logic was added to remove the plist after it fires once; since
  `Year` is pinned it will never refire, so it's inert rather than harmful,
  but it will linger in `launchctl list` — worth a manual
  `launchctl bootout gui/501/com.ollietrades.hm-signals-v2-monday-check`
  after Monday's run confirms clean, if the Admiral wants it tidied up.
