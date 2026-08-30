# Relay — 2026-08-30 — recall_refresh_run.sh enabled, catch-up blocked (Ollie Max down)

## Context

Follow-on to tonight's revive/retire batch. Admiral approved enabling
`recall_refresh_run.sh` and asked for the ~3-run catch-up bound to be
verified directly, with the final embedded count reported.

## What happened

- Crontab line uncommented (backed up first:
  `~/backups/cron/crontab.bak-20260830-104709-pre-recall-refresh-enable`).
- Attempted to run the catch-up manually tonight (rather than wait ~3
  weekdays for the real schedule) — **blocked**. The embedding backend,
  Ollie Max (`192.168.1.168:11434`), is unreachable: `ping` reports "Host is
  down"; bigmac's own LAN path is fine (gateway responds normally).
- Ran the real wrapper multiple times to rule out a fluke — every attempt
  failed identically on the embedding call
  (`urllib.error.URLError: [Errno 65] No route to host`).
- **Good news inside the bad news:** this gave a real (not simulated) test
  of the ntfy migration — 3 `HM-DEJAVU recall_refresh FAILED (rc=1)`
  warnings landed correctly in the `notifications` table via
  `engine.alert_channels.send_alert`, confirming the migration fires
  correctly on genuine failures, not just the isolated test from the prior
  session.
- `recall_corpus` count: **unchanged at 252**. Zero rows embedded, zero
  progress on the 1,343-row gap tonight.
- Crontab comment corrected to reflect this honestly (was going to say
  "manually verified to complete" — rewrote before that shipped, since it
  didn't happen).

## Current state

Left enabled per approval — no further crontab action needed. The catch-up
will resume automatically the moment Ollie Max is reachable again, either
via its own next scheduled weekday 15:00 MST run or a manual re-run.

## Not done this pass

Did not attempt to remotely power-cycle Ollie Max — its Shelly plug is
manual-only by standing doctrine (CLAUDE.md Shelly Plugs section: "NEVER
self-cycling on DB hosts... a human runs it deliberately"). Flagging that
Ollie Max being down also means the live fleet's LLM-routed agents have no
inference backend right now, if it's still down when trading resumes — the
Admiral may want to check on it before Monday open, independent of the
recall_refresh catch-up.
