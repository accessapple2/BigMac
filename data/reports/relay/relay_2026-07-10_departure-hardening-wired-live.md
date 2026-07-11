# Relay: XO-DEPARTURE-HARDENING Phase 1 wired live (S6, work block 12)

**Date:** 2026-07-10
**Commit:** `471225d` (pushed to `exec-pipeline`)
**Prior work block:** the four scripts proposed and dry-run-tested
(relay `4c84d90`), including the equity-curve fix found along the way.

## Ask

"Wire in the four proposed scripts."

## What shipped

**1. `scripts/tour_api_restart.sh`** — added as a fifth `check_and_restart`
line in `origin_healthcheck.sh` (already live-scheduled every 5 min).
Confirmed `tour_api` returned 200 before wiring (no risk of an immediate
restart-storm on deploy), then manually ran the updated script post-edit
and confirmed a clean silent pass across all 5 endpoints — no restart
fired, no false positive.

**2. `scripts/disk_space_alert.sh`** — crontab `0 6,13,20 * * *` (3x/day,
every day — disk health doesn't take weekends off).

**3. `scripts/door1_kill_gate_check.py`** — crontab `5 14 * * 1-5`, 5
minutes after `eod_report.py`'s existing 2pm slot to avoid an exact-minute
collision.

**4. `scripts/ollie_machine_kill_gate_check.py`** — crontab `10 14 * *
1-5`, 10 minutes after `eod_report.py`.

## Verification

- `crontab -l` line count: 77 → 87. Diffed the before/after crontab
  files directly — confirmed only the 3 new additive blocks landed,
  nothing else touched, reordered, or duplicated.
- `origin_healthcheck.sh`: `bash -n` syntax check clean; ran it manually
  post-edit, zero new log lines appended (the script only logs on
  failure) — all 5 checks, including the new tour-api one, passed
  silently.
- All four scripts were already syntax-checked and dry-run-verified
  against live data in the prior work block before this one; this block
  only added the scheduling/wiring layer, no logic changes.

## What's now live going forward

- Every 5 min: tour_api gets the same HTTP-response healthcheck +
  auto-restart + ntfy coverage the other four services already had.
- 3x/day: disk-space WARN/ALARM pushes if usage crosses 85%/95%.
- Weekdays ~2:05pm: Door-1 G1-G4 verdict computed and pushed (PREVIEW
  framing until the real 2026-07-24 DAY-30 date, VERDICT framing after).
- Weekdays ~2:10pm: ollie-machine's single-agent gate status pushed
  (currently: zero trades, days-remaining countdown until 2026-07-24).

None of these four scripts ever halt, pause, or modify anything — all
compute-and-push only, matching the ticket's own stated doctrine.

## Testing / verification carried over from the prior block (unchanged)

No test suite changes this block (pure scheduling/wiring, no new logic).
Full suite last verified at 986 passed in the prior block; nothing here
touches Python logic that suite would exercise differently.

## docs/XO_BACKLOG.md

`XO-DEPARTURE-HARDENING` Phase 1 entry updated: heading downgraded from
🟡 to 🟢, body updated from "PROPOSED, not yet wired" to "WIRED IN LIVE
2026-07-10" with each script's final cron schedule and the pre-wiring
safety check performed for the healthcheck addition.

## Open items (carried forward, unchanged)

1. `HM-STATUS-PAGE-STALE-CACHE` — still needs a Cloudflare dashboard
   change only the Captain can make.
2. `HM-STRATEGIES-EXECUTOR-STATUS-NEVER-SET`/`HM-SWINGDESK-CLOSE-
   PHANTOM-ROW` pnl gap — on hold pending a live MLEG close.
3. `HM-DRAWDOWN-BLIND-TO-OPTIONS-PNL` — needs a dedicated design session,
   zero current urgency.
4. The `options_books` stored-counter drift — still harmless, still out
   of scope.
5. XO-DEPARTURE-HARDENING Phase 1 items 1-3 remaining sub-pieces (not
   covered by tonight's four scripts): the Cloudflare-edge status-page
   fix (item 1), and item 10's pre-flight alarm test (kill each service
   one at a time day-before-departure, confirm the phone actually
   receives the pushes) — that last one specifically needs the
   Captain's own device and can't be verified by Scotty alone.
