# Relay: watchdog_cron.log subscribed to weekly log rotation

**Date:** 2026-07-11
**Commit:** `427cd54`

## What was asked

"subscribe watchdog_cron.log to log rotation" — `logs/watchdog_cron.log`
(watchdog.py's real, actively-written log, discovered during last night's
pre-flight status check to be a different file than the stale
`logs/watchdog.log` `logging.basicConfig` implies) had no rotation
mechanism.

## What was found

5.6MB, growing since 2026-05-30 (42 days) — ~133KB/day, unbounded, same
failure mode `trader.log` had before `scripts/rotate_logs.sh` existed
(grew 107MB→345MB before that fix). `watchdog.py` is a long-running
process (60s heartbeat loop, PID confirmed alive) that holds this log
path open via `nohup ... >> logs/watchdog_cron.log 2>&1 &` in
`watchdog_supervisor.sh` — same live-writer situation as `trader.log`,
meaning any rotation must truncate-in-place, never rename.

## What shipped

Added `rotate_one "logs/watchdog_cron.log" $((10 * 1024 * 1024))
"watchdog_cron"` to `scripts/rotate_logs.sh` (10MB threshold, same tier
as `logs/hm_ops_sentinel_cron.log` — comparable growth rate). The
existing `rotate_one` function already truncates-in-place for every
target uniformly, so no new code path was needed, just a new call.
Already covered by the existing weekly cron (`0 5 * * 0`, Sun 05:00 MST)
— no crontab change required, same script file.

## Verification

- `bash -n` syntax-checked clean.
- Ran the script live (real run, not simulated): correctly evaluated
  `logs/watchdog_cron.log` at 5,825,953 bytes, under the 10,485,760-byte
  threshold, logged `[SKIP]` as expected (will archive+truncate
  automatically once it crosses 10MB on a future weekly run).
- Confirmed `watchdog.py` (PID 20972) still alive and still writing
  fresh heartbeat lines to the same log path immediately after the dry
  run — no disruption to the live process.

## Open items

None new. Same carried-forward items as prior relays.
