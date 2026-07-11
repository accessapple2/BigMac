# Relay: signal-center restarted to pick up HM-NTFY-IPV6-NOROUTE-SWEEP fix

**Date:** 2026-07-11
**No code commit this block** — operational restart only, verified live.

## What happened

`signal-center/server.py` (its own Python 3.9 `venv/`, port 9000) was
still running old PID `19199` (up since Tuesday), which predates the
27-file ntfy hardening sweep committed `92a24d9`/`8695002` on 2026-07-10.
Flagged in the pre-flight alarm test status check as the one live
process not yet carrying the fix.

## What shipped

Restarted via `scripts/signal_center_restart.sh` (kill by process match +
port, relaunch under `venv/bin/python3`, its dedicated Python 3.9
environment — flask + pyotp aren't in the main `.venv`).

- New PID `48447`, bound `:9000`.
- Startup log clean — no import errors picking up the `engine.
  alert_channels` delegate call inside `_morpheus_log_action`'s
  FAILED-action ntfy path.
- `GET /api/health` → `200`.

## Current live status — all three long-lived processes now carry the fix

| Process | Port | PID | Fix live? |
|---|---|---|---|
| `main.py` (trader) | 8080 | 35742 | Yes (restarted 2026-07-10) |
| `swingdesk/backend.py` | 8889 | 35976 | Yes (restarted 2026-07-10) |
| `signal-center/server.py` | 9000 | 48447 | Yes (restarted this block) |

This closes the last open item from `HM-NTFY-IPV6-NOROUTE-SWEEP` — every
long-lived process that holds one of the 31 hardened ntfy senders in
memory is now confirmed running the fixed code. Cron/launchd-invoked
one-shot scripts were already self-healing on next invocation, no action
needed there.

## Open items

None new from this block. Same 5 carried-forward items as the prior
relay (`relay_2026-07-10_ntfy-ipv6-sweep-complete.md`) remain untouched.
