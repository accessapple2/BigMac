# QUESTION — when to restart the trader to activate HM-SCREENED-SCAN-HB

Asked 2026-09-14 (Mon), market session in progress.

## Context

HM-SCREENED-SCAN-HB is committed but **not live**. The running trader (PID 1670,
up since Sun 2026-09-13 10:44 MST) still runs the old `run_mccoy_screened_scan` /
`run_qwen3_screened_scan` bodies. It changes:

1. A `[SCREENED-HB]` line on every 60 s tick (each slot's fired/skipped status),
   plus `data/screened_scan_heartbeat_<player_id>.json`, which the 05:00 log
   rotation can't erase.
2. A seat gate: `ollama-qwen3` (`halt_mode='full'`, no provider) stops running a
   full `run_scan` at 9:35 and 12:30 ET.
3. `[pre-open]` / `[midday]` escaped, so they show up in trader.log again.

Tests: `tests/test_screened_scan_scheduler.py` (14) + `tests/test_riker_xo_schedule_gate.py`
(6) = 20 passed; `py_compile` clean. A restart is the only thing that activates it.
Full trace: `relay_2026-09-14_screened_scan_silence_trace.md`.

Next screened slot: **12:30 ET = 09:30 MST today** (McCoy midday, window open
until 13:50 ET / 10:50 MST).

## Question

When should the trader be restarted to activate it?

## Options

- **A — After today's close (after 14:00 MST), before tomorrow's 9:35 ET slot. (Recommended)**
  No intraday disruption, and today's midday slot runs on the known-good old code.
  Tomorrow's pre-open slot is the first live test of the heartbeat and seat gate.
- **B — Now, before the 09:30 MST midday slot.**
  Today's midday slot becomes the first live test. It restarts mid-session and kills
  any in-flight scans, exit monitors and daemon threads during market hours.
- **C — Hold until the Admiral reviews the diff.**
  Nothing changes on the live process until explicitly approved.
