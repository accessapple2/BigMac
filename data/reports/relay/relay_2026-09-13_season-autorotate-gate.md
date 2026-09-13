# Relay — 2026-09-13. Auto season-rotation gated off (`6758b55`)

RULE #1 held: no rotation ran, nothing was deleted; the only DB operations were backups.

## Why
`main.py`'s Sunday job polled every 30 min against a 23:55–23:59 AZ window, so restart phase
decided whether it fired. The 09:28:35 restart put a poll at ~23:58:35 tonight; the margin
check passes (8 active vs 8), so it would have started Season 9 three days into S8, reset
cash, deleted `ollama-plutus` (3) and `m5-allocator` (2) position rows, and left S8
`end_date` blank. Seasons are started deliberately now.

## What changed
- **Flag:** `config.SEASON_AUTOROTATE_ENABLED` via `_env_flag()` — true only for explicit
  `1/true/yes/on`; absent or anything else = **false**. `.env` does not set it; the running
  process (PID 95807) has no such env var → resolves false.
- **Gate:** `engine/season_autorotate.run_scheduled_rotation()` checks the flag first and
  returns before any sentinel read, scope/margin check, or `rotate_season()` call, logging
  `season auto-rotation disabled by SEASON_AUTOROTATE_ENABLED=false; skipping`
  once at the process's first poll and once per Sunday window.
- **Hardening (enables nothing while off):** 5-min poll; window widened to 23:50–23:59 so
  it exceeds the poll interval (Kirk/CTO rule); at-most-once per Sunday via an in-memory
  set plus a durable "current season started today" check.
- **Untouched:** `rotate_season()`, `start_season()`, the margin guard, and the
  position-row DELETE.

## Backups (before any change)
`data/backups/trader_pre-rotation-gate_2026-09-13.db` (1.3 GB) and
`data/backups/signals_pre-rotation-gate_2026-09-13.db` (185 MB, from the real
`signal-center/signals.db`; `data/signals.db` is a 0-byte placeholder) — integrity ok,
taken with `sqlite3 .backup`. Both rsynced to `/Volumes/Crucial X9/OLLIETRADES_BACKUPS/backups/`
now (byte counts match, integrity ok on the X9) and both match `offhost_backup.sh`'s
ad-hoc glob, so nightly runs keep carrying them. The glob only scans `data/backups/`, not a
repo-root `backups/`. Code copies: `data/backups/rotation_gate_2026-09-13/`.

## Verification
- Tests: `tests/test_season_autorotate_gate.py` (7) + season config/rotation tests — 19 pass;
  full suite failure set identical to baseline.
- Restart 09:51:06 (PID 95807). `season_config` latest row = Season 8 (3 rows, no new row);
  `current_season` = 8; positions `ollama-plutus`=3, `m5-allocator`=2.
- **Job's own first scheduled poll (09:54:11):** `[SCHED-JOB] start name=run_season_rotation`
  → `season auto-rotation disabled by SEASON_AUTOROTATE_ENABLED=false; skipping`
  (`season_autorotate.py:70`) → `[SCHED-JOB] done name=run_season_rotation`.
- **Tonight:** a persistent session monitor reports at 23:49 and 00:02 AZ (season, config
  rows, the two agents' position counts, rotation log lines). PENDING until it fires.

## HOLD — next task
No rotation, auto or manual, and do not enable the flag, until `rotate_season()`'s position
DELETE blocks on broker-backed positions instead of orphaning them (KMI/TQQQ came from
exactly this). Ledgered at the top of `docs/XO_BACKLOG.md`.
