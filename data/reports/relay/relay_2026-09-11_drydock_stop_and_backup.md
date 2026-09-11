# Relay — Dry Dock: trader stopped, backed up, verified. 2026-09-11

Gate 0 of the dry-dock sequence, per the Captain's directive. Nothing else
starts until this is confirmed — confirmed below.

## Auto-restart vectors neutralized first

Two live vectors would have silently undone a manual stop within 5
minutes: `HM-TRADER-KEEPALIVE` cron (`pgrep main.py || trader_restart.sh`)
and `watchdog_supervisor.sh` cron (relaunches `watchdog.py`, whose own
60s-cadence `check_bridge()` calls `trader_restart.sh` when port 8080 is
down). Both commented out (not deleted — restore markers included,
`# DRY-DOCK-2026-09-11 DISABLED (restore at undock): ...`) per the repo's
Cron Edit Safety Rule: backed up to `/tmp/xo_cron_backup_drydock_
20260911_061026.txt`, edited a file copy (not a pipe), diffed (exactly
the two intended lines changed), count-guarded (172->172), installed.
`@reboot` entries left untouched — no reboot planned, out of scope for
this gate. `watchdog.py` was not separately running at the time.

## Trader stopped

`main.py` (PID 45372) — single writer on `logs/trader.log`, identified
the same way `trader_restart.sh` does (lsof, write-mode FD). SIGTERM,
cleared within the wait window, no SIGKILL needed. Confirmed: no
`main.py` process (`pgrep -f "autonomous-trader/main.py"`), port 8080
free. `signal-center/server.py` (a separate service, not "the trader")
left running — not in scope for this stop.

## trader.db WAL checkpoint + backup

`PRAGMA wal_checkpoint(TRUNCATE)` in the zero-reader window right after
kill (same pattern `trader_restart.sh` itself uses) — clean, `0|0|0`.

## Backups — both via sqlite3's online `.backup` API, both verified

- `data/backups/trader_20260911_dry-dock.db` (1,317,203,968 bytes,
  matches source exactly)
- `data/backups/signals_20260911_dry-dock.db` (180,293,632 bytes,
  matches source exactly — `signal-center/signals.db`, not the 0-byte
  stub at `data/signals.db`)

**`PRAGMA integrity_check` on both backups: `ok`.**

**Row counts, source vs. backup, every RULE1-protected table plus key
signal-center tables — all exact matches:**

| trader.db | source | backup |
|---|---|---|
| trades | 2781 | 2781 |
| portfolio_positions | 12 | 12 |
| signals | 110284 | 110284 |
| rikers_log | 9579 | 9579 |
| war_room | 184057 | 184057 |
| decision_audit | 140520 | 140520 |
| signals_v2 | 109568 | 109568 |
| agent_ratings | 42615 | 42615 |
| desk_execution_trace | 358 | 358 |
| crew_decisions | 40438 | 40438 |
| notifications | 36363 | 36363 |
| ai_players | 82 | 82 |

| signals.db | source | backup |
|---|---|---|
| signal_history | 12198 | 12198 |
| intelligence_feed | 2925 | 2925 |
| trade_signals | 2502 | 2502 |
| predictions | 2841 | 2841 |
| scored_predictions | 6040 | 6040 |

RULE #1 intact throughout: no row deleted or rewritten anywhere, only new
backup files created and two cron lines commented (not removed) with
explicit restore markers.

## Stopped state, confirmed

- `main.py`: not running
- Port 8080: not bound
- Auto-restart cron vectors: both disabled, restorable at undock
- `trader.db`, `signal-center/signals.db`: backed up and integrity-verified
- `signal-center/server.py`: still running (separate service, out of scope)

Proceeding to item A1 (S8 season rotation).
