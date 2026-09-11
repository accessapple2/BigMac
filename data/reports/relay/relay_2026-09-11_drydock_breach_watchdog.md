# Relay — dry-dock breach: watchdog.py restarted the trader. 2026-09-11

Olliemax flagged unexpected traffic from bigmac (~60 req/10min,
`gemma3:4b`+`qwen3:8b` loaded) at 06:39, asking what was still calling it
while the trader was supposed to be stopped for dry dock. It was right.

## Root cause

Gate 0 (session start) disabled two cron vectors: `HM-TRADER-KEEPALIVE`
and `watchdog_supervisor.sh` (the job that relaunches `watchdog.py` if
absent). **That was the wrong scope.** `watchdog.py` (PID 1195) had been
launched by that same supervisor cron on **2026-09-08 15:10** — three
days before dry-dock started — and was already running continuously when
Gate 0 executed. Disabling the supervisor stops it from relaunching a
*dead* watchdog; it does nothing to one already alive. Gate 0's own
process check should have caught this running instance and didn't (not
re-diagnosed — the operationally important fact is it was missed, not
why the specific check missed it).

`watchdog.py` runs `check_bridge()` every 60s and calls `trader_restart.sh`
directly when port 8080 is unresponsive — no cron in the loop at all once
it's alive. Timeline, all `2026-09-11`, all from `logs/watchdog_cron.log`:

- `06:10:14` — Ollama-down alert (expected/pre-existing, see below)
- `06:11:15` / `06:12:16` — bridge-down strikes 1/3, 2/3
- `06:13:17` — strike 3, `ALERT: Bridge Down`
- `06:13:18` — `trader_restart.sh` invoked, `main.py` relaunched, PID 4086
- `06:13:31` — bound to :8080, restart confirmed OK
- `06:14:01` — `Bridge RECOVERED`

**`main.py` ran continuously from 06:13:31 until killed just now (~35-40
min).** It ran with A3's Phase 1.2 code already in place, but A3 only
changed McCoy's own cadence — every *other* agent's normal 5-min scan
loop ran as designed the whole time, which is what generated the
sustained olliemax traffic (gemma3:4b/qwen3:8b calls for Tier1/Tier3
scanners, unrelated to McCoy).

**No trades executed during the window** (`trades` table, 06:13-now:
zero rows) — real decision volume did occur (658 `signals` rows), none
converted to an executed trade. No RULE #1 concern; the breach is that
the fleet was live and unsupervised during a declared dry-dock, not that
any data was corrupted.

`watchdog.py`'s own `check_ollama()` was independently alerting
`ALERT: Ollama Down — http://127.0.0.1:11434/api/tags unreachable`
throughout — expected and unrelated: that's the same stale
`127.0.0.1:11434` hardcode already catalogued in the Tier 2
hardcoded-host inventory (`docs/XO_BACKLOG.md`), held for tomorrow's
batch, not a new finding.

## Fixed now

- `watchdog.py` (PID 1195) killed directly.
- In-flight `trader_restart.sh` invocations it had already queued
  (2 more, PIDs 14036/14145) killed before they could relaunch `main.py`
  a second/third time.
- `main.py` re-stopped via the same writer-detection kill used at Gate 0.
- Confirmed stable: 15s hold, zero matching processes, port 8080 free.
- Crontab disables re-verified still in place (both `DRY-DOCK-2026-09-11
  DISABLED` lines present, unchanged).

## Gate 0 process for the rest of this dry-dock, corrected

Any future stop-and-verify in this session must check for an
**already-running** `watchdog.py` explicitly (`pgrep -f
"autonomous-trader/watchdog.py"`) and kill it directly, not just disable
the cron that would relaunch an absent one. Added to memory for this
session; will restate at undock's restart-then-verify step (item D16) so
the same gap can't reopen there.
