# Relay — 2026-09-12. Two sentinel false-alarm fixes (monitoring-only).

RULE #1 respected: both changes are monitoring/alerting code. Nothing in
the trading decision path was touched. No trade/trader history row
affected.

## 1. `sys_scan_liveness` weekend false alarm — fixed

**Root cause:** `check_scan_liveness()` (`main.py`) already had a "market
closed — no cadence to violate" guard (`if _get_scan_interval() is None:
return`), but `_get_scan_interval()` never actually returns `None` on
weekends — it returns `SCAN_INTERVAL_WEEKEND` (3600s), since the fleet
legitimately still scans hourly on weekends. So the guard never fired.
Worse: the watchdog's threshold is a fixed `2 * _TIER1_INTERVAL` = 3600s
(60 min), which exactly equals the weekend cadence itself — zero slack,
so any scheduler jitter trips it every weekend cycle. This is the same
"stood-down, not stalled" state `engine/bridge_vote.py` already
recognizes and logs (`[BRIDGE_VOTE] Skipping — weekend`).

**Fix:** `check_scan_liveness()` now checks
`engine.market_calendar.is_trading_day(az_now().date())` — the same
canonical session-day predicate used elsewhere in this repo (REVEILLE,
Schwab cadence monitor, S6 backtests) — and returns early on non-trading
days, same as it already does for the (dead) `None`-interval case. Verified
live: today (2026-09-12) is a Saturday, `is_trading_day()` correctly
returns `False`. `py_compile` clean.

Not touched: `_get_scan_interval()` itself (still correctly returns 3600s
so weekend scans keep firing hourly — this fix only stops the *watchdog*
from alarming on that legitimate cadence, it doesn't change the cadence).

## 2. `com.ollietrades.mlx-qwen3` / `sentinel_mlx_qwen3_unhealthy` — retired

**Traced, confirmed decommissioned, not just presumed:**
- The local MLX server (`mlx_lm.server`, port 8899) crashed 2026-09-09
  11:19:41 MST on a KeepAlive respawn (`OSError: [Errno 48] Address
  already in use`) and never recovered. Confirmed live before touching
  anything: `lsof -i :8899` empty (no listener), `curl` connection
  refused, probe heartbeat (`data/mlx_qwen3_heartbeat.json`) already
  correctly reporting `healthy: false` since that timestamp — the probe
  and sentinel were doing their job; the underlying service was just
  actually dead.
- **No live caller.** `config.py`'s `AI_PLAYERS` has zero entries with
  `"provider": "mlx"` — the `mlx-qwen3` agent id itself was already
  repointed to `provider="ollama"` / `ministral-3:3b` on olliemax back on
  2026-05-17 (HM-BN.1, inline comment at `config.py:502`). So
  `engine/ai_brain.py`'s `mlx_providers` tier has been permanently empty
  regardless of this server's health, since May — this box's local MLX
  server has not fed any live trading decision in four months.
- Timing coincides with bigmac's broader 2026-09-09 local-model
  stand-down (`~/.ollama/models` deleted the same window) — this looks
  like an intentional decommission that just never finished retiring the
  monitoring side, leaving `sentinel_mlx_qwen3_unhealthy` alerting hourly
  on a service nobody meant to keep alive.

**Retired, together, per the Admiral's instruction:**
- `launchctl bootout` on both `com.ollietrades.mlx-qwen3` and
  `com.ollietrades.mlx-qwen3-probe` — confirmed unloaded (`launchctl
  list` clean, PID 95800 gone).
- Both plists moved (not deleted) to
  `~/Library/LaunchAgents/_retired/*.plist.retired-2026-09-12`.
- `scripts/hm_ops_sentinel.py`: call site
  (`check_mlx_qwen3_heartbeat(alerts)`) commented out with a retirement
  note (same "comment in place, don't delete" convention as the Sulu
  persona retirement in `CLAUDE.md`), removed from the `main()` status
  print line. The function itself is left defined and undeleted, with a
  RETIRED note at the top of its docstring, for revival if a local MLX
  server is ever reinstated. `py_compile` clean; existing
  `tests/test_mlx_qwen3_heartbeat_sentinel.py` (tests the function
  directly, not the retired call site) still passes 8/8 unchanged.

Not touched: `scripts/mlx_qwen3_probe.py` itself (left in place, undeleted,
now simply never invoked since its trigger plist is unloaded+archived).
`data/mlx_qwen3_heartbeat.json` left as-is (will just stop updating).

## What's still open

Neither fix required a trader restart (both are cron/launchd-scheduled
sentinel-side code, not `main.py`'s live process — the `check_scan_
liveness` fix lives in `main.py` but only runs inside the trader's own
scheduler thread, so it WILL need the usual `launchctl kickstart -k
gui/$(id -u)/com.trademinds.trader` before it's live in the running
process; not done here, no restart authorized this session).
