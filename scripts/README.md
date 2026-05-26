# `scripts/` — Operational tooling

Quick reference for the hand-runnable scripts. Each entry: what, when, how.

---

## Admiral pre-bell suite (HM-ADMIRAL-PREMARKET-CHECK, 2026-05-25)

The intended runway is a one-shot pre-bell check, then a single
safety-gated unhalt. Designed for Memorial Day Tuesday 2026-05-26 and
every future post-holiday morning.

### `admiral_premarket_check.sh`

Eight read-only checks. Run before bell to confirm system, calendar,
agents, flags, broker, scanners, DB, and convergence are all healthy.

```bash
scripts/admiral_premarket_check.sh
```

Exit code 0 = all PASS, safe to proceed. Exit code 1 = at least one
FAIL, do not unhalt without investigating.

Checks:
1. **System health** — trader PID, signal-center PID, ports 8080 + 9000
2. **Market calendar** — `engine.market_calendar.get_market_status()` direct call (source of truth, not the live process which may run pre-merge bytecode)
3. **Agent halt state** — neo-matrix + ollie-auto current `halt_mode`
4. **Conviction-stop flags** — `CONVICTION_SCALED_{STOPS,TRAIL,OPTIONS_STOP}_ENABLED` from `.env` (expected: all False or absent)
5. **Alpaca state** — equity + pending-orders count via `/api/alpaca/{status,orders}`
6. **Scanner job health** — `MAX(computed_at)` from `rs_rank`, `minervini_trend`, `squeeze_watch.scan_ts` (within 24h)
7. **DB integrity** — `portfolio_history` row count for yesterday
8. **Convergence smoke** — `/api/navigator/convergence` first signal includes `strategy_names` array (verifies b76ea91 fix still live)
9. **Code freshness** — trader PID start time vs `git log -1 HEAD` commit time. FAIL if HEAD commits exist newer than PID start, with count of unloaded commits + pointer to the restart script. Born from the Memorial Day 2026-05-25 finding that the trader had been running pre-Memorial-Day bytecode for 24h while the holiday gates lived on disk only.

Read-only. No writes. No mutations. Safe to run any time.

### `admiral_trader_restart.sh`

Controlled restart with safety gates. Stops the running trader and
starts a fresh process so newly-shipped commits become live in-process.

```bash
scripts/admiral_trader_restart.sh           # dry-run preview
scripts/admiral_trader_restart.sh --confirm # execute
```

Why this exists (not `launchctl kickstart`): per CLAUDE.md "LaunchAgent
Reboot Lifecycle" (2026-05-23), `launchctl kickstart gui/$UID/com.trademinds.trader`
returns "Domain does not support specified action" on this Mac when run
over SSH. Production startup uses `scripts/trader_reboot_start.sh` via
`@reboot` cron with a `nohup … &` detach. This script reuses the same
detach pattern after a graceful SIGTERM → SIGKILL fallback.

Safety gates (all must pass + `--confirm`):
1. Git working tree clean (no uncommitted tracked changes)
2. Conviction flags all False or absent
3. neo-matrix + ollie-auto at a safe `halt_mode` (NULL = unsafe)
4. Python interpreter at `.venv/bin/python3` present
5. Trader PID currently alive (this is restart, not cold-start)

Sequence:
1. Capture old PID + start time, show `git log` of commits to be loaded
2. Gate eval — abort if any gate fails
3. SIGTERM → 20s grace → SIGKILL fallback
4. `nohup .venv/bin/python3 main.py &` (detached, mirrors reboot script)
5. Wait up to 30s for new PID + port 8080 listening
6. Re-run premarket check
7. Print old PID / new PID / commits loaded / premarket result

### `admiral_unhalt_agents.sh`

Safety-gated unhalt for neo-matrix + ollie-auto.

```bash
scripts/admiral_unhalt_agents.sh           # dry-run preview, no writes
scripts/admiral_unhalt_agents.sh --confirm # execute UPDATE
```

Behavior:
1. Re-runs `admiral_premarket_check.sh` as the gate. Aborts non-zero if any check FAILs.
2. Prints current `halt_mode` + `halted_at` + `halt_reason` for both rows.
3. **Without** `--confirm`: prints the SQL it WOULD run, exits 0.
4. **With** `--confirm`: executes `UPDATE ai_players SET halt_mode='active' WHERE id IN ('neo-matrix','ollie-auto') AND halt_mode='exit_only'`.
5. Verifies post-state.
6. Prints reminders (halt flip read on next cycle, no restart needed, `halted_at` + `halt_reason` preserved per CLAUDE.md "do not clear" rule).

The Memorial Day morning runway: `admiral_premarket_check.sh` → fix any reds → `admiral_unhalt_agents.sh --confirm`. Done.

---

## Backtest tooling

| Script | Purpose |
|---|---|
| `backtest_baseline.py`     | Run the canonical baseline replay |
| `backtest_report_card.sh`  | Generate a per-agent OOS report card |
| `backtest_status.sh`       | Show in-flight backtest status |
| `backtest_watcher.sh`      | Tail the backtest output stream |

See `scripts/README_BACKTESTS.md` for the full backtest doctrine.

---

## Operations / housekeeping

| Script | Purpose |
|---|---|
| `bigmac_mem_watch.sh`           | Tail bigmac RAM + Ollama VRAM telemetry |
| `daily_watch_summary.sh`        | EOD watchlist roll-up |
| `daily_ministral_metrics.py`    | Ministral provider latency + accuracy metrics |
| `finmem_decay_daily.sh`         | FinMem memory decay nightly cron target |
| `close_player_positions.py`     | Force-close a single player's open positions (manual) |
| `agent_allocation_report.py`    | Allocation report per agent vs portfolio |

---

## Reboot/launch wrappers (HM-REBOOT-LIFECYCLE, 2026-05-23)

LaunchAgent boot is broken on this Mac per the lesson in `CLAUDE.md`.
These shell wrappers are invoked from `@reboot` crontab entries instead.

| Script | Crontab target |
|---|---|
| `trader_reboot_start.sh`         | `com.trademinds.trader`        |
| `signal_center_reboot_start.sh`  | signal-center Flask app on :9000 |
| `cloudflared_reboot_start.sh`    | bridge.ollietrades.com tunnel  |

---

## Data export / migration

| Script | Purpose |
|---|---|
| `export_training_data.py`        | Export curated training corpus |
| `extract_plutus_corpus_v1.py`    | Plutus fine-tune corpus extractor |
| `backfill_13_megacaps_2026-05-14.py` | One-off megacap signal backfill (kept for reference) |
| `benchmark_cycle_player.py`      | Per-player cycle wall-time benchmark |

---

## Archived

`scripts/_archive/` and `scripts/archive/` hold superseded scripts.
Per the sacred-data rule: never deleted, only moved aside with a
dated suffix so they can be revived if a regression demands it.
