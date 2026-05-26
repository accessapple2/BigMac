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

Read-only. No writes. No mutations. Safe to run any time.

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
