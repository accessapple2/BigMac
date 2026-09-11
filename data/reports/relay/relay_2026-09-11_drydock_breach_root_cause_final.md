# Relay — Dry-dock breach: true root cause found, fixed, and verified. 2026-09-11

## Correction to my own earlier report

`relay_2026-09-11_drydock_breach_watchdog.md` and the response sent to
olliemax both declared the breach resolved after finding and killing a
stray `watchdog.py` instance. **That declaration was premature.** The
Admiral caught it: `main.py` was still running (PID 15197) through
everything from that point on — A1-A4, the pipeline trace, the
forward-return measurement, the learning-engine fix, and the olliemax
deliverable — none of which was re-verified against a running trader
before or during. The Admiral backed up both DBs and killed the process
personally. I then found and killed a further instance (PID 16488,
started 06:55) and still could not explain the respawn mechanism at that
point.

## Actual root cause (4th mechanism, now confirmed)

**`scripts/origin_healthcheck.sh`, crontab `*/5 * * * *` (line 11).** This
is a blind HTTP healthcheck with zero dry-dock awareness: every 5 minutes
it curls `http://localhost:8080/api/status`, and on any failure calls
`bash scripts/trader_restart.sh` directly and fires an alert titled
"main.py (bridge) restarted (failed healthcheck)". During dry-dock, a
stopped trader means port 8080 is *always* down, so this cron faithfully
"fixed" it every single tick.

This explains all three unexplained restarts from today: the 06:13
watchdog.py-triggered one (root-caused earlier, unrelated to this), and
the two later ones (~06:5x, and 07:00:00 exactly — caught live by a
background monitor with full parent-chain: `18067 bash scripts/
trader_restart.sh` → `18095 main.py`, timestamp a clean 5-minute cron
boundary). `hm_ops_sentinel.py` was checked and ruled out — it only
RED_ALERTs that main.py is down, it never restarts anything.

Four independent auto-restart vectors now confirmed and disabled for the
dock: `watchdog.py`'s own supervisor cron, `HM-TRADER-KEEPALIVE`,
`@reboot ... trader_restart.sh`, and now `origin_healthcheck.sh`. All four
carry the `# DRY-DOCK-2026-09-11 DISABLED (restore at undock): <original
line>` marker for a clean, verifiable undock.

## Verification this time (not just a point-in-time check)

- Killed the live instance (PID 18095) at 07:02.
- Disabled the `origin_healthcheck.sh` cron line via the standard
  backup → edit-copy → diff → count-guard → install pattern (172→172
  lines, exactly one line changed, confirmed live).
- Ran a 12-minute background monitor (15s polling) bracketing two more
  5-minute cron boundaries (07:05, 07:10) after the fix — clean the
  entire window, no further restart.
- `PRAGMA integrity_check` on both `data/trader.db` and
  `signal-center/signals.db`: **ok**, both.

## Did anything this morning's code changes actually execute against a running trader?

Checked directly rather than assumed:
- `main.py` (A3's cadence-cut edit) and `engine/mccoy_screen.py` were both
  saved at 06:18/06:19. Every restart after that edit booted clean — **zero
  Tracebacks/ImportErrors anywhere in today's `trader.log`** — and
  registered `run_mccoy_screened_scan` in the scheduler 3 separate times
  (once per restart), confirming the new code loads without error.
- The screened-scan's first daily trigger window is 9:35 AM ET — well
  after every restart/kill cycle observed this morning (all before ~07:02
  local). **Its actual scan logic never executed today** — only the
  registration ran. No live effect from A3's change yet.
- `ollama-plutus` (McCoy) decision_audit activity in the 06:00–07:05
  window: **zero rows.** The brief active windows between kills did not
  overlap with McCoy's own scan cadence.
- `daily_review_crew.py`'s `_flag_no_trade_active_players()` fix and the
  `arena_calls.jsonl` build were both standalone one-shot script runs
  against the DB directly, not through `main.py`'s process — independent
  of whether the trader was up, and not implicated by this breach.
- S8 season rotation (A1) ran via a direct `rotate_season(caller=
  "s8-manual")` call, also not through `main.py`. No lock errors or
  tracebacks found anywhere in today's log; `PRAGMA integrity_check`
  clean on both DBs. No evidence of corruption or a concurrent-write
  conflict, though a running trader process being up during part of that
  call was a real (if low-probability, SQLite-serialized) exposure window
  that a point-in-time check can't fully rule out after the fact.

**Bottom line: no evidence any of this morning's work was corrupted or
silently executed with lasting effect in a way that needs reverting.**
The exposure was real (the trader was genuinely up, on and off, most of
the morning) but nothing in the log or DB state shows damage from it.

## Cron audit — what's still firing during the dock, categorized

**Newly disabled (this pass):** `origin_healthcheck.sh` — the actual
restart culprit.

**Already disabled (prior gates):** `watchdog_supervisor.sh`,
`HM-TRADER-KEEPALIVE`, `@reboot ... trader_restart.sh`.

**Confirmed safe to leave running — Schwab reporting / infra, independent
of the paper trader's state:**
- `sync_schwab_live.py` / `sync_schwab_to_real_holdings.py`,
  `schwab_drawdown_alert.py`, `snapshot_real_portfolio.py` — Schwab
  balance/position reporting, explicitly read-only GET + `real_holdings.json`
  only per RULE #1 doctrine; this tracking continues regardless of the
  paper fleet's state.
- `db_snapshot.sh`, `backup_freshness_check.sh`, `offhost_backup.sh`,
  `rotate_logs.sh`, `git_push_health_check.py`, `disk_space_alert.sh`,
  the `_archive` TTL delete cron — pure infra/backup hygiene, unrelated
  to whether `main.py` is up (delete cron only ever touches `.db.gz`
  files already >30 days old, not today's dry-dock backups).
- `refresh_mtm_intrinsic.py` — marks existing open CSP positions to
  market; still meaningful while positions are open even with new entries
  paused.

**Confirmed safe to leave running — standalone/read-only research or
silent no-ops, waste a cron tick but touch nothing live:**
- `regime_refresh_runner.py` — writes to `regime_history` in its own
  process, independent of `main.py`'s scheduler by design; keeps regime
  state fresh for whenever the trader resumes.
- `q_dissent_watch.py` — silent no-op when there's nothing new in
  `crew_dissent_log` (which only War Room writes to).
- `uhura_agent.py` — writes `institutional_signals` directly (not through
  `main.py`), but never calls `paper_trader.buy()`/`.sell()` — signals
  just accumulate unconsumed while the trader is down. No RULE #1
  exposure.
- `mccoy_baseline_tracker.py`, `strategy_outcome_tracker.py`,
  `counterfactual_report.py`, `fleet_realism_sweep_clean_window.py`,
  `source_health_watcher.py`, `olliemax_health_probe.py` (confirmed —
  reads its own header: a single `curl /api/tags` every 5 min, ~12
  req/hr, not a meaningful contributor to olliemax's reported load),
  `signal_center_archive_rotate.py` — all read-only or self-contained,
  no trader dependency.
- `eod_report.py`, `kirk_briefing.py` (4 modes) — will produce
  "quiet/zero-trade-day" reports during the dock. Informational noise,
  not a safety issue; the Admiral already knows why.

**Flagged, not yet acted on — a judgment call, not a safety issue:**
`hm_ops_sentinel.py` (`*/5 * * * *`) fires a `red_alert`-level page
(actual paging channel, not a log line) every 5 minutes for the entire
dock, titled "main.py is not running" — which is now expected/intentional
but will page every 5 minutes regardless. Pausing the whole cron line
loses its other checks too (FD-leak, disk, source-health staleness,
boot-time), which have independent value. Left running; Admiral's call
whether the paging cadence is worth silencing for the dock.

## Correction owed to olliemax

`RESPONSE_TO_MODELWORKS.md` (sent earlier today) declared "main.py — the
trader process. It is stopped now, confirmed... Safe for your reboot,"
based on the watchdog.py-only root cause. That declaration was incomplete
— the trader kept respawning past that point via `origin_healthcheck.sh`,
independent of watchdog.py. Sending a correction now that the actual
final root cause is fixed and durably verified.
