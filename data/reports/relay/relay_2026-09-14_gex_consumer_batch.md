# Relay — 2026-09-14 evening — stale-GEX consumer batch (HM-GEX-CONSUMER-BATCH)

Executes the batch approved this morning in `relay_2026-09-14_screened_scan_silence_trace.md` §10:
every stale-capable GEX consumer moved onto `canonical_gex_if_fresh()` or the tier-0 30-min bar,
`battle_station` first, each with a test that fails on the code it replaced. One commit per consumer.

## Why it ran tonight, not at 14:05

The 14:05 after-close batch never ran.

- **Cause, per the Captain:** it was waiting on a background wait inside the working session. The wait
  died when that session ended, the same session-bound failure as the monitors on 9/11.
- **What this session verified:** nothing durable was ever scheduled for it. There was no crontab line,
  no LaunchAgent/LaunchDaemon, no `at` job, and no in-session cron. There was also no trace of batch
  work between `1f3388b` (09:31) and this session: no commits, no touched consumer files, no new tests,
  no log lines.
- **Not verified:** no written record of the 9/11 monitor failure turned up in the 9/11–9/12 relays,
  `XO_BACKLOG.md` or `DOCTRINE.md`. The 9/11 comparison is the Captain's.

**Rule:** if a scheduled follow-up matters, it goes in cron, not a session timer. (On bigmac that means
cron or a system LaunchDaemon; macOS has no systemd.) A session timer lives and dies with the session
that set it, and nothing alarms when it silently doesn't fire. Cron is also what survives an SSH-only
reboot on this box (CLAUDE.md, LaunchAgent Reboot Lifecycle).

## What shipped

| # | Commit | Consumer | Now reads | Tests (fail on old code / total) |
|---|---|---|---|---|
| 1 | `fa71a1c` | `engine/battle_station.py` — monitor, morning briefing, status | monitor + status: tier 0 (`alpaca_gex_if_fresh`, new); briefing: `canonical_gex_if_fresh` | 8 / 8 |
| 2 | `802bbf3` | `engine/ollie_commander.py` `_get_gex_pts` | `gex_levels` row gated by `gex_levels_row_is_fresh` (new, 30-min) | 9 / 11 |
| 3 | `1328689` | `engine/super_trader.py` `_gex_multiplier` | same gate | 3 / 6 |
| 4 | `30dc75c` | `engine/scout_critic.py` Scout brief | same gate; stale → explicit UNAVAILABLE line | 4 / 4 |
| 5 | `ceaf408` | `gex_calculator.build_alpaca_gex_prompt_section` (via `providers/base.py`) | refused at/after the 30-min bar | 4 / 5 |
| 6 | `e140b42` | `engine/kirk_advisory.py` `_get_gex_context` | `canonical_gex_if_fresh`; plus `$0` put-wall guard | 5 / 5 |
| 7 | `fb4690a` | `kirk_briefing.py` `gather_gex` | `canonical_gex_if_fresh` | 2 / 2 |
| 8 | `b1ffe35` | `engine/ready_room.py` DB-snapshot fallback | fallback only inside the 30-min bar (`created_at`) | 6 / 6 |
| 9 | `1f4a0f3` | `engine/signal_bridge.py` `_w3_context` | tier 0 (`alpaca_gex_if_fresh`) | 2 / 3 |
| 10 | `b6237ae` | `dashboard/app.py` Archer context GEX line | tier 0 (`alpaca_gex_if_fresh`) | 2 / 2 |

- **How "fails on old code" was checked:** each test file was run against the HEAD version of the files
  it covers, with the new versions restored afterward (verified with `cmp`).
- **Tests that pass on both:** those check that a *fresh* row still works, so they pass on old code by
  design.
- **Combined run after commit 10:** 102 passed, 2 xfailed (all ten new files, the existing GEX tests, and
  the existing tests that import `battle_station`).

## How each consumer got its gate

- **Tier 0 only (`canonical_gex.alpaca_gex_if_fresh`)** is used where a level is acted on within the
  minute, or where the read runs on a hot or per-row path. That covers battle_station's CLOSE_NOW
  flip-cross, the battle_station status endpoint, signal_bridge (runs per emitted row every 30 min) and
  the Archer context. There are two reasons:
  - `canonical_gex_if_fresh()` accepts a daily row up to 24h old.
  - Its tier 3 is a Polygon live compute, which must not run inside a 60s job or once per symbol.
- **`canonical_gex_if_fresh()`** is used for once-per-run readers: the battle_station morning briefing,
  kirk_advisory and kirk_briefing.
- **Row gate on `gex_levels`** is used for ollie_commander, super_trader and scout_critic. They score off
  `composite_score` / `composite_signal`, which `canonical_gex` does not produce, so there is nothing to
  route them to.
  - I did not build a new composite from canonical regime data, because that would change live
    confidence scoring without sign-off.
  - A stale row now scores exactly like no row: 0.2 crew points, 1.0× confidence.
- **`gex_levels.calc_time` is naive LOCAL time** (`gex_overlay` writes `datetime.now().isoformat()`).
  `snapshot_age_days()` assumes naive means UTC, so it reads these rows 7h young and would pass a row up
  to about 7.5h old under a 30-min bar. `gex_levels_row_is_fresh()` parses it as local instead.
- **ready_room** judges its fallback on `gex_snapshots.created_at` (genuine UTC), not the naive-local
  `timestamp`. That's the same basis as canonical tier 0 and `risk_manager`.

## Live effect of what was stale

- **`gex_levels`:** 489 rows over 67 symbols, all dated 2026-05-23..05-30 (read-only query tonight).
  - 477 rows say NEUTRAL and 12 say BULLISH; none say BEARISH.
  - The 12 symbols whose newest row is BULLISH are BB, BBY, FSLR, LLY, MRVL, MSI, MU, RL, ROK, RVTY, TSN
    and ZBRA.
  - On a BUY those got 0.4 Ollie crew points (neutral is 0.2) and a 1.10× super_trader multiplier.
- **battle_station:** it had no open option positions today, so the stale flip never auto-closed
  anything. It would have on the first position opened.

## Consequences to expect

- **Pre-market GEX is now blank** in the battle_station morning levels (06:25 MST) and Kirk briefings.
  - Pre-market, tier 0 is past its 30-min bar, the daily store is frozen, and Polygon is 403.
  - The OR War Room post and DayBlade's morning-levels prompt already omit GEX lines when the columns are
    NULL.
  - Yesterday's close snapshot isn't reachable, because `canonical_gex` gates tier 0 at 30 min
    internally. Making a ≤1-day Alpaca close snapshot usable pre-market is a design change, not done
    here.
- **Kirk advisory** reports regime `unknown` off-hours. The DEPLOY line no longer names a put wall
  unless one is fresh.
- **Ready Room** returns its existing "GEX data unavailable" result if the live Alpaca compute fails and
  the DB snapshot is past 30 min, instead of briefing on old levels.
- **Archer's context** says "GEX: UNAVAILABLE — no fresh gamma snapshot" off-hours. Before, it always
  said "GEX: ? | $xB", because `gex_snapshots` has no regime column.

## Flagged, not fixed

1. **`composite_score` scale mismatch.** The data is 0–100 (37–69.5), but ollie_commander and
   super_trader compare it to `> 0.6`, so every row counts as "strong". This only matters if the
   gex_levels writer is ever revived.
2. **The `gex_levels` 7-day prune isn't running**; rows from 05-23 are still there. Nothing was deleted
   (data doctrine).
3. **`signal_bridge` still reads flow lean from `flow_gex.db` `flow_aggregates`.** That's not GEX, and
   its freshness wasn't checked tonight.
4. **§11 restart follow-ups are unchanged** (persisting McCoy's done-today set; a `firing` heartbeat
   status).

## Live verification

- **Restart:** `zsh scripts/trader_restart.sh` at 20:12 MST, after the close and outside McCoy's
  late-recovery window (the §11 re-fire can't happen).
  - Exit 0; PID 50034 (started 07:56) was replaced by PID 76684.
  - One `trader.log` writer, bound to :8080.
- **New code is running:** `GET /api/battle-station/status` returned `gex_regime='positive_gamma'` before
  (May's `gex_levels` row) and `'unknown'` after (no fresh tier-0 snapshot).
- **Error log:** 20 new `trader_error.log` lines after the restart, all the normal startup banner. No
  tracebacks and no mentions of the changed modules. "Dashboard thread did not confirm startup" also
  appeared at the 07:56 restart, and the dashboard answered normally.
- **Scheduler after restart:** running. The first `[SCHED-JOB] start` came at 20:16:00
  (`run_events_bus_consumer`), 3m49s after the 20:12:11 relaunch. PID 76684 stayed alive and no
  Traceback reached `trader_error.log` (a background wait watched all three).
  - That's slower than this morning: after the 07:56:18 restart the first job started at 07:57:20
    (+62s).
  - Cause not investigated. Worth a look if the next restart is slow too.
- **Not live-verified:** `kirk_briefing.py` (cron, picked up on its next run) and the 06:25 morning
  briefing (first run tomorrow).

## Open

- Pre-market GEX source (see Consequences) — a decision for the Admiral.
- The flagged items above.
