# Relay — 2026-09-14: "zero decision_audit rows since Friday" trace, 9:35 slot, screen funnel, HM-SCREENED-SCAN-HB

## 1. Verdict: nothing was broken

- `decision_audit` max `created_at` before today was `2026-09-11 19:54:15`. **`created_at` is UTC**:
  that's Friday 12:54 MST / 15:54 ET, inside market hours.
- McCoy (`ollama-plutus`) writes ~100% of `decision_audit` (5,013 of 5,015 rows on 9/10). Since
  HM-XO-PLAN Phase 1.2 (9/11) McCoy scans only at 9:35 and 12:30 ET on weekdays, so zero rows
  from Saturday through Monday premarket is by design. Before that change, weekend rows came from the
  old every-2h `_SCAN_TIER2` scan (e.g. 9/5: 624 rows including 205 `HM-MARKET-CLOSED` rejects).
- Both daemon threads were alive in PID 1670 (started Sun 10:44:55 MST). A native `sample` at
  06:28:51 showed threads `mccoy_scheduler` and `qwen3_scheduler` parked in `time_sleep → nanosleep`.
  py-spy needs root.
- "No MCCOY-DAEMON lines in any log": a healthy tick logged nothing, and the startup lines were in
  `logs/_archive/trader_2026-09-14.log.gz`. `scripts/rotate_logs.sh` (05:00 daily, >100 MB) gzips
  trader.log and truncates it in place. The fds were not detached (inodes match).
- The archive (9/9 16:09 → 9/14 05:00) contained **no** screened-scan outcome line at all, so today
  was the dedicated thread's first real slot.

## 2. The 9:35 ET slot (06:35 MST), live

| | |
|---|---|
| scan start | 06:35:18 MST (`Session: MARKET`), two `run_scan`s: McCoy + qwen3 |
| qwen3 | finished 06:37:55, `16/100`; seat `halt_mode='full'`, no provider, so nothing to score |
| McCoy | finished 06:40:32, `16/100` screened + 5 Discovery adds (APLS, CNTA, SNAP, MRVL, HIMS) = 21 symbols, ~35 model calls |
| `signal_emit` | 21: BUY 16, HOLD 5 (GDXU, ORCX, CNTA, SMR, NAIL — conf 0.45-0.58) |
| `gate_reject` | 25 rows over the 16 BUYs: 9 symbols rejected twice by regime (`[regime_router] long_equity not approved in BEAR_CROSS (avoid-list)` + `REGIME-ROUTER: … (avoid-list)`), 7 `LOW_CONVICTION` (52% ×4, 60% ×2, 62% ×1) |
| `trade_fire` / trades | **0 / 0** |

The 5 HOLDs have no gate row because HOLD never reaches `buy()`. Nothing was lost.
Every BUY was stopped: 9 by the BEAR_CROSS avoid-list, 7 by the 65% conviction floor.

## 3. Screen funnel: why 16, not 100

`engine/mccoy_screen.get_mccoy_screened_symbols()` → `volume_scanner.get_todays_volume_alerts(limit=300)`.
Replayed read-only in code order. The 9/14 row reproduces the live 16 exactly:

| slot (UTC) | alerts | dedup to symbols | LIMIT 300 | blacklist / Alpaca-tradable | price ≥ $1 | $vol ≥ $1M | cap 100 |
|---|---|---|---|---|---|---|---|
| 9/14 13:35:18 | 1,082 | 60 | 60 | 50 | 49 | **16** | 16 |
| 9/11 13:35 | 1,901 | 100 | 100 | 90 | 89 | 34 | 34 |
| 9/11 16:30 | 2,025 | 132 | 132 | 117 | 116 | 40 | 40 |
| 9/10 13:35 | 1,518 | 75 | 75 | 65 | 63 | 29 | 29 |
| 9/10 16:30 | 2,166 | 166 | 166 | 136 | 134 | 54 | 54 |
| 9/09 13:35 | 2,467 | 679 | 300 | 265 | 264 | 200 | 100 |
| 9/09 16:30 | 2,875 | 690 | 300 | 279 | 278 | 220 | 100 |
| 9/08 13:35 | 1,294 | 80 | 80 | 70 | 69 | 30 | 30 |
| 9/08 16:30 | 1,750 | 171 | 171 | 148 | 147 | 66 | 66 |

- **16 is typical.** Pre-open slots return 16-34 on 4 of 5 days, midday 40-66. The top-100 cap bound
  only on 9/9. Phase 1.2 in practice scores whatever survives the filters, not a top 100.
- **The 1,082 → 60 step is repetition, not filtering.** Premarket Volume Radar re-alerts the same
  names every ~15 min (18.0 alerts per symbol before the slot).
- **The $1M dollar-volume floor is the biggest filter every day** (55-67% of what reaches it).
  The tradable filter removed TALK, HOLX, ASGN, VRE, LEG, CTRA, CUK today.

### Stale pre-market data: quantified

- At 9:35:18, **all 49** names reaching the floor were judged on pre-bell rows (newest alert
  13:10:35 UTC = 9:10 ET); `dollar_volume` = price × cumulative volume at that alert.
  **The floor removed 33.**
- **Freshness alone rescues none of them.** 26 of the 33 re-alerted in the 13:36:15 batch, and
  0 of the 26 clear $1M on the fresh row; 6 minutes of regular-session volume isn't enough.
- **What 60 s later would have returned: 100 symbols** (9:36:59 replay: 529 symbols → LIMIT 300 →
  246 tradable → 245 → 177 pass floor → cap 100), **with only 2 overlapping today's 16**.
  The difference is the candidate pool: Volume Radar's first regular-session batch (518 alerts) landed
  57 s after the screen ran.
- **The 9:35 slot races Volume Radar's first regular-session run.** That batch landed at 13:33:54
  (9/8), 13:33:11 (9/9), 13:31:21 (9/10), 13:36:15 (9/14), and not before 13:55 on 9/11 (no
  regular-session batch at all in the window). On 9/14 and 9/11 the slot scored pre-market movers only.

## 4. Live HOLDs vs the bakeoff's BUY 721/721

- **The live model HOLDs routinely.** `ollama-plutus` signals, last 30 days: BUY 9,896, HOLD 1,386.
  Today's 5/21 is not new behavior.
- **The bakeoff prompts differ materially from today's live prompt.** Its 16 prompts
  (`~/modelworks/fleet_checks/mccoy/arena_calls.jsonl`, captured 9/11 17:13 UTC) are **pre-fix,
  client-truncated request bodies**. All 3,179 McCoy emits with `prompt_text` on 9/10-9/11 carry
  `prompt_truncation_flag='qwen3_8b_default_ctx'`; the num_ctx fix `3daaa99` landed 9/11 12:40 MST,
  after the capture.
  - The captured bodies (16.3-16.6k chars) start at persona → BRAIN CONTEXT.
  - They are missing the leading ~10k chars the model now receives: MARKET REGIME header,
    WATCHLIST, CATALYSTS, GEX, ARENA INTELLIGENCE, TACTICAL DISPLAY, VOLUME RADAR, Chekov's reports,
    LEARNING CONTEXT, and the 6,091-char YOUR TRADING TRACK RECORD.
  - Replaying them at num_ctx 24576 doesn't restore what was cut.
  - All 16 were already live BUYs when captured.
- **Same symbol, opposite answers.** The capture includes ORCX and NAIL, both re-scored today:
  ORCX 9/11 BUY 0.82 (reasoning: "breaking below its opening range … bearish breakout") vs today
  HOLD 0.45, on the same `VOLATILITY BREAKOUT: ORCX BEARISH` block. NAIL: BUY 0.78 → HOLD 0.55.
- **The screened-scan path does not add different context.** Same `run_scan` → `build_prompt`
  template; only the watchlist block differs (21 vs 312 stocks). Live HOLD rate: 6.3% on
  312-stock/truncated prompts (9/10-9/11), 23.8% on 21-stock/untruncated prompts (today, n=21).
- **Not separable from today's data.** The regime lines also moved (`BULL_TREND` / MA-cross
  `CAUTIOUS_BEAR` on 9/11 → `CHOPPY` / `BEAR_CROSS` today). Restored context and regime shift are
  confounded.
- **Clean test (not run — GPU contention during market hours):** replay the stored full
  `decision_audit.prompt_text` for the 9/10-9/11 emits (same regime, untruncated) against the
  truncated captures, same seat, same samples.
- Side note: every MULTI-TIMEFRAME block in both captures and today's prompts reads
  `NEUTRAL (score: ~0) — Rate limited`.

## 5. olliemax model churn: not Riker

- **Riker is not involved.** gemma3:4b had 0 loads since 02:00 MST; it's resident (`UNTIL Forever`).
- **7 loads and 4 evictions since 04:00**, all around bridge_vote. "50" was likely log lines.
  - Loads: phi3:mini ×3, 0xroyce/plutus ×2, qwen3:8b blob ×2.
  - 0xroyce/plutus and qwen3:8b both have Modelfile `num_ctx 24576` × `OLLAMA_NUM_PARALLEL=2` =
    49,152 ctx (11.3-12.1 GiB predicted), so loading 0xroyce/plutus evicts qwen3:8b.
- **bridge_vote ran twice** (06:01:32 and 06:06:59 MST). Both guards compare the row count to
  `len(BRIDGE_VOTERS)`, but only 2 voters are un-halted, so the guard never trips and every 5-min
  job tick in 9:00-9:10 ET votes.
- **Cost per morning:**
  - vote chats 5.9-16 s;
  - each load 1-2 s;
  - two collateral qwen3:8b reloads made the next caller wait: 7.98 s at 06:02:09 (vs ~1.5 s normal)
    and options-sosnoff 8.36 s at 06:11:14 (vs 0.9 s);
  - zero failed calls; no loads during the 9:35 slot.
  - Roughly 15 s of added latency, doubled by the duplicate vote.
- **Options for the Admiral (nothing changed):**
  - fix the guard to count eligible voters (halves the cost; it's a real bug);
  - give bridge_vote's 0xroyce/plutus call a small num_ctx (probably fits beside qwen3:8b + gemma3;
    unmeasured; any other 0xroyce caller at 24576 would then reload it);
  - accept the reload cost.

## 6. Shipped — NOT LIVE until restart

HM-SCREENED-SCAN-HB (`engine/screened_scan_scheduler.py`, `main.py`, `docs/runbooks/logging.md`):

1. **Heartbeat.** Every 60 s tick logs one line per seat:
   `[SCREENED-HB] player=ollama-plutus tick=N et=Mon 09:35 pre-open=fired midday=outside_window`.
   It also rewrites `data/screened_scan_heartbeat_<player_id>.json` (pid, `thread_started_at_utc`
   stamped from inside the thread, tick, last tick, per-slot status/since/last fired/n_symbols),
   which log rotation and restarts don't erase.
2. **Seat gate (chosen over unscheduling).** A slot fires only if arena built a provider for the seat
   AND `halt_mode='active'` right now. Otherwise it logs `skipped_seat:<reason>` every tick of the
   window and is not marked done. It fails open on a DB read error (efficiency gate; downstream halt
   gates unchanged). Gating is cleaner than unscheduling: the skip stays visible, and a revive (plus
   the restart that building a provider needs anyway) works with no code change.
3. **Bracket fix.** Slot tags are escaped (`McCoy screened scan [pre-open]` renders again). The class
   is documented in `docs/runbooks/logging.md`; ~24 other `console.log` sites with `[{value}]` were
   not swept.
4. **Slot/window semantics unchanged:** 9:35 / 12:30 ET, 20+60 min window, one slot per tick,
   ET hour-0 reset, weekends skipped, error/empty screen still marks the slot done.

Verification: `tests/test_screened_scan_scheduler.py` (14) + `tests/test_riker_xo_schedule_gate.py` (6)
= **20 passed**; `py_compile` clean for main.py and the new module. main.py can't be imported in tests,
so the seat gate and wrapper delegation are tested by source extraction. **Not runtime-smoked in the
live process.** Restart timing: `QUESTION_screened_scan_hb_restart.md`.

**Post-restart checks:**
- `[SCREENED-HB]` lines every 60 s for both seats;
- both JSON files present with fresh `last_tick_at_utc`;
- qwen3 slots show `skipped_seat:no_provider_built` (or `halt_mode=full`);
- McCoy's next slot shows `fired` with n_symbols.

## 8. Live incident: shared scheduler stalled 06:41-07:18 MST (HM-RED-ALERT-SELF-LOCK)

Found while validating GEX against FlashAlpha's 09:54 ET recap. Question relay:
`QUESTION_red_alert_scheduler_stall.md`. The Admiral chose hotfix-then-restart.

- **Symptom.**
  - The `[WR-DEBUG-HB]` main-loop heartbeat stopped at 06:41:26.
  - The last `[SCHED-JOB]` line was `06:41:27 start name=run_volume_red_alert`, with no `done`.
  - None of the 168 shared-queue jobs ran for ~37 min of market hours, including
    `run_alpaca_gex_refresh`, so canonical GEX tier 0 went stale and fell through to the 7/21 row.
- **Evidence.**
  - Main-thread native stack: `sqlite3_step → btreeBeginTrans → sqliteDefaultBusyCallback → unixSleep`.
  - PID 1670 was the only process with trader.db open.
  - Log: `07:12:29 War Room post failed: database is locked`.
  - Alerts landed every ~169 s.
- **Mechanism.**
  - `volume_scanner.red_alert_check` held one connection open across its whole symbol loop and committed
    only at the end.
  - The first red-alert INSERT took the write lock.
  - Every later `_post_to_war_room → war_room.save_hot_take` (a second connection, via `_db_write_retry`)
    waited out the busy timeout and failed.
  - No red_alert row committed, and other writers hit `database is locked`.
- **Same class as 9/11's missed McCoy 12:30 slot:** the second time one long job has taken out the shared
  queue. See the new DOCTRINE.md entry (`487a845`), which leaves open the Admiral decision on a per-job
  timeout for that queue.
- **Fix `989ed38`.**
  - The loop only decides.
  - Rows go in one short `executemany` + commit.
  - War Room posts happen after that connection is closed.
  - A write failure is logged.
- **Test.** `tests/test_red_alert_no_self_lock.py`: 3 tests, **all failed against the old code** (lock probe
  `[True, False, False]`, the live pattern) and pass against the fix.
- **Restart** 07:18:02 → PID 43636, `RESTART OK`.
- **Verified on the first real post-restart run** (07:24:27, not a clean startup):
  - 7 RED/CRITICAL alerts;
  - all **7 War Room posts landed** within 1 s (LEG, HOLX, EA, TALK, ASGN, TNON, VRE);
  - **5 red_alert rows committed** (the 2 CRITICALs already flagged re-post without insert);
  - **0** `War Room post failed`;
  - **0** `database is locked` lines in any minute since the restart;
  - wall **0.600 s**;
  - `[WR-DEBUG-HB] loop alive jobs=168` back at 07:22:48.

## 9. GEX prompt block served 7/21 levels as current (HM-GEX-PROMPT-FRESHNESS, `e5f2dc7`)

- **Structural check vs FlashAlpha 09:54 ET** (flip 767.52, call wall 760, put wall 750, net −$14.47B,
  spot 759.88):
  - **canonical Alpaca snapshot** (05:02 MST): passes all four checks (net −$12.77B, call wall 775 ≥ spot,
    put wall 750 ≤ spot, flip 766.31);
  - **served** `/api/market/gex` and every agent prompt: the 7/21 `flow_gex.db` row (net **+**$846M,
    **call wall 748 < spot**, put wall 607, flip 752), which fails sign and call-wall side.
- **Root cause.**
  - `gex_overlay.get_gex_context_for_prompt()` never called `canonical_gex()`. It read the dead Polygon
    intraday cache, then `latest_snapshot()` (flow_gex.db) with no age check.
  - HM-GEX-ALPACA-REPOINT only repointed `canonical_gex()`. Its commit note that the prompt injection "used
    Alpaca the whole time" was wrong for this block.
  - The API path goes through `canonical_gex()`, but tier 0 needs an Alpaca row < 30 min old. Those rows
    only land when `run_alpaca_gex_refresh` runs in RTH, and today the stall stopped it.
- **Exposure.**
  - The block is in 100% of logged McCoy prompts since 9/10 (2,112 + 1,067 + today's).
  - On 9/10-9/11 the truncation cut it away before the model saw it.
  - From the 9/11 12:40 num_ctx fix on, the model sees it: every McCoy decision today.
- **Fix.**
  - Per symbol via `canonical_gex_if_fresh()`: Alpaca under its 30-min bar, else the shared 1-day gate.
  - Each level shown carries as-of, age and source.
  - Stale/missing/degenerate prints `GEX UNAVAILABLE — … Do not assume any call wall, put wall, gamma flip
    or gamma regime`.
- **Test.** `tests/test_gex_prompt_freshness.py`: 4 tests, **all failed against the old builder**, all pass.
- **Rendered read-only at 14:26 UTC:** both SPY and QQQ `GEX UNAVAILABLE`, correct at the time
  (newest Alpaca row 2.4 h old). A fresh Alpaca row landed 14:27:16.
- **Not live until the restart after 07:56 MST.** An earlier restart would re-fire McCoy's pre-open slot
  again (see §11).

## 10. Other stale-capable GEX consumers (inventory, NOT fixed yet)

**Live decision paths with no freshness gate:**

| consumer | reads | age of data | effect |
|---|---|---|---|
| `engine/battle_station.py` `_generate_signal` (via `gex_overlay.get_latest_gex`) | `gex_levels` | **last write 2026-05-30** | `CLOSE_NOW` on "broke below/above gamma flip" → `_auto_close` (paper orders via alpaca_options); `TIGHTEN` near walls; morning levels table. A stale row existing means it never recomputes. **Dormant today:** positions come from Alpaca and no "Battle Station: monitoring N" line today = no open option positions. Live the moment one opens. |
| `engine/ollie_commander.py` `_get_gex_pts` | `gex_levels` | 2026-05-30 | 0-0.4 pts of crew scoring |
| `engine/super_trader.py` `_gex_multiplier` | `gex_levels` | 2026-05-30 | 0.85-1.10× confidence |
| `engine/scout_critic.py` | `gex_levels` | 2026-05-30 | flip/walls text in an LLM brief |
| `engine/providers/base.py` → `gex_calculator.build_alpaca_gex_prompt_section` | Alpaca cache → `gex_snapshots` | any | per-symbol prompt block; labels `[Nm old]` but never refuses |
| `engine/kirk_advisory.py` `_get_gex_context` | `gex_snapshots` | any | regime + put wall in Kirk's portfolio advisory |
| `kirk_briefing.py` `gather_gex` (5 crontab lines) | `gex_snapshots` | any | Kirk daily briefing |
| `engine/ready_room.py` fallback | `gex_snapshots` | any | only when live compute fails → briefing → bridge_vote |
| `engine/signal_bridge.py` `_w3_context` | `flow_gex.db` | 2026-07-21 | shadow-signal context (observation-only) |
| `dashboard/app.py` `_build_computer_context` (L20398) | `gex_snapshots` | any | Ship's Computer context text |

**Already marked or harmless:**
- `/api/market/gex[/{t}]`, gex-overlay levels/heatmap and chart-data carry `stale`/`age_days`, but still
  serve the 7/21 levels as the payload.
- `/api/gex-snapshot` has no marker (observation-only).
- `risk_manager` GEX gate: gated (30 min).
- `screener_engine` / `daily_enrichment` read `autonomous_trader.db`, which has no `gex_levels` table,
  so they return nothing.
- `gex_engine.get_latest_gex_for_uhura`: no live caller found.

**`battle_station` is dormant by accident, not by design.** Nothing gates it:
- It auto-closes positions when price crosses a gamma flip read from `gex_levels`, last written
  2026-05-30.
- It hasn't fired only because Alpaca has had no open option positions.
- The first option position opened would be judged against a level from another season.

**Approved (Admiral, 2026-09-14): one batch after the close.**
- `battle_station` first.
- Each consumer moves onto `canonical_gex_if_fresh()` or the tier-0 30-min bar.
- Each fix gets a test that fails on today's code.

Per-job scheduler timeout: spec requested, not built. See `docs/HM-SCHED-JOB-TIMEOUT-spec.md`.

## 11. Restart side effects (07:18)

- **McCoy's pre-open slot re-fired at startup.**
  - The done-today set is in memory, and the late-recovery window (to 10:55 ET) was still open.
  - `Session: MARKET` at 07:18:04, a second full screened scan on post-bell data.
  - It finished 07:52:58 (`fired_late`, **100 symbols**, ~35 min, 103 `signal_emit` rows vs 21 at the
    real 9:35 slot).
  - The restart for HM-GEX-PROMPT-FRESHNESS was held until after 07:56 MST (window end 10:55 ET) so it
    could not re-fire a third time.
  - The old code had the same behavior.
  - Fix later: persist done-today in `data/screened_scan_heartbeat_<player>.json`.
- **`[SCREENED-HB]` works but rich wraps the body onto continuation lines.** grep with `-A3`, or read the
  JSON files (qwen3: `pre-open=skipped_seat:no_provider_built` every tick, seat gate confirmed live).
- **McCoy's heartbeat file shows `tick 0` for the whole re-fire:** a tick is recorded only after the fired
  slot returns, so the runbook's ">2 min stale = dead" rule false-alarms during a long scan. Needs a
  `firing` status written before the scan.

## 12. Today's McCoy decisions were made on stale GEX (flagged, not rewritten)

`data/reports/relay/flag_stale_gex_decision_audit_2026-09-14.json` lists every affected row: flag
`STALE_GEX_IN_PROMPT`, evidence query, reason. **No decision_audit row was modified.** Final counts
(regenerated 07:53 MST, after the re-fire ended and before the GEX-fix restart):
- **124 `signal_emit`**:
  - 21 at the 9:35 ET slot;
  - **103 in the restart re-fire** of the pre-open slot (07:18-07:52 MST, `fired_late`, 100 symbols);
  - 71 BUY / 53 HOLD;
  - last flagged emit 14:50:32 UTC;
  - every McCoy emit today carries the block (0 without it).
- 70 `gate_reject` linked by `signal_id` and 61 regime_router rejects matched by player+symbol within 60 s.
- **0 trades.**
- The restart re-fire produced 5× the real slot's rows, so it is most of today's stale-GEX exposure.
  That is one more reason to persist done-today across restarts (§11).

The 9/10-9/11 rows also contain the block, but truncated away before the model; not flagged.

## 7. Open

- ~~Restart to activate HM-SCREENED-SCAN-HB~~ live since 07:18 (seat gate + heartbeat verified).
- ~~Restart after 07:56 MST to activate HM-GEX-PROMPT-FRESHNESS~~ **done and verified.**
  - **Restart:** 07:56:18 → PID 50034, `RESTART OK`.
    - No pre-open re-fire: both heartbeats `pre-open=outside_window` under the new PID.
    - Scheduler alive: `[SCHED-JOB]` lines from 07:56:29.
    - Startup queue sweep: 8 McCoy `gate_reject` `stale_signal` rows for 2026-09-10 signals (age ~314,000 s,
      no prompt). Not decisions, not flagged.
  - **Production check at McCoy's 12:30 ET midday slot:** `decision_audit` id 141083 (TNON, 16:30:56 UTC).
    Its prompt carried `SPY (as of 2026-09-14 16:21:27 UTC, 9 min old, source alpaca): King Node $775 |
    Gamma Flip $762 | Put Wall $760 | Call Wall $775 | Regime: LONG GAMMA · stable (spot above flip)`, with
    QQQ likewise from Alpaca and 9 min old. **PASS.**
    - The 7/21 block is gone.
    - The Alpaca refresh is current on the unblocked scheduler.
    - Rows from this slot on are not stale-GEX rows.
- Item-3 consumer batch (§10), battle_station first, awaiting go.
- Persist done-today across restarts; `firing` heartbeat status (§11).
- DOCTRINE open decision: per-job timeout for the shared scheduler queue.
- Screen design (Admiral's call, nothing changed): slot timing vs Volume Radar's first
  regular-session batch; what the $1M floor should mean at 9:35.
- Truncated vs untruncated replay experiment (§4), after close.
- bridge_vote duplicate-vote guard bug + num_ctx decision (§5).
- Optional: sentinel staleness check on `data/screened_scan_heartbeat_*.json` (cron = different
  mechanism than the thread it watches).
- Sweep of the ~24 other bracket-swallowing log sites.
