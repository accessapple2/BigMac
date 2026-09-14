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

## 7. Open

- Restart to activate HM-SCREENED-SCAN-HB (question filed).
- Screen design (Admiral's call, nothing changed): slot timing vs Volume Radar's first
  regular-session batch; what the $1M floor should mean at 9:35.
- Truncated vs untruncated replay experiment (§4), after close.
- bridge_vote duplicate-vote guard bug + num_ctx decision (§5).
- Optional: sentinel staleness check on `data/screened_scan_heartbeat_*.json` (cron = different
  mechanism than the thread it watches).
- Sweep of the ~24 other bracket-swallowing log sites.
