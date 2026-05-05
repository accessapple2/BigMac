# HM-T-fleet — Silent-Inertness Audit
*2026-05-05, Scotty investigation, no fixes applied*

## Question
PED (`post_earnings_drift`) was found silently inert in HM-T (2026-05-04, retired in commit 0ead8d4): scheduled but produced zero signals because its qualifying-ticker condition was structurally unreachable. **How many other agents are PED-class?**

## Inventory

- **Total scheduled `schedule.every(...).do(...)` registrations in `main.py`:** 130 (excluding archive paths)
- **Total `ai_players` roster:** 49 rows
  - `is_active=0`: 1 (`webull`, the human benchmark — by design)
  - `halt_mode != 'active'`: 4 (`dayblade-sulu`, `gemini-2.5-pro`, `grok-3`, `ollama-llama`)
  - `halt_mode='active'`: 44
- **Orphan in `signals` not in roster:** 1 (`debate-pipeline`, 1 row dated 2026-03-31)
- **Orphan in `trades` not in roster:** 0

Note that "scheduler registrations ↔ players" is many-to-many and NOT 1:1. Most jobs (`run_scanner`, `run_war_room`, etc.) dispatch to the active fleet roster as a unit; only a few (`run_dayblade`, `run_capitol_scan`, `run_volume_red_alert`, `run_riker_synthesis`) are agent-specific. So the question splits in two:

1. **Per-player liveness** — does this `ai_players` row produce any observable output (signals or trades) post-roster-add?
2. **Per-job liveness** — does this scheduled job produce any observable output (rows in expected tables, logs, downstream dispatch)?

This audit prioritizes (1) because that's what HM-T did for PED. (2) is partially covered for the most prominent dormant jobs (battle-station feeders, signal_scorecard) per `docs/AUDIT_6_INVESTIGATION_2026-05-04.md` and 2026-05-03 fleet reality.

## Classification table

Legend: 🟢 Active · 🟡 Sparse · 🟠 Inert (PED-class) · 🔴 Halted · ⚫ Orphaned · ⚪ By-design (gate / tracker / human)

| Player | Class | Lifetime sigs | Lifetime trades | sigs 7d | trades 7d | Most recent | Notes |
|---|---|---:|---:|---:|---:|---|---|
| anderson-bcs        | 🟠 Inert | 0     | 0   | 0    | 0  | (never)              | options_agents.py BCS class — file imported by nothing |
| capitol-trades      | 🟢 Active| 0     | 64  | 0    | 13 | 2026-05-04 11:25     | rule-based — emits direct to trades, no signals row |
| chekov              | 🟡 Sparse| 0     | 14? | 0    | 0  | 2026-04-21           | retired/muted per CLAUDE.md threshold raise |
| claude-haiku        | 🔴 Halted-equiv | 5,514 | 72  | 0    | 0  | 2026-03-17           | not formally halted but stopped — paid-model wind-down 2026-04-16 |
| claude-sonnet       | 🔴 Halted-equiv | 2,561 | 67  | 0    | 0  | 2026-04-06           | paid-model wind-down |
| covered-call        | 🟠 Inert | 0     | 0   | 0    | 0  | (never)              | options_agents.py CC class — orphaned |
| cto-grok42          | 🟡 Sparse| 25    | 13  | 25   | 13 | 2026-05-05 02:07     | Sunday-rotation player — fires once weekly window |
| dalio-metals        | 🟡 Sparse| 357   | 37  | 0    | 0  | 2026-04-20 (trade)   | metals briefer; runs daily at fixed hour |
| dayblade-0dte       | 🟠 Inert (was 🟢) | 771   | 291 | 0    | 0  | 2026-04-01 (sig)     | T'Pol/plutus — empirical pause since 2026-04-07 (HM-T-fleet candidate, see fleet reality 2026-05-03) |
| dayblade-sulu       | 🔴 Halted| 2,664 | 15  | 0    | 0  | 2026-04-07 (sig)     | halt_mode=exit_only since 2026-03-31; emitted 196 post-halt sigs (HM-A leak) |
| deepseek-7b-grok4   | 🟢 Active| 1,756 | 107 | 1,311| 56 | 2026-05-05 06:02     | top emitter |
| energy-arnold       | 🟡 Sparse| 9,843 | 20  | 811  | 0  | 2026-05-05 04:35 (sig)| high signal volume, ZERO trades 7d — bridge_voter wired but bridge_votes stalled 2026-05-01 13:01 (per CLAUDE.md HM 2026-05-03) |
| enterprise-computer | ⚪ By-design | 0 | 0 | 0    | 0  | n/a                  | system role: metals_tracker writes to war_room (not signals); paper_trader/risk_manager exempt-list |
| gemini-2.5-flash    | 🔴 Halted-equiv | 4,386 | 27 | 0    | 0  | 2026-04-23           | paid-model wind-down |
| gemini-2.5-pro      | 🔴 Halted| 2,012 | 102 | 0    | 0  | 2026-03-17           | clean halt (0 post-halt sigs) |
| ghost-kirk-0dte-bc  | ⚪ By-design | 0 | 0 | 0    | 0  | n/a                  | ghost-trading tracker; doesn't emit `signals` |
| ghost-kirk-bc       | ⚪ By-design | 0 | 0 | 0    | 0  | n/a                  | ghost-trading tracker |
| ghost-long-call     | ⚪ By-design | 0 | 0 | 0    | 0  | n/a                  | ghost-trading tracker |
| ghost-naked-put     | ⚪ By-design | 0 | 0 | 0    | 0  | n/a                  | ghost-trading tracker |
| gpt-4o              | 🔴 Halted-equiv | 650 | 80 | 0    | 0  | 2026-03-17           | paid-model wind-down |
| gpt-o3              | 🔴 Halted-equiv | 1,007 | 21| 0    | 0  | 2026-04-06           | paid-model wind-down |
| grok-3              | 🔴 Halted| 2,453 | 101 | 0    | 0  | 2026-03-17           | clean halt (0 post-halt sigs); zombie deactivated 2026-04-23 drydock |
| grok-4              | 🔴 Halted-equiv | 5,767 | 25| 0    | 0  | 2026-04-08 (sig)     | paid-model wind-down |
| mccoy-bps           | 🟠 Inert | 0     | 0   | 0    | 0  | (never)              | options_agents.py BPS class — orphaned |
| mlx-qwen3           | 🟡 Sparse-NoTrades | 2,716 | 0 | 1,051 | 0 | 2026-05-05 06:24 (sig) | high signal volume, zero trades — signals don't convert downstream |
| navigator           | 🟡 Sparse| 307   | 10  | 0    | 0  | 2026-04-14 (sig)     | retired/archive candidate per CLAUDE.md |
| neo-matrix          | 🟡 Sparse| 0     | 14  | 0    | 0  | 2026-04-21 (trade)   | rule-based, exempt-list — emits via crew_scanner direct |
| ollama-coder        | 🟡 Sparse-NoTrades | 1,459 | 0 | 1,100 | 0 | 2026-05-05 06:15 | qwen2.5-coder:7b utility/lab role; signals not gated through |
| ollama-deepseek     | 🟡 Sparse| 398   | 47  | 25   | 0  | 2026-04-29 (sig)     | weekly cadence |
| ollama-gemma27b     | 🟡 Sparse| 216   | 0   | 25   | 0  | 2026-04-29 (sig)     | weekly cadence, no trades |
| ollama-glm4         | 🟡 Sparse| 25    | 0   | 25   | 0  | 2026-04-29           | weekly cadence, no trades |
| ollama-kimi         | 🟡 Sparse| 230   | 44  | 25   | 0  | 2026-04-29 (sig)     | weekly cadence |
<!-- HM-T-fleet-correction: 2026-05-05 06:45 MST — '2 post-halt trades' was wrong (7-day window query conflated with lifetime); actual lifetime post-halt SELL count is 7, all clean exits (Verdict A). See "ollama-llama post-halt trades — VERDICT A" section below. -->
| ollama-llama        | 🔴 Halted (signal-leak only) | 3,617 | 53 | 663 | 2 | 2026-05-01 19:37 (sig) | halt_mode=exit_only since 2026-04-25; **947 post-halt sigs** (signal-emission gate gap). 7 post-halt trades — all clean exits, Verdict A (see correction note below) |
| ollama-local        | 🟡 Sparse| 6,890 | 150 | 25   | 0  | 2026-05-01 (sig)     | high lifetime emit, recent activity dropped to weekly |
| ollama-plutus       | 🟢 Active| 1,010 | 53  | 700  | 40 | 2026-05-05 05:50     | McCoy plutus-3B path |
| ollama-qwen3        | 🟢 Active| 2,790 | 76  | 800  | 44 | 2026-05-05 04:48     | top emitter |
| ollie-auto          | ⚪ By-design | 0 | 69 | 0    | 11 | 2026-05-01 (trade)   | quality gate writes to trades when forwarding, not signals |
| options-sosnoff     | 🟢 Active| 1,871 | 12  | 800  | 0  | 2026-05-05 04:55     | options-flow signals; 0 trades 7d (signals not converting) |
| quark-ic            | 🟠 Inert | 0     | 0   | 0    | 0  | (never)              | options_agents.py IC class — orphaned |
| qwen-coder-haiku    | 🟡 Sparse| 25    | 0   | 25   | 0  | 2026-04-29 (sig)     | weekly cadence, no trades |
| qwen3-14b-grok3     | 🟡 Sparse| 41    | 0   | 25   | 0  | 2026-04-29 (sig)     | weekly cadence |
| qwen3-14b-pro       | 🟠 Inert | 0     | 0   | 0    | 0  | (never)              | proving_ground / super_backtest_v4 / cost_tracker references — lab/backtest scaffold, not production emitter |
| qwen3-8b-4o         | 🟡 Sparse| 25    | 0   | 25   | 0  | 2026-04-29 (sig)     | weekly cadence, no trades |
| qwen3-8b-flash      | 🟢 Active| 864   | 56  | 763  | 30 | 2026-05-05 05:04     | top emitter |
| qwen3-8b-o3         | 🟡 Sparse| 25    | 0   | 25   | 0  | 2026-04-29 (sig)     | weekly cadence, no trades |
| qwen3-8b-sonnet     | 🟡 Sparse| 25    | 0   | 25   | 0  | 2026-04-29 (sig)     | weekly cadence, no trades |
| red-alert           | 🟠 Inert (channel-mismatch) | 0 | 0 | 0 | 0 | (never) | `engine/red_alert.py:281` writes to `red_alert_log` (not `signals`) — table doesn't exist in current schema either; Volume Red Alert job (`run_volume_red_alert` every 5 min) calls `engine/volume_scanner.py::red_alert_check` which writes to `volume_alerts`, not via player_id |
| super-agent         | 🟡 Sparse| 83    | 16  | 0    | 0  | 2026-04-08 (sig)     | exempt-list system role; idle for ~27 days |
| webull              | ⚪ By-design | 0 | 127 | 0   | 0  | 2026-02-26 (trade)   | human benchmark, is_active=0 |

## Class summary

| Class | Count |
|---|---:|
| 🟢 Active (signals + trades in 7d) | 4 (deepseek-7b-grok4, ollama-plutus, ollama-qwen3, qwen3-8b-flash) |
| 🟢 Active-emit-only (signals 7d, no trades) | 1 (options-sosnoff) |
| 🟡 Sparse — weekly cadence (~25-sig batch on Sunday) | 12 (the qwen3-* and ollama-* lab tier) |
| 🟡 Sparse — high-emit-no-convert | 3 (mlx-qwen3, ollama-coder, energy-arnold) |
| 🟡 Sparse — rule-based / different write path | 4 (capitol-trades, chekov, neo-matrix, super-agent) |
| 🟡 Sparse — daily-fixed-time | 2 (dalio-metals, cto-grok42) |
| 🟠 Inert (PED-class) | 7 (anderson-bcs, covered-call, mccoy-bps, quark-ic, qwen3-14b-pro, red-alert, dayblade-0dte) |
| 🔴 Halted (formally) | 4 (dayblade-sulu, gemini-2.5-pro, grok-3, ollama-llama) |
| 🔴 Halted-equiv (paid-model wind-down, never formally halted) | 6 (claude-haiku, claude-sonnet, gemini-2.5-flash, gpt-4o, gpt-o3, grok-4) |
| ⚫ Orphan in signals not in roster | 1 (debate-pipeline) |
| ⚪ By-design (gate / tracker / human) | 6 (enterprise-computer, ollie-auto, ghost-{kirk-0dte-bc,kirk-bc,long-call,naked-put}, webull) |

## Inert agents (🟠 PED-class) — root cause per agent

### `anderson-bcs`, `mccoy-bps`, `quark-ic`, `covered-call`
- **Code path:** `engine/options_agents.py` defines 9 `OptionsAgent` subclasses with `scan(regime, vix, convergence_signals)` methods (lines 99, 140, 229, 322, 405, 507, 592, 661, 731) and a `run_scan_cycle` orchestrator at line 799.
- **Why inert:** **Nothing imports `options_agents`.** Confirmed by `grep -rn "from engine.options_agents\|from engine import options_agents\|options_agents\." engine/ main.py` returning 0 hits.
- **Connection to fleet reality:** Already flagged in CLAUDE.md "Pending TODOs (additions from 2026-05-03 reconciliation)" — *"Backtest 8 orphaned options strategies in `engine/options_agents.py` (zero `main.py` refs) — wire/retire decision blocks Sunday Deep Dive Phase 4."*
- **Recommendation:** **Retirement candidate.** Same archive pattern as PED — move `engine/options_agents.py` to `archive/retired/2026-05-05-options-agents/` and document in CLAUDE.md retirement section. The ai_players rows can stay (per sacred-data rule) but should get `halt_mode=exit_only` and `halt_reason='HM-T-fleet 2026-05-05 — options_agents.py file orphaned, zero callers'`. **Effort:** 30 min (mirrors yesterday's PED retirement commit).

### `qwen3-14b-pro`
- **Code path:** Referenced in `engine/cost_tracker.py`, `engine/proving_ground.py`, `engine/super_backtest_v4.py`, `engine/crew_scanner.py`, `engine/crew_specialization.py`. Provider-instantiated in `main.py:101`: `OllamaProvider("qwen3-14b-pro", "qwen3:14b", url=OLLIE_URL, timeout=180)`.
- **Why inert:** Provider exists in the providers list but never gets dispatched in any scan loop that writes to `signals`. The `cost_tracker.py`, `proving_ground.py`, and `super_backtest_v4.py` references are diagnostic / lab-only — they don't fire production signals.
- **Recommendation:** **Repair candidate** (not retirement). This is a 14B model that *should* be voting, but isn't. Investigate one level deeper: is it suppressed because it's too slow? Has it been routed to Ollie box but the dispatch-loop ignores Ollie players? Effort: 60 min investigation. **Defer to next session.**

### `red-alert`
- **Code path:** `engine/red_alert.py:281` writes to `red_alert_log`. Scheduler entry: `main.py:2926` `schedule.every(5).minutes.do(run_volume_red_alert)` calls `engine/volume_scanner.py::red_alert_check`. War-room display row at `engine/war_room.py:38,56`.
- **Why inert:** Write path goes to `red_alert_log` (table that **does not exist** in current `data/trader.db` schema) or to `volume_alerts` (the volume_scanner path), neither of which uses `player_id='red-alert'` keying. The roster row is purely cosmetic for war-room icon display.
- **Recommendation:** **Repair OR clarify.** Either (a) add a `player_id='red-alert'` write path so the player row is meaningful, or (b) document in CLAUDE.md that `red-alert` is a display-only roster row (similar to `enterprise-computer`). Effort: 15 min for option (b), 90 min for option (a). **Defer to next session.**

### `dayblade-0dte`
- **Class:** Listed Inert here as a *new* observation (was 🟢 active until 2026-04-07). Per fleet reality 2026-05-03 it's an "empirical pause, not formal halt." 28 days of zero signals as of today.
- **Recommendation:** **Watch list, then halt.** If no signals by 2026-05-15 (40 days idle), treat as PED-class and apply formal halt + retirement archive per CLAUDE.md halt runbook. The Monday market-hours verification protocol (`/tmp/scotty_session_2026-05-03/dayblade_monday_exit_verification.md`) takes precedence — verify position-exit machinery first before halting.

## Halted-but-emitting (HM-A territory)

Per HM-A finding: `is_halted` (now `halt_mode != 'active'`) is a trade-execution gate ONLY, NOT a signal-emission gate. The 4 formally-halted players:

| Player | Halt date | Pre-halt sigs | Post-halt sigs | Post-halt trades | Status |
|---|---|---:|---:|---:|---|
| ollama-llama | 2026-04-25 | 2,670 | **947** | 7 (all clean exits — Verdict A) | 🚨 **Signal leak only** — `halt_mode=exit_only` correctly blocks new entries; signal-emission gap is the architectural concern |
| dayblade-sulu | 2026-03-31 | 2,468 | 196 | 0 | Was signal-leaking; stopped 2026-04-07 |
| gemini-2.5-pro | 2026-04-30 | 2,012 | 0 | 0 | ✅ Clean — paid-model already idle pre-halt |
| grok-3 | 2026-04-25 | 2,453 | 0 | 0 | ✅ Clean — already idle pre-halt |

The 947 ollama-llama post-halt signals already have a fix queued in CLAUDE.md ("Add signal-emission gate ... for fully-retired players: ollama-llama, grok-3, possibly dayblade-0dte").

<!-- HM-T-fleet-correction: 2026-05-05 06:45 MST — original audit (commit 836fd09) reported "2 post-halt trades" as a NEW finding suggesting a possible gate hole. That count was wrong (7-day-window query conflated with lifetime). Follow-up probe corrected to 7 trades, all SELLs of pre-halt positions. Verdict A — exit_only gate enforcing correctly. -->

### ollama-llama post-halt trades — VERDICT A (no bug)

Follow-up probe on 2026-05-05 06:45 MST classified the 7 post-halt trades. All SELLs of pre-halt positions. Position math verified clean:

| Symbol | Pre-halt bought | Pre-halt sold | Net held at halt | Post-halt sold |
|---|---:|---:|---:|---:|
| MSFT | 1.4001 | 0 | **1.4001** | 1.4001 (0.7 + 0.35 + 0.1751 + 0.175) |
| NVDA | 61.8709 | 58.8724 | **2.9985** | 2.9985 (2.2489 + 0.3748 + 0.3748) |

The 7 post-halt SELL chunks: 4 × MSFT + 3 × NVDA, executed across 9 days from 2026-04-27 01:20 MST to 2026-05-03 21:58 MST. Final SELL closed both positions to flat.

**Conclusion:** `halt_mode='exit_only'` is enforcing entries-blocked / exits-allowed correctly at the trade-execution gate (`engine/paper_trader.py:547,1091`). The 947 post-halt SIGNALS remain the real architectural concern (signal-emission gate gap, documented in CLAUDE.md TODOs). Trade gate: working as designed.

The original "2 post-halt trades" claim in commit 836fd09 was a query-window error — `trades_7d` count read as lifetime. Corrected here.

## Orphans

| Player | Source | Detail |
|---|---|---|
| `debate-pipeline` | `signals` table | 1 row from 2026-03-31 (early debate prototype). Player ID never created in `ai_players`. Vestigial. |

No orphans in `trades`. No roster rows missing from any expected join table.

## Recommended actions

### Immediate retirement candidates (apply PED archive pattern)
1. **anderson-bcs, mccoy-bps, quark-ic, covered-call** — orphaned in `engine/options_agents.py`. Archive the file, halt the 4 player rows, document in CLAUDE.md. Mirrors yesterday's PED retirement (commit 0ead8d4). **One bundled commit.**

### Repair candidates (investigation needed before action)
2. **qwen3-14b-pro** — should be voting; investigate dispatch loop. 60-min diagnostic session.
3. **red-alert** — clarify role (display-only) OR wire signal write path. 15-90 min depending on choice.
4. **ollama-llama signal-emission gate** — already in CLAUDE.md TODOs; HM-T-fleet confirms urgency (947 leaked sigs in 6 days post-halt). The trade gate is **clean** (7 post-halt trades verified as exits per Verdict A); only the signal-emission path needs a halt-aware gate. <!-- HM-T-fleet-correction: removed incorrect "+2 leaked trades" claim -->

### Watch list (re-check in 1-2 weeks)
5. **dayblade-0dte** — 28 days idle; halt formally if still silent at 40 days.
6. **energy-arnold** — bridge_votes stopped collecting 2026-05-01 13:01 per CLAUDE.md HM 2026-05-03; signals continue but no trades. Likely related to bridge stall investigation.
7. **mlx-qwen3** + **ollama-coder** — high-emit-no-trade pattern. Investigate why the gate filters these out before downstream dispatch.

### Cosmetic / no-action
8. **enterprise-computer** — by-design (system role for metals + war room display). Document in CLAUDE.md or the Schema doc to prevent re-flagging.
9. **ghost-* (4 agents)** — by-design (ghost-trading trackers). Already understood.
10. **debate-pipeline orphan** — single 2026-03-31 row, vestigial. Optionally back-create the player row OR delete the orphan signal. Lowest priority.
11. **Paid-model wind-down (claude-*, gemini-2.5-*, gpt-*, grok-4)** — these have never been formally halted but are operationally dead per Free Models First doctrine. Should they be `halt_mode=exit_only` for cleanliness? Operational question for the Admiral.

### Per-job dormancy (carry-forward — partial coverage)
- `run_morning_briefing`, `run_opening_range`, `run_battle_station_monitor` — battle-station feeders missing per fleet reality 2026-05-03. Already known.
- `run_signal_scorecard` — `signal_scorecard` table empty since creation per fleet reality 2026-05-03. Already known.
- `run_indicator_bench` — writes to `indicator_benchmarks` (0 rows). PED-class job; investigation deferred.
- `run_holly_nightly_job` — `holly_deepdives` last write 2026-04-21 (14 days idle).
- `run_strategy_lab_auto`, `run_proving_ground_*`, `run_holodeck_weekly` — Sunday-cadence; verify Sunday 2026-05-04 fired correctly before flagging.

## Open questions for the Admiral

1. **Paid-model halting policy.** The 6 paid-model players (claude-*, gemini-2.5-*, gpt-*, grok-4) are operationally dead but never formally halted. Should I apply `halt_mode=exit_only` with `halt_reason='Free Models First wind-down 2026-04-16'` for cleanliness, or leave them as cosmetic-only roster rows?
2. **options_agents retirement scope.** Yesterday's PED retirement archived a single agent file. options_agents.py contains 9 strategy classes (BCS, BPS, CC, IC, plus 5 more). Should I archive the entire file in one commit, or split per-strategy?
3. **dayblade-0dte timeline.** 28 days idle. Halt now (formal) or wait until 40 days (per the watch-list rule)?
4. **mlx-qwen3 / ollama-coder dispatch suppression.** These emit thousands of signals but produce zero trades. Is the gate intentionally filtering them out (correct behavior, no action) or is there a wiring gap (repair needed)?

## Method notes

- Activity numbers from live `data/trader.db` snapshot at 2026-05-05 06:24 MST.
- Lifetime / 7d windows computed from `signals.created_at` and `trades.executed_at`.
- "By-design" classifications cross-referenced against `engine/paper_trader.py` exempt lists, `engine/war_room.py` display registry, and the ghost-trading flow described in CLAUDE.md.
- No code changes, no schema changes, no halts applied this session. Document only.
