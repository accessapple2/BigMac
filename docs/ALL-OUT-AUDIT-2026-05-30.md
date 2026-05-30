# ALL-OUT AUDIT — 2026-05-30 (USS TradeMinds)

**Supersedes + extends `FULL-AUDIT-2026-05-30.md` (cf9ef2c).** That one was ticket+config+DB+git+grep. This one
went WIDER: 6 parallel read-only sweeps (code-markers / models / roster / data / docs-git / daemons-logs) to find
what is in NO ticket, NO doc, and what we've been **assuming-true without verifying**. Read-only. Nothing fixed.

Prime directive honored: the ticket list was treated as INCOMPLETE. The prizes below (§1 orphan markers, §4 sibling
pollution, §6 assumptions, and the daemon graveyard) are the stuff that was on **no** list.

═══════════════════════════════════════════════════════════════════════════════════════════
## 🚨 SURPRISES — read these first (things that surprised the auditors)
═══════════════════════════════════════════════════════════════════════════════════════════
1. **🔴🔴 7-DAY SILENT DAEMON GRAVEYARD.** The May-23 reboot re-bootstrapped only 4 services via cron `@reboot`.
   **~10 launchd jobs never came back and have been DEAD for 7 days** — including **`com.trademinds.watchdog`
   (the safety net), `com.trademinds.healthcheck`, and `com.ollietrades.morningbriefing`** (no standalone morning
   brief / intel report since 05-23). **The monitor that should have caught this is itself one of the corpses.**
   In NO ticket. *(Plus 7 more that were already dead BEFORE the reboot — scanner since 04-11, uhura 04-17, etc.)*
2. **🔴 The hard execution SAFETY-GATE doc is a LIE.** `strategies/executor.py` header says "_EXECUTION_ENABLED
   hardcoded to False … gated until Task 7" — actual constant (line 22) was flipped to **`True`** ~3.5 weeks ago
   (siblings bull_call/bear_put/bull_spread too). The most safety-critical comment in the codebase contradicts its code.
3. **🔴 Two P0 data-integrity bugs are FIXED in code but MEMORY.md still flags them OPEN** ("invalidates all PnL
   analytics until fixed"). `price-writeback` (43f65de+ef1cc9e) and `alpaca-provenance` (71e523d) shipped. A
   session resuming from memory would re-investigate a solved bug — the exact verify-before-fix failure mode.
4. **🔴 `prompt_version` + `strategy_id` are SILENTLY DROPPED at the trades INSERT.** Code passes them on every
   live trade (ai_brain/paper_trader/crew_scanner/dayblade), yet **100% of rows — including the newest — are NULL.**
   The learning-loop columns are non-functional. *(Direct risk to the just-shipped save_signal hook — see §6.)*
5. **🔴 dalio pollution is 18 rows, not 2 — and it's AAPL, not ONDS.** The −255.08 residual is dominated by
   **AAPL id 1372 (−229.48)** + ONDS id 2545 (−91.05), offset by DELL/PLTR gains. The "ONDS-sibling" hypothesis
   was wrong. And `known_contaminated` is **INVERTED**: set on the 1 already-fixed row (2539), on NONE of the 18.
6. **🔴 `holly-scanner` is a roster GHOST.** Wired into RULES_SCANNERS + rules-dispatch, iterated every scan cycle,
   yet has **zero `ai_players` rows** — no halt control, no WR-vote identity. It slips the entire governance layer.
7. **🟠 navigator + ollama-plutus run models in NEITHER config NOR DB.** navigator fires gemma4:26b (154×) alongside
   qwen3:8b; McCoy (#1 Sharpe agent) runs ministral-3:3b on **36% of its calls** instead of 0xroyce/plutus. The
   "read the DB not config" doctrine is necessary but **insufficient — the DB doesn't capture runtime reality either.**
8. **🟠 `trades_clean` view has ZERO readers.** The contaminated-flag "deprecation" redefined a view nothing queries
   → the migration was cosmetic; no downstream PnL path actually changed.
9. **🟠 Two parallel signal systems both live & fresh:** legacy `signals` (67.9K rows) + `signals_v2` (5.3K) — an
   incomplete migration nobody closed. signals_v2 pending = **3,076 and CLIMBING** (QUEUE_AUDIT's "draining to 815" is 4× stale).
10. **🟠 `mlx-qwen3` is `halt_mode='full'` but RAN 3× today** (one call 112s) — full-halt agents are supposed to be skipped.
11. **🟠 `[HM-EQ] snapshot failed:` for the ENTIRE ~20-agent fleet, 3×/run, empty reason string** — swallowed exception, no ticket.
12. **🟢 The record is mostly honest.** FULL-AUDIT-2026-05-30 held every spot-check; CLAUDE.md every fact held (only 2 `:line` anchors drifted). One phantom SHA (B26 `9ee1c5c` doesn't exist).

**SURPRISES count: 12** (8 red/orange untracked, load-bearing).

═══════════════════════════════════════════════════════════════════════════════════════════
## ⬛ ONE-SCREEN EXEC SUMMARY
═══════════════════════════════════════════════════════════════════════════════════════════
**Total open (this audit):** ~46 distinct items — **~26 NEW orphans** not on any prior list (14 code-markers, 10
daemon-deaths, the dalio-18, the dual-signal/dropped-column data bugs) + ~20 carried from FULL-AUDIT.
**Ship-today (off-market): ~14** · **Genuinely market-locked: ~8** · **Human-gated: ~5** (incl. launchd re-bootstrap, browser smoke, Admiral execution-gate confirm).
**Repeat-offender count:** all 5 known classes re-confirmed + the daemon-graveyard is a NEW 6th class candidate
(*reboot-survival-gap*: in-process daemons survive via cron @reboot, standalone launchd jobs silently don't).
**Highest-risk UNVERIFIED assumption:** *"no alarm == healthy."* **FALSIFIED** — the watchdog/healthcheck that raise
alarms have been dead 7 days; orphan-safety + uptime-monitoring now rest entirely on manual restart-script runs.
**The one thing to do first:** re-bootstrap (or cron-rehome) the dead launchd safety jobs — the system has had no
automated watchdog/healthcheck/morning-intel for a week and didn't know it.

═══════════════════════════════════════════════════════════════════════════════════════════
## §1 — EVERY OUTSTANDING ITEM (ticketed + ORPHAN)
═══════════════════════════════════════════════════════════════════════════════════════════
Legend: BR=blast-radius. ⟳=repeat-offender-class. ⭐=ORPHAN (in no ticket). Trig=falsifiable trigger.

### ⭐ ORPHAN code-markers (the no-stone-unturned prize)
| source | item | BR | concrete blocker | falsifiable trigger | ⟳ |
|---|---|---|---|---|---|
| executor.py:4-22 | safety-gate docstring says False, code=True | 🔴 money | needs Admiral: is exec-enabled intended? | comment matches constant | config-drift |
| executor.py:308 | iron-condor close known-broken (HM-AC-extension, no ticket) | 🔴 money | atomic MLEG close unimpl | 4-leg IC closes all legs atomically | — |
| exit_manager.py:195 | TODO Task-7b real-time polygon chain (5+ wks) | 🟠 | Polygon chain not wired | live chain lookup on exit | — |
| setup_classifier.py:10 | permanent stub returns 'neutral' (5+ wks) | 🟠 | Task-6+ unbuilt | classifier returns non-neutral | — |
| bull_spread_v1.py:139 | width ladder hardcoded for ~$700 SPY | 🟠 | no ticket | widths scale by price | hardcoded |
| portfolios/manager.py:450,467 | not wired to alpaca_bridge / webull (8+ wks, oldest rot) | 🟠 | wiring unbuilt | manager reads live broker | — |
| dashboard/app.py:21411 + index.html:29840 | auth half-done: service-token+recovery-hash TODO, admin-token header not sent | 🔴 sec | Phase-1 secret-gen incomplete | admin calls carry token | — |
| options_selector.py:62 | _BSM_RISK_FREE=0.045 frozen | 🟢 | Polygon inactive | rate from config/live | hardcoded |
| options_exec.py:5 | whole module, ZERO callers (5+ wks) | 🟢 | no agent wired | a caller exists | dead-code |
| tick_recorder.py:24 | unbounded tick record, no tier-sampling (C1.x) | 🟢 | C1.x unbuilt | tier-aware sampling on | — |
| universe_refresh.py:332 | ADRC/PFD asset classes silently skipped | 🟢 | "for now" | classes included or doc'd | — |
| stops.py:67 | boundary 0.80/0.90 copy-paste "revisit", no owner | 🟢 | — | owner assigned | ⟳ tier-boundary |

### Carried (from FULL-AUDIT, status unchanged) — §C floor (market-locked), Phase-2 fetch (market-locked),
dalio aggregator (go-gated), contaminated-flag migrate (go-gated, **now known cosmetic — see §4**), 6 standing items
(re-verify first), WAVE 7 (human-gated), signals_v2 sweep (post-floor), LOW hygiene items. *(Full table in cf9ef2c §1-6.)*

### ⭐ ORPHAN operational items (from daemon sweep — §details in §-Daemons below)
launchd graveyard (10 dead), FD-leak `Too many open files` (421×), finviz dep missing (297×), `[HM-EQ]` fleet
snapshot fail, `schwab_cadence_check` broken (`no such table: schwab_holdings`), iShares/Aladdin 0-holdings, Ollama
`phi3:3b` not-found @192.168.1.168 (849×). **None ticketed.**

═══════════════════════════════════════════════════════════════════════════════════════════
## §2 — MODELS (config vs DB vs RUNTIME, all agents)
═══════════════════════════════════════════════════════════════════════════════════════════
**Doctrine confirmed + sharpened:** DB `model_id` is canonical for *intended* model and matches runtime for ~13 clean
agents — BUT runtime caught **2 agents running models absent from BOTH config AND DB**, so even the DB isn't ground truth.

**All 10 documented drifts VERIFIED** (plutus→0xroyce/plutus, qwen3→ministral-3:3b, llama→qwen3:8b, local→gemma3:4b,
gemma27b→ministral-3:3b, kimi→ministral-3:3b [runtime proves Kimi never hits cloud], grok3/4o/o3→qwen3:8b,
**neo-matrix→`8000 / Independent` garbage placeholder**).

**NEW (uncaught by the doctrine):**
- 🔴 **navigator runs gemma4:26b (154×)** beside qwen3:8b — leftover from the navigator bakeoff shadow bleeding into
  the live id; a 26B model on a 16GB box forces VRAM swaps (latency tax).
- 🔴 **ollama-plutus (McCoy) runs ministral-3:3b on 36% of calls** — the fleet's best agent silently runs a generic
  3B brain a third of the time instead of finance-trained Plutus.
- 🟠 **mlx-qwen3 (`halt_mode=full`) ran 3×** via a warmup path — halt-skip leak.
- 🟠 **cto-grok42** runs devstral-small-2 at wall=0.00s on all 77 calls (no-op/fast-fail) — "active" is illusory;
  triple-source mismatch (config-comment + main.py:262 both say qwen2.5-coder; DB+runtime say devstral).

**ZOMBIE assignments (assigned, never observed running):** ~35 rows — 15 full-halted aliases (qwen3-8b-4o/o3,
qwen3-14b-grok3, qwen-coder-haiku, gemma27b, claude-haiku/sonnet, gpt-4o/o3, grok-3/4, gemini-2.5-pro/flash, chekov,
dayblade-sulu) + **20 `navigator_bm_*`/`navigator_bn1_*` bakeoff shadows** holding the most exotic model_ids in the DB
(gemma4:31b, qwen3.6:35b-a3b) that exist nowhere in runtime. **Garbage-placeholder model_ids:** neo-matrix, plus
non-model sentinels (red-alert=system, ollie-auto=ollie, alpaca-mirror=sync, trade-desk/webull=human, super-agent=crewai).

═══════════════════════════════════════════════════════════════════════════════════════════
## §3 — AGENTS / ROSTER (full fleet, every flag)
═══════════════════════════════════════════════════════════════════════════════════════════
71 `ai_players` rows. **Arena LLM set == {McCoy (ollama-plutus), Dax (ollama-qwen3)} — VERIFIED against current code**
(skip-set at ai_brain.py:610-613 lists exactly the 6: deepseek-7b-grok4, ollama-coder, cto-grok42, ollama-deepseek,
ollama-kimi, qwen3-8b-sonnet). The 6-removal claim holds statically; live confirmation is market-gated (§6).

**Drift flags:**
- ⭐ **holly-scanner** — TRUE ORPHAN (in RULES_SCANNERS + dispatch, no `ai_players` row). `data-tng` = benign alias-orphan.
- **benched-in-scan-list:** dayblade-sulu (exit_only+paused) at `_SCAN_TIER1` PRIORITY-1; super-agent (full+paused)
  in T1; mlx-qwen3 (full) in T1; dayblade-0dte (full+paused) in RULES_SCANNERS. **`_SCAN_TIER3` is 70% dead** (7/10 full-halt).
- **active-but-silent (dead `signals` emitter, trade path still fires):** navigator (last signal 04-14), dalio-metals
  (03-31) — the exact §C "re-homed to trade-only, old emitter dropped" pattern, still drifting. Also energy-arnold,
  options-sosnoff, ollama-deepseek/kimi, qwen3-8b-sonnet (never traded), cto-grok42 — all active-flag but inert (WR-vote-only).
- **inert "active" zombies:** enterprise-computer, quark-ic, qwen3-14b-pro (active flag, never signal/trade).
- **zero-yield rules agent:** ollama-coder (Data) emits signals but **last_trade = never** — 0 realized output post-skip-set.
- Redundant-path: deepseek/coder resolved by skip-set ✓. McCoy/Dax keep rules+arena by design (the floor).

═══════════════════════════════════════════════════════════════════════════════════════════
## §4 — DATA INTEGRITY (every known + hunted unknowns)
═══════════════════════════════════════════════════════════════════════════════════════════
**DBs:** data/trader.db (460MB, 31MB VACUUM-reclaimable, 45 empty tables), autonomous_trader.db (3.7MB, LIVE — read
by healthcheck/eod_scorecard, **separate** from trader.db), swingdesk.db, alpha_signals.db, ghost_trades.db,
proving_ground.db, backtest.db (76 versioned tables, heavy v2-v5 sprawl). **Stray 0-byte `trader.db` at repo root** +
empty backtest_results.db / deep_scan.db — a `sqlite3 trader.db` from wrong CWD silently hits an empty DB.

**DALIO −255.08 = 18 polluted rows** (full list in daemon-agent output): AAPL 1372 −229.48 (90%), ONDS 2545 −91.05,
GOOGL/QQQ small, offset by +DELL/+PLTR. id 2539 already fixed. **→ confirms the tracking-aware aggregator is the
only sane fix** (18-row whack-a-mole is absurd; AAPL/ONDS aren't even "metals").

**`known_contaminated` = DEAD + INVERTED:** zero code writers; 236 rows flagged (ollie-auto 190, super-agent 16,
neo-matrix 29, dalio 1) — mapping arbitrary vs real pollution. Flagged the fixed row, none of the 18 wrong ones.

**`trades_clean` view = ORPHAN:** correct definition (118 rows, alpaca-bounded) but **ZERO readers in code.** The
contaminated-flag "deprecation" was cosmetic — no PnL path consumes the view. *(The real migration still TODO.)*

**Dropped-at-INSERT columns:** `trades.prompt_version` + `trades.strategy_id` = 100% NULL incl. newest 100 rows,
despite 4 call-sites passing them → the live INSERT path drops them. `signal_id` 99.9% NULL, `corrected_pnl` 89% NULL.

**Dual signal systems:** signals 67.9K + signals_v2 5.3K, both fresh, both read — incomplete migration. signals_v2
pending 3,076 (climbing), 79% non-actionable. **positions.conviction 58% NULL** (matches ~57%; backfill unfinished).

**Orphan/ghost tables (zero readers):** trades_archived(8), positions_archived(3), trades_archive_HM_COVERED_CALL_RECORDING(5),
positions_archive_hmble(1), kill_switch_events_legacy(1), portfolio_history_archived(0). **Dead column:** known_contaminated.

═══════════════════════════════════════════════════════════════════════════════════════════
## §5 — CORRECTIONS TO THE RECORD
═══════════════════════════════════════════════════════════════════════════════════════════
**MEMORY.md (highest-value):** all 111 pointer files exist, but **2 stale CLAIMS** — `project_hm_trades_price_writeback_bug`
("until fixed", 153% overstated) and `project_hm_trades_alpaca_provenance` ("100% mislabeled") **are SHIPPED**
(43f65de, ef1cc9e, 71e523d). `bulk_prices_fixture_bug` also stale-ish (9f915b1 shipped). **RECOMMEND: flip these to SHIPPED.**

**SHIPPED-BUT-UNTRACKED commits (no backlog entry):** 43f65de (price-writeback P0), ef1cc9e (avg-price P0), a0ac71e
(auth-phase1), 5a76d63 (trade-desk-autopilot), 71e523d (provenance P0), d3fd3d5 (fleet-wire, "direct to main no PR"),
1ed02d3/2cfdb2a (ghost-scorecard/post-exit), 9f915b1 (bulk-prices-fix). The P0 fixes live only in memory memos.

**CLAIMED-BUT-UNBACKED:** B26 closure SHA **`9ee1c5c` DOES NOT EXIST** (phantom; 1 bad of ~26 checked). Conviction-stop
SHAs (9b55466, 263c8dd, ecc86b1) all verified real.

**Status-drift:** signals_v2 815-"draining" (QUEUE_AUDIT) vs 3,076-climbing (live) — thesis now false. HM-DATA-INTEGRITY-FORENSICS
CLOSED (XO_BACKLOG) vs verify-for-closure (QUEUE_AUDIT). Duplicate open conviction-stop-WIRE entry at XO_BACKLOG:1857 = queue-rot.

**CLAUDE.md:** facts all held; only `:line` drift — uvicorn bind `main.py:2944→3003`, Riker `4226→4311`.

═══════════════════════════════════════════════════════════════════════════════════════════
## §6 — UNVERIFIED-ASSUMPTIONS LEDGER (the real no-stone-unturned)
═══════════════════════════════════════════════════════════════════════════════════════════
| # | We ASSUME | Why we believe it | What CONFIRMS it | When |
|---|---|---|---|---|
| A1 | arena == {McCoy, Dax} | skip-set static-verified (ai_brain.py:610) | live scan shows only plutus/qwen3 `:infer` | **Monday open** |
| A2 | save_signal hook persists rules signals | loaded, 0 errors, signals-table is live-written | a rules BUY writes a `sources='rules'` row — **AND check it's not dropped like prompt_version (§4)** | **Monday open** |
| A3 | 4 skip-set agents absent from arena | code loaded | observed absent on a live scan | **Monday open** |
| A4 | spike-fixes (indicators/catalyst/fetch) still hold | confirmed Friday; assumed since | a full scan completes <budget this cycle; bounded-fetch logs fire | **Monday open** (weekend = no scan) |
| A5 | single-writer / orphan-safety holds | lsof=1 now (PID 61501) | **FALSIFIED-ADJACENT: the watchdog that would catch a mid-session orphan is DEAD 7d** → safety = manual restart-script only | continuous gap until watchdog restored |
| A6 | Phase-2 leaves are the only unbounded fetches | grep was `engine/` only | **REFUTED:** orphan markers found unbounded/failing fetches in `strategies/`, `portfolios/`, + live log failures (iShares, Yahoo 429, finviz) | now (refuted) |
| A7 | contaminated-flag deprecation cleaned PnL | view redefined | **REFUTED:** trades_clean has 0 readers — migration cosmetic | now (refuted) |
| A8 | dalio is a 2-row data blip | row 2539 + "ONDS sibling" | **REFUTED:** 18 rows, AAPL −229 dominant | now (refuted) |
| A9 | morning brief / intel ran today | scheduled 06:00 | **launchd path DEAD**; in-process main.py:3535 path unconfirmed — verify morning_brief.json mtime | today |
| A10 | "no alarm == healthy" (meta) | quiet logs | **FALSIFIED:** watchdog+healthcheck dead 7d; quiet == blind | restore monitors |

═══════════════════════════════════════════════════════════════════════════════════════════
## §7 — SHIP-TODAY vs MARKET-LOCKED vs HUMAN-GATED
═══════════════════════════════════════════════════════════════════════════════════════════
**🟢 SHIP-TODAY (off-market, no live scan needed):**
- Correct the 2 stale P0 memory memos → SHIPPED (memory hygiene; recommend now).
- Re-home the dead launchd jobs into cron `@reboot` (or document the gui-bootstrap step) — diagnosis done, fix is config.
- `pip install finvizfinance` (297× silent degrade). · Remove 2 stale one-shot HM-TIER3 cron lines (05-28).
- Fix CLAUDE.md `:line` anchors (2944→3003, 4226→4311). · Fix executor.py safety-gate **docstring** to match code (comment only).
- Build the `prompt_version`/`strategy_id` INSERT-drop fix (single-file, off-scan-path, YELLOW: build+diff+commit).
- VACUUM trader.db (31MB reclaim) — low-risk maintenance window. · Delete stray 0-byte root `trader.db`/orphan empties (archive-rename per sacred-db).

**⛔ GENUINELY MARKET-LOCKED (Monday+, each has a hard physical reason):**
- §C floor build (needs live per-symbol cost to size N). · Phase-2 fetch batch + new fetch failures (needs market-hours load to confirm).
- save_signal firing (A2), arena absence (A1/A3), spike-fix hold (A4) — all need a live scan.
- nightly rs_rank/minervini confirm (needs a market-day run). · `[HM-EQ]` fleet-snapshot fail — may be market-gated; investigate on a market-day.
- signals_v2 sweep (post-floor, don't drain a filling pond).

**🧍 HUMAN-GATED (name the action):**
- **launchctl re-bootstrap** of dead LaunchAgents — needs Captain's logged-in GUI session (gui/501 unreachable from SSH). *(Or accept the cron-rehome above.)*
- WAVE 7 frontend — browser smoke (HM-BJ.E2). · Admiral go: is `_EXECUTION_ENABLED=True` intended? · Admiral: save_signal scope (acted-on vs every-eval).
- Confirm execution-gate flip (SURPRISE #2) was deliberate before trusting the live-trade path.

---
*Method: 6 parallel read-only subagents (code-markers, model-drift, roster, data-integrity, docs-git, daemons-logs),
findings reconciled here. No file/DB/service modified. Carries forward cf9ef2c; does not replace its ticket detail.*
