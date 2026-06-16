# CLAUDE.md Archive — May 2026

Historical content moved out of `CLAUDE.md` on 2026-05-27 per HM-CLAUDE-MD-TRIM
(power-paste ITEM 1). Sections here are completed/shipped narratives, sprint
logs, and one-time events. Active doctrine remains in `CLAUDE.md`. Doctrine
rules that emerged from these sessions were extracted and kept in the main file
(Logging sink split, Console init verification, Diagnostics-first, etc.).

---

## Pending TODOs (carry-forward audit)

Original "Pending TODOs" block from CLAUDE.md as of 2026-05-27. Status notes
added during the trim.

- Polygon Stocks + Options Starter ($29/mo each) ACTIVE since 2026-05-12. (Now
  in main CLAUDE.md under Free Models First.) Note 2026-05-27: Polygon Starter
  does NOT include WS trades — see `drafts/HM-LESSON-VERIFY-DATA-SOURCE-FIRST.md`.
- Build Elder Council agents (Sarek 5yr, Janeway 10yr, Surak 20yr) — stub
  strategy modules + DCA paper-trade logic. **STILL OPEN.**
- Build Metals Command quadrant agents (Scotty news, O'Brien recommendations);
  upgrade `section-metals` to 4-quadrant grid with spot + ETF tracking.
  **STILL OPEN.**
- Rename dashboard `section-webull` label → "Starfleet" (keep internal id to
  avoid 50+ ref breakage). **STILL OPEN.**
- Ghost-trading experiments for Bench 4:
  - Uhura-EDGAR: 60-day ghost run, promote to Active if Sharpe > Capitol's
  - Aladdin: wire iShares ETF flow → paper-trade sector rotation signals
  - Spock-R1: SUPERSEDED by HM-CN 2026-05-17. Spock specialty bakeoff in
    HM-BN.2 wave to revalidate (deepseek-r1:7b? deepseek-r1:14b? phi4:14b?
    hold qwen3:8b?).
  - Picard: convert weekly briefing into Ollie regime-table modifier.
- Chekov rehab: extract S5 version (`git show 859a4f0:engine/chekov_autotrade.py`),
  ghost-trade S5 vs current for 30 days.
- Candidate C (2022 bear) OOS backtest — may be done; verify.
- Research TradingAgents / FinMem integration (FinGPT sentiment blended in
  S6.3 per commit 7ebabb6).

### Pending TODOs additions from 2026-05-03 reconciliation
- Backtest 8 orphaned options strategies in `engine/options_agents.py` (zero
  `main.py` refs).
- Wire `signal_scorecard` writer OR remove from Alpha Engine plan (still 0 rows
  as of 2026-05-10).
- Add signal-emission gate (in addition to execution gate) for fully-retired
  players: ollama-llama, grok-3, possibly dayblade-0dte.
- Reconcile DB `ai_players.model_id` ↔ `config.py` model drift (~25 rows;
  cosmetic).
- Retire legacy convergence scanner (`engine/strategies.py` scan_strategies
  path). Convergence-write path silently dead since 2026-04-07.

---

## Recently Shipped (S6.3, no longer pending)

- Lt. Uhura SEC EDGAR agent (commits ad9d832, 4c78d04) — institutional veto
  wired into trade gateway
- Covered_call P&L bug fix (commit 14689a7) — denominator corrected to position
  notional
- CAUTIOUS rsi_bounce disabled (commit e799d07)
- Plutus-3B upgrade for McCoy (commit 3721c33)
- FinGPT news sentiment blended into alpha signals (commit 7ebabb6)

---

## 2026-04-23 Drydock Part 2 — Major Overhaul

24 fixes applied in one session. Gate held `_EXECUTION_ENABLED=False` throughout.
Alpaca paper ~$79,716 equity. No live exposure.

### Highlights
- **5 rogue qwen3:14b callers routed to Ollie Box**: neo-matrix (config.py),
  engine/premarket_scanner.py, engine/research_caller.py, crew/agents.py
  (5 CrewAI roles → ChatOllama with OLLIE_URL), engine/chart_analyzer.py
- **dashboard/app.py 3-path Gemini fallback → Ollie Box** (lines 4969/5025/5348):
  google-provider players now fall back to OLLIE_URL, not OLLAMA_URL (bigmac)
- **5 Elder Council + Swing Desk agents** (surak, janeway, kirk, pike, sarek):
  `_OLLAMA_URL` fallback changed `localhost:11434` → `192.168.1.166:11434` (Ollie)
- **2 zombie players deactivated** via `is_active=0` in ai_players table:
  `ollama-llama` (deepseek-r1:14b, 9.7GB) and `grok-3` (qwen3:14b). Sacred
  trade/signal/cost data (145 trades, 5025 signals, 2924 cost rows) preserved.
- **Spock dedup TOCTOU race fixed**: `engine/crew_scanner.py::_save_spock_alert`
  rewritten as atomic `INSERT ... WHERE NOT EXISTS`.
- **Webull sync disabled** (account liquidated), webull positions zeroed
- **Costs page `[object Object]` fixed**: `dashboard/static/index.html:10976`
  now reads `fvp.free.total_pnl` / `fvp.paid.total_pnl` — renders correctly

### RAM Result
94% peak → 45% mid-session → 1.8GB active healthy after all fixes landed.
Root cause: multiple sprint-era agent paths all defaulted to localhost:11434.

### Architecture Note
Each sprint added agent paths that defaulted to localhost:11434. Permanent fix:
single config-level OLLAMA_URL default applied at process start (future sprint).

### Open Items (subsequently resolved or carried forward)
- `/api/wheel/status` intermittent 500 (`dashboard/app.py:7592`)
- iv_history day 3 verification
- Chrome extension final cleanup
- Alert ACK hygiene
- Ghost scorecard calibration via `/api/signals/scorecard`
- Alpha threshold tuning for bull_spread_v1

---

## 2026-04-25 — Saturday Drydock — 14 fixes

Gate held `_EXECUTION_ENABLED=False` throughout. Alpaca paper account untouched.

### Category 1 — Kill switch architecture (3 fixes)
- **is_halted gate in `buy()`** (`paper_trader.py:~542`): SQL check before every buy
- **is_halted gate in `sell()`** (`paper_trader.py:~1083`): same pattern
- **Zombies halted in DB**: `ollama-llama` + `grok-3` → `is_halted=1`, `halt_reason` set
- Audit finding: `is_active`, `is_paused`, and `crew_role` are all decorative.
  `is_halted` is the only working per-player kill switch.

### Category 2 — Autopilot $0 P&L bleed (2 fixes)
- **Layer 1** (`autopilot.py:126`): skip RSI trim when `current_price=0`
- **Layer 2** (`main.py:~1095`): widen `prices` dict from `WATCH_STOCKS` only →
  all symbols in open positions

### Category 3 — Latent fallback bombs defused (2 fixes)
- **`_try_db_fallback` option filter** (`market_data.py:~487`): added
  `AND asset_type='stock'`
- **BUY_CALL fail-closed** (`paper_trader.py:~1364`): `buy_price` init changed
  from `price` (stock price!) to `None`; guard added

### Category 4 — Operational cleanup (7 fixes)
- Capitol Trades dedup (`crew_scanner.py:~2569`)
- NTFY topic renames (4 files)
- "Riker Fleet Alert" → "Fleet Alert"
- ntfy tags "trademinds" → "ollietrades" (6 occurrences)
- BSM ceiling (`options_selector.py`)
- Earnings blackout (`options_selector.py`)

### RAM Result
Bigmac Ollama clean throughout — nothing loaded at EOD. All heavy models on Ollie Box.

### Open Items (carry forward, since resolved or migrated)
- `/api/wheel/status` intermittent 500 — low priority
- 5 Neo consecutive-loss alerts — already `acknowledged=1`
- iv_history Day 4/5 verification
- Ghost scorecard calibration

---

## 2026-05-03 — Fleet Reality Reconciliation

Read-only audit of every fleet member against running code, DB state, scheduler
entries, and signal volumes. Source: `/tmp/scotty_session_2026-05-03/fleet_reality_2026-05-03.md`.

### State Flag Semantics (extension to 2026-04-25 finding)
- `is_halted` is a trade-execution gate ONLY. **Does NOT gate signal emission.**
  Verified ollama-llama emitted 947 signals between halt date 2026-04-25 and
  2026-05-02. To fully retire: remove scheduler entry OR add `is_halted` check
  in the signal-emission path.
- `is_active`, `is_paused`, `crew_role` confirmed still decorative.

### DayBlade reality
- `dayblade-0dte` (T'Pol/plutus): `is_halted=0`, `is_active=1` — scheduler
  ACTIVE (`main.py:2554` every 5 min). No signals since 2026-04-07 (26 days
  idle as of audit). Has weekend-standby gate.
- `dayblade-sulu`: `is_halted=1` since 2026-03-31 (R:R 0.10 dormancy).

### Battle Station — dormant via missing inputs
- `main.py` schedulers ACTIVE: `run_battle_station_monitor` every 2 min,
  `run_morning_briefing` daily 06:00, `run_opening_range` daily 06:45.
- launchd feeders that supplied data are absent. Feeders never re-added after
  Phase 2 cleanup.

### signal_scorecard — silent writer (16-col schema, 0 rows)
- Schema present, empty since creation. Writer never wired.
- Blocks ghost scorecard calibration (B5 in carry-forward TODOs).

### mlx-qwen3 — fully active
- `is_active=1`, `is_halted=0`. 965 signals over last 30d.
- `ai_players.model_id` shows `phi3:mini`, but `config.py:128` defines mlx
  with `mlx-community/Qwen3-8B-4bit`. Drift non-blocking (runtime reads config).

### energy-arnold — high-volume noise generator
- Model: `qwen3:8b` via Ollama. `is_active=1, is_halted=0`. 9,632 total signals.
- Confidence distribution bimodal: AVG 0.258, 6,643 at conf=0.0 (69%), 1,209 at
  conf=1.0 (13%) — mostly noise + occasional over-confident outputs.
- bridge_voter wiring: 248 votes total in `bridge_votes` table.
- IMPROVE decision pending parser investigation. Not retired this round.

---

## Lessons (2026-05-05 Day 2) — full narratives

(Doctrine summaries extracted to main CLAUDE.md under "Logging sink split" and
"Doctrine Lessons". Full narratives retained here for context.)

### Logging sink split — full discovery narrative
Learned the hard way during the 2026-05-05 reconciliation drift investigation
(commit `ec81add`) when the investigation initially classified Thread D
("HM-V NTFY didn't fire") as a real bug before discovering 5 NTFY events were
happily landing in `trader_error.log` at HTTP 200.

### Alert rate-limit semantics — in-memory only
`engine.alert_channels._rate_state` is in-memory dict scoped to the running
process. NOT persisted to `settings` table.

Consequence: `rate_limit_secs=86400` means "first occurrence per error class
per process lifetime", not "per 24h wall-clock." Service restarts reset.

On heavy-restart days, each unique `alert_type` can fire up to N times where
N = restart count. 2026-05-05 (Day 2 of gate-flip soak) had 4 restarts; HM-U
submit-error alert classes fired up to 4× instead of intended 1×.

For daily-cadence canary alerts (Item 5 reconciliation drift detection, fires
once per day at 13:30), fine — canary fires once and in-memory state holds.

Future option: persist `_rate_state` to `settings` table to make rate-limiting
survive restarts. Documented here so actual semantics are clear.

---

## Lessons Banked 2026-05-13 (morning)

**logger.info vs console.log**: Rich Console writes to `logs/trader.log`;
stdlib `logger.info()` writes to `logs/trader_error.log`. Two sinks SEPARATE
despite "trader_error" misleading-name. Any new HM-* instrumentation must use
`console.log()` for trader.log routing, or scripts grepping must explicitly
target trader_error.log. Surfaced when HM-CD-instr + HM-AN2 lines were
invisible (cron scripts grepped wrong file). Fixed by HM-LOG-CHANNEL commit
8d7a607.

**HM-CD-migrate doctrine — Ollama keep_alive**: Universal `keep_alive: "30s"`
(legacy 16GB constraint) forces full model reload on every call —
ollama-coder measured 207s wall, 90%+ in model swap. Fixed by per-model
`_HM_CD_KEEP_ALIVE` lookup: high-frequency models (qwen3:8b=7 agents,
qwen2.5-coder:7b=2 agents) get 30m residency; alpha-squad rotation gets 10m;
rare models keep 30s default. Originally banked as "Ollie Box 32GB has
headroom for 2-3 concurrent 7B-class models" — CORRECTED 2026-05-13: actual
hardware is RTX 5060 with 8GB VRAM. qwen2.5-coder:7b alone uses 4.59GB
resident; realistic budget is ONE 7B model fully resident at a time, possibly
a second with partial offload. Doctrine logic (per-model keep_alive lookup)
remains correct; the VRAM-headroom premise was wrong. Commit 999984a (logic).

**Diagnostics first**: HM-CD-migrate ALMOST became a Polygon data migration
based on stale assumptions. Real cause (model swap) emerged only when
ollama-coder logs were read in context. HM-CD-instr instrumentation
(logger.info → trader_error.log) was the savior. Reinforces XO rule:
diagnostics first, theorize second.

---

## Lessons Banked 2026-05-13 (afternoon)

**HM-WATCHDOG sweep complete**: All HM-* anchored instrumentation across the
codebase now uses `console.log()` consistently. Files normalized:
engine/crew_scanner.py (7 sites, HM-LOG-CHANNEL commit 8d7a607), main.py
(1 site, HM-WATCHDOG commit 5d6ce29), engine/battle_station.py (3 HM-AF
sites, HM-WATCHDOG-2). When adding new HM-* observability lines, default to
`console.log()` not `logger.info()`.

**HM-AN2 BLOCKED enhancement**: BLOCKED log line now pulls
`paper_trader._last_rejection.get(player_id)` inline so morning_an2_observation
cron output is self-contained.

**HM-BP-FOLLOW-UP parked**: 5+ gemini-2.5-pro trades from 2026-03-12 have
entry_price corruption (entry ~$10, exit ~$210 for mega-caps). Reject filter
handles compute; data cleanup deferred.

**Diagnostics-first reinforcement**: Today's morning ship arc proved this
discipline twice in production. Both HM-CD-migrate and HM-BP would have
shipped wrong fixes without the discovery phase. RULE: never modify production
code paths before reading current behavior via grep + sqlite + log inspection.

---

## Lessons Banked 2026-05-13 (evening — HM-CONSOLE-INIT)

`logger.* → console.log` flips need module-level `Console()` init verified
before commit. This morning's automated sweep (HM-LOG-CHANNEL `8d7a607`,
HM-WATCHDOG `5d6ce29`, HM-WATCHDOG-2 `10eb1e7`) flipped `logger.info/warning`
calls to `console.log` in `crew_scanner.py` and `battle_station.py` without
verifying these modules imported Rich Console. They didn't. Every flipped call
raised `NameError: name 'console' is not defined` at runtime.

**The bug ran silently for ~12 hours** because `run_scan_cycle` wraps the body
in `try/except` that logs "Scan cycle crashed (will retry next cycle): name
'console' is not defined" and moves on. Cycles "succeeded" from the
scheduler's view but never finished emitting their HM-CD-instr lines.

**Discovery path:** the bug only surfaced when the trader was restarted
(commit `6bfa53f` deployed; restart fired). Fresh process loaded clean
bytecode and immediately threw the NameError again on the first cycle.

**Doctrine extracted to main CLAUDE.md.** Fix shipped: commit `ef1c02c` added
the canonical Console init block to both files. Trader restarted on the fix.
HM-CD-instr cycle walls collapsed from 220-408s (CPU + crashes) to 80s
(GPU + clean), per-agent walls from 60-207s to 2-3s.

---

## Banked 2026-05-13 (late afternoon)

**HM-STALE-TRIM-OBS-V2 query fix**: V1 used trades-table arithmetic (BUY
without matching exact-qty SELL = stale) which produced false-positives on
partial scale-outs. V2 anchors on positions table (source of truth) joined to
trades for first-BUY context. Also filters to halt_mode='active' players only,
excluding halted zombies. **Lesson extracted to main CLAUDE.md: positions
table is canonical for "is position open"; trades arithmetic is not equivalent.**

**ollama-local halted**: Stale signal emitter pattern. halt_mode was 'active'
but no trades in 53 days; signals continued but all gate-rejected. Set to
exit_only pending audit of which gate (mandate/confidence/sizing) is the
rejection source. File HM-FLEET-REJECTION-AUDIT for next session if other
emitters show same pattern.

---

## HM-AN2.3 FIRED 2026-05-13 evening (Captain go: "the show must go on Maestro!")

**neo-matrix flipped to halt_mode='active'.** First autonomous AI agent on the
fleet authorized to deploy capital on live (paper) trades.

**Posture at fire:**
- Auto-traded only (paper money on Alpaca)
- 5 trades/day cap (MAX_TRADES_PER_DAY)
- ALLOCATION_POLICY_EXEMPT (no fixed sizing constraint)
- WARNING_ONLY_PLAYERS (some risk warnings don't block)
- HM-AN2-BLOCKED-INLINE active (gate reasons logged inline)
- Starting equity: ~$7,173.69 cash, zero positions, 100% cash
- Trader uptime at fire: 41:52+, running clean main.py

**Pre-state archived** in hm_an23_revert_log table with fired_at timestamp.

**Revert command:**
```sql
UPDATE ai_players SET halt_mode='exit_only',
                      halt_reason='Reverted HM-AN2.3 ' || datetime('now','localtime')
WHERE id='neo-matrix';
```

**22 days of observation-only preceded this:** halt_mode='exit_only' since
2026-05-11, ~60 HM-AN2 candidates observed safely-blocked, all gate-reason
patterns documented via HM-AN2-BLOCKED-INLINE.

**Caveat banked:** if battle_station_monitor cadence drift (913s vs 120s
target, observed at 16:52) is symptomatic of broader scheduler issues,
neo-matrix's HM-AN2.C consume path could be affected.

---

## HM-BK Phase 1 LOADED 2026-05-13 evening

**Movers poller plist loaded.** com.ollietrades.movers-poller running on
5-minute cadence, self-gates to market hours (09:30–16:00 ET Mon-Fri).

**Off-hours behavior:** invocations exit cheaply (no Polygon calls, no DB
writes).

**Phase 2 deferred (Captain-attended):**
- signal_center wiring as tier='mover'
- Dashboard /movers route (needs browser smoke)
- mcap + optionable enrichment via /v3/reference/tickers (nightly cadence)

**To unload:**
```bash
launchctl unload ~/Library/LaunchAgents/com.ollietrades.movers-poller.plist
```

---

## 2026-05-19 → 2026-05-20 — Frontend polish + DB schema + plist cleanup window

**Cadence:** 2 days, 18 PRs merged, 1 schema change, 2 DB triggers added, 1
plist fixed, 1 in-place script edit, 1 ops hygiene action (Cloudflared zombie
cleanup). Drafted as a unit because MSI Ollie migration completed 2026-05-20.
(Post-migration verification: Ollie Max live at 192.168.1.168:11434 with
qwen3:8b, qwen3:14b, qwen2.5-coder:7b, deepseek-r1:14b, gemma3:4b, ministral-3:3b,
0xroyce/plutus:latest installed — verified 2026-05-23 health audit.)

### DB state changes (verify post-MSI-migration)

```sql
-- Triggers shipped 2026-05-19 (HM-HALTED-AT-BACKFILL+ENFORCE, PR #15).
SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'trg_ai_players_halted_at%';
--   trg_ai_players_halted_at_on_insert
--   trg_ai_players_halted_at_on_update

-- Column shipped 2026-05-20 (HM-MOVERS-TICKER-TYPE-SCHEMA+BACKFILL):
SELECT name, type FROM pragma_table_info('mover_watchlist') WHERE name='ticker_type';
--   ticker_type | TEXT
-- 43/434 rows backfilled from scan_universe (CS=37, ETF=6). 391 NULL by design.

-- 3 NULL halted_at rows recovered (HM-HALTED-AT-BACKFILL).
SELECT COUNT(*) FROM ai_players WHERE halt_mode != 'active' AND halted_at IS NULL;
-- Expected: 0
```

### Plist state changes

- **`com.ollietrades.morningbriefing.plist`** — FIXED 2026-05-20 09:10 AZ
  (HM-MORNINGBRIEFING-PLIST-V2). Removed duplicate `/opt/homebrew/bin/python3`
  from ProgramArguments. Added `PYTHONPATH=/Users/bigmac/autonomous-trader`
  to EnvironmentVariables.
- **Cloudflared cleanup 2026-05-20 ~10:40 AZ**: unloaded `com.cloudflare.cloudflared`
  (auto-respawned zombie PID 973 via KeepAlive=true) +
  `homebrew.mxcl.cloudflared` (dead launchctl entry). Single canonical
  `com.trademinds.tunnel` plist remains.

### Engine code carrying NEW behavior (git-tracked, MSI inherits via `git pull`)

- **`engine/morning_briefing.py` `__main__` block** — extended 2026-05-20 to
  run BOTH `generate_morning_briefing(force=True)` (audio MP3 + CIC post) AND
  `generate_daily_intel_report(force=True, push_ntfy=False)` (writes
  `data/morning_brief.json`). Single launchd job, two outputs.
- **`dashboard/app.py:1638-1700ish`** — new endpoint `/api/fleet/active` returns
  DB-driven `halt_mode='active'` player list (HM-FLEET-CORE-DB-DRIVEN PR #16).
  Surface 3+4 (server-side `FLEET_ACTIVE` + `_FLEET_CORE_IDS` Python constants)
  STILL OPEN.
- **`engine/morning_briefing.py:1144` analogue** — `_emit_wr_duration` emits
  `[WR-DUR] cycle wall=Ns` in `logs/trader.log`. Stall NTFY rate-limited
  per-process-lifetime.

### In-place edits NOT in git (must explicit-copy to MSI)

- **`~/ollietrades/etf_regime_trader.py`** — datefmt changed from `%H:%M:%S`
  to `%Y-%m-%d %H:%M:%S` at line 26 (HM-ETF-LOGFMT, 2026-05-20 09:23 AZ).
  `~/ollietrades/` is NOT a git repo.

### Fleet count reality check (changed since 2026-05-19)

Live DB truth: 21 active (`SELECT COUNT(*) FROM ai_players WHERE halt_mode='active'`),
6 `exit_only`, 43 `full`. Prior memos sometimes quoted "19 active" — the extra
2 are season-1 carryovers (`alpaca-mirror`, `mlx-qwen3`, `red-alert`)
intentionally `halt_mode='active'`.

### Frontend ships (PRs #14-#31, all in git)

| Theme | PR(s) | Visible change |
|---|---|---|
| Log date prefix | #14 | `[YYYY-MM-DD HH:MM:SS]` prefix on every trader.log line |
| Fleet membership DB-driven | #16, #17, #18, #30 | `_FLEET_CORE` + `_FLEET_IDS_SET` consume `/api/fleet/active` |
| Halt-mode trigger enforcement | #15 | INSERT/UPDATE triggers auto-fill halted_at |
| Stale badge family | #19, #27 | Game Plan, Riker XO, Bridge Vote `⚠ Stale (Nh)` badges at >24h |
| LCARS aesthetic | #23, #29 | Stardate + real date; 13 new SECTION_LABELS (100% Layer-2 coverage) |
| Last-updated footers | #21 | Sentiment / Macro / Sectors footer "Last updated: H:MM AZ" |
| Market-closed unification | #20 | `📅 Market Closed` amber unified across 3 surfaces |
| Sidebar tooltips | #22 | 6 ambiguous nav items get `title=` hover hints |
| Earnings countdown | #24 | "[SYM] earnings in: Hh Mm (label)" banner |
| VIX cache | #25 | localStorage-cached last-known value when market closed |
| Favicon | #26 | `/static/icon-192.png` (was 404→/login) |
| Console noise | #28 | Meta tag deprecation fix + LiveChart warn→debug |
| Morning intel two-output | #31 | morning_brief.json refreshes daily |
| DB schema + backfill | (no PR# yet) | `mover_watchlist.ticker_type` column |

### Lessons banked 2026-05-20

Five new feedback memos in `~/.claude/projects/-Users-bigmac/memory/`:

1. `feedback_xo_log_pattern_match_drift` — read source for diagnosis, not log
   appearance. ETF Regime "5s replay loop" was actually one-per-day with
   HH:MM:SS-only datefmt.
2. `feedback_morning_audit_plist_label_drift` — launchctl exit-status entries
   are NOT process owners. Cross-match PIDs from `ps aux` against `launchctl
   list`.
3. `feedback_xo_broker_reconciliation_drift` — verify ALL three books
   (real-money / Alpaca paper / fleet) before declaring exposure resolved.
   NVDA "closed on Webull" hid two errors: Webull liquidated 5/13 AND Alpaca
   paper still held 12.34 sh ghost.
4. `captain_actions_log_2026-05-20` — NVDA real-money zero, paper-book ghost
   closed via `/api/alpaca/close/NVDA`.
5. Project tickets: `project_hm_layer_2a_design`, `project_hm_lcars_coverage_audit`,
   `project_hm_wr_stall_alarm_rate_limit`,
   `project_hm_decision_support_observability_audit`,
   `project_hm_daily_intel_report_scheduler`,
   `project_hm_movers_ticker_type_schema` (RESOLVED).

### Still open after wave (queued for post-migration / next session)

- L2 — `dashboard/app.py:1638` `FLEET_ACTIVE` + `app.py:7511` `_FLEET_CORE_IDS`
  server-side mirrors. Bundled with Layer 2a v1 ship.
- Layer 2a v1 — log-only `[WR-BUDGET-EXCEEDED]` instrumentation, design banked
  at `project_hm_layer_2a_design`. ~2h scope.
- HM-OLLIE-AUTO-MAY-REGRESSION-AUDIT — WR April 86.8% → May 60.0%.
- HM-DECISION-SUPPORT-OBSERVABILITY-AUDIT — multi-hour audit, post-migration.
- LCARS Layer-3 — themed content for top-5 panels, 12-16h, post-migration.

### Migration runbook

`data/scotty_msi_migration_runbook_2026-05-20.md` is the ground-truth carry-over
list for MSI Ollie cutover. Sections A-G + F-Supplement (Cloudflared detail).


---

> Relocated from CLAUDE.md (HM-PRIME Part C).

## Drift Catalog 2026-05-17

10 distinct drift classes caught in a single audit-first session. Captured
here so the doc can be trusted without re-verifying each session.

1. **Model assignment silent bypasses** — main.py per-call overrides shadow
   `ai_players.model_id` for ~3 routed players; DB truth ≠ runtime truth.
2. **Hidden bypasses from config.AI_PLAYERS-scope-limited side-by-side** —
   HM-CN audit only walked `config.AI_PLAYERS`; main.py and crew_scanner
   overrides outside that scope went undetected for months.
3. **Role-vs-reality gaps** — Spock documented as LLM second-opinion
   (qwen3:8b) but Sniper Role #1 is rule-based RSI-bounce (no LLM call); Troi
   options wheel blocked by Bridge gate despite role.
4. **Dead-code gates** — `PAID_MODEL_IDS` guard unreachable for cto-grok42
   path; the model swap never traversed the gate that would have blocked it.
5. **Fleet-config-vs-reality** — 5.8 Ollie migration assumed mistral:7b moved
   to Ollie Box; reality was bigmac. Pin → routing decisions based on false
   state.
6. **Docs-vs-bind reality** — CLAUDE.md claimed port 8080 "bound network-wide"
   but `main.py:3250` pins to `127.0.0.1`. LAN reachability is Cloudflare
   tunnel only. (Fixed in network bindings section above.)
7. **debate_engine.py is NOT CrewAI** — any doc describing
   `engine/debate_engine.py` as CrewAI-orchestrated is wrong. Reality: plain
   `asyncio` + `aiohttp` + Ollama HTTP.
8. **war_room_debates vs debate_history_v2 table confusion** —
   `/api/war-room/debate-history` serves `war_room_debates` (Captain
   Ask-Arena, ~1,447 rows). 12-agent structured debates live in
   `debate_history_v2` (~272 rows), served by `/api/war-room/debates/recent`.
9. **Journal "gitignored per convention" — wasn't** —
   `data/scotty_journal_*.md` was treated as gitignored-by-convention but
   `.gitignore` had no matching pattern. (Fixed via `git check-ignore` audit.)
10. **Dormant-code-becomes-production-via-Path-1** —
    `options_exec.open_options_trade` had zero callers (dormant) until a fix
    wired it into a path running every signal cycle. A function with no prior
    call traffic is now production-load-bearing without the test/audit
    history a tenured path would have.

### Backup discipline
Every `.bak*` file is dated (`<file>.bak_<purpose>_YYYYMMDD_HHMMSS`) and
preserved **24h minimum** before any cleanup — per
`feedback_db_archive_not_delete` doctrine.


---

> Relocated from CLAUDE.md (HM-PRIME Part C).

### Future considered epic: submit-time manual-halt gate (NOT built, 2026-06-09)
There is **no order-time trading-halt gate on the MANUAL order path** (Trade Desk +
Symbol Focus quick-actions + `kirkPositionAction`). `/api/kill-switch` is a *flatten-all*
action (`engine/kill_switch.py::kill_all_positions`), not a submit gate; `is_halted`/`halt_mode`
gate the **agent/fleet** paths only. So manual Captain orders fire regardless of any halt.
This is **defense-in-depth, not urgent** (agent paths ARE gated; manual is Captain-initiated +
auth-gated + paper-only). If ever built, the considered shape is: a settings-backed
`manual_trading_halted` flag checked at submit, **buy-side only with an exit-allowance**
(exits/closes/reduces must ALWAYS fire even when halted — never gate the sell/close path). A
separate change, deliberately deferred. (Surfaced during HM-DRYDOCK Symbol Focus routing, where
the quick-action ENTRIES were rerouted through the gated Trade Desk submission for OCO + clean
attribution; the kill-switch "gate" turned out not to exist to inherit.)

There is **no `agent_state` table** in any DB. A vestigial reference in
`archive/retired/2026-05-04-post-earnings-drift/post_earnings_drift.py::is_halted()`
(PED retired 2026-05-04) queries the phantom table but is wrapped in
`try/except → False`, so it's functionally inert. PED's actual protection is
`self.gated = True` (paper-only).

**Why `halted_at` is mandatory:** no schema default, no trigger before
2026-05-19. Forgetting it left NULL timestamps that audit #6A flagged. As of
HM-HALTED-AT-BACKFILL+ENFORCE (PR #15, 2026-05-19), INSERT/UPDATE triggers
auto-fill `halted_at` for `halt_mode != 'active'` rows, so the manual UPDATE
above is belt-and-braces — the triggers backstop it.

To unhalt: same UPDATE pattern, `halt_mode='active'`, leave `halted_at` and
`halt_reason` as historical record (do not clear).
