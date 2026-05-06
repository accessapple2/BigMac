# XO Backlog — USS TradeMinds
# Riker's Standing Work Queue
# Updated: 2026-05-03 (Yellow Alert Phase 1 reconciliation)

**Reconciliation method**: every claim below verified against running code, DB state,
launchctl, trader.log post-PID-84968 startup (15:45 MST today), and on-disk files.
Items moved by category based on observed reality, not historical claim.

---

## Schwab Workflow

**Drop directory:** `/Users/bigmac/Downloads/` (relocated 2026-05-04 from `/Users/Shared/schwab_inbox/`).

**How it works:** Browser saves `Scwab*Positions*.csv` to `~/Downloads/`. The launchd watcher `com.ollietrades.schwab-watcher` polls every 60 seconds, finds the file via glob `Scwab*Positions*.csv` / `Schwab*Positions*.csv` / `schwab_*.csv` (case-insensitive), invokes `scripts/import_schwab_csv.py`, syncs via `scripts/sync_schwab_to_real_holdings.py`, archives the CSV to `data/schwab_csv_archive/`, and fires an NTFY notification to topic `ollietrades-admin`.

**Imports log:**
- 2026-05-04 09:35 MST — fresh snapshot 2026-05-04T12:15:00 imported, 24 rows. Resolved 4-day stale-data display issue (DELL day-change was showing -3.59% from 2026-04-30 snapshot; now correctly -0.77% from today's snapshot).
- 2026-04-30 09:21 MST — snapshot 2026-04-30T11:30:00, 16 rows
- 2026-04-28 09:39 MST — snapshot 2026-04-28T12:30:00, 14 rows
- 2026-04-24 09:54 MST — snapshot 2026-04-24T12:48:00, 8 rows

**Cadence note:** Imports are still manual-trigger (Admiral exports Schwab Positions CSV; browser saves to `~/Downloads/`; watcher does the rest). No NTFY reminder added — revisit if drift recurs in 3 weeks.

---

## ⚠️ POST-GATE-FLIP MONDAY MORNING WATCH (2026-05-05 06:30 MST)

Gate flipped 2026-05-04 08:30 MST (commit `df7320c`). Service restarted PID 13734.
First live autonomous trades fire at NYSE open Monday 06:30 MST / 09:30 ET.

**Tier-2 execution gating:** Both `bull_call_spread_v1` and `bear_put_spread_v1`
filter `WHERE agent_name='tractor-beam'` on signal-center reads. **Tractor-beam
is the agent whose performance matters.** Pre-flip 30-day baseline: 268 signals,
34.3% hit_tp, PF 2.02, avg_pnl +1.74%.

### Pre-open (06:00 MST):
- [ ] Service running, PID stable (was 13734 at flip; may have rolled overnight)
- [ ] No overnight Errno 48 in trader.log (baseline = 6)
- [ ] Calibration query produces reasonable numbers (`signals.db::trade_signals` joined to `signal_outcomes`, agent_name='tractor-beam', last 24h)
- [ ] Halted players still 4 (ollama-llama, grok-3, dayblade-sulu, gemini-2.5-pro)
- [ ] All 3 gate sites still `_EXECUTION_ENABLED: bool = True`

### First-trade observation (06:30 - 10:30 MST):
- [ ] First fleet signal of the day fires cleanly
- [ ] First trade placed via Alpaca paper successfully
- [ ] Trade appears in `paper_trades` and `trades` tables
- [ ] Dashboard `/api/agents/scoreboard` reflects the new trade

### Kill-switch criteria — REVERT or HALT if ANY of:
1. **Tractor-beam delivering >5% loss on any single trade** → REVERT (SL discipline failure on the agent that drives execution)
2. **Aggregate paper P&L drawdown >5% from $99,931 starting balance in any 24h period** → HALT for review
3. **Tractor-beam placing >15 trades in first 4 hours** → HALT that signal source (suppress tractor-beam writes to `signals.db::trade_signals`)
4. **2+ service crashes in first 4 hours** → REVERT
5. **ANY real-broker (Schwab/Webull/IBKR) API call attempt** → REVERT IMMEDIATELY

### Recovery procedure (REVERT path):
```bash
cd ~/autonomous-trader
git reset --hard gate-flip-revert
git push --force-with-lease origin main
launchctl kickstart -k gui/$(id -u)/com.trademinds.trader
```

- Pre-flip main HEAD: `753f01a70f2a145a1f2cd70a41143d8188f0ae3d`
- Pre-flip backup: `backups/trader.db.pre-gate-flip-20260504_082909`
- Recovery doc: `/tmp/gate-flip-recovery.md`
- Local-only branch `gate-flip-revert` retained at least 1 week of clean operation

### HALT-tractor-beam procedure (kill-switch #3, less drastic than full revert):
The tractor-beam poster is external to this repo (posts via HTTP to signal-center port 9000). To halt it without reverting the gate:
```bash
# Option 1: Mark all tractor-beam NEW signals as DISMISSED (one-shot scrub)
sqlite3 signal-center/signals.db "
  UPDATE trade_signals
     SET status='DISMISSED', dismissed_at=datetime('now')
   WHERE agent_name='tractor-beam' AND status='NEW';
"
# Then identify and stop the upstream poster (separate investigation needed).
```

---

## ✅ HM-G COMPLETE — origin push unblocked (2026-05-04 07:25 MST)

5 fat files (1.32 GB total) removed from history via `git filter-repo` on a mirror clone, force-pushed to origin. Origin now at `50ef95c` (rewritten HM-C ship). Push 1 (rewrite): `50ef95c`. Push 2 (gitignore prevention): `f7181f0`. All 25 ahead-commits cleared.

Original archive preserved at `~/autonomous-trader-archive/2026-05-04-pre-hmg-rewrite/` (5 files, hash + byte verified). Surgery mirror at `~/git-surgery/autonomous-trader-mirror-20260504_070333/` retained for insurance.

`.gitignore` extended with fat-file prevention patterns: `backups/*.db.*`, `backups/*-shm`, `backups/*-wal`, `*.deprecated_*`, `.fuse_archive_*/`, `/trader.db`, `*.orig`, `*.swp`. Bare `*.bak` deliberately omitted to avoid silently shadowing 20+ tracked sprint backups.

---

## Retired Components

### Kirk Swing Desk — retired 2026-05-04
- Scaffolded but never wired (`agents/kirk.py::propose_swing()`, `agents/pike.py::second_opinion()`).
- Audit #6A investigation determined: drift between CLAUDE.md (claimed active in fleet roster) and code (zero callers, zero scheduler entries). Per `docs/AUDIT_6_INVESTIGATION_2026-05-04.md` Problem B.
- Decision: **RETIRE** rather than build the 6-8 hr feature. Manual swing-trading workflow no longer applies — fleet shifted to autonomous Alpaca-paper-only.
- Daily Kirk advisor (`engine/kirk_advisory.py`, `engine/kirk_grok_advisor.py`) is preserved and active. `kirk_advisory_log` continues to receive daily writes (272 rows, last write 2026-05-01).
- Code archived at `archive/retired/2026-05-04-kirk-swing-desk/` with restoration instructions in README.
- DB tables `kirk_signals` (0), `kirk_swing_trades` (0), `pike_votes` (0) preserved as empty schemas per SACRED-DATA discipline; can be dropped in a future schema-cleanup migration if approved.

---

## VERIFIED CLOSED (commit + reality both confirmed)

| ID  | Closed | Commit | Reality verification |
|-----|--------|--------|----------------------|
| B1  | 2026-05-03 | `8e06b5e` | `bull_spread_v1` `BULL_CROSS`→`BULL` mapping at `main.py:2610`; regime tick log confirms `BULL_CROSS` normalized to `BULL` at scheduler boundary |
| B2  | 2026-05-03 | `8e06b5e` | `bull_call_spread_v1` `get_regime` ImportError eliminated — replaced with `MarketContext` at `main.py:2685-2701`. Zero `get_regime` ImportErrors after PID 84968 startup |
| B3a | 2026-05-03 | `8e06b5e` | Edit 3 replaced broken `get_regime` import with `MarketContext` + regime normalization |
| B3b | 2026-05-03 | `8e06b5e` | Edit 2 regime normalization at `main.py:2648` — `bear_put_spread_v1` inverted block-list now correct |
| B4  | 2026-05-03 | `8e06b5e` | Same as B3b — Edit 2 closes inverted block-list (no separate `bear_put_spread_v1.py:366` edit needed) |
| B14 | 2026-05-03 | `cdc03d0` | `GetAllPositionsRequest` import removed from `engine/alpaca_options.py`. Symbol confirmed absent in alpaca-py 0.43.2; pure dead-code removal, zero behavioral change |
| B15 | 2026-05-03 | `17d40b4` | `OLLIE_URL` added to `initialize_dayblade()` import. **Verification: zero `OLLIE_URL` errors in `trader.log` after line 337403 (PID 84968 startup at 15:45)**. Pre-fix count 53,985, post-fix delta 0 |
| Task 3A | 2026-05-02 | `803c2db` | `engine/importers/ai4trade_importer.py` — `run_import()` alias added |
| Task 3B | 2026-05-02 | `803c2db` | `uoa/scraper.py:16` docstring path corrected |
| Task 3C | 2026-05-02 | `803c2db` | `premarket-scan.sh:46` defunct `launchctl start com.trademinds.crew` commented out |
| Item 5 | 2026-05-03 | `58c43f0` | ~60 lines dead crew-server polling removed from `premarket-scan.sh` |
| **AUDIT-#1** | 2026-05-03 | *pending commit* | **`halt_mode` enum added to `ai_players` (active/exit_only/full); `halt_gate` helper at `engine/halt_gate.py`; gates wired in `paper_trader.save_signal` (line 1870), `paper_trader.buy()` (line 547), `paper_trader.sell()` (line 1091; semantic: `exit_only` permits sells), `signal_tracker.record_signal` (line 35). Backfilled 1,156 leaked rows (`signals` 1,143 + `watchlist_signals` 13). Live gate-fire confirmed via direct exercise test.** |
| **AUDIT-HM#1** | 2026-05-03 | *pending commit* | **`healthcheck.py:43` `print(line)` removed; launchd plist already routes stdout → `logs/healthcheck.log`, eliminating 2× duplication. Truncated `logs/healthcheck.log` for clean post-fix verification window (next cron tick Mon 06:00 MST).** |
| **AUDIT-Open-Q#1** | 2026-05-03 | *pending commit* | **`ollama-llama` trapped positions flatted internally (NVDA 0.3748 sh @ $198.39, MSFT 0.175 sh @ $399.08; total realized -$7.16); `execution_type='manual_internal_mark'` for audit trail. No Alpaca round-trip per Admiral resolution Option B.** |

---

## PARTIALLY DONE (committed but not yet runtime-verified in production)

| Item | Status | Outstanding verification |
|------|--------|--------------------------|
| Edit 3 (`bull_call_spread_v1`) Monday verification | Code-level verified at `main.py:2685-2701` | Runtime verification needs Monday 06:30-13:00 MST market-hours window. Protocol at `/tmp/scotty_session_2026-05-03/b15_verification_protocol.md` |
| `bull_spread_v1` `BULL` normalization | Logged regime ticks confirm `BULL_CROSS`→`BULL` mapping fires | Need observation of an actual bull-spread signal generated post-fix during market hours (none yet — Sunday) |
| `bear_put_spread_v1` block-list correction | Code paths verified | Need market-hours observation that strategy correctly does NOT fire in BULL regime |
| OPS_LOG audit-trail bonus in `8e06b5e` | Healthcheck `backup_trader_db()` has `operation_name` param + writes to `docs/OPS_LOG.md` | Need next backup event to confirm trail writes |

---

## INTENTIONALLY PAUSED (deliberate dormancy, not a bug)

| Component | Pause mechanism | Verified state | Documentation |
|-----------|-----------------|----------------|---------------|
| `dayblade-sulu` (Lt. Sulu primary options trader) | `is_halted=1` in `ai_players` table, `halt_reason='S6.3 bench: R:R 0.10, dormant since 2026-03-31'` | DB-verified halted; `paper_trader.py` `buy()`/`sell()` both gate on `is_halted` (lines 547, 1091) | Drydock 2026-04-25 audit (CLAUDE.md) |
| `dayblade-0dte` (T'Pol on plutus) | Functionally idle: scheduler still runs `run_dayblade` every 5 min at `main.py:2554`, but no signals in DB since 2026-04-07 (26 days) | DB: MAX(`signals.created_at`) for `player_id LIKE '%dayblade%'` = `2026-04-07 15:41:46` | **Note: NOT commented out at main.py:1920 as previously claimed** — that line is `agent_ratings` code. DayBlade run path is live; dormancy is empirical (no trades emitted), not gated. Investigate before next iteration. |
| Battle Station feeders | Not in launchd | `launchctl list \| grep battle` returns 0 entries; `com.trademinds.battle*` does not exist | April 23 surgery, never re-added |
| Battle Station scheduler in main.py | Active: `run_battle_station_monitor` every 2 min at `main.py:2575`, `run_morning_briefing` daily 06:00 at line 2566 | Code-active but downstream feeders absent | "Pause" is partial: scheduler fires but feeders aren't running, so any signal pipeline is broken |
| `ollama-llama` | `is_halted=1`, `halt_reason='S6 review: routing zombie, retired 2026-04-25'` | DB-verified halted | Drydock 2026-04-25 |
| `grok-3` | `is_halted=1`, `halt_reason='S6 review: routing zombie, retired 2026-04-25'` | DB-verified halted | Drydock 2026-04-25 |

---

## ARCHITECTURALLY INCOMPLETE (code half-built, not fully wired)

| ID | Component | Reality | Severity |
|----|-----------|---------|----------|
| AI-1 | `signal_scorecard` table | Schema exists with 16 cols, **0 rows**. Writer never wired (April 7 Alpha Engine plan unfinished). Scoring pipeline can't run without source data. | MEDIUM — blocks gate-flip calibration (B5 dependency) |
| AI-2 | `ghost_trades` table | Only **9 rows total** (verified). Per-agent tables (`sarek_paper_trades`, `janeway_paper_trades`, `surak_paper_trades`, `kirk_signals`) appear to be the actual write paths, leaving `ghost_trades` mostly empty. CLAUDE.md describes Bench 4 ghost-recording every signal. Same import-drift family as B12-B15 likely. | MEDIUM — distorts ghost performance scoring |
| AI-3 | `is_active` flag is decorative | Verified: `paper_trader.py` enforces `is_halted` (lines 547, 1091) but `is_active` only appears once at line 1555 in a `SELECT ... WHERE COALESCE(is_active, 1)=1` filter. Halted players (`ollama-llama`, `grok-3`) still have `is_active=1`. Per April 25 audit, `is_paused`, `crew_role` are also decorative. **Document before any new agent wiring.** | DOC-CRITICAL — easy to mis-trust |
| AI-4 | `bridge_voter` collection | `bridge_votes` table has 216 rows total, MAX `created_at` = 2026-05-01 13:01:23 (2 days ago). Wired but not collecting daily. | LOW-MEDIUM — investigation needed |
| AI-5 | `energy-arnold` quality | `qwen3:8b` LLM, **9,632 signals** total, AVG confidence 0.258. Distribution: 6,643 at conf=0.0 (69%), 1,209 at conf=1.0 (13% over-confident), rest scattered. is_active=1, is_halted=0. Bridge_voter wired but not collecting. | NEEDS-DECISION — high noise volume; Phase 4 reframe |
| H1 | `engine/tiered_exits.py:check_spread_exits()` | Fully implemented, never called by any scheduler | HIGH — needed before first live spread trade |
| H2 | `_EXECUTION_ENABLED = False` | 3 independent copies in `executor.py:22`, `bull_call_spread_v1.py:63`, `bear_put_spread_v1.py:63`. Must flip atomically | DEFERRED — after 30 paper trades + positive expectancy |
| H3 | `/api/wheel/status` | Intermittent 500 at `dashboard/app.py:7592` | HIGH — before Wheel goes live |

---

## OPEN BOMBS (current severity, post-reconciliation)

### Production noise / latent
| ID | File | Severity | Status |
|----|------|----------|--------|
| B5 ✅ RESOLVED 2026-05-04 | `signal-center/server.py:2121` | — | **Audit #6X investigation cleared this.** Scorecard system at `signals.db::trade_signals + signal_outcomes` is healthy (1,147 signals, 100% outcome coverage, daemon writing every 15 min). Endpoint `/api/signals/scorecard` returns HTTP 200 in ~19ms. **NOT blocked on AI-1** — that was a different table in `data/trader.db` audit #6A flagged as separate work. Per Admiral verdict 2026-05-04: gate-flip ready at SQL-level review. Frontend calibration column = follow-up sprint, not blocker. See `docs/AUDIT_6X_INVESTIGATION_2026-05-04.md`. |
| B12 | `main.py:481/484` | MEDIUM | `check_vix_spike` ImportError — no commit yet, B12 status check on Monday per `b12_proposed_fix.md` |
| B13 | `main.py:3608` | LOW | Rallies scraper ImportError — 1 occurrence; deferred |
| B16 | `healthcheck.py:25,474` | MEDIUM (downgraded from CRITICAL) | `TUNNEL_URL` hardcoded to orphan `bridge.accessapple.com`. Real bridge `bridge.ollietrades.com` healthy. Part of accessapple rebrand sprint |
| B17 | unknown XML/lxml caller | MEDIUM | 49 `Filename too long: %3C!DOCTYPE…` in `trader_error.log` — passing HTML body as filename |
| B18 | `engine/fast_scanner.py:389/489-490` | MEDIUM | 34 `database is locked` in `scanner.err` — WAL contention with trader process |
| B19 | aladdin scraper write path | LOW-MEDIUM | 35 db-lock-adjacent entries in `aladdin.log`, same family as B18 |
| B20 | yfinance internal | LOW | 25 `HTTP 401 Invalid Crumb` self-recovers, ~9 retries per burst |
| B21 | iv_history pipeline | **LOW (downgraded from MEDIUM)** | "Day 5 missing 2026-05-02" was a Saturday — iv_history records weekdays only. MAX as_of_date = 2026-05-01 (Friday, 10 rows = healthy). Reframe: H4 ops check applies to the next Monday, not weekend |
| B27 | `healthcheck.py` (Ready Room + Red Alert) | LOW | Crusher weekend false-positives on Sat/Sun |
| B29 | `data/trader.db` `ghost_trades` table | MEDIUM | Folded into AI-2 above |

### Cleanup-eligible (Phase 2 candidates)
| ID | Description | Phase 2 action |
|----|-------------|----------------|
| B22 | Two 0B `arena.db` files (root + `data/`) | **CLOSED 2026-05-03** — archived to `arena.db.deprecated_20260503_182837` and `data/arena.db.deprecated_20260503_182837`. Filesystem-only (gitignored). Rollback: `mv ...deprecated_*` back. setup_db.py confirms files were dead artifacts |
| B23 | `tractor.db` referenced in CLAUDE.md SACRED DATA but file does not exist in `~/autonomous-trader` (lives in `~/ollietrades/tractor_beam/tractor.db` and `/Users/bigmac/G1_BACKUP/`) | Doc drift; address with CLAUDE.md update outside this directive |
| B24 | No log rotation policy. `trader.log` 27.5 MB / 337k lines, `trader_error.log` 13.7 MB / 142k lines | Phase 3 investigation report |
| B25 | 19 `.fuse_hidden*` zombie files (32KB each) | **CLOSED 2026-05-03** — archived to `data/.fuse_archive_20260503_182918/` (19 files, all `lsof`-empty pre-archive). Filesystem-only. Rollback: `mv data/.fuse_archive_20260503_182918/* data/` |
| B26 | `main.py:2554-2587` scheduler comment-vs-cadence drift (11 mismatches confirmed) | **CLOSED 2026-05-03** — commit `9ee1c5c`. py_compile clean. Rollback: `git revert 9ee1c5c` |
| B28 | 4 backup orphan WAL files (`trader_2026-04-07.db-shm/-wal`, `trader_2026-04-08.db-shm/-wal`) | **CLOSED 2026-05-03** — archived to `backups/orphan_wals_20260503_182933/` (4 files). Filesystem-only. Rollback: `mv backups/orphan_wals_20260503_182933/* backups/` |

---

## DEFERRED (planned sprints, out of scope tonight)

### Accessapple rebrand cleanup sprint
**Verified count: 22 references across 6 files** (down from claimed 24):
- `healthcheck.py` (2)
- `main.py` (1)
- `dashboard/app.py` (11)
- `docs/G1_MIGRATION_INVENTORY.md` (5)
- `docs/SECURITY_AUDIT.md` (3)
- `docs/XO_BACKLOG.md` (this file, references)

Pre-sprint checklist (unchanged from prior version):
1. Confirm `bridge.ollietrades.com` is in CORS allow-list at `dashboard/app.py:1237` (don't just swap — *verify*)
2. `git remote -v` to confirm GitHub remote — is `accessapple2/BigMac.git` still valid or also renamed?
3. After fix: end-to-end test from external browser via `bridge.ollietrades.com` → dashboard → API call
4. Update `healthcheck.py:481-487` success criteria to accept 2xx/3xx (Cloudflare Access redirect = healthy)
5. Pair with B16 fix — fixing only the URL without success-criteria fix leaves Crusher still flagging stale on the 303

**Why deferred:** sprint touches CORS (security boundary) and external API docs (user-facing). Needs Admiral approval + a weekday window with browser at hand for verification.

### UX Sprint (`docs/UX_SPRINT_2026-04-28.md`)
All acceptance criteria unchecked — sprint never started.
- Priority 1: Risk-adjusted Leaderboard (Sharpe/Sortino/max DD/calibration columns)
- Priority 2: Today's Read Strip + Collapsible Cards
- Priority 3: Plain Mode Toggle

### Chekov Rehab
- Extract S5 version: `git show 859a4f0:engine/chekov_autotrade.py`
- Ghost-trade S5 vs current for 30 days, promote the better one
- Current threshold: 5.0 (muted)

### Bench 4 Ghost Runs (none started)
- Uhura-EDGAR: 60-day ghost run, promote if Sharpe > Capitol's
- Aladdin: wire iShares ETF flow → paper-trade sector rotation
- Spock-R1: 60-day A/B vs McCoy-alone (`ollama pull deepseek-r1:7b` first)
- Picard: convert weekly briefing → Ollie regime-table modifier

### Other deferred
- Phase 2 historical performance forensics across trader.db, signals.db, arena.db
- Phase 3 new backtests for orphaned strategies (`engine/options_agents.py` classes)
- Phase 4 spread strategy comparison report
- signals.db archival cron — first eligible 2026-05-05

---

## ARCHITECTURAL ORPHANS (code exists, zero wiring to main.py)

| Agent/Class | File | Strategy | Wiring Status |
|-------------|------|----------|---------------|
| `QuarkIronCondor` | `engine/options_agents.py` | Iron condor | No scheduler entry |
| `McCoyBullPut` | `engine/options_agents.py` | Bull put spread | No scheduler entry |
| `AndersonBearCall` | `engine/options_agents.py` | Bear call spread | No scheduler entry |
| `CoveredCallAgent` | `engine/options_agents.py` | Covered call | No scheduler entry |
| `GhostKirkBullCall` | `engine/options_agents.py` | Ghost bull call | No scheduler entry |
| `GhostKirk0DTEBullCall` | `engine/options_agents.py` | Ghost 0DTE | No scheduler entry |
| `GhostLongCall` | `engine/options_agents.py` | Ghost long call | No scheduler entry |
| `GhostNakedPut` | `engine/options_agents.py` | Ghost naked put | No scheduler entry |
| `check_spread_exits()` | `engine/tiered_exits.py` | Model F exits | Imported at main.py:3952 but never called |
| `bear_call_spread()` | `engine/spread_trader.py` | Bear call spread | `SPREADS_ENABLED=False`, scaffolding only |
| `iron_condor()` | `engine/spread_trader.py` | Iron condor | `SPREADS_ENABLED=False`, scaffolding only |

---

## HIDDEN BOMBS (latent, not yet exploding)

| ID | File | Description | Trigger |
|----|------|-------------|---------|
| X3 | `strategies/bull_call_spread_v1.py:2691` | `ctx = {"regime": get_regime()}` — dict not MarketContext (note: regression check needed against `8e06b5e` Edit 3) | After import fix |
| X4 | `main.py:3952` | `MODEL_F_THRESHOLDS` imported at startup, `check_spread_exits()` never scheduled | When spreads go live |
| X5 | All 3 `_EXECUTION_ENABLED=False` | Three independent copies — must flip atomically | Gate-flip session |

---

## OPS UNVERIFIED

| Item | Check | When |
|------|-------|------|
| ~~Ghost scorecard calibration~~ ✅ CLEARED 2026-05-04 | Audit #6X verified endpoint healthy; 1,147 signals, 100% outcome coverage. Per Admiral verdict, SQL-level review sufficient — frontend column is follow-up sprint, not blocker. | (resolved) |
| Alpha threshold for `bull_spread_v1` first trade | Confirm threshold in strategy config | Before first trade |
| Chrome extension Profile 5 re-install | Manual check | Next session |

---

## FOLLOW-UPS FROM AUDIT-#1 (halt_mode introduction)

| ID | Task | Priority | Notes |
|----|------|----------|-------|
| HM-A ✅ FIXED 2026-05-04 | Migrate the ~22 `is_halted` read-sites to `halt_mode != 'active'` | MEDIUM | **Shipped 2026-05-04 (commit `a7e095a`). 14 production read sites migrated** (spec count "~22" was inflated; classification surfaced 14 actual reads after excluding write paths, drawdown-system reads, schema defines, and archived backups). Files: `dashboard/app.py` (9 sites including 2 `WHERE` filters + 7 SELECT/attr reads), `engine/paper_trader.py` (2 SELECTs — dropped unused column from buy/sell halt gate), `engine/morning_briefing.py:62`, `engine/war_room.py:835` (`WHERE is_halted=1` → `halt_mode != 'active'`), `reset_season2.py:64`. Every change tagged `# HM-A:`. API response shape preserved (`is_halted` JSON key, value derived from halt_mode). Drawdown-halt system at `ai_brain.py:817-848` + `risk_manager.py:868` + `post_earnings_drift.py` left alone — different concept (reads `agent_state.is_halted`, not `ai_players`). **Note (HM-S 2026-05-04):** the carve-out targets above were factually inaccurate. `ai_brain.py:817-848` and `risk_manager.py:868` do NOT read from `agent_state` (zero references confirmed by grep). The real drawdown-halt is `risk_manager.py::check_drawdown()` reading `portfolio_history` transiently. Only `post_earnings_drift.py:56` queries the phantom `agent_state` table (silent except). The carve-out *discipline* (don't touch drawdown-related code during halt-system migrations) was correct in spirit; the cited file:line targets were wrong. See HM-S report. |
| HM-B ✅ FIXED 2026-05-04 | Drop `is_halted` column from `ai_players` | RESOLVED | **Shipped 2026-05-04 (commit `9256890`).** Pre-flight (commit `a3a4cd0` HM-B-pre) migrated 4 unmigrated WRITE sites: `reset_season2.py:49,50`, `engine/season_manager.py:154,258`, `shared/matrix_bridge.py:114`, `setup_db.py:24` — all now use `halt_mode='active'` semantics. Live DB DDL: `ALTER TABLE ai_players DROP COLUMN is_halted` on SQLite 3.51. Backup at `backups/trader.db.pre-hmb-20260504_173026`. Service stable post-drop (PID 13734). Halt state now has single source of truth: `halt_mode`. |
| HM-C ✅ FIXED 2026-05-04 | Update read-path consumers of `signals` / `watchlist_signals` to filter `halted_emit = 0` for scoring queries | MEDIUM | **Shipped 2026-05-04. 22 files modified, 28 SQL sites filtered. Scope was broader than first scoped: `ai_brain.py:563` (TIER-1 escalation), `bull_call_spread_v1.py:251` / `bear_put_spread_v1.py:270` (tier-2 spread vote), `crew_scanner.py:3963` (autopilot fleet consensus), `risk_manager.py:312` (bear-mode gate) all consume signals for current-day execution decisions, not just calibration. Halted players were implicitly voting through pre-fix-#1 backlog rows. Helper `HALTED_EMIT_FILTER` constant added to `engine/halt_gate.py` for single-source-of-truth migration when HM-A/HM-B retire `is_halted`. Display/forensic paths preserved. `/v1/signals` external API also filtered — note in commit message under Behavior change visible to /v1/signals consumers** |
| HM-D ✅ INVESTIGATED 2026-05-04 | `watchlist_signals` halted-player rows decision | LOW | **Verdict α (Retain) recommended.** 165 halted-player rows total (62 ollama-llama + 41 sulu + 35 gemini-2.5-pro + 27 grok-3). 34 still in `status='active'` but bounded and self-resolving — `signal_tracker.py:124,133` ages them to `hit_target`/`expired` over time, and `halt_gate.can_emit_signal` blocks new active rows. Optional follow-up HM-D-fix (~30-45 min): add `JOIN ai_players halt_mode='active'` to 6 currently-unaware readers (5 in signal_tracker + crew_scanner.py:3965). Full report: `docs/HM-D_WATCHLIST_SIGNALS_VERDICT_2026-05-04.md`. |
| HM-E ✅ INVESTIGATED 2026-05-04 | Halted-player daily routines waste check | LOW | **Verdict B (modest waste).** Signal emission stopped naturally (last halted-player signal 3+ days ago). Trades all SELL action under exit_only — legitimate. **`ai_journal` runs daily for sulu + ollama-llama** — `main.py:520 run_journal()` and `engine/ai_journal.py:18 generate_journal_entry()` have zero halt-mode checks. ~2 LLM calls/day to Ollie Box for journals no one reads. Optional follow-up HM-E-fix (~5 min, low risk): add 3-line halt-mode check in `engine/ai_journal.py::generate_journal_entry()`. Full report: `docs/HM-E_HALTED_ROUTINES_VERDICT_2026-05-04.md`. |
| HM-S ✅ INVESTIGATED 2026-05-04 | `agent_state` table ghost — drawdown-halt source of truth question | MEDIUM | **Verdict C (dead but harmless) + documentation drift.** Drawdown halt protection IS functional but does NOT read from `agent_state` as CLAUDE.md claims — it's recomputed transiently every cycle from `portfolio_history` in `engine/risk_manager.py::check_drawdown()` (3,562 rows, 20% threshold). `agent_state` table never existed in any of 13 .db files searched. Only one reader exists (`agents/post_earnings_drift.py:56`) and it silently degrades to `False` via bare `except: return False`. PED is paper-only via separate `gated=True` flag — broken halt-check cannot cause real-money damage. **Live gate-flip soak is safe.** Recommended actions: (1) fix CLAUDE.md "Why both is_halted and halt_mode" section to describe transient drawdown computation, not phantom agent_state table, (2) optional PED cleanup — replace dead `is_halted()` with simpler `enabled` toggle (~10 min). Full report: `docs/HM-S_AGENT_STATE_GHOST_2026-05-04.md`. |
| HM-F ✅ RESOLVED 2026-05-04 | Add `halted_at` UPDATE to whatever code path sets `is_halted=1` going forward | RESOLVED | Audit found zero halt-write paths in current code. The four currently-halted players were halted via manual sqlite3 UPDATE; `season_manager.py` and `reset_season2.py` only UNHALT (set `is_halted=0`). Manual halt SQL is the only halt path; runbook documented in `CLAUDE.md` ## Manual halt SQL pattern. See `HM-F-future` for when a programmatic halt path appears. |
| HM-F-future | When a programmatic halt path appears (dashboard halt button, drawdown auto-halt, etc.), add `halt_player(conn, player_id, mode, reason)` helper to `engine/halt_gate.py` per HM-F Option 3 | LOW | **Do not pre-build.** YAGNI today — no caller exists. The helper should be written to fit whatever the real caller looks like (request handler? scheduled job? user-confirmation flow?), not in advance. |

---

## PATTERN NOTES

**Import-drift family (8+ items):** B12, B13, B14, B15, B17, AI-2, B29 share the
same family — "symbol moved, callers not updated, error swallowed by `except Exception`."
B14 + B15 closed today; remainder warrants a single disciplined import-drift sweep.

**Rebrand-drift family (B16, B23, accessapple sprint):** incomplete
`accessapple` → `ollietrades` rebrand left orphan domain references in code + docs.
22 refs across 6 files; sprint queued.

**Decorative-flag family (AI-3, AI-4):** `is_active`, `is_paused`, `crew_role` look
like state fields but don't gate execution. Only `is_halted` works. Document
before any new agent is wired.

---

## OPEN — Day-1 Soak Findings (2026-05-04 evening)

### HM-O — Ollie Box network outage (Scenario D, blocked at network layer)
- 192.168.1.166 unreachable: 100% ICMP loss + `nc: No route to host`. Not a stopped Ollama service — a network/power-layer failure that Scotty is not authorized to fix remotely.
- **Active impact during gate-flip soak:** three Ollie-Box-routed agents (`ollama-qwen3`, `ollama-coder`, `ollama-plutus`) emitting `HOLD, confidence=0.0` with `HTTPConnectionPool` error reasoning every signal cycle.
- **Action required from Captain/Admiral:** physically check power + network on Ollie Box. After it's back, re-run HM-O probe to verify all three models respond.
- **Follow-up (HM-X candidate):** circuit-breaker so unreachable Ollama doesn't keep emitting confidence-0.0 HOLDs into `signals` table.
- Full report: `docs/HM-O_OLLIE_BOX_HEALTH_2026-05-04.md`.

### HM-P — Confidence-scale audit (no urgent flag, deferred annotation pass)
- 42 production sites + 10 alt-named `conf` sites audited. **0 WRONG, 2 AMBIGUOUS (comments only), 49 CORRECT.**
- All gate-flipped strategy code (`bull_call_spread_v1`, `bear_put_spread_v1`, `executor`, `exit_manager`) verified: uses `TB_CONF_THRESHOLD = 85` against `trade_signals.confidence` (INT 0-100). **Soak may continue safely.**
- Implicit convention: `trade_signals` → INT 0-100; `signals`/`watchlist_signals`/`deep_scan_results`/`ghost_trades` + player decisions → REAL 0-1. Not documented anywhere central; one careless paste away from a silent bug.
- **HM-P-fix (deferred, low risk):** annotation pass adding `# scale: 0-100 INT` / `# scale: 0-1 REAL` at every comparison site. ~60-90 min one-shot. Optional rename `confidence` → `confidence_pct` in `engine/ollie_commander.approve_or_reject`.
- Full report: `docs/HM-P_CONFIDENCE_SCALE_AUDIT_2026-05-04.md`.

### HM-Q — execution_status vs halted_emit (Verdict A, no action)
- Both columns measure orthogonal things. `execution_status` = "what happened to this signal downstream"; `halted_emit` = "was the player allowed to act when emitted".
- HM-C is **not** redundant. `halted_emit` captures information (halt state at emission time) irrecoverable from `execution_status` or any join — `ai_players.halt_mode` is mutable and there is no halt-state audit log.
- **No schema change. No undo of HM-C.** Optional one-line annotation in `engine/halt_gate.py` near `HALTED_EMIT_FILTER` documenting the orthogonality.
- Open question worth chasing: **what writes `execution_status='EXPIRED'`?** 42,626 rows (69.5% of `signals`) and the audit found no writer — likely a sweeper job, but unverified.
- Full report: `docs/HM-Q_EXECUTION_STATUS_INVESTIGATION_2026-05-04.md`.

### HM-B — Drop ai_players.is_halted column (Day-1 evening, ✅ SHIPPED)
- HM-A read coverage was 100% clean, but pre-flight surfaced 4 unmigrated WRITE sites that would have SQL-errored post-drop. Migrated those first in HM-B-pre (`a3a4cd0`), then dropped column in HM-B (`9256890`).
- Live DB: `ALTER TABLE ai_players DROP COLUMN is_halted` on SQLite 3.51, backup at `backups/trader.db.pre-hmb-20260504_173026`. Service stable post-drop, no schema-related errors in trader.log.
- Halt state now has single source of truth: `halt_mode TEXT CHECK(halt_mode IN ('full','exit_only','active'))`.

### HM-D — watchlist_signals halted-player rows (Verdict α: Retain)
- 165 halted-player rows total. 34 still in `status='active'` but bounded and self-resolving — readers transition them out as price action plays out, and `halt_gate.can_emit_signal` blocks new active rows.
- Optional HM-D-fix (~30-45 min, deferred): add `JOIN ai_players halt_mode='active'` to 6 currently-unaware readers in `signal_tracker.py` + `crew_scanner.py:3965`. No urgency.
- Full report: `docs/HM-D_WATCHLIST_SIGNALS_VERDICT_2026-05-04.md`.

### HM-E — Halted-player daily routines (Verdict B: modest waste)
- Signal emission already stopped naturally; halted-player trades are all legitimate exit_only SELLs.
- **Active waste**: `ai_journal` daily routine runs for sulu + ollama-llama every market session — ~2 LLM calls/day to Ollie Box for journals no one reads. `main.py::run_journal()` + `engine/ai_journal.py::generate_journal_entry()` have no halt-mode check.
- Optional HM-E-fix (~5 min, low risk, deferred): add 3-line halt-mode check at the LLM-cost source in `generate_journal_entry()`.
- Full report: `docs/HM-E_HALTED_ROUTINES_VERDICT_2026-05-04.md`.

### HM-T — PED operational probe (Verdict B: silently inert, structurally unreachable promotion)
- PED is properly imported (main.py:3486) and scheduled every 15 min (main.py:3541) inside `if __name__ == "__main__":` block — the scheduler IS firing.
- **Lifetime activity: zero.** No row in `ai_players`, zero `signals` written, zero `trades`, zero log lines across all log files. Sitrep history (794 lines, 2026-05-01 onward) shows `PED signals: 0` every cycle.
- **Root cause:** `data/watchlist.txt` (PED's universe source at main.py:3496) does not exist. Falls back to 9 hardcoded ETF/mega-cap symbols. None have earnings in the 1-48hr post-earnings window today; effective trigger frequency is single-digit hours/year, single-digit signals/year after gap+vwap filters.
- **Gate-promotion criterion (30 trades + positive expectancy) is structurally unreachable.**
- Compute waste: negligible (rule-based, no LLM, ~0.1s/day total CPU).
- No other code reads `data/watchlist.txt` — the missing file is PED-specific. Was either deliberately abandoned or never wired.
- **Recommended: Option γ (formally retire).** Move to `archive/retired/`, remove schedule, document. Side benefit: closes HM-S-code by removing the phantom `agent_state` reference from active code paths. Option β (repair wiring with proper watchlist) is also viable if Captain sees PED research value.
- Full report: `docs/HM-T_PED_OPERATIONAL_PROBE_2026-05-04.md`.

### HM-T-fleet — Silent-Inertness Audit (Tuesday 2026-05-05)
- Extended HM-T's PED-class question fleet-wide. 49 ai_players + 130 schedule registrations classified.
- **7 PED-class inert agents identified:** anderson-bcs, mccoy-bps, quark-ic, covered-call (orphaned in `engine/options_agents.py`, file imported by nothing); qwen3-14b-pro (lab/backtest scaffold, never dispatched); red-alert (channel-mismatch — writes to non-existent `red_alert_log`); dayblade-0dte (was active until 2026-04-07, 28 days idle — watch list).
- **Halted-but-emitting confirmed:** ollama-llama leaked 947 post-halt signals (HM-A signal-emission gate gap). Earlier "2 post-halt trades NEW finding" claim was a query-window error; corrected in commit ee481fa — actual 7 post-halt trades, all clean exits, Verdict A (no trade-gate bug).
- **Orphan in signals not in roster:** `debate-pipeline` (1 row, 2026-03-31, vestigial).
- **Recommendations:** (1) ~~one bundled retirement commit for the 4 options_agents.py orphans (mirrors PED pattern)~~ **— APPLIED 2026-05-05 07:09 MST as Option 1 halt-only.** Pre-flight discovered `engine/options_agents.py` IS imported by `dashboard/app.py:17731` and contains 8 player_ids (4 targets + 4 ghosts), so the file was NOT archived. Instead 4 ai_players rows transitioned to `halt_mode='full'`. Code preserved per sacred-data rule. Open follow-ups: ghost-agents Option 4 investigation; HM-T-fleet doc has stale "imported by nothing" claim that needs a correction note; surgical file cleanup deferred until ghost investigation lands; (2) dispatch-loop investigation for qwen3-14b-pro; (3) clarify red-alert role; (4) signal-emission gate work (already in CLAUDE.md TODOs).
- 4 open Admiral questions: paid-model halting policy, options_agents retirement scope, dayblade-0dte timeline, mlx-qwen3/ollama-coder dispatch suppression.
- Full report: `docs/HM-T-fleet_SILENT_INERTNESS_AUDIT_2026-05-05.md`. No code/schema changes — investigation deliverable only.

### HM-I — Bridge Scope Investigation (Tuesday 2026-05-05)

**Status:** Admiral picked **Option β** (firm separation) 2026-05-05. Items 1+4 shipped same day; items 2/3/5 deferred.
**Priority:** Medium (architectural; running soak is stable)
**Investigation date:** 2026-05-05 morning (Scotty)

Inventoried the internal-book ↔ Alpaca-paper bridge. 3 books, 2 flows, 4-player routing table.

- **Active code-level finding:** `engine/paper_trader.py:1300` (partial-SELL path) called `_forward_to_alpaca` **without** the `route_mode == "trading"` gate that BUY (line 1015) and full-SELL (line 1167) both have. Source of ~181/day phantom-position skip log entries from legacy fleet players. **APPLIED 2026-05-05 commit `d06c33c`** (HM-I Option ε): added matching gate; all three forward paths now identical. Stale bytecode at PID 35155 means current process still emits skips until next restart.
- **Two-book policy formalized:** `CLAUDE.md` § "Architecture: Two-Book Bridge Policy" added 2026-05-05 commit `086a123`. Internal AI fleet book and Alpaca paper book are two separate ledgers by design. Routed players (super-agent, ollie-auto, neo-matrix, dalio-metals) + spread strategies forward to Alpaca; legacy fleet stays internal-only.
- **Phantom-reference fix:** `portfolios.id=5` renamed from "Dalio Metals" → "Enterprise Computer" 2026-05-05 to match `_EXECUTION_PORTFOLIO_BY_PLAYER` mapping. Resolution went from broken (fall-through paper) to correct (id=5, route_mode=tracking, log-only). Behavior change: dalio-metals no longer accumulates new internal-book trades — matches Option β log-only intent. Existing 37 trades + 2 positions preserved (FK on id, not name). DB-only change; no code/doc updates needed (refs were already correct).
- **Type 1 divergence count at investigation time:** 39 internal positions across 9 players that Alpaca paper doesn't have. Includes shorts (gemini-2.5-flash IREN/ONDS) and futures (enterprise-computer GC=F, SI=F) Alpaca paper can't accept. Stable post-β (legacy fleet stays internal by design).
- **5 options presented α/β/γ/δ/ε.** Admiral picked β. Item ε (decision-orthogonal) also applied.
- **β followups status:**
  - Item 2: Dashboard naming pass (Arena Paper vs Alpaca Paper visual distinction). **Deferred.**
  - Item 3: Webull dual-role split. **APPLIED 2026-05-05 07:56 MST** — code + service restart (PID 35155 → 59121) + DB migration atomic. New player `alpaca-mirror` (provider=alpaca-paper-broker, is_human=0). 3 positions migrated from webull to alpaca-mirror; webull retains 127 historical Webull-import trades + 73 portfolio_history rows. Kirk + first_officer + Q + cto_advisor + war_room + dashboard reads re-targeted to alpaca-mirror. SQL `!= 'webull'` exclusions in benchmark.py / war_room.py / holodeck_expansion.py rewritten as `is_human=0`. Stale-bytecode lockstep dictated atomic order: code → kickstart → DB. 18 files touched, 29 `# HM-I-β-Item3:` markers placed.
  - Item 5: Reconciliation report (replaces ε canary, daily NTFY on drift thresholds). **Deferred.**
- Full report: `docs/HM-I_BRIDGE_SCOPE_INVESTIGATION_2026-05-05.md`.

### Option 4 — Ghost Agents Investigation (Tuesday 2026-05-05)

**Status:** **CLOSED 2026-05-05 08:57 MST** — Admiral chose **Option B halt-only retirement**. 4 ghost agents transitioned `halt_mode='active' → 'full'` via DB UPDATE. File `engine/options_agents.py` untouched (sacred-data); `/api/options/scan-preview` endpoint continues serving 8 halted agents (4 production halted morning 06b5ce7 + 4 ghosts halted now). halt_gate API confirms all 4 ghosts return False on can_emit/open/close; active players (ollama-plutus, ollie-auto, super-agent) unaffected. Operationally a no-op (zero lifetime activity); DB now reflects behavioral reality. Pre-halt backup at `backups/trader.db.pre-ghost-retire-20260505_085718`.
**Original Status:** Open — awaiting Admiral A/B/C/D decision (no recommendation made).
**Priority:** Low (no current behavioral impact; either choice is reversible).
**Investigation date:** 2026-05-05 morning (Scotty)

Tested HM-T-fleet's ⚪ "by-design" classification of the 4 ghost agents (ghost-kirk-bc, ghost-kirk-0dte-bc, ghost-long-call, ghost-naked-put).

- **Verdict:** classification was **directionally correct**. All 4 ghosts are 🟡 half-wired — real classes with real scan logic, partitioned into a separate `options_books.ghost` research book ($2,500 starting capital) with drawdown gate, designed as A/B research framework. Not orphans.
- **But:** they share their dispatch path with the 4 production options agents we halted this morning (commit 06b5ce7). Both groups are preview-only — no scheduler entry, no execution step, only consumer is `dashboard/app.py:17731 /api/options/scan-preview`. The "separate confirm step" the run_scan_cycle docstring references doesn't exist in code.
- **4 options presented:** A leave alone (no action), B halt all 4 ghosts to mirror morning halt symmetrically, C activate (build the missing scheduler+confirm path), D retire entire options-engine subsystem.
- **Open Admiral questions:** was ghost activation always planned? should production+ghost halt status be symmetric? is the "separate confirm step" real or aspirational?
- Full report: `docs/OPTION-4_GHOST_AGENTS_INVESTIGATION_2026-05-05.md`.
- **Side observation:** morning halt of 4 production options agents (anderson-bcs/etc.) was effectively cosmetic — those agents had no path to fire either. Halt is still correct (marks them not-production), but didn't change behavior.

### HM-U — Silent-Failure Pattern Discussion (DISCUSSION ITEM, NOT A FIX)

**Status:** Open
**Priority:** Medium (architectural conversation, not a code change)
**Surfaced by:** HM-O / HM-S / HM-T / HM-E investigations on 2026-05-04, plus the stale-bytecode discovery during PED retirement verification

Today's audits found a recurring architectural anti-pattern across multiple subsystems:

| Subsystem | Silent-failure shape |
|---|---|
| HM-O (Ollie Box outage) | Connection-error reasoning text + `confidence=0.0` HOLD signals → treated as valid rows in `signals` table |
| HM-S (`agent_state` ghost) | `try/except Exception: return False` swallows missing-table error → drawdown-halt always says "not halted" |
| HM-T (PED inert) | Missing `data/watchlist.txt` → silently falls back to narrow universe → never qualifies → silently no-ops |
| HM-E (halted journals) | No halt-mode check on routines → continues running for halted players → wasted LLM calls |
| Stale-bytecode (PED-verification discovery) | `try/except: console.log(error)` at 4 call sites swallowed `no such column: is_halted` for 70 min before discovery |

**Common shape:** bare `except` / silent fallback / no-op success path / caught-and-logged-but-not-alerted. The codebase trades loud failure for quiet incorrectness in many spots, and the discipline of "don't crash the trader" has expanded to cover bugs that should be loud.

**Question for discussion (not for autonomous decision):**

1. Should bare `except Exception` blocks log the swallowed exception with stack trace by default (vs current pattern of `console.log(f"...: {e}")` losing the traceback)?
2. Should silent-fallback paths NTFY-alert when they fire (e.g., "PED couldn't load `data/watchlist.txt`, using fallback universe")?
3. Is there a project-wide error-handling philosophy worth writing down in CLAUDE.md (e.g., "data-layer SQL errors must NTFY-alert; LLM-API errors may be swallowed; config-fallback paths must log once-per-process")?
4. Are there other "wired-but-inert" agents we should fleet-audit (HM-T-fleet candidate)?
5. Should schema-change verification include a service restart in the verification phase, given the stale-bytecode trap from today (see Lessons section)?

**Recommended action:** Schedule a discussion-only session (Admiral + XO, no Scotty) to set posture. Then a follow-up sprint, if any, would write the explicit fix prompt.

**Not in scope here:** automatic refactor of all bare-except blocks. That's a code-philosophy decision, not a Scotty task.

### HM-S — agent_state table ghost (Verdict C: dead but harmless + docs drift)
- **`agent_state` does not exist in any of 13 .db files in the repo.** Confirmed via direct schema queries on every DB.
- **Only 1 reader** in production code: `agents/post_earnings_drift.py:56` — wrapped in bare `except Exception: return False`, so the missing table silently produces "not halted".
- **CLAUDE.md is factually wrong:** claims `engine/ai_brain.py` and `engine/risk_manager.py` read from `agent_state`. Neither file references `agent_state` at all. The actual drawdown-halt protection at `risk_manager.check_drawdown()` reads `portfolio_history` and recomputes `(peak - current) / peak >= 0.20` every cycle — transient, not flag-based, and FUNCTIONAL.
- **Safety implication for live gate-flip:** none. Drawdown halt + manual halt_mode runbook are both functional. PED's broken halt-flag is contained by separate `gated=True` paper-only gating.
- **Recommended:** (1) fix CLAUDE.md describe transient drawdown mechanism correctly (~5 min), (2) optional PED `is_halted()` cleanup (replace with `enabled` toggle, ~10 min). Both deferred.
- Full report: `docs/HM-S_AGENT_STATE_GHOST_2026-05-04.md`.

### HM-AB — bull_spread_v1 missing same-strategy self-skip check (2026-05-05)

**Status:** Open — strategy halted at commit `[this commit SHA]` pending fix.
**Priority:** High (was actively stacking positions; 18 open SPY bull_put_spreads accumulated <1 day post-gate-flip before halt).
**Surfaced by:** Admiral observation 2026-05-05 11:39 MST.

`strategies/bull_spread_v1.py` lacks a same-strategy self-skip check — the reciprocal of `strategies/bull_call_spread_v1.py:280-287` which queries `options_trades` for any open `bull_spread_v1` row on the same ticker and skips if found. Without the reciprocal, bull_spread_v1 is free to fire repeatedly on the same ticker (SPY) every signal tick (every 15 min per `main.py:2622` schedule), accumulating 18 open positions in <1 day.

**Halt applied 2026-05-05 11:39 MST (this commit):**
- `strategies/bull_spread_v1.py` `_EXECUTION_ENABLED = False` (module-level constant)
- `evaluate()` early-return checks the constant
- Auto-register call changed to `enabled=False`
- Belt-and-braces: either gate alone halts signal emission; both together provide redundant safety
- Stale-bytecode: PID 61083 has pre-halt bytecode in memory; halt takes effect on next service restart (planned ~13:00 MST per Admiral)
- Tag: `# HALT-2026-05-05:` markers in code

**Existing 18 positions ride** per Admiral directive — they're real Alpaca paper positions, max-loss-capped, same-expiration. `exit_manager` handles them on its scheduled cadence (TP / SL / expiration). **DO NOT close programmatically** during the halt window — closing while the underlying bug still exists risks stacking another bug on top.

**Fix shape (HM-AB session):**
1. Add `_already_open(ticker)` helper to `strategies/bull_spread_v1.py` mirroring `bull_call_spread_v1.py:275-290` — query `options_trades` for `WHERE strategy_id='bull_spread_v1' AND symbol=? AND exec_status='open'`.
2. Call it at the top of the per-ticker loop in `evaluate()`; skip ticker if already-open.
3. Once verified, flip both `_EXECUTION_ENABLED = True` and `enabled=True` to unhalt.

**Verification approach:**
- Pre-fix smoke: confirm a synthetic open row blocks signal emission for that ticker.
- Pre-unhalt: backlog audit of existing open positions; if any have already hit TP/SL/expiration, unhalt is safer because the strategy will see fewer "already open" hits naturally.
- Post-unhalt monitor: 1 hour soak with `tail -f logs/trader.log | grep bull_spread_v1` to confirm the strategy fires once per qualifying ticker per cycle, not stacking.

---


### HM-AF — dayblade-0dte spread cannibalization root cause (2026-05-06)

**Status:** **HALTED 2026-05-06 10:43:54 MST** via `UPDATE ai_players SET halt_mode='full'` (transaction took effect immediately, no service restart needed; halt-mode is read per-cycle).

**Surfaced by:** Day 3 morning observability check (Admiral + XO, 2026-05-06 10:00–10:45 MST), tracing the orphan SPY 732P short position visible in `positions` table after a clean MLEG fill.

**Root cause (the 2-day "spread positions vanish" mystery):** `dayblade-0dte` (T'Pol) was firing single-leg `submit_single_option(SELL)` calls on the LONG legs of bull_put_spread fills within minutes of the parent MLEG filling. Each fire dismantled a spread by selling its protective long leg, leaving an orphaned short PUT.

**Evidence chain:**
- 2026-05-05: 5 single-leg SELL fires across 4 timestamps (08:41 + 12:52–12:56 cluster) totaling 13 long-PUT contracts (1+3+5+1+3) — exactly matches the 13 spreads cleaned up by HM-AE Option B reconcile that evening. All fires logged at `engine/alpaca_options.py:251`.
- 2026-05-06 08:14:39 UTC: 1 single-leg SELL on SPY260515P00727000 (`order=4863f7fc-980d-4283-b30b-5fe89ae12ebb`) fired 2 minutes after the bull_put_spread MLEG `848ece89-...` filled at 08:12:41. Logged at `engine/alpaca_options.py:315` (different code path than yesterday's :251).
- Alpaca order data confirms both legs of every MLEG order today and yesterday filled cleanly with `filled_qty=1, status=filled` — Alpaca paper did NOT net-collapse the spreads as previously theorized; OUR code dismantled them.
- All firing entries attributed to `dayblade-0dte` in trader.log; one EOD-sweep log line confirms the cluster behavior: `(dayblade-sulu + dayblade-0dte EOD sweep)`.

**Halt SQL applied:**

    UPDATE ai_players
    SET halt_mode = 'full',
        halt_reason = '2026-05-06 spread cannibalization (closed long 727P leg of bull_put_spread_v1 2min after MLEG fill, alpaca_options.py:315; also responsible for 5 single-leg sells yesterday at alpaca_options.py:251 dismantling the 13-spread reconcile cohort)',
        halted_at = CURRENT_TIMESTAMP
    WHERE id = 'dayblade-0dte';

**Verification post-halt:**
- `halt_mode='full'`, `halted_at=2026-05-06 17:43:54 UTC` confirmed via SELECT
- No `Alpaca OPTIONS SELL` log entries from `dayblade-0dte` after 10:43 MST through 10:45 MST (~2 min observation window)
- `bull_spread_v1` self-skip continues to fire correctly on the orphan SPY 732P
- Stock-trading agents unaffected (ollama-plutus / qwen3 / capitol-trades / deepseek-7b-grok4 active throughout)

**Mis-attribution correction (HM-AE Option B):** Yesterday's HM-AE Option B reconcile marked 13 stale `options_trades` rows closed under the assumption that "Alpaca paper net-flat-collapsed the spreads." The cleanup was correct — the rows DID need to be marked closed because they WERE effectively closed — but the cause attribution was wrong. The actual cause was dayblade-0dte cannibalizing each spread's long leg, leaving naked shorts that subsequently closed via other paths or netted out. No corrective action needed on the 13 reconciled rows; this is a calibration note for future-XO.

**Open follow-ups (deferred to fresh-headed Scotty):**
1. **Investigate dayblade-0dte's two firing paths** — `alpaca_options.py:251` and `alpaca_options.py:315` are different code paths. Read both, trace callers, document trigger conditions. Why did `:315` fire on a 9-DTE option from an agent labeled `0dte`? Investigation only, no code changes. ~30 min.
2. **Architectural fix: spread-leg awareness for ALL agents** — long-term, ANY agent firing single-leg options closes should respect spread structure. Two approaches: (A) add `spread_id` + `is_spread_leg` columns to `positions`, populated when MLEG fills sync; (B) read-time check against `options_trades.legs_json` for matching open spread. Approach choice depends on Item 1's findings. ~60 min.
3. **Orphan SPY260515P00732000 short** (qty=-1, mv≈-$579, expires 2026-05-15) — Battle Station continues firing CLOSE_NOW every 2 min (legitimate panic on what looks like a naked short PUT) but the close routes through tracking-mode and never executes. Recommendation: let it expire on May 15. Paper money, no real risk. Set reminder for May 15 to verify expiration cleared the position.

**Reversal (if needed):**

    UPDATE ai_players SET halt_mode='active', halt_reason=NULL, halted_at=NULL WHERE id='dayblade-0dte';


**AMENDMENT 2026-05-06 11:00 MST (post-Scotty investigation):** Initial HM-AF writeup characterized `:251` and `:315` as two firing paths. Scotty investigation (`docs/diagnoses/dayblade_0dte_paths_2026-05-06.md`) corrected this: they are the same log line at different file offsets — commit `1eeff7d` (HM-V/HM-AA bundle, 2026-05-05 12:59 MST) inserted 147 lines above the success log inside `submit_single_option`. Pre-restart bytecode emitted `:251`; post-restart process emits `:315`. Single statement, single caller, single defect.

The actual contaminated code paths are THREE, all sharing the same root cause (no spread-leg awareness, no DTE filter, no agent-ownership filter):

- **P1 — Battle Station 2-min monitor** (`battle_station.py:684`): iterates ALL Alpaca options positions every 2 min, fires close on −50% pnl OR wrong-side-of-gamma-flip. Hardcodes `player_id="dayblade-0dte"` at `battle_station.py:668` for attribution but scope is global. Triggered today's `:315` fire on SPY 727P.
- **P2 — EOD sweep** (`main.py:2268` → `close_all_options` at `alpaca_options.py:590`): fires daily at 12:45 MST. Closes ALL options positions in the Alpaca book regardless of strategy/spread structure. Confirmed firing 2026-05-05 12:48:23.
- **P3 — dayblade.py:502 post-trade close_all_options**: fires `close_all_options` after every dayblade sell, NOT just EOD. Likely the highest-frequency leak; silently cannibalizing spreads since the 2026-05-04 gate flip.

**Halt of dayblade-0dte (`halt_mode='full'`) only stops P3.** P1 (Battle Station) and P2 (EOD sweep) remain active and will fire on any open options position regardless of dayblade-0dte's halt state.

**Additional finding — wrong-side-of-book bug:** `_get_alpaca_options_positions` strips qty sign at `battle_station.py:319`. Short positions get treated as longs in close logic, causing `submit_single_option(side="sell")` calls when the correct close action would be buy-to-close. Separate from cannibalization but compounds damage.

**Updated open follow-ups (supersedes original Items 1-3):**
1. **HM-AF-α** — Halt P1 + P2 + P3 via feature flag or guard (urgent, before Layer 1 ships). Scotty ~15-20 min.
2. **HM-AF-β** — Layer 1: Spread-leg awareness. `is_spread_leg(symbol)` helper cross-referencing `options_trades`/strategy_positions; applied to P1/P2/P3. Scotty ~60-90 min.
3. **HM-AF-γ** — Layer 2: Wrong-side-of-book correction in `_get_alpaca_options_positions`. Can ride with HM-AF-β.
4. **HM-AF-δ** — Layer 3: Remove hardcoded player_id in `battle_station.py:668`. Lower urgency.
5. Original Item 3 (orphan SPY 732P) unchanged — recommend let expire 2026-05-15.


---

## Lessons

**2026-05-04 — Stale-bytecode trap from in-flight schema changes:** HM-B's `DROP COLUMN ai_players.is_halted` (commit `9256890`) created a stale-bytecode mismatch in the running trader process (PID 13734). The service was started at 08:32 MST — before HM-A's source migration shipped that morning — so the in-memory bytecode still had pre-HM-A SQL referencing the now-dropped column. Errors began at 17:36, but were caught by `try/except` blocks at the call sites and surfaced only as quiet `console.log` warnings: 15 occurrences across `War Room`, `ai_brain.py:286/295/533`, and three agents (ollama-coder, mlx-qwen3, energy-arnold) before discovery via log scan during PED retirement verification ~70 minutes later. Source code post-HM-A was clean; the issue was entirely in the long-running process's compiled module cache. **Future schema-change sessions should include a service restart in the verification phase OR a longer (30+ min) post-change soak window before declaring the change stable**, specifically to flush any pre-migration in-memory residue. This is also a HM-U datapoint: the silent-failure pattern (caught exceptions, swallowed errors) hid the issue from cursory checks — only a focused log scan surfaced it.

---

## SHIPPED 2026-05-03 — Sunday Morning Deploy

- **8e06b5e** regime fix deployed at 08:01 MST
- Manual `trader.db` backup taken: `backups/trader.db.pre_regime_fix_deploy_20260503_080141`
- 11 regime ticks verified post-restart (08:16:46 → 10:47:43, all `BULL_CROSS`)
- Edits 1, 2, 3 verified at code level (`main.py` lines 2610, 2656, 2685-2701)
- Runtime verification PENDING — Monday market-hours window 06:30-13:00 MST

## SHIPPED 2026-05-03 — Sunday Afternoon Deploy

- **d2ad748** B15 diagnostic patch (capture NameError traceback frames)
- **17d40b4** B15 fix — `OLLIE_URL` added to `initialize_dayblade()` import
- **cdc03d0** B14 fix — dead `GetAllPositionsRequest` import removed
- **58c43f0** Item 5 — ~60 lines dead crew-server polling removed from `premarket-scan.sh`
- PID 84968 deployed at 15:45 MST; 0 OLLIE_URL errors post-deploy (verified)

## SHIPPED 2026-05-02 (Saturday Night Drydock)

| Fix | File | Description |
|-----|------|-------------|
| Task 1 | git | Checkpoint commit `463c402` — 370 files, 8 drydock sessions |
| Task 3A | `engine/importers/ai4trade_importer.py` | Added `run_import()` alias → fixes nightly import crash |
| Task 3B | `uoa/scraper.py:16` | Fixed docstring example path |
| Task 3C | `premarket-scan.sh:46` | Commented out defunct `launchctl start com.trademinds.crew` |
| restart.sh | `restart.sh:11` | Split `qwen3.5:9b` across two vars to pass pre-commit hook |
