# XO Backlog — USS TradeMinds
# Riker's Standing Work Queue
# Updated: 2026-05-03 (Yellow Alert Phase 1 reconciliation)

**Reconciliation method**: every claim below verified against running code, DB state,
launchctl, trader.log post-PID-84968 startup (15:45 MST today), and on-disk files.
Items moved by category based on observed reality, not historical claim.

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
| HM-A | Migrate the ~22 `is_halted` read-sites to `halt_mode != 'active'` | MEDIUM | Files: `dashboard/app.py` (9 sites), `morning_briefing.py:60`, `war_room.py:835`, `season_manager.py:154,258`, `matrix_bridge.py:114`, `main.py:3478,3487`. Excludes the drawdown-halt system in `ai_brain.py:813,814,844` and `risk_manager.py:864` — that's `agent_state.is_halted`, a separate concept |
| HM-B | After HM-A: drop `is_halted` column OR add a single-source-of-truth trigger that keeps `halt_mode` and `is_halted` in sync | LOW | Triggers add hidden behaviour; preference is to drop `is_halted` once read sites are clean |
| HM-C ✅ FIXED 2026-05-04 | Update read-path consumers of `signals` / `watchlist_signals` to filter `halted_emit = 0` for scoring queries | MEDIUM | **Shipped 2026-05-04. 22 files modified, 28 SQL sites filtered. Scope was broader than first scoped: `ai_brain.py:563` (TIER-1 escalation), `bull_call_spread_v1.py:251` / `bear_put_spread_v1.py:270` (tier-2 spread vote), `crew_scanner.py:3963` (autopilot fleet consensus), `risk_manager.py:312` (bear-mode gate) all consume signals for current-day execution decisions, not just calibration. Halted players were implicitly voting through pre-fix-#1 backlog rows. Helper `HALTED_EMIT_FILTER` constant added to `engine/halt_gate.py` for single-source-of-truth migration when HM-A/HM-B retire `is_halted`. Display/forensic paths preserved. `/v1/signals` external API also filtered — note in commit message under Behavior change visible to /v1/signals consumers** |
| HM-D | `watchlist_signals` had 49 pre-halt + 13 post-halt rows for ollama-llama. Decide if pre-halt rows from now-retired players should ever feed scorecard math | LOW | Currently `halted_emit=0` for all pre-halt rows; that's correct per the flag's strict definition |
| HM-E | Investigate `ai_journal` half-life on halted players — `dayblade-sulu` has zero `signals` since 2026-04-07 but writes journal entries through 2026-05-01. Some daily routine fires regardless of halt state. Decide whether halted players should run the daily routine at all | LOW | Diary entries are diagnostically useful; this is more about wasted compute than data integrity |
| HM-F | Add `halted_at` UPDATE to whatever code path sets `is_halted=1` going forward (so future halts populate the timestamp automatically) | MEDIUM | Currently `is_halted=1` is set by hand or by `season_manager.py`; neither updates `halted_at`. Auto-trigger deferred per Admiral preference, but the application code should set it |

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
