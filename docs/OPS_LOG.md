# OllieTrades Ops Log
Automated audit trail for DB operations.

## Backfilled (2026-05-02 audit forensics)
- 2026-04-30T17:01:06 | schwab_sync | backup=trader.db.pre_schwab_sync_20260430_170106 | unknown bytes (CSV import — 0 new rows written, schwab_holdings unchanged at 38)
- 2026-04-30T17:35:25 | learning_cycle | backup=trader.db.pre_learning_20260430_173525 | unknown bytes (nightly daily_lessons run — 18 rows added, kirk_advisory_log +4)

## Live entries
- 2026-05-03T06:00:03 | daily_backup | backup=trader_2026-05-03.db | 225336KB

## 2026-05-03 ~20:05 MST — Production deploy: tier-2 fix + 14 pending commits

Commit deployed: 721b2fa (fix: tier-2 spread tiebreaker — migrate strategy_signals → signals; add missing persist)
Plus 13 prior commits ahead of origin from Saturday + today's work.

Pre-restart state:
- PID 84968, uptime ~4h 17m, B15 fix verified holding (0 OLLIE_URL errors post-startup)
- Backup: backups/trader.db.pre_tier2_deploy_20260503_200351 (220.4 MB)
- All 3 _EXECUTION_ENABLED gates False (executor.py:22, bull_call:63, bear_put:63)

Restart action: launchctl kickstart -k gui/$(id -u)/com.trademinds.trader

Expected post-restart:
- New PID assigned
- All 14+ commits effective in production (tier-2 + Saturday/Sunday backlog)
- B15 fix continues holding (zero OLLIE_URL errors expected)
- tier-2 tiebreaker now reads signals table (verification Monday market hours)

Rollback if needed:
- git revert HEAD (reverts tier-2 commit 721b2fa)
- launchctl kickstart -k gui/$(id -u)/com.trademinds.trader (restart on reverted code)

Refs: /tmp/scotty_session_2026-05-03/OPTION_A_DEPLOY_DIRECTIVE.md
Refs: /tmp/scotty_session_2026-05-03/tier2_landmine_fix_proposal.md (Section I, Admiral verdicts)
- 2026-05-05T06:00:05 | daily_backup | backup=trader_2026-05-05.db | 234968KB
- 2026-05-06T06:00:02 | daily_backup | backup=trader_2026-05-06.db | 242832KB
- 2026-05-07T06:00:06 | daily_backup | backup=trader_2026-05-07.db | 251812KB
- 2026-05-07: HM-AO closed as already-shipped — bug fixed in 86bb32b (Apr 24). Same-class bug pivoted to HM-AO-β (scripts/ollie_backtest_*.py).
- 2026-05-07 09:30: HM-AS diagnosed. battle_station_monitor cadence median 2:01 (on target); p95 5:07; tail driven by single-threaded schedule.run_pending() blocking on slow jobs. Architectural, not bug. 80% fire-rate recovery preserves α-lift evidence integrity. HM-AS-β (10-min observability log when interval >180s) queued for post-soak.

## 2026-05-07 10:00 — HM-AT TCC diagnosis + manual GUI fix path

The Schwab CSV watcher (`com.ollietrades.schwab-watcher`) appeared dormant 2026-05-06 and 2026-05-07. Initial theory was launchd fast-exit throttle: commit `e8b7f9e` (fix: HM-AT prevent launchd throttle in schwab_csv_watcher) added defensive `sleep 11` to script end.

**Revised diagnosis 2026-05-07**: `launchctl print gui/$(id -u)/com.ollietrades.schwab-watcher` confirmed `runs = 7`, `last exit code = 0` — the agent IS launching every 60s as designed. The real cause is **macOS TCC denying the launchd audit session access to `~/Downloads/`**. Manual runs (SSH/Terminal) inherit the Full Disk Access grant from the host app; launchd's audit session does not. The `nullglob` setting in `scripts/schwab_csv_watcher.sh` swallows the empty-glob expansion silently — every cycle exits clean with exit 0 and no log trace of the failure. Manual SSH-launched runs successfully processed all 6 backlogged CSVs (Apr 30 → May 7 06:16) during diagnosis, archive count 2 → 13.

**Manual fix path** (Admiral, GUI step):

1. Open System Settings → Privacy & Security → Full Disk Access.
2. Toggle ON for `/bin/bash` (use `+` to add if not listed; navigate to `/bin/bash`).
3. Reload the agent:
   ```
   launchctl bootout gui/$(id -u)/com.ollietrades.schwab-watcher
   launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.ollietrades.schwab-watcher.plist
   ```
4. Verify within 90s by dropping a Schwab CSV in `~/Downloads/` and confirming it lands in `data/schwab_csv_archive/` and an entry appears in `logs/schwab_watcher.log`.

**Recovery on macOS update or TCC reset**: same GUI step. Document any TCC reset events here for pattern visibility.

**Defense-in-depth note**: `e8b7f9e` (`sleep 11`) is retained — harmless overhead and protects against the throttle theory if it ever becomes a compounding factor.

**Backlog**: HM-AT-β tracks migration of watcher to `~/autonomous-trader/inbox/` to eliminate TCC dependency entirely (post-soak).

## 2026-05-07 — HM-AT-β shipped: watcher migrated off ~/Downloads/ to ~/autonomous-trader/inbox/

Ship reason: GUI fix path for HM-AT (Full Disk Access grant) is unavailable — bigmac is a headless Mac Mini M4 with SSH-only access. HM-AT-β escalated from post-soak to immediate.

Changes:
- `scripts/schwab_csv_watcher.sh` — `WATCH_DIR` moved from `/Users/bigmac/Downloads` to `$HOME/autonomous-trader/inbox`
- `scripts/import_schwab_csv.py` — `DOWNLOADS` constant renamed to `INBOX`, repointed to `REPO_ROOT/inbox`; `--latest` glob and error message follow
- `docs/SCHEMA.md` — `schwab_holdings` table notes updated to reflect new watch dir
- `docs/XO_BACKLOG.md` — Schwab Workflow section updated with new path + scp command; HM-AT-β marked **SHIPPED**
- `.gitignore` — `inbox/*` ignored, `inbox/.gitkeep` tracked
- New empty dir: `~/autonomous-trader/inbox/.gitkeep`

**NEW WORKFLOW for Admiral** (PowerShell on Bonnie laptop, replaces browser-save-to-Downloads):
```
scp "C:\Users\Bonnie\Downloads\Sc*Position*.csv" bigmac@192.168.1.248:~/autonomous-trader/inbox/
```

The launchd watcher polls `inbox/` every 60s (StartInterval=60) and processes on the next tick. Verification: log entries land in `logs/schwab_watcher.log`; CSV moves to `data/schwab_csv_archive/`; NTFY fires to topic `ollietrades-admin`.

**Defense-in-depth retained**: `e8b7f9e` (`sleep 11`) stays — harmless overhead.

**Recovery**: `git revert <this-commit-sha>` + `launchctl bootout gui/$(id -u)/com.ollietrades.schwab-watcher && launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.ollietrades.schwab-watcher.plist`.

## 2026-05-07 — HM-AK fleet roster cleanup (12 dormant agents halted)

Diagnosis revealed 12 ai_players rows with `halt_mode='active' AND is_active=1` but **zero trades, signals (or fixed-pool 25-sig bootstrap), zero war_room posts** in the last 7 days. Two cohorts:

- **Paid-API zombies (6)** — should have been benched under Free-Models-First (CLAUDE.md 2026-04-13): `claude-haiku`, `claude-sonnet`, `gpt-4o`, `gpt-o3`, `grok-4`, `gemini-2.5-flash`. None of them showed any api_costs entries, signals, trades, or war_room activity over 7 days.
- **Dormant Ollama (6)** — scaffolded but never promoted, all show exactly 25 fixed-pool sigs and zero trades/posts: `qwen-coder-haiku`, `qwen3-14b-grok3`, `qwen3-8b-4o`, `qwen3-8b-o3`, `ollama-glm4`, `ollama-gemma27b`.

Side benefit: halting these 12 eliminates **all three duplicate display name conflicts** (`Lt. Cmdr. Worf` × 3, `Lt. Cmdr. Spock` × 2, `Qwen3 14B Pro` × 2) — the active iteration now contains only the canonical agent for each name.

**Action:** halt_mode UPDATE per CLAUDE.md halt SQL pattern.
- 11 with no open positions → `halt_mode='full'`
- `gemini-2.5-flash` had 2 open positions → `halt_mode='exit_only'` (allows close-out)

**Halt-mode breakdown:** before 37/9/4 (active/full/exit_only) → after **25/20/5** (12 moved). Total ai_players rows unchanged at 50.

**SQL artifact:** `migrations/HM-AK_dormant_cleanup_2026-05-07.sql` (committed for reproducibility + rollback).

**Rollback:**
```sql
UPDATE ai_players SET halt_mode='active', halted_at=NULL, halt_reason=NULL
 WHERE halt_reason LIKE 'HM-AK 2026-05-07%';
```

**Architectural note (deferred):** the active scan loops at `main.py:1991`, `engine/risk_radar.py:168`, `engine/autopilot.py:63` use `WHERE is_active=1` and do **not** filter by `halt_mode`. Per-trade halt gates downstream block actual execution, but the iteration loops still touch all halted rows. HM-AK does not address this — sized for HM-AK-β if/when prioritized.

**No service restart required** — halt_mode is read fresh per CLAUDE.md ("halt_mode is now the only working per-player kill switch", read by `engine/halt_gate.py` per request).

## 2026-05-07 — HM-AQ Captain decision: broaden WATCH_STOCKS to market-cap+volume universe

**Decision (Captain):** WATCH_STOCKS expands from 20 manually-curated mega-cap tickers to ~500-800 dynamically-refreshed tickers matching:
- Market cap ≥ $5B
- Daily $ volume (20-day avg) ≥ $50M
- Refresh cadence: weekly (Sunday pre-Monday-open)
- Refresh source: **Polygon screener API** (Polygon Options Starter $29/mo activation under HM-AQ-β; first paid exception under Free-Models-First doctrine, approved-in-principle 2026-04-16)

**Risks acknowledged by Captain:** dashboard noise, scan-loop slowdown across 12+ iteration sites (per HM-AU audit), more spread attempts on lower-liquidity options *only if* HM-AQ-γ ships.

**Catches:** all 6 missed movers from 2026-05-07 morning (DDOG +30.87%, FTNT +22.92%, MDB +14.19%, ZTS −21.37%, ARM −8.18%, TPR −8.14%) would have been in coverage under the new criteria.

**Scope split:**
- WATCH_STOCKS broadens (this decision).
- Spread-strategy universes (`TIER_1+TIER_2` in `bull_spread_v1.py` / `bull_call_spread_v1.py` / `bear_put_spread_v1.py`) stay at 10 tickers. Spread quality > coverage on illiquid options. Expansion deferred as **HM-AQ-γ** (marker, not active queue).

**Implementation queued as HM-AQ-β** (4-8 h Scotty):
- `engine/universe_refresh.py` (Polygon screener integration)
- launchd weekly refresh (`com.ollietrades.universe-refresh`)
- Storage migration (`config.py:WATCH_STOCKS` → DB table or refreshed file)
- Polygon Options Starter $29/mo activation (first paid exception under Free-Models-First)
- Audit/retest of 12+ dashboard iteration sites for rate-limit + latency impact
- Soak window before promoting to live

**Canonical reference:** `docs/UNIVERSE.md` (created in this commit).

**No code changes in this entry.** Doc-only Captain decision logging.

## 2026-05-07 — HM-AR audit: earnings_universe DEPRECATED + path map docs

Diagnosis surfaced that the original HM-AR ticket framed three independent earnings paths as a single system. The audit untangles them:

1. **Options blackout (LIVE, safety-critical)** — `engine/options_selector.py::_next_earnings_date` → `data/earnings_cache.json` + yfinance fallback. Independent of any SQLite table.
2. **`main.py:679 run_earnings_universe_inject()` (LIVE)** — daily 06:00 AZ scheduler. Despite the function name, writes to `scan_universe`, NOT `earnings_universe`. Naming-drift artifact.
3. **`engine/earnings_injector.py` + `earnings_universe` table (DEAD ORPHAN)** — writer + reader exist, but no external caller; docstring claims a 06:00 AZ schedule that was never wired. Table empty since creation.

**Classification:** DEPRECATED. No safety regression — path 1 (the safety-critical one) is intact and independent.

**Documented:**
- `docs/EARNINGS.md` (new) — three-path map, full audit findings
- `docs/SCHEMA.md` — `earnings_universe` row updated to point at audit
- `docs/XO_BACKLOG.md` — HM-AR marked AUDITED+DOCUMENTED, HM-AR-β cleanup ticket queued

**Recommended cleanup (HM-AR-β, ~15 min Scotty):** archive `engine/earnings_injector.py` → `archive/retired/2026-05-07-earnings-injector/`; rename `run_earnings_universe_inject` → `run_earnings_scan_inject` in `main.py` to fix the naming-drift lie. Leave the empty `earnings_universe` table in place (sacred-data rule).

**No code changes in this entry.** Doc-only audit.

## 2026-05-07 — HM-AV: ALPACA_*→APCA_* consolidation

**Context:** Memory said ~34 ALPACA_* sites; grep audit found **6**, all fallback chains in dead code. `.env` has only `APCA_*` keys (no `ALPACA_*` entries), so the `or os.getenv("ALPACA_*")` branches were reading unset env vars and falling through every call. The "back-compat preservation" intent baked into those fallbacks was already moot.

**Files (3 sites in 2 files):**
- `engine/premium_tracker.py:36-37` — `_headers()`, full fallback chain
- `dashboard/app.py:17099-17100` — healthcheck `CHECK 1: Alpaca creds present`, fallback chain (no third branch)
- `dashboard/app.py:17564-17565` — `_alpaca_options_headers()`, full fallback chain

**Action:** simplified each to canonical pattern matching the other 71 APCA_*-only sites:
```python
key    = os.getenv("APCA_API_KEY_ID", "")
secret = os.getenv("APCA_API_SECRET_KEY", "")
```

**Runtime behavior unchanged** — the simplified code reads the same env var that the prior fallback chain was reading first. The dead branches are gone.

**Post-HM-AV state (2026-05-07):** ALPACA_* fully migrated; APCA_* is canonical (71 active sites). Confirmed via grep audit. Future audits should grep `APCA_(API_KEY_ID|API_SECRET_KEY)` only.

**Service restart required.** Reversal: git revert + restart.

## 2026-05-07 — HM-AR-β: earnings_injector.py archived + run_earnings_universe_inject renamed

Path (a) "formal retirement" applied per Captain decision logged in HM-AR-β ticket. HM-AR audit (commit `136a62c`, `docs/EARNINGS.md`) classified the path as DEPRECATED orphan: zero callers across the codebase, no launchd/cron entry ever wired, table empty since creation, options-blackout uses an independent path.

**Actions:**
1. Archived `engine/earnings_injector.py` → `archive/earnings_injector.py.retired-20260507`. Sacred-data: `mv`, never `rm`. Header comment added documenting retirement context.
2. Renamed `main.py:679 run_earnings_universe_inject()` → `run_earnings_scan_inject()` to fix the naming-drift lie. The function writes to `scan_universe` (via `engine.deep_scan.inject_earnings_tickers`), not the orphan `earnings_universe` table — the old name confused multiple investigations. 4 rename sites in `main.py` updated: definition (679→684 post-comment-add), error log line, adjacent comment, schedule binding.
3. `earnings_universe` SQLite table left in place (empty, sacred-data rule).
4. `docs/EARNINGS.md` path map updated: section 2 now references the new function name; section 3 marked RETIRED with archive path.
5. `docs/XO_BACKLOG.md` HM-AR-β marked SHIPPED.

**No functional change.** Pure code hygiene — eliminates the naming-drift confusion that wasted investigator time during HM-AR.

**Service restart required** to load new bytecode (function name changed in scheduler binding). Reversal: `git revert <sha> && launchctl kickstart -k gui/$(id -u)/com.trademinds.trader && mv archive/earnings_injector.py.retired-20260507 engine/earnings_injector.py`.

## 2026-05-07 — HM-AQ-β SHIPPED: dynamic WATCH_STOCKS universe (~1,223 names)

5 commits cover the Captain's decision (HM-AQ, 2026-05-07): replace the static 20-name `config.WATCH_STOCKS` constant with a Polygon-driven dynamic universe of stocks + ETFs matching market_cap ≥ $5B + dollar_volume ≥ $100M, refreshed weekly via launchd.

**Commit chain:**
1. `5eb479c` migration(HM-AQ-β): scan_universe ALTER TABLE — `market_cap` + `options_eligible` columns
2. `dd43bab` feat(HM-AQ-β): `engine/universe.py` accessor (returns 20-name fallback pre-refresh)
3. `12ad22d` feat(HM-AQ-β): `engine/universe_refresh.py` 3-step Polygon pipeline
4. `404f0a2` refactor(HM-AQ-β): 38 files migrated to `engine.universe`; `config.WATCH_STOCKS` deleted; `FIXED_WATCHLIST` converged
5. (this commit) feat(HM-AQ-β v3): bug-fix bundle + plist + perf fix + wet refresh

**v3 bundle in commit 5:**
- `migrations/HM-AQ-β_universe_ticker_type_2026-05-07.sql` — adds `ticker_type TEXT DEFAULT 'CS'` (v2 dry-run revealed Polygon doesn't return market_cap for ETFs; needed type-aware branching)
- `engine/universe_refresh.py` — sys.path fix, NTFY ASCII title, ETF/ETN/ETV branches, per-symbol audit logging, $100M floor, MAX_FINAL_COUNT=2500, sample-print None-cap fix
- `engine/universe.py` — type-aware filter SQL, $100M threshold, ETF/ETN constants
- `dashboard/app.py` + `main.py` — **bulk-endpoint perf fix at 9 sites** (replaces per-symbol fan-out with `get_bulk_prices(get_active_universe())` — ~25× faster on 1,223 symbols)
- `~/Library/LaunchAgents/com.ollietrades.universe-refresh.plist` (new) — Sunday 14:00 MST
- `docs/UNIVERSE.md`, `docs/XO_BACKLOG.md` (HM-AQ-β SHIPPED + HM-AQ-β.2 ADRC ticket added)

**v1→v2→v3 dry-run iteration:**
- v1 ($50M, no ETF branch) — TQQQ/IWM/XLE all dropped, killed
- v2 ($50M, ETF branch + ETV skip) — 1,554 finalists tripped 1500 sanity bound (intended diagnostic)
- v3 ($100M, MAX_FINAL_COUNT=2500) — 1,223 finalists in band, sample crash on ETF None-cap (cosmetic, fixed)

**Wet refresh result** (post-commit): see GATE 4 verification block below this entry once executed.

**Performance impact (dashboard latency):**
- Before HM-AQ-β v3: per-symbol fan-out via ThreadPoolExecutor(max_workers=6) → ~47s for 1,223 symbols
- After v3: single `get_bulk_prices()` Alpaca call → ~1-2s
- Captain caught the math error in my pre-commit estimate; actual fix is the bulk-endpoint pattern, not threshold tightening.

**Rollback:** `git revert` last 5 commits + drop ALTER COLUMNs (SQLite pre-3.35: schema rebuild required) + `launchctl bootout gui/$(id -u)/com.ollietrades.universe-refresh` + service restart. Or simpler: revert commit 5 only (consumers still work via fallback path in `engine.universe`).

## 2026-05-07 — HM-AM Phase 1 SHIPPED: Total Portfolio Unification (data layer)

Captain mental-model 2026-05-06 ("metals are an extension of the total portfolio") materialized as a read-only data layer. Schwab + Dilithium Reserve + Alpaca paper now reachable through one API.

**Module:** `engine/total_portfolio.py` (new). Public API: `get_total_portfolio()` returns unified `TotalPortfolio` with positions + cash + totals + per-source resilience flags. `get_portfolio_summary()` for lightweight callers. 30s TTL cache (matches `engine/universe.py` precedent).

**First smoke** (`venv/bin/python3 engine/total_portfolio.py`):
```
total_value:    $138,371.20
total_cash:     $104,308.93
total_invested: $34,062.27
positions:      22 (11 Schwab + 2 metals + 9 Alpaca paper)
sources_loaded: ["schwab", "metals", "alpaca_paper"]
sources_failed: []
```

**Per-source resilience verified.** First smoke initially failed Alpaca with `AlpacaBridge.account()` — bridge method is `status()` not `account()`. Fixed; the failure surfaced cleanly via `sources_failed` rather than crashing the whole module.

**Phases 2-4 deferred** to fresh sessions per Captain scope:
- Phase 2: Kirk advisory integration (switch `_load_real_holdings()` → `get_total_portfolio()`)
- Phase 3: Advisory Team prompt context expansion
- Phase 4: `dalio-metals` strategy realign

**No consumer integration in Phase 1.** Kirk/Advisory Team/dalio-metals untouched. No service restart required (new module not yet imported by `main.py`).

**Files:** `engine/total_portfolio.py` (new) + `docs/TOTAL_PORTFOLIO.md` (new) + `docs/XO_BACKLOG.md` HM-AM section + `docs/SCHEMA.md` metals_ledger cross-ref.

**Reversal:** `git revert <commit-sha>` — module deletion. Sacred-data: read-only module, no DB writes.
- 2026-05-08T06:00:04 | daily_backup | backup=trader_2026-05-08.db | 263584KB
- 2026-05-09T06:00:06 | daily_backup | backup=trader_2026-05-09.db | 266724KB
- 2026-05-10T06:00:04 | daily_backup | backup=trader_2026-05-10.db | 268920KB
- 2026-05-11T06:00:06 | daily_backup | backup=trader_2026-05-11.db | 273908KB
- 2026-05-12T06:00:07 | daily_backup | backup=trader_2026-05-12.db | 278860KB
- 2026-05-22T06:00:06 | daily_backup | backup=trader_2026-05-22.db | 373792KB

## 2026-05-22 — HM-ARCHER-FLOAT-RESTORE recovery snippet (browsers with stale localStorage)

Pre-PR-#61 (merged 2026-05-21) the Archer floater's ✕ dismiss wrote `archerFloatHidden=true` to localStorage with no UI to reverse it. The fix is live on main, but any browser that dismissed before yesterday's merge still has the stale key and sees an empty floater area.

**One-shot recovery — paste into browser DevTools console at http://127.0.0.1:8080:**
```js
localStorage.removeItem('archerFloatHidden'); showArcherFloat();
```

Clears the persistent flag and force-shows the floater. The new `#archer-bring-back` pill (line 31215 IIFE) handles all future dismiss/restore cycles natively, so this paste is a one-time per-browser unstick.

## 2026-05-22 — HM-DECISION-AUDIT-V1.1 SHIPPED: gate downgrade math captured

`engine/paper_trader.py` + decision_audit schema gain `raw_confidence` / `meta_confidence` / `confidence_modifier` columns. Hook B parses meta% from `LOW_CONVICTION:` rejection reasons and joins `model_adjustments` for the per-player modifier.

The 24-point delta (deepseek-7b-grok4 raw 0.85 × modifier 0.72 = meta 0.61) is now queryable in one table. 17 prior gate_reject rows backfilled. Root: `engine/learning_engine.py:71` — NOT the risk gate.

**Files:** `engine/paper_trader.py` (commit 727573d) + 3-col ALTER on `decision_audit`.

**Reversal:** `git revert 727573d` + drop new cols (or leave NULL — back-compat preserved). Restart: `launchctl kickstart -k gui/$(id -u)/com.trademinds.trader`.

**Backup:** `data/trader.db.bak_HM-DECISION-AUDIT-V1-1_20260522_062104` (365MB).

## 2026-05-25 08:45 AZ — HM-MARKET-HOLIDAY-CALENDAR emergency halt (Stage 3)

**neo-matrix + ollie-auto** halt_mode set to `full` due to Memorial Day market-closed doctrine violation.

**Root cause:** Production trader has no US market holiday calendar. Memorial Day = Monday = weekday 0; dashboard's `market_is_open` widget (`dashboard/app.py:19886`) is weekday-only and returns True; trader fired as a normal Monday. `US_HOLIDAYS` constants exist only in backtest scripts (`scripts/ollie_backtest_12m.py`, `scripts/s6_*_backtest.py`), never imported by production execution path.

**Impact contained:**
- Stage 1 — 6 Alpaca orders cancelled, 0/0 filled (commit `6cdf9d5`)
- Stage 2 — 11 local rows archived to `positions_archived` + `trades_archived` per Doctrine Rule #2 (commit `c35aa51`, session `07d121ec-574e-44b0-a280-b2948c638c64`)
- Stage 3 — both offending players halted (this commit)

**Halt SQL applied:**
```sql
UPDATE ai_players
   SET halt_mode = 'full',
       halt_reason = 'HM-MARKET-HOLIDAY-CALENDAR doctrine violation 2026-05-25',
       halted_at = datetime('now', 'localtime')
 WHERE id IN ('neo-matrix', 'ollie-auto');
```

Verification:
```
id          halt_mode  halt_reason                                               halted_at
----------  ---------  --------------------------------------------------------  -------------------
neo-matrix  full       HM-MARKET-HOLIDAY-CALENDAR doctrine violation 2026-05-25  2026-05-25 08:45:42
ollie-auto  full       HM-MARKET-HOLIDAY-CALENDAR doctrine violation 2026-05-25  2026-05-25 08:45:42
```

**Re-engagement path:** Tuesday 2026-05-26 09:30 ET via Admiral manual unhalt (`UPDATE ai_players SET halt_mode='active', halted_at=NULL, halt_reason=NULL` per the standard unhalt pattern in CLAUDE.md), OR automatic when HM-MARKET-HOLIDAY-CALENDAR structural fix lands (Stage 4 in progress).

**Other 17 active AI players unaffected** — only the 2 that fired today are halted. Per guard rail: do not halt non-offending players.

**Trader behavior:** halt_mode is read per-cycle from ai_players, no restart needed. Next read by trader will pick up `full` and suppress further signal emission + trade execution for both players.

**Commits (NOT pushed; held pending Stage 4 Phase B tests):**
- `6cdf9d5` Stage 1 — order cancel + forensic snapshot
- `c35aa51` Stage 2 — local reconcile (Doctrine Rule #2 archive)
- (this commit) Stage 3 — agent halt

## 2026-05-25 09:00 AZ — HM-MARKET-HOLIDAY-CALENDAR full structural fix shipped (Stage 4 Phases A-D)

Memorial Day emergency arc closed. Trader now blocks signal emission +
order submission on weekends + 10 US holidays + early-close windows +
before/after hours. Rule #4 codified in `docs/DOCTRINE.md`.

**Phases:**
- A `7d55d35` — `engine/market_calendar.py` (NYSE holidays 2025-2027,
  early-close days, `MarketStatus` enum, helpers). 18/18 tests pass.
- B `3cd4838` — 7 hard gates (paper_trader buy/sell/short_sell +
  alpaca_bridge buy/sell/short_sell + alpaca_options.execute_options_signal)
  + soft update on `risk_manager.is_market_hours`. 11/11 gate tests pass.
- C `bf54ee8` — `dashboard/app.py::fleet_pulse` + `renderFleetPulse` JS;
  banner now shows `🛌 HOLIDAY · MEMORIAL DAY` instead of generic STANDBY.
  Live smoke verified.
- D (this commit) — docs/DOCTRINE.md Rule #4 + this OPS_LOG entry.

**Test totals (Phase A + B):** 29 passed, 3.92s.

**Visual smoke:** GET /api/fleet/pulse returns
`{market_status: "closed_holiday", holiday_name: "Memorial Day",
next_market_open: "2026-05-26T09:30:00-04:00", reasons: ["Holiday —
Memorial Day · fleet at rest"]}`. Banner JS produces "🛌 HOLIDAY ·
MEMORIAL DAY".

**Next:** Push all 7 commits (Stage 1 + 2 + 3 + Phase A + B + C + D) to
origin/main. Restore PG-V2 SUB-1 work from stash. Move halt_mode from
`full` to `exit_only` overnight for `neo-matrix` + `ollie-auto`
(conservative posture for first overnight after fix; Admiral promotes
to `active` Tuesday 09:30 ET via manual unhalt).

## 2026-05-25 09:30 AZ — HM-PROVING-GROUND-FORMALIZE-V2 — three-SUB structural ship

Reference: NTFY Proving Ground review banked Memorial Day morning identified
that the Sniper Mode trial (started 2026-04-10) had no exit criteria, no
dedicated NTFY topic, and was running 15 days past its 30-day spec without
formal extension. Admiral GREEN-LIGHT EXTEND-FORMALIZE this session.

**SUB-1** `e79a12a` — Dedicated NTFY topic `ollietrades-proving-ground`.
- `engine/ntfy.py::_fire_pg` + module-level `NTFY_PROVING_GROUND_TOPIC` env-overridable constant
- `engine/proving_ground.py` import-alias change (zero call-site diff)
- CLAUDE.md formalization block documented
- Admiral mobile NTFY receipt confirmed 09:00 AZ

**SUB-2** `af22d32` — Exit criteria + state machine evaluator + Admiral CLI.
- Thresholds (Admiral-locked): SHIP=10d go>=5 AND |dd|<=15%; KILL=any of dd>15% past Day 60 / go<3 for 10d / trades collapse >50%; WARN=go in 3-4 band 5+ days
- `scripts/migrations/hm_proving_ground_formalize_v2_sub2.sql` — `exit_status` column on running_scorecard + `state_transitions` append-only audit table
- `engine/proving_ground.py::ship_kill_evaluator` + helpers; terminal-sticky for shipped/killed
- `scripts/proving_ground_admiral.py` — ONLY path to terminal states; requires --confirm + --agent ollie-auto
- `main.py` — daily 13:18 AZ scheduler hook (between scorecard at 13:15 and ntfy at 13:30)
- **10/10 evaluator tests pass.** **DRY-RUN** against current 45 days: state=warning (NOT kill_warning); regression test pinned: K1 dd-past-Day-60 condition correctly does NOT fire before Day 60 even with dd=-24% sustained.

**SUB-3** (this commit) — Extension to Day 60.
- TRIAL_DAYS = 60 in engine/proving_ground.py (was 30)
- CLAUDE.md formalization block updated with extension rationale + thresholds (sign-convention-corrected) + heads-up that K1 will fire at Day 60 due to sustained -24% drawdown
- Day 60 boundary = 2026-06-09
- Forced-evaluation NTFY (HIGH severity) emits daily past Day 60 until Admiral terminal action

**Phase 1B Hard Gate #2 PASSED** at SUB-2 dry-run (state=warning, in allowed set).

Branch hm-proving-ground-formalize-v2 ready for merge to main.

**Heads-up for 2026-06-09:** the evaluator will fire kill_warning the
moment trial passes Day 60 because max_drawdown has held at -24% across
the entire trial. Admiral can preempt with --kill before Day 60 OR let
the structural finding surface itself and respond at that point.

---

## 2026-05-25 evening — HM-ALPACA-OOB-ORDER-CANCEL

**Surfaced by:** `scripts/admiral_premarket_check.sh` CHECK 5 (the script
shipped earlier this same session) on its first live smoke run.

**State found:** 5 Alpaca-paper `status=accepted` orders, all QQQ buy×1
market, submitted Memorial Day 2026-05-25 between 08:55 AZ and 09:46 AZ:

  · 176ace6e-da57-4b3b-ae26-adb8568fad61  08:55:32
  · f0a6f8c7-cefa-4b9a-a54c-fa5258a12ad6  08:55:56
  · f4afbe16-eb0f-452c-b9d1-ef5af43867a8  08:59:20
  · 9930f338-c37d-46e1-90da-fef354b452bf  09:19:32
  · fee4a5d1-a7f5-4515-afe6-c78aa7afab75  09:46:57

**Investigation (read-only, 20 min):**

- ZERO local trace. None of the 5 order IDs appear in `trades` table.
- ZERO matching `/api/alpaca/buy` POST hits in `trader.log` for the
  Memorial Day window (last POST was 2026-05-22 19:57:07).
- ZERO `player_id='trade-desk'` trades on 2026-05-25.
- ZERO `/api/alpaca/*` GET hits from a dashboard session during the
  08:00-10:00 AZ window (Captain wasn't viewing the local UI either).
- ZERO positions table rows in QQQ.
- HALT-GATE confirmed live in-process (suppressed Impulse MULL at
  09:46:20, 4 sec before order #5 — the local trader's halt-cascade
  was working).
- Trader PID 43883 started Sun May 24 13:14:05 → all of today's Phase
  A/B/C/D commits exist on disk but NOT in running bytecode. Phase B
  gates would not have caught the orders even if they HAD originated
  in-process; they did not.

**Conclusion:** The 5 orders were submitted to Alpaca paper **out-of-
band**, from outside the local trader process. Probable vectors:
Alpaca's own web UI, Alpaca mobile app, or a separate alpaca-py SDK
script. No fleet agent fired them. No fleet code path is responsible.

**Action (Admiral-approved):** cancel-by-API via
`POST /api/alpaca/cancel/{order_id}` × 5. All 5 returned
`{"success":true, "status":"canceled"}`. Post-probe at 19:30 MST
confirmed open-orders count = 0; all 5 IDs visible under
`?status=all` with `status=canceled`.

**Premarket re-run at 19:30 MST: 8/8 PASS** (CHECK 2 still WARN for
Memorial Day holiday — correct; it is still Memorial Day tonight).
Safe-to-unhalt achieved.

---

**Admiral confirmation 2026-05-25 evening (after cancel + restart):**

Source of the 5 OOB orders was **Admiral manual Alpaca UI activity** —
single intended test order, UI lag caused click repetition that
registered 5× at the broker side. Not a security incident, not a
fleet violation, not anomalous external activity. The pattern fits
the timing precisely (08:55:32 + 08:55:56 only 24 sec apart, then
3-4 min spacing as the human re-attempted thinking the previous click
hadn't taken).

**Companion datapoint:** 4× AAPL bracket orders 2026-05-23 05:49-05:59
AZ — also Admiral manual activity, testing OCO bracket functionality
($1.00 unreachable limit + market buy + limit sell + stop sell). Both
events are Admiral exploratory/testing activity against the paper
account, not fleet behavior.

**Doctrine gap REFRAMED:** the gap was never about detecting
"unauthorized activity." The gap is that the fleet had **zero
observability** of Alpaca-paper activity it didn't originate — even
when the Admiral was the originator. A premarket check that says
"safe to unhalt" must surface the full picture of what's pending at
the broker, whoever submitted it.

**`HM-ALPACA-OUT-OF-BAND-ORDER-RECONCILE` — REPRIORITIZED to MEDIUM**

Original framing (cleared): "unauthorized activity detection."
Updated framing: **fleet observability of all Alpaca paper account
activity, Admiral or otherwise.** The fleet doesn't need to PREVENT
Admiral OOB orders — just SEE them and not panic.

Bonus capability worth scoping into the ticket: **UI-lag click-repeat
detection.** Pattern signature is N orders, identical symbol+side+qty,
submitted within a small window (e.g. >2 orders with same shape in
60 sec). A console.log + NTFY at the moment of detection would let
the Admiral know the broker took N copies before the rest fire. ~1-2h
scope still — this is observability, not gating.

**Premarket CHECK 5** is the manual version of the observability
half of this ticket; the automated reconcile would run on a 5-15 min
cadence during market hours plus a once-at-pre-bell sweep.

---

**Restart executed 2026-05-25 19:35:52 MST (separate commit
`0483afb`):** Old PID 43883 → SIGTERM 2s → New PID 67615 → port 8080
listening 8s. 79 commits loaded. Post-restart premarket 9/9 PASS
(CHECK 9 code-freshness added in the same commit; PID 67615 now
verifies as current bytecode). `/api/fleet/pulse` now returns
`market_status: "closed_holiday"` + `holiday_name: "Memorial Day"`
+ `next_market_open` — proof that Memorial Day Phase B/C/D code is
LIVE in-process, no longer disk-only.

## 2026-05-28 17:33 AZ — HM-SCHWAB-WATCHER-CRON: launchd→cron migration (silent-gap fix)

**Incident:** Schwab CSV import pipeline silently frozen 2026-05-23 → 2026-05-28
(5 days). `/api/portfolio/real` (and Morpheus real-world net worth, HM-AM) served
stale data: 1 position (INTU 3 @ $307) from snapshot `2026-05-23T16:33:00`. No
position dated today; no fresh CSV in `inbox/` since 05-23.

**Root cause:** The watcher (`schwab_csv_watcher.sh`, 60s poll) and its 48h
staleness alarm (`schwab_cadence_check.py`, daily 06:30) ran ONLY via launchd
plists (`com.ollietrades.schwab-{watcher,cadence}`). Neither was loaded in
`launchctl` — the documented `launchctl bootstrap gui/$UID` "Domain does not
support specified action" over SSH + RunAtLoad-needs-Aqua-session failure (same
failure that blocked the trader `launchctl kickstart` earlier today). Last
successful auto-import: 2026-05-18 (watcher log ends there). The 05-23 update
was a MANUAL import (CSVs left unarchived in `inbox/`, no watcher-log entry).
**Double failure:** the staleness alarm shared launchd with the watcher, so it
died by the same cause and never fired.

**Fix:** migrated both jobs to crontab (cron survives reboot here — proven by
the existing trader/signal-center/cloudflared/swingdesk `@reboot` wrappers):
  - `* * * * * /bin/bash .../schwab_csv_watcher.sh >> logs/schwab_watcher_cron.log 2>&1`
  - `30 6 * * * venv/bin/python3 .../schwab_cadence_check.py >> logs/schwab_cadence_cron.log 2>&1`
Crontab backed up to `/tmp/crontab.backup_20260528_173325` (append-only; all 4
production `@reboot` entries verified intact, 25→27 lines). Stale 05-23 CSVs
archived out of `inbox/` → `data/schwab_csv_archive/`. Both launchd plists
archived → `archive/launchagents_2026-05-28/` (cron is now the SOLE trigger;
no double-fire). Watcher deps verified: scripts compile under venv 3.9,
`schwab_holdings` populated (166 rows, last 2026-05-23).

**Why cron not launchd:** launchd-at-boot is the known-broken path on this box
(needs an Aqua login session; `bootstrap` fails over SSH). cron `@reboot` and
periodic entries demonstrably fire here. So reboot-durable services live on cron.

**Verification:** cron daemon running (pid 532); entry installed; first tick
fired ~30s after install (created `schwab_watcher_cron.log` at 17:34:00 — empty,
correct: watcher no-ops with zero output on an empty `inbox/`). End-to-end
import confirmation pending Captain's next fresh-CSV drop (cron ingests <60s).

**Lesson (→ CLAUDE.md doctrine):** an alarm that shares a failure mode with the
thing it monitors provides no defense. The cron fix still has watcher + alarm on
the SAME mechanism — cross-mechanism alarm relocation tracked in XO_BACKLOG.

## 2026-05-28 ~18:xx AZ — HM-PUSH-UNBLOCK: git history purge (12 DB backups) + force-push

**Incident:** `git push` rejected `GH001: Large files detected`. Root cause: 12
`trader.db` backups (300–384MB each) accumulated in git history over prior
sessions, sitting in an **87-commit unpushed backlog** — every push had been
failing silently for days.

**Discovery: REACTIVE, not proactive.** A normal `git push` during HM-AUDIT-T0
(committing GPU/F-4 doc fixes) surfaced it. Nobody was watching push health.

**Fix:** `git filter-repo --invert-paths` purge of `data/trader.db.backup-*` +
`backups/*/trader.db.bak` from ALL history → `git push --force-with-lease origin
main` (8ed2037 → 9838383). 0 blobs >100MB remain; **.git 1.8GB → 193MB.**

**No remote work lost:** origin was a strict ancestor (87 behind, the original
push failed only on large-files, not non-fast-forward), so force-push overwrote
nothing unique. Sole committer confirmed (4 git identities, all Steve; CI =
grep-gate scanner only, non-deploying, passed green).

**Recovery net:** `~/ollie-pre-purge-2026-05-28.bundle` (889MB full `--all`).
**Sacred preservation:** 31 DB backup files moved to
`~/ollie_db_backups_archive_2026-05-28/` BEFORE the purge (archive-don't-delete).
**Prevention:** `data/trader.db.backup-*` now gitignored.
**Trader impact:** none — runs from local working tree, no git pull; PID 48180
unaffected.

**DOCTRINE (2nd instance today):** 87 commits of silent push failure fired no
alert — because nothing monitors push health independently of the push pipeline.
This is the SAME class as the Schwab cadence alarm dying on the same launchd as
its watcher. Monitors must fail INDEPENDENTLY of what they monitor. See
CLAUDE.md "Alarms must not share a failure mode with what they watch."
- 2026-05-30T09:47:16 | daily_backup | backup=trader_2026-05-30.db | 471040KB
- 2026-05-31T06:00:06 | daily_backup | backup=trader_2026-05-31.db | 471040KB
- 2026-06-01T06:00:07 | daily_backup | backup=trader_2026-06-01.db | 471040KB
- 2026-06-02T06:00:06 | daily_backup | backup=trader_2026-06-02.db | 496652KB

## 2026-06-02 — CALIBRATION FLAG: _STALE_BUDGET_S sub-poll budgets (0dte/swing)

**Not a bug to fix tonight — logged so it isn't lost.** Surfaced while adding
`intraday` to `engine/events_bus._STALE_BUDGET_S` (HM-INTRADAY-STALE-BUDGET).

**Observation.** The map is in SECONDS (`timedelta(seconds=budget)`,
events_bus.py:51). Two budgets are BELOW the signals_v2 bus consumer's poll
interval:
- `0dte: 2` (2s) and `swing: 30` (30s) — vs `run_events_bus_consumer` polling
  every **60s** (`main.py:4304`, `schedule.every(1).minutes`).

So any `signals_v2` row stamped 0dte/swing is past its `stale_after` before the
consumer's first pass and gets marked `status='stale'` (events_bus_consumer.py:92-100)
**without ever dispatching on the bus path.** (intraday=900s and position=300s
clear the 60s poll fine.)

**Does this lose trades? No.** Verified the primary execution path is NOT the bus:
- `engine/ai_brain.py` executes directly via `buy()` (ai_brain.py:1409) and
  `execute_signal()` (1620).
- `engine/crew_scanner.py` calls `buy()` directly (crew_scanner.py:2875) in the
  same cycle it emits the signal.
- `engine/dayblade.py` (T'Pol, 0dte) emits via `save_signal` (1095) with the
  DEFAULT `timeframe="SWING"` (no explicit tf) and manages its own options path.

`save_signal` → signals_v2 is a RECORDING + secondary-dispatch side-channel
(HM-EVENTS-BUS-CONSUMER 2026-05-26). So 0dte/swing dispatch via the faster DIRECT
path; they are effectively **dead on the bus consumer path specifically** — their
signals_v2 rows silently expire as `stale`. No missed trades today, but the bus
consumer is a no-op for those two timeframes, and any future migration that relies
on the bus for 0dte/swing execution would silently drop them.

**Calibration question for the Admiral (separate from tonight's two fixes):**
either (a) raise 0dte/swing budgets above the 60s poll (e.g. 0dte→120s, swing→300s)
if the bus is meant to dispatch them, (b) drop the consumer poll below the budgets,
or (c) accept the bus as intraday/position-only and document 0dte/swing as
direct-path-only. No change made — flagging only. Cross-ref CLAUDE.md
"Role-conversions leave stale parallel paths."
- 2026-06-03T06:00:06 | daily_backup | backup=trader_2026-06-03.db | 502504KB

## 2026-06-03 — Auth hardening + doc drift (053dfc5)

- Hardened /api/* auth bypass: AuthMiddleware localhost bypass now requires
  not _is_via_cf_tunnel() — tunnel traffic can no longer satisfy the loopback
  bypass even if uvicorn proxy-header rewriting breaks (dashboard/app.py)
- Made uvicorn proxy_headers=True + forwarded_allow_ips="127.0.0.1" explicit
  with SECURITY comment documenting the auth dependency (main.py:3250)
- Marked secondary uvicorn.run at app.py:20650 as non-canonical entry point
- Fixed stale CLAUDE.md uvicorn bind line refs: 2944→3250 (two locations)
- Verified: loopback 200 (scanner intact), tunnel 401 (kill-switch gated)
- Working tree has pre-existing unrelated changes (OPS_LOG.md, swingdesk.db,
  backtest artifacts) — not staged, left as-is

## 2026-06-03 — relative_strength PBO verdict confirmed GENUINELY FRAGILE

- 144-config decorrelated grid (pbo_rs_decorrelated.py) run 2026-06-01
- PBO = 0.6348 (gate requires ≤0.30) — FAILS
- Median logit = −0.73 (IS-best below OOS median on 63% of splits)
- Decorrelation was valid (mean |corr| 0.89→0.63, −0.26 drop confirmed)
- Verdict: fragility is genuine, not a collinearity artifact
- DSR passes (aggregate edge may exist) but no parameterization is selectable OOS
- Decision: keep relative_strength shadow/observation only — DO NOT GRADUATE
- Graduation gate remains open — bull_flag + rsi_bounce + rsi_divergence accruing
- 2026-06-04T06:00:06 | daily_backup | backup=trader_2026-06-04.db | 515164KB
- 2026-06-05T06:00:06 | daily_backup | backup=trader_2026-06-05.db | 522944KB
- 2026-06-06T06:00:07 | daily_backup | backup=trader_2026-06-06.db | 539220KB

## 2026-06-11 Morning Sweep (HM-CRUSHER-MISLABEL)
- Root cause: healthcheck.py SCANNER_LOG pointed at logs/scanner.err (dead since May 22) while the Dr. Crusher dialog blamed trader_error.log. Patched line 29 + warning labels to trader_error.log. Backup: healthcheck.py.bak.20260611.
- Rotated trader.log / trader_error.log / signal-center.log to archive/logs (gz, dated suffix).
- Pruned 9 orphaned local Ollama models (Ollie Box era): phi4:14b, llama3.1, llama3.2:3b, deepseek-r1 7b+14b, qwen3:14b, qwen2.5-coder:7b, plutus x2. Kept bigmac residents phi3/gemma3/mistral + cloud stubs. Disk 94 to 70 pct (60Gi free).

## 2026-06-11 — HM-BRIDGE-WEDGE-2: bridge "listening but not responding" RCA + fix

**Symptom:** Watchdog forced-restarted the trader 4× in 8 days (Jun 4 06:53, Jun 6 19:25, Jun 9 07:25, Jun 11 09:08) on "Process listening but not responding" probing `/api/status`. Jun 11 also re-hung 11 min after the restart (09:09→09:20 loop).

**Ticket hypothesis REFUTED.** The dashboard does NOT share a thread/loop with Ollama calls — `:8080` runs on its own uvicorn daemon thread (`main.py:run_dashboard`); Ollama calls run in separate threads via OllamaQueue and completed in **1–7s** throughout the Jun 11 hang (trader_error.log). The model-swap coincidence was spurious.

**Actual root cause: SQLite connection FILE-DESCRIPTOR leak → EMFILE wedge.** Measured the live process: trader.db FDs grew 37→69→120 (total 307→468) in ~6 min, tracking request/scan volume. The leak is the `conn = _conn(); …; conn.close()` pattern with **no try/finally** (~21 sites in app.py + tick_recorder + ready_room): trader.db is heavily written during scan/swap cycles → reads raise on lock contention → `close()` is skipped → the raised exception's traceback pins the frame-local connection so refcounting can't reclaim the FD. When the process hit the inherited soft NOFILE cap (`launchctl limit maxfiles` = 256), `accept()` returned EMFILE — kernel holds the listen socket but the app can't answer HTTP ("listening but not responding") — and every new `sqlite3.connect` failed with "unable to open database file" (the storm seen across tick_recorder/ReadyRoom/Finnhub right before each restart). Restart reset FDs → worked ~10 min → re-leaked → watchdog restart loop.

**Fix (3 layers, defense-in-depth):**
1. **Backstop (ceiling):** `main.py` raises `RLIMIT_NOFILE` soft→hard (capped 65536) at startup. Alone would have prevented all 4 hangs.
2. **Root cause (per-site try/finally close):** `engine/tick_recorder.py` (_flush_buffer + cleanup), `engine/ready_room.py` (persist + get_latest + get_history), `dashboard/app.py` hot endpoints (`/api/status` ← the watchdog's probe, `_save_notification`, `_patch_leaderboard_season_overlays`, `player_trades`, `player_signals`, `player_history`, `/api/season`).
3. **Catch-all reaper:** `main.py` FD-pressure GC daemon (`fd_gc_reaper`) — `gc.collect()` only when num_fds > 1500, reclaiming cycle-pinned leaked connections from large endpoints not re-indented (e.g. the 280-line `leaderboard`).

Plus req#3 hygiene: explicit `(connect, read)` timeouts on the two canonical Ollama paths (`ollama_provider.py` → `(5, 85)`, `crew_scanner._query_ollama` → `(5, 60)`) so an unreachable Ollie Max fails fast on connect. (All other Ollama sites already had bounding timeouts — none could block indefinitely.)

**Verify:** py_compile + import-smoke all 6 files; `dashboard.app` imports (775 routes). Restart via `scripts/trader_restart.sh`; confirmed single writer + :8080 bound; watched FD count stays bounded + no watchdog strikes for 10 min.

**Follow-up (deferred):** convert the remaining large multi-connection endpoints (`leaderboard`, `recent_trades`, `recent_signals`, `comparison_chart`, `player_detail`) to explicit try/finally; currently covered by the reaper + raised rlimit.
