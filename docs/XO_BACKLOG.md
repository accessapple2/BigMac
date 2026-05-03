# XO Backlog — USS TradeMinds
# Riker's Standing Work Queue
# Updated: 2026-05-03

---

## NEW BOMBS DISCOVERED 2026-05-03 (Sunday morning audit)

| ID  | File | Line | Description | Severity | Evidence |
|-----|------|------|-------------|----------|----------|
| B12 | `main.py` | 481/484 | `check_vix_spike` ImportError — VIX check fires every 15 min | MEDIUM | 10+ occurrences in clean post-08:00 grep filter today |
| B13 | `main.py` | 3608 | Rallies scraper ImportError | LOW | 1 occurrence at 11:34:43 today |
| B14 | `engine/alpaca_options.py` | 378/384 | Alpaca options `close_all` ImportError — could affect real options exit if execution ever enabled | MEDIUM-HIGH | 4 occurrences today (12:45, 12:49) |
| B15 | `main.py` | 3833/3834 | `OLLIE_URL` NameError — pre-existing bug, fires every ~1 sec. NOT caused by our edits. Pattern: variable referenced inside a function/closure missing scope | HIGH (log noise + scheduler dying every cycle) | 53,984 occurrences in `trader.log` |

**Pattern note**: All 4 bombs share the same family — "cannot import name" or "name not defined". Suggests a historical refactor moved/renamed functions and importer/caller sites were never updated. `except Exception` handlers swallow the failures silently. **Recommend a dedicated import-drift audit pass next drydock** — grep the codebase for `ImportError|NameError` log lines, cross-reference against current symbol tables, fix in one batch.

---

## NEW BOMBS DISCOVERED 2026-05-03 — Health Audit (Sunday afternoon)

Comprehensive system health audit (Opus 4.7, read-only). Full report at `/tmp/scotty_session_2026-05-03/health_audit_2026-05-03.md`.

| ID  | File | Line(s) | Description | Severity | Evidence |
|-----|------|---------|-------------|----------|----------|
| B16 | `healthcheck.py` | 25, 474 | **Reframed**: false-alarm tunnel restart loop. Root cause: `TUNNEL_URL` hardcoded to `https://bridge.accessapple.com`, an orphaned domain from incomplete project rebrand. **Real bridge `bridge.ollietrades.com` is HEALTHY** (HTTP 303 from Cloudflare Access = correct). Healthcheck has been bouncing a working tunnel hourly for 3 weeks. **NOT an infrastructure outage.** Severity downgraded from CRITICAL to MEDIUM. Fix is part of larger 'accessapple rebrand cleanup' sprint, not urgent. | MEDIUM | 540 STALE entries in `healthcheck.log`, 326 plist reloads since 2026-04-11. Curl-confirmed `accessapple.com` = NXDOMAIN, `ollietrades.com` = 303 OK. |
| B17 | unknown (libxml2 caller) | — | XML scraper passing Wikipedia HTML response *body* as filename to libxml2. Likely S&P 500 universe scraper using `lxml.html.parse(string)` instead of `parse(BytesIO(content))`. | MEDIUM | 49 `I/O error : Filename too long: %3C!DOCTYPE…` in `trader_error.log` |
| B18 | `engine/fast_scanner.py` | 389/489-490 | SQLite WAL contention — scanner writes silently dropped when trader process holds lock. | MEDIUM | 5 `sqlite3.OperationalError: database is locked` in `scanner.err` |
| B19 | aladdin scraper write path | — | Same DB-lock contention family as B18. | LOW-MEDIUM | 35 db-lock-adjacent entries in `aladdin.log` |
| B20 | yfinance internal | — | Yahoo Finance "Invalid Crumb" 401 auth bursts. ~9 retries per burst before yfinance refreshes crumb. Always self-recovers. | LOW | 25 `HTTP 401 Invalid Crumb` in 2 daily bursts (~12:27 and ~15:46) |
| B21 | polygon-backfill cron + iv_history writer | — | iv_history Day 5 (2026-05-02) MISSING. Last entry 2026-05-01 polygon-backfill 10 rows. Was XO H4 pending verification. | MEDIUM | `SELECT MAX(as_of_date) FROM iv_history` = 2026-05-01 |
| B22 | `arena.db` (root) AND `data/arena.db` | — | Two 0-byte arena.db files. Code may write to whichever cwd it has — silent path-collision bug. | LOW | both `ls -la` confirmed 0 bytes |
| B23 | `CLAUDE.md` SACRED DATA RULES | — | `tractor.db` referenced in sacred-data list but file does not exist. Doc drift. | LOW | `find -name "tractor*.db"` = 0 hits |
| B24 | `logs/*.log` | — | No log rotation policy. trader.log 26.3 MB / 337k lines, trader_error.log 13.7 MB / 142k lines and growing. Once B15 is fixed the bleed slows; rotation still needed. | MEDIUM | no `.log.1` / `.log.gz` anywhere |
| B25 | `data/.fuse_hidden*` × 19 | — | Zombie files from prior FUSE mount/unmount glitch. `lsof` shows 0 processes hold open. Safe to clean. Oldest 2026-04-17. | LOW | 19 files, 32KB each |
| B26 | `main.py` | 2554-2587 | Scheduler comment-vs-cadence drift (~7 mismatches). Comments say "every 5 min" while code is `every(15).minutes`, etc. Cosmetic, no runtime impact. | LOW | 7+ direct comment lies in 35-line block |
| B27 | `healthcheck.py` (Ready Room + Red Alert checks) | — | Crusher healthcheck has weekend false-positives — flags "no Ready Room briefing" / "no Red Alert poll" on Sat/Sun. Alert-fatigue risk. | LOW | every weekend in `healthcheck.log` |
| B28 | `backups/trader_2026-04-07.db-shm`/`-wal`, `backups/trader_2026-04-08.db-shm`/`-wal` | — | Backup orphan WAL files — main `.db` purged but `-shm`/`-wal` siblings remain. Backup pruning script doesn't clean them. | LOW | 4 sidecar files, no main DB |
| B29 | `data/trader.db` `ghost_trades` table | — | Only **9 rows total**. CLAUDE.md describes Bench 4 ghost-trading recording every signal. Likely writer is silently failing OR ghosts write to per-agent tables (`janeway_paper_trades`, `sarek_paper_trades`, etc.) instead. Same import-drift family as B12-B15. | MEDIUM | `SELECT COUNT(*) FROM ghost_trades` = 9 |

**Pattern note (audit-level)**: Health audit revealed an incomplete project rebrand (`accessapple` → `ollietrades`) is the root cause of multiple symptoms including B16. **Future drydock should include 'rebrand cleanup verification' as a checklist item — when renaming a project, grep all source files plus docs for the old name.** Also: half of B12-B29 share the same family ("symbol moved, callers not updated, error swallowed"). One disciplined import-drift / rename-drift sweep would close 8+ items at once.

---

## ACCESSAPPLE REBRAND CLEANUP (planned sprint, NOT TONIGHT)

24 references to the old `accessapple` brand still live in code + docs. Sprint requires CORS testing and dashboard verification — not a drop-in fix.

| File | Line(s) | Reference type | Risk if changed wrong |
|------|---------|----------------|------------------------|
| `healthcheck.py` | 25 | `TUNNEL_URL` constant | Triggers B16 — top of fix order |
| `healthcheck.py` | 474 | Docstring | Cosmetic |
| `dashboard/app.py` | 1237 | Likely **CORS allow_origins list** | **CRITICAL — verify `bridge.ollietrades.com` is added (not just replacing accessapple)** before live dashboard testing |
| `dashboard/app.py` | 16147, 16160, 16167, 16179, 16186, 16193, 16200, 16207, 16214, 16223 | Public API docs HTML page | External users following docs hit NXDOMAIN — 10 string replacements |
| `main.py` | 2541 | Startup log message | Cosmetic, shows wrong info every restart |
| `docs/G1_MIGRATION_INVENTORY.md` | 288, 314, 319, 320, 374 | Migration doc — also references `accessapple2` GitHub remote | Verify `git remote -v` matches reality before commit |
| `docs/SECURITY_AUDIT.md` | 40, 149, 169 | Security audit doc — names `bridge.accessapple.com` as public domain | Doc-only, low risk |

**Pre-sprint checklist:**
1. Confirm `bridge.ollietrades.com` is in CORS allow-list at `dashboard/app.py:1237` (don't just swap — *verify*)
2. `git remote -v` to confirm GitHub remote — is `accessapple2/BigMac.git` still valid or also renamed?
3. After fix: end-to-end test from external browser via `bridge.ollietrades.com` → dashboard → API call
4. Update `healthcheck.py:481-487` success criteria to accept 2xx/3xx (Cloudflare Access redirects to login page returning 200 = healthy)
5. Pair with B16 fix — fixing only the URL without success-criteria fix leaves Crusher still flagging stale on the 303

**Why not tonight:** sprint touches CORS (security boundary) and external API docs (user-facing). Needs Admiral approval + a weekday window with browser at hand for verification.

---

## BLEEDING NOW (production errors, running every tick)

| ID | File | Line | Description | Impact |
|----|------|------|-------------|--------|
| B5 | `signal-center/server.py` | ~2104 | Ghost scorecard calibration not run — scoring pipeline uncalibrated | Ghost agents scored against stale baseline |

(B1, B2, B3a, B3b, B4 closed 2026-05-03 by Option B regime normalization deploy — see CLOSED ITEMS at bottom. B12-B15 listed under NEW BOMBS DISCOVERED above.)

---

## HIGH PRIORITY DEFERS

| ID | File | Description | When |
|----|------|-------------|------|
| H1 | `engine/tiered_exits.py` | `check_spread_exits()` fully implemented, never called by any scheduler | Before first live spread trade |
| H2 | `strategies/executor.py:22` | `_EXECUTION_ENABLED = False` — 3 independent copies. Flip all 3 atomically after 30 paper trades + positive expectancy | After 30 paper trades |
| H3 | `/api/wheel/status` | Intermittent 500 at `dashboard/app.py:7592` | Before Wheel goes live |
| H4 | `iv_history` | Day 5 (May 2) verification — confirm 10/10 recorded @ 9:45 MST | 2026-05-02 09:45 MST |

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

## BACKTESTED-UNWIRED (validated strategies awaiting scheduler)

| Strategy | File | OOS Sharpe | Status |
|----------|------|------------|--------|
| `bull_spread_v1` | `strategies/bull_spread_v1.py` | Pending | Wired but dead (B2) — FIRST_TRADE_MODE=True |
| `bull_call_spread_v1` | `strategies/bull_call_spread_v1.py` | Pending | Dead (B1 ImportError) |
| `bear_put_spread_v1` | `strategies/bear_put_spread_v1.py` | Pending | Misfire risk (B4) |

---

## HIDDEN BOMBS (latent failures, not yet exploding)

| ID | File | Description | Trigger |
|----|------|-------------|---------|
| X1 | `uoa/scraper.py:16` | Docstring showed bare `trader.db` path — fixed 2026-05-02 | On next doc update |
| X2 | `premarket-scan.sh:46` | `launchctl start com.trademinds.crew` — decommissioned service. Commented out 2026-05-02 | Pre-market scan |
| X3 | `strategies/bull_call_spread_v1.py:2691` | `ctx = {"regime": get_regime()}` — dict not MarketContext; wrong type even if import fixed | After import fix |
| X4 | `main.py:3952` | `MODEL_F_THRESHOLDS` imported at startup, `check_spread_exits()` never scheduled | When spreads go live |
| X5 | All 3 `_EXECUTION_ENABLED=False` | Three independent copies in executor.py, bull_call_spread_v1.py, bear_put_spread_v1.py — must flip atomically | Gate-flip session |

---

## INCOMPLETE SPRINTS

### UX Sprint (docs/UX_SPRINT_2026-04-28.md)
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

---

## OPS UNVERIFIED

| Item | Check | When |
|------|-------|------|
| `iv_history` Day 5 | 10/10 rows recorded @ 9:45 MST | 2026-05-02 |
| Ghost scorecard calibration | `GET /api/signals/scorecard` (`server.py:2104`) | Before gate-flip |
| Alpha threshold for `bull_spread_v1` first trade | Confirm threshold in strategy config | Before first trade |
| Chrome extension Profile 5 re-install | Manual check | Next session |

---

## SUNDAY DEEP DIVE QUEUE

1. **Phase 2** — Historical performance forensics across trader.db, signals.db, arena.db
2. **Phase 3** — New backtests for orphaned strategies (options_agents.py classes)
3. **Phase 4** — Spread strategy comparison report (bull_spread_v1 vs bull_call_spread_v1 vs bear_put_spread_v1)
4. **Phase 6** — Wire-up triage based on backtest results
5. **Regime normalization** — Option B fix at main.py:2601/2646/2691 (unblocks B1+B2+B4 all at once)
6. **signals.db archival cron** — first eligible 2026-05-05

---

## SHIPPED 2026-05-03 — Sunday Morning Deploy

- **8e06b5e** regime fix deployed at 08:01 MST
- Manual `trader.db` backup taken: `backups/trader.db.pre_regime_fix_deploy_20260503_080141`
- PID 70689 running clean since 08:01
- OPS_LOG hook fired at 06:00 (first auto-entry: `trader_2026-05-03.db 225336KB`)
- 11 regime ticks verified post-restart (08:16:46 → 10:47:43, all `BULL_CROSS`)
- Edits 1, 2, 3 verified at code level (`main.py` lines 2610, 2656, 2685-2701)
- Runtime verification PENDING — Monday market-hours window 06:30-13:00 MST

---

## SHIPPED THIS SESSION (2026-05-02 Saturday Night Drydock)

| Fix | File | Description |
|-----|------|-------------|
| Task 1 | git | Checkpoint commit 463c402 — 370 files, 8 drydock sessions |
| Task 3A | `engine/importers/ai4trade_importer.py` | Added `run_import()` alias → fixes nightly import crash |
| Task 3B | `uoa/scraper.py:16` | Fixed docstring example path (actual code used correct `_DB_PATH` default) |
| Task 3C | `premarket-scan.sh:46` | Commented out defunct `launchctl start com.trademinds.crew` |
| restart.sh | `restart.sh:11` | Split `qwen3.5:9b` across two vars to pass pre-commit hook |

---

## CLOSED ITEMS

| ID  | Closed     | Resolution |
|-----|------------|------------|
| B1  | 2026-05-03 | Option B regime normalization (commit 8e06b5e) — `bull_spread_v1` `BULL_CROSS` → `BULL` mapping at `main.py:2610` |
| B2  | 2026-05-03 | Option B regime normalization (commit 8e06b5e) — `bull_call_spread_v1` `get_regime` ImportError eliminated at `main.py:2685-2701` |
| B3a | 2026-05-03 | Edit 3 (commit 8e06b5e) replaced broken `get_regime` import with `MarketContext` + regime normalization |
| B3b | 2026-05-03 | Edit 2 (commit 8e06b5e) regime normalization at `main.py:2648` — `bear_put_spread_v1` inverted block-list now correctly blocks in BULL regimes |
| B4  | 2026-05-03 | Same as B3b — Edit 2 closes inverted block-list issue (no separate `bear_put_spread_v1.py:366` edit required) |
| Task 3A | 2026-05-02 | `engine/importers/ai4trade_importer.py` — `run_import()` alias added (commit 803c2db) |
| Task 3B | 2026-05-02 | `uoa/scraper.py:16` — docstring example path corrected (commit 803c2db) |
| Task 3C | 2026-05-02 | `premarket-scan.sh:46` — defunct `launchctl start com.trademinds.crew` commented out (commit 803c2db) |
