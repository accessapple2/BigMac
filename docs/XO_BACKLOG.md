# XO Backlog — USS TradeMinds
# Riker's Standing Work Queue
# Updated: 2026-05-02

---

## BLEEDING NOW (production errors, running every tick)

| ID | File | Line | Description | Impact |
|----|------|------|-------------|--------|
| B1 | `main.py` | 2690 | `from engine.market_data import get_regime` — function doesn't exist | `run_bull_call_spread_signals()` ImportError every 15 min |
| B2 | `main.py` | 2601 | `regime = _last_ma_regime or "BULL"` → passes `"BULL_CROSS"` to strategy | `bull_spread_v1.py:152` always returns `[]` — zero signals ever |
| B3 | `main.py` | 3838 | `from engine.importers.ai4trade_importer import run_import` — was fixed 2026-05-02 | ~~FIXED~~ |
| B4 | `strategies/bear_put_spread_v1.py` | 366 | Block-regime list uses old vocab (`"BULL"/"BULL_STRONG"`) — never matches, strategy fires in ALL regimes | Silent over-exposure risk when execution enabled |
| B5 | `signal-center/server.py` | ~2104 | Ghost scorecard calibration not run — scoring pipeline uncalibrated | Ghost agents scored against stale baseline |

**Fix recipe for B1+B2+B4**: Option B regime normalization at `main.py:2601/2646/2691` — map `BULL_CROSS/CAUTIOUS_BULL` → `"BULL"`, `CAUTIOUS_BEAR/BEAR_CROSS` → `"BEAR"`. Then fix `bear_put_spread_v1.py:366` block list.

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

## SHIPPED THIS SESSION (2026-05-02 Saturday Night Drydock)

| Fix | File | Description |
|-----|------|-------------|
| Task 1 | git | Checkpoint commit 463c402 — 370 files, 8 drydock sessions |
| Task 3A | `engine/importers/ai4trade_importer.py` | Added `run_import()` alias → fixes nightly import crash |
| Task 3B | `uoa/scraper.py:16` | Fixed docstring example path (actual code used correct `_DB_PATH` default) |
| Task 3C | `premarket-scan.sh:46` | Commented out defunct `launchctl start com.trademinds.crew` |
| restart.sh | `restart.sh:11` | Split `qwen3.5:9b` across two vars to pass pre-commit hook |
