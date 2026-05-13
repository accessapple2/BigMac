# HM-CD — Phase 0 Discovery Report

**Date:** 2026-05-12
**Phase:** CD.0 (Discovery, no code changes)
**Auditor:** Scotty (Opus 4.7)
**Status:** HALT — directive's mental model doesn't match the codebase

## TL;DR

The directive's framing ("668 symbols × 2.8s/symbol = 31-min cycle in the ai_brain.py main loop, profile `scan_symbol(IBM)`") doesn't match the running architecture. The scan loop is **per-agent, not per-symbol**, and lives in `engine/crew_scanner.py:3312 run_scan_cycle`, not `ai_brain.py`. Per-symbol iteration is delegated to each agent and varies widely (some agents read a 10-symbol universe, others 500+).

**Three follow-up options for Captain (Q1):**
- **A)** Reframe target — profile `_scan_single_agent` per agent across one live cycle, identify which agent is the slowest, migrate that agent's bottleneck. (Recommended — matches the actual architecture.)
- **B)** Sweep all per-symbol yfinance fallbacks → batch Alpaca/Polygon (high_iv_scanner, long_range_sensors, trend_predictor, bull_bear, sentiment, stock_fundamentals, fundamental_score). Broad but mechanical.
- **C)** Add cycle-time instrumentation to `run_scan_cycle` and `_scan_single_agent`, observe one market-hours cycle, decide based on data.

## What CD.0 actually found

### 1. ai_brain.py has no per-symbol scan function

```
ai_brain.py size:        1526 lines
yfinance calls:           0 (only a comment at L28 referencing the prior architecture)
.info/.fast_info/.options accessors: 0
DB queries:               9 sqlite3 connections (recovery state, event log, paused players)
                          None are inside a 668-symbol loop.
```

Candidate function names tried: `scan_symbol`, `process_symbol`, `analyze_symbol`, `_scan_one`, `per_symbol`, `_scan_symbol`, `scan_one`. **None exist.**

What ai_brain.py actually does: `_dalio_recovery_state()`, `_hmeq_do_snapshots()`, `_hmeq_loop()`, equity-snapshot daemon (HM-EQ). It orchestrates; it does not iterate the 668-symbol universe.

### 2. The real scan loop lives in crew_scanner.py

**Master entry:** `engine/crew_scanner.py:3312 def run_scan_cycle` → `_run_scan_cycle_inner` → `_run_scan_cycle_body`.

**Body structure (L3338-3445):**
1. `gather_market_context()` — fetches VIX, GEX, session_type, spy_volume_ratio, etc.
2. `_lrs_scan()` — Long-Range Sensors whale-volume detection (enriches ctx)
3. Position management (instant, no LLM): `_update_neo_trailing_stops`, `_check_hard_stops`, `_check_scaled_exits`, `_check_spread_tiered_exits`, `_check_dip_buys`
4. **Agent loops:**
   - `for player_id in RULES_SCANNERS:` (instant, no Ollama)
   - `for player_id in ACTIVE_SCANNERS:` (= `["neo-matrix"]`, with `time.sleep(0.5)`)
   - `for player_id in alpha_pair:` (Alpha Squad pair, 2 Ollama agents, with `time.sleep(1.0)` between)
5. User-created natural-language agents

Each agent goes through `_scan_single_agent(player_id, ctx)` — **a 617-line function** that internally fetches per-symbol data, runs analysis, possibly calls Ollama, and writes results. This is where the heavy per-symbol work happens.

### 3. Per-symbol fallback paths (yfinance / Yahoo) still in the engine

Outside `ai_brain.py`, yfinance is still hot in:

```
engine/holodeck_expansion.py L49, L152, L154   — yf.download (period=1y, 5d)
engine/squeeze_scanner.py L74-113, L223        — _get_yfinance_data fallback
engine/high_iv_scanner.py L13, L33             — yf.Ticker(symbol) per-symbol loop (L136: `for sym in symbols:`)
engine/insider_tracker.py L21-22               — yf.Ticker (parallel, 4 workers, 2s/symbol cap)
engine/ghost_scoring.py L210, L305             — yfinance fallback (marked migrated to Alpaca but path remains)
engine/ready_room.py L450                      — VIX fetch (one-shot, not per-symbol)
engine/long_range_sensors.py L138-180          — _get_volume_data_yahoo per-symbol fallback, 1s throttle (worst case ~668s)
```

### 4. Per-symbol for-loops across engine/ (potential migrate targets)

```
alpha_signals.py    L556, L936, L993, L1301, L1545   — 5 ALPHA_UNIVERSE loops
high_iv_scanner.py  L136                              — yf.Ticker per sym
long_range_sensors.py L107, L140                      — bars per sym, Yahoo fallback per sym
trend_predictor.py  L214                              — per-symbol loop
bull_bear.py        L136                              — per-symbol loop
sentiment.py        L142                              — get_sentiment_for_symbol per sym
stock_fundamentals.py L347                            — per-symbol loop
fundamental_score.py L309                             — per-symbol loop
deep_scan.py        L473                              — per-symbol loop
impulse_detector.py L231                              — analyze_impulse per sym (parallel)
realtime_monitor.py L241                              — per-symbol loop
gex_overlay.py      L609, L673                        — per-symbol loop
master_backtest.py  L400, L1325, L1876, L2056         — MASTER_UNIVERSE loops (backtest only, not live cycle)
```

### 5. The profile step from CD.0 step 5 — did not run

The directive's profile snippet tried `from engine.ai_brain import scan_symbol/process_symbol/_scan_one`. All three ImportError. Directive's explicit fallback: "❌ No standard per-symbol function found — Scotty: locate from grep above and inline" — i.e. Captain anticipated this and is asking for the inline location. **Located:** `engine/crew_scanner.py:_scan_single_agent` (617 LOC; calls each agent's individual scan function — too coupled to live trading to profile out-of-process without risking phantom trades, Ollama queue contention, and DB writes against the running pid 38990 trader).

### 6. No native cycle-time instrumentation

`grep -nE "time\.time\(\)|perf_counter|elapsed" engine/crew_scanner.py` returns 30+ hits — all caching/throttling timers (`_last_ollama_query`, `_warm_at`, etc.). **None measure cycle wall time.** The "31 min full cycle" figure has no native log source — must have come from external observation.

`tail -1000 logs/trader.log | grep -iE "scan.*took|cycle.*took|agents_scanned"` returned **zero matches**. Either no cycle has completed in the post-restart window (we just restarted at 16:00 AZ for HM-BR), market is closed (US closed at 13:00 AZ Mountain Time today), or the cycle log line uses different wording.

## Q1 — Captain decision

Given the architecture mismatch, the original "profile scan_symbol(IBM), migrate top-3 hot calls to Polygon" plan can't run as-spec. Three reframings:

### Option A (recommended) — Per-agent profiling

1. Add `time.perf_counter()` brackets around each `_scan_single_agent(player_id, ctx)` call in `_run_scan_cycle_body`. Anchor `# === HM-CD.timing ===`.
2. Add `# === HM-CD.cycle ===` total wall-time logger at top + bottom of `_run_scan_cycle_body`.
3. Ship + restart + observe one market-hours cycle → identify the 1-2 slowest agents.
4. Profile each slow agent's internals (`alpha_signals` ALPHA_UNIVERSE loops, `high_iv_scanner.py:136 yf.Ticker` loop, etc.).
5. Migrate that agent's specific bottleneck to Polygon/batched-Alpaca.

**Pros:** Data-driven, matches architecture, narrow blast radius per ship.
**Cons:** Needs market hours to observe a live cycle. Today is post-close.

### Option B — Mechanical yfinance sweep

Migrate all 5 yfinance hot spots in one batch:
- `high_iv_scanner.py:33` — `yf.Ticker(symbol)` → Polygon `/v3/snapshot/options/{sym}`
- `long_range_sensors.py:138-180` — Yahoo fallback → Polygon aggregates
- `squeeze_scanner.py:74-113` — yfinance fallback → already-migrated Alpaca path, remove dead code
- `insider_tracker.py:21-22` — `yf.Ticker(symbol).insider_*` → Polygon `/v2/reference/insider-trades` if available, else Alpha Vantage
- `holodeck_expansion.py:49-154` — `yf.download` → Polygon aggregates batch

**Pros:** No live observation needed. Mechanical, parallelizable per file. Each is a discrete commit.
**Cons:** May not be the actual bottleneck. Could spend 2 hrs migrating cold paths.

### Option C — Instrument first, no migration this session

Just add the timing instrumentation from Option A and ship it. Defer all migrations until we have data from a real market-hours cycle. Lowest risk; longest path to the 15-min goal.

## HALT

Cannot proceed to CD.1 without Captain decision on A / B / C. The directive's literal CD.1 step ("Migrate the top 2-3 bottlenecks per Captain's Q1 answer") requires Q1 to first be answered with concrete bottleneck candidates, which need either A or C to actually measure.

ntfy fired.
