# HM-CLOSE-GAP Wave 2 — Consolidated Captain Qs

**Date:** 2026-05-12
**Phase:** AN2.0 → 4 ticket Phase-1 discoveries; HALT for Captain
**Auditor:** Scotty (Opus 4.7)

Four Phase-1 discoveries below, each with one explicit Captain question.

---

## HM-BM — Recon Aggregator Semantic Fix

### Findings
- `engine/reconciliation.py` (677 LOC) — the by-design comment at L186 confirms: *"alpaca-mirror is by-design 1:1 with Alpaca"*.
- `engine/paper_trader.py:1531` lists alpaca-mirror in `_BROKER_MIRROR_PLAYERS` set; L1595 lists it in `_ALLOCATION_POLICY_EXEMPT`.
- Today's recon JSON (`data/reconciliation/2026-05-12.json`):
  - `routed_drift.in_both_qty_mismatch`: non-empty (drift candidates)
  - `routed_drift.in_alpaca_not_internal`: non-empty (mirror-only positions)
  - `unrouted_drift: {qwen3-8b-flash: 1}` — only 1 unrouted drift, not the 6 from origin claim
  - `summary.internal_position_count: 50`, `alpaca_position_count: 10`
- The 6 fractional positions (LLY/MA/SPGI/UNH/WMT/XOM) from origin not visible in today's payload — either resolved organically or surfaced via different schema reading.

### Captain Q1
Three options from HM-BM.md still apply:
- **A** exclude alpaca-mirror from internal book sum (mirror equals Alpaca by definition — least intrusive fix)
- **B** compare ollie-auto only to Alpaca (ollie-auto becomes canonical intent book; mirror purely observational)
- **C** re-architect: single canonical position record per (player, symbol), no mirror

**Scotty recommends A** — engine/reconciliation.py:186 already documents the mirror's 1:1 invariant; excluding it from the sum honors that invariant explicitly instead of letting circular comparison inflate "drift" that isn't real divergence.

---

## HM-BN — Proving Ground Window Enforcement

### Findings
- `engine/proving_ground.py` shipped `running_scorecard` schema at L91 (with cohort metadata + bench-status columns).
- `INSERT OR REPLACE INTO running_scorecard` at L357 — daily upsert, no Day-30 cap, no graduation event.
- Reading the file end-to-end: **no `graduate()` function, no `cohort_complete` event, no Day-30 hard halt.** The `/30` suffix is purely the *window length* used for rolling metrics computation, not a cap.
- Data confirms: today Day 33, total_trades 272, scorecard rows go back continuously. Captain's earlier directive HM-BN-ANSWERS.md (if exists) would supersede.

### Captain Q2
Two options:
- **A** Hard HALT at Day 30 — emit graduation NTFY summarizing the 6 benchmark statuses, lock further appends to `running_scorecard` until Captain starts a new cohort
- **B** Relabel to "rolling 30-day window" — Day counter keeps incrementing; `/30` indicates the metric window length, not a cap. Bench statuses are evaluated as rolling-window snapshots

**Scotty recommends B** — matches the actual code behavior. The `/30` log suffix is misleading but the underlying compute is already rolling-window. Option A would require new event-emission + halt code; Option B is a single log-line label change ("Day 33/30" → "rolling 30d window, Day 33").

---

## HM-BP — Max Drawdown Calculation Validation

### Findings
- Calc at `engine/proving_ground.py:218-225` is mathematically correct:
  ```python
  peak = equity
  max_dd = 0.0
  for pct in pnl_pcts:
      equity += equity * pct / 100.0
      peak    = max(peak, equity)
      dd      = (equity - peak) / peak * 100.0
      max_dd  = min(max_dd, dd)  # monotonically more negative
  ```
- max_drawdown UNIT: **percentage** (stored as -87.557, meaning -87.557%).
- DD behavior: **accumulates monotonically** — once -87.557 hit, never recovers even if equity rebounds. This is by design (max-historical-dd) but semantically the value is "worst-ever" not "current".
- Statistics across `running_scorecard`: min=-87.557, max=0.0, avg=-51.11, count=19. Recent 5 days all show -87.557 (stuck).
- An -87.5% real drawdown would have wiped the paper book. Cohort still trading 76.5% WR. **Therefore the value cannot reflect live-fleet behavior.**
- Most likely: `pnl_pcts` ingestion is **polluted by backtest/IS trades** crossing into live cohort accounting, OR an early-cohort fat-finger entry that locked in a phantom -87% pct before the recent stable period.

### Captain Q3
- **A** Treat -87.557 as real → tighten risk gates, investigate Day 14-15 root cause (origin ticket cross-ref), possibly invalidate the graduation
- **B** Calc-side bug → audit `pnl_pcts` source; recompute historical scorecard with live-only trades; re-evaluate `dd_bench_status` across all cohort days

**Scotty recommends B** — given paper-book continuity at 76.5% WR + 272 trades, an -87.5% real drawdown is not consistent with observable fleet state. Cause is almost certainly polluted pnl input (most likely a single legacy trade with a wrong entry_price denominator).

---

## HM-BQ — HM-AS-β Cadence Drift Root Cause

### Findings
- Detector at `main.py:1006-1024` (battle_station_monitor) + `main.py:1409-1423` (squeeze_watcher).
- Drift metric: `time.time() - last_fire_ts` vs target interval. Warning threshold: 180s for 120s-target (1.5x cap).
- 149 occurrences in trader_error.log (origin said 146 — drift since).
- Recent drift samples:
  - battle_station: 930s, 1399s, 1403s, 2614s, 3637s (target 120s — 7-30x late)
  - squeeze_watcher: 3704s, 3728s, 3970s (target 1800s — ~2x late)
- battle_station_monitor runs in the **main scheduler loop**; squeeze_watcher runs as a **daemon thread** (separate). Both drift — pattern is NOT thread-bound.
- Drift clusters around 14:00-15:30 + 18:00-20:00 (heavy backtest / after-hours batch windows).

### Root-cause candidates
- **A** Backtest CPU starvation — daily backtest blocking the worker thread
- **B** Ollama inference blocking — LLM calls running on bigmac CPU instead of routing to Ollie Box
- **C** Scheduler queue design — single-threaded `schedule.run_pending()` serializes; slow handler stalls everything
- **D** External I/O blocking — yfinance/Polygon timeouts holding the loop

### Captain Q4
Cannot pick a single root cause without correlation data (CPU samples + per-handler timing). Scotty recommends:
- **First step**: ship per-handler timing instrumentation (similar to HM-CD-instr pattern — wrap `schedule.run_pending()` calls in `time.perf_counter()` brackets, log `[HM-AS-β-instr]` per-handler wall)
- **After observation**: pick A/B/C/D based on which handler shows the longest wall

Captain decision: **(i) ship per-handler instrumentation as Phase 1 of HM-BQ?** Yes/no. If yes, scope similar to HM-CD-instr (cc5da70). If no, Captain picks A/B/C/D blind based on operational intuition.

---

## Summary — 4 Captain Qs queued

| Ticket | Q | Scotty rec |
|---|---|---|
| HM-BM | A/B/C — recon scope change | **A** (exclude alpaca-mirror) |
| HM-BN | A/B — hard halt vs rolling window | **B** (relabel only) |
| HM-BP | A/B — real DD vs calc bug | **B** (audit pnl input) |
| HM-BQ | i: instrument first vs pick blind | **instrument first** (mirror HM-CD-instr) |

All four Captain Qs are independent. Captain can answer in any order; each ticket ships independently in a future session.

Wave 2 closes here per directive; ntfy fired. Wave 3 (daily watch automation) proceeds without halting on these Qs.
