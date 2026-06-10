# HM-DRIFT-RECON — Backtest-to-Live Drift Reconciliation (SPEC ONLY, no execution)

**Status:** spec doc only — no code, no schema changes, no execution. **BUILD
gated on Admiral approval post-trip.** Zero code this turn.
**Pre-registered:** all bars below are fixed BEFORE any data is collected — no
goalpost moves after the first sweep (same discipline as
`drafts/HM-FORGE-PHASE4-AB-SCORECARD-SPEC.md`).

## 1. Purpose
Catch **post-graduation decay**: a strategy that cleared its pre-graduation
gates (PBO ≤ 0.30 AND DSR ≥ 0.95, `strategies/validation.py`) but whose **live**
performance has since drifted materially below its **backtest baseline**. PBO
catches fragility *before* graduation; this catches it *after*. **Report-only** —
the monitor never halts or resizes anything (halt is an Admiral key).

## 2. Scope — graduated strategies with a backtest baseline (from records, not memory)

Enumerated from disk on 2026-06-10. **Two facts shape this scope:**

**(a) The canonical reconciliation table already exists but is EMPTY.**
`trader.db::strategy_scores` has exactly the right shape —
`(strategy_name, live_return, backtest_return, degradation, regime, status,
retire_reason, scored_at)` — and **0 rows**. `holodeck_backtest_results`
(`strategy_name, win_rate_pct, sharpe, edge_vs_bah, …`) is also **0 rows**.
So there is **no single populated table** mapping graduated strategy →
backtest baseline today. *(This is itself a drift instance — "graduated with a
baseline" claimed, but no populated baseline table on disk — and is the natural
first WARN row HM-DRIFT-MONITOR (spec #4) would emit.)*

**(b) Baselines that DO exist are scattered across run tables:**
`strategy_backtests` (1,124 rows, `strategy_type`), `backtest_results` (936,
player×ticker), `backtest_runs` (57, e.g. `v6_5day_20260412_baseline`),
`backtest_history` (278), `strategy_lab_results` (3,779, e.g.
`rsi_mean_reversion`, `momentum`, `buy_the_blood`).

**Enumerated graduated/live strategies in scope (the actual list per records):**

| Strategy | Baseline source on disk | Live source | Notes |
|----------|------------------------|-------------|-------|
| `the_continuation` | OOS-validated: Sharpe **1.47**, **58% WR**, +5.6%/6wk, tuned 8% stop / 6% tgt / 20d hold (`docs/XO_BACKLOG.md:46`) | holly-scanner live fills | **BULL-ONLY**: +1.57%/trade 66% WR in bull, ~flat (+0.10%/trade) otherwise (`XO_BACKLOG.md:49`) → **regime-segment the comparison** |
| `count_de_monet` | OOS Sharpe **0.59** (marginal) (`XO_BACKLOG.md:47`) | holly-scanner | currently OFF / marginal — include only if live n≥20 accrues |
| `bull_call_spread_v1` | `strategies` table (enabled=1); spread A/B backtests in `strategy_backtests` | live spread executor | adaptive debit/credit by IV rank |
| `bull_spread_v1` | `strategies` table (enabled=1) | live | mixed-DTE A/B |
| `bear_put_spread_v1` | `strategies` table (enabled=1) | live | adaptive bear spread |
| `SUPER_MAX W0+W1` | expectancy gate: BUY **+0.43R @ 5d** (shipped 2026-05-31, `signal_outcomes`⟷`trade_signals`) | `scored_predictions` realized R | W2/W3 still spec-gated — out of scope until graduated |

**STEP 0 of the build (pre-req, not a metric):** establish the canonical
`(strategy → baseline)` map. The build's first job is to **populate
`strategy_scores`** (or a read-side view over the scattered run tables) so the
recon has one authoritative baseline per strategy. **No comparison runs until a
baseline source is registered for that strategy** — a missing baseline is
reported as `baseline_missing`, never silently treated as "no drift."

## 3. Metrics — pre-registered, rolling 30 calendar days

Computed **per strategy**, rolling **30d**, requiring **n ≥ 20 live trades**.
Where a strategy is regime-conditional (e.g. `the_continuation` BULL-ONLY), the
comparison is segmented to its **graduated regime** — comparing live bull-regime
trades to the bull-regime backtest, never blended.

| ID | Metric | Breach condition | Tier |
|----|--------|------------------|------|
| **T1** | Win-rate breach | live WR **< backtest WR − 15pp** | breach |
| **T2** | R breach | live avg R **< 0.6 × backtest avg R** | breach |
| **n-gate** | Sample size | **n < 20** live trades in window | **report sample size, NO verdict** (neither PASS nor breach) |

- Below n=20: emit `{strategy, n, status: "insufficient_sample"}` — no T1/T2
  evaluation, no WARN. Silence here is **not** "healthy."
- `baseline_missing` (Step 0 unresolved): emit `{strategy, status:
  "baseline_missing"}` — no verdict; this is a config-drift row, not a
  performance breach.

## 4. Action on breach — report-only (no auto-anything)

- **On T1 or T2 breach:** NTFY `ollietrades-admin` **WARN** + append one dated
  row to the ledger (`data/drift_recon_ledger_<YYYY-MM-DD>.md`, never
  overwritten/deleted, per sacred-data rule).
- **NO auto-halt. NO sizing change. NO rotation change.** Halt/resize is an
  **Admiral key** — the monitor's job is to *surface*, the Admiral's to *act*.
- Rationale: PBO/DSR gate fragility **pre**-graduation; this is the **post**-
  graduation watch. The two together bracket a strategy's whole lifecycle.

## 5. Cadence
- **Weekly sweep** (proposed: Sunday, ahead of the trading week) over all
  in-scope strategies.
- **On-breach immediate:** if the realized-outcome writer flips a strategy into
  breach intra-week, fire the WARN immediately rather than waiting for Sunday.
- Both paths are read-only over `scored_predictions` / `signal_outcomes` /
  trade tables.

## 6. Output schema — aligned to the kirk R3 sidecar naming

One JSON record per strategy per sweep, plus a markdown ledger. Field naming
matches `kirk_briefing.py` R3 sidecar + the Phase 4 grader: `schema_version`
int, `consumer: null` until a Reflexion reader wires in, snake_case keys, ISO
`*_at_az` timestamps.

```json
{
  "schema_version": 1,
  "consumer": null,
  "kind": "drift_recon",
  "strategy": "the_continuation",
  "window_days": 30,
  "regime_segment": "bull | all",
  "generated_at_az": "<ISO8601 America/Phoenix>",
  "n_live_trades": 0,
  "live_win_rate": null,
  "backtest_win_rate": null,
  "live_avg_r": null,
  "backtest_avg_r": null,
  "t1_wr_breach": false,
  "t2_r_breach": false,
  "status": "ok | breach | insufficient_sample | baseline_missing",
  "baseline_source": "<table/doc the baseline was read from>"
}
```

Window-level rollup (the sweep summary) is computed from these per-strategy
records at report time — not a separate emitted schema.

## 7. Safety posture
- **Read-only** over outcome/trade tables — **zero execution-path touch.** All
  four gates and **RULE #1 (Schwab hands-off)** unaffected.
- No `config.py` / agent-routing / doctrine change. A breach produces a WARN +
  ledger row only; any response is a separate, Admiral-keyed action.
- Sacred-data: ledgers + sidecars are **additive**; never overwrite or delete.

## 8. Known spec anomalies (flagged, not blockers)
1. **Empty canonical tables.** `strategy_scores` and `holodeck_backtest_results`
   are 0-row; Step 0 (populate/register baselines) is a hard pre-req before any
   T1/T2 evaluation can run. Until then every strategy reports `baseline_missing`.
2. **Regime-conditional baselines.** `the_continuation` is BULL-ONLY; a naive
   blended comparison would false-trigger in bear tape. The spec mandates
   regime-segmented comparison — this requires a per-trade regime tag on live
   fills (confirm the holly-scanner records one before build).
3. **Spreads lack a clean single baseline row.** `bull/bear_*_spread_v1`
   baselines live as A/B rows in `strategy_backtests` keyed by `strategy_type`,
   not a graduation record; Step 0 must pick the canonical baseline run per
   spread (the graduated config, not every A/B arm).

## 9. Out of scope (separate epics)
Auto-halt / auto-resize on breach (Admiral key); SUPER_MAX W2/W3 (still
spec-gated, not graduated); populating the historical backtest baselines for
strategies that never wrote one; the drift **taxonomy** itself (owned by spec #2
when it ships — this recon emits rows that taxonomy will later classify).

## 10. Relationship to other specs
- **Spec #2 (taxonomy):** HM-DRIFT-RECON breaches are one input *class* into the
  broader drift taxonomy; align `status` values when #2 lands.
- **Spec #4 (HM-DRIFT-MONITOR):** the empty-`strategy_scores` finding in §2(a)
  is exactly the **doc-claims-vs-DB-truth** class spec #4 monitors. The two
  specs are complementary: #4 watches *config/claims* drift daily; #3 watches
  *performance* drift weekly.
