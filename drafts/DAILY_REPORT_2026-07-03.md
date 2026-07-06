# OLLIETRADES DAILY REPORT — 2026-07-03
_Generated 2026-07-04 05:00 UTC · gate: 50% (days-limited) · measurement: GREEN_

## 1. EDGE STATUS

**COLLECTING — not testable.**
Clean trading days: **10/20** · min bucket n: **1,371/400** · est. ready ~2026-07-19

*No edge established. Per-source returns are NOT actionable before the gate.*

## 2. GATE PROGRESS

| Metric | Value | Target | Status |
|--------|-------|--------|--------|
| Clean trading days | **10** | ≥20 | 10 remaining |
| n_under10 (proj <10%) | **2,742** | ≥400 | ✓ |
| n_over10  (proj ≥10%) | **1,371** | ≥400 | ✓ |
| n_null (no projection) | **11,691** | — | tracking |
| % to gate | **50% (days-limited)** | 100% | — |

## 3. PIPELINE VITALS

| Check | Value | Status |
|-------|-------|--------|
| Obs logged today | **198** | — |
| Realized fills today | **0** | — |
| Artifact-zeros | **0** | GREEN ✓ |
| Evaluator last run | **0.1h ago** | GREEN ✓ |
| Measurement health | — | **GREEN** |
| War room debates | **0** | — |
| Regime | CAUTIOUS_BEAR (22d · size 0.5x) | — |
| Error log | 0 real / 180 total lines today | — |

## 4. REPORT CARD

| # | Category | Grade | Δ / Reason |
|---|----------|-------|------------|
| 1 | Dashboard / Command UI | **B+** | — |
| 2 | Signal Generation / Scanners | **D** | — |
| 3 | Execution / Trade Pipeline | **B** | — |
| 4 | Performance / Alpha Measurement | **C+** | — |
| 5 | Fleet / Agents | **C+** | — |
| 6 | Risk / Safety Controls | **A-** | — |
| 7 | Monitoring / Alerting | **C+** | — |
| 8 | Data / Source Health | **B** | — |
| 9 | Models / Plutus | **C** | — |
| 10 | Resilience / Departure-Readiness | **A-** | — |

## 5. COST

| Item | Value |
|------|-------|
| Real API spend today | **$0.0000** |
| Doctrine ceiling | $1.50 / day |
| Status | **✓ WITHIN DOCTRINE** |

*Note: phantom billing from shadow CSP seats fixed 2026-06-29; historical rows left intact.*

## 6. INCIDENTS / CHANGES

### Shipped
- `49902bc bootstrap_intelligence: fix nightly crash + batch the write, gate first population`
- `7c82202 db-lock retry: breadth/sector snapshot writes retry with backoff past busy_timeout`
- `303b9d9 scanner: Race tile holiday-aware status + %-change bounds guard`
- `69d04c9 backtester: surface raw-mode reentry_blocked + friction_paid counters`
- `7ca21c9 scanner ghost-fix: stale mover-join guards, T-format 90min window leak, stale_plan flag, market_open gate`
- `95d0055 backtest realism: dispatch staleness, reentry guard, friction; swing stale budget 30s->3600s`

### Incidents
*(none — edit this section if something broke or was fixed outside git)*

## 7. LEDGER ROW

```
2026-07-03,10,2742,1371,11691,0,1,GREEN,0.0000,B+,D,B,C+,C+,A-,C+,B,C,A-
```
*Appended to drafts/daily_ledger.csv (append-only)*
