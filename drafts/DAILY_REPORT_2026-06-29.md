# OLLIETRADES DAILY REPORT — 2026-06-29
_Generated 2026-06-30 02:08 UTC · gate: 30% (days-limited) · measurement: GREEN_

## 1. EDGE STATUS

**COLLECTING — not testable.**
Clean trading days: **6/20** · min bucket n: **978/400** · est. ready ~2026-07-20

*No edge established. Per-source returns are NOT actionable before the gate.*

## 2. GATE PROGRESS

| Metric | Value | Target | Status |
|--------|-------|--------|--------|
| Clean trading days | **6** | ≥20 | 14 remaining |
| n_under10 (proj <10%) | **1,910** | ≥400 | ✓ |
| n_over10  (proj ≥10%) | **978** | ≥400 | ✓ |
| n_null (no projection) | **9,613** | — | tracking |
| % to gate | **30% (days-limited)** | 100% | — |

## 3. PIPELINE VITALS

| Check | Value | Status |
|-------|-------|--------|
| Obs logged today | **1,553** | — |
| Realized fills today | **7,217** | — |
| Artifact-zeros | **0** | GREEN ✓ |
| Evaluator last run | **0.8h ago** | GREEN ✓ |
| Measurement health | — | **GREEN** |
| War room debates | **293** | — |
| Regime | CAUTIOUS_BEAR (18d · size 0.5x) | — |
| Error log | 0 real / 69 total lines today | — |

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
| Real API spend today | **$3.2287** |
| Doctrine ceiling | $1.50 / day |
| Status | **✗ OVER DOCTRINE** |

*Note: phantom billing from shadow CSP seats fixed 2026-06-29; historical rows left intact.*

## 6. INCIDENTS / CHANGES

### Shipped
- `e1ad81c fix(cost): zero-rate wr-shadow-v1/v7d in TOKEN_RATES`
- `77be547 fix(env): load dotenv at module top — APCA + ANTHROPIC keys resolve`
- `deaf5eb feat(measurement): forward-collection live — realized returns, clean buckets`
- `615c4fe feat(carrier/p1): forward-collection staging — weekly snapshot + WHERE WE LANDED`
- `96a29c7 fix(carrier/p1): same-bar artifact → NULL not 0.0 in realized return`
- `915c35e feat(carrier/p1): realized-return rewire — Alpaca bars + backfill script`
- `4486885 fix(carrier): wire slotFlow to /api/market/flow-lean (per-sym + market-wide fallback)`

### Incidents
*(none — edit this section if something broke or was fixed outside git)*

## 7. LEDGER ROW

```
2026-06-29,6,1910,978,9613,0,1,GREEN,3.2287,B+,D,B,C+,C+,A-,C+,B,C,A-
```
*Appended to drafts/daily_ledger.csv (append-only)*
