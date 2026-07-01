# OLLIETRADES DAILY REPORT — 2026-06-29
_Generated 2026-06-30 05:00 UTC · gate: 35% (days-limited) · measurement: GREEN_

## 1. EDGE STATUS

**COLLECTING — not testable.**
Clean trading days: **7/20** · min bucket n: **981/400** · est. ready ~2026-07-19

*No edge established. Per-source returns are NOT actionable before the gate.*

## 2. GATE PROGRESS

| Metric | Value | Target | Status |
|--------|-------|--------|--------|
| Clean trading days | **7** | ≥20 | 13 remaining |
| n_under10 (proj <10%) | **1,912** | ≥400 | ✓ |
| n_over10  (proj ≥10%) | **981** | ≥400 | ✓ |
| n_null (no projection) | **10,685** | — | tracking |
| % to gate | **35% (days-limited)** | 100% | — |

## 3. PIPELINE VITALS

| Check | Value | Status |
|-------|-------|--------|
| Obs logged today | **1,553** | — |
| Realized fills today | **7,217** | — |
| Artifact-zeros | **0** | GREEN ✓ |
| Evaluator last run | **0.4h ago** | GREEN ✓ |
| Measurement health | — | **GREEN** |
| War room debates | **304** | — |
| Regime | CAUTIOUS_BEAR (18d · size 0.5x) | — |
| Error log | 0 real / 70 total lines today | — |

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
| Real API spend today | **$3.3589** |
| Doctrine ceiling | $1.50 / day |
| Status | **✗ OVER DOCTRINE** |

*Note: phantom billing from shadow CSP seats fixed 2026-06-29; historical rows left intact.*

## 6. INCIDENTS / CHANGES

### Shipped
- `e336667 chore: gitignore runtime data dirs (xo_briefs, signals, learning)`
- `d248936 fix(witness-ab): correct qwen3.6:14b → qwen3:14b in library hold taxonomy`
- `87f899f chore(witness-ab): add model taxonomy comment block to scorer`
- `3561f8b feat(witness-ab): shadow A/B witness gpt-oss:20b + deepseek-r1:14b vs McCoy`
- `aa9fe90 feat(reporting): daily report generator + ledger + grades`
- `e1ad81c fix(cost): zero-rate wr-shadow-v1/v7d in TOKEN_RATES`
- `77be547 fix(env): load dotenv at module top — APCA + ANTHROPIC keys resolve`
- `deaf5eb feat(measurement): forward-collection live — realized returns, clean buckets`
- `615c4fe feat(carrier/p1): forward-collection staging — weekly snapshot + WHERE WE LANDED`
- `96a29c7 fix(carrier/p1): same-bar artifact → NULL not 0.0 in realized return`

### Incidents
*(none — edit this section if something broke or was fixed outside git)*

## 7. LEDGER ROW

```
2026-06-29,7,1912,981,10685,0,1,GREEN,3.3589,B+,D,B,C+,C+,A-,C+,B,C,A-
```
*Appended to drafts/daily_ledger.csv (append-only)*
