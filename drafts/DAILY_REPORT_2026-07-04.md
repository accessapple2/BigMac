# OLLIETRADES DAILY REPORT — 2026-07-04
_Generated 2026-07-04 17:11 UTC · gate: 50% (days-limited) · measurement: RED_

## 1. EDGE STATUS

**COLLECTING — not testable.**
Clean trading days: **10/20** · min bucket n: **1,403/400** · est. ready ~2026-07-20

*No edge established. Per-source returns are NOT actionable before the gate.*

## 2. GATE PROGRESS

| Metric | Value | Target | Status |
|--------|-------|--------|--------|
| Clean trading days | **10** | ≥20 | 10 remaining |
| n_under10 (proj <10%) | **2,858** | ≥400 | ✓ |
| n_over10  (proj ≥10%) | **1,403** | ≥400 | ✓ |
| n_null (no projection) | **13,069** | — | tracking |
| % to gate | **50% (days-limited)** | 100% | — |

## 3. PIPELINE VITALS

| Check | Value | Status |
|-------|-------|--------|
| Obs logged today | **1,550** | — |
| Realized fills today | **0** | — |
| Artifact-zeros | **0** | GREEN ✓ |
| Evaluator last run | **4.3h ago** | RED ✗ |
| Measurement health | — | **RED** |
| War room debates | **0** | — |
| Regime | CAUTIOUS_BEAR (23d · size 0.5x) | — |
| Error log | 0 real / 4 total lines today | — |

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
- `75b63f1 troi guardrails: CSP notional visibility + cap gate on new opens`
- `c1dd786 backtester: MIN_HOLD_DAYS -> live 24h/72h-swing parity`
- `00c4978 docs: XO_BACKLOG entries for rules-consolidation document-only items + tickets`
- `9d3e097 base.py: retire Sulu's DayBlade persona, Iron Condor King canonical`
- `f9e3a4c main.py: Tier-1 roster sweep + scan-interval docstring fix; halt_gate.py: fix stale is_halted docstring`
- `9b3767f risk_manager: remove stale 0.08 conviction-stop staticmethod (live bug, not dead code)`
- `2787efa config/risk_manager/base: align position/cash/stop/options numbers to canonical`
- `acd62d1 trading_rules.txt: rewrite for the options era, bake in canonical numbers`
- `a384667 guardian_sweep: extend exit-only stop coverage to all agents, not just guardian-of-forever`

### Incidents
*(none — edit this section if something broke or was fixed outside git)*

## 7. LEDGER ROW

```
2026-07-04,10,2858,1403,13069,0,0,RED,0.0000,B+,D,B,C+,C+,A-,C+,B,C,A-
```
*Appended to drafts/daily_ledger.csv (append-only)*
