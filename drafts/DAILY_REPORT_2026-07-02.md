# OLLIETRADES DAILY REPORT — 2026-07-02
_Generated 2026-07-03 05:00 UTC · gate: 50% (days-limited) · measurement: GREEN_

## 1. EDGE STATUS

**COLLECTING — not testable.**
Clean trading days: **10/20** · min bucket n: **1,347/400** · est. ready ~2026-07-18

*No edge established. Per-source returns are NOT actionable before the gate.*

## 2. GATE PROGRESS

| Metric | Value | Target | Status |
|--------|-------|--------|--------|
| Clean trading days | **10** | ≥20 | 10 remaining |
| n_under10 (proj <10%) | **2,649** | ≥400 | ✓ |
| n_over10  (proj ≥10%) | **1,347** | ≥400 | ✓ |
| n_null (no projection) | **11,618** | — | tracking |
| % to gate | **50% (days-limited)** | 100% | — |

## 3. PIPELINE VITALS

| Check | Value | Status |
|-------|-------|--------|
| Obs logged today | **223** | — |
| Realized fills today | **7** | — |
| Artifact-zeros | **0** | GREEN ✓ |
| Evaluator last run | **0.4h ago** | GREEN ✓ |
| Measurement health | — | **GREEN** |
| War room debates | **220** | — |
| Regime | CAUTIOUS_BEAR (21d · size 0.5x) | — |
| Error log | 0 real / 1050 total lines today | — |

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
| Real API spend today | **$1.3242** |
| Doctrine ceiling | $1.50 / day |
| Status | **✓ WITHIN DOCTRINE** |

*Note: phantom billing from shadow CSP seats fixed 2026-06-29; historical rows left intact.*

## 6. INCIDENTS / CHANGES

### Shipped
- `2f19ba5 docs: HANDOFF -- door1 centralized+activated, stress-test correction, test-pollution needs approval`
- `f4fb759 fix: centralize door1 leveraged-ETF CSP ban -- zero new writes, everywhere`
- `1cd7f24 docs: HANDOFF -- McCoy fix activated live, wheel risk rule drafted, tomorrow's follow-ups`
- `977079e docs: draft wheel leveraged-ETF risk rule (25% cap + regime gate), not applied`
- `7e4df58 feat: tag McCoy's post-fix exits for new-vs-old comparison, add startup verification log`
- `c47e34a docs: HANDOFF -- data pulls (win/loss asymmetry, wheel stress test) + McCoy label fix writeup`
- `5e6824b fix: Fleet Report Card P&L label mismatch vs Leaderboard (3rd recurrence of this bug class)`
- `18485fe docs: HANDOFF -- McCoy exit asymmetry fix, honest mixed backtest evidence, other-agents audit`
- `0f06b81 fix: McCoy exit asymmetry -- target-relative tiers replace flat +4% clip`
- `b35b489 docs: log Proving Ground kill -- ollie-auto terminated, Admiral-confirmed with rationale`

### Incidents
*(none — edit this section if something broke or was fixed outside git)*

## 7. LEDGER ROW

```
2026-07-02,10,2649,1347,11618,0,1,GREEN,1.3242,B+,D,B,C+,C+,A-,C+,B,C,A-
```
*Appended to drafts/daily_ledger.csv (append-only)*
