# W5-C — regime_allocations schema extension + Blend E seed

**Date:** 2026-05-23
**Author:** HM-MASTER-PLAN W5-C (Captain authorized Option A — schema-only ship)
**Commit:** see git log

## Schema change

`data/trader.db` — `regime_allocations` table.

Five new columns added (all `REAL DEFAULT NULL`):

| Column                | Purpose                                  |
|-----------------------|------------------------------------------|
| `csp_pct`             | Cash-secured put sleeve target           |
| `bull_put_spread_pct` | Bull put spread sleeve target            |
| `momentum_pct`        | Momentum/trend-follow exposure (REDUCE)  |
| `long_call_pct`       | Long-call options exposure (REDUCE)      |
| `mean_reversion_pct`  | Mean-reversion stats exposure (REDUCE)   |

The 6 non-seeded regimes (BEAR_CHOPPY, BEAR_CROSS, BULL, BULL_LOW_VOL,
CAUTIOUS_BEAR, CAUTIOUS_BULL, EUPHORIC) retain their existing column
values and have NULL for all 5 new columns. NULL semantics: "not yet
specified" — distinct from 0.0 ("explicitly excluded by Captain").

## Captain Blend E seed (3 regimes)

Decimal-converted from percentage spec. Each regime sums to **1.0000** exact.

### BULL_CROSS — IC-heavy bull (sum 1.00)
- long_equity_pct: 0.15
- ic_pct: **0.25**
- csp_pct: **0.25**
- bull_put_spread_pct: **0.20**
- momentum_pct: 0.05
- long_call_pct: 0.05
- mean_reversion_pct: 0.05
- bear_call_spread_pct: 0.00
- hedge_pct: 0.00
- cash_pct: 0.00

Was: long_equity=0.65, ic=0.05, hedge=0, cash=0.30. Captain spec shifts
50pp out of long_equity into IC + CSP + bull_put_spread.

### BEAR — defensive with explicit hedge sleeve (sum 1.00)
- long_equity_pct: 0.05
- ic_pct: **0.30**
- csp_pct: **0.20**
- bull_put_spread_pct: **0.15**
- momentum_pct: 0.05
- long_call_pct: 0.00
- mean_reversion_pct: 0.05
- bear_call_spread_pct: 0.00
- hedge_pct: 0.20
- cash_pct: 0.00

Was: long_equity=0.10, ic=0.08, bear_call=0.15, hedge=0.10, cash=0.57.
Captain spec absorbs 57pp of cash into IC + CSP + bull_put + doubled hedge.

### CHOP — IC-dominant range regime (sum 1.00, NEW row)
- long_equity_pct: 0.05
- ic_pct: **0.40**
- csp_pct: **0.30**
- bull_put_spread_pct: **0.20**
- momentum_pct: 0.00
- long_call_pct: 0.00
- mean_reversion_pct: 0.05
- bear_call_spread_pct: 0.00
- hedge_pct: 0.00
- cash_pct: 0.00

⚠ **CHOP is a forward-compat regime key — NOT yet in
`engine.regime_router.REGIME_STRATEGY_MATRIX`.** Closest existing matrix
label is `BEAR_CHOPPY`. Until a producer surfaces "CHOP" via
`get_current_regime()`, this seed is informational only. Reconciling
the regime taxonomy (CHOP ↔ BEAR_CHOPPY) is a separate ticket worth
tracking — flag if/when a consumer references CHOP.

## Interpretation rule for unspecified buckets

For Captain-seeded regimes (BULL_CROSS, BEAR, CHOP): any bucket Captain
did NOT mention in the breakdown is set to **0.0** to maintain tight
100% sum. Existing values were replaced wholesale.

For non-seeded regimes (the other 6): existing column values preserved
unchanged; the 5 new columns are NULL.

## No enforcement layer this ship

The Blend E targets land in `regime_allocations` but are NOT yet wired
into the allocation policy. Per Captain plan: "No enforcement layer
yet — Captain will review and tune percentages."

Follow-up work (separate ticket):
- Seed the other 6 regimes (BULL, BEAR_CHOPPY, etc.) with Blend E-shaped
  defaults
- Add CHOP to `engine.regime_router.REGIME_STRATEGY_MATRIX` if the
  forward-compat seed is intentional
- Wire the new columns into `paper_trader.get_capital_allocation_policy`
  or a new `apply_blend_e_allocation()` enforcement helper
- Per-agent strategy classification (which active agent maps to which
  bucket: momentum / long_call / mean_reversion / IC / CSP / etc.)

## Backup

Full DB backup before mutation:
`backups/trader.db.pre-w5c-blend-e-schema-20260523_081658` (~373MB)

## Rollback

```bash
# Full restore
cp backups/trader.db.pre-w5c-blend-e-schema-20260523_081658 data/trader.db
# Or surgical reverse (drop new columns is non-trivial in SQLite;
# easier to drop the seeded rows and let app code tolerate NULLs):
sqlite3 data/trader.db "
  UPDATE regime_allocations SET csp_pct=NULL, bull_put_spread_pct=NULL,
    momentum_pct=NULL, long_call_pct=NULL, mean_reversion_pct=NULL;
  DELETE FROM regime_allocations WHERE regime='CHOP';
  -- Restore BULL_CROSS + BEAR to pre-seed values per pre-mutation snapshot.
"
```
