# W5-C — remaining regime_allocations seed

**Date:** 2026-05-23
**Author:** HM-MASTER-PLAN W5-C (Captain directive: seed remaining regimes)
**Commit:** see git log
**Precedent:** `2026-05-23_w5c_regime_allocations_blend_e.md` (initial schema + 3-regime Blend E seed in 81432b9)

## What shipped

Seeded 5 additional regimes in `regime_allocations`:

| Regime          | Action | long_equity | ic   | csp  | bull_put | bear_call | momentum | long_call | mean_rev | hedge | cash | le_max | Notes |
|-----------------|--------|-------------|------|------|----------|-----------|----------|-----------|----------|-------|------|--------|-------|
| EUPHORIC        | UPDATE | 0.50        | 0.15 | 0.10 | 0.05     | 0.00      | 0.05     | 0.10      | 0.00     | 0.00  | 0.05 | 0.65   | Bull-tilted, light IC, some long_call upside |
| CAUTIOUS_BULL   | UPDATE | 0.40        | 0.20 | 0.15 | 0.10     | 0.00      | 0.05     | 0.05      | 0.00     | 0.05  | 0.00 | 0.55   | Risk-on but IC sleeve grows |
| BEAR_CHOPPY     | UPDATE | 0.10        | 0.35 | 0.25 | 0.10     | 0.10      | 0.00     | 0.00      | 0.05     | 0.05  | 0.00 | 0.30   | IC + CSP dominant; bear_call sleeve |
| SIDEWAYS        | INSERT | 0.15        | 0.35 | 0.25 | 0.10     | 0.05      | 0.00     | 0.00      | 0.10     | 0.00  | 0.00 | 0.30   | Range-bound — IC-heavy, mean-rev tilt. **Forward-compat: not yet emitted by regime_router** |
| VOLATILE        | INSERT | 0.00        | 0.30 | 0.10 | 0.05     | 0.05      | 0.00     | 0.00      | 0.00     | 0.20  | 0.30 | 0.10   | High-VIX preservation — heavy cash + hedge, IC captures vol premium. **Forward-compat: not yet emitted by regime_router** |

Every row sums to **1.0000** (verified post-write).

## Blend E philosophy applied

Per W5-C precedent and Captain's "IC + CSP + bull_put_spread dominant in bear/chop, long_equity allowed in BULL/EUPHORIC":

- **Bull-tilted (EUPHORIC, CAUTIOUS_BULL):** long_equity remains the dominant sleeve but IC grows materially (0.15–0.20) and a CSP sleeve appears (0.10–0.15). Long-call exposure preserved for upside leverage.
- **Bear / choppy (BEAR_CHOPPY):** IC takes the largest sleeve (0.35). CSP second (0.25). bull_put_spread + bear_call_spread provide directional credit-spread exposure. Long_equity drops to 0.10 with explicit hedge sleeve.
- **Range-bound (SIDEWAYS):** IC-dominant (0.35) plus mean-reversion (0.10) for stat-arb against the range. No directional bias.
- **Vol blowoff (VOLATILE):** zero long_equity, IC captures vol premium (0.30), cash + hedge total 0.50 for capital preservation.

## Forward-compat caveat

`SIDEWAYS` and `VOLATILE` are **not currently produced** by
`engine.regime_router.REGIME_STRATEGY_MATRIX` (verified by grep — only
`holodeck_expansion.py` references `SIDEWAYS` as a backtest partition
label). Seeded today so future code can opt into them without a
separate schema edit. Until a producer emits these labels via
`get_current_regime()`, the rows are informational only.

Per the W5-C-Blend-E doc, the same caveat applies to `CHOP` (seeded
2026-05-23 in 81432b9). Both `CHOP` and the new `SIDEWAYS` describe
similar regimes — reconciling the taxonomy (`CHOP` ↔ `SIDEWAYS` ↔
existing `BEAR_CHOPPY`) is a separate ticket if/when a consumer
references either name.

## What's still NULL

The 4 regimes whose W5-C-new columns remain `NULL` (not yet specified):

- `BEAR_CROSS`
- `BULL`
- `BULL_LOW_VOL`
- `CAUTIOUS_BEAR`

Per W5-C NULL semantics, `NULL ≠ 0.0`. NULL means "Captain hasn't
specified yet"; 0.0 means "explicitly excluded." These 4 keep their
HM-IC-SQUADRON-era column values and will be seeded in a future wave
when Captain has the bandwidth.

## No enforcement layer this ship

Same posture as the original W5-C: values land in the table for
review/tune. The `paper_trader.get_capital_allocation_policy` /
`apply_blend_e_allocation()` wiring is still a separate follow-up.

## DB backup

`data/trader.db.bak_W5-C-REMAINING_20260523_095444` (373 MB)
preserved per sacred-data discipline. Restore command:

```bash
cp data/trader.db.bak_W5-C-REMAINING_20260523_095444 data/trader.db
launchctl kickstart -k gui/$(id -u)/com.trademinds.trader   # or via wrapper
```

## Verification

```sql
SELECT regime,
       ROUND(long_equity_pct + ic_pct + COALESCE(csp_pct,0)
             + COALESCE(bull_put_spread_pct,0) + COALESCE(momentum_pct,0)
             + COALESCE(long_call_pct,0) + COALESCE(mean_reversion_pct,0)
             + bear_call_spread_pct + hedge_pct + cash_pct, 4) AS row_sum
  FROM regime_allocations
 WHERE regime IN ('EUPHORIC','CAUTIOUS_BULL','BEAR_CHOPPY','SIDEWAYS','VOLATILE');
-- All return 1.0
```
