# L4 — CPU-Era Data Contamination

**Issue:** Anything measured before 2026-05-13 08:13 AZ is CPU-baseline,
not GPU-baseline. Don't trust pre-08:13 wall times for tuning decisions.

**Affects:**
- HM-CD-instr cycle measurements
- HM-BQ-instr handler walls
- Today's daily-watch P&L analysis (some agents likely skipped trades due to CPU latency)
- HM-AN2 candidate volume (pre-GPU was constrained by Ollama timeouts)

## Tier 1 action — Add data quality marker (Scotty can do solo)

Edit relevant SQL views / scripts to tag data as `cpu_era` or `gpu_era`:

```sql
-- Add a column to running_scorecard tracking computation epoch
ALTER TABLE forecast_scorecards ADD COLUMN data_epoch TEXT DEFAULT 'gpu';
ALTER TABLE signal_scorecard ADD COLUMN data_epoch TEXT DEFAULT 'gpu';

-- Backfill: all measurements before 2026-05-13 08:13 are 'cpu'
UPDATE forecast_scorecards SET data_epoch='cpu'
WHERE created_at < '2026-05-13 08:13:00';

UPDATE signal_scorecard SET data_epoch='cpu'
WHERE created_at < '2026-05-13 08:13:00';
```

Then update queries that compute baselines to filter `WHERE data_epoch='gpu'`
so we're tuning against current-reality data, not historical CPU constraints.

## Tier 2 — Re-baseline observation period

The 30-day rolling scorecard has 491 trades. Most were CPU-era. Either:
- Wait 30 days for natural rollover to GPU-era data
- Mark scorecard as "cpu-era baseline" and start a fresh GPU-era baseline

**Recommend:** Add a `gpu_era_scorecard` view that filters to post-08:13 data.
Original scorecard remains for historical comparison; new view drives decisions.

Estimated effort: 20 min for the view + backfill.

## What NOT to do

Don't delete or modify historical trade data — those records are sacred per
the no-delete rule. Add metadata columns, don't overwrite.
