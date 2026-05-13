# L4 — data_epoch tagging applied 2026-05-13

ALTER TABLE applied to forecast_scorecards and signal_scorecard.
Column: data_epoch TEXT DEFAULT 'gpu'
Backfill: rows with timestamp before 2026-05-13 08:13 AZ tagged 'cpu'.

## Usage

Future scorecards / baselines should filter:
```sql
WHERE data_epoch = 'gpu'
```
for honest post-recovery measurements.
