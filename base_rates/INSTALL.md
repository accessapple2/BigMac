# Install on bigmac — step by step

## 0. What you'll do

1. SCP the tarball from laptop → bigmac
2. Extract into `~/autonomous-trader/`
3. Install one new dep (yfinance — already likely there)
4. Apply schema to signals.db
5. Run a quick smoke test against a tiny universe (5 tickers, ~2 min)
6. If smoke passes, kick off the full ingest

All commands assume you're SSH'd in: `ssh bigmac@192.168.1.248`

---

## 1. Copy the package over

From your laptop:

```bash
# Replace the path below with wherever you saved the tarball
scp ~/Downloads/base_rates_v0.1.0.tar.gz bigmac@192.168.1.248:/tmp/
```

## 2. SSH in and extract

```bash
ssh bigmac@192.168.1.248
cd ~/autonomous-trader
tar -xzf /tmp/base_rates_v0.1.0.tar.gz
ls base_rates/   # should show: base_rates/ tests/ universe.txt README.md
```

## 3. Install dependency

```bash
cd ~/autonomous-trader
# pandas + numpy already installed; only yfinance + pytest may be missing
pip install yfinance pytest --quiet
```

## 4. Run unit tests (sanity)

```bash
cd ~/autonomous-trader/base_rates
python -m pytest tests/ -v
```

Expect: **19 passed**. If anything fails, stop and report.

## 5. Apply schema to signals.db

Find your signals.db path first:

```bash
ls -lh ~/autonomous-trader/signals.db 2>/dev/null || find ~/autonomous-trader -name "signals.db" 2>/dev/null
```

Then apply:

```bash
cd ~/autonomous-trader/base_rates
python -m base_rates.migrate --db ~/autonomous-trader/signals.db
```

Expect: `[migrate] schema applied to /Users/.../signals.db`. Idempotent — safe to re-run.

## 6. Smoke ingest (5 tickers, 5 years)

Build a tiny universe to test end-to-end without waiting:

```bash
cd ~/autonomous-trader/base_rates
cat > smoke_universe.txt <<EOF
SPY
AAPL
NVDA
COIN
AAOI
EOF

python -m base_rates.ingest \
  --universe smoke_universe.txt \
  --db ~/autonomous-trader/signals.db \
  --years 5
```

Expect: ~60-90 seconds. Output should show `[1/5] SPY ... wrote ~1250 rows` etc.

## 7. Smoke query

```bash
python -m base_rates AAPL --db ~/autonomous-trader/signals.db --min-n 10
```

Expect: a clean text report with N, win rate, median return, etc.

## 8. Full ingest (background)

If smoke worked, kick off the real one. ~50 tickers × 20 years ≈ 5-15 min depending on yfinance throttling.

```bash
cd ~/autonomous-trader/base_rates

# In a screen/tmux/nohup so SSH disconnect won't kill it
nohup python -m base_rates.ingest \
  --universe universe.txt \
  --db ~/autonomous-trader/signals.db \
  --years 20 \
  > /tmp/base_rates_ingest.log 2>&1 &

# Watch progress
tail -f /tmp/base_rates_ingest.log
```

Resumable: if it dies mid-run, just re-run the same command. It'll skip symbols already up-to-date.

## 9. Daily refresh (optional)

Once happy, add a cron entry to keep it current:

```bash
crontab -e
```

Add:

```
# base_rates daily refresh — 30 min after market close ET
30 13 * * 1-5  cd ~/autonomous-trader/base_rates && /usr/bin/python -m base_rates.ingest --universe universe.txt --db ~/autonomous-trader/signals.db --years 20 >> /tmp/base_rates_ingest.log 2>&1
```

(Adjust hour for AZ time. 30 13 = 1:30pm AZ in standard time when market closes 1pm AZ.)

## Common queries

```bash
# Latest data for a symbol
python -m base_rates COIN --db ~/autonomous-trader/signals.db

# Historical interrogation: "what did the base rate say BEFORE that 4/29 move?"
python -m base_rates AAOI --date 2026-04-29 --db ~/autonomous-trader/signals.db

# JSON for piping into other tools
python -m base_rates NVDA --db ~/autonomous-trader/signals.db --json | jq

# Stricter sample-size gate
python -m base_rates ONDS --db ~/autonomous-trader/signals.db --min-n 50
```

## Troubleshooting

**yfinance returns empty for a symbol.** Skip it; the ingest already handles
this and logs a warning. yfinance occasionally rate-limits — re-run later.

**"db not found" on query.** Pass `--db` with the full path. Default is
`./signals.db` relative to wherever you invoke from.

**N is always low.** Bucket boundaries may be too tight for that symbol's
behavior. Edit `DEFAULT_BUCKETS` in `base_rates/buckets.py` and re-run ingest
(buckets are pre-computed and stored).

**RSI doesn't match TradingView exactly.** Wilder's smoothing is the standard
but slight differences come from how the first 14 bars are seeded. Within
±0.5 RSI is normal and won't change bucket assignment.

## Sacred rules (per OllieTrades convention)

- This module **never** writes to trader.db. signals.db only.
- This module **never** auto-triggers from the fleet. Interrogation only.
- `_EXECUTION_ENABLED` is not even a concept here — there's no execution.
