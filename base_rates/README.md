# base_rates

Conditional base-rate research utility for OllieTrades.
Research/interrogation only. Not wired into the trade loop.

Inspired by TradeOdds: pick a symbol that moved today, find historical days
where 6 indicator states matched, report what happened over the next N days.

## What it does

For any symbol on any date, find historical analogs that match on:

1. **Move intensity** — today's % change bucket
2. **RSI zone** — RSI(14) bucket (oversold / neutral / overbought)
3. **RSI slope** — rising or falling
4. **VIX level** — VIX value bucket
5. **VIX move** — VIX % change bucket
6. **Market trend** — SPY above/below 200d SMA

Then report N, win rate, median forward return, P25/P75 spread, and median
path drawdown.

## Quickstart

```bash
# 1. Apply schema (idempotent)
python -m base_rates.migrate --db /path/to/signals.db

# 2. Backfill 20yr OHLCV + features for the universe
python -m base_rates.ingest --universe universe.txt --db /path/to/signals.db

# 3. Query
python -m base_rates AAPL --db /path/to/signals.db
python -m base_rates AAPL --date 2026-04-29 --db /path/to/signals.db
python -m base_rates AAPL --db /path/to/signals.db --json
```

## Sample output

```
=== Base Rate: AAPL as of 2026-04-30 ===

Today's bucket vector:
  move_intensity   0.03 to 0.05
  rsi_zone         50 to 70
  rsi_slope        rising
  vix_level        15 to 20
  vix_move         -0.01 to 0.01
  market_trend     SPY>200d

Historical matches: N = 47
  Win rate (5d):     63.8%
  Median fwd return:    +1.20%
  Mean fwd return:      +0.85%
  P25 / P75:            -1.50% / +3.20%
  Median path maxDD:    -2.10%
```

## Honesty rails

- **N=11 is not a base rate, it's anecdote.** Default `--min-n 30` warns when
  the sample is too small. The headline win rate is meaningless with low N.
- **Wide IQR is flagged.** If P75 - P25 exceeds 10pp, you'll see a warning —
  the median can mislead when outcomes scatter.
- **Path drawdown shown.** Headline 5-day return ignores the drawdown along
  the way; we report median path maxDD too.
- **Bucket boundaries matter.** Move RSI cutoff 2 points and the match set
  shifts. Defaults are reasonable but not sacred. Tune via `overrides=` in
  `assign_buckets()` or change `DEFAULT_BUCKETS`.

## Universe

Edit `universe.txt` to add/remove tickers. One per line, `#` for comments.
Default starter list is ~50 names. To add S&P 500: dump the constituents into
the file (no special format needed).

Universe size affects ingest time, not query time. Queries are O(1) per
symbol thanks to the bucket-vector index.

## Schema

Two tables in `signals.db`:

**`base_rate_features`** — one row per (symbol, date). Stores OHLCV-derived
features, forward outcomes, and pre-computed bucket vector for fast match.

**`base_rate_ingest_log`** — one row per symbol, tracks last successful date
for resumability.

## File layout

```
base_rates/
├── __init__.py        # public exports
├── __main__.py        # python -m base_rates → CLI
├── buckets.py         # pure bucket logic
├── features.py        # RSI, forward returns, max DD
├── migrate.py         # schema setup
├── ingest.py          # yfinance backfill
├── query.py           # match + aggregate
└── cli.py             # text/JSON output
tests/
├── test_buckets.py
├── test_features.py
└── test_query.py
universe.txt           # edit me
```

## Tests

```bash
python -m pytest tests/ -v
```

19 tests cover buckets, features (RSI, forward return, max DD), and end-to-end
query against an in-memory DB.

## What v1 deliberately does NOT do

- No dashboard panel (add later if it earns it)
- No auto-trigger from the fleet (it's interrogation, on-demand)
- No multi-window outputs (5d only — keep surface area small)
- No bucket auto-tuning (manual overrides only)
- No execution coupling — research utility only

## Future ideas (not in v1)

- Multiple forward windows (1d, 5d, 20d) side by side
- Cross-symbol regime queries (what did the *whole market* do after a setup?)
- Bucket sensitivity analysis (how does N change as we widen RSI bucket ±5?)
- "Tractor Beam tiebreaker" mode: read fleet signals from trader.db, return
  base rate as confirmation/veto
