# L5 — run_squeeze_watcher 48.8s Wall

**Status:** Isolated to daemon thread (not blocking scheduler). Confirmed today.
**But:** 48.8s for a Finviz scrape is slow. Finviz HTTP can hang. Could fail silently.

## Tier 1 investigation (no Captain consult)

```bash
# Distribution of squeeze_watcher walls over last 7 days
grep "HM-BQ-instr.*run_squeeze_watcher" logs/trader_error.log | \
  grep -oE "wall=[0-9.]+s" | sort -n | uniq -c

# Are there hangs/timeouts in the scrape itself?
grep -E "Finviz.*timeout|Finviz.*fail|Squeeze.*error" logs/trader.log | tail -20

# How many candidates does each scrape produce? Productivity vs cost.
grep "Squeeze Scanner:.*candidates from Finviz" logs/trader.log | tail -10
```

## Hypothesis ranking

**A. Finviz HTTP latency** — single biggest contributor
- Add explicit timeout to the HTTP request (currently may be default-30s+)
- Switch to async HTTP for parallel sub-requests if multiple URLs hit

**B. yfinance hydration** — the 273 candidates get hydrated with yfinance data
for screening (RSI, volume, etc.). This is per-symbol sequential yfinance.
Each call adds 100-300ms. 273 × 150ms = 41 seconds. Likely culprit.

**C. SQLite write throughput** — inserting 273 rows shouldn't take long, but
worth measuring.

## Tier 1 fixes (after measurement confirms hypothesis)

**If hypothesis B confirmed:**
1. Migrate hydration from yfinance → Polygon Stocks API (we have it now)
2. Use Polygon batch endpoints to fetch RSI/volume for 273 symbols in 1-3 calls
3. Expected speedup: 48s → 5-10s

**If hypothesis A confirmed:**
1. Add aiohttp + asyncio.gather() to parallelize Finviz fetches
2. Or migrate squeeze scanner candidates source from Finviz scrape to Polygon
   technical screener

## Tier 2 observation period

After fix, monitor HM-BQ-instr for run_squeeze_watcher walls over 7 days.
Target: median < 10s, p95 < 20s.

Estimated effort: 1-2 hours for hydration migration to Polygon.
