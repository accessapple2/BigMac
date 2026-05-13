# L3 — Polygon ReadTimeout Pattern

**Observed today:**
- RRX timeout after GPU restart
- UVIX fallback to Alpaca

**Worth measuring before acting.** Could be 2 timeouts in a day (acceptable) or 40 (real problem).

## Tier 1 measurement (no Captain consult)

```bash
# Count timeouts/fallbacks per hour over last 7 days
grep "HM-CB Polygon.*fallback\|polygon.*timeout\|polygon.*timed.out" logs/trader.log | \
  awk '{print substr($1,2,5)}' | sort | uniq -c | sort -rn | head -20

# Symbol-frequency: which tickers timeout most often?
grep "HM-CB Polygon.*fallback" logs/trader.log | \
  grep -oE 'for [A-Z]{2,5}' | sort | uniq -c | sort -rn | head -20

# Time-of-day pattern
grep "HM-CB Polygon.*fallback" logs/trader.log | \
  awk '{split($1, t, ":"); print t[1]}' | sort | uniq -c
```

## Hypothesis branches

**If <5 timeouts/day:** Polygon Stocks Starter is fine, no action needed.
File as monitor-only.

**If 5-20 timeouts/day on niche/leveraged tickers (UVIX, SQQQ, TQQQ etc.):**
Real symptom. These tickers have lower liquidity → Polygon's caching/SLA
deprioritizes them. Fixes:
1. Tighten fallback timeout from 5s → 3s (faster fail-over)
2. Pre-warm Polygon cache for the small set of leveraged-ETF universe
3. Skip Polygon entirely for symbols in `_LOW_LIQUIDITY_TICKERS` set,
   go direct to Alpaca

**If >20 timeouts/day OR concentrated on mega-caps:**
Polygon Stocks Starter ($29/mo) may be undersized for fleet's throughput.
Upgrade options:
- Polygon Stocks Developer ($79/mo) — 5x rate limit
- Move to Alpaca-primary, Polygon for options only
- Both: hedge by keeping Alpaca as primary, Polygon for higher resolution

## Tier 1 ship if measurement shows niche-ticker concentration

Add `_LOW_LIQUIDITY_TICKERS = {"UVIX","SQQQ","TQQQ","SOXL","SOXS",...}` set
to engine/market_data.py. Skip Polygon for these, go straight to Alpaca.

Estimated effort: 30 min including testing.

## Out of scope for Tier 1

Subscription upgrades — Captain capital decision.
