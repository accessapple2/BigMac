# L3 — Polygon Timeout Pattern Report 2026-05-13

## Hourly distribution (last 7d of trader.log)

```
 445 polyg
 325 ost='
 121 M-CB
  22 11:01
  17 10:46
  15 07:09
  13 07:39
  12 08:43
  12 08:39
  12 07:03
  11 10:05
  11 07:34
  10 10:06
  10 07:36
  10 04:00
   9 10:45
   8 11:02
   8 08:37
   8 07:40
   6 10:24
```

## Top symbols by fallback frequency

```
  73 for CTRA
   4 for RRX
   3 for WWD
   3 for WCC
   3 for VOOG
   3 for PDBC
   3 for OKLO
   3 for LH
   3 for IREN
   3 for GLWG
   3 for FN
   3 for CWVX
   3 for ACLS
   2 for ZS
   2 for XOP
   2 for XLF
   2 for WY
   2 for WM
   2 for WEC
   2 for VSS
```

## Totals

```
  trader.log fallback count: 502
```

## Hypothesis interpretation

- <5/day total → No action (monitor-only)
- 5-20/day on niche tickers → Tier 1 `_LOW_LIQUIDITY_TICKERS` skip-Polygon set
- >20/day or mega-cap concentration → Captain decision on subscription


## SUBSCRIPTION CORRECTION (2026-05-13 late)

**Original report mistakenly assumed Stocks Starter ($29).** We are on
Polygon Stocks Developer ($79) which has 100 req/sec rate limit and full
options data. Fallback root-cause is NOT subscription undersizing.

### Revised hypothesis ranking

1. **Rate-limit bursts within Developer tier** — scan cycles can burst
   above 100 req/sec briefly. Add observability: log Polygon response
   headers (X-RateLimit-Remaining, X-RateLimit-Reset) to identify if
   bursts are the cause.

2. **Polygon coverage gaps for specific symbols** — CTRA at 73 hits
   is suspicious for a mid-cap energy stock. Investigate whether
   Polygon's CTRA endpoint has intermittent gaps.

3. **Local network/DNS to Polygon endpoint** — measure DNS resolution
   time + TCP handshake to api.polygon.io periodically to rule out
   local-side latency.

4. **Genuine Polygon SLA issues** — check Polygon status page during
   the timeout-cluster hours.

### Revised Tier 1 actions

- Add `_LOW_LIQUIDITY_TICKERS` skip set including CTRA-pattern names
- Migrate scan-cycle data fetches to Polygon batch endpoints
  (reduces request count per scan from N to ~1)
- Add Polygon response-header logging to confirm/rule-out rate limits
