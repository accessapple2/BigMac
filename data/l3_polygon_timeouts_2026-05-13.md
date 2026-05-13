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
