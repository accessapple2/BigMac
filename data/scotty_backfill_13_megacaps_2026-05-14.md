# Scotty: Targeted Backfill — 13 Mega-Caps — 2026-05-14

Replaces sentinel values from commit 36cb5c6 with real Polygon/yfinance data.
Run elapsed: **17.1s**.

## Summary

- Total targets: 13
- Updated:       12
- Skipped (lookup failure):     1
- Skipped (validation failure): 0
- In watchlist after run:       13/13

## Diff — sentinel/old → real

| symbol | pre market_cap | post market_cap | source | type | opt | status |
|---|---|---|---|---|---|---|
| AAPL | 99,999,999,999 | 4,389,610,087,720 | polygon | CS | 1 | UPDATED |
| MSFT | 99,999,999,999 | 3,010,076,026,408 | polygon | CS | 1 | UPDATED |
| GOOGL | 99,999,999,999 | 4,877,920,125,283 | polygon | CS | 1 | UPDATED |
| AVGO | 99,999,999,999 | 1,973,362,352,409 | polygon | CS | 1 | UPDATED |
| JPM | 99,999,999,999 | 804,523,303,254 | polygon | CS | 1 | UPDATED |
| CRM | 99,999,999,999 | 135,666,032,905 | polygon | CS | 1 | UPDATED |
| PEP | 99,999,999,999 | 204,017,506,380 | polygon | CS | 1 | UPDATED |
| AMGN | 99,999,999,999 | 181,683,000,000 | polygon | CS | 1 | UPDATED |
| DHR | 99,999,999,999 | 117,482,846,376 | polygon | CS | 1 | UPDATED |
| MRVL | 99,999,999,999 | 155,609,844,698 | polygon | CS | 1 | UPDATED |
| HOOD | 99,999,999,999 | 69,113,763,125 | polygon | CS | 1 | UPDATED |
| CDE | 5,500,000,000 | 20,421,242,521 | polygon | CS | 1 | UPDATED |
| UPRO | NULL | NULL | — | ETF | 0 | SKIPPED (lookup: polygon returned None; yfinance returned None) |

## Watchlist verification

- `get_active_universe(force_refresh=True)` size: 681
- IN watchlist (13/13): AAPL, MSFT, GOOGL, AVGO, JPM, CRM, PEP, AMGN, DHR, MRVL, HOOD, CDE, UPRO

## Per-symbol [BACKFILL] log lines

```
```

## Action items

**Lookup failures** (sentinel value preserved — re-attempt later):
  - **UPRO**: polygon returned None; yfinance returned None
