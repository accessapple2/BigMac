# Scotty: Universe Refresh Partial Failure — 2026-05-10

## Summary
The `engine/universe_refresh.py` job that ran on 2026-05-10 left 13 symbols
in `scan_universe` with `NULL` (blank) `market_cap` and `options_eligible=0`.
Because `engine/universe.get_active_universe()` filters via `_BASE_SQL` which
requires `market_cap >= MIN_MARKET_CAP` ($5B) for CS rows, these 13 symbols
were silently excluded from the active universe (Tier C of the
`paper_trader.py` SCANNER_FILTER gate).

## Affected symbols (13)
**Mega-caps (12):** AAPL, MSFT, GOOGL, AVGO, JPM, CRM, PEP, AMGN, DHR, MRVL, HOOD, CDE
**Misclassified ETF (1):** UPRO (was `ticker_type='CS'`, should be `'ETF'`)

## Symptom
From 2026-05-10 onward, AI players (ollie-auto, neo-matrix) repeatedly
proposed trades on these symbols and hit SCANNER_FILTER Tier D at
`paper_trader.py:749`, which requires confidence ≥ 90% when a symbol is not
in any of: convergence signals, top-50 universe scan, or watchlist.

Tier D reject counts in `logs/trader.log` (since trader restart 2026-05-13 19:14):

```
AMGN  147     JPM   57     PEP   25     CRM   17
DHR    25     MRVL  22     MSFT   3     AVGO   2
AAPL    2     GOOGL  1     HOOD   0     CDE    0    UPRO   0
```

(HOOD/CDE/UPRO show 0 today because Signal Center hadn't yet routed those
symbols to paper_trader — but they are also exposed to the same gate.)

Note: WFC, GS, KO, LNG appeared in earlier reject corpora but their
`scan_universe` rows have proper `market_cap`. Their rejects were a startup
cache-fallback artifact (the 20-name `_FALLBACK_UNIVERSE` doesn't include
them) that self-healed once `scan_universe` was first queried successfully.
Last Tier D reject for each of those four was 2026-05-13 afternoon. They
were NOT included in the backfill.

## Likely root cause
All 13 rows have `options_eligible=0` AND blank `market_cap`, while
correctly-populated rows have `options_eligible=1` AND real `market_cap`.
This co-occurrence suggests the upstream lookup that populates both fields
(`_fetch_market_cap_yfinance` or `_fetch_ticker_details_polygon` in
`engine/universe_refresh.py`) failed for these specific tickers during the
2026-05-10 batch — most plausibly:

1. Polygon rate-limiting during the bulk fetch
2. yfinance timeout or auth issue on the fallback path
3. A symbol-specific data quirk (e.g., temporary listing change)

The `_write_universe` upsert at `engine/universe_refresh.py:249` writes the
row regardless of whether `market_cap` and `options_eligible` were resolved,
so partial failures silently produce filter-invisible rows.

## Manual remediation applied 2026-05-14
SQL UPDATE on `data/trader.db` `scan_universe` table:
- `market_cap = 99999999999` (sentinel ~$100B, grep-able as manual
  backfill marker) for the 12 mega-caps
- `market_cap = 5500000000` for CDE (near threshold; sentinel would be
  misleading)
- `ticker_type = 'ETF'` for UPRO
- `options_eligible = 1` for the mega-caps (0 for CDE — verify before
  routing options orders to it)
- `last_updated = datetime('now')` on all rows

Verification: `get_active_universe(force_refresh=True)` size went from 668
to 681 (delta=13, exact). All 13 targets confirmed in returned set.

This change is **self-healing**: the next successful
`engine/universe_refresh.py` run will overwrite these rows with real
Polygon/yfinance values. The sentinel `99999999999` is grep-able so we can
spot leftover manual rows if a future refresh doesn't cover them.

## Proposed ticket: HM-UNIVERSE-REFRESH-RESILIENCE

**Scope** (proposed for `engine/universe_refresh.py`):

1. **Retry with backoff** on Polygon/yfinance NULL returns. Current code
   accepts the first None and writes a partial row. Should retry 2-3× with
   exponential backoff before accepting failure.

2. **Alert on >1% NULL rate** in a refresh batch. Currently silent failure
   — refresh marked "success" even when 13 rows have NULL market_cap. Add a
   post-write sanity check: if >1% of CS rows have NULL market_cap, NTFY
   the captain.

3. **Identify why these 13 specifically failed.** Re-run the refresh with
   verbose logging on these symbols. Check `engine/universe_refresh.py`
   logs from 2026-05-10 (if retained) for Polygon/yfinance errors keyed to
   these tickers.

4. **Test:** add a unit test for the partial-failure path. Either retry
   to success, or exclude the row from the write entirely — but do not
   silently produce filter-invisible rows.

5. **Optional:** add a follow-up cron job that scans `scan_universe` for
   NULL market_cap on CS rows daily and re-attempts the lookup.

## Captain action
- [ ] Review tomorrow morning
- [ ] Prioritize HM-UNIVERSE-REFRESH-RESILIENCE
- [ ] Decide whether to force-run `universe_refresh.py` now to overwrite
      sentinels with real values, or wait for the next scheduled run
- [ ] Inspect `engine/universe_refresh.py:249` `_write_universe` for the
      partial-failure code path
