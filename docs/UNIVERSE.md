# Trading Universe — Inclusion Criteria & Refresh Policy

**Status:** Captain's decision logged 2026-05-07 (HM-AQ). Implementation queued as HM-AQ-β.
**Source of truth:** `config.py:WATCH_STOCKS` (current); to be migrated per HM-AQ-β.

## Two distinct universes

OllieTrades has **two separate trading universes** with different inclusion criteria. **Do not conflate them.**

### 1. WATCH_STOCKS — broad equity universe (this document)

The list iterated by dashboard surfaces, fast scanners, signal generators, and all equity-side flows.

**Currently** (2026-05-07): 20 manually-curated mega-cap tickers in `config.py:24`.
```
SPY, QQQ, TQQQ, NVDA, TSLA, AAPL, AMD, META, MSFT, GOOGL, AMZN, MU,
ORCL, NOW, AVGO, PLTR, DELL, XLE, INTC, NUKZ
```

**Approved 2026-05-07 (HM-AQ)**: dynamic universe per criteria below.

### 2. Spread-strategy universes (`TIER_1 + TIER_2`)

The 10-ticker list iterated by `bull_spread_v1`, `bull_call_spread_v1`, `bear_put_spread_v1`. Lives in each strategy's source file as `TIER_1` (3 indices) and `TIER_2` (7 large-caps).

**Out of scope for HM-AQ.** Spread strategies operate on options chains where quality of fill, bid-ask spread, and open interest dominate edge. Expanding spread universes onto less-liquid options is a separate quality-vs-coverage decision tracked as **HM-AQ-γ** (deferred — not in the active queue, kept as a marker).

> **Rationale for scope split (Captain 2026-05-07):**
> Spread quality outweighs coverage on illiquid options. The 10-ticker spread universe is curated for liquidity that supports defined-risk debit/credit spreads. Adding mid-cap or thinly-traded names introduces fill risk, wider spreads, and execution slippage that can eat the edge a spread strategy is supposed to capture. WATCH_STOCKS expansion is about discovery breadth on equities; spread universe expansion would need its own analysis on per-name option-chain liquidity (avg daily option volume, OI floor, bid-ask spread floor). Separate Captain decision when surfaced.

## WATCH_STOCKS inclusion criteria (effective HM-AQ-β ship)

| Criterion | Threshold | Rationale |
|---|---|---|
| **Market cap (stocks only)** | **≥ $5B** | Excludes micro/small-caps where price is easier to push and harder to fill |
| **Daily $ volume** (today's close × today's volume) | **≥ $100M** | Liquidity floor; revised v3 from $50M after v2 dry-run produced 1,554 finalists (too wide for dashboard latency budget) |
| **Ticker types included** | **CS, ETF** | Common stocks pass cap+volume; ETFs pass volume only (no cap analog) |
| **Ticker types skipped** | **ETN, ETV, PFD, ADRC, FUND, ...** | ETN: debt notes; ETV: leveraged-vol products (decay-prone); others: not tradable inventory |
| **Refresh cadence** | **Weekly** | Sunday 14:00 MST pre-Monday-open; rolls in fresh inclusions, retires names that fell below thresholds |
| **Refresh source** | **Polygon Stocks + Options ($29/mo each, $58/mo total — both active 30+ days)** | Stock screener via `/v3/reference/tickers/{TICKER}` for cap; `/v2/aggs/grouped/{date}` for volume; `/v3/reference/options/contracts` for options eligibility |
| **Sanity bounds** | **100 ≤ final ≤ 2500** | Outside band → fail-safe abort + NTFY ollietrades-admin |

**Expected universe size at v3 ($100M floor):** ~600-900 tickers (mix of CS + ETF). v2 at $50M produced 1,554 — too wide for dashboard latency budget (chunks of 50 × ~3.1s/snapshot).

### v2 → v3 threshold revision history (2026-05-07)

| Version | Floor | Step 1 candidates | Step 2 finalists | Outcome |
|---|---|---:|---:|---|
| v1 | $50M (no ETF branch) | 2,073 | crashed before ETF support | killed mid-run |
| v2 | $50M (with ETF branch + ETV skip) | 2,073 | 1,554 (1,113 CS + 441 ETF) | rejected by sanity bound 1500 — fail-safe abort, no DB write |
| **v3 (current)** | **$100M** | **predicted ~1,000-1,400** | **predicted ~600-900** | **expected ship-ready** |

ETF count at $50M: 441. At $100M, ETF count expected to drop to ~150-250 (most sector SPDRs and major broad-market ETFs are above $100M; smaller regional/sector ETFs drop out).

## Risks acknowledged (Captain 2026-05-07)

- **Dashboard noise** — 25-40× more rows render per surface. UI density needs review post-ship.
- **Scan-loop slowdown** — 12+ iteration sites in `dashboard/app.py` (per HM-AU audit) walk `WATCH_STOCKS`. Latency impact must be measured during HM-AQ-β soak window.
- **More spread attempts on illiquid options** — only applicable IF future Captain decision on HM-AQ-γ broadens spread universes too. **For now, spread universes stay at 10 tickers.**

## Catches (Captain rationale 2026-05-07)

- All 6 missed movers from 2026-05-07 morning (DDOG +30.87%, FTNT +22.92%, MDB +14.19%, ZTS −21.37%, ARM −8.18%, TPR −8.14%) would have been in the universe under the new criteria.
- Discovery surface expands from 20 names to ~500-800 — coverage of liquid mid/large-cap moves goes from "near-zero" to "near-complete."

## Implementation notes (HM-AQ-β SHIPPED 2026-05-07)

**Substrate:** existing `scan_universe` table, extended with two new columns.
- `migrations/HM-AQ-β_universe_columns_2026-05-07.sql` — adds `market_cap REAL` + `options_eligible INTEGER DEFAULT 0`
- `migrations/HM-AQ-β_universe_ticker_type_2026-05-07.sql` — adds `ticker_type TEXT DEFAULT 'CS'` (v3 refinement for ETF inclusion)

**Reader:** `engine/universe.py::get_active_universe()` — list[str], 30s TTL cache, fail-safe fallback to legacy 20-name list if `scan_universe` is empty/stale.

**Writer:** `engine/universe_refresh.py` — Polygon Stocks + Options (both $29/mo, $58/mo total). 3-step pipeline:
1. `/v2/aggs/grouped/locale/us/market/stocks/{prev-day}` → 12K rows in 1 call → filter dollar_volume ≥ $100M
2. `/v3/reference/tickers/{TICKER}` per candidate (5 cps) → branch on `type`:
   - `CS` (common stock) → require market_cap ≥ $5B (yfinance fallback)
   - `ETF` → include on dollar_volume only
   - `ETN` → skip (debt notes)
   - `ETV`, `PFD`, `ADRC`, `FUND`, ...→ skip (not in scope)
3. `/v3/reference/options/contracts?underlying_ticker=X&limit=1` per finalist → set `options_eligible=1`

**Sanity bounds:** `100 ≤ final ≤ 2500`. Out-of-bounds → fail-safe abort, NTFY `ollietrades-admin`, retain prior `scan_universe`.

### Bulk-endpoint pattern (perf)

Multiple consumers iterated `get_active_universe()` calling `get_stock_price(sym)` per-symbol — fine at 20 names, broken at 1,223 (~47s/snapshot via `ThreadPoolExecutor(max_workers=6)` over chunks of 50). HM-AQ-β v3 migrates these sites to **`engine.market_data.get_bulk_prices(symbols)`** which uses Alpaca's bulk endpoint `/v2/stocks/quotes/latest?symbols=...` returning all symbols in ONE HTTP call (~1-2s for 1,223 names). **~25× faster.**

Sites migrated in commit 5:
- `dashboard/app.py:3866` (was the chunked `ThreadPoolExecutor`)
- `dashboard/app.py:4581` (market_sectors)
- `dashboard/app.py:5162, 5228` (confidence panel)
- `dashboard/app.py:6105` (risk_radar prices)
- `dashboard/app.py:6478` (pair_pnl)
- `main.py:549` (journal entries)
- `main.py:1109` (war_room)
- `main.py:1988` (scoreboard)

Sites intentionally left on per-symbol path (different patterns, not API-fanout):
- `dashboard/app.py:3874` (iterates already-cached dict, no API call)
- `dashboard/app.py:3906` (per-agent×symbol SQL queries, not API)
- `engine/historical_backtest.py:*` (backtest ETL, run once)
- `engine/strategy_lab.py:986` (research/lab loop)

### v1 → v2 → v3 dry-run iteration history

| Version | Floor | Pipeline state | Outcome |
|---|---|---|---|
| v1 | $50M | No ETF branch (all ETFs failed cap filter) | Killed mid-run after observing TQQQ/IWM/XLE excluded |
| v2 | $50M, with ETF branch + ETV skip | 12,098 → 2,073 → 1,554 finalists (1,113 CS + 441 ETF) | Tripped sanity bound 1500 → fail-safe abort (intended diagnostic) |
| v3 (pre-fix) | $100M, with ETF branch | 1,453 → 1,223 finalists (927 CS + 296 ETF) | Crash in dry-run sample-print (ETF None-cap) — fixed |
| **v3 (final)** | **$100M floor**, MAX_FINAL_COUNT=2500 | **1,223 finalists** | **Ship** |

Bug fixes folded into commit 5:
- sys.path insertion for standalone script invocation (commit 3 missed this)
- NTFY title ASCII-only (β character broke latin-1 HTTP header)
- ETF inclusion branch (Captain refinement during v1 dry-run)
- ETV explicit type-skip
- Per-symbol audit logging (`etf_included`, `stock_capfail`, `type_skipped`, `no_cap_skipped`, `fallback yfinance`)
- $50M → $100M dollar-volume floor (Captain decision after v2 1,554 finalists strained dashboard latency budget)
- Sample-print None-cap handling (v3 cosmetic crash fix)
- `MAX_FINAL_COUNT` 1500 → 2500
- Bulk-endpoint migration at 9 fan-out sites (the perf fix)

### HM-AQ-β.2 backlog (deferred)

ADRC inclusion. ADRC = American Depositary Receipt (sponsored). Examples skipped today: BP, NIO, GGB, VIST, LEGN. Many trade liquid options. Captain decision needed: include with cap+volume filter (treat like CS), or skip permanently. Effort if including: ~10 min — add ADRC to the included-types in `_fetch_ticker_details_polygon` branch.

## Implementation roadmap (HM-AQ-β SHIPPED — original section preserved below)

See `docs/XO_BACKLOG.md` HM-AQ-β. Summary:

1. New `engine/universe_refresh.py` — Polygon screener API client + cap/volume filter.
2. Storage migration — `config.py:WATCH_STOCKS` constant → DB table (e.g. `universe_active` with `(symbol, last_refreshed_at, market_cap, avg_daily_dollar_volume, included_reason)`) or daily-rebuilt file.
3. Weekly cron — launchd plist `com.ollietrades.universe-refresh`, fires Sunday 14:00 MST (post-close, pre-Monday-open).
4. Polygon Options Starter $29/mo activation.
5. Audit/retest the 12+ `WATCH_STOCKS` iteration sites for rate-limit + latency impact.
6. Soak window before promoting to live (2026-05-07 era convention).

**Effort estimate:** 4-8 h Scotty. Owner: Scotty (Engineering).

## Cross-references

- HM-AQ — Captain decision (this doc)
- HM-AQ-β — implementation ticket (active queue)
- HM-AQ-γ — spread-universe expansion (deferred marker, not active)
- HM-AU — Kirk advisory source routing audit (related: 2026-05-07 morning observation that surfaced HM-AQ)
- CLAUDE.md "Free-Models-First" doctrine — Polygon Options Starter is the first paid exception, approved-in-principle 2026-04-16
- `config.py:24` — current static `WATCH_STOCKS` list
- `bull_spread_v1.py`, `bull_call_spread_v1.py`, `bear_put_spread_v1.py` — `TIER_1+TIER_2` spread universes (out of scope here)
