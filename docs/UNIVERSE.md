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
| Market cap | **≥ $5B** | Excludes micro/small-caps where price is easier to push and harder to fill |
| Daily $ volume (20-day average) | **≥ $50M** | Liquidity floor for both equities and basic options availability |
| Refresh cadence | **Weekly** | Sunday afternoon pre-Monday-open; rolls in fresh inclusions, retires names that fell below thresholds |
| Refresh source | **Polygon screener API** | Polygon Options Starter $29/mo (approved-in-principle 2026-04-16, activation under HM-AQ-β) — richer screener than Alpaca |

**Expected universe size:** ~500-800 tickers. Bound depends on the cap+volume floor sensitivity to market regime; in a wide bull market closer to 800, in risk-off closer to 500.

## Risks acknowledged (Captain 2026-05-07)

- **Dashboard noise** — 25-40× more rows render per surface. UI density needs review post-ship.
- **Scan-loop slowdown** — 12+ iteration sites in `dashboard/app.py` (per HM-AU audit) walk `WATCH_STOCKS`. Latency impact must be measured during HM-AQ-β soak window.
- **More spread attempts on illiquid options** — only applicable IF future Captain decision on HM-AQ-γ broadens spread universes too. **For now, spread universes stay at 10 tickers.**

## Catches (Captain rationale 2026-05-07)

- All 6 missed movers from 2026-05-07 morning (DDOG +30.87%, FTNT +22.92%, MDB +14.19%, ZTS −21.37%, ARM −8.18%, TPR −8.14%) would have been in the universe under the new criteria.
- Discovery surface expands from 20 names to ~500-800 — coverage of liquid mid/large-cap moves goes from "near-zero" to "near-complete."

## Implementation roadmap (HM-AQ-β)

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
