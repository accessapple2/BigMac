# HM-BK — Small-Cap Mover Watchlist Tier

## Why
PSIX (May 11–13 2026) collapsed 50%+ on Q1 miss + withdrawn FY guide. Fleet universe is mega-cap heavy; small/mid-cap movers are invisible to the signal center. Add a screening tier that surfaces these names without disturbing the S6 free fleet.

## Goal
A self-refreshing watchlist of liquid small/mid-cap movers, feeding the signal center alongside the core S6 list, tagged `tier='mover'`.

## Acceptance Gates
1. New table `mover_watchlist` in trader.db: symbol, last_price, pct_change, volume, mcap, optionable, refreshed_at, source
2. Refresh job runs every 5 min, market hours only (09:30–16:00 ET) via launchd
3. Filters: market cap ≥ $500M, avg daily volume ≥ 500K, optionable=true, |pct_change| ≥ 5%
4. signal_center reads mover_watchlist and routes to fleet agents with tier='mover'
5. Dashboard `/movers` route surfaces the live list with nav link
6. ZERO interference with existing S6 free fleet routing — purely additive
7. Manual browser smoke test before ship (HM-BJ.E2 rule)

## Data Source
Primary: Polygon `/v2/snapshot/locale/us/markets/stocks/gainers` + `/losers` (active $29/mo plan)
Fallback: Finviz Elite screener export

## Sequence
1. Schema migration — add mover_watchlist table (no rm, additive only)
2. Build `scrapers/polygon_movers.py` poller
3. Wire into signal_center as new tier
4. launchd plist `com.ollietrades.movers-poller` — 5min cadence, market hours, bound to process lifecycle (DAEMON LIFECYCLE RULE)
5. Dashboard /movers route + nav link in static/index.html
6. Smoke test: ≥10 symbols populate, signal_center logs tier='mover' entries, manual browser check
7. Self-verify in one block, paste closure

## Out of Scope
- Auto-trading on movers — signals only; manual gate before bull/bear spread activation
- Sub-$500M micro caps
- Non-optionable names


---

## PHASE 1 LANDED — 2026-05-13 (backend only)

**Shipped tonight (autonomous-safe):**
- ✓ Gate 1: `mover_watchlist` table created in `data/trader.db` (11 cols, 2 indexes)
- ✓ Gate 2 partial: poller exists in `scrapers/polygon_movers.py`; plist installed at `~/Library/LaunchAgents/com.ollietrades.movers-poller.plist`, **NOT loaded yet**
- ✓ Gate 3 partial: |pct_change| ≥ 5% filter applied in-poller. mcap ≥ $500M and optionable=true deferred to phase-2 enrichment (snapshot endpoint doesn't include these fields).
- ⊘ Gate 4: signal_center wiring DEFERRED (separate codebase at `./signal-center/`, needs investigation)
- ⊘ Gate 5: Dashboard `/movers` route DEFERRED (browser smoke test required per HM-BJ.E2 rule)
- ✓ Gate 6: ZERO interference confirmed — additive table, no S6 fleet paths touched
- ⊘ Gate 7: Manual browser smoke DEFERRED (no UI shipped tonight)

**Standalone test result:**
- 42 movers captured (21 gainers + 21 losers) from a forced-market-hours real Polygon fetch
- Sample includes microcap pumps (TDIC +959%) — these will get screened by the future mcap/optionable enrichment in phase 2
- Raw universe is intentionally permissive at this stage; no auto-trading wired

**To activate the poller (Captain ready check):**
```bash
launchctl load ~/Library/LaunchAgents/com.ollietrades.movers-poller.plist
```
Self-gates to market hours (09:30–16:00 ET, Mon-Fri); off-hours invocations exit cheaply.

**Phase 2 remaining work:**
1. signal_center wiring (read mover_watchlist, route to fleet with tier='mover')
2. Dashboard `/movers` route + nav link in static/index.html (browser smoke test required)
3. mcap + optionable enrichment job (per-ticker /v3/reference/tickers calls, separate cadence — e.g. nightly)
