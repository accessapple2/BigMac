# HM-OLLIE-LIVE-SCANNER-PHASE-1-SHIPPED

Date: 2026-05-27

## Outcome
Holly-style live convergence scanner shipped to bridge.ollietrades.com.

## What landed
- `GET /api/scanner/convergence` endpoint in `dashboard/app.py:1652-1859`
  - 25s in-memory cache
  - Joins strategy_signals + mover_watchlist + volume_baselines + volume_alerts + positions
  - Returns tier1/tier2/tier3 arrays, 116ms cold response, ~5ms cached
- 🔭 Ollie Live Scanner card in `dashboard/static/index.html:5031-5247`
  - 3 collapsible <details> tiers
  - Heatmap columns (Price, Chg Close, Chg Open, Rel Vol, Vol 5 Min)
  - IN FLEET badge greys held tickers
  - Row-click → existing openTickerDetail()
  - 30s poll, per-tier audio toggles (T1 default ON)

## Verified live (10:36 AM AZ)
- Tier 1: MNTS, STLD, KEY (3 rows, 100/100/90 conf)
- Tier 2: 20 rows at 82% conf
- Tier 3: 68 rows
- MNTS shows full heatmap (568% Vol 5 Min)
- Mega-caps show `--` for heatmap columns (mover_watchlist source gap)

## Known gap (HM-SCANNER-MEGACAP-PRICE-GAP)
mover_watchlist sources from Polygon's biggest-%-mover snapshot which excludes
mega-caps. STLD/KEY/ADI/AMZN/AVGO etc. fire convergence but show `--` for
price/heatmap. Fix path 1 (LEFT JOIN positions/trades for prices) logged as
Phase 1.5 follow-up.

## Spec deviation
Route renamed `/api/scanner/live` → `/api/scanner/convergence` to avoid
shadowing the legacy signal-center BUY-signal stream at the same path.

## Next
Phase 2: live event tape from volume_alerts. See HM-OLLIE-LIVE-SCANNER-DASHBOARD-TILE.md.
