# HM-OLLIE-LIVE-SCANNER-DASHBOARD-TILE

## Background
2026-05-27: shipped scripts/ollie_scanner.sh — Holly-style terminal scanner
showing tiered multi-strategy convergence with entry/stop/target/RR and an
"already in position" skip list. Refreshes via `watch -n 30`.

## Goal
Port to a dashboard tile on bridge.ollietrades.com so Admiral can monitor
from the browser without SSH.

## Scope
- New section on the Bridge tab between "Sector Heatmap" and "Fleet Activity"
- Three collapsible tiers (5+, 4, 3 strategy convergence)
- Each row: ticker, strategy badges, conf%, entry, stop, target, RR
- Already-in-position rows: greyed out with "IN FLEET" badge
- Auto-refresh every 30s (matches main fleet activity cadence)
- Color: green ticker bg if Tier 1, yellow if Tier 2, light if Tier 3
- Click ticker → open ticker drawer with charts (existing handler)

## Data source
strategy_signals table — `ticker, strategy_name, confidence, entry_price,
stop_price, target_price, created_at`. Filter created_at > now-90min.

## Backend
New endpoint: GET /api/scanner/live → returns JSON {tier1: [...], tier2: [...], tier3: [...]}.
Sample SQL already in scripts/ollie_scanner.sh.

## Frontend
- dashboard/static/index.html new section ~line 6500 (near Fleet Activity)
- New JS function `renderOllieLiveScanner()` polled on the standard 30s tick
- CSS: reuse trade-card styling, add convergence-badge class

## Priority
Medium — terminal version unblocks Admiral immediately, but tile is more useful
for Bonnie/passive monitoring.

## Open questions
- Show tickers we DON'T have in fleet only? Or all with greyed-out IN FLEET tag?
  (Default: show all, grey the in-fleet ones — preserves context.)
- Audio alert when a new Tier 1 appears? Optional toggle.
