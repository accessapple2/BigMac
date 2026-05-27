# HM-TRADES-VIEW-OPTION-TYPE

## Issue
Dashboard "Today's Trades" / "Fleet Activity" tables render covered calls and 
other option legs alongside stocks without indicating the asset type. A
covered call (qty -1, premium $0.57) next to its stock entry (qty 22.5, $19.26)
looks like a -97% loss to the casual eye.

## Repro
- 2026-05-27 navigator MNTS:
  - 15:06 BUY 22.5 MNTS stock @ $19.26
  - 16:20 SELL -1 MNTS option (call, $21.15 strike, 6/10 expiry) @ $0.57 premium
- Trades view shows both as plain stock rows. Looks like a wipeout.

## Fix
In dashboard/static/index.html trades-rendering JS:
- When row.asset_type='option', prefix symbol with option_type ("C" or "P"),
  strike, and expiry. Show premium per contract not per share.
- E.g. render as "MNTS C21.15 06/10  -1  $0.57 prem" instead of "MNTS -1.0 $0.57".

## Priority
Low — cosmetic, not affecting execution. Add to UI polish backlog.
