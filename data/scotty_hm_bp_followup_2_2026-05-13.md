# HM-BP-FOLLOW-UP-2 — options exit_price writer trace

**Trigger:** HM-BP commit e2a59c4 added reject filter for abs(pnl_pct) > 50%.
HM-BP-FOLLOW-UP discovery 5e452d4 identified the source as options trades where
entry_price = premium ($8-$22) but exit_price = underlying spot ($200-$400).
This doc finds WHERE the options exit_price writer persists spot instead of premium.

## Discovery (read-only)

Code sites that assign exit_price for options handlers — see grep output above.

## Hypothesis ranking

1. **engine/options_agents.py** — options.expire / options.exit handler may
   pull spot price from the underlying ticker instead of the option premium quote
2. **engine/battle_station.py** — monitor_active_options may close with stock
   price instead of option price when option chain quote unavailable
3. **paper_trader.sell** when asset_type='option' — may not have option-aware
   exit_price routing

## Next steps for HM-BP-FOLLOW-UP-2 Phase 2 (next session)

1. Read each candidate site, identify which one persists exit_price
2. Determine where it gets the price from (Polygon options chain, Alpaca option quote, fallback to spot?)
3. Fix: ensure options exits ALWAYS use option premium, never underlying spot
4. Backfill: optional one-time SQL to recompute exit_price for corrupt rows
   from realized_pnl + entry_price math (if realized_pnl is sound)

## Out of scope today

- Backfilling historical corrupt rows (Captain decision)
- Modifying the writer (needs careful review of option price source path)
