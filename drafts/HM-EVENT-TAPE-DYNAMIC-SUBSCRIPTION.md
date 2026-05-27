# HM-EVENT-TAPE-DYNAMIC-SUBSCRIPTION

## Issue
Tick recorder subscribes to a static 28-symbol IEX list. When scanner surfaces
new Tier 1/2 convergence candidates (e.g., CRSR and MUU at 12:13 PM 2026-05-27),
the event detector cannot see their tick movement and never fires events on
them, even though they are the most actionable candidates.

## Repro 2026-05-27 12:13 PM AZ
- Tier 2 scanner: CRSR (Vol 5 Min 379%), MUU (239%)
- Event tape: 0 events
- price_ticks has zero rows for CRSR or MUU (not subscribed)

## Fix paths

Path 1 (recommended): dynamic subscription
- Every 30 sec, fetch current scanner Tier 1/2/3 ticker list
- Diff against current IEX subscriptions
- Call ws.send({action:subscribe, params:T.NEW1,T.NEW2,...}) for adds
- Call ws.send({action:unsubscribe, params:T.OLD1,...}) for drops
- Cap total subscribed at 200 to respect IEX free-tier limits

Path 2: bigger static list
- Subscribe to top 500 most-active tickers via daily refresh
- Simpler but less adaptive

## Priority
Medium. Phase 2.5 works for in-fleet event detection but misses the most
actionable signals (fresh convergence). Add to power paste backlog.
