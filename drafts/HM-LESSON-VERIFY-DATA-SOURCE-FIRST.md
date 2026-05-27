# HM-LESSON-VERIFY-DATA-SOURCE-FIRST

## Background
2026-05-27: Spec HM-OLLIE-EVENT-TAPE-V2-REALTIME locked "Polygon Stocks Starter
$29/mo — already paid, true streaming, sub-second" as the data source for
Phase 2.5 tick recorder. Scotty implemented Component 1 cleanly, then a live
probe revealed Stocks Starter is REST-only, 15-min delayed; WebSocket trades
access requires Stocks Advanced ($499/mo).

## Lesson
For any spec that names a data source as "available", verify the actual tier
capability BEFORE locking the spec. A live probe of the auth + first
subscribe is a 60-second test that prevents downstream rework.

## Rule going forward
- New specs that depend on external streaming/API: include a "Data Source
  Probe" section showing the actual capability test that confirms the tier
  works for the intended use.
- Dont trust marketing-page descriptions ("starter includes real-time data")
  — they often refer to REST snapshots, not streaming.
- If pivot needed mid-build, the cost is one module modification — not the
  whole spec. Component-based design saved us here.

## Cost of this miss
~3 hours of Scotty cycles (Component 1 written Polygon-shaped, will need
~20 LOC change for Alpaca). Recovery via Alpaca IEX is $0 incremental.
