# HM-OLLIE-EVENT-TAPE-V2-REALTIME (Phase 2.5)

## Vision
Replace the batch-shaped event tape with a true real-time event detector that
produces Holly-style "Running up quickly +$X in <1min" events from Polygon tick
streams. Match or exceed Trade Ideas event tape.

## DECISIONS LOCKED (2026-05-27 Admiral sign-off)
- **Tick source:** Polygon (already paid, true streaming, sub-second)
- **Disk/CPU budget:** negligible on bigmac M4 — proceed without budget concerns
- **Phase 2 (volume_alerts batch tape):** KEEP as secondary "Batch Scan Alerts"
  panel. New realtime tape sits above as the headline.

## Four components to build

### Component 1 — Tick recorder
**New module:** `engine/tick_recorder.py`
- Subscribes to Polygon trades/aggregates stream
- Records to new table `price_ticks` (symbol, price, volume, ts)
- Tier-aware sampling:
  - **In-fleet tickers + Tier 1/2 scanner tickers:** every tick (full firehose)
  - **Tier 3 + watchlist tickers:** every 15 sec aggregated
  - **Everything else in S&P 1500:** every 60 sec aggregated
- Rolling 4-hour retention with hourly cleanup job
- Estimated row count: ~50-100k/day
- Estimated disk: <100MB/week

**Polygon endpoint:**
- WebSocket: wss://socket.polygon.io/stocks with action=subscribe param=T.* for trades
- Reference: https://polygon.io/docs/stocks/ws_stocks_t
- Auth: existing POLYGON_API_KEY env var

### Component 2 — Event detector
**New module:** `engine/event_tape.py`
- Runs every 30 sec in main.py main loop (already exists)
- For each ticker with ticks in last 5 min:
  - Compute delta-price / delta-time from last N ticks
  - Detect event types (described below)
  - Write to new event_tape table
  - Dedupe: max 1 event per symbol per event_type per 60 sec

**Event types to detect:**
1. `running_up_fast` — price up >1.5x ATR/min OR up >0.5% in <1min
   - Narration: "Running up quickly: +$X.XX in less than one minute"
2. `running_down_fast` — mirror
   - Narration: "Running down quickly: -$X.XX in less than one minute"
3. `crossed_above_close` — price crosses yesterdays close from below
   - Narration: "Price crossed above prev close: $X.XX"
4. `crossed_below_close` — mirror
5. `volume_burst` — 60-sec volume > 3x rolling 20-min baseline
   - Narration: "Volume burst: 3.2x normal"
6. `new_session_high` — exceeds todays high by >0.1%
   - Narration: "New session high: $X.XX"
7. `new_session_low` — mirror
8. `gap_fill_complete` — bounced back to fill morning gap
   - Narration: "Gap filled to $X.XX"
9. `breakout_resistance` — clears 20-day high
   - Narration: "Breakout above 20-day high: $X.XX"
10. `failed_breakdown` — undercut prev low then reclaimed
    - Narration: "Failed breakdown — reclaimed $X.XX"

### Component 3 — Event tape table

New table `event_tape` columns:
- id INTEGER PRIMARY KEY AUTOINCREMENT
- symbol TEXT NOT NULL
- event_type TEXT NOT NULL
- narration TEXT NOT NULL
- price REAL
- magnitude REAL
- in_scanner_tier INTEGER (1, 2, 3, or NULL)
- detected_at TEXT DEFAULT (datetime now)
- metadata TEXT (JSON for ad-hoc context)

Indexes:
- idx_event_tape_detected_at ON event_tape(detected_at)
- idx_event_tape_symbol ON event_tape(symbol)

Rolling 24h retention, cleanup at midnight.

New table `price_ticks` columns:
- id INTEGER PRIMARY KEY AUTOINCREMENT
- symbol TEXT NOT NULL
- price REAL NOT NULL
- volume INTEGER
- ts TEXT NOT NULL

Indexes:
- idx_price_ticks_symbol_ts ON price_ticks(symbol, ts)
- idx_price_ticks_ts ON price_ticks(ts)

Rolling 4h retention, hourly cleanup.

### Component 4 — API + UI rewire

**Backend — dashboard/app.py:**
- New endpoint: GET /api/scanner/events_realtime?since=<ts>&limit=50
- Reads from event_tape table NOT volume_alerts
- Same response shape as existing /api/scanner/events for frontend compat
- 5-second cache (events are time-sensitive)

**Frontend — dashboard/static/index.html:**
- New panel "Live Event Tape" inserted ABOVE existing Phase 2 card
- Existing Phase 2 card renames to "Batch Scan Alerts" with caption
  explaining its the 75-min batch scanner output
- New card uses same row style, tier badges, audio toggles
- Calls new /api/scanner/events_realtime endpoint

## Why this beats Trade Ideas
1. **In-scanner-tier flag is meaningful** — events tied to multi-strategy convergence
   not just generic movers
2. **Audio dings are smart** — T1 ding only when realtime event fires on a ticker
   already converging across 5+ strategies (real signal, not noise)
3. **Plutus integration potential** — Phase 3 idea: Dr. McCoy critiques events
   live ("MNTS running up +$0.18/30sec — your covered call is at risk")
4. **Cost: ~$0** — Polygon stream already subscribed

## Risks to flag
1. **Polygon rate limits** — $29 starter bundle should handle this, but smoke
   test the WS connection before assuming throughput
2. **Backpressure** — if main.py loop is slow, ticks could pile up. Use bounded
   queue (drop oldest, max 1000 ticks in-flight)
3. **Event spam** — if thresholds are too loose, the tape becomes noise. Start
   with conservative thresholds; tune from production usage
4. **WS reconnect** — Polygon WS disconnects happen. Auto-reconnect with
   exponential backoff. Log dropped-connection events to trader.log

## Acceptance criteria
- [ ] Polygon WS connects and stays up >1 hour without manual intervention
- [ ] price_ticks table populating with >=1k rows/min during market hours
- [ ] event_tape table populating with at least one event every 5 min during
      market hours on average (depends on market activity)
- [ ] /api/scanner/events_realtime responds <200ms
- [ ] New Live Event Tape card renders above Batch Scan Alerts
- [ ] Events appear with proper amber fade-in
- [ ] T1 audio ding fires when realtime event lands on Tier 1 scanner ticker
- [ ] No regressions to Phase 1 scanner or Phase 2 batch tape

## Estimated effort
1.5 days for Scotty. Possibly stretched to 2 if Polygon WS handling has edge cases.

## Acceptance test plan
- Manual: Watch the bridge dashboard during a 30-min live market window. Should
  see events accumulate in the live tape. Click a row → ticker drawer opens.
- Performance: top -pid $(pgrep -f main.py) should show CPU < 10% sustained.
- Database: sqlite3 data/trader.db "SELECT COUNT(*) FROM price_ticks WHERE ts > datetime now -1 hour" should return >=5000.

## Sacred rules
- Dont drop trader.db, arena.db, tractor.db. Migration is ADD-only.
- No new network calls beyond the Polygon WS stream.
- Bound the tick recorder memory at 1000 in-flight ticks max.
- Manual browser smoke before declaring done.

## Build sequence (Scotty)
1. Component 1 first: Polygon WS connection + tick recorder + price_ticks table.
   Verify table populating before moving on.
2. Component 2: event detector module + event_tape table. Smoke with conservative
   thresholds.
3. Component 4 backend: /api/scanner/events_realtime endpoint.
4. Component 4 frontend: new card above existing batch tape.
5. End-to-end browser smoke during live market window.
