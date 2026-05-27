# HM-OLLIE-EVENT-TAPE-V2-REALTIME (Phase 2.5)

## Vision
Replace the batch-shaped event tape with a true real-time event detector that
produces Holly-style "Running up quickly +$X in <1min" events from a live
trade stream. Match or exceed Trade Ideas event tape.

## DECISIONS LOCKED (2026-05-27 Admiral sign-off, revised post-pivot)
- **Tick source:** ~~Polygon Stocks Starter~~ **Alpaca IEX feed** (free with
  paper account). Polygon Stocks Starter ($29/mo) does NOT include WebSocket
  trades — confirmed via live probe 2026-05-27: auth_success then subscribe
  `T.*` returns `{"status":"error","message":"not authorized"}`. WebSocket
  trades require Polygon Stocks Advanced ($499/mo). Lesson banked at
  `drafts/HM-LESSON-VERIFY-DATA-SOURCE-FIRST.md`.
- **Disk/CPU budget:** negligible on bigmac M4 — proceed without budget concerns.
- **Phase 2 (volume_alerts batch tape):** KEEP as secondary "📋 Batch Scan Alerts"
  panel. New realtime tape sits above as the headline.

## Data Source Probe (2026-05-27, pre-build verification)
- **wss://stream.data.alpaca.markets/v2/iex** (Alpaca IEX feed, free)
- Auth: `{"action":"auth","key":ALPACA_API_KEY,"secret":ALPACA_SECRET_KEY}`
  → `[{"T":"success","msg":"authenticated"}]`
- Subscribe: `{"action":"subscribe","trades":["AAPL","MSFT","NVDA"]}`
  → `[{"T":"subscription","trades":["AAPL","MSFT","NVDA"], ...}]`
- Trade frame shape:
  `{"T":"t","S":"MSFT","i":12053,"x":"V","p":412.33,"s":40,"c":["@"],"z":"C","t":"2026-05-27T18:17:17.514045708Z"}`
- Coverage caveat: IEX is ~3% of US equity volume. Event detection on IEX-only
  ticks will lag whole-market by a few seconds and miss some moves entirely.
  Acceptable for v1 PoC; SIP upgrade path documented below.

## Upgrade path (deferred until live-session edge proves real)
- **Alpaca SIP feed** — `wss://stream.data.alpaca.markets/v2/sip`, $99/mo,
  full-volume coverage across all venues. Same WS protocol, same field names,
  same auth — flip the URL constant in `engine/tick_recorder.py` and bump
  the data plan in Alpaca dashboard. Zero code rewrite.
- **Polygon Stocks Advanced** — $499/mo, original spec target. Larger code
  delta (different WS protocol + field names) — re-pivot only worth it if SIP
  proves insufficient.

## Four components to build

### Component 1 — Tick recorder
**New module:** `engine/tick_recorder.py`
- Subscribes to Alpaca IEX WS trade stream
- Records to new table `price_ticks` (symbol, price, volume, ts)
- Tier-aware sampling (deferred to follow-up C1.x; v1 records every tick on
  subscribed symbols):
  - **In-fleet tickers + Tier 1/2 scanner tickers:** every tick (full firehose)
  - **Tier 3 + watchlist tickers:** every 15 sec aggregated
  - **Everything else in S&P 1500:** every 60 sec aggregated
- Rolling 4-hour retention with hourly cleanup job
- Estimated row count: ~50-100k/day on IEX (~5-10x more on SIP)
- Estimated disk: <100MB/week on IEX

**Alpaca endpoint:**
- WebSocket: `wss://stream.data.alpaca.markets/v2/iex`
- Auth: `ALPACA_API_KEY` + `ALPACA_SECRET_KEY` env vars (already configured)
- Reference: https://alpaca.markets/docs/api-references/market-data-api/stock-pricing-data/realtime/

### Component 2 — Event detector
**New module:** `engine/event_tape.py`
- Runs every 30 sec in main.py main loop (already exists)
- For each ticker with ticks in last 5 min:
  - Compute Δprice / Δtime from last N ticks
  - Detect event types (described below)
  - Write to new `event_tape` table
  - Dedupe: max 1 event per symbol per event_type per 60 sec

**Event types to detect:**
1. **`running_up_fast`** — price up >1.5× ATR/min OR up >0.5% in <1min
   - Narration: "Running up quickly: +$X.XX in less than one minute"
2. **`running_down_fast`** — mirror
   - Narration: "Running down quickly: -$X.XX in less than one minute"
3. **`crossed_above_close`** — price crosses yesterday's close from below
   - Narration: "Price crossed above prev close: $X.XX"
4. **`crossed_below_close`** — mirror
   - Narration: "Price crossed below prev close: $X.XX"
5. **`volume_burst`** — 60-sec volume > 3× rolling 20-min baseline
   - Narration: "Volume burst: 3.2× normal"
6. **`new_session_high`** — exceeds today's high by >0.1%
   - Narration: "New session high: $X.XX"
7. **`new_session_low`** — mirror
8. **`gap_fill_complete`** — bounced back to fill morning gap
   - Narration: "Gap filled to $X.XX"
9. **`breakout_resistance`** — clears 20-day high
   - Narration: "Breakout above 20-day high: $X.XX"
10. **`failed_breakdown`** — undercut prev low then reclaimed
    - Narration: "Failed breakdown — reclaimed $X.XX"

### Component 3 — Event tape table

**New table `event_tape` columns:**
- `id INTEGER PRIMARY KEY AUTOINCREMENT`
- `symbol TEXT NOT NULL`
- `event_type TEXT NOT NULL`
- `narration TEXT NOT NULL`
- `price REAL`
- `magnitude REAL`
- `in_scanner_tier INTEGER` (1, 2, 3, or NULL)
- `detected_at TEXT DEFAULT (datetime('now'))`
- `metadata TEXT` (JSON for ad-hoc context)

**Indexes:**
- `idx_event_tape_detected_at ON event_tape(detected_at)`
- `idx_event_tape_symbol ON event_tape(symbol)`

Rolling 24h retention, cleanup at midnight.

**New table `price_ticks` columns:**
- `id INTEGER PRIMARY KEY AUTOINCREMENT`
- `symbol TEXT NOT NULL`
- `price REAL NOT NULL`
- `volume INTEGER`
- `ts TEXT NOT NULL`

**Indexes:**
- `idx_price_ticks_symbol_ts ON price_ticks(symbol, ts)`
- `idx_price_ticks_ts ON price_ticks(ts)`

Rolling 4h retention, hourly cleanup.

### Component 4 — API + UI rewire

**Backend — dashboard/app.py:**
- New endpoint: `GET /api/scanner/events_realtime?since=<ts>&limit=50`
- Reads from `event_tape` table NOT `volume_alerts`
- Same response shape as existing `/api/scanner/events` for frontend compat
- 5-second cache (events are time-sensitive)

**Frontend — dashboard/static/index.html:**
- New panel "📡 Live Event Tape" inserted ABOVE existing Phase 2 card
- Existing Phase 2 card renames to "📋 Batch Scan Alerts" with caption
  explaining it's the 75-min batch scanner output
- New card uses same row style, tier badges, audio toggles
- Calls new `/api/scanner/events_realtime` endpoint

## Why this beats Trade Ideas
1. **In-scanner-tier flag is meaningful** — events tied to multi-strategy
   convergence, not just generic movers
2. **Audio dings are smart** — T1 ding only when realtime event fires on a
   ticker already converging across 5+ strategies (real signal, not noise)
3. **Plutus integration potential** — Phase 3 idea: Dr. McCoy critiques events
   live ("MNTS running up +$0.18/30sec — your covered call is at risk")
4. **Cost: ~$0** — Alpaca IEX feed bundled with paper account

## Risks to flag
1. **IEX coverage gap** — only ~3% of US equity volume. Some trades on NASDAQ /
   NYSE / dark pools won't appear in our stream. Mitigation: this is a v1 PoC;
   upgrade to SIP ($99/mo) once edge proves real.
2. **Backpressure** — if main.py loop is slow, ticks could pile up. Use bounded
   queue (drop oldest, max 1000 ticks in-flight). Already implemented.
3. **Event spam** — if thresholds are too loose, the tape becomes noise. Start
   with conservative thresholds; tune from production usage.
4. **WS reconnect** — Alpaca WS disconnects happen. Auto-reconnect with
   exponential backoff (10s → 20s → 40s → 120s max). Log dropped-connection
   events to trader.log.
5. **Subscription cap** — Alpaca IEX free tier allows up to 30 concurrent
   trade subscriptions on one connection. Recorder caps universe at 60 — needs
   tighter bound. Reduce `_MAX_SUBSCRIBED_SYMBOLS` to 30 for IEX tier.

## Acceptance criteria
- [ ] Alpaca WS connects and stays up >1 hour without manual intervention
- [ ] `price_ticks` table populating with ≥100 rows/min during market hours
      (lower than the Polygon estimate due to IEX coverage)
- [ ] `event_tape` table populating with at least one event every 5 min during
      market hours on average (depends on market activity)
- [ ] `/api/scanner/events_realtime` responds <200ms
- [ ] New Live Event Tape card renders above Batch Scan Alerts
- [ ] Events appear with proper amber fade-in
- [ ] T1 audio ding fires when realtime event lands on Tier 1 scanner ticker
- [ ] No regressions to Phase 1 scanner or Phase 2 batch tape

## Estimated effort
1.5 days for Scotty. Possibly stretched to 2 if Alpaca WS handling has edge cases.

## Acceptance test plan
- Manual: Watch the bridge dashboard during a 30-min live market window. Should
  see events accumulate in the live tape. Click a row → ticker drawer opens.
- Performance: `top -pid $(pgrep -f main.py)` should show CPU < 10% sustained.
- Database: `sqlite3 data/trader.db "SELECT COUNT(*) FROM price_ticks WHERE ts > datetime('now','-1 hour')"`
  should return ≥1000 during market hours on IEX.

## Sacred rules
- Don't drop `trader.db`, `arena.db`, `tractor.db`. Migration is ADD-only.
- No new network calls beyond the Alpaca WS stream.
- Bound the tick recorder memory at 1000 in-flight ticks max.
- Manual browser smoke before declaring done.

## Build sequence (Scotty)
1. Component 1 first: Alpaca WS connection + tick recorder + `price_ticks` table.
   Verify table populating before moving on.
2. Component 2: event detector module + `event_tape` table. Smoke with
   conservative thresholds.
3. Component 4 backend: `/api/scanner/events_realtime` endpoint.
4. Component 4 frontend: new card above existing batch tape.
5. End-to-end browser smoke during live market window.
