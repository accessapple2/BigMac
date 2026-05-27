# HM-OLLIE-LIVE-SCANNER-DASHBOARD-TILE v2

## Vision
Match Trade Ideas Holly AI + Swing Trade Watcher + event tape, then exceed it
by leveraging the autonomous fleet (which Trade Ideas users don't have).

## Reference visuals (Trade Ideas screenshots, 2026-05-27)
- Swing Trade Watcher right-side panel: 20-row tabular heatmap
  - Columns: Symbol, Price ($), Chg Close, Chg Opn (%), Rel Vol, Vol 5 Min (%)
  - Cells color-coded green→red by intensity (Vol 5 Min "468.6" deep green = firing)
  - Selected row highlights blue
- Big chart panel left: daily candles, anchored VWAP wave, vol bars below
- Event tape footer: "Running up quickly: +$0.05 in less than one minute  AMPY 5.10  07:16AM"
- Tiny weekly chart bottom-left with multi-month % gain badge

## Three layers to build (sequenced)

### Phase 1 — Scanner panel + heatmap (this week)
Drop into dashboard/static/index.html near Fleet Activity (~line 6500).
- New section: "🔭 Ollie Live Scanner"
- Three sub-tables (Tier 1: 5+ strategies, Tier 2: 4, Tier 3: 3)
- Per row: ticker | strategies-agreeing-badges | conf | entry | stop | target | rr
- "IN FLEET" grey badge for held tickers
- Auto-refresh every 30s (matches existing tick)
- NEW columns matching Swing Trade Watcher:
  - Chg Close (% from prior close) — color heatmap
  - Chg Open (% from today's open) — color heatmap  
  - Rel Vol (vs 20-day avg)
  - Vol 5 Min (% of 5-min historical avg) — deep-green when >300%
- Data source: strategy_signals + market_snapshots + volume_baselines
- New endpoint: GET /api/scanner/live → {tier1[], tier2[], tier3[]}

### Phase 2 — Live event tape (next week)
- Footer ribbon below scanner: scrolling event feed
- Events sourced from volume_alerts table (already exists)
- Each event: ticker, narration string ("Running up quickly +$X in <1min"),
  price, timestamp, magnitude badge
- Audio "ding" toggle for Tier 1 events
- New endpoint: GET /api/scanner/events → last N events streaming
- Backend event detector: new module engine/event_tape.py
  - Watches market_snapshots delta vs prev tick
  - Fires "Running up quickly" when delta > 1.5x ATR/min
  - Fires "Price crossed above close" when crossing prev day close
  - Fires "Vol burst" when 5-min vol > 3x avg

### Phase 3 — Tick-rate refresh (later)
- WebSocket push from main.py → dashboard
- Sub-second updates for ticker the Admiral is watching
- Falls back to 30s polling for non-focused tickers
- Adds endpoint: WS /api/scanner/stream

## Data sources already available
- strategy_signals (ticker, strategy_name, confidence, entry/stop/target, created_at)
- volume_alerts (ticker, alert_type, timestamp, magnitude)
- volume_baselines (ticker, avg_5min, avg_daily)
- market_snapshots (ticker, price, change_pct, ts)
- mover_watchlist (curated ticker list)
- ticker_metadata (sector, market_cap, etc.)
- /api/movers (existing endpoint at dashboard/app.py:1583)

## Why this beats Trade Ideas Holly
1. Our convergence engine already runs 10+ strategies in parallel; Holly runs
   her overnight backtested top-N.
2. Our fleet of AI agents (Spock, Worf, Dr. McCoy, dayblade) AUTONOMOUSLY
   executes on convergence. Trade Ideas users still click. We're past that.
3. Our IC Squadron + bull/bear spread agents add options dimension Holly lacks.
4. Plutus financial-intelligence officer critiques every closed trade.
5. We control the entire stack — can add domain-specific events Holly can't
   (e.g., "Sector rotation into Energy detected", "Gamma flip below SPY spot").

## Open questions
1. Polling cadence: 30s (cheap) vs 5s (Trade Ideas-class) vs WebSocket (best)?
2. Event tape: from volume_alerts only, or build new event_tape.py?
3. Audio alert: toggle per-tier? Per-symbol?
4. Mobile-friendly variant (Bonnie's phone)?

## Priority
HIGH — this becomes the primary "what should I look at right now?" view.

## Estimated effort
- Phase 1: 1 day (backend endpoint + frontend tile + test)
- Phase 2: 2-3 days (event detector + event tape UI + alert toggle)
- Phase 3: 3-5 days (WebSocket migration + load test)
