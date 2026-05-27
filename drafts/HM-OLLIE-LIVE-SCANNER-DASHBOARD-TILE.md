# HM-OLLIE-LIVE-SCANNER-DASHBOARD-TILE v3 (LOCKED FOR PHASE 1 BUILD)

## Vision
Match Trade Ideas Holly AI + Swing Trade Watcher + event tape, then exceed it
by leveraging the autonomous fleet.

## Reference visuals (Trade Ideas screenshots, 2026-05-27)
- Swing Trade Watcher right-side panel: 20-row tabular heatmap
  - Columns: Symbol, Price ($), Chg Close, Chg Opn (%), Rel Vol, Vol 5 Min (%)
  - Cells color-coded green→red by intensity (Vol 5 Min "468.6" deep green = firing)
  - Selected row highlights blue
- Big chart panel left: daily candles, anchored VWAP wave, vol bars below
- Event tape footer: "Running up quickly: +$0.05 in less than one minute  AMPY 5.10  07:16AM"

## DECISIONS LOCKED (2026-05-27 Admiral sign-off)
- **Polling cadence:** 30s for now. WebSocket upgrade path in Phase 3 IF we
  determine tighter cadence delivers material edge in production.
- **Event tape source:** reuse existing `volume_alerts` table.
- **Audio alert:** Tier 1 ding ON by default. Per-tier toggle in scanner header.
- **Mobile variant:** desktop-first now, mobile follow-up later.

## Phase 1 build — Scanner panel + heatmap (SHIP THIS)

### Backend (dashboard/app.py)
- New endpoint: `GET /api/scanner/live`
- Returns JSON:
```json
  {
    "ts": "2026-05-27T...",
    "tier1": [{"ticker":"MNTS","strategies":["breakout_volume",...],
               "conf":100,"entry":19.41,"stop":16.90,"target":23.18,"rr":1.5,
               "price":19.50,"chg_close_pct":1.2,"chg_open_pct":0.8,
               "rel_vol":1.4,"vol_5min_pct":287.3,"in_fleet":true}, ...],
    "tier2": [...],
    "tier3": [...]
  }
```
- SQL joins:
  - strategy_signals (last 90 min) → tiered by COUNT(DISTINCT strategy_name)
  - LEFT JOIN market_snapshots (latest row per ticker) for price/chg
  - LEFT JOIN volume_baselines for rel_vol / vol_5min calc
  - LEFT JOIN positions (qty != 0) for in_fleet flag
- Cache 25s in-memory to absorb polling spam

### Frontend (dashboard/static/index.html)
- New section "🔭 Ollie Live Scanner" inserted near Fleet Activity (~line 6500)
- Three collapsible tier tables:
  - Tier 1 (5+ strategies, green border)
  - Tier 2 (4 strategies, yellow border)
  - Tier 3 (3 strategies, light border)
- Each row columns (matching Swing Trade Watcher):
  - Symbol | Price | Chg Close | Chg Open | Rel Vol | Vol 5 Min | Conf | Entry | Stop | Target | R/R | Strategies
- Cell coloring: green→red heatmap on Chg Close/Open/Vol columns
- "IN FLEET" greyed-out badge for held tickers
- Click row → existing ticker drawer
- New JS function `renderOllieLiveScanner()` on standard 30s tick
- Header controls: tier collapse toggles, audio toggle per-tier (default Tier 1 ON)

### Phase 1 acceptance criteria
- [ ] Endpoint responds <500ms
- [ ] Tile renders all 3 tiers correctly
- [ ] Heatmap colors visually distinct
- [ ] IN FLEET badge applied to held tickers
- [ ] Refreshes every 30s without UI flash
- [ ] Click row opens ticker drawer
- [ ] Light + dark theme both readable

## Phase 2 — Live event tape (next)
- Footer ribbon below scanner: scrolling event feed
- Pulls from `volume_alerts` table (created_at desc, last 15 min)
- Each event: ticker, narration, price, ts, magnitude badge
- Tier 1 audio ding when new alert appears for a ticker also in Tier 1 scanner
- Per-tier audio toggle persisted in localStorage
- New endpoint: `GET /api/scanner/events?since=<ts>`

## Phase 3 — Tighter cadence (gated on Phase 1+2 in production)
- Evaluate: is 30s costing us trades vs 5s? Measure first.
- If yes, WebSocket migration: main.py → dashboard push
- Sub-second ticker focus when row hovered

## Data sources already in DB (verified 2026-05-27)
- strategy_signals (ticker, strategy_name, confidence, entry_price, stop_price, target_price, created_at)
- volume_alerts (ticker, alert_type, magnitude, timestamp)
- volume_baselines (ticker, avg_5min, avg_daily)
- market_snapshots (ticker, price, change_pct, ts)
- mover_watchlist (curated tickers)
- ticker_metadata (sector, market_cap, etc.)
- positions (in_fleet flag source)
- existing /api/movers endpoint at dashboard/app.py:1583 (reference impl)

## Why this beats Trade Ideas Holly
1. Convergence engine runs 10+ strategies in parallel; Holly's overnight backtested top-N
2. Autonomous fleet executes on convergence — Holly users still click
3. IC Squadron + spread agents add options dimension Holly lacks
4. Plutus officer critiques every closed trade
5. Full-stack control — can add domain events Holly can't (sector rotation, gamma flip, etc.)

## Effort estimate
- Phase 1: 1 day (Scotty on bigmac)
- Phase 2: 1-2 days
- Phase 3: 3-5 days (only if Phase 1/2 prove cadence is the bottleneck)
