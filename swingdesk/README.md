# SwingDesk — Phase 1

Standalone swing trading assistant. Reads from ~/autonomous-trader/.env.
Real Polygon candles. No Alpaca wiring yet (Phase 2).

## Deploy

```bash
# Copy to your machine
cp -r swingdesk/ ~/autonomous-trader/swingdesk/

# Install deps (uses OT venv)
cd ~/autonomous-trader
./venv/bin/pip install fastapi uvicorn

# Start backend
cd ~/autonomous-trader
./venv/bin/python3 swingdesk/backend.py
# → http://localhost:8889

# Open frontend
open ~/autonomous-trader/swingdesk/index.html
# or serve it: python3 -m http.server 8890 --directory swingdesk/
```

## What's live
- Polygon candles: real OHLCV bars for chart + EMA overlays
- Watchlist: live snapshot prices from Polygon
- Scanner: scores your universe with real EMA/RSI/vol
- Position sizer: risk-first math, ATR-based stop/target suggestions
- Trade planner: logs planned trades to swingdesk.db
- Risk gate: daily loss limit, max positions, circuit breaker

## What's NOT wired yet (Phase 2)
- Alpaca order submission (keys loaded, not used)
- Autonomous signal → order loop
- OllieTrades Test Kitchen tab integration

## Phase 2 checklist
- [ ] Wire /api/trade/plan → Alpaca bracket order
- [ ] Autonomous loop: scanner → score > 65 → gate check → submit
- [ ] Correlation guard (don't stack correlated positions)
- [ ] Migrate as Test Kitchen tab in OT dashboard (port 8080)

## DB
swingdesk.db lives at ~/autonomous-trader/swingdesk.db
Separate from trader.db — safe, no OT data touched.
