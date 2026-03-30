# TradeMinds System Test Checklist

## Core Services
- [x] `127.0.0.1:8080` Arena server listens and serves auth/login flow
- [x] `127.0.0.1:8000` Unified Trader listens and serves dashboard
- [x] Startup scripts choose correct interpreters and fail loudly on bind/start errors

## Dashboards
- [x] Arena dashboard HTML loads from `dashboard/app.py`
- [x] Unified Trader HTML loads from `main_crew.py`
- [x] Unified Trader Starfleet Positions panel loads `/api/positions`
- [ ] Swagger docs are reachable on both services where expected

## Unified Trader APIs (`8000`)
- [x] `/api/crew/*` background run, scout, review, Sunday, runs, strategies, lock control
- [x] `/api/portfolios/*` list, unified view, exposure, detail, open positions, close/open trade flows
- [x] `/api/positions` legacy Arena positions feed
- [x] `/healthz`
- [x] `/api/schedule`

## Arena APIs (`8080`)
- [x] Auth/session endpoints
- [x] Arena leaderboard, player detail, trades, open positions, signals, history, pnl
- [x] Operations/status endpoints
- [x] Market data endpoints
- [x] News / war room endpoints
- [x] Scanner status endpoints
- [ ] Risk / model control / kill switch endpoints
- [x] Backtest / strategy lab / analytics endpoints
- [x] UOA endpoints mounted under `/api/uoa`
- [x] Broker status endpoints (`/api/alpaca/*`, `/api/webull/*`)

## Broker Integrations
- [x] Alpaca Paper connection status
- [x] Alpaca positions endpoint
- [ ] Alpaca order endpoints where exposed
- [ ] Alpaca sync into `portfolio_positions`
- [x] Webull live portfolio fetch
- [x] Webull sync into legacy `positions`

## Background Jobs
- [x] Arena scheduler loop boots
- [x] Crew APScheduler boots
- [ ] Scanner tick
- [ ] DayBlade tick
- [ ] VIX / earnings / journal / war room jobs
- [ ] Webull sync job
- [ ] Alpaca position sync job
- [ ] Sunday jobs: Picard, Archer, strategy lab, crew strategy, reference import, weekly tuning

## Scanners / Engines
- [x] Gap scanner
- [x] Theta scanner
- [ ] Imbalance detector
- [ ] Discovery scanner
- [x] Universe scanner
- [x] Strategy scan
- [ ] Strength scanner
- [ ] Cross-asset / skew / flow lean
- [ ] GEX / SMA / impulse / trend forecast

## AI Agents / Players
- [x] AI player roster loads from `ai_players`
- [x] Active/paused state reflected correctly
- [ ] Crew pipeline agent flows reachable
- [ ] War Room synthesis / Riker XO reachable
- [x] DayBlade and metals specialist paths reachable

## Data Stores
- [x] `data/trader.db` schema present
- [x] `positions` table reads correctly
- [x] `portfolio_positions` table reads correctly
- [x] `crew_runs` / `crew_strategies` / `portfolios` readable

## Static Checks
- [x] Python syntax compile pass across repo
- [x] Obvious runtime import/name errors fixed
- [x] Logs reviewed for repeated startup/runtime failures

## Verified Notes
- `2026-03-27`: Added low-risk compatibility aliases for `/healthz`, `/api/schedule`, `/api/operations/status`, `/api/recent-trades`, `/api/recent-signals`, `/api/news-feed`, `/api/backtest/history/leaderboard`, `/api/costs`, and `/api/uoa/scans`.
- `2026-03-27`: Fixed Arena backtest route masking by moving `GET /api/backtest/{player_id}` after specific `/api/backtest/*` endpoints.
- `2026-03-27`: DayBlade status latency improved from about `17.9s` to about `5.9s` by parallelizing ticker price fetches.
- `2026-03-27`: Verified scanner triggers: gap, theta, navigator universe, navigator strategies, navigator full scan, UOA quick scan, and UOA ticker scan all return success start states.
- `2026-03-27`: Verified model-control state changes restore correctly for `super-agent` and global `pause_all`.
- `2026-03-27`: Verified Strategy Lab `run` and `optimize` routes complete on the current live process; `backtest/run` starts successfully and reports `running`.

## Remaining Coverage
- [ ] Trade execution or broker order-placement endpoints
- [ ] Kill-switch or destructive admin actions
- [ ] Long-running backtest completion and result persistence after full run
- [ ] Background job execution over time, not just startup/route health
- [ ] Scanner families not yet exercised: imbalance, discovery, strength, cross-asset, skew, flow lean, impulse, trend forecast
