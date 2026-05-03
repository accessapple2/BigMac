# Session 2026-04-27 — Polygon Port + 6-Month Fleet Backtest

**Duration:** ~6 hours focused engineering
**Sprint goal:** Repair UI surfaces 2/3/4, complete Polygon migration, run real 6-month fleet backtest

---

## Shipped (in production, verified)

### UI Surface Repairs

1. **Surface 4 — Sector Watch:** Full rebuild from broken Yahoo+CORS-proxy fetches to internal /api/sectors/heatmap. 12-sector SPDR rotation strip with click-to-expand holdings. File: dashboard/static/sector_watch.html (backup .bak.20260427_rebuild).

2. **Surface 2 — Leaderboard:** SQL fix in dashboard/app.py line 1911 — added COALESCE(p.halt_reason, "") as halt_reason to SELECT. Eliminated silent HTTP 500 errors. Backup .bak.20260427_leaderboard_halt_reason.

3. **Surface 3 — Screener Pro frontend wire-up:** Two edits to dashboard/static/index.html: line 7654 fetch URL switched to /api/screener/pro; line 7665 added r.above_sma200 fallback. Backup .bak.20260427_screener_pro.

### Polygon Engine Port (the headline)

4. **engine/market_data.py:** Added get_polygon_bars(symbols, days, max_workers) alongside existing get_alpaca_bars(). ThreadPoolExecutor parallelization, returns same dict-of-DataFrames shape as Alpaca. Backup .bak.20260427_polygon.

5. **strategies/polygon_client.py:** Added fetch_market_snapshot() — Polygon bulk endpoint returning ~12,488 US tickers with full OHLCV in 2 seconds. Includes volume for liquidity filtering. Backup .bak.20260427_market_snapshot.

6. **engine/screener_engine.py:** Five stacked patches (each backed up separately):
   - _compute_indicators(df) — RSI(14) Wilder, SMA20/50/200, EMA8/21, above_sma flags, uptrend
   - _parse_yf_row calls **_compute_indicators(df) instead of null stubs
   - Fleet data unconditional (was only computed for WATCHLIST symbols)
   - _run_screener_lock (threading.Lock) + _last_known_results for single-flight dogpile prevention
   - Snapshot pre-filter architecture: Pass 0 (snapshot ~12k tickers in 2s) -> Pass 1 (liquidity + user filters -> top 200) -> Pass 1.5 (300-day bars on survivors only for indicator computation) -> Pass 2 (fundamentals/fleet/GEX)
   - Polygon import: get_polygon_bars as get_alpaca_bars alias
   - Backups: .bak.20260427_indicators, .bak.20260427_concurrency, .bak.20260427_pre_polygon_wire, .bak.20260427_pre_snapshot

### UOA Path Fix

7. **engine/trade_cards_api.py line 29:** UOA_DB = "trader.db" -> UOA_DB = "data/trader.db". Was hitting 12KB stub instead of 188MB live DB. Wheel endpoint, options flow, IV calculations all now hit real data. Backup .bak.20260427_uoa_path.

### 180-Day Backtest

8. **scripts/s6_180d_backtest.py:** Replaced download_data() body — yfinance -> get_polygon_bars. Avoids the rate-limiting that killed Season 6 last week. Backup .bak.20260427_pre_polygon.

### Memory + Safety

9. Memory rule hardened: "PUSH WORK, DO NOT GATEKEEP — HARD RULE" with explicit forbidden-phrase list. Disagreement only on technical merit, never on calendar/fatigue/timing.

---

## Browser-Verified End-to-End

Oversold Bounce preset on the live dashboard:
- Status: **6 of 50 matches**
- Real oversold names: LMT @ RSI 19.0, RTX @ 23.7, MCD @ 30.0, DPZ @ RSI 29.9 with 5.49x normal volume

Today the panel went from "0 of 10 matches returning empty placeholder data" -> real, actionable trade ideas surfaced from a 12,488-ticker universe in ~17 seconds.

---

## 6-Month Fleet Backtest — Real Numbers

**Period:** 2025-08-08 -> 2026-04-27 (180 trading days)
**Capital:** $100,000  |  **Benchmark:** SPY +12.24%
**Universe:** 25 unique symbols across 9 active S6 agents

| Rank | Agent | Return | Sharpe | MaxDD | WinRate | Trades | Alpha vs SPY |
|------|-------|--------|--------|-------|---------|--------|--------------|
| 1 | ollama-coder | +17.09% | 1.90 | -7.82% | 54.0% | 274 | **+4.85%** |
| 2 | ollama-qwen3 | +13.24% | 1.38 | -9.98% | 57.0% | 158 | **+1.00%** |
| 3 | chekov | +9.90% | 1.34 | -5.05% | 55.7% | 183 | -2.34% |
| 4 | ollama-llama | +6.45% | 0.71 | -9.85% | 56.0% | 191 | -5.79% |
| 5 | navigator | +4.95% | 0.64 | -7.20% | 53.1% | 256 | -7.29% |
| 6 | capitol-trades | +2.98% | 0.44 | -7.05% | 57.1% | 170 | -9.26% |
| 7 | neo-matrix | +2.97% | 0.31 | -9.92% | 53.4% | 232 | -9.27% |
| 8 | ollama-plutus | +1.50% | 0.28 | -4.51% | 41.9% | 62 | -10.74% |
| 9 | **ollie-auto** | **-2.45%** | **-1.28** | -3.02% | 39.3% | 56 | **-14.69%** |

### Headline findings

- Only 2 of 9 active agents beat SPY: Coder (+4.85% alpha, Sharpe 1.90) and Qwen3 (+1.00% alpha, Sharpe 1.38)
- The Super Trader gating layer (ollie-auto) is the worst performer: -2.45% return, Sharpe -1.28, win rate 39.3%
- Capitol Trades and Neo-Matrix are suspiciously identical (+2.97% and +2.98%) — possible signal correlation worth investigating
- Median agent underperforms SPY by ~7% — naive SPY buy-and-hold beats 7 of 9 active agents

### Methodology vs Apr 25 baseline

Apr 25 used engine/mega_backtest_6month.py (retrospective DB analysis, ~110 effective days). Today is a **true historical replay** — Polygon supplied 180 trading days of real prices, agents replayed signals against those prices. Different methodology, more rigorous.

---

## Tests Status

| Test | Status |
|------|--------|
| 1. Fleet replay (9 agents, 6mo, P&L + per-agent stats) | SHIPPED — see table |
| 2. Strategy isolation (neo-matrix, BULL momentum, oversold) | Partially in Test 1 as agents; presets are screener filters not registered agents — needs new harness |
| 3. Super Trader pipeline (Grade A/B, tiered TP, 0.75x TP1, 2hr stop) | Already in Test 1 as ollie-auto — result: -2.45%, Sharpe -1.28 |
| 4. Holodeck VectorBT regression vs Apr 17 OOS | NO BASELINE — holodeck_backtest_results table has 0 rows; Apr 17 numbers (CSP 6.05/5.42 per memory) not persisted anywhere we found |

---

## Next-Session Priorities

### Sprint priority 1 — Diagnose Ollie (Gate)

Likely candidates per memory:
- TP1 at 0.75x exiting winners too early
- 2-hour time stop killing momentum trades
- Grade A/B gate too tight

**Plan:** Run s6_180d_backtest.py against ollie-auto with each gate component disabled in turn. A/B test which feature is the killer.

### Sprint priority 2 — Investigate alpha producers (Coder + Qwen3)

These are the only winners. Read their signal logic, find what they do differently. Consider weighting them higher in autopilot.

### Sprint priority 3 — Strategy isolation harness (Test 2)

Build a harness that backtests screener presets as standalone strategies (Oversold Bounce, Momentum Breakout) using the 180-day Polygon-backed historical data. Answer: do the presets generate alpha or are they noise.

### Sprint priority 4 — Stand up Holodeck OOS persistence (Test 4)

holodeck_backtest_results table exists but has 0 rows. Wire engine/master_backtest.py (or whatever harness produced those Sharpes) to persist. Run a fresh OOS validation. Establish a real baseline.

### Sprint priority 5 — yfinance Goliath migration

Pattern C cluster (8 files: options/earnings/insider). Today Polygon migration pattern is reusable.

---

## Investigations dropped or downgraded

- **_fetch_alpaca_assets() 11,672 vs 88 mismatch:** Snapshot architecture bypassed it entirely. Low priority now.
- **Apr 17 Holodeck OOS Sharpe numbers:** Not in any docs file, not in DB. Treat as not recoverable.

---

## Files modified in production today

| File | Change | Backup |
|------|--------|--------|
| dashboard/static/sector_watch.html | Full rebuild | .bak.20260427_rebuild |
| dashboard/app.py (line 1911) | Leaderboard halt_reason fix | .bak.20260427_leaderboard_halt_reason |
| dashboard/static/index.html (lines 7654, 7665) | screenerPro wire-up | .bak.20260427_screener_pro |
| engine/market_data.py | Added get_polygon_bars | .bak.20260427_polygon |
| engine/screener_engine.py | 5 stacked patches | .bak.20260427_indicators, _concurrency, _pre_polygon_wire, _pre_snapshot |
| strategies/polygon_client.py | Added fetch_market_snapshot | .bak.20260427_market_snapshot |
| engine/trade_cards_api.py (line 29) | UOA_DB path fix | .bak.20260427_uoa_path |
| scripts/s6_180d_backtest.py | yfinance -> Polygon | .bak.20260427_pre_polygon |

---

## Operational notes

- Daemon: com.trademinds.trader on port 8080 (legacy launchd plist name; project renamed to OllieTrades Apr 8)
- Live DB: ~/autonomous-trader/data/trader.db (197MB, 180 tables)
- Stub at ~/autonomous-trader/trader.db (12KB) — only uoa/*.py files still hit it via bare "trader.db" strings; engine/trade_cards_api.py was the last critical caller, now fixed
- Polygon subscription: stocks $29 + options $29 = $58/mo total (already paid)
- Polygon Starter: unlimited API calls, 5+ years history, 15-min delayed data — sufficient for current backtest and screener use cases
- Backtest output: data/s6_180d_backtest.json + backtest_history table

---

**Bottom line:** Major sprint shipped. Real fleet performance signal in hand for the first time — no longer guessing. Next session has a clear priority stack.
