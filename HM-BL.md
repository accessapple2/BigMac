# HM-BL — Earnings Catalyst Scanner

## Why
PSIX miss (EPS $0.36 vs $0.74 = –51%, revenue $128.6M vs $160.8M = –20%, FY guide withdrawn) was a textbook bear_put_spread setup. Fleet had no machinery to see it. Build a catalyst detector that scores earnings events and feeds spread strategies in ghost mode first.

## Goal
Detect earnings-driven collapse/squeeze setups within 15 min of release, score them, and route qualifying signals to fleet agents via signal_center.

## Acceptance Gates
1. New tables in trader.db:
   - `earnings_catalysts` — symbol, earnings_dt, eps_actual, eps_consensus, rev_actual, rev_consensus, eps_surprise_pct, rev_surprise_pct, guidance_action {raised|maintained|cut|withdrawn}, premarket_gap_pct, score, side {bull|bear}, detected_at
   - `ghost_earnings_watch` — mirrors ghost_options_watch pattern for paper tracking
2. Trigger thresholds:
   - BEAR: EPS miss ≥ 25% OR revenue miss ≥ 15% OR guidance pulled/cut
   - BULL: EPS beat ≥ 25% AND revenue beat AND guidance raised
3. Poller runs every 15 min, focused windows: pre-market 04:00–09:30 ET, post-close 16:00–20:00 ET
4. Qualifying signals fire into signal_center as signal_type='earnings_catalyst' with side + confidence
5. bear_put_spread_v1 / bull_call_spread_v1 accept new signal type
6. GHOST MODE FIRST — no live execution until 10 ghost trades clear (mirrors HM-AB gate discipline)
7. Dashboard `/catalysts` route shows day's detections + ghost P&L

## Data Source
Polygon `/vX/reference/financials` — actuals + consensus
Polygon news endpoint — guidance language parsing
Polygon aggregates — premarket gap (04:00–09:30 ET)

## Sequence
1. Schema: earnings_catalysts + ghost_earnings_watch
2. Build `scrapers/earnings_scanner.py`
3. Guidance language parser — regex + keywords: "withdrew", "no longer providing", "suspends", "reaffirms", "raises"
4. Score function — weight EPS miss, revenue miss, guidance action, premarket gap
5. Wire into signal_center as signal_type='earnings_catalyst'
6. Ghost mode wiring — mirror ghost_options_watch (first closed trade INTC 0DTE pattern)
7. launchd plist `com.ollietrades.earnings-scanner` — 15min cadence, focused windows, lifecycle-bound
8. Dashboard /catalysts route + browser smoke test
9. Self-verify, paste closure

## Dependencies
HM-BK helpful (mover tier ingests catalysts naturally) but not blocking — can ship independently.

## Out of Scope
- Live execution before 10 ghost trades validated
- Earnings whisper number tracking (phase 2)
- After-hours options (liquidity poor)
- 8-K event detection beyond earnings (phase 2)
