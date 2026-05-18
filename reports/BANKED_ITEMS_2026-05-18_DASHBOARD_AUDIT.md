# Banked Items — Chrome Dashboard Audit 2026-05-18

## Shipped 2026-05-18 — HM-DASHBOARD-CHROME-AUDIT-FIXES (commit 2c34069)

5 UI integrity fixes from the Chrome audit, shipped inline rather than
banked. Dashboard browser smoke still owed.

- **D1 MOVERS unbounded %** — fetchMovers sanity-caps |pct| ≤ 1000;
  renders "⚠️ N/A" for divide-by-zero artifacts (VIDA +294,999,900%).
- **D2 Backtest BEST EVER caps** — holodeck sweep summary now caps
  WR ≤ 100, |Sharpe| ≤ 10, |Return| ≤ 1000%, MaxDD ≥ -100; out-of-bound
  values render ⚠️-flagged in muted color.
- **D3 Wheel exp dates** — Bridge Counselor Troi render falls through
  expiry_date/strike_price/option_type (the actual /api/wheel/status
  schema) and legacy expiry/strike/type names.
- **D4 Congress undefined amount** — Recent Trades table + Bridge
  embed both guard t.amount_range / c.amount with em-dash fallback.
- **D5 Ready Room negative-delta timestamp** — future-dated briefings
  now render "updates at HH:MM" instead of "-420m ago"; auto-refresh
  trigger skipped for future timestamps.

NOTE: `HM-STARFLEET-MARKET-MOVERS-ZERO-PERCENT` below is a DIFFERENT
movers panel (Starfleet showing 0.00% — not the unbounded % fixed in
D1) and remains banked.

## High-value (single ship fixes multiple symptoms)

### HM-SIGNAL-CENTER-PROXY-NULL-CACHE
Already banked. Affects 5 Signal Center panels.

### HM-CHARTS-STALE-DATA-SOURCE (NEW)
- Charts + Big Charts show SPY $681.41 vs real $737
- Header / Tactical / Sniff Scan all read $737 correctly
- Different data source on the Charts component
- Audit-first: find what feed Charts uses vs what other panels use
- ~$56/share discrepancy = ~8% — affects trading decisions

### HM-BRIDGE-SECTOR-HEATMAP-WIRE (NEW)
- Bridge tab Sector Heatmap: all 12 sectors "undefined ▲0.0%"
- Sector Watch (separate tab) shows live data correctly
- Heatmap widget reading wrong source or unwired
- ~30 min audit + ~15 min fix

## Medium

### HM-BATTLE-STATION-HALT-STATUS-BANNER (NEW)
- T'Pol intentionally halted → Battle Station has no producer
- Currently shows "--" everywhere, user assumes broken
- Add banner: "⏸ T'Pol halted (spread cannibalization 2026-05-06)
  — Battle Station idle. See Models tab to re-enable."
- ~15 min cosmetic improvement

### HM-LIVE-SCANNER-NAVIGATOR-RETIRED-STRING
- Live Scanner shows "Navigator retired" message
- Navigator is the BEST performer (+1.2%, 67% WR, 26 trades)
- Stale hardcoded string from when Navigator was actually retired
- Find string, update or remove
- ~10 min

### HM-FLEET-REPORT-CARD-LOADING-STUCK
- Bridge tab Fleet Report Card stuck on "Loading…" indefinitely
- Other Fleet panels load fine
- Specific to this widget — fetch never resolves
- Unclear scope — audit-first

### HM-AGENT-DEEP-DIVE-ANALYTICS-MISSING
- Win Rate, Avg Win/Loss, Profit Factor all show "—" for Navigator
  (36 trades, should be calculable)
- Either calc never runs or stored P&L not populated
- Bank: audit which trades have closed P&L vs open

### HM-GHOST-SCORECARD-WIN-RATE-ZERO
- All agents show 0.0% win rate across all closed/expired trades
- Scoring logic likely not marking outcomes
- Different from Agent Deep Dive (different surface)

### HM-FEAR-GREED-WIDGET-STANDALONE-UNDEFINED
- Bridge embed shows F&G 76/GREED correctly
- Standalone Fear & Greed page returns undefined on load
- Two code paths, fix to use same source as Bridge embed

### HM-LIVE-CHART-VOL-ZERO-STALE-STREAM
- Live Chart shows MSFT correctly but VOL: 0
- RECONNECT button visible — stream disconnected
- WebSocket health issue

### HM-STARFLEET-MARKET-MOVERS-ZERO-PERCENT
- Gainers/Losers all show 0.00%
- ACTIVE volume column works fine (different data field)
- Price change calc missing or wrong field

## Low-priority polish

### HM-DOM-STRUCTUREFIX-WARNINGS
- 88 console warnings on load: sections rendered outside .main
- JS auto-corrects, no functional impact
- Template-level fix preferred
- Defer indefinitely

### HM-METALS-COMMENTARY-ABORT-ERROR
- AbortError on metals commentary fetch
- Likely rapid section-switch cancellation
- Cosmetic console noise

## Confirmed working (no action)

- Sniff Scan, Sector Watch, Race, Inst Intel, Screener Pro,
  Squeeze, Congress, Models, Dilithium, Leaderboard, Crew
  Activity, Alerts, Ready Room base, Bridge base
- SSE feed, ticker tape, paper trade disclaimers
- 7/7 awareness sources feeding
