# Banked Items — Chrome Dashboard Audit 2026-05-18

## Shipped 2026-05-18 Round 2.1 — follow-up to Round 2 smoke misses

Chrome smoke of Round 2 (dfb5381) caught two render-path misses. Same
class of bug both times: I patched one renderer for each symptom but
each symptom had a second renderer reading the same upstream data with
the same wrong assumption. Re-patches:

- **D1 (Bridge Sectors) — second renderer fixed.** Round 2 caught the
  small `g-sectors-inner` glance-row text panel; the visual heatmap
  grid below it is driven by a separate `loadBridgeHeatmap()` function
  at `dashboard/static/index.html:5089` which still read `s.name` /
  `s.pct_change` / `s.ticker`. Cells rendered "undefined ▲ 0.00%" and
  `showSectorDetail` opened "undefined (XLV) — Top 10 Holdings".
  Rewired to `s.sector` / `s.change_pct` / `s.etf` with the legacy
  field names kept as fallbacks. Sector tip dict + holdings popup
  follow through correctly now.
- **D5 (Backtest BEST EVER WR 10000%) — wrong file fixed last round.**
  My Round 2 cap targeted `fetchStrategyLab` in `index.html`; the
  🏆 BEST EVER banner Captain saw lives in
  `dashboard/static/backtest_arena.html` (the separate `/backtest`
  page), reading `d.best.win_rate.value` from `/api/backtest/history`.
  API currently returns `best.win_rate.value=10000` (Bollinger),
  `best.return.value=12366` (RSI), `best.sharpe.value=43.02` (Chekov)
  — all corrupted sweep rows. Added `_capWR` / `_capSh` / `_capRet`
  helpers to backtest_arena.html and applied them to both the BEST
  EVER bar AND the per-row history table cells (so a Bollinger row
  sorted to the top doesn't also render 10000.0%). Out-of-bound
  values render with ⚠️ and muted opacity.

Lesson banked for future audits: when a symptom names a specific
panel, grep for ALL renderers that hit the same data, not just the
first one. The "Sector Watch standalone is fine, only Bridge is
broken" framing in the audit prompt was a hint there were two
renderers; I should have also asked "what about the visual heatmap
grid on the same Bridge tab?"

Browser smoke owed: revisit Bridge tab sector heatmap grid (cells
should show sector name + ETF ticker + signed %), `/backtest` page
🏆 BEST EVER bar (should show ⚠️-flagged values until corrupted DB
rows are cleaned up).

## Shipped 2026-05-18 Round 2 — HM-DASHBOARD-CHROME-AUDIT-FIXES-ROUND-2

5 quick wins per Chrome audit second pass (~14:50 ET). Big news first:
Wheel exp dates VERIFIED FIXED (2026-06-16 / 06-17 rendering clean) —
earlier "undefined" report caught DOM mid-fetch. UI-only, single bundled
commit, backend untouched.

- **D1 Bridge SECTORS — undefined labels** — heatmap renderer was reading
  `s.name` / `s.pct_change`; `/api/sectors/heatmap` returns `s.sector` /
  `s.change_pct` with top-level `spy_change_pct`. Renamed fields, added
  em-dash fallback for empty leader/laggard sets, sorted by change_pct
  before slicing leaders/laggards.
- **D2 Bridge CONGRESS dates — undefined** — mini-panel was reading
  `t.member` / `t.amount`; `/api/congress/trades` returns `politician` /
  `amount_range` / `transaction_date`. Realigned to API schema (matches
  Round 1's Recent Trades full-table pattern) and added the transaction
  date column the audit asked for.
- **D3 Bridge VIX — "N/A"** — repointed from `/api/economy` (no vix
  field) to `/api/market/vix` (returns `current.vix` + regime), the same
  source the header VIX cell and Live Chart already use. Inner expand
  also surfaces regime/state badge.
- **D4 Screener Pro init error** — `registerSectionInit('screener-pro',
  'sector-watch', 'sector-watch', function(){…})` was passing the string
  `'sector-watch'` as `initFn`, triggering "TypeError: initFn is not a
  function" inside the LazyInit flush and silently dropping the Scan
  button wiring. Restored to `registerSectionInit('screener-pro',
  function() { screenerPro.run(); })`.
- **D5 Backtest BEST EVER WR 10000.0%** — Strategy Lab ("Backtest &
  Optimize" card) `fetchStrategyLab` was doing `s.win_rate * 100` while
  the API returns the value already in percent form, inflating 100 →
  10000. Added local `_capWRPct` helper that auto-detects decimal-vs-pct
  by magnitude (≤1 → ×100, else as-is), caps at 100, and flags with ⚠️
  in muted color when out of bounds. Holodeck sweep summary already had
  the cap pattern from Round 1; this surface needed its own copy.

Browser smoke owed per Frontend Ship Rule at 127.0.0.1:8080:
Bridge tab (Sectors row + Congress row expanded + VIX cell), Screener
Pro (Scan button fires), Backtest page (Strategy Lab cards render
plausible WR or ⚠️-flagged).

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

### ~~HM-BRIDGE-SECTOR-HEATMAP-WIRE~~ — SHIPPED Round 2 D1
- Bridge tab Sector Heatmap: was rendering "undefined ▲0.0%" × 12
- Root cause: field-name drift (`s.name`/`s.pct_change` → `s.sector`/
  `s.change_pct`). Sector Watch standalone reads the same endpoint
  with the right keys, which is why it kept working.
- Fixed in Round 2 — see Shipped section above.

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
