# Banked Items — Chrome Dashboard Audit 2026-05-18

## Shipped 2026-05-18 Round 4 — HM-SIGNAL-CENTER-DEAD-ENDPOINTS

Closes the 4 remaining Signal Center keys that Round 3's null-cache fix
exposed as upstream-failing (rather than null-poisoned). Audit-first per
guardrail; each of the 4 had a distinct root cause:

| Endpoint | Root cause | Fix |
|---|---|---|
| `/api/ema-pullback` | Route is on SC itself (port 9000), not trader (port 8080); proxy was 404'ing | Inject from local `_ema_pullback_cache` (no HTTP self-loop) |
| `/api/red-alert/status` | Route never registered anywhere; only `red_alert_check()` function in `engine/volume_scanner.py:320` | Removed from `_SIGNALS_ENDPOINTS`; banked as HM-RED-ALERT-ROUTE-WIRE |
| `/api/dayblade/status` | 200 in 11–19s — exceeds default 5s `_bridge_get` timeout | Per-endpoint timeout override 25s |
| `/api/metals/signals` | 200 in 13s solo, 25–35s under SC concurrent load | Per-endpoint timeout override 40s |
| (also) `/api/risk-radar` | >60s even solo — genuine perf issue | 35s timeout (catches warm cycles); banked as HM-TRADER-RISK-RADAR-SLOW |

Patch summary:
- `_SIGNALS_TIMEOUTS` dict added, threaded into `_bridge_get` call from
  `_fetch_all_signals`. Default still 5s for the fast majority.
- `ema_pullback` key removed from `_SIGNALS_ENDPOINTS`; injection from
  local cache added at end of `_fetch_all_signals`. Fallback to
  `prev_data` for symmetry with the null-cache path. Cache warms on
  first external `/api/ema-pullback` hit (frontend Matrix tab visit
  triggers this automatically; SC startup does NOT pre-warm — a
  Captain-restart will show ema_pullback as MISSING for the first
  cycle until something hits the route externally).
- `red_alert_score` key removed entirely.

Verification (post second SWR cycle, warm trader):

```
$ rtk proxy curl http://127.0.0.1:9000/api/signals/all | …
NULL: []
MISSING: []
served: 35/35
```

Cold-start behavior:
- First cycle after SC restart blocks ~35s on the slowest timeout
  (risk_radar at 35s, parallelism bounded by max_workers=12).
- Frontend `/api/signals/all` returns block-built response on first
  hit, then SWR-cached for 60s, refreshed in background.
- `ema_pullback` will show MISSING (not NULL) until the route is hit
  at least once externally — acceptable since the frontend hits the
  Matrix tab on load.

## Round 4 follow-ups banked

### HM-RED-ALERT-ROUTE-WIRE (NEW)
- `engine/volume_scanner.py::red_alert_check()` exists but has no HTTP
  route. Signal Center's two `_SIGNALS_ENDPOINTS` references to
  `/api/red-alert/status` always 404'd because nothing serves it.
- Two options when the work surfaces:
  1. Build a `/api/red-alert/status` route in `dashboard/app.py` that
     calls `red_alert_check()` and returns its result as JSON.
  2. Retire the `red_alert_check()` function as dead code (verify no
     other consumers first).
- Effort: ~20 min route-build OR ~10 min dead-code retire.

### HM-TRADER-RISK-RADAR-SLOW (NEW — production perf issue)
- `/api/risk-radar` on the trader consistently takes >60s even when
  isolated (no concurrent load). 35s timeout in SC catches warm
  cycles but not all.
- Likely culprit: per-player risk recompute that iterates every
  active player's positions and refetches prices. Sniff probable
  N+1 query or unbatched yfinance/Polygon fanout.
- Captain-visible impact: Bridge `risk_radar` panel can briefly
  show MISSING during cold-start cascade.
- Investigation banked: profile `dashboard/app.py::risk_radar` (or
  wherever the route lives), check for batching opportunities, add
  per-player result caching with a short TTL.
- Effort: ~1–2 hour audit + ~1 hour optimization.

### HM-TRADER-COLD-START-CASCADE (NEW)
- Pattern observed during Round 4 verification: when SC restarts and
  fires its first concurrent fanout (12 parallel `_bridge_get` calls)
  against the trader, otherwise-fast endpoints like `/api/regime`
  (0.01s solo) timed out during the cold cascade. Steady-state is
  fine but the first 35–60s after SC restart shows transient gaps.
- Mitigations to consider:
  1. Reduce SC `max_workers` from 12 to 6 to lighten concurrent
     trader pressure.
  2. Stagger the first cycle's fetches over ~10s instead of
     simultaneous fanout.
  3. Add a startup warmup that hits each endpoint sequentially.
- Low priority — only affects the ~30s post-restart window.

## Shipped 2026-05-18 Round 3 — high-value bank sweep

5 items from the banked list, audit-first per item per guardrail.

### Item 1 — HM-SIGNAL-CENTER-PROXY-NULL-CACHE — SHIPPED

`signal-center/server.py::_fetch_all_signals` was writing `None` into the
SWR cache whenever a per-endpoint fetch failed (either via `except` OR via
`_bridge_get` returning `None` for non-200 / redirect-no-session — the
banked description only captured the `except` path, the actual code also
silently null-cached non-200s). 6 cache keys observed NULL pre-fix on a
live `/api/signals/all` probe: `bull_bear`, `dayblade`, `ema_pullback`,
`metals`, `red_alert_score`, `risk_radar`.

Fix: pass the prior cache snapshot into each refresh cycle as
`prev_data`. On any failed fetch (exception OR None return), prefer
`prev_data.get(key)` over writing None. If no last-good exists, omit
the key entirely (frontend treats absence as cache-miss → degrade
gracefully, NOT render `null`). History INSERTs are gated on a
`fresh_keys` set so last-good fallbacks don't duplicate rows.

Type-annotation gotcha banked: Python 3.9 doesn't support PEP 604 union
syntax (`dict | None`). Crash-looped on first restart. Re-shipped with
plain `prev_data=None`. **Lesson:** Signal Center runs on system Python
3.9 (`/Library/Developer/CommandLineTools/.../Python.framework/3.9/`),
NOT the trader venv. Future patches there: `Optional[X]` or string
annotations, never `X | None`.

Post-fix verification on live `/api/signals/all`:
- NULL keys: 6 → 0 (poisoning bug fixed)
- MISSING keys: `['dayblade', 'ema_pullback', 'metals', 'red_alert_score']`
  — these 4 endpoints fail upstream every cycle. Fresh process had no
  last-good to fall back on. Two are HTTP 404 from the trader
  (`/api/ema-pullback`, `/api/red-alert/status` — dead endpoint configs);
  two return 200 to anon curl but None to `_bridge_get` (session/auth
  issue). Banked separately as **HM-SIGNAL-CENTER-DEAD-ENDPOINTS**
  (see below).

Service restarted via `launchctl kickstart -k gui/$(id -u)/com.trademinds.signal-center`.

### Item 2 — Bridge CONSENSUS panel SPLIT — BANKED (case a)

Audit-first per guardrail; case (a) confirmed (producers idle), so
banked as **HM-UHURA-DATA-SIGNAL-PRODUCERS-IDLE** (below) instead of
fixed inline.

Live `/api/consensus` probe: 515 tickers in universe; only 12 have any
officer stance (all spock-only). 0 tickers covered by Data or Uhura.
Root cause: Spock briefing names ~12 tickers max (`engine/consensus.py::
_get_spock_stance` parses tickers mentioned in CTO briefing text); Data
First-Officer `_briefing_cache` empty + mlx-qwen3 24h-fallback returns
zero rows; Uhura ollama-llama war_room posts empty for 24h. Not a
frontend bug — frontend correctly shows `comparison: no_data` and lacks
direction.

### Item 3 — Bridge METALS $2,311 gap — SHIPPED

Audit found the underlying data inconsistency:
- Bridge header `g-metals-text` reads `/api/metals/portfolio` →
  reads from `engine.metals_tracker.get_dilithium_portfolio()` →
  sources from `metals_ledger` SQL table (truth: 65 oz silver,
  6 purchases summing correctly, 1 oz gold).
- Bridge `metals-panel` detail (the `renderMetals` block at
  `index.html:34747`) reads `/api/metals/exposure` → sourced from
  `data/metals.json` (stale: silver 35 oz, `last_updated: 2026-04-21`,
  missing the 2026-05-04 20 oz purchase and earlier ledger deltas).

User's audit prompt assumption "detail is correct" was backwards.
Detail panel was the stale one.

Fix: `metals_exposure()` in `dashboard/app.py:13726` now sources
physical holdings from `metals_ledger` (single source of truth). Falls
back to `data/metals.json` only if the SQL query fails (edge case for
fresh installs). Both Bridge surfaces will now show the same silver oz
count and dollar value after trader restart.

**Trader restart owed** for this fix to take effect (`~/autonomous-trader/restart.sh`).

### Item 4 — Tomorrow's Game Plan unavailable — defensive UI fix shipped

Audit-first per guardrail; case (b) confirmed (cron timing).

- `/api/morning-brief` returns yesterday's brief (2026-05-17, has
  headline + BULL_CROSS regime) — used by auto-load on page visit.
  Works.
- `/api/morning-brief/run` returns today's brief — currently
  `{game_plan: {}, unavailable: true, message: "Daily Intel
  unavailable — POST /api/morning-brief/force-run to generate"}`.
  Today's brief hasn't generated yet (cron schedule: 6 AM & 8 PM AZ).
  Returns 200 with empty `game_plan`, which is what triggered the
  user-visible "Intel report unavailable" — the frontend `.catch()`
  block AND a follow-on render with empty `gp.headline` both surface
  bad UI states.

Two small frontend defensive fixes inside `loadGamePlan`:
1. When `gp.headline` is missing AND the server provided a `message`
   field (the new "unavailable" shape), surface the server's message
   instead of the generic "No intel report yet" text.
2. Both the empty-state path AND the `.catch()` block now hide the
   regime badge — so a stale BULL_CROSS tag from a prior successful
   load doesn't sit next to an "unavailable" body.
3. `.catch()` message now reads "Intel report unavailable — retry
   shortly" to signal it's a transient state, not a hard failure.

No backend fix this round — generator-not-run-today is cron-cadence,
not a bug.

### Item 5 — Wheel count mismatch 9 vs 6 — SHIPPED

`/api/wheel/status` returns `puts_open: 9` and `positions` array of
length 9 (verified live). The Bridge Wheel renderer at
`index.html:27366` had a hardcoded `.slice(0, 6)` that truncated the
list rendered in the expand panel — header count was right, list was
short.

Fix: bumped slice to `.slice(0, 20)` so all 9 currently-open puts
render (and any future growth up to 20 contracts also renders). Header
count and list now agree.

---

## Round 3 follow-ups banked

### HM-SIGNAL-CENTER-DEAD-ENDPOINTS (NEW)
Four `_SIGNALS_ENDPOINTS` entries fail every fetch cycle on a fresh
process, leaving their keys missing from the cache instead of NULL
post-fix. Need separate cleanup:
- `/api/ema-pullback` → HTTP 404 on the trader; either fix the path
  or remove the key from `_SIGNALS_ENDPOINTS` (the EMA pullback
  scanner section in the dashboard reads from a different endpoint).
- `/api/red-alert/status` → HTTP 404; same — verify the actual red
  alert endpoint and update the config, or retire the key.
- `/api/dayblade/status` → returns 200 to anon curl but None via
  `_bridge_get` session — likely auth/session issue (the endpoint
  may require admin scope). Either widen the session permission or
  use a publicly-readable endpoint.
- `/api/metals/signals` → same as dayblade (200 vs None via session).
- Effort: ~30 min to audit + fix configs. Low-risk after.

### HM-UHURA-DATA-SIGNAL-PRODUCERS-IDLE (NEW, multi-component epic)
Bridge Consensus shows ⚠️ SPLIT for 503/515 tickers because the
producer side is empty:
- **Data (First Officer)**: `engine.first_officer._briefing_cache`
  is empty; mlx-qwen3 fallback returns 0 rows (filtered by
  `HALTED_EMIT_FILTER`). Either revive the briefing producer or
  unblock the mlx-qwen3 stream.
- **Uhura (ollama-llama)**: war_room posts empty for last 24h.
  ollama-llama is in the documented zombie set (`halt_mode='full'`,
  HM-AK 2026-05-07 cleanup) — the consensus aggregator queries a
  retired producer. Either swap to a live producer or remove Uhura
  from the consensus panel.
- **Spock**: only 12 tickers covered (Spock briefing names a
  curated subset). Decision needed: is this by-design (Spock opines
  on top names only) or should the briefing scan a wider universe?
- This is producer/infra, not frontend. Bank for a dedicated session.

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

### HM-BACKTEST-DB-WR-DATA-CORRUPTION (NEW — surfaced Round 2.1 D5)
- `/api/backtest/history` returns BEST EVER rows that are not just
  display-glitches but actual data corruption in the source table:
    - `best.win_rate.value=10000.0` → agent **Bollinger** (db-8),
      date 2026-04-10
    - `best.return.value=12366.0` → agent **RSI**, date 2026-04-10
    - `best.sharpe.value=43.017` → agent **Chekov**, date 2026-05-15
- Pattern: same 2026-04-10 date on the RSI+Bollinger rows suggests a
  bad sweep run that day; Chekov sharpe blip is a different date so
  may be a separate corruption event.
- Round 2.1 frontend caps these to ⚠️-flagged so they don't render
  as raw 10000.0%, but the underlying rows are still in the DB and
  will keep surfacing as the "best ever" pick on any sort.
- Cleanup approach (when banked work surfaces):
  1. Find the backing table (likely `backtest_history` or similar)
     and query for `win_rate > 100 OR ABS(sharpe) > 10 OR
     ABS(return_pct) > 1000`.
  2. Confirm with Captain whether the corrupted rows should be
     deleted, marked with a `data_quality_flag`, or excluded from
     the `best` aggregation at the API layer.
  3. Per sacred-data rule: don't `DELETE`; either archive to
     `backtest_history_quarantine` or add a flag column and filter
     in the BEST EVER SQL.
- Effort: ~30 min SQL audit + decision; ~15 min API fix.
- Until shipped, frontend ⚠️ flags are the user-facing protection.

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
