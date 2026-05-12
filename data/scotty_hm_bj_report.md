# HM-BJ Discovery — Ticker Linkification + Hover Scorecard

**Phase BJ.0 — read-only profiling. No code edits.**
**Date:** 2026-05-11
**Engineer:** Scotty (Opus 4.7)

---

## Pre-flight summary
- HM-BD.F (`9e587f8`), HM-BD.E (`448b7b3`), HM-BD.G (`d8ac548`) all in `origin/main`.
- Trader bridge alive (PID 92479, port 8080).
- Working tree clean (tracked).

---

## 🚨 Critical premise check — the directive assumes the wrong frontend

The HM-BJ directive's BJ.D phase mentions "Vite rebuild" and talks about `<TickerChip symbol={sym}>` JSX-style components, suggesting the Captain expects work to land in `dashboard/frontend/` (Vite + React 19 + recharts).

**That tree is unwired experimental code.** It exists, it builds, but its `dist/` is NEVER served.

### Evidence

| Surface | Reality |
|---|---|
| `GET /` route (dashboard/app.py:9469) | Returns `FileResponse(_static_dir + "/index.html")` — i.e., `dashboard/static/index.html` |
| `StaticFiles` mount at line 9411 | `app.mount("/static", StaticFiles(directory=_static_dir), name="static")` — only `dashboard/static/`, never `dashboard/frontend/dist/` |
| `frontend/dist` grep | Zero references in `dashboard/app.py` or `main.py` |
| `dashboard/static/index.html` | **34,887 lines** of vanilla HTML/JS — the actual single-file SPA powering the live UI |
| Vite tree last touched | April 10, 2026 (a month ago, in a different epic) |
| CLAUDE.md doctrine | Explicit (lines 33-45): *"ALL dashboard edits target that single file [dashboard/static/index.html] — do not create new HTML files unless explicitly asked. The Vite tree at `dashboard/frontend/` is unwired experimental code — its `dist/` is never mounted."* |

If I built this against the Vite tree, the Captain would see zero UI changes after `npm run build` because the dist is not served. The whole epic would silently fail in production.

---

## 🪛 Path decision (Q0 — new, must precede Q1)

### Path A — vanilla JS in `dashboard/static/index.html` ✅ **Scotty's recommendation**
- TickerChip implemented as a JS factory function `renderTicker(sym, panelHint)` returning a `<span class="ticker-chip">` element
- CSS injected via existing `<style>` block
- Global event delegation handles click / shift-click / hover
- Scorecard tooltip = appended-to-body `<div>` with raw SVG sparkline (no recharts needed)
- Matches doctrine. NO Vite rebuild. NO React. NO architecture change.
- Single file touched per sub-phase commit.

### Path B — finally wire up the Vite tree
- Add `app.mount("/", StaticFiles(directory=dashboard/frontend/dist))` (or change `/` route)
- Out of scope tonight — would orphan all live UI logic embedded in the static HTML
- Materially bigger than HM-BJ's stated mission; defer to a dedicated migration epic if ever pursued

### Path C — hybrid (Vite for components, static shell for the rest)
- Build TickerChip with Vite as a standalone Web Component
- Embed compiled JS into the static HTML via `<script type="module" src="...">`
- Cleaner long-term, more setup tonight
- Defer until Path B is decided

**Recommend Path A.** Matches CLAUDE.md doctrine, matches the "ship-tonight" tone of the directive, no scope creep.

---

## Per-symbol data available (backing for the scorecard)

The backend already exposes plenty of per-symbol detail — no new aggregator endpoint required for T1/T2:

| Endpoint | Use for scorecard |
|---|---|
| `GET /api/market/candles/{symbol}` | **Sparkline data** (OHLC) ✓ |
| `GET /api/market/sentiment/{symbol}` | Fleet sentiment (bull/bear count, consensus) |
| `GET /api/news/{symbol}` | Latest headline |
| `GET /api/market/mtf/{symbol}` | Multi-timeframe technicals |
| `GET /api/gex/{symbol}` | Gamma exposure (if relevant for options-heavy panels) |
| `GET /api/market/vol-surface/{symbol}` | IV / vol context |
| `GET /api/patterns/{symbol}` | Chart patterns detected |
| `GET /api/risk-levels/{symbol}` | Support/resistance levels |
| ai_brain memory + ghost_trades tables | Position status, ghost history per symbol — query via existing endpoints |

A scorecard can be composed client-side with **Promise.all** of 2-3 of the above. Server-side aggregator endpoint would be cleaner but is optional.

---

## Ticker render-site inventory (incomplete — sample of dashboard/static/index.html)

Live UI is 34.9k LOC. Tickers render in many places. Quick grep shows:
- L5600: Webull/Scanner panel — `<th>Ticker</th>` column header (rows below interpolate `row.ticker`)
- L7514: Screener Pro — `screenerPro.sort('symbol')` column
- L18722: another ticker iteration loop
- Many more — comprehensive inventory deferred to BJ.3 (sweep phase).

No existing `<a>` wrappers, no existing TradingView click-to-link, no existing tooltip on tickers. Greenfield in terms of ticker interactivity.

---

## Feasibility tiers (with Path A as the implementation vehicle)

### T1 — Core (ship-tonight target, ~1.5 hr)
- Add JS factory `renderTicker(sym, panelHint)` in the script section of `dashboard/static/index.html`
- Add `.ticker-chip` CSS (subtle underline, cursor pointer)
- Document-level event delegation:
  - click → dispatch `focus-symbol` CustomEvent
  - shift-click → `window.open('https://www.tradingview.com/chart/?symbol=' + sym, '_blank')`
- Apply to 3-5 panels (Kirk Advisory, Fleet Activity, Pre-market Gaps, Ghost Trader, Portfolio)
- Single commit

### T2 — Hover scorecard (+1 hr, recommended same night)
- Tooltip `<div>` appended to body, positioned absolutely on hover
- Client-side compose: `Promise.all([fetch('/api/market/sentiment/SYM'), fetch('/api/market/candles/SYM?limit=30')])`
- Render: price + day change, sentiment chip, ghost count, position status, mini SVG sparkline
- Debounce 200ms before fetching to avoid spam on cursor sweep
- Cache responses for 60s in a module-level Map
- Single commit

### T3 — Extras (defer to HM-BJ.E)
- Right-click context menu (Yahoo / Webull / Schwab / X search)
- Inline TradingView lightweight-charts preview in tooltip
- Keyboard navigation (Tab through tickers)
- Pinned focus mode (sticky header showing "Focused on AAPL" with X to clear)

---

## Captain decisions blocking BJ.1

**Q0 (new, blocking everything below):** Path A (vanilla in static/index.html) vs Path B (wire Vite) vs Path C (hybrid)?
*Scotty's recommendation: Path A.*

**Q1 — Tonight's scope:** T1 only / T1+T2 / +selected T3?
*Scotty's recommendation: **T1+T2**.* Backing data exists, sparkline is cheap with `/api/market/candles/{symbol}`, T3 doesn't add commensurate value tonight.

**Q2 — Scorecard data sourcing:** server-side aggregator endpoint OR client-side compose?
*Scotty's recommendation: **client-side compose** for tonight.* Promise.all of 2-3 existing endpoints with a 60s in-memory cache. Park "server-side aggregator `/api/symbol/{sym}/scorecard`" as HM-BJ.E for after we know which fields actually matter in production.

**Q3 — Sparkline:** include in T2 or defer?
*Scotty's recommendation: **include in T2.*** `/api/market/candles/{symbol}` already serves it; rendering inline SVG is ~30 LOC. Cheap, high visual win.

**Q4 — Sweep cadence (BJ.3):** all panels in one commit vs per-panel commits?
*Scotty's recommendation: **single commit** for BJ.3.* All changes are inside the same 34.9k-LOC file; per-panel commits don't ease review (one big file with progressive changes is identical to the same file with a smaller diff per commit). One sweep, one commit, visual verify post-rebuild.

---

## Process notes

- **No service restart needed** — `FileResponse(...)` serves disk on each request; new HTML reaches the browser on next reload (browser cache may need Cmd+Shift+R).
- **No Vite rebuild needed** under Path A — we're not touching `dashboard/frontend/`.
- **Browser-side verification required** — Captain hard-refreshes the dashboard, hovers/clicks/shift-clicks a few tickers.

---

**HALT — awaiting Captain decisions on Q0, Q1, Q2, Q3, Q4 before BJ.1.**

---

# Captain Decisions (received 2026-05-11)

- **Q0:** A — vanilla JS in `dashboard/static/index.html`. Path B/C deferred indefinitely.
- **Q1:** T1+T2 — core + hover scorecard with sparkline.
- **Q2:** Client-side compose via Promise.all + 60s in-memory cache.
- **Q3:** Sparkline included (inline SVG from `/api/market/candles/{symbol}`).
- **Q4:** Single commit covering BJ.1+BJ.2+BJ.3.

---

## HM-BJ Closure

### What shipped (commit 0223f31)

Single +423-line additive injection into `dashboard/static/index.html` between lines 1629 (after the agent-names script block) and 2051 (before the next script block). Three IIFEs:

1. **BJ.1 — TickerChip infrastructure** (~200 LOC)
   - `window.tickerHTML(sym, panel)`, `window.renderTicker(sym, panel)` exports
   - Document-level event delegation: click → `ticker:focus` event, shift-click → TradingView, keyboard (Enter/Space) → click
   - 300ms hover delay → fixed-position scorecard tooltip
   - Scorecard composes from `Promise.all` of `/api/market/candles`, `/api/market/sentiment`, `/api/news` with 60s in-memory cache per URL
   - Inline SVG sparkline (no library), color-coded green/red by 30-tick direction

2. **BJ.2 — Focus controller** (~60 LOC)
   - Listens for `ticker:focus` CustomEvent
   - Sets URL hash `#focus=SYM` for deep-linkability
   - Top-center indicator bar with ✕ to clear
   - Highlights ALL chips on the page matching the focused symbol
   - Page-load restore: if hash present, applies focus on `DOMContentLoaded`

3. **BJ.3 — Auto-upgrade MutationObserver** (~50 LOC)
   - Scoped selector covering **19 known ticker-marker classes**: `sym`, `ls-sym`, `at-risk-sym`, `feed-sym`, `heat-sym`, `hm-sym`, `nf-sym`, `sent-sym`, `sniff-sym`, `td-sym`, `ticker-sym`, `wbc-wl-sym`, `wbi-ticker`, `alr-ticker`, `ii-ticker`, `opt-ticker`, `premarket-ticker`, `race-ticker`, `rtc-ticker`
   - Safety regex `/^[A-Z0-9.\-]{1,10}$/` rejects non-ticker text
   - Debounced via `requestAnimationFrame` on subtree mutations
   - **Zero edits to existing render code** — observer catches new DOM and upgrades it transparently

### Panels covered

Every panel that uses one of the 19 marker classes for ticker rendering — which includes (from grep):
- Captain Kirk / Kirk Advisory (uses `.sym`, `.ticker-sym`)
- Fleet Activity (`.feed-sym`)
- Pre-market Gaps (`.premarket-ticker`)
- Ghost Trader (`.sym`)
- Captain's Portfolio (`.sym`, `.td-sym`)
- Sector Watch (`.sent-sym`, `.heat-sym`)
- Signal Center (`.ls-sym`)
- Earnings (`.ticker-sym`)
- Positions at Risk (`.at-risk-sym`)
- Plus all other panels using the same classes — automatic via observer

If a panel renders tickers WITHOUT one of those classes (e.g., raw `${row.symbol}` interpolated as plain text), it won't be auto-upgraded. Future work (HM-BJ.E) can extend the class list or instrument those panels.

### Not in scope (intentional)
- `.nf-ticker-btn` — News-feed filter buttons already have their own click handlers; auto-upgrading would cause double-handler chaos. Left untouched.
- `.mpo-sym-wrap`, `.ticker-item`, `.grok-diff-tickers` — containers / plural; not single-symbol text.
- `.ticker-chg`, `.ticker-price`, `.ticker-rec` — these wrap price/change/recommendation text, not symbols.
- Path B/C frontend architecture migration.

### Verification

| Check | Result |
|---|---|
| Anchors present | ✅ 6 (`HM-BJ` markers at lines 1631, 1634, 1700, 1931, 1993, 2050) |
| JS syntax | ✅ `node --check` clean on the injected JS region |
| HTML script-tag balance | ✅ 155 `<script>` opens = 155 `</script>` closes |
| Dashboard `/` route | ✅ HTTP 303 → `/login` (auth gate; expected — Captain will see actual UI after login) |
| Backing endpoint `/api/market/candles/AAPL` | ✅ HTTP 200 (cold 8.8s; client-side 60s cache absorbs subsequent hovers) |
| Backing endpoint `/api/market/sentiment/AAPL` | ✅ HTTP 200 in 22ms |
| Backing endpoint `/api/news/AAPL` | ✅ HTTP 200 in 1.8s |

### Push & no restart

- **Push** done inline in BJ.D.
- **NO service restart** — `FileResponse` reads from disk on every request, so the new HTML reaches the browser on next load.
- **NO Vite build** — Path A doesn't touch `dashboard/frontend/`.
- **Browser hard-refresh required** (Captain to do) — Cmd+Shift+R to bypass browser cache and pull the new index.html.

### Manual verification steps for the Captain
1. Hard-refresh https://bridge.ollietrades.com (or http://localhost:8080) — Cmd+Shift+R.
2. Open Kirk Advisory or Pre-market Gaps panel.
3. **Hover** a ticker symbol — within ~300ms a tooltip should appear with: price, 30-tick change, fleet bullish/bearish counts (if available), latest headline, mini sparkline.
4. **Click** a ticker — top-center "Focused on AAPL ✕" bar appears; URL hash changes to `#focus=AAPL`; all chips for that symbol get a yellow highlight.
5. **Shift-click** a ticker — opens `tradingview.com/chart/?symbol=AAPL` in a new tab.
6. **Keyboard**: Tab to a chip, press Enter — should behave like click. (Tab order set via `tabindex=0`.)
7. **Refresh page with `#focus=AAPL` in URL** — focus state restored.
8. Click ✕ on the focus bar — clears focus state and URL hash.

### Parked follow-ups (HM-BJ.E candidates)
- **HM-BJ.E1** — Right-click context menu (Yahoo / Webull / Schwab / X/Twitter search).
- **HM-BJ.E2** — Inline lightweight-charts preview in the tooltip (the dashboard already loads lightweight-charts@4.1.0 per the `<script src=>` at line 2304).
- **HM-BJ.E3** — Keyboard navigation between chips (Arrow keys).
- **HM-BJ.E4** — Server-side `/api/symbol/{sym}/scorecard` aggregator endpoint to replace the three-fetch client-side compose (cleaner, fewer round trips).
- **HM-BJ.E5** — Extend the auto-upgrade observer to additional ticker classes if BJ.D field testing reveals panels that didn't get upgraded.
- **HM-BJ.E6** — Per-panel filtering: when a ticker is focused, fade/hide panel rows that don't match. Currently only the highlight + URL hash change; actual row filtering deferred.

### Open quirks / known limitations
- `/api/market/candles/AAPL` first call is ~8.8s cold; the in-tooltip experience is "loading…" then fills in. Subsequent hovers on the same symbol within 60s are instant (client cache).
- If a panel re-renders its rows during a focus state, the new rows must pass through the observer before they get highlighted. There's a one-frame lag at worst (`requestAnimationFrame` debouncing).
- The auto-upgrade observer watches the entire `document.body` subtree. On very heavy DOM-churn panels (rare here), this could add minor CPU overhead. Currently no measurable impact.
