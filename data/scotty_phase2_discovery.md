# Phase 2 Discovery — 2026-05-10 (Scotty / Race tile)

**Log:** `/tmp/scotty_phase2_discovery_20260510_1901.log`
**Session:** Saturday late-evening, market closed (live smoke-test data reflects this).

## Phase 1 baseline confirmed

- `engine/momentum/bridge.py` + `engine/momentum/__init__.py` present
- `docs/HM_AN_BRIDGE.md` shipped
- `/api/momentum/heartbeat` + `/api/momentum/recent_signals` both **200**
- 3 `/api/momentum` refs in `dashboard/app.py`

## Frontend stack — REALITY vs. directive

This is the single most consequential finding. **The directive's UI architecture does not match what's actually being served.**

### What's live (verified via curl + file inspection)

| Path | Size | Last edit | Status |
|------|------|-----------|--------|
| `dashboard/static/index.html` | **1,909,862 B (1.9 MB)** | **May 8, 2026** | **LIVE — this is the dashboard** |
| `dashboard/static/css/lcars.css` | ~1700 lines | active | LCARS theme is here |
| `dashboard/static/js/lcars.js` | active | active | LCARS interactivity |
| `dashboard/frontend/dist/index.html` | **597 B** | **April 25, 2026** | **STALE, NOT MOUNTED** |
| `dashboard/frontend/src/` | 30+ `.jsx` components | various | Experimental, unmounted |

### How dashboard/app.py mounts content
- Only static mount: `app.mount("/static", StaticFiles(directory=_static_dir))` — that's it
- **No mount of `dashboard/frontend/dist/`**
- `/` → 303 → `/login` (auth wall); after login serves `dashboard/static/index.html`

### Nav pattern in the live UI (vanilla JS, not React)
- Function `mobileNav(section, el)` at `dashboard/static/index.html:20322`
- Each "tab" = `<div id="section-NAME" style="display:none;">` (verified: `section-dashboard`, `section-trades`, `section-charts` at lines 3751, 6005, 6037)
- Nav buttons: `<button class="mobile-nav-item" onclick="mobileNav('NAME',this)">…</button>` (verified at lines 12795–12802)

### Confirms CLAUDE.md exactly
> "Dashboard is served from `dashboard/static/index.html` on port 8080. The Vite tree at `dashboard/frontend/` is unwired experimental code — its `dist/` is never mounted." — `CLAUDE.md` (and memory `feedback_port8080_html.md`, `feedback_correct_index_html.md`)

**Implication:** if Race is built in `dashboard/frontend/src/components/Race.tsx` per the directive, `npm run build` will succeed but the user will see no Race tab. Need Admiral resolution — see Blockers below.

## Universe sources — already-built infrastructure found

This phase's 2.1 build can be **mostly skipped** — the universe accessor already exists:

| File | Purpose | Status |
|------|---------|--------|
| `engine/universe.py` | `get_active_universe()` reads filtered `scan_universe` table | ✅ active, used by fleet |
| `engine/universe_refresh.py` | Weekly Polygon-driven refresh (Sundays 14:00 MST) | ✅ |
| `engine/full_universe.py` | Pulls ALL tradeable from Alpaca /v2/assets | ✅ |
| `data/trader.db.scan_universe` | **2,949 rows** raw; filtered to ~600-900 | ✅ |
| `docs/UNIVERSE.md` | Captain decision rationale | ✅ |

Filter criteria already in place:
- `market_cap >= $5B` (CS rows)
- `avg_volume * avg_price >= $100M` (dollar volume)
- `last_updated within 14 days`
- ETFs included on dollar-volume parity; ETNs excluded
- Fallback to 20-name mega-cap list if `scan_universe` empty

Schema: column is `symbol`, not `ticker` (the directive's CSV pattern would have used `ticker`).

## Alpaca client — no `get_snapshots`, but bulk price IS wired

| Path | Function | Status |
|------|----------|--------|
| `engine/alpaca_bridge.py` | `AlpacaBridge.latest_prices(symbols)` — uses `alpaca-py` SDK | ✅ but SDK not installed at top level |
| `engine/market_data.py` | `get_bulk_prices(symbols, timeout)` — **batched, no SDK, alpaca→yahoo fallback** | ✅ **LIVE-TESTED** |
| `engine/market_data.py` | `_get_alpaca_bulk_prices(symbols)` — single REST call to `/v2/stocks/quotes/latest?symbols=…&feed=iex` | ✅ |

**`alpaca-py` SDK is NOT installed in the default Python.** The `latest_prices` method in `alpaca_bridge.py` imports it lazily and errors gracefully if missing. `market_data.py` uses `requests` directly — no SDK dependency.

Live smoke test:
```
get_bulk_prices(['AAPL','MSFT','NVDA','TSLA','SPY'])
→ {'TSLA': {...}, 'NVDA': {...}, 'MSFT': {...}, 'AAPL': {...}, 'SPY': {...}}
each row: {symbol, price, change_pct, high, low, volume, timestamp, source='alpaca'}
```

Caveat: returned `change_pct=0.0` for closed-market Saturday data. The day's `open` price is **not in the response**. For Race's "since open" computation we need either:
- (a) Add a `get_bulk_snapshots()` to `market_data.py` that calls Alpaca's `/v2/stocks/snapshots` (returns open + latestQuote in one call) — small new function following existing pattern
- (b) Use Yahoo daily bar for `open` + `get_bulk_prices` for `last`

(a) is cleaner and one batched call instead of two sources.

## Theme/styling

- **LCARS already implemented** at `dashboard/static/css/lcars.css` (~1700 lines) + `dashboard/static/js/lcars.js`
- Linked from `dashboard/static/index.html:1945` (CSS) and `:32412` (JS)
- 1,855 inline CSS class/style references in the static HTML (it's a large single-file SPA)
- Mobile-nav grid already present at lines 12795–12802 with `.mobile-nav-item` class, theme-aware (light/dark)
- Existing `--accent`, `--nav-bg`, sectioned `[data-theme="light"]`/`[data-theme="dark"]` overrides

## Build pipeline
- `node v24.14.1`, `npm 11.11.0` available
- Vite tree builds successfully into `dashboard/frontend/dist/` — but that `dist/` is not served
- **No build step needed if Race targets `dashboard/static/index.html`**

## 🚨 Blockers raised — see `data/scotty_questions_phase2_20260510.md`

Three cascading questions that change ~80% of the Phase 2 build. Cannot proceed to Phase 2.1 without Admiral resolution.

## Phase 1 endpoints still serving
- `/api/momentum/heartbeat` → 200
- `/api/momentum/recent_signals` → 200

## Endpoint count baseline
- 618 (unchanged from Phase 1 close — restart picked up new routes without touching count)
