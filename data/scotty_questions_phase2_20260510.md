# Phase 2 Blockers — Scotty (2026-05-10)

These three questions cascade through every subsequent Phase 2 task. **Phase 2.1 cannot begin until Admiral resolves at least #1.** Recommendations below are biased toward minimum-divergence and live-surface visibility.

---

## Q1 (CRITICAL) — Where does the Race UI live?

### The conflict
- **Directive says:** build `dashboard/frontend/src/components/Race.tsx` + `RaceTab.tsx` + run `npm run build`
- **CLAUDE.md says:** "Dashboard is served from `dashboard/static/index.html` on port 8080. The Vite tree at `dashboard/frontend/` is unwired experimental code — its `dist/` is never mounted." (Reinforced by memory hooks `feedback_port8080_html.md`, `feedback_correct_index_html.md`.)
- **Discovery confirms CLAUDE.md:** `dashboard/static/index.html` is 1.9 MB, edited May 8 (active). `dashboard/frontend/dist/index.html` is 597 bytes, last touched April 25. `dashboard/app.py` mounts only `/static`, not the Vite `dist/`.

### Options

| Option | What it means | Tradeoff |
|--------|---------------|----------|
| **A — vanilla in `dashboard/static/index.html`** *(recommended)* | New `<section id="section-race">` block + `<button class="mobile-nav-item" onclick="mobileNav('race',this)">` + JS in a new `dashboard/static/js/race.js` + CSS in a new `dashboard/static/css/race.css` (or extend `lcars.css`) | Ships visible value immediately. Matches reality. No build step. Smaller diff. |
| **B — React in `dashboard/frontend/`, then also wire the mount** | Build `Race.tsx` + extend `dashboard/app.py` to `app.mount("/app", StaticFiles(directory=dashboard/frontend/dist))` or similar | Brings the React tree online for the first time. Significant architectural shift. Existing static dashboard stays canonical for everything else, so users would see two UIs. |
| **C — React only, accept that it's invisible** | Build `Race.tsx` exactly per directive; ignore the mount problem | Pure directive compliance but ships **zero visible value**. Compile-only success. |

**Recommendation:** **A.** It's the only option that produces a Race tab the Admiral can click after restart. CLAUDE.md and two memory hooks all point to this. The 1.9 MB `static/index.html` is where every active dashboard feature lives.

If A is chosen: the rest of the directive's frontend phases (2.4 Race.tsx, 2.5 nav, 2.6 Vite build) get replaced with:
- 2.4-A: HTML section + JS module + CSS (3 small files or one extended file)
- 2.5-A: insert `<button class="mobile-nav-item" onclick="mobileNav('race',this)">` into the existing mobile-nav grid
- 2.6-A: no build step; just edit-in-place

---

## Q2 — Universe module: reuse or duplicate?

### The conflict
- **Directive says:** create `engine/momentum/universe.py` reading from `data/universe/sp500.csv` (CSV-backed) with `lru_cache`. Halt-and-ask if no SP500 list found.
- **Discovery found a complete, production-grade universe accessor already in place:**
  - `engine/universe.py::get_active_universe()` — already filtered, already cached (30s TTL), already documented (`docs/UNIVERSE.md`)
  - Backed by `scan_universe` table (2,949 raw rows, filtered to ~600-900)
  - Refreshed weekly by `engine/universe_refresh.py`
  - Has documented fallback to a 20-name mega-cap list if table empty

### Options

| Option | What it means | Tradeoff |
|--------|---------------|----------|
| **A — reuse `engine/universe.py::get_active_universe()`** *(recommended)* | `engine/momentum/race.py` imports `from engine.universe import get_active_universe` | Zero duplication. Always in sync with weekly refresh. No CSV to keep updated. |
| **B — build `engine/momentum/universe.py` per directive (CSV-backed)** | Parallel implementation; CSV needs manual seeding + maintenance | Duplicates 60+ lines of universe logic; CSV will drift from `scan_universe` over time. |

**Recommendation:** **A.** The directive's Phase 0.5 specifically said "Existing file in repo (e.g., `data/universe/sp500.csv` or `engine/scanner/sp500_list.py`) — reuse, don't duplicate." `engine/universe.py` is exactly that — better than a CSV because it's already live-filtered.

---

## Q3 — Price/snapshot source

### The conflict
- **Directive says:** `from engine.providers.alpaca_provider import get_snapshots` — call returns `{ticker: SnapshotObj}` with `.latest_trade.price`, `.daily_bar.open`, `.daily_bar.volume`
- **Reality:**
  - `engine/providers/alpaca_provider` does not exist
  - `alpaca-py` SDK is not installed at the project Python level (`ModuleNotFoundError: No module named 'alpaca'`)
  - `engine/alpaca_bridge.py::AlpacaBridge.latest_prices(symbols)` lazy-imports `alpaca-py`, so it's also unusable today
  - **What IS live:** `engine/market_data.py::get_bulk_prices(symbols)` — batched Alpaca-direct REST → Yahoo fallback → individual fallback. **Live-tested:** returns 5/5 symbols in one call.

### Options

| Option | What it means | Tradeoff |
|--------|---------------|----------|
| **A — extend `engine/market_data.py` with `get_bulk_snapshots()`** *(recommended)* | Add one new function: GET `/v2/stocks/snapshots?symbols=…` returning `{symbol, last_price, open, volume, hi, lo}`. Mirror the existing `_get_alpaca_bulk_prices` pattern. | One small function, no SDK dependency, single batched call, matches existing code style. |
| **B — install `alpaca-py` SDK and use directive's SDK pattern** | `pip install alpaca-py` + new `engine/providers/alpaca_provider.py` | New dependency (~10 transitive deps). Two ways to talk to Alpaca in the codebase. |
| **C — derive open from Yahoo daily bar + last from `get_bulk_prices()`** | Two batched calls (different sources) per refresh | Less consistent (Alpaca + Yahoo timing drift). Race-relevant volume + open from same source loses coherence. |

**Recommendation:** **A.** Alpaca's snapshots endpoint is exactly the right primitive for Race (one call → last_trade, daily_bar.open, daily_bar.volume). Adding it to `market_data.py` keeps the codebase's two existing Alpaca patterns (`_get_alpaca_bulk_prices`, `_get_alpaca_price`) consistent with a new `_get_alpaca_bulk_snapshots` sibling.

---

## Recommended path forward (if all three recommendations accepted)

This is the **minimum-divergence, maximum-visibility** path:

| Phase | Original directive | Recommended (Option A/A/A) |
|-------|-------------------|----------------------------|
| 2.1 | New `engine/momentum/universe.py` + `data/universe/sp500.csv` | **Skip; reuse `engine/universe.get_active_universe()`** |
| 2.2 | `engine/momentum/race.py` — calls `get_universe()` + `get_snapshots()` | `engine/momentum/race.py` — calls `get_active_universe()` + `get_bulk_snapshots()` (new in market_data.py) |
| 2.2.5 (new) | — | Add `get_bulk_snapshots(symbols)` to `engine/market_data.py` |
| 2.3 | `/api/momentum/race` endpoint in `dashboard/app.py` | **Unchanged** |
| 2.4 | `Race.tsx` + CSS in `dashboard/frontend/src/components/` | `<section id="section-race">` + JS + CSS into `dashboard/static/index.html` (or split into separate `static/js/race.js` + `static/css/race.css` and `<link>` them) |
| 2.5 | Wire `RaceTab` into React nav | Insert `<button class="mobile-nav-item">` into existing mobile-nav grid in `static/index.html` |
| 2.6 | `npm run build` to regenerate `dist/` | **Skip**, no build step required. Just verify file edits + endpoint count. |

**Estimated commit count under recommendation:** 4-5 atomic commits (instead of 6) — `2.2.5 market_data snapshots`, `2.2 race engine`, `2.3 race endpoint`, `2.4+2.5 race tile UI + nav` (likely combined since both touch the same file), `2.7 closure doc`.

**Estimated effort:** halved vs. directive (~4-6h vs. ~10-12h), and ships visible value instead of compile-only success.

---

## Risk if Admiral wants strict directive compliance (Options B/B/B)

- Q1 strict: ship Race.tsx that no user can see; would need a follow-on PR to mount the React tree at a sub-path AND duplicate auth-redirect behavior
- Q2 strict: maintain CSV that drifts from the live `scan_universe` weekly refresh
- Q3 strict: introduce `alpaca-py` as a new dependency; two Alpaca-talking patterns in the codebase

These are all surmountable, just longer/riskier. Surfacing so the choice is explicit.

---

**Awaiting Admiral decision on Q1 (minimum), Q2, Q3.**
