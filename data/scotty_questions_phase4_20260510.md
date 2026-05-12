# Phase 4 Blockers — Scotty (2026-05-10)

Three cascading questions, same pattern as Phase 2's resolved A/A/A. Each can be answered independently; recommendations biased toward minimum-divergence and maximum-UX-coherence.

---

## Q1 (CRITICAL) — Phase 3 dependency on `flags` module

### The conflict
- **Directive says:** `compute_detail()` imports `from engine.momentum.flags import get_flags_bulk` and includes a `flags: [...]` field in the response payload
- **Reality:** `engine/momentum/flags.py` does not exist. Phase 3 (Scanner tile) was never executed in this session — `engine/momentum/scanner.py` also missing. `/api/momentum/scanner` returns 404.

### Options

| Option | What it means | Tradeoff |
|--------|---------------|----------|
| **A — Omit `flags` from Phase 4 payload entirely** *(recommended)* | `compute_detail()` returns `flags: []` placeholder; no import of `flags.py`; UI shows empty/hidden flags section | Cleanest decoupling. Detail ships independently of Phase 3. When Phase 3 ships, retrofit takes 5 lines. |
| **B — Stub `engine/momentum/flags.py`** | New ~15 LOC file with `get_flags_bulk(tickers) -> dict[str, FlagSet]` returning empty objects | Lets directive code compile unchanged. Phase 3 will replace the stub. Slight risk: stub may shape Phase 3's eventual API in the wrong direction. |
| **C — Block on Phase 3 first** | Build Phase 3 (Scanner + flags) before Phase 4 | Maximum coherence but delays Phase 4 shipping. The directive itself ordered Phase 4 before Phase 5 (closer) — implying Phase 3 was assumed already done. |

**Recommendation:** **A.** The flags section is a UI nicety, not load-bearing for the detail-panel deliverable. Race + Scanner tiles use flags primarily as filter chips; the Detail panel just displays them as badges next to the ticker. Empty list = no badges. Phase 3 retrofit later is trivial.

---

## Q2 (CRITICAL) — UI surface: `openTickerDetail` already exists and is comprehensive

### The conflict
- **Directive proposes:** new `dashboard/frontend/src/components/MomentumDetail.tsx` — a side panel with Chart / Fundamentals / Signals tabs
- **Reality:** `dashboard/static/index.html:13079` already has `function openTickerDetail(symbol)` — a comprehensive modal that uses TradingView lightweight-charts (1D/5D/1M/3M/1Y), RSI/SMA/EMA indicators, Crew Consensus, Chekov's Convergence, Kirk's Recommendation, "Debate in War Room" button, "Full Chart" widget. The Race tile's `raceOnRowClick` already routes here via the Phase 2 fallback chain. Click any Race row right now in production → this modal opens.

### Options

| Option | What it means | Tradeoff |
|--------|---------------|----------|
| **A — Enhance existing `openTickerDetail` modal** *(recommended)* | Add three new sections to the existing modal: (i) Fleet Signals for this ticker (per-player, last 24h), (ii) Fundamentals from `stock_fundamentals.data` JSON blob, (iii) Multi-timeframe Alpaca bars (5m/1h via new endpoint). Mark sections with `=== Phase 4: ... ===` anchors. | Single source of truth. Race users get an upgraded modal, not a parallel one. UX coherent. Diff is +~150 lines in the existing function. |
| **B — Build a NEW dedicated MomentumDetail panel in static** | New `<div id="section-momentum-detail">` overlay separate from `#posDetailModal`. Race row click reroutes from `openTickerDetail` to the new panel. | Pure directive intent. Risks: (i) duplicates significant UX functionality (chart, indicators, crew consensus already exist there); (ii) 2 detail UIs in the codebase confuse future maintenance; (iii) ~3× larger diff. |
| **C — Backend-only this session, defer UI** | Ship `engine/momentum/detail.py` + `/api/momentum/detail/{ticker}` only. UI decision (extend modal vs new panel) becomes a separate ticket. | Cleanest commit graph, smallest blast radius. Race continues to use `openTickerDetail` modal as-is until UI sprint follow-up. Loses momentum on the visible deliverable. |

**Recommendation:** **A.** UX-coherent (one modal, richer), aligns with the Phase 2 Path A precedent (vanilla static, reuse existing infrastructure), avoids duplicating the chart + crew consensus that already work. Phase 4 backend (4.1 + 4.2) still ships standalone — the UI extension consumes it.

---

## Q3 — Fundamentals + earnings data source

### The conflict
- **Directive references:** `polygon_fundamentals` table and `earnings_calendar` table with specific columns
- **Reality:** neither table exists. Available are:
  - `stock_fundamentals (symbol PK, data TEXT JSON blob, smart_score INT, grade TEXT, updated_at TEXT)`
  - `earnings_universe (ticker, added_date, …)` — tracking, not a forward calendar
  - `earnings_impact (symbol, report_date, expected_eps, actual_eps, beat_miss, price_reaction_1d)` — historical only

### Options

| Option | What it means | Tradeoff |
|--------|---------------|----------|
| **A — Parse `stock_fundamentals.data` (JSON blob); skip earnings calendar entirely in v1** *(recommended)* | `_fundamentals()` returns `{symbol, smart_score, grade, updated_at, …parsed_from_data_blob}`. Earnings: omit `next_earnings_date` from payload; add as a TODO for when the calendar table ships. | Uses what exists. No empty-promise fields in the API. |
| **B — Add a /api/momentum endpoint that calls Polygon directly** | Live fetch fundamentals at request time | Adds external dep + latency to a per-click endpoint. The 30s cache the directive specified would still hit Polygon often. Skip. |
| **C — Skip fundamentals entirely v1** | `compute_detail()` returns `fundamentals: {}` | Loses real value. `stock_fundamentals` table exists — we should use it. |

**Recommendation:** **A.** Hits the existing-data primitive (Phase 2 Path A pattern again).

---

## Recommended path forward (Path A again across all three)

| Phase | Original directive | Recommended (A/A/A) |
|-------|-------------------|---------------------|
| 4.1 | `compute_detail()` with `flags.get_flags_bulk` import + `polygon_fundamentals` + `earnings_calendar` | `compute_detail()` returns `{ticker, ts, bars:{5m,1h,1d}, fundamentals:{from stock_fundamentals}, signals:[per-ticker 24h], flags:[]}` — no flags.py import |
| 4.2 | `/api/momentum/detail/{ticker}` | **Unchanged** |
| 4.3 | New React `MomentumDetail.tsx` + sparkline component + side-panel CSS | Extend `openTickerDetail()` in `dashboard/static/index.html` with three new sections: Fleet Signals (24h), Fundamentals snippet, multi-timeframe (5m/1h) charts. Wrap edits in `=== Phase 4: Detail enhancement ===` anchors. |
| 4.4 | Click-through + URL state in React | Race row click already calls `openTickerDetail` — no rewiring needed. Add `?detail=TICKER` URL param + `popstate` handler so refresh keeps modal open. |
| 4.5 | `npm run build` | **Skip** — vanilla static |
| 4.6 | Closure report | Unchanged |

**Estimated effort:** 3-5h (vs. directive's 8-12h)
**Estimated commits:** 4 (4.1 backend + 4.2 endpoint + 4.3+4.4 UI enhancement + 4.6 closure)

---

## Risk if strict directive compliance (Options B/B/B with Q2=B variant)

- Q1=B: stub `flags.py` ships, Phase 3 has to delete-and-replace the stub interface
- Q2=B: two detail modals coexist; user confusion when one row opens one modal and another row opens the other
- Q3: directive references tables that don't exist → 4.1 would crash on `_fundamentals()` `OperationalError`

---

**Awaiting Admiral decision on Q1, Q2, Q3.**
