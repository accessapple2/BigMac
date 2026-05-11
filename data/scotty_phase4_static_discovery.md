# Phase 4-static Discovery — 2026-05-10 20:14 MST

Read-only sweep. No edits. Log: `/tmp/scotty_phase4_static_discovery_20260510_2013.log`.

## TL;DR — scope is much smaller than the plan assumed

| Plan item | Actual state |
|---|---|
| 4-static.1 CSS additions | ✅ Race CSS already shipped lines `34170–34218` (Phase 2 block). New CSS only needed for Scanner (blocked) and optionally Phase 5 helpers (never built). |
| 4-static.2 Race markup | ✅ Already shipped lines `34221–34230` (`#section-race`). |
| 4-static.2 Scanner markup | ❌ Backend endpoint missing — **scope-blocked.** |
| 4-static.2 Detail modal | ✅ Already wired as `#posDetailModal` + `_momentumDetailEnrich()` (line ~34570) fetching `/api/momentum/detail/{ticker}`. No new modal needed. |
| 4-static.3 Race nav entry | ⚠️ **MOBILE entry exists** (line `12796`), **DESKTOP sidebar entry MISSING**. **This is the only real gap.** |
| 4-static.4 Inline JS | ✅ Race poller `raceStart()/raceStop()` shipped lines `34290–34328`. `openTickerDetail` shipped line `13079`. |
| 4-static.5 Nav-click wiring | ⚠️ Will land with 4-static.3 — single change point. |
| Phase 5 helpers (referenced as prerequisite) | ❌ Not shipped. `momentum-restyle`, `deltaFlash`, `updateHeartbeat`, `.heartbeat-dot` all absent. (Phase 5 only got to 5.0b — the panel map.) |

**Practical Phase 4-static delta = ~5 lines added to the sidebar nav block.**

## File

- `dashboard/static/index.html` — 34,674 lines, 1.93 MB
- Served via FastAPI `FileResponse` at `dashboard/app.py:9466` (root `/`)
- Cache headers: confirmed `no-cache` on leaderboard/backtest variants; `/` route serves file-fresh per request (no in-process cache). **Browser hard-refresh = enough to see edits. No service restart required for HTML changes.**

## Phase 5 helpers — confirmed absent

Searched for: `momentum-restyle`, `deltaFlash`, `updateHeartbeat`, `heartbeat-dot`, `.panel-header`

Results:
- `momentum-restyle` — **0 matches**
- `deltaFlash` — **0 matches**
- `updateHeartbeat` — **0 matches**
- `heartbeat-dot` — **0 matches**
- `.panel-header` — exists, but only as **scoped descendant selectors** for 4 specific panels:
  - `.fleet-feed-panel .panel-header` (line `347`)
  - `.grok-diff-panel .panel-header` (line `412`)
  - `.at-risk-panel .panel-header` (line `423`)
  - `.metals-panel .panel-header` (line `445`)
  - There is no global `.panel-header { ... }` rule.

**Implication:** Phase 5 only reached 5.0b (panel map at `data/phase5_panel_map.md`). 5.1–5.7 never executed. Phase 4-static plan's "reuse Phase 5 helpers" premise is invalid for this branch.

This is fine — Phase 4-static's only mandatory delta (the sidebar Race entry) doesn't need any of those helpers.

## Sidebar nav — confirmed structure

Main visible nav at lines `2165–2204` (Bridge / Ready Room / Ollie / Starfleet / Crew Activity / Battle Station / Charts / Live Chart / Sniff Scan / Backtest / Inst. Intel). "More" expandable list at `2196–2210` (Models / Fear & Greed / Sectors / Macro / News / Costs / Navigator / War Room / Congress / Holodeck).

Pattern is uniform:
```html
<div class="sidebar-item" onclick="showSection('xxx')"><span class="icon">…</span> Label</div>
```

Recommended Race insertion: between "Sniff Scan" (`2173`) and "Backtest" (`2174`), or alongside Charts/Live Chart. The icon `🏁` matches the mobile-nav entry at `12796` already in place.

## Existing Race tile (Phase 2 — already shipped)

| Asset | Lines |
|---|---|
| CSS block `<style>` | `34170–34218` (`.race-section`, `.race-list`, `.race-row`, flash animations, mobile breakpoint at 640px) |
| Section markup | `34221–34230` (`<div id="section-race">…<ul id="raceList"></ul>`) |
| JS poller `<script>` | `34232–34328` |
| Globals exposed | `window.raceStart()`, `window.raceStop()`, `window.raceOnRowClick(ticker)` |
| Click wire | Row onclick calls `openTickerDetail(ticker)` if defined (line `34307`) — already integrated |
| Mobile nav entry | `12796` — `<button class="mobile-nav-item" onclick="mobileNav('race',this);if(typeof raceStart==='function')raceStart()">` |
| **Desktop nav entry** | **MISSING** |

## Existing Detail modal (Phase 4 enrichment — already shipped)

| Asset | Line |
|---|---|
| `function openTickerDetail(symbol)` | `13079` (uses `#posDetailModal`, `#posDetailBody`, `#posDetailTitle`) |
| `window._momentumDetailEnrich(symbol)` | `~34568` — appends `<div id="momentum-detail-extra">` into open modal body |
| `/api/momentum/detail/{ticker}` fetch | `34579` |
| URL state sync `?detail=TICKER` | `~34608` |
| Existing modal scaffolding | `#posDetailModal` (defined elsewhere in file, used by 18+ existing call sites) |

Plan's proposed new modal (`#detail-backdrop` + `#detail-panel` aside) would **conflict and duplicate** the existing `#posDetailModal`. Recommend skipping plan steps 4-static.2 (modal section) and 4-static.4 (modal JS) entirely.

## Showsection routing — multi-patched

`window.showSection` is monkey-patched 7 times across the file (active definitions at `12359`, `14196`, `14664`, `27069`, `28725`). Cascade pattern: each patch wraps the previous via `var _sh = window.showSection; window.showSection = function(id) { _sh(id); /* extra */ }`. Adding `showSection('race')` to a sidebar click will work the same as every other existing sidebar entry.

`section-race` already obeys the `display:none` ⇄ `display:block` pattern that `showSection` toggles (verified by markup at line `34221`).

## Endpoint health check

Dashboard running locally: PID 44662 on `127.0.0.1:8080`. Direct `curl` (not behind the dashboard's redirect at `/`):

| Endpoint | HTTP | Notes |
|---|---|---|
| `/api/momentum/heartbeat` | **200** | `{bridge_alive: true, signal_center.reachable: true, phase: "HM-AN.1"}` |
| `/api/momentum/race?limit=3` | **200** | Returns ranked rows. Shape matches existing Race JS at line 34290+ |
| `/api/momentum/scanner?limit=3` | **404** | **NOT REGISTERED.** Only 4 routes exist in `dashboard/app.py`: heartbeat, recent_signals, race, detail. Backend comment at `app.py:17904` reads `"Race + Scanner tiles will build on this"` — scanner was planned but never shipped. |
| `/api/momentum/detail/AAPL` | **200** | Returns `{ticker, ts, bars: {5m, 1h, 1d}, fundamentals: {…37 keys…}, signals: [], flags: []}` — rich shape |

Detail response is much richer than the plan's JS expected:
- `fundamentals` is a 37-key dict (smart_score, grade, pe_trailing, eps_forward, target_mean, week52_high, … etc.) — perfectly handled by existing `renderFundamentals(d.fundamentals)` in `_momentumDetailEnrich`, but the plan's naive `Object.entries(fundamentals)` loop would dump all 37 keys raw.

## LCARS palette in current use

```
--bg:      #0a0e17
--surface: #111827
--accent:  #00d4aa
--text:    #e2e8f0
```

Phase 2 Race tile defines its own private palette inside `.race-section` (line `34172–34177`):
```
--race-orange: #ff9c00
--race-blue:   #99ccff
--race-up:     #00cc66
--race-down:   #ff4444
```

These match the plan's hex values exactly. Reuse `var(--race-orange)` etc. in any new CSS, rather than re-declaring.

## Scanner blocker (backend gap)

Two paths forward, escalating to the Admiral:

**Option A — defer Scanner indefinitely.** Phase 4-static.2's Scanner section/JS/CSS are dropped. Ship the Race nav entry only. Closure report notes the gap. This is what the standing rule says: "Stop on ambiguity. Write blockers to `data/scotty_questions_phase4_static_<date>.md` and skip."

**Option B — extend the plan to include backend.** Add a Phase 3-extension: write `engine/momentum/scanner.py` + register `@app.get("/api/momentum/scanner")` in `dashboard/app.py`. This violates Phase 4-static's "no backend changes" lock and probably belongs to a separate Phase 3 ticket.

Recommendation: **Option A.** File the scanner blocker, ship the Race nav entry. Phase 3 (scanner backend) becomes a future ticket.

## Existing modal/overlay patterns (for reference)

| Modal | Location | Class/ID |
|---|---|---|
| TradingView chart | `264–271` | `#tv-chart-modal`, `.tv-modal-overlay`, `.tv-modal-content`, `.tv-modal-header` |
| Wizard | `535–540` | `.wizard-overlay`, `.wizard-modal` |
| Alert popup | `881+` | `.alert-popup-overlay` |
| Position detail | `1068+` | `#posDetailModal`, `.pos-modal-body`, `.pos-modal-section` (THE one Detail uses) |

## Phase 4-static — revised plan

Given the discoveries, the minimal viable retrofit is:

| Step | Effort | What |
|---|---|---|
| 4-static.0 | DONE | Discovery (this doc) |
| 4-static.1 (CSS) | **SKIP** | Race CSS already shipped; Scanner blocked; Phase 5 helpers never built and not needed for the gap |
| 4-static.2 (markup) | **SKIP** | Race markup shipped; Scanner blocked; Detail modal is `#posDetailModal` already |
| 4-static.3 (Race nav) | **DO** | Add 1 desktop sidebar entry calling `showSection('race')` + invoking `raceStart()`. Location: line ~2173 between Sniff Scan and Backtest. ~3 lines added. |
| 4-static.4 (JS) | **SKIP** | Race poller + detail enrichment already shipped |
| 4-static.5 (wiring) | **MERGE INTO 4-static.3** | Single change point |
| 4-static.6 (verify) | **DO** | Grep anchor, hard-refresh test, smoke endpoints |
| 4-static.7 (closure) | **DO** | Write closure + file Scanner blocker to `data/scotty_questions_phase4_static_20260510.md` |

Net commits: **1 feature commit + 1 docs commit** (closure + questions). Down from the planned 5.

## Decision points for the Admiral

1. **Confirm Option A** (defer Scanner) vs. Option B (build Scanner backend in a separate Phase 3 ticket).
2. **Confirm Race nav placement** — recommend between "Sniff Scan" (2173) and "Backtest" (2174), matching the existing main visible nav, with the same 🏁 icon as the mobile entry at 12796.
3. **Phase 5 helpers** — leave deferred? The original Phase 5 plan covered `deltaFlash`/`updateHeartbeat`/`heartbeat-dot`/`panel-header` for restyling Kirk Advisory / Captain's Portfolio / Bridge Votes / War Room. None of that is needed to make the Race tile reachable, but the Phase 5 closure is now stuck at 5.0b. Decide separately.

Halted. Awaiting Admiral's decision on the three points above before proceeding to 4-static.3.
