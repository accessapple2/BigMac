# Phase 5 Panel Map — Where Each Panel Actually Lives

**Halted before any restyle. This is the source-of-truth map.**

Generated 2026-05-10 via read-only inspection of the served dashboard.

## Serving model (recap)

Per `CLAUDE.md` and confirmed in code:

- `dashboard/app.py:9466` → `FileResponse(_static_dir + "/index.html")` at root `/`.
- `dashboard/app.py:9406` → only `StaticFiles` mount is `/static` → `dashboard/static/`.
- The Vite tree at `dashboard/frontend/` builds to `dashboard/frontend/dist/` but is **never mounted as the served dashboard**. Its `App.jsx`, `AIChatFeed.jsx`, etc. are unwired experimental code.

**Implication:** all four panels below are vanilla HTML/JS in `dashboard/static/index.html`. None are React components in the served path.

## External CSS files mounted under `/static/css/`

- `lcars.css` — theme (`<link>` at line 1945; active).
- `backtest_panels.css` — backtest sub-pages (`<link>` at line 32409).
- `mobile_fixes.css` — disabled (commented out at line 1944).

None of the four panels below have rules in any of these three files (verified via grep). All styling is either:
1. Inline `style="..."` attributes on the markup itself, or
2. Inline `<style>` blocks earlier in the same `index.html` (most notably the `readability-fixes` block at lines 1888–1942).

## The four panels

### 1. Kirk Advisory

| Aspect | Location |
|---|---|
| **Markup file** | `dashboard/static/index.html` |
| **Markup lines** | • Card: `4396–4408` (`#kirk-advisory-card`, `#kirkAdvBody`, `#kirk-badge`)<br>• Full detail section: `5405–5485` (`#section-kirk-detail`)<br>• Kirk Advisory Team card: `5487+`<br>• Toast/badge variants injected by JS: `25231–25260` |
| **CSS file** | Inline only — all styles via `style="..."` attributes on the elements themselves. No `#kirk-advisory-card` rule in `lcars.css` / `backtest_panels.css` / `mobile_fixes.css` (verified by grep).<br>**Misleading hint:** the `<style id="hide-kirk">` block at lines 1884–1887 *sounds* Kirk-related but only hides `#ctoBanner` (an unrelated CTO Daily Advisory panel). |
| **JS handlers** | Same file: `fetchKirkAdvisory()` at `24897`, polling at `24999–25000`, `/api/kirk/advisory` fetch logic. |
| **Type** | **Vanilla HTML/JS in `dashboard/static/index.html`.** Not React. |

### 2. Captain's Portfolio

| Aspect | Location |
|---|---|
| **Markup file** | `dashboard/static/index.html` |
| **Markup lines** | • Glance-bar chip (top-of-dashboard): `3988`<br>• "Captain's Live Portfolio" header in Kirk detail: `5418`<br>• "View Captain's Portfolio →" button injected by JS: `15488`<br>• `getPlayerDescription('webull')` Kirk label: `15546`<br>• "CAPTAIN'S PORTFOLIO" badge built by JS innerHTML: `25241`<br>• Modal/title definition: `26090`<br>• **Actual position table** is the existing `section-webull` infrastructure (per `CLAUDE.md`: the `section-webull` internal id stays, label migrates to "Starfleet"/Captain). |
| **CSS file** | Inline only. No external rule for any `captain*` / `kirk-badge` selector in the three `/static/css/` files. Visual styling driven by `.kirk-badge` class (defined in inline `<style>` blocks) and `style="..."` attributes. |
| **JS handlers** | Same file — fed by `/api/kirk/advisory` and the `webull`/Alpaca-mirror player path. |
| **Type** | **Vanilla HTML/JS in `dashboard/static/index.html`**, layered on top of the legacy `section-webull` markup. Not React. The `WebullPortfolio.jsx` component in `dashboard/frontend/src/components/` exists but is part of the unwired Vite tree. |

### 3. Bridge Votes

| Aspect | Location |
|---|---|
| **Markup file** | `dashboard/static/index.html` |
| **Markup lines** | • Card: `4441–4463` (`#bridge-vote-card`, `#bridge-vote-consensus`, `#bridge-vote-conviction`, `#bridge-vote-chevron`, `#bridge-vote-tally-bar`, `#bridge-vote-body`, `#bridge-vote-rows`, `#bridge-vote-footer`)<br>• Trade-Reasoning Tier-3 dropdown reference: `5696`<br>• Hidden/disabled in card-hide list: `14199` |
| **CSS file** | Mixed — **only panel of the four with a real external-ish rule**:<br>• Inline `<style id="readability-fixes">` block, **same `index.html`, lines 1933–1937**:<br>&nbsp;&nbsp;`#bridge-vote-card { background:#111827 !important; border:1px solid #1e293b !important; }`<br>• Everything else inline `style="..."` on the markup. |
| **JS handlers** | Same file: `toggleBridgeVote()` at `25008`, `fetchBridgeVote()` at `25017`, `renderBridgeVote()` at `25027+`. |
| **Type** | **Vanilla HTML/JS in `dashboard/static/index.html`.** Not React. |

### 4. War Room

| Aspect | Location |
|---|---|
| **Markup file** | `dashboard/static/index.html` |
| **Markup lines** | • Sidebar nav item: `2202` (`onclick="showSection('war-room')"`)<br>• Section container: `8764–8855` (`#section-war-room`)<br>• Title bar: `8773`, `8822`<br>• Input + send button: `8787–8788`<br>• Feed scroll container: `8854–8855` (`#warRoomContent`)<br>• Section registry: `14039`, `14120` (init = `fetchWarRoom`), `14495–14496`, `14648` |
| **CSS file** | Inline only — `style="..."` attributes on every element in the section. No `war-room` / `warRoom` selector exists in `lcars.css` / `backtest_panels.css` / `mobile_fixes.css` (verified by grep). Inherits theme variables (`var(--bg)`, `var(--surface)`, `var(--accent)`) from `lcars.css`. |
| **JS handlers** | Same file: `sendQuestionToWarRoom()` at `5908`, `fetchWarRoom()` at `20616+`, debate-progress tracker at `20447–20491`, debate-history strip at `20491–20530`. Endpoints: `/api/war-room/post`, `/api/war-room/debate-status`, `/api/war-room/debate-history`. |
| **Type** | **Vanilla HTML/JS in `dashboard/static/index.html`.** Not React. (The `AIChatFeed.jsx` component with a `warRoom` prop at `dashboard/frontend/src/App.jsx:1212` is unwired Vite-tree experimental code, not what users see.) |

## Cross-cutting observations for the restyle (informational only — no action taken)

1. **There is no panel-level external stylesheet to edit.** A restyle that wants to live in `/static/css/lcars.css` (or a new file) would have to **introduce** rules for `#kirk-advisory-card`, `#bridge-vote-card`, `#section-war-room`, and the Captain's Portfolio surface — none of which currently exist in those files (except the single bridge-vote background override in the inline `readability-fixes` block).

2. **Inline `style="..."` attributes will override any new external CSS rule** unless either:
   - The new rules use `!important` (as the existing `#bridge-vote-card` override does), or
   - The inline styles are stripped from the markup as part of the restyle.

3. **Captain's Portfolio is the most diffuse of the four** — it's not a single block, it's a label-and-badge layer over the `section-webull` infrastructure (per the existing TODO in `CLAUDE.md` to rename the label without touching the 50+ internal id references).

4. **None of the four touch the Vite tree.** A React-side restyle would not affect the served dashboard.

---

# 5.0b — Extended line-level audit (Kirk Advisory + Captain's Portfolio)

Added 2026-05-10 after Phase 5 architecture pivot. The original map was anchor-line granularity; this pass enumerates every surface the restyle has to touch (or deliberately skip).

All line numbers are in `dashboard/static/index.html` unless otherwise noted.

## Kirk Advisory — 5 surfaces

Kirk shows up in five distinct DOM regions. Three are static markup we can restyle directly; two are 100% JS-injected (body filled by `innerHTML`) and a restyle needs to land either in the surrounding container or inside the JS string.

### A. Main Kirk Advisory Card (top of Bridge / dashboard)
| Aspect | Detail |
|---|---|
| Lines | `4396–4411` (`<!-- Kirk Advisory Card -->` … `</div>`) |
| Static IDs | `#kirk-advisory-card`, `#kirk-badge`, `#kirkAdvTime` |
| Dynamic IDs (filled by JS) | `#kirkAdvBody` (placeholder text `"Loading advisory..."`) |
| JS that fills body | `fetchKirkAdvisory()` at `24897–24996`; bootstrap at `24999–25000` (setTimeout 5s + setInterval 120s) |
| Toggle wiring | `_wireKirkToggle()` at `24880–24895` (binds `.kst-btn` clicks → re-runs fetchKirkAdvisory) |
| CSS class anchors | `.card`, `.card-header`, `.kirk-source-toggle` (defined `406`), `.kst-btn` / `.kst-btn.active` (`407–409`), `.source-badge` family (`365–368`) |
| Header `onclick` to deep-dive | **commented out** (`<!-- HIDDEN: ... -->` at line `4400`) — no live navigation from card to detail page |
| Restyle target | ✅ Outer card markup is static. Header can have `class="panel-header"` added at the inline level. Body needs to be skipped or wrapped because `fetchKirkAdvisory` writes raw HTML strings into `#kirkAdvBody`. |

### B. Kirk Advisory Detail page (`section-kirk-detail`)
| Aspect | Detail |
|---|---|
| Lines | `5405–5413` (banner + body placeholder) |
| Static IDs | `#section-kirk-detail`, `#kirkDetailBody` |
| JS that fills body | `fetchKirkDetail()` at `25310–25608` (huge function — builds Captain's Positions table, cash recommendation, history, etc. all via `innerHTML`) |
| Section init | `registerSectionInit('kirk-detail', fetchKirkDetail, [])` at `25712` |
| Navigation | **No live link** — every `showSection('kirk-detail')` onclick is commented `<!-- HIDDEN: ... -->` (`4400`, `5460`, `15488`). Section is reachable only by direct URL state or registry-triggered init. |
| Restyle target | ⚠️ Body is 300 lines of JS-built HTML strings. Practical approach: add `.panel-header` to the static banner (5407–5410) and `.dense-table` to a wrapper around the JS-injected positions table — but the JS string at line `25420+` would need a class added or our CSS would need to target by structure (e.g., `#kirkDetailBody table`). |

### C. Kirk Advisory Team card (`wb-advisory-team-card`, inside Webull section)
| Aspect | Detail |
|---|---|
| Lines | `5487–5517` |
| Static IDs | `#wb-advisory-team-card`, `#wbTeamModelBadge`, `#wbTeamFallbackBadge`, `#wbTeamCostBadge`, `#wbTeamMeta`, `#wbTeamScanBtn`, `#wbAdvTab-grok`, `#wbAdvTab-worf`, `#wbAdvPanel-grok`, `#wbAdvPanel-worf` |
| JS that fills tab panels | Around `16800–16850` (Grok body) and counterpart for Worf — both via `innerHTML` |
| Restyle target | ✅ Static container; tab strip can take `.panel-header` styling. Inner panels need wrappers or selector-by-structure. |

### D. Cockpit "Ask Kirk" response box (Sniff Scan cockpit panel)
| Aspect | Detail |
|---|---|
| Lines | `6399–6403` |
| Static IDs | `#cockpit-kirk-response`, `#cp-kirk-text`, `#cp-btn-kirk` (button at `6395`) |
| JS handler | `window.sniffCockpitAskKirk` at `33496–33533` — POST to `/api/kirk/ask`, writes plain text to `#cp-kirk-text` |
| Restyle target | ✅ Static, but tiny. Likely **skip** — not a momentum surface. |

### E. Position-modal Kirk recommendation strip (`trc-kirk`)
| Aspect | Detail |
|---|---|
| Lines | `13117` (placeholder injection inside `showPosDetail`) + `13236–13275` (fetch + render) |
| Static IDs | `#trc-kirk` — but injected by `showPosDetail`, not in the static document |
| Restyle target | ⚠️ **Skip** — modal-internal, fully dynamic. Restyle would need to chase the `innerHTML` string at `13244–13270`. Not worth it for Phase 5 scope. |

### Kirk Advisory — global helpers / page-level
| Aspect | Lines | Note |
|---|---|---|
| Page-level Kirk footer | `25775` (`#kirk-footer`) | Single styled div in body — could pick up `.panel-header` typography. Low priority. |
| Toast helper `showKirkAlert` | `25209–25250` | Builds toast.innerHTML; uses `.toast-kirk` + `.kirk-badge` classes already defined `25790–25791`. |
| Polling alert banner | `24967–24990` (`#kirk-alert-banner` built by JS, animation `@keyframes kirkPulse` at `25792`) | Lives inside `#kirkAdvBody`; out of scope for static restyle. |
| Sidebar entry to Kirk Detail | **None.** No `sidebar-item` opens `section-kirk-detail` (verified via grep). |

### Kirk Advisory — existing CSS rules (in main `<style>` block)
| Selector | Line(s) |
|---|---|
| `.source-badge`, `.source-badge.paper`, `.source-badge.real`, `.source-badge.all`, `.source-badge.mixed` | `365–368` |
| `.kirk-source-toggle` | `406` |
| `.kst-btn`, `.kst-btn:hover`, `.kst-btn.active` | `407–409` |
| `.toast-kirk`, `.toast-kirk .kirk-badge`, `@keyframes kirkPulse` | `25790–25792` (inline `<style>` block at 25780) |
| **Misleading sentinel:** `<style id="hide-kirk">` | `1884–1887` — only hides `#ctoBanner` (CTO Advisory). Don't put new Kirk rules here. |

## Captain's Portfolio — 1 primary surface + 7 satellite references

### F. `section-webull` — the Captain's Portfolio page
| Aspect | Detail |
|---|---|
| Lines | `5414–5663` (entire `<div id="section-webull">…</div>` block) |
| Section ID | `#section-webull` — internal id preserved per `CLAUDE.md` (label was migrated to "Starfleet"; the 50+ refs to `webull` stay) |
| Banner | `5417–5419` — "Captain's Live Portfolio" header (static markup, easy restyle target) |
| Stat cards | `5423–5448` — five `.stat-card` (Total Value / Cash / Open P&L / Day P&L / Win Rate). All inline `style`; classes `.stat-card` already exist `570+`. Values: `#wbTotalValue`, `#wbCash`, `#wbPositionCount`, `#wbOpenPnl`, `#wbCostBasis`, `#wbDayPnl`, `#wbDayPct`, `#wbWinRate`, `#wbWinLoss`, `#wbReturnPct` |
| Action buttons | `5450–5475` — BUY / SELL / REVIEW cards using `.wb-action-row` / `.wb-action-card` / `.wb-action-buy` / `.wb-action-sell` / `.wb-action-review` (defined `760–767`); REVIEW onclick is commented HIDDEN at `5460` |
| Action panels (drawer bodies) | `5475–5476` — `<div class="wb-action-panel" id="wb-panel-buy"></div>` + sell counterpart. Bodies filled by JS (`wbToggleActionPanel`). |
| Positions table card | `5476–5485` — `.card` with title 🛸 Starfleet; `#webullPortfolio` is the JS-filled container; `#webullLastUpdate` is the timestamp slot — **natural heartbeat-dot target.** |
| Kirk Advisory Team card (overlap with surface C) | `5487–5517` |
| Portfolio Value + Sector Allocation row | `5518–5540` — `#wb-sparkline`, `#wb-sparkline-range`, `#wb-donut-wrap`, `#wb-period-btns` |
| Embedded Trade Cards row | `5541–5641` — `.wbi-tab` / `.wbi-panel` / `.wbc-tab` / `.wbc-panel` infrastructure (classes defined `769–840+`). Largely tabbed bodies filled by JS. |
| Recent Trades card | `5655–5660` — `#wb-recent-trades`, static container with JS-filled body |
| Section closing div | `5663` |
| Main render function | `fetchWebullPortfolio()` at `16679–16792`; bootstrap `setInterval(fetchWebullPortfolio, 60000)` at `16793–16794` |
| Section routing | `'webull': 'fetchWebullPortfolio'` at `14087`; also in `_PERIODIC_REFRESH` at `25752` |
| Restyle target | ✅ Banner, stat cards, table card header are all clean static targets. P&L delta-flash hooks attach to value containers (`#wbOpenPnl`, `#wbDayPnl`). Heartbeat dot lives next to `#webullLastUpdate`. |

### Captain's Portfolio — satellite references (label/badge/nav only, no panel)
| # | Line | Purpose |
|---|---|---|
| G | `1618` | `'webull': 'Kirk'` — short-name map (probably a leaderboard nicety) |
| H | `2168` | Sidebar nav: `<div class="sidebar-item" onclick="showSection('webull')"><span class="icon">🛸</span> Starfleet</div>` — **the only live entry into section-webull** |
| I | `3986–3989` | Glance-bar Portfolio chip — `onclick="navToPortfolio()"`. `navToPortfolio` is at `26253+` |
| J | `8285` | Label map `{'webull':'Captain Kirk'}` (small inline lookup) |
| K | `13942` | Player-name map `'webull':'Captain Kirk'` |
| L | `14011` | Letter-badge map `'webull':'K'` |
| M | `15089` | Persona-label map `'webull':'Captain Kirk'` |
| N | `15546` | Bio description `'webull': '👨‍✈️ <b>Captain Kirk — The Human Benchmark</b>...'` used in tooltips/player-detail |
| O | `15488` | "View Captain's Portfolio →" button injected by JS into arena cards — onclick is **commented HIDDEN** (broken link) |
| P | `25241` | "CAPTAIN'S PORTFOLIO" badge inside `showKirkAlert` toast (innerHTML string) |
| Q | `26090` | Modal title `'💰 The Captain\'s Portfolio'` (which modal isn't obvious from this line alone — likely a help/about modal) |
| R | `26253+` | `function navToPortfolio()` — bridges glance chip to section-webull |

### Captain's Portfolio — existing CSS rules
All Captain's Portfolio styling lives in the main `<style>` block (`14–1602`) and uses generic classes plus the `.wb-*` family:
| Selector group | Lines |
|---|---|
| `.stat-card`, `.stat-value`, `.stat-label`, `.stat-sub` | `570+` |
| `.wb-action-row`, `.wb-action-card`, `.wb-action-{buy,sell,review}`, `.wb-action-panel` | `760–767` |
| `.wbi-tab`, `.wbc-tab`, `.wbi-card`, `.wbi-badge-*`, `.wbi-risk-badge`, `.wbi-ticker`, `.wbi-dec-badge`, `.wbc-panel`, `.wbi-panel` | `769–840+` (52 wb-* class rules total) |
| `.card`, `.card-header`, `.card-title` | inherited theme classes (defined earlier and in `lcars.css`) |

No external rule in any `/static/css/*.css` file targets `#section-webull` or the `.wb-*` classes (verified via grep). All Captain's Portfolio CSS is inline in `index.html`.

## Decisions for Phase 5 sub-phases

| Phase | Surface | Approach |
|---|---|---|
| 5.2 (Kirk) | A (main card 4396–4411) | Add `panel-header` class + heartbeat dot next to `#kirkAdvTime`; wrap `#kirkAdvBody` with `dense-table` class. JS-injected body untouched (CSS targets by structure). |
| 5.2 (Kirk) | B (detail 5405–5413) | Add `panel-header` to static banner only. Leave JS-built body alone — selectors like `#kirkDetailBody table` can pick up `.dense-table`-like rules via descendant selector instead of class. |
| 5.2 (Kirk) | C (team card 5487–5517) | Add `panel-header` to header strip. Tabs already styled; light density pass only. |
| 5.2 (Kirk) | D, E | **Skip** — too small / too dynamic for the scope. |
| 5.3 (Captain's Portfolio) | F (section-webull 5414–5663) | Add `panel-header` to "Captain's Live Portfolio" banner (5417–5419) + Positions table header (around 5478). Heartbeat dot next to `#webullLastUpdate`. `deltaFlash()` wired to `#wbOpenPnl` / `#wbDayPnl` inside `fetchWebullPortfolio` at `16702`/`16706` (one-line wrapper around the value-set). |
| 5.3 (Captain's Portfolio) | G–R | Satellite references — **no restyle needed**. Phase 5 only touches the panel. |

## Gaps filed (none — all panels mappable)

No questions filed to `data/scotty_questions_phase5_<date>.md`. All four panels are cleanly mappable.

End of 5.0b extension. Phase 5 sub-phases 5.1–5.7 proceed using this map.

