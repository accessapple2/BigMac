# SCOPE — #10 DOM lazy-load refactor (dashboard/static/index.html) — HOLD for go

**Not built. Scope/plan only.** Big refactor; needs Admiral go + browser smoke gate.

## Problem (measured)
`dashboard/static/index.html` = **44,238 lines**, **110 `id="section-*"` blocks** + a pile of
orphan `.card`s, all parsed into the live DOM at page load. `showSection(name)` (line 21692) just
toggles `display:none` across every `[id^="section-"]` and shows one. So the browser builds and
lays out ~100 sections' worth of DOM (tables, canvases, forms) on first paint even though one is
visible. Costs: slow DOMContentLoaded, high node count/memory, and every global sweep
(ticker-chip MutationObserver — now guarded in #9; market-closed text rewriter; `_dashboardUrls`
fetch stubs) walks the whole tree.

## Approach — lazy-mount per section
1. **Instrument first (no behavior change).** Add a dev timing block: `performance.now()` at parse
   start/DOMContentLoaded, `document.querySelectorAll('*').length`, and per-section node counts.
   Rank sections by node count to target the heaviest (charts, big tables) first. This is the only
   step safe to land before the refactor — do it, measure, decide scope from data.
2. **Move each section body into a `<template id="tpl-<name>">`.** The section shell stays
   (`<div id="section-<name>">` empty); the heavy inner DOM lives inert in the template (templates
   are parsed but not rendered — no layout cost).
3. **Hydrate on first show.** `showSection(name)`: if `#section-<name>` is empty and `#tpl-<name>`
   exists → clone template content into it, mark `data-hydrated`, then run the section's init. Cache
   (don't re-hydrate). Inactive sections stay empty shells.
4. **Section-init registry (the hard part).** Today section JS runs at load (handlers bound,
   fetch-on-show like `lsAutoLoad()`, charts sized). After lazy-mount those nodes don't exist at
   load → null derefs. Refactor to `SECTION_INIT = { '<name>': fn, ... }` invoked AFTER hydration
   (once). Audit every `getElementById`/`querySelector` that runs at top level against a section
   body and move it into its init fn.

## Risk register (the DOM-element-drift class — high)
- **Top-level JS touching section nodes** (chart constructors, `addEventListener`, initial fetches)
  will hit nulls post-lazy-mount. Must be enumerated and moved into the init registry — this is the
  bulk of the work and the main breakage risk.
- **Inline `onclick="..."` handlers** referencing IDs across sections still resolve at click time
  (fine once hydrated) but break if clicked before first show — gate cross-section actions on
  hydration.
- **`_dashboardUrls` page-load fetch allowlist** + the index.html:2399 200-stub: fetches that target
  a not-yet-hydrated section's elements no-op silently — keep fetch-on-show in the init fn.
- **getElementById collisions** (known: index.html accretes duplicate IDs) — hydrating a clone with
  a duplicate ID makes the collision worse; de-dupe IDs in targeted sections first.
- **Charts/canvas** that size to parent offsetWidth at load → must size on show (already true for
  some; verify).

## Phasing (recommended)
- **P0 (safe, land now on go):** instrumentation only → real numbers + heaviest-section ranking.
- **P1 (pilot):** lazy-mount the top 3–5 heaviest sections (likely live-scanner, options chain,
  big leaderboards, charts). Build the SECTION_INIT registry for just those. Measure the win.
- **P2:** roll out to the remaining sections in batches; each batch browser-smoked.
- **P3:** drop the orphan-card hide loops once everything is section-scoped.

## Effort / call
Large (multi-session). **Recommend P0+P1 pilot first** (instrument + top-5), measure the
DOMContentLoaded/node-count delta, then decide whether the full rollout is worth it. Do NOT attempt
all 110 in one pass — the section-init audit is where it breaks. Frontend → Admiral smoke each batch.
