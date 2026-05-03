# Drydock — UI Consumption Audit

**Filed:** 2026-04-27 (Monday post-sprint test drive)
**Author:** XO (Claude) at Admiral's direction
**Status:** Open — bundle with index.html split sprint
**Severity:** Yellow (no live trading impact, paper account; fleet executes correctly,
operator visibility degraded)

---

## Pattern

**Backend serves real data. UI reads only a slice, wrong fields, or nothing at all.**

This is now a recurring class of finding distinct from sprint regressions. Each
instance has a working API behind it — the gap is in the consumer, not the producer.
That makes the fix mechanical (wire the right field / call the right endpoint /
loop the right collection) rather than investigative.

The pattern was first surfaced in Monday Sprint Phase D (`signal_history` mystery
solved as browser-driven design gap). The test drive on 2026-04-27 confirmed it
shows up in at least three more surfaces. One surface — Inst. Intel / Lt. Uhura —
demonstrates the same pipeline pattern wired correctly, which is the proof that
this is a UI-side fix, not a backend rebuild.

---


## Root Cause Hypotheses — Investigate Before Fixing

Surface-level symptom diversity does not imply causal diversity. Eight gaps are now characterized (see Surfaces Affected below), but they should be treated as candidates for collapse into a smaller number of shared root causes before the split sprint commits to eight independent panel edits.

The fix cost differs by an order of magnitude depending on which hypothesis holds: one repoint of a path constant, one cache invalidation, or one aggregator refresh could resolve multiple panels simultaneously. Eight mechanical wiring edits would not.

**Investigation order: cheapest first.** Each hypothesis below has a concrete probe that takes minutes, not hours, and either eliminates the hypothesis or narrows the fix surface.

### Hypothesis 1 - Stale browser-side cache layer

A service worker, IndexedDB store, in-memory cache, or localStorage TTL on the frontend that hydrates panels from a snapshot rather than re-fetching the live API on each render.

- Symptom signature: Fresh API response visible in browser network tab, stale data rendered in the panel. Hard reload (Cmd+Shift+R) or clearing site data fixes it temporarily.
- Surfaces consistent with this hypothesis: signal_history (Phase D - the cache layer became the de facto write trigger), Big Charts price ribbon desync, potentially Sector Watch.
- Probe: Inspect the frontend bundle for service worker registration, IndexedDB schemas, localStorage keys with TTL logic.
- Eliminates / confirms quickly: Force a hard reload on each broken panel with DevTools cache disabled. Surfaces that recover are cache-layer victims.

### Hypothesis 2 - Wrong DB path constant inherited from a prior season

A TRADER_DB_PATH or equivalent path constant resolving to a stale snapshot file, or worse, a CWD-relative bare string that lands on whatever happens to be in the working directory, rather than the live data/trader.db.

- Symptom signature: Data exists, agent names are right, schemas look correct, but contents are frozen at a specific date or stuck at a small total count.
- Surfaces consistent with this hypothesis: Ghost Scorecard Recent trades dated 2026-04-14 (plausible season cutover boundary), Leaderboard zeros, Agent Deep Dive zeros for active agents.
- Known precedent in the codebase: The uoa/ files already exhibit this exact bug class - they reference a bare "trader.db" string that resolves via CWD to a 28KB stub file instead of the live 183MB data/trader.db. That instance is logged as a post-market audit TODO and is cited here as proof the hypothesis is not theoretical.
- Probe: Grep the codebase for path constants and bare-string DB references. For each hit, confirm the resolved absolute path lands on the live 183MB data/trader.db.
- Eliminates / confirms quickly: A single grep + path resolution pass settles it. If multiple panels resolvers land on the same wrong path, the fix is one path constant edit and N panels recover for free.

### Hypothesis 3 - Intermediate snapshot-based aggregator

A scheduled job (cron, systemd timer, or in-process loop) that materializes panel-ready views from the live tables into a separate snapshot/bridge/cache table, with the served endpoints reading the materialized table rather than the live source.

- Symptom signature: Endpoint explicitly reports no data / fresh:false / source:none while the underlying live table is fully populated. Or endpoint returns a single slice when the live table has much more.
- Surfaces consistent with this hypothesis: /api/riker/synthesis returning fresh:false source:none while rikers_log is writing live (most suggestive - endpoint reports on aggregator state not data state); war-room Bridge panel stale slice while /api/war-room writes current; Sector Watch rendering only IGV while /api/sectors/heatmap has all 12; Screener Pro payload missing chg_pct/rvol/rsi.
- Probe: Identify scheduled jobs and last-run timestamps via crontab, launchctl list, and recent log mtimes. Check trader.db for tables named snapshot_, bridge_, cache_, _view, _rollup, _summary and compare MAX(updated_at) against live source tables.
- Eliminates / confirms quickly: If a snapshot table exists and its last-update timestamp matches the freeze boundary on the corresponding panel, the aggregator is the cause.

### Combinatorial possibilities

These hypotheses are not mutually exclusive. Plausible combinations:

- (2 + 3): Path constant points the aggregator at a stale DB; aggregator runs successfully against the wrong source and materializes stale-but-internally-consistent snapshots. Symptom: surfaces look almost right but frozen at a date.
- (1 + 3): Aggregator runs correctly, but a frontend cache holds the prior aggregator output past its TTL. Symptom: refresh fixes it temporarily, freeze returns.
- (1 + 2): Cache holds responses from an endpoint that reads the wrong DB. Symptom: hardest to diagnose because both layers are wrong.

### Decision rule for the split sprint

Before opening any of the eight panel-level fixes from Surfaces Affected below:

1. Run all three probes (cache audit, path-constant grep, aggregator timer check). Time budget: under one hour total.
2. For each surface, mark which hypothesis or combination explains it.
3. If three or more surfaces collapse to a single root cause, fix the root cause first and re-test all eight panels.
4. Only proceed to mechanical wiring fixes for surfaces that demonstrably do not share a root cause with others.

The eight surfaces listed below remain useful as a verification checklist post-fix, regardless of which hypothesis holds.

---
## Surfaces Affected

### 1. signal_history (Phase D — already documented)

- **Gap:** Aggregation pre-computed only on browser request, leaving stale state
  when no operator was on the dashboard.
- **Backend:** Aggregator function exists and runs correctly when invoked.
- **UI fix needed:** Trigger aggregation on a timer (or background job) instead
  of relying on the dashboard fetch as the sole entry point.
- **Reference:** `MONDAY_SPRINT_2026-04-27.md` Phase D.

### 2. Leaderboard (Fleet Status / Starfleet Intelligence — Crew Performance & Rankings)

- **UI route:** Left rail → Leaderboard (`ref_121` desktop)
- **Symptom:** All 8 agents show Value $10,000.00, Day +$0.00, Total P&L +$0.00,
  Return +0.0%, Win % 0% — even though Bridge top bar shows real fleet metrics
  ($79,564 value, -$436 day, 24 positions, Chekov +1.3%) and Crew Activity feed
  shows 15 trades today with Spock booking +8.6%/+8.9%/+7.5% scaled exits.
- **Gap:** Leaderboard table queries a season-scoped rollup table
  (likely `agent_performance` or equivalent) that is not populated for S6 (Live).
- **Counter-data source that works:** Top bar + left rail Fleet stack reads from
  the same source that populates Crew Activity → these are wired to live trade
  fills, not the season rollup.
- **UI fix needed (mechanical):** Either (a) point the Leaderboard table at the
  same live-trade source the top bar uses, or (b) add a rollup job that
  aggregates closed trades into `agent_performance` on a cron / post-trade hook,
  so the season-scoped query returns non-zero rows.

### 3. Screener Pro (Signal Discovery Engine)

- **UI route:** Left rail → Screener Pro (`ref_115` desktop)
- **Backend endpoint:** `GET /api/screener` → 200, returns `{results: [...]}` with
  20 entries.
- **Payload fields present:** `symbol, price, pe_trailing, pe_forward, market_cap,
  short_pct_float, recommendation, sector, industry, match_reasons, updated`
- **UI table columns expected:** `SYMBOL, PRICE, CHG%, RVOL, RSI, FLEET, SCORE, ACTION`
- **Gap A — missing fields:** Payload has no `chg_pct`, `rvol`, `rsi`, `score`,
  `fleet`, `action`. So even when results return, six of eight columns render
  blank.
- **Gap B — preset filters reference absent fields:** MOMENTUM BREAKOUT preset
  applies `RVol ≥ 1.5, Trend: Uptrend, Chg% ≥ 1`. Engine has no rvol/chg%/trend
  data to evaluate against, so 0 of 20 tickers can possibly pass any of these
  presets. Test runs on MOMENTUM BREAKOUT and OVERSOLD BOUNCE both returned
  "0 of 20 matches" cleanly (no error, just empty filter result).
- **Gap C — universe hardcoded:** 20 tickers exclusively: SPY, QQQ, TQQQ, NVDA,
  TSLA, AAPL, AMD, META, MSFT, GOOGL, AMZN, MU, ORCL, NOW, AVGO, PLTR, DELL,
  XLE, INTC, NUKZ. The "70+ filters" framing implies a market-wide screener;
  this is a tech-heavy curated watchlist instead.
- **Gap D — sector enrichment partial:** 5 of 20 tickers (the indices/ETFs:
  SPY, QQQ, TQQQ, XLE, NUKZ) stuck on `sector: "Unknown"` instead of
  Index/Energy/Nuclear.
- **UI fix needed (mechanical):**
  - Backend: extend the screener_universe / `/api/screener` payload to include
    chg_pct, rvol, rsi, score (and the OT-specific fleet/action fields).
  - Backend: expand universe beyond 20 tickers OR rebrand UI as "Watchlist
    Screener" so user expectations match.
  - Backend: fix sector tagging for ETFs (lookup table, not yfinance).
  - UI: no changes needed once payload matches column expectations.

### 4. Sector Watch (Market Sector Intelligence)

- **UI route:** Left rail → Sector Watch (`ref_116` desktop)
- **Backend endpoint:** `GET /api/sectors/heatmap` → 200, returns
  `{sectors: [...]}` with **all 12 sector ETFs**: XLC, XLP, XLB, XLF, XLY, XLK,
  XLU, XLRE, XLI, XLE, XLV, ITA. Each sector entry includes `change_pct`,
  `source: "finviz"`, and `holdings: [{symbol, price, change_pct, volume}, ...]`
  for the top constituents.
- **Symptom:** UI renders ONE sector card (IGV — iShares Expanded Tech Software
  ETF) with status "LOADING..." indefinitely, then a 13-stock grid of IGV
  constituents (MSFT, CRM, PLTR, ORCL, SNOW, OKTA, DDOG, ADBE, NOW, PANW, APP,
  CRWD, INTU) with all prices/chg% dashed out.
- **Gap:** UI consumes a different (apparently 404-ing or static) endpoint and
  ignores the working `/api/sectors/heatmap` entirely. Network tab shows
  `/api/sectors`, `/api/sector-watch`, `/api/sectors/health`, `/api/sector/igv`
  all returning 404, while `/api/sectors/heatmap` (which works) is hit only
  once on page load by something else and the result is discarded for the
  Sector Watch render.
- **UI fix needed (mechanical):** Point the Sector Watch render at
  `/api/sectors/heatmap`. Render all 12 sectors with change_pct as a
  rotation strip (winners green, losers red), expand-on-click to show holdings.
  IGV-specific thesis pair ("AI hype overblown vs Real disruption") can stay
  as a pinned section below the rotation strip.
- **Side note (not part of this fix):** KMI/WMB position bleeding (currently
  -8.60% / -3.26% on Positions at Risk panel) won't surface in XLE top holdings
  because they're midstream pipelines, often slotted into AMLP/MLPX. Surfacing
  position-level sector context is a separate enhancement, not part of the
  consumption audit.

---

## Counterexample — The Pattern Done Right

### Inst. Intel / Lt. Uhura (Institutional Intel)

- **UI route:** Left rail → Inst. Intel (`ref_113` desktop)
- **Behavior:** Header shows "Last scan: 2026-04-27" (today, 5:30 AM AZ daily run
  hit clean). Stat cards populated: Funds Tracked 6, Unique Tickers 6,415,
  Active Signals 55. Institutional Signals (Last 3 Days) table fully rendered
  with AMD / CNTA / DELL / HOOD STRONG SELL rows, dates 2026-04-24 → 2026-04-27,
  insider count column populated ("5 insider sells in 30 days", "6 insider
  sells in 30 days").
- **Why this matters:** Same data-pipeline pattern as the four broken surfaces
  — daily aggregation job → table → API → UI render. The fact that Uhura is
  fully wired proves the bridge architecture supports this pattern correctly.
  The four broken surfaces are not waiting on missing infrastructure; they're
  waiting on the same wiring Uhura already has.

---

## Recommendation

**Bundle this audit with the index.html split sprint.**

Justification:
- All four broken surfaces and the Uhura counterexample are rendered out of the
  same monolithic frontend (`index.html` and its associated JS bundle).
- Splitting `index.html` will already require touching every panel's render
  function and API consumption layer.
- The cost of fixing chg_pct/rvol/rsi field wiring on Screener Pro, repointing
  Sector Watch to `/api/sectors/heatmap`, and routing the Leaderboard table
  away from the empty rollup is tens of lines per panel — trivial overhead
  on top of the split sprint, and avoids re-opening the same files twice.
- Each panel's exact gap is documented above so the fix is mechanical, not
  investigative, when the sprint runs.

### Sprint shopping list (paste-ready when split sprint kicks off)

1. **Leaderboard:** Repoint table query from season-rollup table → live trade
   source (same source as top-bar fleet metrics + Crew Activity feed).
   Alternative: schedule an `agent_performance` rollup job on cron + post-fill
   hook.
2. **Screener Pro:** (a) Extend `/api/screener` payload to include
   `chg_pct, rvol, rsi, score, fleet, action`. (b) Decide universe: expand
   beyond 20 tickers OR rebrand UI label to "Watchlist Screener". (c) Fix
   sector tagging for ETFs (SPY/QQQ/TQQQ/XLE/NUKZ).
3. **Sector Watch:** Repoint render from current (mostly-404) endpoints →
   `/api/sectors/heatmap`. Render 12-sector rotation strip with change_pct
   coloring; preserve IGV thesis pair as pinned section.
4. **signal_history (Phase D follow-up):** Confirm aggregation now runs on a
   timer/background job per Phase D fix; remove any remaining dashboard-fetch-
   triggers-aggregation paths.

### Out of scope for this audit

- Battle Station 0DTE chain blanks (separate pre-existing OCC parsing gap on
  the standing list).
- `/api/gex/regime` Alpaca creds gap (separate pre-existing item on the
  standing list).
- KMI/WMB midstream sector context on Positions at Risk panel (separate
  enhancement, not a wiring gap).
- Mobile bottom-nav FLEET button no-op (separate routing nit, not a data
  consumption issue).

---

## Test Drive Evidence

Captured 2026-04-27 ~14:00 ET, MARKET OPEN, paper account, 8 agents active,
$79,564 value, -$436 day, 24 positions, Chekov +1.3% lead.

- Screenshot: Main Bridge — green
- Screenshot: Sniff Scan — green (live SPY chart, scanner hits firing)
- Screenshot: Live Chart — green (1834 bars, all overlays)
- Screenshot: Crew Activity — green (Spock scaled exits live)
- Screenshot: Leaderboard — yellow (zeros described above)
- Screenshot: Battle Station — yellow (pre-existing, out of scope)
- Screenshot: Inst. Intel — green (counterexample)
- Screenshot: Screener Pro — yellow (0 of 20 on two presets)
- Screenshot: Sector Watch — yellow (IGV-only render)

API probes (via in-page fetch):
- `GET /api/screener` → 200, 20 results, payload missing chg_pct/rvol/rsi.
- `GET /api/sectors/heatmap` → 200, 12 sectors with full holdings.
- `GET /api/sectors` → 404
- `GET /api/sector-watch` → 404
- `GET /api/sectors/health` → 404
- `GET /api/sector/igv` → 404

---

## Sign-off

XO recommendation: file as-is, defer execution to index.html split sprint, no
mid-week hotfix needed since fleet trades correctly through all four gaps and
this is paper. Operator visibility (Leaderboard ranking, Screener actionability,
Sector rotation read) is the cost paid until the split sprint lands.
