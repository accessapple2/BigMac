# Detail Panel Phase 4 Closure — 2026-05-10

**Mission:** click any Race row → get the **why**. Multi-timeframe price, fundamentals, fleet signals, URL-stateful.

**Outcome:** ✅ delivered, non-invasively, on top of the existing `openTickerDetail()` modal. Same Path-A approach as Phase 2 (60% scope reduction vs. directive after Phase 0 surfaced 3 cascading blockers).

---

## Summary

- Discovery: `data/scotty_phase4_discovery.md`
- Blockers raised + Admiral resolved A/A/A: `data/scotty_questions_phase4_20260510.md`
- Commits staged: **3** (vs. directive's planned 5; UI + URL state merged since both touch same file)
- Service restart required: **YES** (Phase 4.2 adds a FastAPI route; static UI serves immediately)
- Push: **NOT performed** — Admiral pauses VPN, runs `git push origin main`

## Commits (newest first)

| Hash | Subject |
|------|---------|
| `2d9d4c0` | feat(dashboard): Phase 4.3+4.4 — detail enrichment + URL state |
| `495b718` | feat(dashboard): Phase 4.2 — `/api/momentum/detail/{ticker}` endpoint |
| `bc191f1` | feat(momentum): Phase 4.1 — detail engine |

Total diff: **+552 / -0** across 3 files.

## Files added

| Path | Lines | Purpose |
|------|-------|---------|
| `engine/momentum/detail.py` | 189 | `compute_detail(ticker)`: multi-timeframe bars + fundamentals + per-ticker signals + 30s cache |

## Files modified

| Path | Lines | Change |
|------|-------|--------|
| `dashboard/app.py` | +19 | `/api/momentum/detail/{ticker}` endpoint with regex validation, anchored as `=== Phase 4: Detail panel endpoint ===` |
| `dashboard/static/index.html` | +344 | Detail enrichment block — CSS + JS wrapper that adds 3 sections to existing `openTickerDetail()` modal + URL state |

Endpoint count: 619 → **620** (+1).

## Path A decisions (Admiral approved post-discovery)

| Q | Choice | Reason |
|---|--------|--------|
| Q1 — Phase 3 dependency | Omit `flags` from payload (placeholder `[]`) | `engine/momentum/flags.py` doesn't exist; Phase 3 never ran. Phase 4 ships standalone; Phase 3 retrofit is trivial when delivered |
| Q2 — UI surface | Extend existing `openTickerDetail()` modal | Comprehensive modal already exists in `dashboard/static/index.html:13079` (TradingView lightweight-charts, Crew Consensus, Kirk Rec, Chekov Convergence). Single source of truth, no parallel modal |
| Q3 — Fundamentals data | Parse `stock_fundamentals.data` JSON blob | 1,251 tickers, 56-key blob includes `next_earnings` / `days_to_earnings` natively — no separate calendar table needed |

Net scope: 3 commits vs. directive's planned 5. Skipped: Phase 4.5 npm build (no build step needed — vanilla static).

## What ships (UX)

Click any Race row → existing `openTickerDetail()` modal opens immediately (already working). Then **three new sections appear at the bottom of the modal body**, fetched via one `/api/momentum/detail/{ticker}` call:

### 1. Fundamentals
- Smart Score + Grade pill (A=green, B=blue, C=orange, D/F=red)
- Sector + Industry
- 9-cell metrics grid: **P/E TTM, P/E Fwd, PEG, ROE, Rev Growth, EPS Growth, Mkt Cap, Profit Margin, Beta**
- Target price + upside % + analyst count + recommendation pill
- Next earnings date + days-to-earnings (orange highlight)

### 2. Multi-Timeframe (Alpaca)
- **5-Min · 3d** sparkline (SVG inline)
- **1-Hour · 14d** sparkline (SVG inline)
- Each shows percent change + last price below the line
- (Daily timeframe remains owned by the existing TradingView lightweight-charts embed at the top of the modal — no duplication)

### 3. Fleet Signals · 24h
- Per-player rows: player_id · side (BUY/SELL/HOLD coloured) · confidence % · relative timestamp ('5m ago')
- Reasoning shown as native tooltip on hover
- Empty-state copy when no signals on the ticker (Saturday evening expected: 0)

## URL state (`?detail=TICKER`)

- `openTickerDetail()` wrapper sets `?detail=AAPL` via `history.replaceState`
- `closePosDetail()` wrapper clears it
- `popstate` handler opens/closes the modal as browser back/forward navigates
- On initial page load, reads `?detail=TICKER` and opens the modal (with 100ms defer so other bootstraps finish first)
- Reload preserves the open detail panel — refresh keeps AAPL open

## Esc-to-close

Additive global keydown listener. Only fires when the modal has class `.open`. Does not break any existing close path.

## Live evidence

- **Phase 4.1 in-process smoke (AAPL):**
  - First call: 1.77s (3 serial Alpaca timeframe calls)
  - Cache hit: 0.0000s (same dict object returned)
  - Payload: 11,408 bytes JSON
  - Fundamentals: 38 keys, smart_score=72, grade=B, pe=35.47, target=$305.28, earnings 2026-07-30 (82d), 42 analysts → "buy"
  - Bars: 5m=3, 1h=14, 1d=65
- **Static HTML edits served live:** 15 marker matches in `/static/index.html` confirms enrichment block is being served right now
- **/api/momentum/detail/AAPL:** returns **404** until daemon restart (Phase 4.2)
- **Tag balance:** `<style>` 16/16, `<script>` 153/153 — file well-formed

## Non-invasive design

The original `openTickerDetail()` function (700+ lines starting at line 13079) is **untouched**. The Phase 4 block at the end of the file:

1. Stores a reference to the original `window.openTickerDetail`
2. Replaces it with a wrapper that calls the original then triggers enrichment + URL update
3. Same wrapper-pattern for `closePosDetail`
4. IIFE guarded by `window._mdPhase4Loaded` flag — re-running the directive is safe

This means existing features (Crew Consensus, Kirk's Recommendation, Chekov's Convergence, Debate in War Room button, etc.) continue working exactly as before.

## Endpoints to test after restart

```bash
# Detail endpoint (full payload)
curl -s "http://localhost:8080/api/momentum/detail/AAPL" | python3 -m json.tool | head -30

# Invalid ticker (should return error object, not 500)
curl -s "http://localhost:8080/api/momentum/detail/INVALID..%24" | jq

# Test cache: two calls within 30s should be the same payload
curl -s "http://localhost:8080/api/momentum/detail/NVDA" | jq '.ts'
sleep 1
curl -s "http://localhost:8080/api/momentum/detail/NVDA" | jq '.ts'
# Should print the same timestamp (cache hit)
```

## UI to verify after restart

1. Browser → log in → click 🏁 **Race** tab
2. Click any Race row
3. Existing modal opens (chart + Crew Consensus + Kirk Rec already work)
4. Scroll down within the modal → see three NEW sections appear
   - **Fundamentals** with Grade pill, metrics grid, target/recommendation/earnings
   - **Multi-Timeframe** with 5m and 1h sparklines
   - **Fleet Signals · 24h** (likely empty on weekend; populates Monday)
5. Check URL bar → `?detail=AAPL` appended
6. Hit refresh → modal reopens with AAPL detail intact (URL state working)
7. Hit Esc → modal closes, URL cleared
8. Click another ticker (e.g. from Race row) → all three sections re-render with new data
9. Browser back-button → modal closes (popstate handling)
10. Resize narrow (≤640px) → funds-grid collapses 3-col → 2-col, sparklines grow 60→80px tall, signal-row font 13→15px

## Known limitations (Phase 4 v1)

- **Per-ticker signal queries do a full table scan** — `signals.symbol` column has no index. Acceptable for per-click latency (single SELECT, 3097-row max for popular tickers).
- **Fundamentals freshness** depends on `stock_fundamentals.updated_at`. Latest AAPL entry is 2026-05-09 (one day old) — fine for daily-scope metrics. No live refresh path.
- **flags placeholder** — `payload.flags` is always `[]` until Phase 3 ships. UI doesn't render anything for empty flags.
- **No SSR for `?detail=` initial load** — modal opens client-side after 100ms defer, so reload briefly shows the underlying tab.

## Questions raised

None blocking. Two low-priority observations:

1. **`data/scotty_phase4_discovery.md` + `…questions…md` are untracked.** Consistent with Phase 1 + Phase 2 working artifacts; not committed.
2. **Phase 3 (Scanner tile + flags) is still missing.** Doesn't block Phase 4 or Phase 5 (closer), but a heads-up: when Phase 3 is sequenced, the directive language will assume Race + Scanner both exist. The current Race tile click-through to Detail works regardless.

## Push readiness

- **3 Phase 4 commits** staged on local `main`, ahead of `origin/main`
- Working tree: dirty with **unrelated** files (pre-existing model-watch state, scotty session docs, db backups — same set as Phase 2)
- **Admiral action:**
  1. Pause VPN (or leave on — last push was a no-op)
  2. `git push origin main`
  3. `launchctl kickstart -k gui/$(id -u)/com.trademinds.trader`
  4. Smoke: `curl -s "http://localhost:8080/api/momentum/detail/AAPL" | jq '.fundamentals.smart_score'`
  5. Browser → Race tab → click any row → scroll to see new sections

## Next session (Phase 5 — the closer)

- **Live restyle** across Kirk Advisory / Captain's Portfolio / Bridge Votes / War Room
- Delta-flash, density compression, mobile breakpoints
- Estimated effort: 5-8h
- Same Path A precedent: edit `dashboard/static/index.html` directly, no React tree

## Standing-rule audit

| Rule | Status |
|------|--------|
| Sacred DBs untouched | ✅ all reads via SELECT |
| No `rm -rf`, no destructive ops | ✅ |
| Diff-then-apply | ✅ all 3 commits had a diff preview |
| Bytecode reminder | ✅ flagged in 4.2 + 4.3+4.4 commits + this report |
| One atomic commit per task | ✅ 3 commits |
| NTFY on each commit | ✅ 4 NTFYs (discovery, 4.1, 4.2, 4.3+4.4) + final closure queued |
| Push gate | ✅ no push performed |
| Stop on ambiguity | ✅ 3 blockers raised + resolved before code |
| Idempotent guards | ✅ `=== Phase 4: ... ===` anchors on every insert + `window._mdPhase4Loaded` IIFE guard |
| No service restarts, no process kills | ✅ |
| Signal Center read-only | ✅ |
| Alpaca rate-limit awareness | ✅ per-click endpoint with 30s cache; 3 serial bar calls per uncached request |
