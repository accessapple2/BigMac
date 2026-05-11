# Pre-Market Gap Scanner — Phase 6 Closure

**Generated:** 2026-05-10 20:36 MST
**Branch:** main
**Commits staged:** 4 feature + 1 nav-bundle = **5 functional changes since `origin/main`** (plus 3 prior docs commits)
**Push performed:** NO
**Service restart performed:** NO

## What shipped

### `engine/momentum/premarket.py` (new, 142 lines)
- `compute_premarket(limit=30, force=False)` — batched-Alpaca pre-market gap scanner
- Filter: `|gap_pct| ≥ 3% AND volume ≥ 50K`
- Reuses `engine.universe.get_active_universe()` (full ~hundreds-symbol universe, not the 15-symbol watchlist used by legacy `engine/premarket_scanner.py`)
- Reuses `engine.market_data.get_bulk_snapshots()` (single HTTP call to Alpaca for the entire universe)
- Reuses `engine.momentum.race._market_status_now()` for window classification (DRY — no duplicate timezone logic)
- `flags=[]` always for v1 — flags integration deferred (`engine.momentum.flags` module doesn't exist, separate ticket)
- Smoke test (force=True, off-hours): returns 3 hits with correct shape

### `dashboard/app.py` (+14 lines)
- `GET /api/momentum/premarket?limit=N&force=bool` — thin FastAPI route wrapping `compute_premarket()`
- Coexists with legacy `/api/premarket-gaps` (different consumer — `engine.ai_brain`, different universe, different cadence)
- Anchored: `# === Phase 6: Pre-market gap scanner endpoint ===`

### `dashboard/static/index.html` (+213 lines)
- Pre-market tile markup inserted as **sibling of `.race-section` inside `#section-race`** (lines 34232–34245) — same-tab placement per plan
- Phase 6 CSS block (lines 34347–34422) — scoped `.premarket-*` classes with private LCARS yellow palette (`--pm-accent: #ffcc99`) to distinguish from Race orange. Mobile breakpoint at 640px drops price column.
- Self-contained `.heartbeat-dot` styling (live/stale states + pulse keyframes) — Q7 resolved inline since Phase 5 helpers never shipped
- Phase 6 JS poller (lines 34426–34539) — 60s polling, click→`openTickerDetail` (reuses existing `#posDetailModal` — Q4 resolved by reuse, no parallel modal)
- **Desktop Race sidebar nav entry** at line 2175 — bundles the carry-over Phase 4-static.3 fix per Q5. Without this, the entire Race tab (and now pre-market tile) was reachable only via mobile-nav.

## Commits (newest first)

```
8bfb2ce feat(dashboard-static): Phase 6.4 — pre-market tile JS (poller + render + click-through)
861730e feat(dashboard-static): Phase 6.3 — pre-market gap tile UI + CSS + Race desktop nav
1c7a344 feat(dashboard): Phase 6.2 — /api/momentum/premarket endpoint
f5bbf50 feat(momentum): Phase 6.1 — pre-market gap scanner engine
ec92c3c docs(scotty): phase 6.0 — pre-market gap scanner discovery + 7 questions
fb3fe12 docs(scotty): phase 4-static.0 — discovery + 4 blockers/questions
206b0a4 docs(scotty): phase 5.0b — line-level audit of Kirk Advisory + Captain's Portfolio panels
```

## Question resolution

| # | Question | Resolved by |
|---|---|---|
| Q1 | Plan import paths wrong | Phase 6.1 — engine uses corrected paths (`engine.market_data` / `engine.universe` / `engine.momentum.race._market_status_now`) |
| Q2 | `engine.momentum.flags` doesn't exist | Phase 6.1 — `flags=[]` for v1; future ticket |
| Q3 | Coexist with legacy `premarket_scanner.py` | Phase 6.2 — `/api/momentum/premarket` parallel to `/api/premarket-gaps` |
| Q4 | Alpaca volume field | Phase 6.1 — `snap['volume']` from normalizer (= `dailyBar.v` cumulative); engine logs `skipped_no_prev` / `skipped_filter` counts when empty so Monday's first run will surface any field issues |
| Q5 | Race desktop nav missing | Phase 6.3 — bundled the ~3-line fix into 6.3 |
| Q6 | Legacy `/api/premarket-gaps` broken | OUT OF SCOPE — flagged in discovery doc for separate investigation ticket |
| Q7 | `.heartbeat-dot` CSS absent | Phase 6.3 — self-contained inline (~12 lines) |

## Endpoint smoke-test (current state, restart pending)

```
$ curl -s -o /dev/null -w "%{http_code}" "http://localhost:8080/api/momentum/premarket?force=true&limit=3"
404
```

Expected — Python imports for `engine.momentum.premarket` haven't loaded into the running service. After restart will return `200` with the same shape verified during Phase 6.1 direct-import smoke:

```json
{
  "ts": "2026-05-11T03:33:29.083710Z",
  "window_state": "CLOSED",
  "hits": [
    {
      "rank": 1, "ticker": "RKLB", "gap_pct": 33.94,
      "prev_close": 78.56, "premarket_price": 105.22,
      "premarket_volume": 1102162,
      "flags": [], "direction": "UP", "market_status": "CLOSED"
    }
    // ... up to N hits
  ]
}
```

(Sunday-night gaps will be stale leftover data; Monday 4:00 AM ET will produce live pre-market hits.)

## Admiral action

In this order:

```bash
cd ~/autonomous-trader

# 1. Sanity check what's going up
git log origin/main..HEAD --oneline   # expect 7 commits (5 feature, 3 docs… one earlier docs commit was Phase 5.0b)

# 2. Pause VPN, push
git push origin main

# 3. Restart picks up new Python imports
launchctl kickstart -k gui/$(id -u)/com.trademinds.trader
sleep 5

# 4. Smoke (force=true bypasses window gate since 8PM MST = CLOSED)
curl -s "http://localhost:8080/api/momentum/premarket?force=true&limit=5" | python3 -m json.tool

# 5. Hard-refresh browser → sidebar shows new 🏁 Race entry → click → see Race tile + Pre-market tile below it
```

## UI verification checklist

After push + restart + browser hard-refresh:

1. **Desktop sidebar** → new `🏁 Race` entry appears between `Sniff Scan` and `Backtest`
2. Click `🏁 Race` → Race section opens (Race tile at top, pre-market tile beneath)
3. **Pre-market tile** shows:
   - Header: `🌅 PRE-MARKET — Gap ≥ 3% · Vol ≥ 50K` + window-state label + heartbeat dot
   - Window label reads `○ Market closed` (since it's Sunday night / late evening)
   - Empty-state line: "Pre-market window opens at 4:00 AM ET"
   - Heartbeat dot transitions: starts gray (stale) → green pulsing (live) after first 200 response → returns to gray if no update for 120s
4. **Monday 4:00 AM ET**: window flips to `🟢 LIVE`, real gap hits populate (universe ~hundreds of symbols, top 20 by absolute gap %)
5. Click any pre-market row → existing `#posDetailModal` opens with chart + fundamentals + signals (reuses Phase 4.3 detail panel; no parallel modal)
6. **Mobile (< 640px)**: price column drops, rows compact to 5-column layout, taps stay 48px tall for thumb-safe hits

## Files modified

```
 dashboard/app.py            |  14 +++
 dashboard/static/index.html | 213 +++++++++++++++++++++++++++++++++++
 engine/momentum/premarket.py| 142 +++++++++++++++++++++ (new)
 3 files changed, 369 insertions(+)
```

(Excludes prior docs commits 4-static.0, 5.0b, and 6.0.)

## Standing rule compliance

- ✅ No `git push` performed — Admiral pushes manually
- ✅ No service restart performed — Admiral restarts after push
- ✅ No DB writes anywhere in Phase 6 code (read-only on `data/trader.db` via `get_active_universe()`)
- ✅ Signal Center read-only (`engine.momentum.bridge` not touched)
- ✅ Alpaca discipline: one batched `get_bulk_snapshots(universe)` call per 60s cycle (no per-ticker loops)
- ✅ LCARS personality preserved — pre-market tile uses LCARS yellow accent (`#ffcc99`) to differentiate from Race orange but inherits the same Antonio typography, 18px rounded top corners, and 4px left border treatment
- ✅ Idempotency anchors greppable: `=== Phase 6` matches at 9 distinct locations across 3 files
- ✅ One commit per sub-phase (4 feature commits)

## v2 status

| Phase | Status |
|---|---|
| Phase 1 — HM-AN bridge | ✅ shipped (`2a0b58d`) |
| Phase 2 — Race tile (engine + endpoint + UI) | ✅ shipped (`2a0b58d` → `9b802f0`) |
| Phase 3 — Scanner | ❌ **never built — backend route 404 carry-over blocker** |
| Phase 4 — Detail panel (engine + endpoint + UI enrichment) | ✅ shipped (`bc191f1` → `2d9d4c0`) |
| Phase 4-static — UI retrofit | ⚠️ Race nav bundled into 6.3; Scanner UI still blocked on Phase 3 backend |
| Phase 5 — Live panel restyle | ⏸️ stuck at 5.0b (panel map only); 5.1–5.7 deferred |
| **Phase 6 — Pre-market gap scanner** | ✅ **shipped this session — engine + endpoint + UI + nav** |

## Follow-up tickets to file

1. **Phase 3 — `/api/momentum/scanner` backend.** The Scanner tile in Phase 4-static plan can't ship without this. Recommend dedicated ticket: `engine/momentum/scanner.py` + endpoint registration.
2. **`/api/premarket-gaps` legacy endpoint broken.** Returned empty body during discovery curl. Affects `engine/ai_brain.py:898` consumer (silently — 5-min cache + try/except). Investigate `engine/premarket_scanner.py::scan_premarket_gaps()` per-symbol yfinance timeouts.
3. **`engine/momentum/flags.py`.** Standardize earnings / squeeze / lowfloat / volume-spike flag detection. Consumed by Race, Pre-market, and any future tile.
4. **Phase 5 restyle — resume or abandon.** Decide whether to finish 5.1–5.7 (live restyle of Kirk Advisory / Captain's Portfolio / Bridge Votes / War Room) or close out Phase 5 at 5.0b's map-only state.

## Closing

Pre-market gap scanner is ready to ship. Tomorrow 4:00 AM ET the tile populates with whatever the market hands us. Coffee + bridge → live edge.

— Scotty
