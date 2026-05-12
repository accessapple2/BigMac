# Race Tile Phase 2 Closure — 2026-05-10

**Mission:** ship the Race tile — top gainers since open, refreshed every 30s, LCARS-themed, mobile-responsive. First visible Dashboard Remodel v1 panel.

**Outcome:** ✅ delivered, on the live dashboard surface, with a 50%+ scope reduction vs the original directive after Phase 0 surfaced three blockers.

---

## Summary

- Discovery: `data/scotty_phase2_discovery.md`
- Blockers raised + Admiral resolved A/A/A: `data/scotty_questions_phase2_20260510.md`
- Commits staged: **4** (vs. directive's planned 6; merged & skipped per recommended path)
- Service restart required: **YES** (Phase 2.3 adds a FastAPI route; static UI serves immediately)
- Push: **NOT performed** — Admiral pauses VPN, runs `git push origin main`

## Commits (newest first)

| Hash | Subject |
|------|---------|
| `9b802f0` | feat(dashboard): Phase 2.4+2.5 — Race tile UI + nav integration |
| `28e53e1` | feat(dashboard): Phase 2.3 — `/api/momentum/race` endpoint |
| `cd13282` | feat(momentum): Phase 2.2 — race engine top-gainers computation |
| `eb1f339` | feat(market_data): Phase 2.2.5 — `get_bulk_snapshots()` for Race tile |

Total diff: **+333 / -0** across 4 files.

## Files added

| Path | Lines | Purpose |
|------|-------|---------|
| `engine/momentum/race.py` | 86 | `compute_race(limit)` + `_market_status_now()` |

## Files modified

| Path | Lines | Change |
|------|-------|--------|
| `engine/market_data.py` | +65 | `_get_alpaca_bulk_snapshots()` + `get_bulk_snapshots()` |
| `dashboard/app.py` | +20 | `/api/momentum/race` endpoint, anchored as `=== Phase 2: Race tile endpoint ===` |
| `dashboard/static/index.html` | +162 | Race UI block (CSS + section + poller JS) + 🏁 mobile-nav button between Bridge and Board |

Endpoint count: 618 → **619** (+1).

## Path A decisions (Admiral approved post-discovery)

| Q | Choice | Why |
|---|--------|-----|
| Q1 — UI surface | Vanilla in `dashboard/static/index.html` | CLAUDE.md verified-truth: React tree at `dashboard/frontend/` is unmounted; static HTML is the live surface |
| Q2 — Universe | Reuse `engine/universe.get_active_universe()` | Already filtered, cached, 1,292 names, refreshed weekly |
| Q3 — Snapshots | New `get_bulk_snapshots()` in `engine/market_data.py` | No SDK install, matches existing `_get_alpaca_bulk_prices` pattern, single batched REST call |

Net scope: 4 commits vs. directive's planned 6. Skipped: Phase 2.1 (universe module + CSV), Phase 2.5 (merged into 2.4 since same file), Phase 2.6 npm build (no build step needed for static).

## Live evidence

- **Phase 2.2.5 smoke (in-process):** `get_bulk_snapshots(['AAPL','NVDA',…])` returned 5/5 symbols with correct open/last/volume; AAPL +0.94%, NVDA +1.03%
- **Phase 2.2 end-to-end smoke (in-process):** `compute_race(limit=5)` ran the full pipeline in **0.74 seconds** for 1,292-symbol universe → top-5 ranked output
  - #1 RKLX +43.72% (open 42.27 → last 60.75)
  - #4 RKLB +22.41% (open 85.96 → last 105.22, vol 1.1M)
- **Phase 2.4 static-served confirm:** `curl /static/index.html` returns the new Race block (4 marker matches)
- **/api/momentum/race endpoint:** 404 until restart (Phase 2.3 added a route the running daemon hasn't loaded)

## Endpoints to test after restart

```bash
# Race endpoint (top 5 for a quick eyeball check)
curl -s "http://localhost:8080/api/momentum/race?limit=5" | python3 -m json.tool

# Race endpoint (full 20-row payload — what the UI requests)
curl -s "http://localhost:8080/api/momentum/race?limit=20" | jq '.rows | length'

# Input bound clamping
curl -s "http://localhost:8080/api/momentum/race?limit=9999" | jq '.limit'  # expect 100
curl -s "http://localhost:8080/api/momentum/race?limit=0"    | jq '.limit'  # expect 1
```

## UI to verify after restart

1. Open http://localhost:8080 → login
2. On mobile (or narrow browser) click the new 🏁 **Race** tab in the bottom nav (between Bridge and Board)
3. Confirm:
   - LCARS orange-underlined "RACE — TOP GAINERS" title bar
   - Status badge top-right shows OPEN/PRE/AFTER/CLOSED (currently CLOSED on Saturday)
   - 20-row list with rank / ticker / pct (green or red) / price / volume
   - Resize to ≤640px wide → mobile layout: volume column hides, font grows to 17px, row min-height 56px
   - Wait 30 seconds → meta line "Last update:" timestamp ticks; if a row's pct changes, a green/red flash pulses for 200ms
4. Click any row → either opens existing detail panel (if `openTickerDetail` is defined) or chart (if `openTVChart` is defined) or no-op (Phase 4 will wire detail)

## Known limitations (Phase 2 v1; tracked for Phase 3+)

- **Russell 1000 not yet expanded.** Universe is 1,292 names from `scan_universe` table (the cap≥$5B + dollar-vol≥$100M filter). Adding R1K would require either widening the SQL filter or layering an additional source. Race displays top 20 so universe breadth matters for "missed mover" coverage, not display.
- **No holidays in `_market_status_now()`.** Status is a label only, not a trading decision, so naive weekday/time check is OK. Future swap: Alpaca `/v2/clock` for authoritative state.
- **Click-through is graceful-no-op until Phase 4.** Tries `openTickerDetail` then `openTVChart` then does nothing — won't error if neither exists.
- **No persistent caching at the endpoint level.** Each `/api/momentum/race` hit does one fresh Alpaca call (0.7s). The Race UI polls every 30s per browser tab. Acceptable load: ≤2 calls/min per active tab, all batched.
- **Auth posture not verified for /api/momentum/race.** Phase 1's `/api/momentum/*` endpoints appear to bypass the dashboard auth wall (curl returned 200 without a session cookie during smoke test). If Race should be behind auth, that's a tiny follow-up.

## Next session (Phase 3)

- **Momentum Scanner tile** — 5-min movers + RVOL stream
- New module: `engine/momentum/scanner.py`
- New endpoint: `/api/momentum/scanner`
- Same `dashboard/static/index.html` + nav-button pattern proven this session
- Estimated effort: 6-9h (smaller than directive's 10-15h thanks to Path A precedent)

## Questions raised

None blocking. Two low-priority observations for future cleanup:

1. **`dashboard/frontend/` tree decision pending.** 30+ unmounted React components + a 4-week-stale `dist/`. Either retire (move to `archive/`) or wire (mount `dist/` at a sub-path). Worth a separate ticket; affects no current work.
2. **Stale unrelated working tree.** `data/bull_bear_cache.json`, `docs/OPS_LOG.md`, `docs/model_watch/MODEL_WATCH_2026-05-08.md`, `data/model_watch_log.jsonl`, three `data/trader.db.pre-legacy-flag-*` backups, `archive/stubs/`, `backups/main.py.pre-hm-as-b2-20260508_075409`, `docs/model_watch/MODEL_WATCH_2026-05-10.md`, `reports/`. None are Phase 2 artifacts; carried over from earlier work. Admiral may want a separate session to sweep.

## Push readiness

- **4 Phase 2 commits + 3 Phase 1 commits = 7 commits ahead of `origin/main`**
- Working tree: dirty with unrelated files (pre-existing; not part of Race work)
- **Admiral action:**
  1. Pause VPN
  2. `git push origin main`
  3. `launchctl kickstart -k gui/$(id -u)/com.trademinds.trader`
  4. Smoke: `curl -s "http://localhost:8080/api/momentum/race?limit=5" | jq`
  5. Browser → login → click 🏁 Race tab

## Standing-rule audit

| Rule | Status |
|------|--------|
| Sacred DBs untouched | ✅ read-only via `engine.universe.get_active_universe()` |
| No `rm -rf`, no destructive ops | ✅ |
| Diff-then-apply on every edit | ✅ 4 commits each had a diff preview |
| Bytecode reminder (no service restart) | ✅ flagged in 2.3 + 2.4+2.5 commits + this report |
| One atomic commit per task | ✅ 4 commits, well-scoped |
| NTFY on each commit | ✅ 5 NTFYs (discovery, 2.2.5, 2.2, 2.3, 2.4+2.5) — final closure NTFY queued |
| Push gate | ✅ no push performed |
| Stop on ambiguity | ✅ 3 blockers raised + resolved before code |
| Idempotent guards | ✅ greppable anchors on every insert (`=== Phase 2: Race tile … ===` × 3 distinct surfaces) |
| No service restarts, no process kills | ✅ |
| Signal Center read-only | ✅ Signal Center never touched this phase |
| Alpaca rate-limit awareness | ✅ single batched `/v2/stocks/snapshots` call per `/api/momentum/race` hit (no per-ticker loop) |
