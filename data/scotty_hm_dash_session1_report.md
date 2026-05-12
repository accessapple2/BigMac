# HM-DASH Session 1 — Closure Report

**Date:** 2026-05-12
**Phases shipped:** DASH.1, DASH.2, DASH.2-fixup, DASH.3
**Auditor:** Scotty (Opus 4.7)
**Status:** ✅ Session 1 complete — backend foundation live, restart clean, all 3 endpoints serving

## Commits

| Commit | Phase | Summary |
|---|---|---|
| `5b76a2f` | HM-DASH.1 | `squeeze_score` field added to `/api/momentum/race` response (batch lookup against `squeeze_watch`) |
| `ce8afc2` | HM-DASH.2 | Polygon `/stocks/v1/short-interest` as primary SI source; Finviz fallback only; 24h cache; native DTC |
| `6eceb44` | HM-DASH.2 fixup | `&sort=settlement_date.desc` — default Polygon SI returns 2017 reports; sort fix gets 2026-04-30 |
| `96cb5df` | HM-DASH.3 | `GET /api/squeeze/candidates` endpoint (@timed_cache(60), tier filter, max 100 rows) |

Trader restart: pid 39784 → 41440, port 8080 bound, no `[red]`/ERROR in post-restart 200-line tail.

## Post-restart smoke results

### Endpoint 1 — `/api/momentum/race?limit=3`
HTTP 200, 700ms cold / 637ms warm. Top movers in AFTER session: AAOX +9.89%, KRMN +6.72%, MSTZ +6.32%. New `squeeze_score`, `squeeze_tier`, `squeeze_flag` fields present per row (all null/false today — none of today's top movers are in `squeeze_watch`).

### Endpoint 2 — `/api/squeeze/candidates?limit=5`
HTTP 200, 15ms cold / 7ms warm. Returns count=1: BKSY (composite=50, WATCH tier, scan_ts 2026-05-11). Full payload structure verified.

### Endpoint 3 — Polygon SI verification via live `run_scan(force=True)`
```
candidates from Finviz:    278
qualified squeeze results:   4
si_source breakdown:       {'polygon': 4}   ← 100% Polygon, 0% Finviz fallback
  ASTS  score=4  SI=30.3%  DTC=2.96   src=polygon  settle=2026-04-30
  ABR   score=3  SI=26.7%  DTC=19.74  src=polygon  settle=2026-04-30
  ATYR  score=3  SI=20.8%  DTC=25.28  src=polygon  settle=2026-04-30
  BKSY  score=3  SI=22.7%  DTC=3.75   src=polygon  settle=2026-04-30
```

## Data observations from the live run

### Polygon SI freshness vs Finviz
- Polygon `/stocks/v1/short-interest` reports settle bi-monthly (verified: latest `settlement_date` is 2026-04-30, 12 days old today). The settlement date is the FINRA exchange-reported cutoff — it's authoritative.
- Finviz "Short Float" updates whenever Finviz refreshes its screener (varies; typically same FINRA source but cached 1-4 weeks).
- **Verdict:** Polygon is fresher AND canonical. Finviz fallback is only needed if Polygon errors/times-out per ticker.

### Live data quality flag
ATYR with DTC=25.28 and ABR with DTC=19.74 are notable squeeze setups by traditional metrics. Neither was persisted because of the existing `_MIN_PERSIST_SCORE=5` threshold (see "Open observations").

## squeeze_score thresholds (existing, unchanged by HM-DASH)

```
WATCH    50-74   (composite_score from scanner score 5-7)
ALERT    75-89   (scanner score 8)
PRIORITY 90-100  (scanner score 9-10)
```

`squeeze_flag` in `/api/momentum/race` rows fires at `composite_score >= 75` (ALERT tier or above) — chosen to gate the visual treatment to high-conviction names.

## Endpoint specs (Session 2 frontend consumption)

### `GET /api/momentum/race?limit=<int>`
- Existing endpoint, extended.
- `limit`: 1-100, default 20.
- Returns `{ts, limit, rows: [...]}`.
- Each row: `rank, ticker, pct_change_since_open, last_price, open_price, volume, market_status, squeeze_score, squeeze_tier, squeeze_flag` (last 3 new in HM-DASH.1).
- Cache: 30s (compute_race level).
- Cold: ~700ms (one bulk Alpaca snapshot call covering universe + one SQLite batch query).

### `GET /api/squeeze/candidates?limit=<int>&tier=<str>`
- NEW endpoint (HM-DASH.3).
- `limit`: 1-100, default 20.
- `tier`: empty (all) | `WATCH` | `ALERT` | `PRIORITY` (case-insensitive).
- Returns `{ts, limit, tier_filter, count, rows: [...]}`.
- Each row: `symbol, scan_ts, short_pct, float_m, vol_ratio, rsi, breakout_score, composite_score, threshold_tier, price_at_scan, notes, ntfy_sent, ntfy_deferred, created_at`.
- `notes` extended in HM-DASH.2: now includes `si_source=<polygon|finviz>; si_settle=<date>`.
- Cache: 60s (@timed_cache).
- Cold: ~15ms (single SQLite query, indexed on `(dismissed, scan_ts DESC)`).

## Open observations (not blocking, surface for Captain)

### O1 — `_MIN_PERSIST_SCORE = 5` is conservative
Today's live `run_scan` produced 4 Polygon-enriched candidates with scores 3-4. None were persisted. ATYR (DTC=25.28) and ABR (DTC=19.74) are objectively interesting squeeze names by traditional metrics. The scanner's score formula is heavily weighted on float size (small float = more pts); large-float names with high SI + high DTC may consistently miss the persist bar.

Options for follow-up:
- Lower `_MIN_PERSIST_SCORE` to 3 (more candidates surface, more noise in dashboard)
- Add a DTC-driven bonus to `_score_candidate` (DTC ≥ 10 → +1, DTC ≥ 20 → +2)
- Leave as-is and let the scoring formula remain selective

### O2 — Polygon `settlement_date` lag
Most-recent Polygon SI report is 2026-04-30 (12 days old). FINRA SI reports settle every 2 weeks. Race enrichment uses `composite_score` from `squeeze_watch` (which carries the SI snapshot from when the scanner last fired), so the freshness depends on scanner cadence (30 min in production) rather than the Polygon report date directly.

### O3 — Race-row squeeze_score is null for all top movers today
Expected — today's top movers (AAOX, KRMN, MSTZ, WFRD, IREX) are not in `squeeze_watch`. The enrichment will populate naturally as the scanner runs during market hours and the universe of tracked squeeze candidates overlaps with day movers.

## Session 2 handoff — parked work

Per Captain directive, defer to Session 2:

- **Race tile frontend** (`dashboard/static/index.html`)
  - Visual treatment for `squeeze_flag: true` rows
  - LCARS-themed, mobile responsive (16-18px body, 48px tap targets per May 8 charter)
  - 30s polling cadence (matches /api/momentum/race cache)

- **Dedicated Squeeze panel frontend** (HM-AO-β-2 reactivation)
  - New panel in `dashboard/static/index.html` consuming `/api/squeeze/candidates`
  - Per Dashboard Doctrine 2026-05-08: vanilla-JS, NOT the unwired Vite/React tree
  - Tier-filter UI (WATCH/ALERT/PRIORITY dropdown)
  - Dismiss action — POST endpoint TBD (mark `dismissed = 1`)
  - 60s polling cadence (matches /api/squeeze/candidates cache)

## Session 3 preview

- Phase 4 Detail panel — already shipped at `/api/momentum/detail/{ticker}`. Need to confirm whether to extend with SI/DTC fields when the ticker is in `squeeze_watch`.
- Phase 5 live restyle of `dashboard/static/index.html` to consume all Phase-2/3/4/6 endpoints uniformly.

## Anchors (audit trail)

```
HM-DASH.1   engine/momentum/race.py                4 sites
HM-DASH.2   engine/squeeze_scanner.py              4 sites
HM-DASH.3   dashboard/app.py:18099-18144           1 block
```

`git grep "HM-DASH"` on `main` returns all 9 anchored regions.
