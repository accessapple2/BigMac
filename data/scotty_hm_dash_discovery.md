# HM-DASH Session 1 — Phase 0 Discovery

**Date:** 2026-05-12
**Phase:** DASH.0 (Discovery, no code changes)
**Auditor:** Scotty (Opus 4.7)
**Status:** HALT — directive's scope assumes green-field; most of Session 1 already shipped

## TL;DR

Three of the four "build from scratch" items in HM-DASH already exist on `main`:

| Directive item | Reality |
|---|---|
| DASH.1 squeeze data layer | **Exists** — `engine/squeeze_scanner.py` + `squeeze_watch` table shipped HM-AO-β (2026-05-08). SI source is legacy Finviz "Short Float", not Polygon. |
| DASH.2 universe loader | **Exists** — `engine/universe.py:get_active_universe()` shipped HM-AQ-β (2026-05-07). 668 names. Polygon-driven weekly refresh. |
| DASH.3 Race engine + endpoint | **Exists** — `/api/momentum/race` + `engine/momentum/race.py:compute_race()` shipped Dashboard Remodel v1 Phase 2 (referenced May 10). |
| Polygon SI in tier | **YES, all 6 endpoints HTTP 200.** Native `days_to_cover` field. No fallback needed. |

The remaining shippable gaps are smaller than DASH.1+2+3 as written:
- **Gap A** — `/api/momentum/race` response doesn't include `squeeze_score` per row.
- **Gap B** — `squeeze_scanner.py` SI source is legacy Finviz "Short Float"; could migrate to Polygon `/stocks/v1/short-interest` for canonical SI + DTC.
- **Gap C** — No `/api/squeeze/candidates` or equivalent endpoint backing the HM-AO-β-2 frontend panel (Captain Doctrine 2026-05-08 resolved that the panel goes into `dashboard/static/index.html`, not the Vite tree — so the missing bit is purely the endpoint).

## Pre-flight findings

### Existing remodel docs (`docs/`)
- `DASHBOARD_AUDIT_2026-04-20.md`
- `DASHBOARD_AUTH_PLAN.md`
- `DASHBOARD_DOCTRINE_2026-05-08.md` — Verdict A: `dashboard/static/index.html` is canonical; Vite tree is unwired experimental code.
- `HM-AO-B_SQUEEZE_WATCHER.md` — Squeeze Watcher (Ghost pattern) shipped 2026-05-08. Default OFF (`SQUEEZE_WATCHER_ENABLED`). Schema includes `short_pct, float_m, vol_ratio, rsi, breakout_score, composite_score`. Promotion path defined but not executed.

### HM-AN Phase 1 + Dashboard Remodel v1 endpoints already on `dashboard/app.py`
| Line | Endpoint | Phase |
|---:|---|---|
| 18011 | (anchor) HM-AN Phase 1 momentum bridge endpoints | Phase 1 |
| 18054 | `GET /api/momentum/race` → `compute_race(limit)` | Phase 2 |
| ~18075 | `GET /api/momentum/detail/{ticker}` → `compute_detail` (30s cache, regex-validated) | Phase 4 |
| ~18095 | `GET /api/momentum/premarket` → `compute_premarket(limit, force)` | Phase 6 |

### Existing race-related files (3 of them)
- `engine/momentum/race.py` — **canonical** Dashboard Remodel race engine. Uses `engine.market_data.get_bulk_snapshots` + `engine.universe.get_active_universe`. Returns `{rank, ticker, pct_change_since_open, last_price, open_price, volume, market_status}`. No squeeze fields.
- `engine/stock_race.py` — legacy watchlist version (top-16). Predates the remodel. Different code path.
- `engine/strategy_race.py` — AI swing strategy vs SPY equity curve. Unrelated to top-gainers.
- `data/strategy_race.json` — persistence for strategy_race only.

### Polygon API key
- `POLYGON_API_KEY` present, 32 chars, prefix `Nvrc...`
- Single key; no separate stocks/options keys

## Phase DASH.0 — Polygon endpoint probe (all 6 endpoints HTTP 200)

| Endpoint | HTTP | Wall | Notable response fields |
|---|---:|---:|---|
| `/stocks/v1/short-interest?ticker=GME&limit=5` | 200 | 0.46s | `settlement_date, ticker, short_interest, avg_daily_volume, days_to_cover` ← native DTC |
| `/v3/reference/tickers/GME` | 200 | 0.33s | full ticker metadata |
| `/v2/snapshot/.../tickers/GME` | 200 | 0.33s | snapshot (count=0 post-market; structure intact) |
| `/v3/reference/tickers?market=stocks&active=true&limit=100` | 200 | 0.87s | 100 per page, pagination via `next_url` |
| `/stocks/v1/short-volume?ticker=GME&limit=3` | 200 | 0.36s | `total_volume, short_volume, exempt_volume, short_volume_ratio, nyse_short_volume` ← daily SI proxy |
| `/v2/aggs/ticker/GME/range/1/day/...` | 200 | 0.36s | OHLC bars (v, vw, o, c, h, l, t, n) — RVOL substrate |

**Verdict:** Stocks Starter (or whatever this key's tier is) covers everything HM-DASH needs. Native SI + DTC + short-volume + ticker reference + snapshots + aggs. No FINRA / Yahoo / NASDAQ TotalView fallback required.

## Existing squeeze_scanner.py SI flow

`engine/squeeze_scanner.py:158` reads `row.get("Short Float", 0)` where `row` is a screener row (looks Finviz-shaped). The "Short Float" field is parsed via `_parse_float_val` (handles `%`, `K`, `M`, `B` suffixes). Composite score buckets:
- `>= 40%` → +3
- `>= 30%` → +2
- `>= 20%` → +1
- Float ≤ 5M → +2, ≤ 20M → +1
- vol_ratio ≥ 5 → +2 (from yf_data)

This works today but the SI freshness is unverified (Finviz updates SI roughly twice a month; Polygon `/short-interest` exposes settlement_date for each report). A migration to Polygon would give us:
- Canonical FINRA-derived SI (no scrape brittleness)
- Native DTC (no need to compute from float + volume)
- Settlement-date timestamp on each fact

## Q1, Q2, Q3 — answered (so Captain can re-scope)

### Q1 — Polygon short-interest tier coverage
**Resolved:** YES. `/stocks/v1/short-interest` and `/stocks/v1/short-volume` both return HTTP 200 with the structured fields we want. `days_to_cover` is native. No fallback chain needed.

### Q2 — Universe source
**Resolved:** ALREADY DONE. `engine/universe.py:get_active_universe()` exists. Returns 668 names today, sourced from Polygon-driven weekly refresh (`engine/universe_refresh.py`, Sundays 14:00 MST), filtered by `market_cap >= $5B AND avg_dollar_volume >= $100M AND staleness <= 14 days`. ETFs included, ETNs skipped. Sample: NVDA, TSLA, META, WMT, LLY, MU, V, XOM, COST, MA.

If Captain wants **wider** universe (e.g. full S&P 500 + R1K → ~2,500 names) the lever is in `engine/universe.py`: lower `MIN_MARKET_CAP` (currently $5B) and/or `MIN_DOLLAR_VOLUME` (currently $100M after the HM-AQ-β v3 raise from $50M). Note: HM-AQ-β v3 raised the threshold precisely because the dashboard couldn't render >1,500 names in <3s. Going wider re-opens that pain.

### Q3 — Session 1 scope confirmation
**Needs re-scoping.** As written, DASH.1+2+3 would be net-new-zero (duplicate already-shipped work). The shippable Session 1 backlog that respects the spirit ("backend foundation + Race engine + short-squeeze focus"):

#### Option A — minimum-viable re-scope (recommended, ~1.5 hr)
1. **DASH.1' (new)** — Add `squeeze_score` to `/api/momentum/race` response by reading from `squeeze_watch` table (already populated by HM-AO-β scanner). Each race row gets a `squeeze_score` and `squeeze_flag` field. No new scanner work. Anchor `# === HM-DASH.1 ===`.
2. **DASH.3' (new)** — Add `/api/squeeze/candidates` endpoint reading from `squeeze_watch` table, ordered by composite_score DESC. This backs the deferred HM-AO-β-2 frontend panel. Anchor `# === HM-DASH.3 ===`.

#### Option B — fuller scope (~3-4 hr, recommended if Captain wants Polygon SI integration)
Everything in Option A plus:
3. **DASH.2' (new)** — Migrate `engine/squeeze_scanner.py` SI source from Finviz "Short Float" → Polygon `/stocks/v1/short-interest`. Keeps existing fallback (Finviz) for resilience. Anchor `# === HM-DASH.2 ===`.

#### Option C — full directive as written (~4-5 hr, NOT recommended — produces parallel duplicate modules)
Build `engine/squeeze_data.py`, `engine/universe.py` (re-creation), `engine/race_engine.py` per the literal directive. Creates 4th race module and 2nd universe loader. Will diverge from HM-AQ-β + HM-AO-β + HM-AN cumulative work.

## HALT

Recommend **Option A** for Session 1 (1.5 hr, low risk, surfaces squeeze data via existing endpoint). Defer Polygon SI migration to a focused HM-AO-β-3 follow-up if data quality concerns arise. Frontend integration stays Session 2 per directive.

NTFY fired.
