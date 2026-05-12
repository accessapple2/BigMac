# 🔧 HM-CB + HM-BJ.E4 — Closure Report

**Author:** Scotty (Opus 4.7)
**Date:** 2026-05-12
**Status:** Complete — Polygon primary candles + single-fetch tooltip + memory #27 browser-test compliance

---

## Backstory

This thread started as HM-BJ.E4 (server-side scorecard aggregator to eliminate 3s cold-path on ticker chip hover) and pivoted twice as upstream failure modes revealed themselves:

1. **HM-BJ.E4 backend** (commits `2893af8`, `ca855e6`, `823a4b6`, `5938e17`, `2b367ab`)
   - Built `/api/symbol/{sym}/scorecard` aggregator with `@timed_cache(60)`
   - Caught and fixed: TimeoutError uncovered, `console` NameError, `with ThreadPoolExecutor` shutdown-wait blocking 26s
2. **HM-CA** (commit `dfab0c5`) — Alpaca-first candles. Diagnosed as insufficient: Alpaca free-tier rate-limited unpredictably (SPY 0.35s, IBM 6.3s, NVDA 429 fail). Yahoo fallback still 8-30s on cold symbols.
3. **HM-CB** (commit `10368c9`) — Polygon Stocks Starter as primary. Captain confirmed already paid; CLAUDE.md "approved-in-principle" line was stale.
4. **HM-BJ.E4 frontend** (commit `bb27770`) — re-applied the single-fetch swap after HM-CB verification. Captain browser-tested per the memory #27 rule learned from the HM-BJ.E2 revert.

---

## Final architecture (one diagram)

```
Browser hover
    ↓
.ticker-chip → setTimeout(300ms) → fetchScorecard(sym)
    ↓
ONE fetch: /api/symbol/{sym}/scorecard
    ↓
@timed_cache(60s) — hit returns in ~135ms
    ↓ miss
ThreadPoolExecutor (module-level, max_workers=8)
    ├── market_candles(sym)     ─→  get_intraday_candles  ─→  Polygon (primary, ~0.5s)
    │                                                      ├→  Alpaca (fallback)
    │                                                      └→  Yahoo (final fallback)
    ├── symbol_sentiment(sym)   ─→  ~0.13s (DB-backed)
    └── symbol_news(sym)        ─→  ~1.7s (RSS scrapes)
        ↓ each via _await(future, label, timeout=3s)
        ↓ per-sub-fetch typed-catch nulls just that field on failure
    ↓
Return {candles, sentiment, news}
    ↓
Frontend renderTip → sparkline + scorecard data
```

---

## Measured outcome (post-shipping, full stack)

### `/api/market/candles/<sym>` (cold, fresh symbols)

| Symbol | Latency | Bars | Tier served |
|---|---|---|---|
| SPY  | 0.54s | 106 | Polygon ✓ |
| IBM  | 0.59s | 151 | Polygon ✓ |
| NVDA | 0.56s | 100 | Polygon ✓ |
| MMM  | 0.84s | 113 | Polygon ✓ |
| XOM  | 0.53s | 121 | Polygon ✓ |
| ORCL | 0.53s | 112 | Polygon ✓ |
| JPM  | 0.66s | 136 | Polygon ✓ |
| PEP  | 0.69s | 132 | Polygon ✓ |
| TSLA | 0.54s | 100 | Polygon ✓ |
| META | 0.62s | 118 | Polygon ✓ |

**All 10 cold-symbol candles under 1s. Zero fallback log entries — Polygon served every request.**

### `/api/symbol/{sym}/scorecard` (cold, fresh symbols)

| Symbol | Latency | candles | sentiment | news |
|---|---|---|---|---|
| SPY  | 2.59s | OK | OK | 5 items |
| IBM  | 1.72s | OK | OK | 5 items |
| NVDA | 1.78s | OK | OK | 5 items |
| MMM  | 1.95s | OK | OK | 5 items |
| XOM  | 1.82s | OK | OK | 5 items |
| ORCL | 2.10s | OK | OK | 5 items |

**All 3 fields populated on every symbol. Cold wall under 3s (the news endpoint's 1.7s scrape is the new floor).**

### Warm path (repeat hover within 60s)

`~135ms` consistent across all symbols — server cache + client cache both hit.

---

## Commits on `origin/main` (in dependency order)

```
bb27770 feat(frontend): HM-BJ.E4 — tooltip single-fetch via /api/symbol/{sym}/scorecard (Polygon-backed, ~6x faster cold-path)
10368c9 feat(market_data): HM-CB — Polygon Stocks Starter as primary candles source (Alpaca + yfinance fallbacks)
dfab0c5 feat(market_data): HM-CA — Alpaca-first candles fetch, Yahoo fallback
2b367ab fix(api): HM-BJ.E4 — use module-level executor; ThreadPoolExecutor.__exit__ was blocking 26s
5938e17 fix(api): HM-BJ.E4 — per-sub-fetch timeouts to keep cold path under 3s
823a4b6 fix(api): HM-BJ.E4 — add missing console import (was NameError'ing in scorecard handler)
ca855e6 fix(api): HM-BJ.E4 — null slow sub-fetches instead of 500ing whole scorecard
2893af8 feat(api): HM-BJ.E4 — /api/symbol/{sym}/scorecard aggregator (parallel internal fetches, 60s cache)
```

The earlier 5 backend commits (`2893af8` → `5938e17` → `823a4b6` → `ca855e6` → `2b367ab`) ship the aggregator + its fixes. They're load-bearing — they made the frontend swap possible.

`dfab0c5` HM-CA remains a useful safety net: if Polygon ever 5xxs, the cascade falls to Alpaca before Yahoo.

---

## Captain browser verification (CB.4)

Per the memory #27 rule learned from the HM-BJ.E2 revert: frontend changes must be browser-tested against the running dashboard BEFORE the commit lands on `main`.

- Local edit applied (`dashboard/static/index.html` unstaged)
- FileResponse served the edit on Captain's hard-refresh
- Captain confirmed live: tooltip renders with chart + sentiment + news within ~2s on first hover; instant on repeats; click/shift-click/right-click/arrow-key behaviors intact; DevTools Network tab confirmed single `/api/symbol/SYM/scorecard` call (vs. prior three separate calls)
- ONLY THEN was the frontend commit (`bb27770`) staged + pushed

This is the durable pattern going forward. HM-BJ.E2 burned hours on a runtime bug that `node --check` missed; HM-BJ.E4 took 4 backend revisions to harden but the frontend swap landed clean because Captain saw it work in a real browser first.

---

## Open items (none required, all informational)

1. **CLAUDE.md update** — the "Polygon Options Starter $29/mo APPROVED IN PRINCIPLE, not yet activated" line is stale (Polygon Stocks + Options both paid + active). Flag for next CLAUDE.md edit.
2. **HM-CA** is now load-bearing as the Polygon→Alpaca fallback tier. Don't remove it. The cascade is by design.
3. **HM-BD.H** (cache-mystery from MONSTER 1) still parked. Not blocking anything.
4. **HM-BD.F-audit Tier-2** (L1038 + L1062 silent-pass sites) still optional from MONSTER 1.

---

## ntfy events

- HM-CA shipped — Alpaca-first (then diagnosed as insufficient)
- HM-CB shipped — Polygon primary, cold path FIXED
- HM-BJ.E4 frontend re-applied locally — Captain browser test
- 🏁 HM-CB + HM-BJ.E4 complete (sending now)

---

## What this thread proved

- The HM-BJ.E4 backend architecture (single-aggregator + `@timed_cache` + `ThreadPoolExecutor`) was correct from the start.
- The 3s cold-path target was achievable ONLY after the upstream candles endpoint was fixed. The slow path was never in the aggregator; it was always in `get_intraday_candles`.
- Polygon Stocks Starter is a load-bearing upstream now. Worth the $29/mo.
- Memory #27 (browser-test-before-ship for frontend) earned its keep this thread.
