# 🔧 HM-BJ.E4 — Discovery + Implementation Report

**Author:** Scotty (Opus 4.7)
**Date:** 2026-05-12
**Status:** E4.0 → E4.1 → E4.2 in progress; halt for Captain at end of E4.2

---

## E4.0 — Discovery findings

### Tooltip's current parallel fetches (3 endpoints)

From `dashboard/static/index.html` lines 1883–1891 (`fetchScorecard`):

| # | URL | Backing function (dashboard/app.py) | Returns |
|---|---|---|---|
| 1 | `/api/market/candles/${sym}?limit=30` | `market_candles(symbol)` L3823 | `{candles: [...], markers: [...]}` |
| 2 | `/api/market/sentiment/${sym}` | `symbol_sentiment(symbol)` L4809 | sentiment dict (varies) |
| 3 | `/api/news/${sym}` | `symbol_news(symbol, limit=10)` L3318 | `{news: [...]}` / news dict |

(Note: the `?limit=30` query on `/api/market/candles` is ignored by the endpoint — it accepts `interval` and `range`, not `limit`. The tooltip has been passing a no-op param.)

### Current behavior characterization

- Client-side, the 3 fetches go in parallel via `Promise.all` (line 1888)
- Each one is a separate HTTP round-trip — cold-path ~3s wall time per Captain
- Sub-fetches share no caching between each other (different cache TTLs / endpoints)
- No graceful degradation: if `Promise.all` resolves with one null, `renderTip` handles it; if a fetch throws, the entire scorecard fails

### Parallelization choice

All three backing functions are **sync** (not async). `concurrent.futures.ThreadPoolExecutor` is the right fit — `asyncio.gather` would require rewriting the endpoint chain. ThreadPoolExecutor with `max_workers=3` parallelizes the three sub-fetches internally; the overall response time is `max(t1, t2, t3) + overhead`, not `t1+t2+t3`.

### Cache strategy

`@timed_cache(60)` mirrors the existing pattern (dashboard/app.py:380). 60s TTL means a chip hovered repeatedly in a 1-min window pays only the first cold-path cost; subsequent hovers (any user, any session) hit the in-memory cache in <10ms. HM-BD.G already fixed the cache-timestamp-at-completion bug, so this works correctly for slow sub-fetches.

### Aggregator response shape

```json
{
  "candles": {"candles": [...], "markers": [...]} | null,
  "sentiment": {sentiment payload} | null,
  "news": {news payload} | null
}
```

Each sub-key is null on per-sub-fetch failure (typed catch). Frontend reads `data.candles.candles`, `data.sentiment.bulls`, etc. — same shape as today, just under the new top-level keys.

### Mount location

After `market_candles` (line 3846), before `market_heatmap` (line 3849). Pairs naturally with the candles endpoint.

### Anchor

`# === HM-BJ.E4 ===` / `# === /HM-BJ.E4 ===` around the endpoint definition.

---

## E4.1 — Implementation

(Filled in after commit.)

---

## E4.2 — Timing measurements

(Filled in after restart + smoke.)
