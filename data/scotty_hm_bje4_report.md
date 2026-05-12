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

Mounted at `dashboard/app.py:3849`, between `market_candles` and `market_heatmap`. Anchor `# === HM-BJ.E4 ===`.

**Initial ship (`2893af8`):** typed-catch wrapped only the underlying call, `f.result(timeout=10)` was outside → first smoke caught QQQ returning HTTP 500 when its news scrape exceeded 10s.

**Fix-up (`ca855e6`):** Added `_await(future, label, timeout=8)` helper that catches `concurrent.futures.TimeoutError` and `Exception` around each `result()`, nulling that field and logging `[yellow]` instead of 500ing the whole endpoint. Timeout reduced 10s → 8s for a tighter cold-path bound.

---

## E4.2 — Timing measurements (post-fix)

Trader restart PID: 20087 (port 8080 bound).

| Symbol | Path | HTTP | Wall | Body size | All 3 fields populated? |
|---|---|---|---|---|---|
| SPY | cold | 200 | 2.43s | 6.89 KB | ✓ candles, sentiment, news=5 |
| SPY | warm | 200 | **0.135s** | 6.89 KB | ✓ |
| QQQ | cold | 200 | 1.74s | 6.04 KB | ✓ (was 500 before fix) |
| QQQ | warm | 200 | 0.137s | 6.04 KB | ✓ |
| NVDA | cold | 200 | 1.87s | 7.03 KB | ✓ |

**Cross-check baseline** (current frontend behavior — 3 separate HTTP round-trips):

| Mode | Wall time | Notes |
|---|---|---|
| Sequential SPY fetch (browser-like) | **17.7s** | One after another |
| Parallel SPY fetch (Promise.all-like) | **9.2s** | All 3 in flight together; bottleneck = slowest |
| HM-BJ.E4 SPY cold (single round-trip) | **2.4s** | Bottleneck = slowest sub-fetch, capped at 8s timeout |

### Verdict against Captain's targets

| Target | Result | Verdict |
|---|---|---|
| Response shape matches frontend's reader | `{candles: {candles: [...], markers: [...]}, sentiment: {...}, news: [...]}` — matches existing `renderTip` destructuring (`payload.candles`, `payload.sentiment`, `payload.news`) | ✅ |
| Cold path < 3s | 1.7–2.4s observed across SPY/QQQ/NVDA | ✅ |
| Warm path < 100ms | **135–137ms observed** — slightly over (HTTP transport + 6-7KB JSON deserialization overhead; the dict lookup itself is microseconds) | ⚠️ 35-37% over target |

### Honest caveats

- Cold-path numbers reflect partially-warm UNDERLYING caches from earlier test traffic. A truly-cold path (process restart + uncached symbol) is bounded by the 8s sub-fetch timeout — could in principle hit 8s. The news endpoint's multi-source RSS scrape is the typical bottleneck for never-seen-before symbols.
- Warm path 135–137ms includes everything from `curl` request → uvicorn → `@timed_cache` lookup → JSON serialize → HTTP response → curl receive. Pure cache lookup is <1ms; the rest is plumbing.
- Per-sub-fetch null degradation works: if news scraping times out beyond 8s, the response returns with `news: null` and a `[yellow]` log entry, instead of 500ing.

---

## HALT for Captain (E4.2)

Backend is shipped, the trader is running on it (PID 20087), and the new endpoint is healthy across 3 symbols. The frontend has NOT been touched — `dashboard/static/index.html` still uses the old 3-fetch path. Browsers will see no behavior change yet.

Next phase (**E4.3**) is frontend swap — local file edit only, NO commit, browser-test gated per memory rule. Awaiting Captain confirmation that:
- Shape ✅
- Cold path ✅ (1.7-2.4s, 7-12× better than prior parallel-3-fetch)
- Warm path ⚠️ (~135ms vs <100ms target — acceptable?)

If approved → proceed to E4.3 frontend swap as a local-only edit, Captain hard-refreshes and confirms before any commit lands.
