# HM-BD.G + HM-BD.E Discovery — Cache Fix + Scanner Rewrite

**Phase BDGE.0 — read-only profiling. No code edits, no DB writes, no restart.**
**Date:** 2026-05-11
**Engineer:** Scotty (Opus 4.7)

---

## Pre-flight summary
- Recent commits in `origin/main`: HM-BHBI hotfix (`9c58d9e`), HM-BH (`c1befbc`), HM-BD.1 (`bcb3bca`).
- Service alive (trader bridge PID 85371, port 8080).
- Working tree clean (no tracked diffs; 40 untracked items are prior docs/backups).

---

# HM-BD.G — `@timed_cache` completion-timestamp fix

## 1. The decorator (current, buggy)

`dashboard/app.py:380-396`:
```python
def timed_cache(seconds: int):
    """Cache endpoint response for N seconds. Preserves function signature for FastAPI."""
    def decorator(func):
        @_functools.wraps(func)
        def wrapper(*args, **kwargs):
            key = f"{func.__name__}:{args}:{kwargs}"
            now = _time.time()                              # ← captured at CALL START
            entry = _endpoint_cache.get(key)
            if entry and (now - entry["time"]) < seconds:
                return entry["data"]
            result = func(*args, **kwargs)
            _endpoint_cache[key] = {"time": now, "data": result}   # ← writes call-start time
            return result
        wrapper.__signature__ = _inspect.signature(func)
        return wrapper
    return decorator
```

**The bug:** `now = _time.time()` is captured at the wrapper's entry. After `func(*args)` runs (which may take minutes — see HM-BD.0 cold-path), the entry is stored with `time = now` (call start). Subsequent callers compute `(_time.time() - entry["time"]) < seconds`. If the function ran longer than `seconds`, the cache is "expired" the instant it's written. Every caller re-runs the work.

This is exactly what BD.0 observed: three sequential cold-path calls each took 6–10 min because each call entered a "cache empty" state at start, ran a fresh scan, and the previous cache write was already past TTL.

## 2. The fix (one-character semantic change)

Move the `_time.time()` call to AFTER `func()` returns. Two cheap clock reads instead of one:

```python
def wrapper(*args, **kwargs):
    key = f"{func.__name__}:{args}:{kwargs}"
    entry = _endpoint_cache.get(key)
    if entry and (_time.time() - entry["time"]) < seconds:
        return entry["data"]
    result = func(*args, **kwargs)
    _endpoint_cache[key] = {"time": _time.time(), "data": result}
    return result
```

Same staleness check (`_time.time()` against `entry["time"]`); the entry's timestamp is now "moment of completion," giving callers the full `seconds` TTL of validity after the result was produced.

## 3. Caller impact analysis

**30 callers** of `@timed_cache` in `dashboard/app.py` (no callers in `engine/` or `main.py`):

| TTL | Count of endpoints |
|---|---|
| 60s | 2 |
| 120s | 6 |
| 300s | 12 |
| 600s | 7 |
| 3600s | 3 |

All callers are FastAPI GET endpoints serving the dashboard. The semantic each one wants is **"don't recompute this for N seconds after I produced a value"** — the standard read-through cache pattern.

Reverse case: would any caller WANT "cache for N seconds starting when generation BEGINS"? That semantic would gate frequency-of-execution (run no more than once per N seconds, even if generation takes longer). For that you'd use a scheduler with a guard, not a cache. None of the 30 endpoints fit that pattern — they're all "expensive read, please coalesce."

## 4. Captain question Q1
**Recommended:** ship the completion-time fix.

Q1 is essentially: "Is there any caller whose correctness depends on call-start timing?" Answered above — no. Even if one did, the safer fix-forward path is to make that endpoint use a scheduler instead of misusing the cache.

---

# HM-BD.E — `scan_premarket_gaps()` batched rewrite

## 1. Current implementation (`engine/premarket_scanner.py:19-68`)

Serial for-loop over `config.get_effective_watchlist()` (668 symbols), two yfinance/Alpaca calls per symbol:
- `get_stock_price(symbol)` → premarket price
- `get_alpaca_bars(symbol, days=2)` → prev_close

= ~1,336 sequential network calls cold. Observed 6+ min cold-path.

Output entry shape:
```python
{
    "symbol":          str,
    "prev_close":      float (rounded 2dp),
    "premarket_price": float (rounded 2dp),
    "gap_pct":         float (rounded 2dp),
    "direction":       "gap_up" | "gap_down",
    "scanned_at":      ISO timestamp,
}
```

Filter: `abs(gap_pct) >= 2.0`. Sorted by `abs(gap_pct)` desc. Saved to `data/premarket_gaps.json` via `_save_gaps(gaps)`.

## 2. The replacement primitive

`engine/market_data.py::get_bulk_snapshots(symbols)` — single batched Alpaca multi-symbol-snapshot call. Returns per symbol:
```python
{
    "symbol":     str,
    "last_price": float,       # from latestTrade.p  ← serves as premarket_price
    "open_price": float,       # dailyBar.o
    "high":       float,
    "low":        float,
    "volume":     int,
    "prev_close": float | None,# from prevDailyBar.c ← exactly what we need
    "ts":         str,
}
```

Both fields we need (`prev_close`, premarket-price-equivalent via `last_price`) are present. No need to invent or derive. The `/api/momentum/premarket` endpoint (in `engine/momentum/premarket.py`) already proves this works end-to-end at ~0.6s for the full 668-symbol universe.

## 3. Universe parity (Q2 evidence)

Live probe:
```
config.get_effective_watchlist():       N=668, first5=['NVDA','TSLA','META','WMT','LLY']
engine.universe.get_active_universe():  N=668, first5=['NVDA','TSLA','META','WMT','LLY']
identical: True
in_one_not_other: 0
```

**The two universes are the same set today.** Switching from `config.get_effective_watchlist()` to `engine.universe.get_active_universe()` is functionally a no-op but provides symmetry with `/api/momentum/premarket` (which uses `get_active_universe()`). Same canonical source for both scanners going forward.

## 4. Gap filter (Q3 evidence)

Current legacy scanner: `if abs(gap_pct) < 2.0: continue` — 2% threshold, no volume filter.

`/api/momentum/premarket` (different concern, different endpoint): `MIN_GAP_PCT=3.0 AND volume >= 50_000` — more selective.

HM-BD.E's mandate is to preserve `/api/premarket-gaps` behavior for the dashboard frontend. The dashboard expects 200+ rows under normal market conditions; tightening to 3%+50k would drop it to ~10 rows and break the UI.

**Recommendation Q3:** preserve current `|gap_pct| >= 2.0` threshold, no volume filter.

## 5. Side effects to preserve

`_save_gaps(gaps)` writes `data/premarket_gaps.json`. Two other functions in the same file consume that JSON file:
- `analyze_gaps_with_ai()` → `_load_gaps()` → reads the JSON
- `get_dayblade_gap_candidates()` → `_load_gaps()` → reads the JSON

The rewrite MUST keep calling `_save_gaps(gaps)` at the end, with the same shape, or these downstream functions break.

## 6. Proposed rewrite

```python
def scan_premarket_gaps() -> list:
    """Scan get_active_universe() for pre-market gaps vs previous close.

    HM-BD.E: rewritten over engine.market_data.get_bulk_snapshots() — one
    batched Alpaca multi-symbol-snapshot call instead of 668×2 serial
    yfinance+Alpaca-bars fetches. Cold-path collapsed from ~6 min to <1s.

    Filter: |gap_pct| >= 2.0. Sort: by abs(gap_pct) desc.
    Output shape preserved for dashboard frontend + _save_gaps consumers.
    """
    from engine.market_data import get_bulk_snapshots
    from engine.universe import get_active_universe

    universe = get_active_universe()
    if not universe:
        return []

    snapshots = get_bulk_snapshots(universe)
    if not snapshots:
        console.log("[red]Premarket scan: get_bulk_snapshots returned empty")
        return []

    gaps = []
    scanned_at = datetime.now().isoformat()
    for symbol, snap in snapshots.items():
        prev_close = snap.get("prev_close")
        premarket_price = snap.get("last_price")
        if not prev_close or not premarket_price or prev_close <= 0:
            continue
        gap_pct = round(((premarket_price - prev_close) / prev_close) * 100, 2)
        if abs(gap_pct) < 2.0:
            continue
        gaps.append({
            "symbol": symbol,
            "prev_close": round(prev_close, 2),
            "premarket_price": round(premarket_price, 2),
            "gap_pct": gap_pct,
            "direction": "gap_up" if gap_pct > 0 else "gap_down",
            "scanned_at": scanned_at,
        })

    gaps.sort(key=lambda x: abs(x["gap_pct"]), reverse=True)
    _save_gaps(gaps)
    return gaps
```

~25 LOC (replacing ~50). Imports change (drop `get_stock_price`, `get_alpaca_bars`; add `get_bulk_snapshots`, `get_active_universe`). Output shape, sort order, side effects all preserved.

## 7. Behavior diffs to flag

- **Universe source:** `config.get_effective_watchlist()` → `engine.universe.get_active_universe()`. Verified identical today (668 symbols each, same set). Going forward, only `get_active_universe()` will be honored — if the two sources ever diverge (e.g. someone adds a config-watchlist override), the scanner will follow universe, not config.
- **Data source for prices:** previously hybrid (yfinance for current price, Alpaca for bars). Now Alpaca-only via batched snapshots. Same data source `/api/momentum/premarket` already uses successfully.
- **Off-hours behavior:** `latestTrade.p` outside market hours returns the last-trade price (which post-market = closing-or-extended; in pre-market = pre-market last). Same shape as before — `get_stock_price()` had the same behavior for the same reason.

---

# Captain decisions blocking BDGE.1/.2

| Q | Question | Scotty's recommendation |
|---|---|---|
| **Q1** | Cache completion-time fix safe across 30 callers? | **Yes — ship it.** No caller wants call-start timing semantics. |
| **Q2** | Universe source: `get_effective_watchlist()` or `get_active_universe()`? | **`get_active_universe()`** — verified identical today, symmetry with `/api/momentum/premarket`. |
| **Q3** | Gap filter threshold? | **Preserve `|gap_pct| >= 2.0`, no volume filter** — keeps dashboard UI rowcount intact. |

---

**HALT — awaiting Captain decision on Q1, Q2, Q3 before BDGE.1.**

---

# Captain Decisions (received 2026-05-11)

- **Q1:** YES — cache completion-time fix.
- **Q2:** `get_active_universe()` — universe parity verified, symmetry with `/api/momentum/premarket`.
- **Q3:** Preserve `|gap_pct| >= 2.0` threshold, no volume filter.

Inline workflow per directive.

---

## HM-BDGE Closure

### HM-BD.G outcome (commit d8ac548)
`dashboard/app.py:380-400`: moved the cache-entry timestamp from call-start to completion. Two `_time.time()` reads (one for staleness check, one for entry write) instead of one. Same staleness semantics, but slow funcs now actually warm their cache.

Diff: +9 / -4 lines.

### HM-BD.E outcome (commit 448b7b3)
`engine/premarket_scanner.py:19-78`: rewrote `scan_premarket_gaps()` over `engine.market_data.get_bulk_snapshots()`. One batched Alpaca multi-symbol-snapshot call replaces ~1336 sequential round-trips.

**Direct smoke test** (venv/bin/python3, post-edit, pre-restart):
```
duration: 1.101s
gaps: 229
sample: {'symbol': 'AAOX', 'prev_close': 45.16, 'premarket_price': 65.88, 'gap_pct': 45.88, 'direction': 'gap_up', 'scanned_at': '2026-05-11T20:39:33.766497'}
max abs gap: 45.88%
```

Preserved: output shape, sort order, 2% threshold, `_save_gaps()` side effect.
Changed: universe source (`config.get_effective_watchlist()` → `engine.universe.get_active_universe()`, verified-identical sets); data source (hybrid yfinance+Alpaca → Alpaca-only batched).

Diff: +45 / -36 lines.

### Combined effect
With both commits in place:
- Scanner generation: ~1s (was ~6 min) → fits trivially within any `@timed_cache(300)` TTL.
- Cache writes happen AT completion → first warm-up actually warms.
- Second caller within 5min: returns from in-memory cache → sub-100ms expected.

### Commits to push
```
448b7b3 perf(scanner): HM-BD.E — rewrite scan_premarket_gaps over get_bulk_snapshots (kills cold path)
d8ac548 fix(cache): HM-BD.G — write @timed_cache entry at completion time, not call-start
```

### Lesson reapplied from HM-BHBI
This epic touched type annotations exactly zero times — neither sub-epic needed `int | None` style unions. Both edits use bare `def name() -> list:` (preserved from original) and plain dict access. No PEP 604 risk. Compile checks ran against `venv/bin/python3` (Py3.9.6 — the launchd interpreter) and confirmed clean.

### Out-of-scope follow-ups (parked)
- **HM-BD.F** — loud-fail logging at the bare-except wrappers in `engine/ai_brain.py` (lines 911 + 968-ish). Still open from HM-BD epic.
- **HM-BE-suffix** — rename the `qwen3-14b-pro` id itself (HM-BE narrowed only the display labels).
- **HM-BE-historic** — backtest config files alignment.
- **HM-BH-warn** — second tier `SWAP_WARN_PCT` if operator wants more granular notification.
