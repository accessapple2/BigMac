# HM-BD Discovery — Premarket-Gaps Cold-Path Latency

**Phase BD.0 — read-only profiling. No code edits, no DB writes, no restart.**
**Date:** 2026-05-11
**Engineer:** Scotty (Opus 4.7)

---

## 1. Cold-path timing

| Call | Wall time (initial observation) | Wall time (final, after task completion) | State |
|------|---------------------------------|------------------------------------------|-------|
| 1    | ≥6 min, still running           | **10 min 24.65 s**                       | Cold (cache empty) |
| 2    | not started (blocked on 1)      | **8 min 53.57 s**                        | Cold (cache expired between calls — see anomaly below) |
| 3    | not started (blocked on 1)      | **6 min 30.84 s**                        | Cold (cache expired between calls) |

**Cache anomaly (discovered post-completion):** All three sequential calls were cold-path slow. The `@timed_cache(300)` decorator at `dashboard/app.py:380-396` writes the cache entry only at `t = call_finish` with timestamp `t = call_start`. Because each call took >300s to complete, by the time the next call arrived the cache age (`now - entry["time"]`) already exceeded the 300s TTL → cache miss → full re-scan. Net effect: **the `@timed_cache(300)` on `/api/premarket-gaps` provides zero benefit** — it's structurally broken for any endpoint whose generation cost exceeds its TTL. This makes HM-BD.E (dashboard cold-path fix) materially more urgent than the original triage suggested: the dashboard frontend is paying full cold-path cost on every poll.

Row count from call 1 sanity: 291 gap rows (varies slightly per call as more symbols become eligible during the 25-min window).

The first cold call exceeded both ai_brain.py timeouts (10s on L898, 5s on L927) **by 36×–72× at minimum** and was still hung when the report was written. /tmp/pmg_1.json is 0 bytes after >6 minutes. Both internal callers WILL HTTP-000 every time the cache is cold.

Once the call eventually completes, the `@timed_cache(300)` decorator on the endpoint will hold the result for 5 minutes, then re-cold. The ai_brain in-process `_premarket_gap_cache` (TTL 300s, `engine/ai_brain.py:22`) shares the same TTL, so cold re-emergence is structural, not transient.

For comparison: a side-by-side fetch of `/api/momentum/premarket?force=true` over the **same universe via batched Alpaca snapshots** returned 10 hits in **0.63 seconds** — ≥570× faster than the legacy path.

---

## 2. Scanner anatomy — where the seconds go

`engine/premarket_scanner.py::scan_premarket_gaps()` (lines 19–68):

```python
for symbol in config.get_effective_watchlist():    # N=668
    data = get_stock_price(symbol) or {}            # 1 network call
    bars = get_alpaca_bars(symbol, days=2)          # 1 network call
    # ... compute gap, append if |gap| > 2% ...
```

| Metric | Value |
|--------|-------|
| Symbols scanned | **668** (not 244 — 244 is the filtered output rowcount) |
| Source | `config.get_effective_watchlist()` |
| Fetch pattern | **Serial for-loop, two sequential network calls per symbol** |
| Concurrency | **NONE** — no `ThreadPool`, no `asyncio`, no batched fetch |
| Yahoo throttle | `_yahoo_lock` + `_YAHOO_MIN_GAP` further serialise yfinance calls |
| Network ops cold | **≈ 1,336 sequential round-trips** (668 × 2) |

Hotspot: the serial for-loop. A 1,336-RTT serial scan at even ~250 ms median latency is ~5.5 min — matches the observed ≥6 min cold path. No "N+1 surprise" — the entire design is N+1.

---

## 3. The two ai_brain.py callers

| # | Function context | Line | Timeout | Use of response |
|---|------------------|-----:|--------:|-----------------|
| A | Per-agent scan loop, before symbol analysis. Refreshes shared `_premarket_gap_cache` (300s TTL) if stale. Iterates **every gap** and injects into each agent's prompt as `=== PRE-MARKET GAPS ===` context. | 898 | **10 s** | `g['symbol']`, `g.get('gap_pct',0)`, `g['prev_close']`, `g['premarket_price']`, `g.get('odte_candidate')` |
| B | `dayblade-sulu` focused-scan branch — builds `_gap_syms` set to expand DayBlade's scan universe to gap movers. | 927 | **5 s** | Only `g['symbol']` and `g.get('gap_pct', 0)` (filter `> 2`) |

**Key field requirements (union of both callers):** `symbol`, `gap_pct`, `prev_close`, `premarket_price`, and an optional `odte_candidate` boolean (caller A uses `.get` with default falsy — adapter can simply omit it).

Both callers catch `Exception` silently (bare `except Exception: pass` on A line 911, `except Exception: pass` on B line 929). The HTTP-000 on timeout produces no log, no NTFY, no record — silent feature loss in every cold cycle.

---

## 4. /api/momentum/premarket — alternative shape

Source: `engine/momentum/premarket.py::compute_premarket()` (143 LOC). Reads the **same `get_active_universe()` as the legacy scanner**. One batched call: `engine.market_data.get_bulk_snapshots(universe)` → single Alpaca multi-symbol-snapshot request.

Forced response (off-hours preview, `?force=true&limit=10`):

```json
{
  "ts": "2026-05-11T...Z",
  "window_state": "AFTER",
  "hits": [{
    "rank": 1,
    "ticker": "AAOX",                  ← maps to ai_brain "symbol"
    "gap_pct": 45.88,                  ← exact match
    "prev_close": 45.16,               ← exact match
    "premarket_price": 65.88,          ← exact match
    "premarket_volume": 81716,         ← extra (useful)
    "flags": [],                       ← extra (currently always [])
    "direction": "UP",                 ← was "gap_up"/"gap_down" in legacy
    "market_status": "AFTER"           ← extra
  }, ...]
}
```

| Aspect | Verdict |
|--------|---------|
| Symbol universe | **Same** — both call `get_active_universe()` |
| Field coverage | **All 5 fields ai_brain needs are present** (ticker→symbol rename) |
| `odte_candidate` field | **Absent in new shape**, but caller A reads it via `.get` with falsy default — no behavior change |
| Window gate | New endpoint returns `hits=[]` outside 04:00–09:30 ET unless `force=True`. **For ai_brain consumers we want force=True** because their scan loops fire all day. |
| Adapter feasibility | **Trivial** — ~15-line shape converter (`ticker→symbol`, `direction→gap_up/gap_down`). Single helper in `engine/ai_brain.py` or new `engine/premarket_gaps_adapter.py`. |
| Cold-path latency | **~0.6 s** measured live, vs ≥6 min for legacy. |

---

## 5. Existing cache mechanics + pre-warm precedents

### Cache layers
| Layer | TTL | Location | Notes |
|-------|----:|----------|-------|
| FastAPI endpoint | 300s | `@timed_cache(300)` on `premarket_gaps()` (dashboard/app.py:11070) | Shared across all callers |
| ai_brain in-process | 300s | `_premarket_gap_cache` (`engine/ai_brain.py:22-23`) | Per-process; resets on restart |

Both TTLs identical → no useful staggering. After 5 min idle, both expire simultaneously and the next caller pays full cold-path cost.

### Pre-warm precedents in the codebase
- `engine/gex_scanner.py::refresh_gex_cache()` — designed to be called by `schedule.every(15).minutes.do(run_gex_refresh)` (main.py:2707). Calls `get_all_gex(force=True)` to repopulate cache. Skipped outside market hours.
- `main.py:2652-2660` — `_startup_market_backfill` thread fires at boot via `threading.Thread(target=..., daemon=True).start()` and runs `backfill_market_history(days=365)`.
- **No `@app.on_event("startup")` or `lifespan` handler exists anywhere** — startup hooks live in `main.py` between `app` creation and the `schedule.every(...)` block.

α-path precedent therefore exists and would mirror `refresh_gex_cache` + the `_startup_market_backfill` thread pattern almost line-for-line.

---

## Recommended path — **γ (migrate callers) with α as optional follow-up**

### γ wins on three structural dimensions

1. **Latency.** /api/momentum/premarket returns in ~0.6s today, measured live. Even with α (pre-warm), the legacy endpoint stays slow on every 5-min-stale recurrence and re-cold cases. γ removes the slow path from the critical loop entirely.

2. **Scope isolation.** /api/premarket-gaps continues to serve the dashboard frontend untouched. No UI regression risk. Only the two internal ai_brain callers move.

3. **Permanent vs band-aid.** α only fixes cold-path *at restart*. After 5min idle (TTL), the cache goes stale; the next caller pays full cost again. ai_brain cycles can be sparse (DayBlade dormancy, weekends, off-hours). γ uses an endpoint that is already fast every time.

### Why not α (pre-warm)
- Cache TTL (300s) is shorter than common idle gaps. Pre-warming at startup fixes only the first window; subsequent stales remain slow.
- A scheduled refresher (à la `refresh_gex_cache` every 5 min) would work, but it pays the 6-min cold cost in a background thread every 5 minutes — wasteful, and competes with the actual scan threads for the same yfinance/Alpaca quota during market hours.
- Mitigates symptom, not cause: the structural bug is the 668-symbol serial fetch.

### Why not β (reduce scanner workload)
- The 668-symbol universe is **the active research universe** consumed by all fleet agents. Trimming it removes signal coverage we explicitly want.
- The dashboard frontend (UI consumer at the same endpoint) also presents the full gap list; cutting it changes behavior for users.
- The real architectural answer is "use the batched API." That's already implemented as /api/momentum/premarket. β re-invents what γ leverages.

### γ implementation sketch (preview only — not for execution this phase)
- Single new helper, e.g. `_fetch_premarket_gaps_via_momentum()` in `engine/ai_brain.py`, that:
  - GETs `/api/momentum/premarket?force=true&limit=100`
  - Maps each `hit` → `{symbol: hit["ticker"], gap_pct, prev_close, premarket_price, odte_candidate: False}`
  - Returns the list
- Replace `_req.get("...premarket-gaps", timeout=10)` at L898 with the helper.
- Replace `_req2.get("...premarket-gaps", timeout=5)` at L927 with the same helper (caller B only needs symbol+gap_pct).
- Both timeouts can be reduced (e.g., 5s and 3s) since the new endpoint reliably returns <1s.
- Anchor: `# === HM-BD.1 γ migration ===`
- Single commit, single file touched, fully revertable.
- Estimated diff: +25/-4 lines in `engine/ai_brain.py`.

### Optional α follow-up
If, after γ ships, the dashboard frontend's first load of /api/premarket-gaps still produces user-visible lag (it currently does — same 6-min cold-path), a minimal pre-warm thread can be added at startup mirroring `_startup_market_backfill`. This is **not blocking** and should be a separate decision; the ai_brain callers no longer care.

---

## Captain decisions blocking BD.1

**Q1 — Pick α / β / γ / hybrid?**
*Scotty's recommendation: γ.* The two ai_brain callers are best-served by an already-existing fast endpoint. The legacy endpoint stays put for the dashboard UI.

**Q2 — Adapter location: new file or inline in `engine/ai_brain.py`?**
*Scotty's recommendation: inline helper in `engine/ai_brain.py`.* Only two call sites, both in the same file; a separate module is overkill and would dilute the diff. If a future caller emerges we can extract.

**Q3 — Reduce the two timeouts (10s → 5s, 5s → 3s) at the same time?**
*Scotty's recommendation: yes.* The new endpoint reliably returns under 1s; tighter timeouts surface regressions faster. Belt-and-braces.

**Q4 — Open a parallel α ticket as out-of-scope follow-up for dashboard frontend cold-path?**
*Scotty's recommendation: defer.* Park as an open follow-up in HM-BD.D closure; do not bundle.

---

## Operational note

A cold curl from BD.0 timing bucket 1 (PID 82403) is still running against the service after 6+ minutes — not killed, will eventually complete and warm `@timed_cache(300)` + ai_brain `_premarket_gap_cache`. Killing it would suppress the evidence and not change the conclusion. Captain may ignore or `kill 82403` if desired.

**HALT — awaiting Captain decision on Q1–Q4 before BD.1.**

---

# Captain Decisions (received 2026-05-11)

- **Q1:** γ approved.
- **Q2:** inline helper in `engine/ai_brain.py` approved.
- **Q3:** tighten timeouts (10s→5s, 5s→3s) approved.
- **Q4:** dashboard-UI α follow-up parked as HM-BD.E (out of scope this round).
- **Bonus:** instrument the bare-except wrappers at the call sites → parked as HM-BD.F (out of scope this round).

---

## HM-BD Closure

### Path taken
**γ (migrate callers to `/api/momentum/premarket`).** Both `engine/ai_brain.py` consumers swapped to the batched-Alpaca endpoint via inline adapter `_fetch_premarket_gaps()`. Legacy `/api/premarket-gaps` left untouched — dashboard frontend unaffected. Caller timeouts tightened (10s→5s, 5s→3s).

### Commits staged (not pushed)

```
bcb3bca refactor(ai_brain): HM-BD.1 — migrate premarket callers to /api/momentum/premarket
```

Single commit, single file (`engine/ai_brain.py`), +45 / -6 lines. Fully revertable via `git revert bcb3bca`.

### Static verification (BD.C)
- `main.py`, `engine/ai_brain.py`, `engine/premarket_scanner.py`, `dashboard/app.py` all compile clean.
- 4 HM-BD anchors present in `engine/ai_brain.py` (lines 25, 60, 934, 962).
- No remaining `/api/premarket-gaps` HTTP call sites in `engine/ai_brain.py` (only doc comments remain).

### Live timing comparison (BD.C, pre-restart)
- **Legacy `/api/premarket-gaps`** — STILL timing out at 30s curl cap (cache cold-replay from BD.0 not yet completed). Confirms the original problem persists for any caller still using this endpoint.
- **New `/api/momentum/premarket?force=true&limit=100`** — returned 100 hits in **0.616 s**. Confirms the new caller path is fast and stable.
- **Effective speedup observed:** ≥50× (truncated by the 30s curl cap on legacy; true ratio is ≥570× from BD.0's 6+ min vs new 0.6s).

### Expected cold-path behavior post-restart
After Captain restarts the service:
1. ai_brain.py is reloaded — the `_fetch_premarket_gaps` helper and migrated call sites become live.
2. The `_premarket_gap_cache` in-process cache also resets (still 300s TTL), but cold-path repopulation now hits `/api/momentum/premarket` instead of `/api/premarket-gaps` → ~0.6 s instead of ≥6 min.
3. Both ai_brain callers now complete inside their tightened timeouts (5s / 3s) with substantial headroom.
4. No more HTTP-000 silent failures from these two sites.
5. Dashboard frontend behavior unchanged — `/api/premarket-gaps` still serves it (with its existing cold-path slowness, parked as HM-BD.E).

### Restart needed
**Yes** — Captain runs `launchctl kickstart -k gui/$(id -u)/com.trademinds.trader`.

### Verification plan post-restart
1. Wait for service ready, then `time curl -s "http://localhost:8080/api/momentum/premarket?force=true" >/dev/null` — should be <1 s (sanity).
2. Tail `logs/trader.log` for the line `premarket gaps fetch failed:` — should be absent. If it appears, the helper's logging captured a real failure (and is no longer silent — improvement over the bare-except status quo).
3. Tail `logs/trader.log` for `=== PRE-MARKET GAPS (today) ===` — should appear in agent scan output during a normal cycle when momentum/premarket has hits (i.e., during the 04:00–09:30 ET pre-market window or any time `force=true` is honored).
4. If any agent cycle log shows `Sulu DayBlade: focused scan on N stocks: ...`, the gap_syms set is being populated successfully from the new endpoint.
5. After 5 min idle + a fresh cycle, the in-process cache repopulation should still be <1 s (confirms TTL re-fetch also hits the fast endpoint).

### Out-of-scope follow-ups
- **HM-BD.E** — Dashboard-frontend cold-path. `/api/premarket-gaps` still has the ≥6 min cold path; UI users hit it on first load after restart or after 5 min idle. Smallest fix: add a `_startup_premarket_warm` daemon thread in `main.py` mirroring `_startup_market_backfill` (lines 2652–2660) that fires `scan_premarket_gaps()` once at boot, then optionally schedule a `refresh_premarket_cache()` every 4 min via `schedule.every(4).minutes.do(...)` to stay ahead of the 300s TTL. **Largest-bang option B:** rewrite `engine/premarket_scanner.py::scan_premarket_gaps()` to use `get_bulk_snapshots(get_active_universe())` (the same primitive `/api/momentum/premarket` uses) — collapses the 668×2 serial scan into one batched call, eliminates the cold-path entirely for both consumers. Recommend Option B; it's structurally aligned with HM-BD.1's direction.
- **HM-BD.F** — Loud-failure logging at the bare-except wrappers in `engine/ai_brain.py` (lines 911 and 968-ish). Per CLAUDE.md error-handling posture (established 2026-05-05), error logs should capture `type(e).__name__` + `repr(e)`, and architecture-class paths should NTFY on first occurrence per error class per day. The helper itself already logs loudly on failure (line 47 of the new code), but if the helper succeeds and then a `KeyError`/`AttributeError` happens during dict-access in the format loop or set comprehension, the outer `except Exception: pass` silently drops it. HM-BD.F should replace those two `pass` lines with `console.log(f"[yellow]premarket gaps consumer error: {type(_e).__name__}: {_e!r}")` and consider an NTFY rate-limited at 86400s for the first caller (it touches every agent cycle's prompt context). 5–10 line diff.

### Operational notes
- Cold-curl from BD.0 (PID 82403) is still alive — confirmed by BD.C's legacy curl timing-out at the 30s cap. Captain may `kill 82403` after restart; harmless either way (will resolve naturally on launchctl kickstart).
- Push gate held: BD.1 commit is local-only on `main`. Captain owns `git push`.
- Pre-restart, the running service is still on old ai_brain.py — the migration takes effect only after `launchctl kickstart`.

### Post-closure update: BD.0 cold-curl completed
The BD.0 background curl finally finished post-closure and produced final timings: **call 1 = 10:24, call 2 = 8:53, call 3 = 6:30**. All three were full cold-path re-scans — confirming both (a) the legacy endpoint's true cold-path is ~6–10 min, and (b) the `@timed_cache(300)` decorator provides no protection when the underlying generation cost exceeds the TTL. This elevates HM-BD.E's priority and strongly supports the **Option B (rewrite `scan_premarket_gaps()` over `get_bulk_snapshots`)** recommendation: pre-warming alone can't compensate for a 6-min scan when TTL is 5 min — the cache will expire before the next refresh completes. Only a fundamental speedup (sub-second batched scan) makes the cache work as designed.

A possible additional follow-up, **HM-BD.G — fix the `timed_cache` decorator** itself to use `entry["time"] = result_finish_time` instead of `result_start_time`, so long-running endpoints get their full TTL window after producing a result. ≤5-line change at `dashboard/app.py:380-396`. Considered nice-to-have; not blocking once HM-BD.E lands.

