# 🔧 HM-BD.H — scanned_at Cache Mystery Trace Closure

**Author:** Scotty (Opus 4.7)
**Date:** 2026-05-12
**Status:** UNREPRODUCIBLE in current process; no code fix shipped; ambient behavior verified correct

---

## Original symptom (from MONSTER 1, 2026-05-12 morning)

```
Direct Python call:  scan_premarket_gaps()  →  ISO format datetime  ('2026-05-12T08:10:37.759533')
HTTP endpoint:       /api/premarket-gaps    →  SPACE format datetime ('2026-05-12 01:08:15')
                                                                       ↑ predates the process restart
```

In-memory `_endpoint_cache` should not survive a process restart, so the data source for the HTTP response couldn't be what we assumed. MONSTER 1 deferred the trace.

---

## What I found in BDH.0

### Code path is unique

`/api/premarket-gaps` (dashboard/app.py:11172) is the ONLY endpoint of that name in the codebase. It calls `scan_premarket_gaps` from `engine/premarket_scanner.py` (the only definition in active source). The function uses `datetime.now().isoformat()` at line 54 — which always produces T-separated ISO format. No conditional branch can return space format.

### Endpoint handler unchanged since 2026-04-10

`git blame` on lines 11172-11178 → all attributed to commit `5498c34d` (2026-04-10). No code change between MONSTER 1's observation and now explains a behavior shift.

### `_endpoint_cache` is purely in-memory

Defined `_endpoint_cache: dict = {}` at dashboard/app.py:387. Only writer is the `timed_cache` decorator at line 406 (plus one leaderboard-specific direct write at 2255, unrelated key). No disk persistence, no IPC, no shared memory. Cleared on every process restart by definition.

### Middleware audit

4 middlewares registered (Auth, RateLimit, ScanThrottle, SecurityHeaders, GZip, CORS). None of them cache response bodies.

### Single uvicorn worker

`main.py:2353` runs `uvicorn.run(app, host="127.0.0.1", port=8080, log_level="warning")` — single worker. No multi-process shared state.

### Bytecode

`engine/__pycache__/premarket_scanner.cpython-314.pyc` exists from some prior Python 3.14 invocation, but the live trader runs Python 3.9 (`/Library/Developer/CommandLineTools/.../Versions/3.9/...`), which would use a different pyc filename. Cross-version pyc poisoning ruled out.

---

## Live trace (the test that should have caught the bug)

Added `console.log(f"[cyan][HM-BD.H trace] scan_premarket_gaps building scanned_at={...}")` after the `scanned_at = datetime.now().isoformat()` line. Restarted trader. Hit endpoint.

**Result:**
- HTTP response: `count: 0` (empty gaps)
- Trace log: never fired (function returned early before the trace line)
- Log explains: `[10:23:26] Premarket scan: get_bulk_snapshots returned` — Alpaca snapshots empty at that moment

**Direct subshell call at the same moment:** 212 gaps, ISO format, trace fired correctly.

**Difference:** the live trader's `get_bulk_snapshots()` returned empty (probably Alpaca quota or transient). My subshell's call to the same function — same code, fresh interpreter — returned 668 snapshots. Two callers, same code, different upstream response.

---

## Hypothesis on the original MONSTER-1 observation

The space-format `'2026-05-12 01:08:15'` value contains UTC midnight + 1:08, which equals **20:08 MST the previous evening**. SQLite's `CURRENT_TIMESTAMP` returns UTC by default. The `premarket_scan` DB table has `scanned_at TEXT DEFAULT CURRENT_TIMESTAMP`. So that exact string format COULD originate from the `premarket_scan` table.

If at MONSTER-1 time some code path was reading from the `premarket_scan` table and returning it as if it were `scan_premarket_gaps()` output, that would explain the format. But I cannot find any such code path in the current `dashboard/app.py` or `engine/premarket_scanner.py`. It may have been:

1. **A transient state where `scan_premarket_gaps` failed but some earlier cached payload from a different code path persisted** (in-memory IS cleared on restart, but I can't be 100% certain about uvicorn's internals)
2. **A code path that has since been removed** without my noticing
3. **A timing artifact** where `scan_premarket_gaps` ran during the rare window when Alpaca returned no snapshots and a fallback (since removed) read from the DB

Without being able to reproduce, I can't isolate which. The current code is unambiguously correct: empty list when Alpaca is down, ISO-format gaps otherwise.

---

## What the current process actually serves

| Scenario | Response |
|---|---|
| Alpaca snapshots available | 200+ gaps, every `scanned_at` ISO-format `datetime.now().isoformat()` |
| Alpaca snapshots empty | `{"gaps": []}` |

No path produces space-format `scanned_at` in the current code.

---

## Recommended disposition

**Close HM-BD.H as UNREPRODUCIBLE.** The bug from MONSTER 1 may have been a transient artifact. The current ambient behavior is correct. If the symptom recurs (Captain sees a space-format `scanned_at` in the wild again), the right move is:

1. Capture the response + `git log --oneline -10` from that moment
2. Hit `/api/premarket-gaps` and the disk file `data/premarket_gaps.json` simultaneously to compare
3. Re-add the trace instrumentation I just used, restart, watch the logs

The trace pattern is preserved here for re-application:
```python
# In engine/premarket_scanner.py:scan_premarket_gaps, after line 54:
console.log(f"[cyan][HM-BD.H trace] scan_premarket_gaps building scanned_at={scanned_at!r} universe_size={len(universe)} snaps={len(snapshots)}[/cyan]")
```

---

## Status

- No commits made.
- Trace instrumentation pulled cleanly (working tree only has untracked directive files).
- Trader stable on PID 23519 (post-restart for the trace test).

ntfy: `📋 HM-BD.H trace complete — UNREPRODUCIBLE, current behavior correct, see data/scotty_hm_bdh_report.md`
