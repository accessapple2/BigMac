# HM-BD.F Discovery — Loud-Fail Observability at ai_brain Wrappers

**Phase BDF.0 — read-only profiling. No code edits.**
**Date:** 2026-05-11
**Engineer:** Scotty (Opus 4.7)

---

## Pre-flight summary
- HM-BD.E (`448b7b3`), HM-BD.G (`d8ac548`), HM-BD.1 (`bcb3bca`) all in `origin/main`.
- Trader bridge alive (PID 91516, port 8080).
- Working tree clean (42 untracked = docs/backups only).

---

## 1. The two HM-BD-flagged wrappers (must-fix)

After HM-BD.1's line shifts (helper insertion bumped lines), the wrappers now live at:

### Caller A — gap-context injection block (`engine/ai_brain.py:930-948`)
```python
        # Inject pre-market gap data — shared cache (5 min TTL) avoids N HTTP calls/cycle
        try:
            import time as _gc_time
            _now_gc = _gc_time.time()
            if _now_gc - _premarket_gap_cache["ts"] >= _PREMARKET_GAP_TTL:
                # HM-BD.1 γ: was /api/premarket-gaps timeout=10 → ≥6min cold-path HTTP-000
                _premarket_gap_cache["gaps"] = _fetch_premarket_gaps(timeout=5.0)
                _premarket_gap_cache["ts"] = _now_gc
            _gaps = _premarket_gap_cache["gaps"]
            if _gaps:
                gap_context = "\n=== PRE-MARKET GAPS (today) ===\n"
                for g in _gaps:
                    ...
                scan_ctx += gap_context
        except Exception:                ### ← bare swallow at L947
            pass                          ### ← drops fetch errors, dict shape errors, format errors
```
**What it wraps:** the `_fetch_premarket_gaps()` call (HTTP path, now hitting `/api/momentum/premarket` after HM-BD.1) AND the iteration/formatting of returned gaps.
**Fallback on failure:** silently leaves `scan_ctx` without the gap-context section. Prompt loses that data; no log.

### Caller B — DayBlade-Sulu focused-scan (`engine/ai_brain.py:961-970`)
```python
            try:
                # HM-BD.1 γ: was /api/premarket-gaps timeout=5 → cold-path HTTP-000
                _gap_syms = {
                    g["symbol"]
                    for g in _fetch_premarket_gaps(timeout=3.0)
                    if abs(g.get("gap_pct", 0)) > 2
                }
            except Exception:                ### ← bare swallow at L969
                pass
```
**What it wraps:** the helper call + set comprehension.
**Fallback on failure:** `_gap_syms` stays as `set()` (empty), DayBlade focuses on its baseline movers only. No log.

---

## 2. Bonus inventory — bare-except patterns across the file

```
bare 'except:'             = 0
'except Exception:'        = 28   (no `as e` — can't even reference the exception)
'except Exception as e:'   = 21   (e is bound; may or may not actually use it)
silent 'except Exception: pass' = 24   (highest-risk subset)
```

Examples of the 24 silent-pass sites (with line + immediate next code):

| Line | Context (what gets silently dropped) |
|---|---|
| 274 | weekly scan-pause check |
| 289 | discovery_syms display |
| 517 | per-cycle Ollama RAM probe |
| 579 | inter-model stagger sleep wrapper |
| 612 | mid-cycle model-cooldown check |
| 710 | equity curve snapshot save |
| 745 | trade-memory persistence |
| 772 | various intermediate state writes |
| **947** | **HM-BD caller A — gap-context inject** ← target |
| **969** | **HM-BD caller B — Sulu focused-scan** ← target |
| ... | ~14 more |

Most non-target silent-pass sites wrap **non-network internal state operations** (DB writes, file saves, in-memory cache lookups). They're not in the same risk class as the HTTP-call wrappers — a silent failure of "equity curve snapshot save" is meaningfully less dangerous than a silent failure of "all our premarket data, for every agent cycle, for months."

The 2 HM-BD-flagged wrappers are unique in wrapping an out-of-process call (HTTP localhost via the `_fetch_premarket_gaps()` helper) whose failures are exactly the kind that need to be loud.

---

## 3. Logger pattern in this file

`engine/ai_brain.py` does NOT use `logging.getLogger()`. It uses `rich.console.Console`:

```python
# line 18
console = Console()
```

All in-file error logs use `console.log(f"[red]... {e}")` or similar (sample: line 249 `console.log(f"[red]Trade grade callback error for {player_id}: {e}")`).

The directive's template referenced `logger.warning(...)` — I'll adapt to the file's existing convention: `console.log(f"[yellow]... [{type(e).__name__}]: {e!r}")`.

---

## 4. CLAUDE.md error-handling posture (the doctrine)

`CLAUDE.md:175-179` documents:
> ```python
> except Exception as e:
>     console.log(f"foo error: {type(e).__name__}: {e!r}")
> ```
> `{type(e).__name__}` surfaces the exception class. `{e!r}` is `repr(e)` which usually includes the str representation but with class context preserved.

That format is the doctrine. My fix will use it exactly.

CLAUDE.md also says NTFY on architecture-class paths. Per the brief, the 2 caller wrappers run on every agent cycle (caller A) and every DayBlade-Sulu cycle (caller B) — frequent enough that NTFY would spam. Captain's directive said "log-only" in my pre-confirmation note; sticking with that. NTFY-on-first-occurrence-per-day could be a future HM-BD.F+ if needed.

---

## 5. Proposed replacement pattern

Per-wrapper diff template (preserves the existing fallback semantics; purely additive log):

```python
# === HM-BD.F ===
try:
    ...existing body...
except (requests.RequestException, TimeoutError, ConnectionError, KeyError, ValueError) as e:
    console.log(f"[yellow]premarket gap-context inject failed [{type(e).__name__}]: {e!r}")
except Exception as e:
    # Unexpected class — log loud so we know about it, but don't crash
    console.log(f"[red]premarket gap-context inject UNEXPECTED [{type(e).__name__}]: {e!r}")
```

Why two-tier:
- The expected failure classes (`RequestException` from `_fetch_premarket_gaps()`'s helper, `KeyError`/`ValueError` from dict access in the format loop) get a yellow WARN.
- Anything else gets a red ALERT so we notice it — but still doesn't crash the agent cycle.

The fallback behavior is unchanged: `scan_ctx` remains without the gap section (caller A) or `_gap_syms` remains empty (caller B).

Caller B is a `set comprehension` not iterating dicts, so its expected classes are narrower: `(requests.RequestException, TimeoutError, ConnectionError, KeyError)`. The `_fetch_premarket_gaps()` helper does its own loud log internally (added in HM-BD.1) so the outer wrapper here mainly guards against `g["symbol"]` KeyErrors.

---

## 6. Captain question Q1 — scope

**α-minimal (recommended):** Fix only the 2 HM-BD-flagged wrappers. Matches the lesson exactly: "the two specific HTTP-wrapping silent swallows that hid a 6-min bug."

**α+ defensible:** 2 known + `memory_block` injection at L920 (same agent-cycle setup region, same broad-except pattern, but wraps internal function call not HTTP). 3 sites.

**β all silent-pass:** 24 sites. Requires per-site judgment ("is silence intentional?") and balloons the diff. Doesn't match HM-BD's narrow lesson; some silent-pass sites are intentional and removing them creates log noise.

**Scotty's recommendation: α-minimal.** Tight scope, matches lesson, ships in <30 LOC. If we want broader observability after observing this in production for a week or two, do a separate epic with per-site audit.

---

## 7. Captain question Q2 — log severity

The CLAUDE.md doctrine doesn't prescribe a severity. The file uses color codes:
- `[red]` for errors that should stop work
- `[yellow]` for warnings worth noting
- `[dim]` / `[cyan]` for informational

**Scotty's recommendation:** Two-tier as drafted in §5 — yellow for expected classes, red for unexpected. Captures "this is a known failure mode (don't panic)" vs "this is something new (please look)."

If Captain prefers single-tier, all-yellow keeps it simple.

---

**HALT — awaiting Captain decision on Q1 (scope) and Q2 (severity).**

---

# Captain Decisions (received 2026-05-11)

- **Q1:** α-minimal — only the 2 HTTP wrappers at L947 + L969.
- **Q2:** two-tier — `[yellow]` for expected classes, `[red]` for unexpected.

Inline workflow: push + restart + verify per the BDF.D template.

---

## HM-BD.F Closure

### What shipped (commit 9e587f8)

`engine/ai_brain.py`, single commit, +11 / −4 lines:

1. **Module-level `import requests`** added at line 3 — required so except clauses can reference `requests.RequestException`.
2. **Caller A wrapper (L948-953)** — was `except Exception: pass`; now:
   ```python
   except (requests.RequestException, TimeoutError, ConnectionError, KeyError, ValueError) as e:
       console.log(f"[yellow]gap-context inject failed: {type(e).__name__}: {e!r}")
   except Exception as e:
       console.log(f"[red]gap-context inject UNEXPECTED: {type(e).__name__}: {e!r}")
   ```
3. **Caller B wrapper (L972-977)** — identical pattern but logs as `Sulu DayBlade gap_syms` for context disambiguation.

Both wrappers preserve the original fallback behavior exactly (caller A: `scan_ctx` stays without gap section; caller B: `_gap_syms` stays empty `set()`). Purely additive logging.

### Why this matters

The HM-BD investigation that started this whole epic chain only happened because the Captain *manually noticed* HTTP 000 in production. Two bare-except wrappers had been hiding a 6-minute cold-path bug for months. The bug class — silent failures in network-touching paths — is now permanently observable for both wrappers. The next time either fails, we get a log line with the exact exception class and `repr(e)`, per CLAUDE.md doctrine 2026-05-05.

### Lesson reapplied from HM-BHBI
This edit touched `from __future__ import annotations` already present at the top of `ai_brain.py` (added in some prior epic — likely HM-BD.1's helper). Compile checks + import smoke run against `venv/bin/python3` (Py3.9.6 — the launchd interpreter), not `/usr/bin/python3`. Both clean.

### Out-of-scope follow-ups (parked)
- **HM-BD.F-audit** — review the other 22 `except Exception: pass` sites in `engine/ai_brain.py` to determine which deserve loud-fail treatment vs which are intentionally silent. Per-site judgment required; deferred to its own epic.
- **HM-BD.F+NTFY** — consider adding rate-limited NTFY on first-occurrence-per-class-per-day for these two wrappers (would put them in the "architecture-class paths" tier per CLAUDE.md). Wait for production observation first; defer.
- **HM-BD.H** — investigate the `scanned_at` HTTP response format quirk surfaced during HM-BDGE (still open from prior epic).
- **HM-BE-suffix / HM-BE-historic** — qwen3 id rename + backtest config alignment.
