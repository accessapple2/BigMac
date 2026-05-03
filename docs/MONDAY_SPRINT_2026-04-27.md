# MONDAY SPRINT — 2026-04-27

**Session**: S6.3 Monday Sprint
**Gate**: `_EXECUTION_ENABLED=False` held throughout
**Duration**: Full day sprint (continuation of Sunday Power Close-Out)
**Alpaca Paper Equity**: ~$79,716 (unchanged, no live exposure)

---

## Items Shipped

### Phase A — research_caller.py Fix (PRIMARY)

**Problem**: 170 ReadTimeout errors in one day (~1 error/minute). Root cause: qwen3:8b defaults to extended reasoning mode (think-mode), generating 10,000+ thinking tokens that consume the full 120s wall-clock timeout before any response is returned.

**Initial misdiagnosis**: Model was already qwen3:8b (changed from 14b in a prior session). The problem was `think` mode, not model size.

**Smoke test finding**: `/no_think` prefix in the prompt does NOT reliably disable think mode. Tested live against G1:
- `/no_think` prefix: still generated 531 thinking chars, took 25.84s (warm cache)
- `"think": false` top-level JSON key: 0 thinking chars, 0.13s total

**Fix applied** (`engine/research_caller.py`):
```python
json={"model": _MODEL, "prompt": prompt, "stream": False, "think": False}
```
Updated docstring to explain why `think=False` is required.
Backup: `research_caller.py.bak.20260427.fix2`

**Result**: 0 ReadTimeout errors after main.py restart. 0 errors confirmed today.

---

### Phase B — qwen3 think-mode Audit (Fleet-Wide)

Audited all Ollama callers fleet-wide. Applied `"think": False` to 13 call sites across 6 files. Downgraded 2 sites from qwen3:14b to qwen3:8b.

#### Group A — Already qwen3:8b, added `"think": False`

| File | Path | Notes |
|------|------|-------|
| `engine/bull_bear.py` | `gemini` branch | 30s timeout — was at risk |
| `engine/self_improvement.py` | Main generate call | Had manual `<think>` tag stripping workaround (now redundant but harmless) |
| `engine/premarket_scanner.py` | `grok` branch | 4-model ensemble, no deep reasoning needed |
| `dashboard/app.py` | Line 7431 (arena grok) | |
| `dashboard/app.py` | Line 7437 (arena gemini) | |
| `dashboard/app.py` | Line 7538 (player rec xAI) | |
| `dashboard/app.py` | Line 7544 (player rec google) | |
| `dashboard/app.py` | Line 7691 (Spock position analysis) | |

#### Group B — Downgraded 14b → 8b + added `"think": False`

| File | Was | Now | Reason |
|------|-----|-----|--------|
| `engine/chart_analyzer.py` gemini branch | qwen3:14b | qwen3:8b + think=False | Chart classification — no 14b depth needed |
| `engine/premarket_scanner.py` gemini branch | qwen3:14b | qwen3:8b + think=False | Gap scan classification — speed over depth |

#### Group C — Keep qwen3:14b, add `"think": False` only

| File | Model | Reason kept at 14b |
|------|-------|-------------------|
| `engine/metals_commentary.py` | qwen3:14b | Dalio voice 200-300 word macro narrative, 120s timeout, Admiral approved |

**Backups**: `chart_analyzer.py.bak.20260427.fix3`, `premarket_scanner.py.bak.20260427.fix3`, `bull_bear.py.bak.20260427.fix3`, `self_improvement.py.bak.20260427.fix3`, `metals_commentary.py.bak.20260427.fix3`, `app.py.bak.20260427.fix3`

#### Deferred to separate sprint (Group D)

- `engine/crew_specialization.py` — ChatOllama configs (different calling convention)
- `engine/ollama_provider.py` — default model setting
- `engine/dalio_provider.py` — needs separate review

---

### Phase C — Pin qwen3:8b on G1 (keep_alive=-1)

**Goal**: Keep qwen3:8b warm on G1 to avoid 53s cold-load penalty.

**VRAM finding**: G1 Pro has ~8GB VRAM (NOT 32GB as previously believed). qwen3:8b=5.9GB fills 74%. phi3:mini=2.2GB would push total to 8.1GB — exceeds VRAM, triggers eviction.

**Result**: Pin successfully set (`expires_at: 2318`). However, any fleet call to phi3:mini (Janeway agent) will evict the pinned qwen3:8b. With `think=False`, cold-load qwen3:8b takes 53s which is well within the 120s timeout — so the pin is a nice-to-have, not a requirement.

**Verdict**: Pin useful for burst periods. Not reliable long-term due to phi3:mini eviction from Janeway calls.

---

### Pre-Phase C — Riker XO 404 Investigation (Read-only)

**Finding**: `engine/riker_xo.py` has `OLLAMA_MODEL = "gemma3:4b"` hardcoded inside `_do_riker_synthesis()`. This model is NOT on G1 (available: phi3:mini, qwen3:8b, qwen3:14b, llama3.2:3b). This is a post-Apr 23 migration bug — Riker was using gemma3:4b on bigmac's localhost; after the migration to Ollie Box, gemma3:4b was never pulled on G1.

**Logged as next-sprint followup** (read-only, not fixed this session — no regression introduced here since this is pre-existing).

---

### Phase D — signal_history Writer Investigation

**Problem**: signal_history table in signals.db stopped receiving writes at 2026-04-26T09:00:24 (24+ hours ago).

**Root cause: (c) — Design gap, not a bug.**

The writer (`signal-center/server.py::_bg_refresh_signals()`) is triggered ONLY when the Signal Center frontend polls `/api/signals/all` via its 30-second countdown timer. There is no server-side timer. When the browser tab at port 9000 was closed Sunday at 9 AM during audit work, writes stopped.

**Confirmed**: 0 writes since 2026-04-26 09:00:24. Schema: `[id, timestamp, signal_name, value, score, grade, raw_data, source]`.

**Fix filed for next sprint**: Add a background scheduled writer in `signal-center/server.py` or `main.py` that calls `_fetch_all_signals()` on a 5-minute timer during market hours — makes signal_history a continuous audit trail independent of browser sessions.

---

## Key Discoveries

### 1. qwen3 think-mode is the dominant latency killer
The `"think": false` top-level JSON parameter is the correct and ONLY reliable way to disable qwen3 extended reasoning. The `/no_think` prompt prefix approach is unreliable — still generated thinking tokens in live testing.

### 2. G1 VRAM is ~8GB (corrected from previous assumption of 32GB)
- qwen3:8b = 5.9GB (74% of VRAM)
- phi3:mini = 2.2GB — cannot coexist with qwen3:8b
- Any fleet phi3:mini call (Janeway agent) will evict the pinned qwen3:8b
- Cold-load qwen3:8b with think=False: 53s load + 0.03s eval — within 120s timeout

### 3. signal_history is browser-driven only (architectural gap)
No server-side timer drives signal history writes. This is an audit trail gap — not a bug, but a design limitation.

---

## Smoke Test Results (EOD)

| System | Status |
|--------|--------|
| Dashboard (port 8080) | 200 OK |
| Signal Center (port 9000) | 200 OK |
| Research caller errors today | **0** (was 170 before fix) |
| Trades today | 20 (normal fleet activity) |
| G1 Ollie models loaded at EOD | 0 (idle) |

---

## Filed for Next Sprint

| Item | Priority | File |
|------|----------|------|
| Riker XO: replace `gemma3:4b` with `qwen3:8b` on G1 | HIGH | `engine/riker_xo.py` |
| signal_history: add server-side 5-min scheduled writer | MEDIUM | `signal-center/server.py` |
| `ollama_provider.py`: change default model 14b → 8b | MEDIUM | `engine/ollama_provider.py` |
| `crew_specialization.py`: audit ChatOllama 14b configs | LOW | `engine/crew_specialization.py` |
| `dalio_provider.py`: audit model selection | LOW | `engine/dalio_provider.py` |
| Janeway agent: phi3:mini coexistence with qwen3:8b on G1 — VRAM conflict | LOW | `engine/` |

---

## Backups Created This Session

```
engine/research_caller.py.bak.20260427.fix2
engine/chart_analyzer.py.bak.20260427.fix3
engine/premarket_scanner.py.bak.20260427.fix3
engine/bull_bear.py.bak.20260427.fix3
engine/self_improvement.py.bak.20260427.fix3
engine/metals_commentary.py.bak.20260427.fix3
dashboard/app.py.bak.20260427.fix3
```

---

## Rollback Paths

- Any Phase B file: `cp engine/FILE.bak.20260427.fix3 engine/FILE` then kill -HUP `$(pgrep -f main.py)`
- Dashboard: `cp dashboard/app.py.bak.20260427.fix3 dashboard/app.py`
- G1 keep_alive pin: automatic expiry or `curl -X DELETE http://192.168.1.166:11434/api/delete`

## Lessons

Today's test drive surfaced an XO inference cascade worth pinning as a standing operational principle. The UI session opened with a "war-room cycle stopped firing post-Phase-B deployment" finding that was eventually self-corrected, but only after backend cross-check against source-of-truth tables. Root cause of the miscall was a single bad surface read - a UTC vs ET timezone confusion on Crew Activity timestamps - that propagated into "fleet silent" then "Phase B regression" hypothesis.

Rule for future sessions: when an XO surfaces a "frozen pipeline" or "fleet silent" finding, source-of-truth tables must be cross-checked against API endpoints before raising the flag. The dashboard is a consumption layer; the database is the truth. Never infer fleet state from a single surface. The eight UI consumption gaps catalogued in DRYDOCK_UI_CONSUMPTION_AUDIT_FINAL.md are the structural reason this rule matters - any one panel may be lying about what the backend actually has.
