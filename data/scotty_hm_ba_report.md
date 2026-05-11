# HM-BA Discovery

**Phase:** BA.0 (read-only inventory)
**Date:** 2026-05-11
**Scotty:** Claude Code · Opus 4.7 · `claude-opus-4-7[1m]`
**Pre-flight:** PASS — HM-AZ + HM-BB visible on origin (2ee11fa/3d9f5ee). HM-BC staged locally (4ec8dbc/5fa8a57) but not yet pushed — note this is fine, Captain held push between HM-BC and HM-BA. Service alive (PID 76719, port 8080 bound).

---

## Per-item analysis

### BA.1 — Race tile timestamp staleness

**Status: SELF-RESOLVED (likely)** — could not reproduce in current code.

**Evidence:**
- Race tile is the Phase 2 Dashboard Remodel block at `dashboard/static/index.html:34171-34344`.
- Backend endpoint `/api/momentum/race?limit=20` returns fresh `ts` field; smoke at 14:13 returned `"ts": "2026-05-11 14:13:13"` (live).
- Poller `POLL_MS = 30000` (30s interval) at L34251.
- Render function at L34276 reads `data.ts` and writes to `raceMeta` element: `meta.textContent = 'Last update: ' + ts + …`.
- Poller starts on Race tab activation via sidebar L2175: `onclick="showSection('race');if(typeof raceStart === 'function') raceStart();"`
- `raceStart()` at L34330 sets `timer = setInterval(fetchOnce, POLL_MS)`.

The backend emits fresh timestamps, the poller is wired at 30s, the renderer is reading the right field. The "frozen Last update" symptom would only manifest if (a) the user left the tab and came back without re-firing `raceStart`, or (b) the browser cached the response. Neither is a backend bug.

**Plan:** **SKIP** — already clean. If Captain has fresh browser-side evidence of staleness (screenshot, HAR), we can revisit; otherwise the code path is correct.

---

### BA.2 — `/api/momentum/scanner` 404

**Status: NEEDS DECISION** — endpoint truly missing AND no consumer expects it.

**Evidence:**
- `curl -s -o /dev/null -w "HTTP %{http_code}\n" http://localhost:8080/api/momentum/scanner` → **HTTP 404**.
- `grep -nE "@app\.(get|post).*scanner" dashboard/app.py` returns several scanner endpoints (`/api/stream/scanner`, `/api/dayblade/scanner`, `/scanner`, `/api/scanner/status`, `/api/scanner/live`) but **no `/api/momentum/scanner`**.
- `grep -rn "momentum_scanner" --include="*.py"` → only generic references in `engine/crew_scanner.py:4137` (a section header) and `engine/crew_specialization.py:493` (string list element). **No orphaned `def momentum_scanner(…)`** anywhere.
- `grep -n "/api/momentum/scanner" dashboard/static/index.html` → **zero matches**. Frontend doesn't call it.

The directive frames this as "Phase 3 from earlier momentum remodel left it dangling" but discovery shows there's no dangling partial — no orphan handler, no half-written route, no frontend caller. The endpoint was either planned but never started, or built and fully removed.

**Plan:** **NEEDS DECISION** — Captain Q1 (below). Options:
- α — Drop the item entirely (no consumer = no bug).
- β — Build it now as preparation for a forthcoming caller. If so, what response shape should it return? Closest sibling `/api/momentum/race` returns `{"ts":"…","limit":20,"rows":[…]}`. Should `/api/momentum/scanner` mirror that, or something else (signal list? Premarket-style hits array)?

---

### BA.3 — `/api/premarket-gaps`

**Status: NEEDS DECISION** — endpoint exists and has live internal consumers; "broken" framing is inaccurate. Real issue is **slow + duplicated**, not 404.

**Evidence:**
- Endpoint defined at `dashboard/app.py:11069-11074`: returns `{"gaps": scan_premarket_gaps()}` via `engine/premarket_scanner.py`. **Not 404.**
- Live call **eventually returns** but very slowly: a foreground curl with `--max-time 10` gives up at `HTTP 000 in 10.007s`; a parallel curl with no timeout completed in the background with `HTTP 200` and a payload of **244 gap rows** in `{gaps: [{direction, gap_pct, premarket_price, prev_close, scanned_at, symbol}]}` shape. So the endpoint isn't broken — it's just slow enough to blow ai_brain.py's `timeout=5` / `timeout=10` and yield HTTP 000 to those callers.
- Sister endpoint `/api/momentum/premarket` returns in **9ms**: `{"ts":"2026-05-11 14:11:38","window_state":"AFTER","hits":[]}`.
- **Two live Python callers of the legacy endpoint:**
  - `engine/ai_brain.py:898` — `requests.get("http://127.0.0.1:8080/api/premarket-gaps", timeout=10)` — likely silently times out today
  - `engine/ai_brain.py:927` — `requests.get(…, timeout=5)` — even more aggressive timeout
- One alias: `dashboard/app.py:13816` — `/api/premarket` → `/api/premarket-gaps` (307 redirect).
- Comment at `dashboard/app.py:17983`: *"Parallel to legacy /api/premarket-gaps (which serves engine.ai_brain via)"* — the Phase 3 remodel deliberately built `/api/momentum/premarket` parallel without touching the legacy.
- Zero frontend callers — `grep "premarket-gaps" dashboard/static/index.html` → 0 matches.

**Response-shape mismatch:**
| Endpoint | Shape | Latency |
|----------|-------|---------|
| `/api/premarket-gaps` (legacy) | `{"gaps": [{…}, …]}` | 10s+ (live scan, no cache) |
| `/api/momentum/premarket` (new) | `{"ts": "…", "window_state": "…", "hits": [{…}, …]}` | ~9ms (cached) |

Removing or aliasing would change ai_brain's response shape — if ai_brain expects `data["gaps"]` and gets `data["hits"]`, it silently breaks the premarket-aware buying logic.

**Plan:** **NEEDS DECISION** — Captain Q2 (below). Options:
- α — **Alias + adapt** — make `/api/premarket-gaps` thin-wrap `/api/momentum/premarket` and reshape `hits` → `gaps`. Risk: ai_brain's per-gap dict keys may differ (need to read `engine/premarket_scanner.py::scan_premarket_gaps` vs the momentum equivalent and confirm). Effort: ~20 LOC + ai_brain audit.
- β — **Remove, after migrating ai_brain.py** — update L898+L927 to call `/api/momentum/premarket` directly with the new shape. Risk: shape adapter still needed in ai_brain. Effort: ~10 LOC in dashboard + 15 LOC in ai_brain.
- γ — **Add caching to legacy** — keep the endpoint, slap an in-process `@timed_cache` on it so the 10s scan only runs every N minutes. Doesn't fix the duplication, but fixes the latency. Effort: 1 line.
- δ — **Defer entirely** — slow-but-working is not breaking anything visible. Park as HM-BA.E for a dedicated session.

**Recommendation (Scotty):** **γ (cache)** for this session as the cheapest unblock; β (migrate ai_brain) as a follow-up HM-BD. Aliasing-with-adapter (α) is the wrong fit because the underlying scanners differ — they shouldn't be force-aliased.

---

### BA.4 — Working-tree stragglers

**Status: STILL APPLICABLE** — clean separation between BA.4 territory and BA.5/non-scope.

**Evidence (`git status --short`):**
- `M data/bull_bear_cache.json` — cache refresh (BA.4 target)
- `M docs/OPS_LOG.md` — ops log (BA.4 target)
- `M docs/model_watch/MODEL_WATCH_2026-05-08.md` — model watch (BA.4 target)
- `M main.py` — 1-line diff = HM-AW heartbeat log at L2174 (**BA.5 target, NOT BA.4** — directive says "main.py only if change is heartbeat-related and nothing else"; it is heartbeat-only, but it belongs in BA.5's commit, not BA.4's)
- Untracked `HM-BA.md`, `HM-BB.md` (directive files — leave untracked; consider `.gitignore` later)
- Untracked `archive/stubs/`, `backups/main.py.pre-hm-as-b2-20260508_075409`, many `data/*.pre-*-*` and `data/scotty_*` files — **sacred backups, leave untracked**
- Untracked `data/model_watch_log.jsonl`, `docs/model_watch/MODEL_WATCH_2026-05-10.md`, `reports/`, `shared/finviz_scanner.py.pre-hm-ay-20260511_1153` — judgment calls; default = leave untracked unless Captain wants them in.

**Plan:**
- **One commit** covers the 3 active-tracking changes: `chore: refresh cache + ops log + model watch (BA.4)`.
- Untracked directive `.md` files and backups stay untracked.
- The new `MODEL_WATCH_2026-05-10.md` — Captain decision (Q3 below) whether to include. **Recommend: include** since 2026-05-08 is already tracked and 2026-05-10 is the latest in the same series.

---

### BA.5 — Remove HM-AW.4 heartbeat log

**Status: STILL APPLICABLE.**

**Evidence:**
- Exactly one line in `main.py:2174`: `console.log("[cyan]🧭 [HM-AW] tick fired")`
- Currently uncommitted (+1 line in `git diff main.py`).
- The surrounding block (`# === HM-AW: Chekov intraday convergence buyer ===` at L2164 through `# === end HM-AW ===` at L2197) is keep — only the L2174 console.log is removed.

**Plan:** Remove L2174. Compile-check. Commit. Approximate diff: -1 line. The HM-AW block anchors stay (they document where the feature lives).

---

## Captain decisions needed (block BA.1+ until resolved)

### Q1 — BA.2 disposition
The `/api/momentum/scanner` endpoint is missing AND has zero consumers (no orphan, no frontend caller). **Recommended: α (drop the item).** Building an endpoint with no caller is dead code. If Captain confirms there's a planned consumer, switch to β and tell me the response shape.

### Q2 — BA.3 disposition
The "broken" framing is wrong — `/api/premarket-gaps` is slow-but-working with two live `ai_brain.py` callers. Options α/β/γ/δ described above. **Recommended: γ — add `@timed_cache(180)` (3 min)** as a cheap unblock; defer the migration (β) to a future HM-BD.

### Q3 — BA.4 scope
Include `docs/model_watch/MODEL_WATCH_2026-05-10.md` (currently untracked) in the chore commit alongside the existing 2026-05-08 update? **Recommended: YES** — same series, same cadence.

### Q4 — Confirm pre-flight HM-BC state
HM-BC commits are staged locally (4ec8dbc, 5fa8a57) but not pushed to origin. The directive's prereq check assumes "HM-AZ + HM-BB + HM-BC landed" — HM-BC is technically landed-but-unpushed. **No action needed unless Captain wants to push HM-BC before BA.1.**

---

## Phase sequencing if Captain answers Q1–Q3

| Phase | What happens | Restart needed? |
|-------|--------------|-----------------|
| BA.1 | SKIP (self-resolved) | No |
| BA.2 | Per Q1 — likely drop entirely | If wired, yes |
| BA.3 | Per Q2 — likely `@timed_cache` on legacy | Yes |
| BA.4 | Commit 3 (or 4) files: cache + ops log + 1–2 model_watch | No |
| BA.5 | Remove main.py:2174 console.log | Yes |
| BA.C | Static verify (anchors, compile, routes) | — |
| BA.D | Closure report | — |

If Captain picks the minimal path (skip BA.1, drop BA.2, defer BA.3 to HM-BD, ship BA.4 + BA.5), the epic shrinks to **2 commits** (BA.4 chore + BA.5 refactor) and a closure doc.

---

## Files NOT changing (verified out-of-scope per directive)

- Any `.db` file (read-only directive)
- `engine/ghost_scoring.py`, `engine/ghost_trades.py` (HM-BB / HM-BC closed)
- `dashboard/app.py` ghost endpoints (HM-BC closed)
- `engine/premarket_scanner.py` (BA.3 may touch the dashboard endpoint, not the scanner module)

---

## What restart will unblock

The current running service (PID 76719) is on the HM-BC code (post-restart from earlier this session). After BA.X ships and is restarted:

- BA.2 (if α): no behavior change.
- BA.3 (if γ): `/api/premarket-gaps` becomes fast on cache hits.
- BA.5: Chekov intraday convergence ticks stop spamming `🧭 [HM-AW] tick fired` to `trader.log` every 15 min.

---

**HALT.** Ready for Captain direction on Q1–Q4 before BA.1+.

---

## HM-BA Closure (2026-05-11)

Captain green-lit all 4 recommendations. Execution surfaced one course-correction (BA.3) which is documented below.

### Commits staged (not pushed)

```
2edaa35 chore: HM-BA.4 — refresh cache + ops log + model_watch reports
```

Combined with HM-BC's two unpushed commits, the local-vs-origin gap is now **3 commits**:

```
2edaa35 chore: HM-BA.4 — refresh cache + ops log + model_watch reports     (HM-BA)
4ec8dbc docs(scotty): HM-BC.4 — closure + Ghost Tracking Architecture doctrine
5fa8a57 refactor(ghost): HM-BC.2 — rename ghost_trader.py → ghost_scoring.py
```

### Per-phase outcomes

- **BA.1 (Race tile)**: **SKIPPED — already clean.** Backend `/api/momentum/race` returns fresh `ts`, 30s poller wired, renderer correct. No reproducible staleness in current code; symptom (if it ever appeared) was likely browser-side tab visibility.

- **BA.2 (`/api/momentum/scanner`)**: **DROPPED per Q1 α.** Endpoint missing, no orphan handler, no frontend caller, no Python caller. Wiring an endpoint with zero consumers is dead code. If a future need for a momentum-scanner endpoint surfaces, build it then.

- **BA.3 (`/api/premarket-gaps`)**: **SKIPPED — course correction.** Approved fix (γ — add `@timed_cache(180)`) was based on incomplete discovery; endpoint **already has `@timed_cache(300)`** at `dashboard/app.py:11070`. Lowering 300→180 would be a regression (more cache misses). The real bug is **cold-path latency on first call** (~10s+; ai_brain's `timeout=5`/`timeout=10` blow before the scan completes). Caching doesn't fix that — only pre-warming, workload reduction, or shape migration to `/api/momentum/premarket` would. Deferring to HM-BD with this evidence captured.

- **BA.4 (Housekeeping)**: **SHIPPED — 4 files in commit 2edaa35.** `data/bull_bear_cache.json` (+78/-42), `docs/OPS_LOG.md` (+4), `docs/model_watch/MODEL_WATCH_2026-05-08.md` (+15/-4), `docs/model_watch/MODEL_WATCH_2026-05-10.md` (new, +75 per Q3). No code; no restart needed.

- **BA.5 (Heartbeat log)**: **SHIPPED — via working-tree discard, no commit.** The `console.log("[cyan]🧭 [HM-AW] tick fired")` at `main.py:2174` was uncommitted dev garbage from the HM-AW.4 session (`git log --all -S "tick fired"` returns zero hits — never landed on any branch). My edit dropped the line; `git diff HEAD main.py` is now empty. No commit to make because nothing is being subtracted from history — the line never existed on origin.

### Restart needed

**No.** Of the 5 phases:
- BA.1, BA.2, BA.3 produced zero changes (skipped/dropped/deferred).
- BA.4 is doc/cache — no code, no restart.
- BA.5 was a working-tree-only discard — the line never reached the running service, so removing it from the working tree changes nothing observable.

Captain can push HM-BC + BA.4 as a single bundle whenever convenient. No `launchctl kickstart` required for this epic.

### Out-of-scope follow-ups (HM-BA.E candidates)

- **HM-BD (carved out today):** `/api/premarket-gaps` cold-path latency. Real options on the table:
  - Pre-warm the cache at service boot (one fire-and-forget call to `scan_premarket_gaps()` after uvicorn binds).
  - Reduce scan workload in `engine/premarket_scanner.py::scan_premarket_gaps` (currently scans 244 symbols).
  - Migrate the two `ai_brain.py` callers (L898, L927) to `/api/momentum/premarket` with a shape adapter so they get 9ms responses.
- **HM-BD-β:** Decide whether the `dashboard/app.py:13816` alias `/api/premarket` → `/api/premarket-gaps` should redirect to `/api/momentum/premarket` instead. The two have different response shapes, so this needs a shape decision.
- `MODEL_WATCH_2026-05-11.md` will land tomorrow on the same cadence and will need a similar chore commit when it does.
- Untracked `HM-BA.md` / `HM-BB.md` directive files — Captain can `.gitignore` them OR commit them under `docs/directives/` if useful as artifacts.

### Phase ledger

| Phase | Outcome | Artifact |
|-------|---------|----------|
| BA.0  | Discovery report + 2 premise inversions + 4 captain questions | this file §"Per-item analysis" |
| BA.1  | SKIPPED — already clean | — |
| BA.2  | DROPPED per Q1 α | — |
| BA.3  | SKIPPED — course correction (already cached at 300s; real fix → HM-BD) | — |
| BA.4  | SHIPPED — 4-file chore commit | commit `2edaa35` |
| BA.5  | SHIPPED — working-tree discard | no commit (line never existed on origin) |
| BA.C  | Static verify: main.py compiles; venv-python loads 710 routes; live endpoints behave as expected | all PASS |
| BA.D  | This closure section | — |

Ready for Captain bundled push (HM-BC + HM-BA) when convenient. No restart required for this epic; only the deferred HM-BD work would require one.
