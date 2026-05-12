# Phase 1 Discovery — 2026-05-10 (Scotty / HM-AN read bridge)

**Log:** `/tmp/scotty_phase1_discovery_20260510_1833.log`
**Session:** Saturday late-evening (market closed; 0 signals in trailing 60min — expected)

## Signal Center (:9000)

- **Process:** PID 18380 — `python signal-center/server.py` (Flask, `app.run(host='127.0.0.1', port=9000, threaded=True)`)
- **Framework:** Flask (`@app.route` decorators)
- **Entry point:** `signal-center/server.py` (line 3084) — note the **directory uses a dash** (`signal-center`), not underscore, which the Phase 0 `find ... -name "signal_center"` glob missed
- **Launchd job:** `com.trademinds.signal-center` (active, nice -15)
- **Network binding:** `127.0.0.1` only (per CLAUDE.md "Network Bindings" — HM-AW tracks reopening to network)

### Endpoints (curl probe results)

| Path | Code | Notes |
|------|------|-------|
| `/` | 302 | auth wall → /login |
| `/health` | 302 | auth wall → /login (NOT a health endpoint — name collision with browser login flow) |
| `/api/health` | **200** | ✅ Real healthcheck — body: `{"active_signals":2, "executed_signals":0, "last_signal_at":"2026-05-10T07:00:24Z", "ok":true, "outcome_tracker":"running", "sse_subscribers":0, "total_signals":1184}` |
| `/status` | 302 | auth wall |
| `/api/status` | 404 | does not exist |
| `/heartbeat` | 302 | auth wall |
| `/api/heartbeat` | 404 | does not exist |
| `/signals` | 302 | auth wall |
| `/api/signals` | **200** | ✅ Rich JSON signal feed — see "Bridge design implications" below |

### Notable `/api/*` routes (full list from `signal-center/server.py`)
- Read-side: `/api/health`, `/api/signals`, `/api/signals/active`, `/api/signals/<id>`, `/api/signals/snapshot`, `/api/signals/history`, `/api/signals/scorecard`, `/api/signals/outcomes`, `/api/signals/stream` (SSE), `/api/signals/all`
- Predictions: `/api/predictions/top5`, `/api/predictions/check`, `/api/predictions/history`, `/api/predictions/leaderboard`, `/api/predictions/analysis/<symbol>`
- Other: `/api/stats`, `/api/brain-context`, `/api/feed`, `/api/intelligence-summary`, `/api/screener`, `/api/trade-levels/<symbol>`, `/api/quant-signals`

### Producers writing to :9000 (from prior HM-AN scope doc)
- `engine/alpha_signals.py:5,1408,1478` — composite alpha → `:9000/api/signal` (POST)
- `engine/brain_context.py:358,371,375` — OPTIONS_FLOW
- `engine/ai_brain.py:1313,1357` — high-conf decisions
- `engine/ai_saas_disruption_scanner.py:11`

**Critical gap (the actual HM-AN motivation):** 5 engine producers POST to Signal Center; **zero dashboard read-path consumers**. The bridge fixes that.

## Dashboard (:8080)

- `/api/momentum/*` surface: **does not exist** (only `/api/scanner/status` and `/api/scanner/live` at `dashboard/app.py:10937,10969` use a similar name family, but unrelated)
- Endpoint count baseline: **616** registered routes in `dashboard/app.py`
- `engine/momentum/` directory: **does not exist** — will be created in Phase 1.1
- Existing Signal Center read references in dashboard: only one — `dashboard/app.py:11102` comment ("Top scored symbols from Signal Center (port 9000) intelligence_feed") — no actual fetch wired

## Data shape — two distinct sources to choose between

### Option A: `trader.db.signals` table (Phase 1 directive default)
```
id INTEGER PRIMARY KEY
player_id TEXT REFERENCES ai_players(id)
symbol TEXT
signal TEXT             ← directive incorrectly named this "side"
confidence REAL
reasoning TEXT
asset_type TEXT DEFAULT 'stock'
option_type TEXT
acted_on INTEGER DEFAULT 0
created_at TIMESTAMP
season INTEGER, sources TEXT, timeframe TEXT,
execution_status TEXT, rejection_reason TEXT, halted_emit INTEGER
```
- ~9.6k rows from `energy-arnold` alone; many other emitters; **0 in trailing 60min** (Saturday evening)
- LEGACY_BIMODAL March 23–25 rows excluded via reasoning filter per directive

### Option B: Signal Center `/api/signals` (Phase 2+ swap target per directive)
- Rich JSON: `action, agent_name, confidence, context_json, created_at, entry_price, model_used, reasoning, sources_json, status, stop_loss, symbol, take_profit, timeframe, type`
- 1184 total signals in Signal Center DB; 2 active as of 18:34 MST
- This is what `hm_an_scope.md` (line 19) calls "the gap" — dashboard has zero consumers of Signal Center

**Decision:** follow directive — Phase 1 reads `trader.db` directly; note `/api/signals` as the Phase 2+ swap candidate.

## HM-AN ticket state

- `data/scotty_proposals/hm_an_scope.md`: **exists** (filed 2026-05-10 by Scotty 2.0; converts SCOTTY_AUDIT_2 Section I into a backlog-ready ticket)
- In `docs/XO_BACKLOG.md`: **no** (the proposal doc says "do NOT start the work this session" — yet Captain's directive now greenlights the keel-laying)

## Service health (running, untouched)

| Service | PID | Nice |
|---------|-----|------|
| `com.trademinds.trader` (main.py) | 15010 | -15 |
| `com.trademinds.signal-center` (server.py) | 18380 | -15 |
| `com.trademinds.scanner` | 56963 | -15 |
| `com.trademinds.watchdog` | 53953 | -15 |
| `com.trademinds.mcp` | 841 | 0 |
| `com.trademinds.tunnel` | 32173 | 0 |

## Repo state

- Working tree dirty (3 modified files + 6 untracked — unrelated to HM-AN; will NOT be staged into HM-AN commits)
- **0 commits ahead of `origin/main`** — clean baseline for the 3 Phase 1 commits to land on top of
- Last commit: `76823b4 docs(scotty): loose-ends sweep closure report`

## Bridge design implications

### Adjustments to directive stub code (still within directive scope; bug fixes only)

1. **SQL column fix — required (would crash at runtime as written):**
   - Directive: `SELECT id, player_id, symbol, side, confidence, ...`
   - Reality: column is `signal`, not `side` (no `side` column anywhere in `signals` schema)
   - Fix: `SELECT id, player_id, symbol, signal, confidence, ...`

2. **Healthcheck false-positive fix — required:**
   - Directive: `requests.get(url, timeout=...)` follows redirects by default
   - `/health` returns 302 → login page (200) → false healthy
   - Fix: `requests.get(url, timeout=..., allow_redirects=False)`

3. **Candidate path ordering:**
   - Directive order: `/health, /api/health, /status, /api/status, /`
   - Observed working: only `/api/health` (200)
   - Fix: reorder so `/api/health` is checked first — saves 4 unnecessary 302s per heartbeat

### Recommended bridge surface (Phase 1)

Two endpoints as per directive:
- `GET /api/momentum/heartbeat` — calls `check_signal_center_health()` → `{bridge_alive, signal_center:{reachable, endpoint, last_check_ts, error}, phase}`
- `GET /api/momentum/recent_signals?since_minutes=60&limit=100` — calls `fetch_recent_signals()` → `{count, since_minutes, signals:[…]}`

### Phase 2+ swap candidate

When Phase 2+ swaps `fetch_recent_signals` to Signal Center API, target:
```
GET http://127.0.0.1:9000/api/signals
```
Richer payload (entry_price / stop_loss / take_profit) which Race + Scanner tiles can render directly without re-deriving prices.

## Discovery sign-off

- [x] All 12 Phase 0 probes completed
- [x] No DB writes performed (read-only `sqlite3` queries only)
- [x] No service restarts, no process kills
- [x] Signal Center read-only honored (only `curl` GETs, no POST)
- [x] Three design clarifications documented (SQL column, redirect handling, swap target)

**Next:** HALT for Admiral acknowledgement, then proceed to Phase 1.1 with the three adjustments above.
