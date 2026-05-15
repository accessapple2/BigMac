# Scotty: HM-AN Bridge UI Auth Decision Doc — 2026-05-15

**Status:** decision-ready. No code edited. Captain to pick A/B/C/D.

**Memory blocker:** `project_hm_an_bridge_auth_blocker.md`

## TL;DR — Captain recommendation

**Option C (dashboard proxy layer).** Already feasible with ZERO Signal Center
code change because SC's `_auth_gate` (signal-center/server.py:565-590)
**already grants free `/api/*` access from `127.0.0.1`**. Dashboard runs on
bigmac, so server-side requests from `dashboard/app.py` to
`http://127.0.0.1:9000/api/...` see `remote_addr=127.0.0.1` and bypass auth.

Browser users hit dashboard's existing 2FA TOTP + RBAC layer; the proxy
endpoints inherit that perimeter. No new auth state to manage. No SC
restart needed.

## Critical architectural finding (not in memory)

The memory note frames the 47 HTTP endpoints as "auth-gated." Re-reading the
gate logic:

```python
# signal-center/server.py:565+
@app.before_request
def _auth_gate():
    ...
    # Localhost bypass ONLY for /api/*
    if _sc_is_localhost() and path.startswith("/api/"):
        return None
    ...
```

And `_LOCALHOST = {"127.0.0.1", "::1", "localhost"}` (line 42).

Translation: any process on bigmac making `requests.get('http://127.0.0.1:9000/api/x')`
gets a 200, no cookie, no token, no header. **This is already the
established pattern for `engine.alpha_signals`, `engine.ai_brain`,
`engine.long_range_sensors`, `watchdog.py`, `engine.momentum.bridge` —
9 in-process callers verified earlier today.**

The dashboard's FastAPI backend ALSO runs on bigmac. So the only thing
missing for Option C is a thin proxy layer that the **browser** can hit —
dashboard makes the SC call server-side.

## Option matrix (refreshed)

### A. Session cookie share
- Dashboard hits `/login`, captures session cookie, forwards on each SC request
- Pattern already exists in reverse: SC's `_bridge_get` does this to reach `:8080`
- **Pro:** Works for cross-machine setups (if dashboard moves off bigmac)
- **Con:** Cookie expiry management, refresh logic, session-state in dashboard
- **Estimated time:** 3-4 hours
- **Blast radius:** dashboard/app.py adds a session-management helper
- **Rollback:** revert the helper module
- **OllieTrades-specific con:** unnecessary complexity given localhost
  bypass is already free

### B. API-key bypass
- Add `X-SC-API-Key` header support to `_auth_gate`; bypass session check on valid key
- Generate per-caller keys, rotate via env var
- **Pro:** Cleanest auth model, easy to revoke per-client
- **Con:** **Requires signal-center/server.py code change** + Signal Center restart
- **Estimated time:** 2 hours (SC change) + 2 hours (dashboard wiring) = 4 hours
- **Blast radius:** signal-center/server.py + secret in `.env` + dashboard wiring
- **Rollback:** revert SC patch + restart SC
- **OllieTrades-specific con:** Signal Center is shared infrastructure;
  modifying its auth gate is higher-risk than dashboard-side proxy

### C. ✅ Dashboard proxy layer  (RECOMMENDED)
- Dashboard adds FastAPI routes like `@app.get("/api/sc/predictions/top5")`
  that internally call `requests.get("http://127.0.0.1:9000/api/predictions/top5")`
- **Pro:** Zero Signal Center change. Zero new auth state. Localhost bypass
  already grants the SC-side access. Dashboard's existing 2FA TOTP + RBAC
  becomes the perimeter. Per-endpoint exposure under dashboard control.
- **Con:** Adds ~1ms localhost-loopback latency per call. Dashboard process
  becomes the gatekeeper — must validate caller via dashboard's own auth.
- **Estimated time:** 30 min per Tier-1 endpoint (5 endpoints) = ~2.5 hours
- **Blast radius:** dashboard/app.py, additive routes
- **Rollback:** delete the new routes
- **OllieTrades-specific pro:** matches the existing direct-SQLite read at
  `dashboard/app.py:11247 /api/signal-center/top` in spirit (dashboard
  proxies SC content); but proxies live SC computation rather than
  duplicating SQL

### D. Direct SQLite reads
- Add more direct-DB readers like `/api/signal-center/top`
- **Pro:** Already proven pattern; zero auth concern
- **Con:** **Loses SC's computed values.** `/api/signals/scorecard` is not a
  simple SELECT — it computes win-rates, normalizes, ranks. Replicating
  that in dashboard means duplicating Python logic from SC. `/api/predictions/top5`
  ditto.
- **Estimated time:** 1-2 hours per endpoint that needs replication =
  ~5-10 hours total + ongoing maintenance to keep dashboard SQL in sync
  with SC code changes
- **Blast radius:** dashboard/app.py grows; risk of SQL drift vs SC logic
- **Rollback:** delete the SQL readers
- **OllieTrades-specific con:** SC owns the canonical computation; duplicating
  it violates single-source-of-truth and creates a future "which one is
  right" question

## Tier-1 endpoints — priority by user-visible value

From `project_hm_an_bridge_auth_blocker.md`, prioritized for Phase 1:

| Rank | Endpoint | What it gives the user | Implementation note |
|---:|---|---|---|
| 1 | `/api/predictions/top5` | "Top 5 buy signals today" — the morning glance content | Highest daily-readership panel |
| 2 | `/api/intelligence-summary` | Master score + grade rollup | Captain's "is today bullish/bearish" widget |
| 3 | `/api/signals/scorecard` | Per-agent win-rates | Fleet calibration / Ghost system insight |
| 4 | `/api/predictions/leaderboard` | Historical prediction accuracy ranking | Builds Captain confidence; also useful for retiring poor performers |
| 5 | `/api/signals/outcomes` | Trade outcome audit (tracked_high, tracked_low, would_hit_*) | Debugging signal logic; lower daily value than 1-4 |

Phase-1 implementation order: ship 1, ship 2, ship 3 (the daily-glance set),
then 4 + 5 as backfill.

## Edge cases

### Concurrent dashboard sessions
**Option C:** No issue. Each dashboard request triggers a server-side
SC call. SC sees the bigmac request from 127.0.0.1 regardless of which
browser session originated it. The dashboard's own session model
(per-user TOTP) handles user identity at the dashboard layer.

### Mobile / external access
**Option C:** Works via the existing Cloudflare tunnel pattern.
Browser → `bridge.ollietrades.com` (Cloudflare) → bigmac dashboard:8080
→ SC localhost. The tunnel never sees `:9000` directly. Better security
posture than exposing `:9000` externally.

### CORS
**Option C:** No CORS issue. Dashboard's frontend (`dashboard/static/index.html`)
is served by the same FastAPI process that proxies to SC — same origin,
no cross-origin headers needed.
**Option D:** Also no CORS issue (same reason).
**Option A/B:** Would only have CORS issues if dashboard JS called `:9000`
*directly* from the browser — neither option requires that.

### Bonnie's observer-level access
Per CLAUDE.md "Network Bindings": **browser users → 2FA TOTP + RBAC at the
Signal Center server layer**. With Option C, the proxy endpoints live on
dashboard. Bonnie reaches dashboard via the same Cloudflare tunnel with
her observer credentials; dashboard's RBAC determines which proxy
endpoints she can hit. Captain controls per-route exposure server-side
in `dashboard/app.py`.

**Concrete:** mark proxy routes with a dashboard-side RBAC decorator
(if one exists; otherwise add one). Observer-tier sees `/api/sc/predictions/top5`
and `/api/sc/intelligence-summary` (read-only daily glance) but NOT
`/api/sc/signals/<id>/execute` or other state-modifying paths if we ever
add Phase 3 writes.

### What happens if Signal Center restarts mid-request?
- Option A: cookie likely invalidates; `_bridge_get`-style retry logic
  re-logs-in. Adds complexity.
- Option B: token persists across SC restarts (env-var driven). Clean.
- Option C: 127.0.0.1:9000 returns connection refused; proxy returns 503
  to dashboard caller; dashboard tile shows error state. Simple, observable.
- Option D: same as C if SC has the DB file open with WAL — possible read
  contention, but generally fine since dashboard opens its own conn.

### Cache poisoning risk
**Option C** + proxy with TTL cache (e.g. `@timed_cache(120)` like
`/api/signal-center/top`): if SC briefly returns stale or wrong data
during a glitch, dashboard caches it. Mitigate by short TTL (60-120s)
for Tier-1 endpoints; expire on dashboard restart.

## Recommended implementation plan

### Phase 1 — Tier-1 proxies (this batch)
Five dashboard routes added in `dashboard/app.py`:

```python
import requests
from functools import lru_cache

_SC_BASE = "http://127.0.0.1:9000"
_SC_TIMEOUT = 5  # SC localhost bypass = fast; 5s is generous

def _sc_proxy_get(path: str) -> dict | None:
    try:
        r = requests.get(f"{_SC_BASE}{path}", timeout=_SC_TIMEOUT)
        return r.json() if r.status_code == 200 else None
    except Exception:
        return None

@app.get("/api/sc/predictions/top5")
@timed_cache(120)
def sc_predictions_top5():
    return _sc_proxy_get("/api/predictions/top5") or {"items": []}

# … one per Tier-1 endpoint
```

**Test plan (TDD):**
- `tests/test_sc_proxy.py` — mock `requests.get`, assert proxy returns
  upstream payload, returns sentinel on timeout / non-200
- Live smoke: `curl http://localhost:8080/api/sc/predictions/top5` returns
  same payload as direct SC `/api/predictions/top5` (modulo cache)

### Phase 2 — Extended proxies (next batch)
Audit the remaining 42 SC endpoints by use case; promote to proxy as
dashboard panels are designed. Don't proxy speculatively — each one
needs a UI consumer.

### Phase 3 — Bidirectional write paths (deferred)
For dashboard-initiated writes (dismiss signal, mark executed, etc.):
- Use Option C still — dashboard POSTs to `/api/sc/...` proxy → server-side
  forwards to SC localhost
- Idempotency keys for retries
- Server-side validation BEFORE forwarding (dashboard becomes the policy
  enforcer, not SC)

## Why NOT Option C?

The only scenario where Option C falls short:
- **Dashboard moves off bigmac.** If we ever split dashboard onto a
  separate host, the localhost bypass evaporates. In that scenario, fall
  back to Option B (API key) — change is additive, not destructive.

No current plans to split the host (CLAUDE.md confirms bigmac as the
single research box). So Option C is right for today + the foreseeable
horizon, with Option B as the migration escape hatch.

## Captain action

- [ ] Review this doc
- [ ] Decide A / B / C / D (or "hybrid: C now, B later if hosts split")
- [ ] On approval: Scotty opens `hm-an-bridge-phase1-tier1-proxies` branch
  with 5 proxy routes + tests

## Files

- This doc: `data/scotty_hm_an_bridge_auth_decision_2026-05-15.md`
- Memory: `project_hm_an_bridge_auth_ready.md` (auto-memory)
- Yesterday's scoping context: `data/scotty_proposals/hm_an_scope.md`,
  `data/scotty_hm_an2_discovery.md`, `archive/scotty_directives/HM-AN2.md`
- Auth gate source: `signal-center/server.py:565-590`
- Existing localhost-bypass callers: `engine/alpha_signals.py`,
  `engine/ai_brain.py`, `engine/long_range_sensors.py`, `engine/signal_poster.py`,
  `engine/premarket_scanner.py`, `engine/danelfin_parser.py`,
  `engine/crew_scanner.py`, `engine/momentum/bridge.py`, `watchdog.py`
