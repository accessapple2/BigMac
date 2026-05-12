# HM-AN Phase 1 Closure — 2026-05-10

**Mission:** lay the keel for the Dashboard Remodel v1 read bridge — Signal Center (:9000) → Dashboard backend (:8080) — so the upcoming Race and Scanner tiles have a clean data path.

**Outcome:** ✅ delivered — three atomic commits, no service touched, no pushes, sacred DBs untouched.

---

## Summary

- Discovery: `data/scotty_phase1_discovery.md`
- Commits staged: **3** (1.1 bridge skeleton + 1.2 endpoints + 1.3 docs)
- Service restart required: **YES** (Phase 1.2 added FastAPI routes to `dashboard/app.py`)
- Push: **NOT performed** — Admiral pauses VPN, runs `git push origin main`

## Commits (newest first)

| Hash | Subject |
|------|---------|
| `a349564` | docs(hm-an): bridge documentation for Phase 1 endpoints |
| `2a0b58d` | feat(dashboard): HM-AN Phase 1.2 — `/api/momentum/heartbeat` + `/api/momentum/recent_signals` |
| `c4223fd` | feat(momentum): HM-AN Phase 1.1 — bridge module skeleton |

Total diff: **+252 / -0** across 4 files.

## Files added

| Path | Size | Purpose |
|------|------|---------|
| `engine/momentum/__init__.py` | 0 B | package marker |
| `engine/momentum/bridge.py` | 3.4 KB / 108 lines | `BridgeHealth`, `check_signal_center_health()`, `fetch_recent_signals()` |
| `docs/HM_AN_BRIDGE.md` | 4.6 KB / 109 lines | endpoint specs + Phase 2+ roadmap + restart command |

## Files modified

| Path | Lines | Change |
|------|-------|--------|
| `dashboard/app.py` | +35 | appended two `@app.get` handlers + import line, wrapped in `=== HM-AN Phase 1 ===` anchors |

Endpoint count: 616 → **618** (+2 — exactly the planned delta).

## Three adjustments to the original directive stub (bug fixes, in-scope)

| # | Issue | Fix |
|---|-------|-----|
| 1 | `SELECT id, player_id, symbol, side, …` — column `side` doesn't exist in `signals` schema; would crash at runtime | Changed to `SELECT … signal …` (verified column name in discovery) |
| 2 | `requests.get()` follows redirects by default; `/health` returns 302 → `/login` page (200) → false-positive healthy state | Added `allow_redirects=False` to health probe |
| 3 | Original candidate order `[/health, /api/health, …]` issues 4 needless 302s per heartbeat before reaching the working endpoint | Reordered to `[/api/health, /health, /api/status, /status, /]` |

All three changes documented in `data/scotty_phase1_discovery.md` and traceable in the code via comments referencing the 2026-05-10 discovery.

## Endpoints to test after restart

```bash
# Heartbeat (expects reachable=true, endpoint=/api/health, error=null)
curl -s http://localhost:8080/api/momentum/heartbeat | jq

# Recent signals (last 30 min, capped to 10)
curl -s "http://localhost:8080/api/momentum/recent_signals?since_minutes=30&limit=10" | jq '.count'

# Input bound clamping (should return since_minutes=1440, count<=500)
curl -s "http://localhost:8080/api/momentum/recent_signals?since_minutes=99999&limit=99999" | jq '.since_minutes'
```

Bridge module also passed a live in-process smoke test during Phase 1.1:
```
health: BridgeHealth(signal_center_reachable=True,
                     signal_center_endpoint='/api/health',
                     last_check_ts='2026-05-11T01:39:11Z',
                     error=None)
```

## Next session (Phase 2)

- **Race tile** — top gainers since open
- New module: `engine/momentum/race.py` + `engine/momentum/universe.py`
- New endpoint: `/api/momentum/race`
- Estimated effort: **8–11h** (includes mobile-responsive UI)
- **Data-source decision point:** swap `fetch_recent_signals` from `trader.db` direct read → Signal Center `/api/signals` HTTP endpoint. Signal Center's response carries `entry_price` / `stop_loss` / `take_profit` / `agent_name` / `model_used`, which the Race tile can render without re-deriving prices from `trader.db`. Recommend swap in Phase 2 unless `trader.db.signals` proves to be the higher-signal source over a few days of live data.

## Questions raised

None blocking. Two low-priority observations for future cleanup:

1. **Doc filename convention:** I used `docs/HM_AN_BRIDGE.md` per the directive's literal path, but the rest of `docs/` uses `HM-X_DESCRIPTOR_DATE.md` (dash + descriptor + date). Trivial rename available: `git mv docs/HM_AN_BRIDGE.md docs/HM-AN_BRIDGE_2026-05-10.md`.
2. **Existing SC consumer found in `dashboard/app.py:10979`:** `/api/scanner/live` already reads `signal-center/signals.db` directly from disk (not via the :9000 HTTP service). The prior scope doc (`data/scotty_proposals/hm_an_scope.md`) said "the dashboard does NOT currently read from Signal Center at all" — that was true *for the HTTP path* but slightly understates the picture. The bridge module remains the right abstraction; just noting the existing file-level read flow for completeness.

## Discovery anomaly worth flagging (low priority)

`trader.db.signals` returned **0 rows in trailing 24h** during the Phase 1.1 smoke test. The `signals` table has 9.6k+ historical rows from `energy-arnold` and other emitters, but recent emission appears quiet. Possibilities: (a) Saturday-evening market closure, (b) emission paused, (c) emission landing in a different table. If `(b)` or `(c)`, the recent_signals endpoint will keep returning `count: 0` until Monday or until emission resumes. Worth a verification check post-restart.

## Push readiness

- 3 commits staged on local `main`
- Working tree: dirty with **unrelated** files (`OPS_LOG.md`, `bull_bear_cache.json`, model-watch journal, `trader.db.pre-legacy-flag-*` backups, etc.) — none of these are part of the HM-AN commit set; all predate this session or were generated by unrelated processes
- **Admiral action:**
  1. Pause VPN
  2. `git push origin main` (or selectively cherry-pick if any of the dirty pre-existing state needs to stay local)
  3. Restart trader: `launchctl kickstart -k gui/$(id -u)/com.trademinds.trader`
  4. Run the three `curl` smoke commands from "Endpoints to test after restart"

## Standing-rule audit

| Rule | Status |
|------|--------|
| Sacred DBs untouched (no `rm`, no `VACUUM`) | ✅ read-only `sqlite3` queries only |
| No `rm -rf` on `~/autonomous-trader/`, `~/.claude/`, `data/` | ✅ |
| Diff-then-apply on every edit | ✅ proposed diff shown before each Write/Edit |
| Bytecode reminder (no service restart after .py edits) | ✅ flagged in 1.2 commit + this report |
| One atomic commit per task | ✅ 3 commits, 3 phases, no mixing |
| NTFY on each commit | ✅ 3 NTFYs sent to `ollietrades-crew` |
| Push gate | ✅ no push performed |
| Stop on ambiguity | ✅ no blockers raised; minor questions noted above |
| Idempotent guards on additions | ✅ `=== HM-AN Phase 1 ===` anchors in `dashboard/app.py`; `grep` re-run check before insert |
| No service restarts, no process kills | ✅ |
| Signal Center read-only | ✅ all curls were GET; no edits to `signal-center/server.py` |
