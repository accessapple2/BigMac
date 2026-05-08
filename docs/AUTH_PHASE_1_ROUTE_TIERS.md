# Auth Phase 1 — Route Inventory + 3-Tier Risk Grouping

**Author:** Scotty 2.8.2 (Phase 3 stretch)
**Date:** 2026-05-08
**Source plan:** `docs/DASHBOARD_AUTH_PLAN.md` Section 2 (Tiers S/A/B/C)
**Source helper:** `dashboard/auth.py` (Phase 0 SHIPPED, commit `53b9113`)
**Hard rule:** **No code changes performed.** Inventory only. Phase 1 wiring
is gated on Admiral go.

---

## 1. Method

The Phase 0 plan listed 49 mutating routes in `dashboard/app.py` plus 2
auth pages that must remain open. This pass:

1. Re-greps the live file (`@app.(post|put|delete)\(`) and counts
   **150** decorated mutating routes — the plan's "49" was a curated
   high-blast-radius set, not the literal total. The 49-route subset
   in the plan remains the right Phase 1 scope; the other ~100 routes
   are noise (sub-router mounts, internal microservice endpoints) and
   are TIER C.
2. Verifies every Tier-S and Tier-A line citation against the current
   `dashboard/app.py` — **all 19 line numbers still accurate** after
   the recent month of edits.
3. Re-tiers per Captain's Phase 3 rubric:
   - **TIER A** = kill switches + Alpaca buy/sell + halt mutations + trade firers
   - **TIER B** = config writes + agent state edits + fleet roster + toggle-page mutations
   - **TIER C** = ntfy-only / log rotations / cosmetic UI state
4. Adds 3 high-risk routes the Phase 0 plan missed
   (`/api/alpaca/close/{symbol}`, `/api/alpaca/close-all`,
   `/api/gateway/kill-switch/{agent_id}`).
5. Marks toggle-page mutations explicitly per the Phase 1 finding in
   `docs/MODEL_TOGGLE_INFRASTRUCTURE_MAP.md`.
6. Per-route LOC delta is uniform: **+1 line** (`Depends(verify_admin_token)`
   parameter), **+1 import** at file head if not already present
   (`from dashboard.auth import verify_admin_token`).

---

## 2. TIER A — kill switches, broker authority, trade firers (11 routes)

**Highest blast radius. Phase 1 ships this tier first.**

| # | Method | Route | Line | Effect | LOC Δ | Special concern |
|---|---|---|---|---|---:|---|
| 1 | POST | `/api/kill-switch` | 6499 | Closes ALL positions, ALL models | +1 | First wire — battle-tests the helper |
| 2 | POST | `/api/admin/clean-stale-snapshots` | 8139 | Mutates `portfolio_history` | +1 | — |
| 3 | POST | `/api/alpaca/buy` | 8679 | Live Alpaca paper buy | +1 | Service-account path required (UOA bot, scripted callers) |
| 4 | POST | `/api/alpaca/sell` | 8698 | Live Alpaca paper sell | +1 | Same |
| 5 | POST | `/api/alpaca/close/{symbol}` | 8717 | Live close one position | +1 | **MISSING from Phase 0 plan — add to Tier A.** |
| 6 | POST | `/api/alpaca/close-all` | 8722 | Live close-all | +1 | **MISSING from Phase 0 plan — add to Tier A.** |
| 7 | POST | `/api/trade/manual` | 8644 | Manual market order to Alpaca | +1 | — |
| 8 | POST | `/api/arena/player/{player_id}/buy` | 7713 | DCA buy on any player | +1 | — |
| 9 | POST | `/api/arena/player/{player_id}/trim` | 7732 | Trim any player's position | +1 | — |
| 10 | POST | `/api/arena/player/{player_id}/close` | 7758 | Close any player's position | +1 | — |
| 11 | POST | `/api/gateway/kill-switch/{agent_id}` | 17174 | Per-agent kill via gateway | +1 | **MISSING from Phase 0 plan — add to Tier A.** |

**Tier A subtotal: 11 routes, +11 LOC + 1 import.**

---

## 3. TIER B — config + agent state + fleet roster + toggle-page (24 routes)

**Includes the model-toggle infrastructure surfaced in Phase 1 of the
toggle-infra map.** These mutate `is_paused`, `pause_all`,
`fallbacks_enabled`, agent halt state, fleet sizing, autopilot, etc.
A compromised endpoint here can silently change which models trade
without firing a single market order — the auth burden is real even
if blast radius is one tick lower than Tier A.

### 3.A Toggle-page mutations (5 routes — per Toggle Infra Map §1.D)

| # | Method | Route | Line | Effect | Toggle-page surface |
|---|---|---|---|---|---|
| 1 | POST | `/api/model-control/pause-all` | 7907 | Toggle global `pause_all` | **YES** — `mc-pause-all-btn` button |
| 2 | POST | `/api/model-control/fallbacks` | 7922 | Toggle `fallbacks_enabled` | **YES** — `mc-fallbacks-btn` button |
| 3 | POST | `/api/model-control/pause/{player_id}` | 7960 | Per-player `is_paused` | **YES** — per-row toggle |
| 4 | POST | `/api/settings/pause-all` | 7946 | Older direct setter (same column) | YES (legacy) |
| 5 | POST | `/api/model-control/record-call/{player_id}` | 8119 | Per-call accounting | YES (called by automation) |

### 3.A.1 HM-AO-β Squeeze Watcher mutations (added 2026-05-08, 1 route)

| # | Method | Route | Line | Effect | Notes |
|---|---|---|---|---|---|
| — | POST | `/api/squeeze/dismiss` | ~17786 | UPDATE squeeze_watch SET dismissed=1 | Admiral dismisses surfaced candidate. Phase 0 helper-stub already in route body (`# TODO Phase 1: enable after Admiral secret-gen`); flip the `Depends` line on top of the existing TODO. Read-side `/api/squeeze/recent` and `/api/squeeze/summary` are NOT mutating — leave open. |

### 3.B Agent state + fleet roster (6 routes)

| # | Method | Route | Line | Effect |
|---|---|---|---|---|
| 6 | POST | `/api/agents/{player_id}/pause` | 8068 | Per-agent pause |
| 7 | POST | `/api/agents/{player_id}/unpause` | 8084 | Per-agent unpause |
| 8 | POST | `/api/fleet/reduce-size` | 8098 | Fleet sizing change |
| 9 | POST | `/api/wheel/force-scan` | 7786 | Trigger scan |
| 10 | POST | `/api/arena/force-scan/{player_id}` | 7799 | Per-player scan trigger |
| 11 | POST | `/api/model-control/force-scan` | 8163 | Global scan force |

### 3.C Config + autopilot + scans (6 routes)

| # | Method | Route | Line | Effect |
|---|---|---|---|---|
| 12 | POST | `/api/navigator/universe/scan` | 8362 | Universe scan force |
| 13 | POST | `/api/autopilot/toggle` | 6071 | Autopilot on/off |
| 14 | POST | `/api/gaps/scan` | 5748 | Gap scan |
| 15 | POST | `/api/theta/scan` | 5794 | Theta scan |
| 16 | POST | `/api/quorum/start` | 5889 | Quorum start |
| 17 | POST | `/api/backtest/save-result` | 6118 | Backtest save |

### 3.D Operator surfaces — metals / dilithium / CTO / Webull (7 routes)

| # | Method | Route | Line | Effect |
|---|---|---|---|---|
| 18 | POST | `/api/metals/add` | 4362 | Metals ledger insert |
| 19 | POST | `/api/metals/sell` | 4375 | Metals ledger sell |
| 20 | POST | `/api/metals/set-cost` | 4388 | Metals cost-basis edit |
| 21 | POST | `/api/dilithium/add-purchase` | 4407 | Dilithium reserve insert |
| 22 | POST | `/api/cto/generate` | 4427 | CTO briefing generate |
| 23 | POST | `/api/webull/sync` | 5024 | Webull import |
| 24 | POST | `/api/shorts/cover/{symbol}` | 3685 | Short cover trigger |

**Tier B subtotal: 24 routes, +24 LOC.**

---

## 4. TIER C — ntfy-only / acks / cosmetic / log entries (17 routes)

**Lowest blast radius.** Many of these write to acknowledgement /
log tables only. Captain's rubric covers these but they're still
mutations and should still get auth — the Phase 0 plan is right that
all 49 belong behind `verify_admin_token`. Just ship last.

| # | Method | Route | Line | Effect |
|---|---|---|---|---|
| 1 | POST | `/api/risk/spock-alerts/{alert_id}/acknowledge` | 8008 | Alert ack |
| 2 | POST | `/api/notifications/{notif_id}/ack` | 8058 | Notif ack |
| 3 | POST | `/api/flash-alerts/{alert_id}/dismiss` | 3625 | Alert dismiss |
| 4 | POST | `/api/rikers-log` | 4109 | Riker log entry |
| 5 | POST | `/api/rikers-log/{entry_id}/outcome` | 4127 | Riker log outcome |
| 6 | POST | `/api/rikers-log/sync-spock` | 4136 | Riker→Spock sync |
| 7 | POST | `/api/news/go-deeper` | 3268 | News ML query |
| 8 | POST | `/api/first-officer/ask` | 4084 | First Officer query |
| 9 | POST | `/api/ai-chat` | 7407 | AI chat write |
| 10 | POST | `/api/war-room/post` | 5065 | War-room post |
| 11 | POST | `/api/war-room/trigger` | 5180 | War-room trigger |
| 12 | POST | `/api/war-room/hail-q` | 5238 | Q hail |
| 13 | POST | `/api/war-room/command` | 5278 | War-room command |
| 14 | POST | `/api/war-room/top-picks` | 5349 | Top picks |
| 15 | POST | `/api/war-room/poll` | 5379 | War-room poll |
| 16 | POST | `/api/war-room/challenge` | 5423 | War-room challenge |
| 17 | POST | `/api/war-room/portfolio-review` | 5464 | Portfolio review |

**Tier C subtotal: 17 routes, +17 LOC.**

(Plus 2 outliers in the Phase 0 plan: `/api/bridge/force-vote` at line
153 and `/api/debug/broadcast-test` at line 351. Bridge/force-vote is a
debug poke at the matrix bridge — recommend Tier B if it can change
votes, Tier C if it's read-back; needs a 5-minute read. Debug broadcast
is genuinely Tier C. Both included in the 49 total.)

---

## 5. Routes that must remain OPEN (no auth)

Per Phase 0 plan Section 2.D:

| Route | Line | Reason |
|---|---|---|
| `POST /login` | 833 | Auth surface itself — gating it on auth is recursive |
| `POST /login/pin` | 974 | Same |

---

## 6. Recommended Phase 1 ship order

| PR | Tier | Routes | LOC | Risk |
|---|---|---:|---:|---|
| Phase 1a | TIER A | 11 | +11 +1 import | High blast radius — battle-tests the helper. Ship first, observe for one trading day. |
| Phase 1b | TIER B (toggle subset) | 5 | +5 | Toggle page — separate PR so a regression in the toggle UI only blames toggle changes. |
| Phase 1c | TIER B (rest) | 19 | +19 | Agent state + ops surfaces. |
| Phase 1d | TIER C | 17 | +17 | Ack / log / cosmetic. Lowest priority but completes the surface. |
| Phase 1e | TIER C outliers | 2 | +2 | `/api/bridge/force-vote`, `/api/debug/broadcast-test`. |

**Total: 54 routes (49 plan + 3 newly-flagged + 2 outliers from plan
section labelled "Tier B" already), +54 LOC + 1 import.**

LOC matches the Phase 0 plan's "+50 LOC" estimate within rounding.

---

## 7. Special concerns for Phase 1

### 7.A Service-account paths
Three automations need to send `Authorization: Bearer ${OLLIETRADES_SERVICE_TOKEN}`
on every Tier A POST:

- `scripts/schwab_csv_watcher.sh` (HM-AY-α #3) — 60s cron, may not call any
  Tier A routes; verify before assuming it needs the token
- `scripts/offhost_backup.sh` (HM-AY-α #1) — daily 06:30, no Tier A calls expected
- `scripts/model_watcher.py` (HM-AY-α #6) — Sunday 09:00, doesn't call dashboard
- **Programmatic dashboard callers** to identify: anything that POSTs to
  `/api/alpaca/*`, `/api/arena/player/*/buy|trim|close`, `/api/trade/manual`,
  `/api/kill-switch`, `/api/admin/clean-stale-snapshots`. A grep
  `localhost:8080` across the repo will surface these — defer to Phase 1a
  prep.

### 7.B Phase 1a smoke test (when shipped)
After wiring TIER A, smoke-test each route with all three auth sources:

- TOTP success → 200, action performed
- Invalid TOTP → 401, no action
- Service token success → 200, action performed
- No header → 401, no action
- Recovery key one-shot → 200, sentinel written, second call 401

Same shape as the Phase 0 pytest suite (11/11 green) but against live
routes.

### 7.C Toggle-page deferred work
- Front-end currently calls `togglePauseAll`, `togglePausePlayer`,
  `toggleFallbacks` from `dashboard/frontend/src/api/client.js:57-59`
  with `fetch(..., { method: 'POST' })` and **no auth header**. Phase 1b
  needs a parallel update to `client.js` to inject the bearer token from
  whatever the front-end already uses for session state. That's a 5-line
  change but it's a *frontend* change, so the Phase 1b commit will span
  both files.
- Static dashboard at `dashboard/static/index.html` calls these via
  `<button onclick="togglePauseAll()">` — same auth header injection
  needed in the inline JS.

---

## 8. Halt condition

**No code changes performed.** This is an inventory pass. The
`verify_admin_token` helper at `dashboard/auth.py` has zero callers
outside its own module + test (verified at Phase 0 commit `53b9113`).
That stays true until Phase 1a ships.
