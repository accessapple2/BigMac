# Relay: HM-TOUR-API-OPEN-ORDER-ENDPOINT — fixed and verified

**Date:** 2026-07-18
**Severity:** RED
**Commit:** (pending — see below)

## Ask

`POST /api/paper/order` on `tour_api.py` (:8088) was confirmed reachable
unauthenticated from LAN (192.168.1.248) and Tailscale (100.103.190.24),
bypassing Cloudflare Access. Fix before Monday open.

## 1. Who legitimately calls this endpoint

Grepped the full repo (excluding venvs) and `~/.cloudflared/config.yml`.
Two on-box callers only, both via `localhost`:

- **cloudflared tunnel** — `~/.cloudflared/config.yml`: `tour.ollietrades.com
  → http://localhost:8088`. cloudflared makes an OUTBOUND connection to the
  Cloudflare edge and reverse-proxies to the local origin — it never needs
  the service to listen on a LAN/Tailscale interface.
- **`scripts/origin_healthcheck.sh`** (cron, every 5 min) — hits
  `http://localhost:8088/api/tour/health` directly.

No frontend in-repo (`dashboard/static/bridge-v2.html`, `swingdesk/cockpit.html`)
calls this endpoint — their `alpaca_paper`/`candidate_id` string hits were false
positives (unrelated code paths). No off-box caller (e.g. olliemax) exists
anywhere in the codebase. **Conclusion: 100% of legitimate traffic is on-box.**

## 2. Fix applied — option (a)

`tour_api.py` bind changed `0.0.0.0` → `127.0.0.1` (one line, `__main__`
block). No other host override exists (`tour_api_start.sh` just runs
`python3 tour_api.py`; no env var overrides the bind). This kills LAN +
Tailscale exposure entirely while cloudflared and the healthcheck (both
localhost) keep working unchanged.

Left deliberately out of scope tonight: `.env` already has an unused
`CF_ACCESS_AUD_TOUR` var (provisioned but never wired into any code path —
`grep` confirms zero references). Origin-level CF Access JWT validation
(mirroring `swingdesk/backend.py`'s `SwingDeskAuthMiddleware`) would be
good defense-in-depth on top of the loopback bind, since it's the same
posture already proven out on swingdesk. **Recommend as a P1 follow-up**,
not tonight's RED fix — the loopback bind alone fully closes tonight's
confirmed exposure and is the minimal, auditable change.

## 3. All routes tour_api serves

- `GET /api/tour/state` — read-only
- `GET /api/tour/ticks` — read-only
- `POST /api/paper/order` — the one write route (fixed above)
- `GET /api/tour/health` — read-only

No other unauthenticated write route exists in this file.

## 4. Abuse audit — `logs/tour_api.log` (4.3M lines, since 2026-06-12 build)

- **Zero successful order submissions, ever.** `grep -ic buy` across the
  entire log = 1 hit, and it's the original 2026-06-12 dev smoke-test
  (`agent=tour-api ... blocked — market_closed_after_hours`, from
  `127.0.0.1`). `Alpaca BUY` (the only log line `AlpacaBridge.buy()` emits
  on a real submit) appears **0 times** in the file's history.
- Non-localhost traffic breakdown (all IPs, all-time):
  - `64.43.89.142` (79), `45.86.210.206` (39), `216.245.152.{6,11,41}` (18),
    `109.104.115.158` (1) — internet scanner/recon traffic (GET `/`,
    `/favicon.ico`, `/docs`, `/openapi.json`, `/welcome`, `/start`, `/app`,
    etc. — classic bot fingerprinting). **No POST attempts from any of these.**
  - `192.168.1.104` (5) — LAN, GET `/` and `/favicon.ico` only (looks like a
    browser tab open on the LAN, not a probe).
  - **`192.168.1.248` (1)** — the one and only POST hit ever, `200 OK`. This
    matches the directive's own confirmation-test IP. Traced it: it hit the
    safe `candidate_id in ("", "__noop__")` self-verify path (`tour_api.py`
    line 442), which returns `{"ok": false, "reason": "noop..."}` **without
    ever calling `bridge.buy()`** — confirmed by the total absence of any
    `agent=tour-api` or `Alpaca BUY` log line anywhere near it.
- **Verdict: no unauthorized paper order was ever placed through this
  endpoint.** The confirmed exposure was real, but nothing exploited it
  beyond the safety-conscious test that surfaced it.

## 5. Restart + live verification (2026-07-18 ~22:46 UTC)

- `scripts/tour_api_restart.sh` run (RED-severity restart permission used,
  no other service touched).
- `lsof -iTCP:8088` post-restart: `127.0.0.1:8088 (LISTEN)` — confirmed
  loopback-only, was `*:8088` before.
- `curl http://192.168.1.248:8088/api/tour/health` → connection refused
  (LAN killed).
- `curl http://100.103.190.24:8088/api/tour/health` → connection refused
  (Tailscale killed).
- `curl http://127.0.0.1:8088/api/tour/health` → `200 {"ok":true,...}`
  (local path intact).
- `curl https://tour.ollietrades.com/api/tour/health` → `302` to
  `cloudflareaccess.com/cdn-cgi/access/login/tour.ollietrades.com` — this is
  CF Access correctly gating the hostname at the edge (expected without a
  session cookie; Access evaluates before origin is ever touched, so this by
  itself doesn't prove tunnel→origin reachability).
- Checked `~/Library/Logs/cloudflared.err`/`.log` for origin errors on the
  8088 route: **zero, ever** (`grep -c 8088` → only 4 hits, all are config-
  reload INF lines, not errors). Confirms the tunnel→localhost:8088 path is
  healthy post-restart.
- `py_compile tour_api.py` clean.

**Unrelated, noted but NOT touched (restart permission was tour_api-only):**
`cloudflared.err` shows a burst of `bridge.ollietrades.com` (port 8080)
origin-refused errors at `2026-07-18T17:58 UTC` — several hours before this
work and already quiet by the time I checked (22:47 UTC, no new lines
since). Looks self-resolved; flagging in case it correlates with something
else tonight, not investigated further per the restart-scope constraint.

## 6. Sweep of other listening ports (Section D.10 inventory)

Live `lsof -iTCP -sTCP:LISTEN`:

| Port | Process | Bind | Write endpoints? | Auth? | Verdict |
|------|---------|------|-------------------|-------|---------|
| 8080 | `main.py` (dashboard) | `0.0.0.0` | yes | `AuthMiddleware`, localhost-bypass keyed on `client.host==127.0.0.1` (LAN/Tailscale must auth) — reviewed+verified safe 2026-06-12 per `docs/runbooks/network-bindings.md` | OK, pre-existing, documented |
| 8088 | `tour_api.py` | was `0.0.0.0`, now `127.0.0.1` | yes (1) | **none** | **FIXED tonight** |
| 8889 | `swingdesk/backend.py` | `0.0.0.0` | yes, several (`/api/trade/plan`, `/api/trade/close`, `/api/swingdesk/spread/submit`, `/api/options/build`, `/api/options/trade/plan`, `/api/circuit-breaker/{trigger,reset}`) | `SwingDeskAuthMiddleware` — same `is_localhost()`-gated posture as 8080, requires CF Access JWT or `X-Internal-Token` for non-localhost | OK, already gated — **not touched** |
| 9000 | signal-center | `127.0.0.1` | — | loopback-only by design | OK |
| 8081 | `engine/mcp_server.py` | `127.0.0.1` | — | loopback-only | OK |
| 8090 | `scripts/status_page.py` | `0.0.0.0` | **no** — `do_GET` only, no `do_POST` anywhere in the file | deliberately no-auth by design (`"public, no-auth, up/down health"`, Admiral-approved 2026-07-02) | OK, read-only by design, no fix needed |
| 5001 | `~/ib_chart/ib_server.py` (separate project, not in this repo) | `0.0.0.0` | **no** — every Flask route is default-GET, no `methods=["POST"]` anywhere | none | OK, read-only chart data server, no order path, no fix needed |

**Only `tour_api` (8088) had the combination of (unauthenticated write route
+ non-loopback bind). Everything else in the inventory is either read-only,
already gated, or already loopback-only.**

## Not done / explicitly out of scope tonight

- CF Access JWT wiring for tour_api (`CF_ACCESS_AUD_TOUR` exists unused) —
  flagged above as a recommended P1 defense-in-depth follow-up, not required
  to close tonight's confirmed exposure.
- `swingdesk`'s `CF_ACCESS_AUD_EXTRA` gap (its Access app may use a
  *different* AUD than Bridge's `CF_ACCESS_AUD`, and nothing currently
  copies `CF_ACCESS_AUD_SWINGDESK`/`_SIGNAL`/`_TOUR` into
  `CF_ACCESS_AUD_EXTRA`) — pre-existing, unrelated to tonight's ticket,
  not touched.
- The `bridge.ollietrades.com` (8080) origin-refused burst at 17:58 UTC
  today — noted in §5, not investigated (out of restart scope).
