# HM-AW — Signal Center bind + auth diagnose

**Date:** 2026-05-07
**Status:** **HALTED** at Phase C — 2FA TOTP gap surfaced; LAN bind rolled back
**Author:** Scotty (Claude)

## TL;DR

The Captain note in CLAUDE.md says port 9000 should be reopened to network because
"2FA TOTP + multi-user auth (Captain, Bonnie observer, Dad charts) are in place."

Phase A surfaced the **first gap** (multi-user RBAC missing in port 9000). Captain
elected Option A (LAN + single-user) and a binding change was shipped on commit
`0d3e5dc`.

Phase C (Captain manual LAN verification) surfaced the **second gap**: when Sniff
logged in via the LAN-exposed login page, **password auth succeeded without any
TOTP prompt**. 2FA was advertised but never wired into the login state machine.

Hard Stop #10 fired. Commit `0d3e5dc` was reset (`git reset --hard HEAD~1`).
Service restarted with `host='127.0.0.1'` restored. Listening socket verified
back to `127.0.0.1:9000`. Nothing was pushed to the remote.

HM-AW is **HALTED**. New prerequisite ticket **HM-AW.3** files the 2FA
enforcement work. HM-AW.3 must ship and verify before HM-AW (binding) can land,
and HM-AW.2 (multi-user RBAC) must follow after that for the original mental
model to be true end-to-end.

---

## Findings

### F1 — Bind location

`signal-center/server.py:3084`:
```python
app.run(host='127.0.0.1', port=9000, debug=False, threaded=True)
```

One-line literal. Plist has no override.

### F2 — Plist override

`com.trademinds.signal-center.plist` has no `--host` arg, no `BIND_HOST` env var.
The Python literal at line 3084 is the sole bind authority.

### F3 — Auth gate uniformity

`signal-center/server.py:564 @app.before_request _auth_gate()`:

| Path pattern | Behaviour |
|---|---|
| `/login`, `/logout`, `/robots.txt`, `/static/*` | Always allowed |
| `/api/*` from `request.remote_addr ∈ {127.0.0.1, ::1, localhost}` | Bypassed (line 583) |
| `/api/*` from anywhere else | Requires session |
| Any other path (UI pages) | Requires session, even from localhost |

**Localhost bypass is IP-keyed, not bind-keyed.** Switching `host='0.0.0.0'` does NOT
extend the bypass to LAN clients — they go through the session check. Net safe at
this layer.

### F4 — 2FA TOTP: present but UNREACHABLE (the Phase C bug)

The TOTP infrastructure exists:
- `_SC_TOTP_SECRET = os.environ.get("TOTP_SECRET", "")` (line 40) — read from `.env` via inline loader at lines 21-29
- `_sc_totp = pyotp.TOTP(_SC_TOTP_SECRET) if _SC_TOTP_SECRET else None` (line 41)
- `_SC_TOTP_PAGE` HTML (line 120) renders the 6-digit TOTP form
- Step-2 verification at lines 624-652 checks `_sc_totp.verify(code, valid_window=1)` against `session["totp_pending"]`

**But step-1 NEVER routes to step-2.** Lines 654-665:

```python
# ── Step 1: username + password ───────────────────────────────────────────
username = (request.form.get("username") or "").strip()
password = (request.form.get("password") or "").strip()

if username == _SC_USER and password == _SC_PASS:
    # 2FA disabled — authenticate directly
    session.permanent = True
    session["authenticated"] = True
    session["username"] = username
    _sc_failures.pop(ip, None)
    _sec_log.warning("SC_LOGIN_OK ip=%s user=%s", ip, username)
    return redirect("/")
```

The success branch sets `session["authenticated"] = True` and redirects to `/`
**without ever setting `session["totp_pending"] = True`**. Step-2 is dead code.
The "2FA disabled — authenticate directly" comment is the smoking gun.

This was true while the service was bound to `127.0.0.1` (no external impact —
SSH tunnel was the only access path) and remained true when bound to `0.0.0.0`
(immediate impact — anyone on LAN with valid `_SC_USER` + `_SC_PASS` walks past
the password gate without TOTP).

**Why my Phase A read missed this:** I read the TOTP infra (lines 36-41, 120-170,
623-652) and confirmed `TOTP_SECRET` was set in `.env`. I did not read the step-1
success branch (lines 658-665) end-to-end to verify it sets `totp_pending`. That
was the audit miss.

### F5 — Multi-user RBAC: NOT in signal-center

`signal-center/server.py` reads only:
```python
_SC_USER = os.environ.get("DASHBOARD_USER", "")  # singular
_SC_PASS = os.environ.get("DASHBOARD_PASS", "")  # singular
```

The username check at line 658 is `if username == _SC_USER and password == _SC_PASS:` —
single user, single password.

The multi-user `_parse_users()` registry that supports Sniff/Bonnie/Dad lives in
`dashboard/app.py:557` (port 8080 trader dashboard) and is referenced only from there.

### F6 — Cloudflare tunnel

`/Users/bigmac/.cloudflared/config.yml` exposes only `bridge.ollietrades.com →
localhost:8080`. Adding `signals.ollietrades.com → localhost:9000` would be a separate
ingress entry; not in scope for HM-AW today.

### F7 — Smoke (current state, post-rollback, localhost)

```
GET /                  → 302 → /login    (UI redirect to login — correct)
GET /api/health        → 200             (localhost API bypass — correct)
GET /api/me            → 401             (this route does its own session check; localhost bypass not enough)
GET /login             → 200             (login page — correct)
```

Listen socket post-rollback: `TCP 127.0.0.1:9000 (LISTEN)` ✅

bigmac LAN IP: `192.168.1.248`. Currently unreachable on `:9000` from LAN
(restored to localhost-only).

---

## Phase C result — Captain manual LAN verification (HARD STOP #10)

**What Captain did:**
1. With commit `0d3e5dc` shipped (`host='0.0.0.0'`, service running PID 17902).
2. From a non-bigmac LAN device, opened `http://192.168.1.248:9000/`.
3. Was redirected to `/login`.
4. Entered Sniff username + password.
5. **Was admitted to the dashboard without any TOTP prompt.**

**Diagnosis:** see F4. The step-1 success branch at lines 658-665 sets
`session["authenticated"] = True` directly. The TOTP step-2 path (lines 624-652)
is dead code because nothing in the codebase ever sets `session["totp_pending"]`
to `True`.

**Hard Stop fired correctly.** This is the system working — Captain caught what
Phase A missed.

**Rollback action taken:**
1. `git reset --hard HEAD~1` (undid commit `0d3e5dc` — was unpushed; clean reset)
2. `launchctl kickstart -k gui/$(id -u)/com.trademinds.signal-center`
3. Verified `lsof -nP -iTCP:9000 -sTCP:LISTEN` → `TCP 127.0.0.1:9000 (LISTEN)`
4. Verified `curl http://127.0.0.1:9000/login` → 200
5. No remote push occurred at any point.

**Net state:** identical to pre-HM-AW. Single docs-only commit (this file +
HM-AW HALTED status + new HM-AW.3 ticket) is the only artifact.

---

## Risk register (post-Phase C)

| ID | Risk | Severity | Status |
|----|------|----------|--------|
| R1 | RBAC mismatch — Captain expects Bonnie/Dad to reach 9000 but code is single-user | HIGH | Tracked as HM-AW.2 (sequenced after HM-AW) |
| R2 | TOTP_SECRET available but step-1 never sets `totp_pending` — 2FA is dead code | **HARD STOP #10** | **Tracked as HM-AW.3 — must ship before HM-AW** |
| R3 | Once LAN-exposed, brute-force comes from LAN devices not just localhost | LOW | `_SC_MAX_ATTEMPTS=5` + 15-min IP block handles this; only relevant after HM-AW.3 lands |
| R4 | `/api/*` localhost bypass might mask future security drift | LOW | Acceptable today; flag if a new caller writes sensitive state through it |

---

## Sequencing for the auth+exposure stack

1. **HM-AW.3** — Implement 2FA TOTP enforcement (`signal-center/server.py` step-1 must set `totp_pending` and redirect to `/login?step=2` on password match). **MUST SHIP FIRST.**
2. **HM-AW** — LAN bind change (re-do what was rolled back). Only safe after HM-AW.3 verified.
3. **HM-AW.2** — Port multi-user RBAC from `dashboard/app.py` so Bonnie/Dad can reach 9000 too. Only relevant if Captain wants Bonnie/Dad on 9000.

Until HM-AW.3 ships, the safe posture is `host='127.0.0.1'` (current state).

---

## Lessons (for future Phase A diagnose work)

1. **Read login state machines end-to-end, not by feature.** I confirmed TOTP
   infra existed without confirming it was reachable from password success.
   The fix is "trace from form submit to session set" rather than "find the
   TOTP code path and confirm it works in isolation."

2. **Test the negative.** Phase A should have included an explicit smoke
   confirming Step 2 is reached when Step 1 succeeds — e.g., POST username +
   password and inspect the response for the TOTP form HTML or a
   `Location: /login?step=2` redirect. That single curl would have caught F4.

3. **The harness blocking the LAN curl in Phase C was a feature, not a
   problem.** It forced a Captain-in-the-loop verification from outside the
   sandbox — which is exactly the verification path that surfaced the gap
   that automated curl from inside the sandbox would have missed (it tests
   localhost behaviour, not LAN-as-stranger behaviour).
