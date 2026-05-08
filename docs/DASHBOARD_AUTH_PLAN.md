# Dashboard Auth Plan (HM-AY-β)

**Author:** Scotty 2.4 (Claude Code Opus 4.7)
**Date:** 2026-05-07 ~20:30 MST
**Status:** **Plan only — read-only on `dashboard/app.py`.** No edits to mutating routes performed by this document.
**Source audit:** `docs/SCOTTY_AUDIT_2.md` Section N (Security Tail).
**Priority:** **P0** — biggest unmitigated security gap on the trader.

---

## 1. Problem Statement

`dashboard/app.py` (port 8080, 17,773 lines) has **zero authentication on any mutating route**. Verified: `grep -nE "verify_token|require_admin|verify_admin|auth_required|Security\(|Depends\(\s*verify|Depends\(\s*get_current" dashboard/app.py` returns **0 matches**. The `_v1_auth_error` helper exists (line 15636) but is not wired into any `Depends()` injection.

Currently the trader dashboard relies entirely on:
- localhost binding for direct curl
- Cloudflare tunnel (`bridge.ollietrades.com`) for external access
- Network ACLs (effectively trust-on-LAN)

There is **no application-layer auth**. Any LAN intruder + browser pin into the Cloudflare-fronted hostname → full kill-switch authority + arbitrary trade authority.

**This is not the same problem as HM-AW.3.** HM-AW.3 is about Signal Center's hard-coded `PIN=2026` (line `signal-center/server.py:179`). The dashboard auth gap is a separate, larger problem; bundle them or split per Section 6 below.

---

## 2. Confirmed Mutating Routes — `POST/PUT/DELETE`

49 mutating routes total in `dashboard/app.py`. The 12 most critical (audit Section N called these out) plus the additional ones surfaced this pass:

### Tier S — kill switches and trade authority (must auth first)

| Route | Line | Effect |
|---|---|---|
| `POST /api/kill-switch` | 6499 | Closes ALL positions, ALL models |
| `POST /api/alpaca/buy` | (not in app.py — verify; per audit Sec N at 8679) | Live Alpaca paper buy |
| `POST /api/alpaca/sell` | (8698 per audit) | Live Alpaca paper sell |
| `POST /api/trade/manual` | (8644 per audit) | Manual market order to Alpaca |
| `POST /api/arena/player/{player_id}/buy` | 7713 | DCA buy on any player |
| `POST /api/arena/player/{player_id}/trim` | 7732 | Trim any player's position |
| `POST /api/arena/player/{player_id}/close` | 7758 | Close any player's position |
| `POST /api/admin/clean-stale-snapshots` | 8139 | DB mutation |

### Tier A — fleet control (auth required, less destructive but high blast radius)

| Route | Line | Effect |
|---|---|---|
| `POST /api/model-control/pause-all` | 7907 | Global pause toggle |
| `POST /api/agents/{player_id}/pause` | 8068 | Per-agent pause |
| `POST /api/agents/{player_id}/unpause` | 8084 | Per-agent unpause |
| `POST /api/fleet/reduce-size` | 8098 | Fleet sizing change |
| `POST /api/settings/pause-all` | 7946 | Settings pause |
| `POST /api/model-control/pause/{player_id}` | 7960 | Per-agent pause via control surface |
| `POST /api/wheel/force-scan` | 7786 | Trigger scan |
| `POST /api/arena/force-scan/{player_id}` | 7799 | Per-player scan trigger |
| `POST /api/model-control/force-scan` | 8163 | Global scan force |
| `POST /api/navigator/universe/scan` | 8362 | Universe scan force |
| `POST /api/autopilot/toggle` | 6071 | Autopilot on/off |

### Tier B — data-mutating but lower risk

| Route | Line | Effect |
|---|---|---|
| `POST /api/metals/add` | 4362 | Metals ledger insert |
| `POST /api/metals/sell` | 4375 | Metals ledger sell |
| `POST /api/metals/set-cost` | 4388 | Metals cost basis edit |
| `POST /api/dilithium/add-purchase` | 4407 | Dilithium reserve insert |
| `POST /api/cto/generate` | 4427 | CTO briefing generate |
| `POST /api/webull/sync` | 5024 | Webull import |
| `POST /api/rikers-log` | 4109 | Riker log entry |
| `POST /api/rikers-log/{entry_id}/outcome` | 4127 | Riker log outcome |
| `POST /api/rikers-log/sync-spock` | 4136 | Riker→Spock sync |
| `POST /api/risk/spock-alerts/{alert_id}/acknowledge` | 8008 | Alert ack |
| `POST /api/notifications/{notif_id}/ack` | 8058 | Notif ack |
| `POST /api/flash-alerts/{alert_id}/dismiss` | 3625 | Alert dismiss |
| `POST /api/shorts/cover/{symbol}` | 3685 | Short cover trigger |
| `POST /api/news/go-deeper` | 3268 | News ML query |
| `POST /api/first-officer/ask` | 4084 | First Officer query |
| `POST /api/ai-chat` | 7407 | AI chat write |
| `POST /api/backtest/save-result` | 6118 | Backtest save |
| `POST /api/quorum/start` | 5889 | Quorum start |
| `POST /api/gaps/scan` | 5748 | Gap scan |
| `POST /api/theta/scan` | 5794 | Theta scan |
| `POST /api/model-control/fallbacks` | 7922 | Fallback config |
| `POST /api/model-control/record-call/{player_id}` | 8119 | Per-call accounting |
| `POST /api/war-room/post` | 5065 | War-room post |
| `POST /api/war-room/trigger` | 5180 | War-room trigger |
| `POST /api/war-room/hail-q` | 5238 | Q hail |
| `POST /api/war-room/command` | 5278 | War-room command |
| `POST /api/war-room/top-picks` | 5349 | Top picks |
| `POST /api/war-room/poll` | 5379 | War-room poll |
| `POST /api/war-room/challenge` | 5423 | War-room challenge |
| `POST /api/war-room/portfolio-review` | 5464 | Portfolio review |
| `POST /api/bridge/force-vote` | 153 | Force vote in bridge |
| `POST /api/debug/broadcast-test` | 351 | Debug broadcast |

### Tier C — auth pages (NOT mutating; should remain open)

| Route | Line | Effect |
|---|---|---|
| `POST /login` | 833 | Login attempt |
| `POST /login/pin` | 974 | PIN entry |

These are the auth surface itself; do not gate on `verify_admin_token`.

---

## 3. Proposed Pattern

Single dependency injected at every mutating route via FastAPI `Depends`. One auth source of truth.

```python
# dashboard/auth.py (new file, ~80 LOC)
from fastapi import Header, HTTPException
import os, hmac
from datetime import datetime, timezone
import pyotp  # TOTP — already a dep if HM-AW.3 lands first

ADMIN_TOTP_SECRET = os.environ["ADMIN_TOTP_SECRET"]   # from .env
ADMIN_BEARER      = os.environ.get("ADMIN_BEARER")    # service-account fallback
RECOVERY_KEY_PATH = os.path.expanduser("~/.ollietrades-recovery")

def _check_recovery(token: str) -> bool:
    """One-shot recovery key (mode 600). Consumed on use."""
    try:
        with open(RECOVERY_KEY_PATH) as f:
            stored = f.read().strip()
        if hmac.compare_digest(token, stored):
            os.remove(RECOVERY_KEY_PATH)  # one-shot
            return True
    except FileNotFoundError:
        pass
    return False

def verify_admin_token(authorization: str = Header(None)) -> str:
    """Accepts: Bearer <TOTP>  |  Bearer <ADMIN_BEARER>  |  Bearer <recovery>."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    token = authorization[7:]
    if ADMIN_BEARER and hmac.compare_digest(token, ADMIN_BEARER):
        return "service-account"
    if pyotp.TOTP(ADMIN_TOTP_SECRET).verify(token, valid_window=1):
        return "admin-totp"
    if _check_recovery(token):
        return "recovery"
    raise HTTPException(status_code=401, detail="invalid token")
```

Per-route injection:

```python
from fastapi import Depends
from dashboard.auth import verify_admin_token

@app.post("/api/kill-switch")
def kill_switch(_: str = Depends(verify_admin_token)):
    ...
```

**Why TOTP-bearer not session-cookie:** automation (Schwab CSV watcher, off-host backup verifier) can store a `ADMIN_BEARER` service-account token in `.env` (mode 600) and submit `Authorization: Bearer ${ADMIN_BEARER}` programmatically. Browser users get TOTP. Bonnie's observer view never hits these routes (read-only) — no auth burden.

**Lockout recovery:** `~/.ollietrades-recovery` mode 600, one-shot key. Generated by initial setup. Consumed when used. Admiral regenerates by recreating the file. Survives TOTP secret loss without forcing a code-edit.

---

## 4. Per-Route LOC Delta Estimate

```python
# Before
@app.post("/api/kill-switch")
def kill_switch():
    ...

# After
@app.post("/api/kill-switch")
def kill_switch(_: str = Depends(verify_admin_token)):
    ...
```

**+1 line per route.** ~50 mutating routes (Tier S+A+B) → ~50 lines diff in `dashboard/app.py`.

Plus `dashboard/auth.py` new file (~80 LOC).

Plus `.env` additions: `ADMIN_TOTP_SECRET=...` and `ADMIN_BEARER=...` (already chmod 600).

---

## 5. Test Plan

### Unit
- `pytest tests/test_dashboard_auth.py` — verify TOTP accept, bearer accept, recovery accept, invalid reject, missing-header reject.

### Integration
- Run dashboard with auth wired. From shell:
  ```bash
  curl -X POST http://localhost:8080/api/kill-switch  # → 401
  curl -X POST http://localhost:8080/api/kill-switch -H "Authorization: Bearer $(oathtool -b --totp $TOTP_SECRET)"  # → 200
  curl -X POST http://localhost:8080/api/kill-switch -H "Authorization: Bearer $ADMIN_BEARER"  # → 200
  ```

### Bonnie observer flow
- Bonnie reads `/dashboard` (GET) — no auth required (it's a GET for HTML). Unaffected.
- Bonnie cannot mutate. If she clicks any "Buy/Trim/Close" button on a player card, the JS POST goes through the same `verify_admin_token` path. Without TOTP, she sees a 401 and a friendly error (TBD: front-end TOTP entry modal — out of scope for this doc).

### Schwab CSV watcher flow
- `scripts/import_schwab_csv.py` does NOT call any dashboard route. Unaffected.

### Off-host backup
- `scripts/offhost_backup.sh` does not call any dashboard route. Unaffected.

### Internal cron / launchd
- `crew_scanner.py`, `risk_radar.py`, `riker_synthesis.py` etc. — do they call `/api/...` mutating routes from inside the same process? Audit needed before enabling. Likely no (they hit `engine/` modules directly).

---

## 6. Migration Order

1. **Phase 0 — auth helper lands first** (1 PR): `dashboard/auth.py` + tests + `.env` keys + recovery file generator script. No routes touched yet. Merge + restart trader to load env.
2. **Phase 1 — Tier S routes only** (1 PR, ~10 lines diff): kill-switch, Alpaca buy/sell, trade/manual, arena/buy/trim/close, admin/clean-stale-snapshots. **Highest blast radius first.**
3. **Phase 2 — Tier A routes** (1 PR): pause-all, agent pause/unpause, force-scan family, autopilot/toggle, fleet/reduce-size.
4. **Phase 3 — Tier B routes** (1 PR): metals/dilithium/cto/webull, war-room, riker, ack family.

Each phase runs ≥48h soak before next. If any internal automation breaks (401 from a script that should have a service-account bearer), fix the script in the same phase.

---

## 7. Risks

### R1 — Lockout (TOTP secret loss)
**Mitigation:** `~/.ollietrades-recovery` one-shot file. Generated by Admiral, mode 600, never in git, consumed on first use. Documented in CLAUDE.md.

### R2 — Service-account token leak
**Mitigation:** `.env` mode 600 (verified — see audit Section N: chmod 600, 3789 bytes, 104 keys, not in git). Rotate `ADMIN_BEARER` quarterly per HM-AW.3 broader posture.

### R3 — TOTP clock skew
**Mitigation:** `valid_window=1` allows ±30s drift. Admiral's iPhone TOTP app uses NTP — drift is rare.

### R4 — Internal automation breaks silently
**Mitigation:** Phase 0 (no routes touched) is a 24h verification window. Phase 1 logs every 401 with route name + IP — reviewed before Phase 2.

### R5 — Front-end UX (modal for TOTP entry)
**Out of scope** for this plan. Add as follow-up ticket. For now, Admiral submits via curl or a simple bookmarklet that prompts for TOTP.

---

## 8. Relationship to HM-AW.3

HM-AW.3 (Signal Center 2FA) is currently P2, blocked-on docket. **This plan supersedes the dashboard portion of any "broader auth pass"** — splitting them out makes the dashboard P0 unmistakable.

Recommend split:
- **HM-AW.3** stays P2: rotate Signal Center hard-coded `PIN=2026` to env-derived secret, fail2ban-style lockout. 30 min.
- **HM-AY-β (this plan)** lifts to P0: dashboard auth on mutating routes. ~2 days across 4 phases.

---

## 9. Open Questions for Admiral

1. **Phase 1 timing — flip to P0 immediately or after Sniper Mode KILL (Sat 5/9)?** I recommend immediately; the Cloudflare tunnel is the single weakest link and waiting another 2 days is unjustified.
2. **TOTP app preference — Authy / Google Authenticator / 1Password?** Affects only the QR-code-generation step in setup script.
3. **Service-account bearer per-script or single shared?** Single shared is simpler; per-script (e.g. `OFFHOST_BACKUP_BEARER`, `SCHWAB_WATCHER_BEARER`) is more auditable. I lean per-script for blast-radius.

**Halt condition:** read-only on `dashboard/app.py`. Phase 0 `dashboard/auth.py` is creatable now if Admiral wants — flag for go.
