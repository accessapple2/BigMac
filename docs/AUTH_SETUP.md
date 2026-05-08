# Admin Auth Setup — Admiral's Runbook

Phase 0 of `DASHBOARD_AUTH_PLAN.md`. The auth helper at `dashboard/auth.py`
is a pure dependency — no routes are gated on it yet. Before Phase 1 wiring
lands, the Admiral must populate three secrets. This file is the runbook.

> **Important.** Scotty does NOT generate any of these values. Generation is
> the Admiral's task — the helper code never sees the raw recovery key, and
> the codebase contains no real TOTP secret or service token. All three live
> only in `.env` (mode 600) and the Admiral's authenticator app.

---

## 0. Prereqs

- `pyotp` is already a dep in the trader venv (`/Users/bigmac/autonomous-trader/venv`).
- `qrencode` is recommended for terminal-rendered QR codes:
  `brew install qrencode`

---

## 1. Generate the TOTP secret

```bash
cd ~/autonomous-trader
./venv/bin/python3 -c "import pyotp; print(pyotp.random_base32())"
```

Copy the 32-char base32 string. Add to `~/autonomous-trader/.env`:

```
OLLIETRADES_TOTP_SECRET=JBSWY3DPEHPK3PXP...   # the value you just generated
```

Confirm `.env` is mode 600:

```bash
chmod 600 ~/autonomous-trader/.env
ls -la ~/autonomous-trader/.env  # should show -rw-------
```

### Render a QR code for your authenticator app

```bash
SECRET="<the value above>"
LABEL="OllieTrades%20Admin"
ISSUER="OllieTrades"
URL="otpauth://totp/${LABEL}?secret=${SECRET}&issuer=${ISSUER}"
qrencode -t ANSIUTF8 "${URL}"
```

Scan with the iPhone authenticator (1Password, Authy, Google Authenticator,
etc.). Verify by reading a 6-digit code and:

```bash
./venv/bin/python3 -c "
import os, pyotp
print('valid:', pyotp.TOTP(os.environ['OLLIETRADES_TOTP_SECRET']).verify('123456', valid_window=1))
" </dev/null
# replace 123456 with the live code from the app
```

A `True` confirms enrollment. After this point, the secret can be deleted
from your terminal scrollback — `.env` holds the canonical copy.

---

## 2. Generate the service-account token

This is the bearer token automation will send (Schwab CSV watcher, off-host
backup verifier, model watcher, etc.) when calling mutating routes.

```bash
./venv/bin/python3 -c "import secrets; print(secrets.token_urlsafe(48))"
```

Add to `.env`:

```
OLLIETRADES_SERVICE_TOKEN=<the 64-char URL-safe string>
```

Distribute by *value*, not by reference: paste the string into each
automation's environment / launchd plist / systemd unit. Do not leave the
raw value in any file outside `.env` and the consumer scripts.

---

## 3. Generate the recovery key + hash

The recovery key is shown to you ONCE. Only its sha256 hash is stored on
disk. If you lose your TOTP secret AND your service token, you can recover
admin access by submitting the raw recovery key as a bearer token. It is
consumed on first use — a sentinel file at `~/.ollietrades-recovery-used`
prevents reuse without re-running this section.

```bash
./venv/bin/python3 - <<'PY'
import hashlib, secrets
key = secrets.token_urlsafe(32)
print("RECOVERY KEY (write down NOW, store offline):")
print("  " + key)
print()
print("Add to .env:")
print(f"  OLLIETRADES_RECOVERY_KEY_HASH={hashlib.sha256(key.encode()).hexdigest()}")
PY
```

Steps:

1. Write the **RECOVERY KEY** on paper or store in a password manager
   marked "OllieTrades recovery — do not share". This is the only copy.
2. Paste the `OLLIETRADES_RECOVERY_KEY_HASH=...` line into `.env`.
3. Clear your terminal: `clear; reset` (or close the window).
4. If `~/.ollietrades-recovery-used` exists from a previous use, delete it
   so the new key is ready: `rm -f ~/.ollietrades-recovery-used`

To rotate after consumption: re-run this section, replace the hash in
`.env`, delete the sentinel file, restart the trader to reload env.

---

## 4. Final `.env` shape

```
# ... existing keys ...
OLLIETRADES_TOTP_SECRET=JBSWY3DPEHPK3PXP...
OLLIETRADES_SERVICE_TOKEN=Bp7j-...
OLLIETRADES_RECOVERY_KEY_HASH=ab12...  # 64 hex chars (sha256)
```

```bash
chmod 600 ~/autonomous-trader/.env
```

---

## 5. Verify all three before Phase 1 wiring

```bash
cd ~/autonomous-trader
./venv/bin/python3 -m pytest tests/test_auth.py -v
```

Expect 11/11 PASSED. The tests use disposable values (not the real ones),
but a green pytest confirms the helper is wired into the trader venv
correctly.

To smoke-test against your real `.env`, use a one-off FastAPI app:

```bash
./venv/bin/python3 - <<'PY'
import os
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from dashboard.auth import verify_admin_token

app = FastAPI()

@app.get("/whoami")
def whoami(src: str = Depends(verify_admin_token)):
    return {"src": src}

c = TestClient(app)
print("missing  ->", c.get("/whoami").status_code)
print("service  ->", c.get("/whoami", headers={"Authorization": f"Bearer {os.environ['OLLIETRADES_SERVICE_TOKEN']}"}).json())
# Add a TOTP code from your phone:
totp = input("6-digit TOTP code: ").strip()
print("totp     ->", c.get("/whoami", headers={"Authorization": f"Bearer {totp}"}).json())
PY
```

---

## 6. Phase 1 wiring (NOT in this runbook)

`dashboard/app.py` route migration is gated on a separate Admiral go. When
that ships, each mutating route grows one parameter:

```python
@app.post("/api/kill-switch")
def kill_switch(_: str = Depends(verify_admin_token)):
    ...
```

Until then, the helper exists, is tested, and is ready — no routes change.
