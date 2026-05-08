"""
Admin auth helper for OllieTrades dashboard mutating routes.

Phase 0: helper + tests + setup runbook only. NO route wiring is done here —
that lands in Phase 1+ behind an explicit Admiral go-decision.

Three accepted bearer-token sources (`Authorization: Bearer <token>`):

  1. Admin TOTP code from authenticator app
       env: OLLIETRADES_TOTP_SECRET (base32 secret)
  2. Service-account static token (Schwab CSV watcher, off-host backup
     verifier, model watcher, etc.)
       env: OLLIETRADES_SERVICE_TOKEN
  3. One-shot recovery key (lockout escape — used when TOTP secret is lost)
       env: OLLIETRADES_RECOVERY_KEY_HASH (sha256 hex of the raw key)
       sentinel file: ~/.ollietrades-recovery-used (overridable for tests)

Secret values are NEVER generated, hardcoded, or printed by this module.
The Admiral runbook (`docs/AUTH_SETUP.md`) covers generation + storage.

All token comparisons use `hmac.compare_digest` to avoid timing attacks.
"""

from __future__ import annotations

import hashlib
import hmac
import os
from pathlib import Path
from typing import Optional

from fastapi import Header, HTTPException

try:
    import pyotp  # type: ignore
except ImportError:  # pragma: no cover
    pyotp = None  # module still importable; TOTP path simply rejects.

ENV_TOTP_SECRET = "OLLIETRADES_TOTP_SECRET"
ENV_SERVICE_TOKEN = "OLLIETRADES_SERVICE_TOKEN"
ENV_RECOVERY_HASH = "OLLIETRADES_RECOVERY_KEY_HASH"
ENV_RECOVERY_USED_PATH = "OLLIETRADES_RECOVERY_USED_PATH"

DEFAULT_RECOVERY_USED_PATH = "~/.ollietrades-recovery-used"

SOURCE_SERVICE = "service-account"
SOURCE_TOTP = "admin-totp"
SOURCE_RECOVERY = "recovery"


def _recovery_used_path() -> Path:
    raw = os.environ.get(ENV_RECOVERY_USED_PATH, DEFAULT_RECOVERY_USED_PATH)
    return Path(os.path.expanduser(raw))


def _check_service_token(token: str) -> bool:
    expected = os.environ.get(ENV_SERVICE_TOKEN)
    if not expected:
        return False
    return hmac.compare_digest(token, expected)


def _check_totp(token: str) -> bool:
    secret = os.environ.get(ENV_TOTP_SECRET)
    if not secret or pyotp is None:
        return False
    if not token.isdigit():
        return False
    try:
        # pyotp.TOTP.verify uses hmac.compare_digest internally.
        return bool(pyotp.TOTP(secret).verify(token, valid_window=1))
    except Exception:
        return False


def _check_recovery(token: str) -> bool:
    """
    One-shot recovery key.

    Hash-compares the supplied token against the stored sha256 hash. On the
    first match, an empty sentinel file is written; subsequent attempts (even
    with the correct key) reject because the sentinel is present. Admiral
    rotates by deleting the sentinel + storing a new hash via the runbook.
    """
    expected_hash = os.environ.get(ENV_RECOVERY_HASH)
    if not expected_hash:
        return False
    sentinel = _recovery_used_path()
    if sentinel.exists():
        return False
    candidate = hashlib.sha256(token.encode("utf-8")).hexdigest()
    if not hmac.compare_digest(candidate, expected_hash.lower()):
        return False
    try:
        sentinel.parent.mkdir(parents=True, exist_ok=True)
        sentinel.write_text("consumed\n")
        os.chmod(sentinel, 0o600)
    except OSError:
        return False
    return True


def verify_admin_token(authorization: Optional[str] = Header(None)) -> str:
    """
    FastAPI dependency. Returns the auth source label on success; raises
    HTTPException(401) on rejection.

    Source-precedence (cheapest first): service token, TOTP, recovery. The
    one-shot recovery branch only consumes its sentinel when the prior two
    have failed, so a successful TOTP login does not burn the recovery key.
    """
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=401,
            detail={
                "error": "missing_bearer",
                "message": "Authorization: Bearer <token> header required",
            },
        )

    token = authorization[len("Bearer "):]
    if not token:
        raise HTTPException(
            status_code=401,
            detail={"error": "empty_token", "message": "bearer token is empty"},
        )

    if _check_service_token(token):
        return SOURCE_SERVICE
    if _check_totp(token):
        return SOURCE_TOTP
    if _check_recovery(token):
        return SOURCE_RECOVERY

    raise HTTPException(
        status_code=401,
        detail={
            "error": "invalid_token",
            "message": "token did not match any accepted source",
        },
    )
