"""
Tests for dashboard/auth.py — Phase 0 admin auth helper.

Run standalone:
    python3 -m pytest tests/test_auth.py -v

These tests do NOT generate or hardcode any production secrets — they fabricate
disposable test values (fixed strings + a base32 secret produced via pyotp at
import time) and scope them to each test via monkeypatch.

Coverage:
  - valid TOTP -> 200
  - invalid TOTP -> 401
  - service token valid -> 200
  - service token invalid -> 401
  - recovery key valid (one-shot) -> 200, then 401 on reuse
  - missing Authorization header -> 401
  - constant-time check verified (hmac.compare_digest in source)
"""

from __future__ import annotations

import hashlib
import inspect
import sys
from pathlib import Path

import pytest
import pyotp
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard import auth  # noqa: E402
from dashboard.auth import (  # noqa: E402
    ENV_RECOVERY_HASH,
    ENV_RECOVERY_USED_PATH,
    ENV_SERVICE_TOKEN,
    ENV_TOTP_SECRET,
    SOURCE_RECOVERY,
    SOURCE_SERVICE,
    SOURCE_TOTP,
    verify_admin_token,
)

TOTP_SECRET_TEST = pyotp.random_base32()  # disposable, lives only in this test process
SERVICE_TOKEN_TEST = "svc-token-fixture-do-not-use-in-prod-0123456789"
RECOVERY_KEY_TEST = "recovery-key-fixture-do-not-use-in-prod-abcdef"
RECOVERY_HASH_TEST = hashlib.sha256(RECOVERY_KEY_TEST.encode()).hexdigest()


@pytest.fixture
def app(tmp_path, monkeypatch):
    """FastAPI app with a single auth-gated endpoint plus per-test env scope."""
    sentinel = tmp_path / "recovery-used"
    monkeypatch.setenv(ENV_TOTP_SECRET, TOTP_SECRET_TEST)
    monkeypatch.setenv(ENV_SERVICE_TOKEN, SERVICE_TOKEN_TEST)
    monkeypatch.setenv(ENV_RECOVERY_HASH, RECOVERY_HASH_TEST)
    monkeypatch.setenv(ENV_RECOVERY_USED_PATH, str(sentinel))

    fast_app = FastAPI()

    @fast_app.get("/protected")
    def protected(source: str = Depends(verify_admin_token)):
        return {"source": source}

    return fast_app, sentinel


@pytest.fixture
def client(app):
    fast_app, _ = app
    return TestClient(fast_app)


def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def test_valid_totp_returns_200(client):
    code = pyotp.TOTP(TOTP_SECRET_TEST).now()
    r = client.get("/protected", headers=_bearer(code))
    assert r.status_code == 200, r.text
    assert r.json() == {"source": SOURCE_TOTP}


def test_invalid_totp_returns_401(client):
    r = client.get("/protected", headers=_bearer("000000"))
    assert r.status_code == 401
    body = r.json()
    assert body["detail"]["error"] == "invalid_token"


def test_service_token_valid_returns_200(client):
    r = client.get("/protected", headers=_bearer(SERVICE_TOKEN_TEST))
    assert r.status_code == 200
    assert r.json() == {"source": SOURCE_SERVICE}


def test_service_token_invalid_returns_401(client):
    r = client.get("/protected", headers=_bearer("svc-token-fixture-WRONG"))
    assert r.status_code == 401
    assert r.json()["detail"]["error"] == "invalid_token"


def test_recovery_key_valid_then_reuse_rejects(client, app):
    _, sentinel = app
    assert not sentinel.exists()

    r1 = client.get("/protected", headers=_bearer(RECOVERY_KEY_TEST))
    assert r1.status_code == 200, r1.text
    assert r1.json() == {"source": SOURCE_RECOVERY}
    assert sentinel.exists(), "sentinel must be written on successful recovery use"

    # Same key again must reject — sentinel makes it one-shot.
    r2 = client.get("/protected", headers=_bearer(RECOVERY_KEY_TEST))
    assert r2.status_code == 401
    assert r2.json()["detail"]["error"] == "invalid_token"


def test_missing_authorization_header_returns_401(client):
    r = client.get("/protected")
    assert r.status_code == 401
    assert r.json()["detail"]["error"] == "missing_bearer"


def test_non_bearer_scheme_returns_401(client):
    r = client.get("/protected", headers={"Authorization": f"Basic {SERVICE_TOKEN_TEST}"})
    assert r.status_code == 401
    assert r.json()["detail"]["error"] == "missing_bearer"


def test_empty_bearer_token_returns_401(client):
    r = client.get("/protected", headers={"Authorization": "Bearer "})
    assert r.status_code == 401
    assert r.json()["detail"]["error"] == "empty_token"


def test_constant_time_compare_used_in_source():
    """
    Verify hmac.compare_digest is the comparison primitive used for token
    checks. A naive `==` comparison would short-circuit on first byte mismatch
    and leak token material via timing.
    """
    src = inspect.getsource(auth)
    assert "hmac.compare_digest" in src, "auth module must use hmac.compare_digest for token checks"
    # Spot-check that no string-equality token check slipped in next to a known token name.
    forbidden_patterns = [
        "token == expected",
        "expected == token",
        'expected_hash == candidate',
        'candidate == expected_hash',
    ]
    for pat in forbidden_patterns:
        assert pat not in src, f"forbidden non-constant-time compare: {pat}"


def test_totp_path_does_not_consume_recovery(client, app):
    """A successful TOTP login must NOT burn the one-shot recovery key."""
    _, sentinel = app
    code = pyotp.TOTP(TOTP_SECRET_TEST).now()
    r = client.get("/protected", headers=_bearer(code))
    assert r.status_code == 200
    assert not sentinel.exists(), "TOTP success must not consume recovery sentinel"


def test_no_secrets_in_module():
    """
    Sanity: the auth module body must not contain any literal secret values.
    Phase 0 guarantee — secret generation is the Admiral's runbook task.
    """
    body = inspect.getsource(auth)
    assert TOTP_SECRET_TEST not in body
    assert SERVICE_TOKEN_TEST not in body
    assert RECOVERY_KEY_TEST not in body
    assert RECOVERY_HASH_TEST not in body
