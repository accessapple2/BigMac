"""
Unit tests for dashboard/cf_auth.py — CF Access JWT gate.

Acceptance criteria tested:
  3. No CF JWT + no internal token → 401 gate holds.
  4. Internal token present and correct → admitted; incorrect → rejected.
  5. Loopback is NOT trusted on its own (the loopback bypass lives in
     AuthMiddleware and is conditioned on _is_via_cf_tunnel; cf_auth itself
     has no special-case for 127.0.0.1 — confirmed by this module having
     no source-IP check).
"""
from __future__ import annotations

import hmac
import os
from types import SimpleNamespace
from unittest.mock import patch, MagicMock

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_request(headers: dict) -> MagicMock:
    """Fake FastAPI Request with the given headers (lowercase keys)."""
    # r.headers is a plain dict; .get() is the built-in dict method — no override needed.
    r = MagicMock()
    r.headers = {k.lower(): v for k, v in headers.items()}
    return r


# ---------------------------------------------------------------------------
# try_internal_token
# ---------------------------------------------------------------------------

class TestInternalToken:
    def test_correct_token_admitted(self, monkeypatch):
        monkeypatch.setenv("BRIDGE_INTERNAL_TOKEN", "secret123")
        from dashboard.cf_auth import try_internal_token
        assert try_internal_token(_mock_request({"x-internal-token": "secret123"})) is True

    def test_wrong_token_rejected(self, monkeypatch):
        monkeypatch.setenv("BRIDGE_INTERNAL_TOKEN", "secret123")
        from dashboard.cf_auth import try_internal_token
        assert try_internal_token(_mock_request({"x-internal-token": "wrong"})) is False

    def test_missing_token_rejected(self, monkeypatch):
        monkeypatch.setenv("BRIDGE_INTERNAL_TOKEN", "secret123")
        from dashboard.cf_auth import try_internal_token
        assert try_internal_token(_mock_request({})) is False

    def test_unset_env_returns_false_not_error(self, monkeypatch):
        """When BRIDGE_INTERNAL_TOKEN is not set, any header value is rejected (not an error)."""
        monkeypatch.delenv("BRIDGE_INTERNAL_TOKEN", raising=False)
        from dashboard.cf_auth import try_internal_token
        assert try_internal_token(_mock_request({"x-internal-token": "anything"})) is False

    def test_comparison_is_timing_safe(self, monkeypatch):
        """Token comparison must use hmac.compare_digest, not ==."""
        monkeypatch.setenv("BRIDGE_INTERNAL_TOKEN", "real")
        from dashboard import cf_auth
        with patch.object(hmac, "compare_digest", wraps=hmac.compare_digest) as m:
            cf_auth.try_internal_token(_mock_request({"x-internal-token": "real"}))
            assert m.called, "must use hmac.compare_digest"


# ---------------------------------------------------------------------------
# try_cf_access (JWT validation path)
# ---------------------------------------------------------------------------

class TestCFAccess:
    def test_no_header_returns_false(self, monkeypatch):
        monkeypatch.setenv("CF_ACCESS_TEAM_DOMAIN", "t.cloudflareaccess.com")
        monkeypatch.setenv("CF_ACCESS_AUD", "aud123")
        from dashboard.cf_auth import try_cf_access
        assert try_cf_access(_mock_request({})) is False

    def test_valid_jwt_admitted(self, monkeypatch):
        monkeypatch.setenv("CF_ACCESS_TEAM_DOMAIN", "t.cloudflareaccess.com")
        monkeypatch.setenv("CF_ACCESS_AUD", "aud123")
        from dashboard import cf_auth
        with patch.object(cf_auth, "_valid_cf_jwt", return_value=True):
            assert cf_auth.try_cf_access(_mock_request({"cf-access-jwt-assertion": "tok"}))

    def test_invalid_jwt_rejected(self, monkeypatch):
        monkeypatch.setenv("CF_ACCESS_TEAM_DOMAIN", "t.cloudflareaccess.com")
        monkeypatch.setenv("CF_ACCESS_AUD", "aud123")
        from dashboard import cf_auth
        with patch.object(cf_auth, "_valid_cf_jwt", return_value=False):
            assert not cf_auth.try_cf_access(_mock_request({"cf-access-jwt-assertion": "bad"}))

    def test_unconfigured_fails_closed(self, monkeypatch):
        """If env vars are not set, _valid_cf_jwt must return False (fail closed)."""
        monkeypatch.delenv("CF_ACCESS_TEAM_DOMAIN", raising=False)
        monkeypatch.delenv("CF_ACCESS_AUD", raising=False)
        from dashboard import cf_auth
        # Clear JWK client cache so the missing env var is re-evaluated
        cf_auth._jwks_clients.clear()
        result = cf_auth._valid_cf_jwt("some.jwt.token")
        assert result is False, "must fail closed when env vars are absent"

    def test_jwt_decode_exception_returns_false(self, monkeypatch):
        """Any exception from jwt.decode must return False, not propagate."""
        monkeypatch.setenv("CF_ACCESS_TEAM_DOMAIN", "t.cloudflareaccess.com")
        monkeypatch.setenv("CF_ACCESS_AUD", "aud123")
        from dashboard import cf_auth
        cf_auth._jwks_clients.clear()
        mock_client = MagicMock()
        mock_client.get_signing_key_from_jwt.side_effect = Exception("invalid jwt")
        cf_auth._jwks_clients["t.cloudflareaccess.com"] = mock_client
        assert cf_auth._valid_cf_jwt("garbage") is False


# ---------------------------------------------------------------------------
# is_configured
# ---------------------------------------------------------------------------

class TestIsConfigured:
    def test_both_vars_set(self, monkeypatch):
        monkeypatch.setenv("CF_ACCESS_TEAM_DOMAIN", "x.cloudflareaccess.com")
        monkeypatch.setenv("CF_ACCESS_AUD", "someaud")
        from dashboard.cf_auth import is_configured
        assert is_configured() is True

    def test_missing_aud(self, monkeypatch):
        monkeypatch.setenv("CF_ACCESS_TEAM_DOMAIN", "x.cloudflareaccess.com")
        monkeypatch.delenv("CF_ACCESS_AUD", raising=False)
        from dashboard.cf_auth import is_configured
        assert is_configured() is False

    def test_missing_domain(self, monkeypatch):
        monkeypatch.delenv("CF_ACCESS_TEAM_DOMAIN", raising=False)
        monkeypatch.setenv("CF_ACCESS_AUD", "someaud")
        from dashboard.cf_auth import is_configured
        assert is_configured() is False


# ---------------------------------------------------------------------------
# No source-IP special case (criterion 5 — loopback not trusted here)
# ---------------------------------------------------------------------------

class TestNoLoopbackBypass:
    def test_cf_auth_has_no_ip_check(self):
        """cf_auth.py must not contain IP-based bypass *code* (documentation is ok)."""
        import inspect
        from dashboard import cf_auth
        # Check function bodies only — the module docstring may mention IPs educationally.
        fns = [cf_auth._valid_cf_jwt, cf_auth.try_cf_access,
               cf_auth.try_internal_token, cf_auth.is_configured]
        fn_src = "\n".join(inspect.getsource(f) for f in fns)
        for forbidden in ("client.host", "_is_localhost", "request.client", "== \"127.0.0.1\""):
            assert forbidden not in fn_src, (
                f"cf_auth.py function bodies must not contain {forbidden!r} — "
                "loopback bypass lives in AuthMiddleware, not here"
            )
