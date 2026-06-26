"""
Unit tests for the AuthMiddleware decision order in dashboard/app.py.

Spec (XO runbook — fail-open + loopback bypass hardening):
  - Any of cf-ray / cf-connecting-ip / cf-access-jwt-assertion → via_cloudflare=True
  - Loopback bypass admits ONLY (127.0.0.1 or ::1) AND NOT via_cloudflare
  - Browser via CF tunnel (127.0.0.1 + cf-ray) must NOT be admitted by the bypass
  - is_configured()==False → admit (edge is the gate; rollback must work)
  - is_configured()==True + no valid credential → 401
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch


def _req(host: str, headers: dict | None = None) -> MagicMock:
    """Minimal fake Request with client.host and headers dict."""
    r = MagicMock()
    r.client = MagicMock()
    r.client.host = host
    r.headers = {k.lower(): v for k, v in (headers or {}).items()}
    return r


# Import from cf_auth where the canonical implementations live.
# app.py delegates to these same functions; tests don't need to load the full app.
def _funcs():
    from dashboard.cf_auth import is_via_cf_tunnel, is_localhost
    return is_via_cf_tunnel, is_localhost


class TestIsViaCfTunnel:
    def test_cf_ray_alone_signals_tunnel(self):
        fn, _ = _funcs()
        assert fn(_req("127.0.0.1", {"cf-ray": "abc123"})) is True

    def test_cf_connecting_ip_alone_signals_tunnel(self):
        fn, _ = _funcs()
        assert fn(_req("127.0.0.1", {"cf-connecting-ip": "1.2.3.4"})) is True

    def test_cf_access_jwt_alone_signals_tunnel(self):
        fn, _ = _funcs()
        assert fn(_req("127.0.0.1", {"cf-access-jwt-assertion": "tok.tok.tok"})) is True

    def test_no_cf_headers_returns_false(self):
        fn, _ = _funcs()
        assert fn(_req("127.0.0.1", {"user-agent": "engine-scanner/1.0"})) is False

    def test_empty_headers_returns_false(self):
        fn, _ = _funcs()
        assert fn(_req("127.0.0.1")) is False


class TestLoopbackBypassLogic:
    """
    The bypass condition is:
        _is_localhost(request) and not _is_via_cf_tunnel(request)

    This class tests the truth table so the hardened behaviour is pinned.
    """

    def _bypass(self, host: str, headers: dict | None = None) -> bool:
        is_via_cf, is_local = _funcs()
        r = _req(host, headers)
        return is_local(r) and not is_via_cf(r)

    def test_engine_caller_admitted(self):
        # Same-machine process, no CF headers — must pass through.
        assert self._bypass("127.0.0.1") is True

    def test_engine_caller_ipv6_admitted(self):
        assert self._bypass("::1") is True

    def test_browser_via_cf_tunnel_blocked_from_bypass(self):
        # CF tunnel: arrives as 127.0.0.1 but carries cf-ray — must NOT be bypassed.
        assert self._bypass("127.0.0.1", {"cf-ray": "abc123"}) is False

    def test_browser_via_cf_tunnel_jwt_blocked_from_bypass(self):
        # All three CF headers present (real browser session).
        assert self._bypass("127.0.0.1", {
            "cf-ray": "abc",
            "cf-connecting-ip": "1.2.3.4",
            "cf-access-jwt-assertion": "tok.tok.tok",
        }) is False

    def test_lan_peer_not_admitted(self):
        # LAN caller (Tailscale or direct) — not loopback, no CF headers.
        assert self._bypass("192.168.1.50") is False

    def test_external_ip_not_admitted(self):
        assert self._bypass("93.184.216.34") is False


class TestIsConfigured:
    """
    is_configured() controls the fail-open branch — the entire auth activation
    and rollback story depends on these two states being correctly distinct.
    """

    def test_unconfigured_admits(self):
        """is_configured()==False must return False so the middleware admits.
        Rollback = unset vars + restart → this path → dashboard open behind CF edge."""
        with patch.dict("os.environ", {}, clear=False):
            # Ensure neither var is set
            import os
            os.environ.pop("CF_ACCESS_TEAM_DOMAIN", None)
            os.environ.pop("CF_ACCESS_AUD", None)
            from dashboard.cf_auth import is_configured
            assert is_configured() is False

    def test_configured_when_both_vars_present(self):
        """is_configured()==True when both env vars are set — the JWT gate is live."""
        with patch.dict("os.environ", {
            "CF_ACCESS_TEAM_DOMAIN": "ollietrades.cloudflareaccess.com",
            "CF_ACCESS_AUD": "fake-aud-tag",
        }):
            # Force reimport so the patched env is seen
            import importlib, dashboard.cf_auth as _m
            importlib.reload(_m)
            assert _m.is_configured() is True
            importlib.reload(_m)  # restore to unpatched state

    def test_configured_rejects_missing_credential(self):
        """is_configured()==True + no CF JWT + no internal token → try_cf_access and
        try_internal_token both return False, meaning the 401 branch is reached."""
        with patch.dict("os.environ", {
            "CF_ACCESS_TEAM_DOMAIN": "ollietrades.cloudflareaccess.com",
            "CF_ACCESS_AUD": "fake-aud-tag",
            "BRIDGE_INTERNAL_TOKEN": "",
        }):
            import importlib, dashboard.cf_auth as _m
            importlib.reload(_m)
            r = _req("93.184.216.34")  # external IP, no CF headers, no token
            assert _m.try_cf_access(r) is False
            assert _m.try_internal_token(r) is False
            importlib.reload(_m)
