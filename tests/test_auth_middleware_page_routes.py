"""tests/test_auth_middleware_page_routes.py — HM-CF-ACCESS-PAGE-ROUTES-2026-07-09.

Covers a live-incident finding (2026-07-09): dashboard/app.py's AuthMiddleware
only checked the Cloudflare Access JWT (try_cf_access) for paths starting
with /api/. Any page route (/, /classic, etc.) with no local session cookie
fell straight to the /login redirect WITHOUT ever checking the JWT -- even
though CF injects that header on every proxied request, not just API calls.
This violated the file's own documented posture ("CF Access is the gate, the
origin doesn't double-check") specifically for page loads: a CF-authenticated
browser still got bounced to the app's internal PIN login instead of
transparently through.

Fix: the JWT/internal-token check now runs before the branch that decides
between a 401 (API) and a redirect (page) -- both paths get the same
credential check, only the failure response differs.

These tests exercise AuthMiddleware.dispatch directly with a minimal fake
Request, mocking session/localhost/cf_auth helpers to isolate the branch
under test -- no real ASGI server needed.
"""
from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import dashboard.app as app_module


def _fake_request(path: str, headers: dict | None = None, method: str = "GET") -> MagicMock:
    req = MagicMock()
    req.method = method
    req.url = MagicMock()
    req.url.path = path
    req.headers = headers or {}
    req.client = MagicMock()
    req.client.host = "203.0.113.5"  # external, non-localhost IP
    return req


def _run(coro):
    return asyncio.run(coro)


class PageRouteChecksCfAccessTests(unittest.TestCase):
    def setUp(self) -> None:
        self.middleware = app_module.AuthMiddleware(app=MagicMock())
        self.call_next = AsyncMock(return_value="PASSED_THROUGH")

    def _dispatch(self, path: str):
        req = _fake_request(path)
        with patch.object(app_module, "_get_session_data", return_value=None), \
             patch.object(app_module, "_is_localhost", return_value=False), \
             patch.object(app_module, "_is_via_cf_tunnel", return_value=True), \
             patch("dashboard.cf_auth.is_configured", return_value=True):
            return _run(self.middleware.dispatch(req, self.call_next))

    def test_cf_authenticated_browser_loads_root_without_login_redirect(self) -> None:
        """The exact bug: a CF-Access-authenticated browser hitting / must
        pass through, not get redirected to /login."""
        with patch("dashboard.cf_auth.try_cf_access", return_value=True), \
             patch("dashboard.cf_auth.try_internal_token", return_value=False):
            result = self._dispatch("/")
        self.assertEqual(result, "PASSED_THROUGH")
        self.call_next.assert_awaited_once()

    def test_cf_authenticated_browser_loads_classic_without_login_redirect(self) -> None:
        with patch("dashboard.cf_auth.try_cf_access", return_value=True), \
             patch("dashboard.cf_auth.try_internal_token", return_value=False):
            result = self._dispatch("/classic")
        self.assertEqual(result, "PASSED_THROUGH")

    def test_unauthenticated_browser_still_redirected_to_login(self) -> None:
        """Preserves existing behavior: no CF JWT, no session -> redirect,
        not an open door."""
        with patch("dashboard.cf_auth.try_cf_access", return_value=False), \
             patch("dashboard.cf_auth.try_internal_token", return_value=False):
            result = self._dispatch("/")
        self.assertNotEqual(result, "PASSED_THROUGH")
        self.call_next.assert_not_awaited()
        # RedirectResponse to /login
        self.assertEqual(getattr(result, "status_code", None), 303)


class ApiRouteBehaviorUnchangedTests(unittest.TestCase):
    """Guards against regressing the API-route side while fixing the page
    side -- API auth semantics (401 JSON, not a redirect) must be identical
    to before this change."""

    def setUp(self) -> None:
        self.middleware = app_module.AuthMiddleware(app=MagicMock())
        self.call_next = AsyncMock(return_value="PASSED_THROUGH")

    def _dispatch(self, path: str):
        req = _fake_request(path)
        with patch.object(app_module, "_get_session_data", return_value=None), \
             patch.object(app_module, "_is_localhost", return_value=False), \
             patch.object(app_module, "_is_via_cf_tunnel", return_value=True), \
             patch("dashboard.cf_auth.is_configured", return_value=True):
            return _run(self.middleware.dispatch(req, self.call_next))

    def test_api_route_with_valid_jwt_passes_through(self) -> None:
        with patch("dashboard.cf_auth.try_cf_access", return_value=True), \
             patch("dashboard.cf_auth.try_internal_token", return_value=False):
            result = self._dispatch("/api/status")
        self.assertEqual(result, "PASSED_THROUGH")

    def test_api_route_without_credential_gets_401_json_not_redirect(self) -> None:
        with patch("dashboard.cf_auth.try_cf_access", return_value=False), \
             patch("dashboard.cf_auth.try_internal_token", return_value=False):
            result = self._dispatch("/api/status")
        self.assertEqual(getattr(result, "status_code", None), 401)


if __name__ == "__main__":
    unittest.main()
