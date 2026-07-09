"""tests/test_cf_session_identity.py — HM-CF-SESSION-IDENTITY-2026-07-09.

Covers a live-incident follow-up (2026-07-09): the page-route CF Access fix
(HM-CF-ACCESS-PAGE-ROUTES) let a CF-authenticated browser load / and
/classic without hitting the PIN page, but /api/me and /api/active-users
still 401'd -- proving a valid CF JWT exists only answers "can this request
proceed," not "who is logged in." /api/me literally answers the second
question and had no session to read from, so onAuthLost paused alert
pollers and showed a permanent session-expired banner on every reload.

Fix: get_cf_identity() extracts the `email` claim from a validated CF
Access JWT; AuthMiddleware mints a real local session from it -- via
request.state.cf_session_override so the CURRENT request's own route
handler sees it immediately (the persisted cookie only helps requests
AFTER this one), and via the normal trademinds_session cookie so future
requests don't need the CF path at all.
"""
from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import dashboard.app as app_module
import dashboard.cf_auth as cf_auth_module


def _fake_request(path: str, method: str = "GET") -> MagicMock:
    req = MagicMock()
    req.method = method
    req.url = MagicMock()
    req.url.path = path
    req.headers = {"cf-access-jwt-assertion": "fake-jwt-token"}
    req.cookies = {}
    req.client = MagicMock()
    req.client.host = "203.0.113.5"
    req.state = MagicMock()
    req.state.cf_session_override = None
    return req


def _run(coro):
    return asyncio.run(coro)


class GetCfIdentityTests(unittest.TestCase):
    def test_returns_email_from_valid_jwt(self) -> None:
        req = MagicMock()
        req.headers = {"cf-access-jwt-assertion": "token"}
        with patch.object(cf_auth_module, "_decode_cf_jwt",
                           return_value={"email": "steve@example.com", "aud": "x"}):
            identity = cf_auth_module.get_cf_identity(req)
        self.assertEqual(identity, {"email": "steve@example.com"})

    def test_returns_none_without_jwt_header(self) -> None:
        req = MagicMock()
        req.headers = {}
        self.assertIsNone(cf_auth_module.get_cf_identity(req))

    def test_returns_none_for_invalid_jwt(self) -> None:
        req = MagicMock()
        req.headers = {"cf-access-jwt-assertion": "bad-token"}
        with patch.object(cf_auth_module, "_decode_cf_jwt", return_value=None):
            self.assertIsNone(cf_auth_module.get_cf_identity(req))

    def test_returns_none_when_jwt_has_no_email_claim(self) -> None:
        req = MagicMock()
        req.headers = {"cf-access-jwt-assertion": "token"}
        with patch.object(cf_auth_module, "_decode_cf_jwt", return_value={"aud": "x"}):
            self.assertIsNone(cf_auth_module.get_cf_identity(req))


class SessionMintingMiddlewareTests(unittest.TestCase):
    def setUp(self) -> None:
        self.middleware = app_module.AuthMiddleware(app=MagicMock())

    def _dispatch(self, path: str, identity=None):
        req = _fake_request(path)
        route_saw_session = {}

        async def fake_call_next(r):
            # Simulates a route handler (e.g. /api/me) reading the session
            # via the exact same _get_session_data() the real handlers use.
            route_saw_session["data"] = app_module._get_session_data(r)
            resp = MagicMock()
            resp.set_cookie = MagicMock()
            return resp

        call_next = AsyncMock(side_effect=fake_call_next)
        with patch.object(app_module, "_get_session_data",
                           side_effect=lambda r: getattr(r.state, "cf_session_override", None)) as mock_gsd, \
             patch.object(app_module, "_is_localhost", return_value=False), \
             patch.object(app_module, "_is_via_cf_tunnel", return_value=True), \
             patch("dashboard.cf_auth.is_configured", return_value=True), \
             patch("dashboard.cf_auth.try_cf_access", return_value=True), \
             patch("dashboard.cf_auth.try_internal_token", return_value=False), \
             patch("dashboard.cf_auth.get_cf_identity", return_value=identity):
            result = _run(self.middleware.dispatch(req, call_next))
        return req, result, route_saw_session.get("data")

    def test_current_request_route_handler_sees_minted_session(self) -> None:
        """The exact bug: /api/me's own handler must see a session on the
        SAME request that first proved CF identity, not just future ones."""
        req, response, session_seen_by_handler = self._dispatch(
            "/api/me", identity={"email": "steve@example.com"}
        )
        self.assertIsNotNone(session_seen_by_handler)
        self.assertEqual(session_seen_by_handler["username"], "steve@example.com")
        self.assertEqual(session_seen_by_handler["role"], "admin")
        self.assertTrue(session_seen_by_handler["authenticated"])

    def test_persisted_cookie_set_for_future_requests(self) -> None:
        req, response, _ = self._dispatch(
            "/api/me", identity={"email": "steve@example.com"}
        )
        response.set_cookie.assert_called_once()
        args, kwargs = response.set_cookie.call_args
        self.assertEqual(args[0], "trademinds_session")
        self.assertEqual(kwargs.get("httponly"), True)
        self.assertEqual(kwargs.get("samesite"), "strict")

    def test_request_state_override_set_before_call_next(self) -> None:
        req, response, _ = self._dispatch(
            "/api/active-users", identity={"email": "steve@example.com"}
        )
        self.assertEqual(req.state.cf_session_override["username"], "steve@example.com")

    def test_no_identity_no_session_minted(self) -> None:
        """Internal-token-only requests (no CF JWT) must not mint a
        session -- get_cf_identity returns None for those."""
        req, response, session_seen = self._dispatch("/api/status", identity=None)
        self.assertIsNone(session_seen)
        response.set_cookie.assert_not_called()


if __name__ == "__main__":
    unittest.main()
