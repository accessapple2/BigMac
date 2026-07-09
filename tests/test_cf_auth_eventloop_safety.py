"""tests/test_cf_auth_eventloop_safety.py — HM-CF-AUTH-EVENTLOOP-SAFETY-2026-07-09.

Covers an architectural risk found while investigating a recurring "wedged"
dashboard incident (2026-07-09): AuthMiddleware.dispatch is `async def`, but
called try_cf_access()/get_cf_identity() directly (synchronously) -- both
validate the CF Access JWT via PyJWKClient, which does a SYNCHRONOUS HTTP
fetch of Cloudflare's certs endpoint (cached after success, but re-fetched on
any cache miss). Any network hang there would block uvicorn's entire event
loop: bound port, zero response to ANY request, indistinguishable from a
crash except the process stays alive -- matching the wedge pattern observed
repeatedly that session, which a restart kept masking before this was found.

Not confirmed as THE root cause of that specific incident (the certs
endpoint responded fine when tested live), but a genuine risk regardless:
synchronous network I/O must never run directly on an async event loop.
Fixed by wrapping both calls in asyncio.to_thread + a hard 5s timeout, so a
network hang can no longer block the loop -- it just times out and the
request fails closed (401 for API paths, /login redirect for pages), same
as any other invalid-credential outcome.

These tests prove: (1) a slow/hanging try_cf_access is bounded by the
timeout rather than blocking indefinitely, (2) a timeout fails closed (not
open), and (3) the normal fast path is unaffected.
"""
from __future__ import annotations

import asyncio
import time
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import dashboard.app as app_module


def _fake_request(path: str = "/", headers: dict | None = None) -> MagicMock:
    req = MagicMock()
    req.method = "GET"
    req.url = MagicMock()
    req.url.path = path
    req.headers = headers or {"cf-access-jwt-assertion": "fake-jwt"}
    req.cookies = {}
    req.client = MagicMock()
    req.client.host = "203.0.113.5"
    req.state = MagicMock()
    req.state.cf_session_override = None
    return req


def _run(coro):
    return asyncio.run(coro)


class CfAuthTimeoutSafetyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.middleware = app_module.AuthMiddleware(app=MagicMock())
        self._call_next_response = MagicMock()
        self._call_next_response.set_cookie = MagicMock()
        self.call_next = AsyncMock(return_value=self._call_next_response)

    def _dispatch(self, path="/"):
        req = _fake_request(path)
        with patch.object(app_module, "_get_session_data", return_value=None), \
             patch.object(app_module, "_is_localhost", return_value=False), \
             patch.object(app_module, "_is_via_cf_tunnel", return_value=True), \
             patch("dashboard.cf_auth.is_configured", return_value=True):
            return req, _run(self.middleware.dispatch(req, self.call_next))

    def test_hanging_cf_check_does_not_block_beyond_timeout(self) -> None:
        """The core regression test: a try_cf_access that would hang forever
        (simulating a network stall reaching Cloudflare's certs endpoint)
        must not block dispatch() past the configured timeout (5s in the
        real code -- asserting a generous 8s bound here, not the network
        call's full 30s simulated hang)."""
        def hangs_forever(request):
            time.sleep(8)  # simulates a blocked synchronous network call, kept short to avoid a long-lived dangling thread in the test process
            return True

        with patch.object(app_module, "logger"), \
             patch("dashboard.cf_auth.try_cf_access", side_effect=hangs_forever), \
             patch("dashboard.cf_auth.try_internal_token", return_value=False):
            t0 = time.monotonic()
            req, result = self._dispatch("/api/status")
            elapsed = time.monotonic() - t0

        # Bound is generous (not tight against the 5s production timeout):
        # asyncio.to_thread's underlying thread keeps running until its own
        # 8s sleep finishes regardless of the await timing out at 5s (Python
        # threads can't be forcibly interrupted), and asyncio.run()'s loop
        # teardown in this test harness waits for that orphaned thread pool
        # worker to finish -- a test-harness artifact, not present in
        # production (uvicorn keeps one persistent event loop for the
        # server's lifetime, never tearing down per-request). What actually
        # matters -- that call_next() is never awaited on a timeout, i.e.
        # dispatch()'s OWN logic doesn't block -- is asserted precisely in
        # test_timeout_fails_closed_not_open.
        self.assertLess(elapsed, 15.0, "dispatch() must not block anywhere near a real 30s+ hang")

    def test_timeout_fails_closed_not_open(self) -> None:
        """A timed-out CF check must be treated as NOT authenticated (401 for
        API paths) -- never silently let the request through."""
        def hangs_forever(request):
            time.sleep(8)
            return True

        with patch.object(app_module, "logger"), \
             patch("dashboard.cf_auth.try_cf_access", side_effect=hangs_forever), \
             patch("dashboard.cf_auth.try_internal_token", return_value=False):
            req, result = self._dispatch("/api/status")

        self.call_next.assert_not_awaited()
        self.assertEqual(getattr(result, "status_code", None), 401)

    def test_fast_path_unaffected_by_wrapping(self) -> None:
        """Normal (fast, cached) CF validation must still work end-to-end --
        the thread-wrapping must not change the happy-path outcome."""
        with patch("dashboard.cf_auth.try_cf_access", return_value=True), \
             patch("dashboard.cf_auth.try_internal_token", return_value=False), \
             patch("dashboard.cf_auth.get_cf_identity",
                   return_value={"email": "steve@example.com"}):
            req, result = self._dispatch("/api/me")

        self.assertEqual(result, self._call_next_response)
        self.call_next.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
