"""HM-NTFY-IPV6-NOROUTE-WATCHDOG-FIX 2026-07-10.

watchdog.py::push_alert() had its own unprotected urllib.request POST --
confirmed hitting this box's lack of an IPv6 route to ntfy.sh via real
evidence in watchdog.py's own log (12 "ntfy push failed: <urlopen error
[Errno 65] No route to host>" occurrences, 2026-07-09). Fixed with a
self-contained IPv4-force lock+monkeypatch (not importing
engine.alert_channels -- watchdog.py is deliberately dependency-free from
the engine/ package it monitors, so it keeps working even if that package
has an import-time problem).
"""
from __future__ import annotations

import socket
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fake_urlopen_response():
    resp = MagicMock()
    resp.status = 200
    resp.__enter__ = lambda self: resp
    resp.__exit__ = lambda self, *a: False
    return resp


def test_push_alert_forces_ipv4_during_send_and_restores_after(monkeypatch):
    import watchdog as wd

    monkeypatch.setitem(wd._last_notify, "cooldown-reset", 0)
    original_getaddrinfo = socket.getaddrinfo
    observed_during_call = {}

    def fake_urlopen(req, timeout=8):
        # Capture what socket.getaddrinfo is set to WHILE the send happens.
        observed_during_call["getaddrinfo"] = socket.getaddrinfo
        return _fake_urlopen_response()

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        wd.push_alert("Test Alert", "body", key="cooldown-reset")

    assert observed_during_call["getaddrinfo"] is wd._ipv4_only_getaddrinfo
    # Must be restored to the true original after the call, not left patched.
    assert socket.getaddrinfo is original_getaddrinfo


def test_push_alert_restores_getaddrinfo_even_on_send_failure(monkeypatch):
    import watchdog as wd

    monkeypatch.setitem(wd._last_notify, "cooldown-reset-2", 0)
    original_getaddrinfo = socket.getaddrinfo

    with patch("urllib.request.urlopen", side_effect=OSError("No route to host")):
        wd.push_alert("Test Alert", "body", key="cooldown-reset-2")  # must not raise

    assert socket.getaddrinfo is original_getaddrinfo


def test_ipv4_only_getaddrinfo_forces_af_inet():
    import watchdog as wd

    with patch.object(wd, "_orig_getaddrinfo") as mock_orig:
        wd._ipv4_only_getaddrinfo("ntfy.sh", 443)

    args, _ = mock_orig.call_args
    assert args[0] == "ntfy.sh"
    assert args[1] == 443
    assert args[2] == socket.AF_INET
