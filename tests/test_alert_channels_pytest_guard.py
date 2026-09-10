"""HM-FALSE-RED-ALERT 2026-09-09 — regression test.

Root cause: tests/test_season_rotation_reactivation_scope.py's
test_rotate_season_aborts_and_writes_nothing_when_unsafe legitimately
exercises rotate_season()'s real abort path (by design — it's testing that
the abort itself is safe), which calls _alert_rotation_aborted() ->
send_alert(RED_ALERT, ...) -> real Pushover/ntfy/email sends. Since the
pre-commit hook runs the full suite on every commit, every commit fired a
real RED_ALERT to the phone. The historical ~12-day recurrence cadence
matched commit days exactly, confirmed from the instrumented abort
payload (caller='test', argv=pytest invocation, stack into that test).

This test proves the fix: under pytest, no send function reaches the
network, regardless of alert level.
"""
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import engine.alert_channels as ac


def test_under_pytest_detected():
    # PYTEST_CURRENT_TEST is set automatically by pytest during this call.
    assert os.environ.get("PYTEST_CURRENT_TEST")
    assert ac._under_pytest() is True


def test_red_alert_reaches_no_channel_under_pytest():
    """The actual regression case: a RED_ALERT raised under pytest must not
    touch the network on any channel, bypass_rate_limit or not."""
    with patch("urllib.request.urlopen") as mock_urlopen, \
         patch("smtplib.SMTP") as mock_smtp:
        results = ac.send_alert(
            "test probe — should never leave this process",
            level=ac.AlertLevel.RED_ALERT,
            alert_type="hm_false_red_alert_guard_test",
            bypass_rate_limit=True,
        )
        mock_urlopen.assert_not_called()
        mock_smtp.assert_not_called()
    assert results["pushover"] is False
    assert results["ntfy"] is False
    assert results["email"] is False


def test_send_pushover_returns_false_under_pytest_before_reading_creds():
    with patch("builtins.open") as mock_open, \
         patch("urllib.request.urlopen") as mock_urlopen:
        assert ac._send_pushover("t", "m") is False
        mock_open.assert_not_called()
        mock_urlopen.assert_not_called()


def test_send_email_functions_return_false_under_pytest():
    with patch("smtplib.SMTP") as mock_smtp:
        assert ac._send_email("subj", "body") is False
        assert ac.send_email("subj", "<p>body</p>") is False
        mock_smtp.assert_not_called()


def test_send_ntfy_returns_false_under_pytest():
    with patch("urllib.request.urlopen") as mock_urlopen:
        assert ac._send_ntfy("t", "m") is False
        mock_urlopen.assert_not_called()
