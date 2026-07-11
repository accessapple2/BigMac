"""HM-NTFY-IPV6-NOROUTE-LRS-FIX 2026-07-10.

engine/long_range_sensors.py::send_ntfy() used its own unprotected
requests.post() call, never given the IPv4-force fix already applied to
engine/alert_channels.py's _send_ntfy() for the same box's confirmed lack
of an IPv6 route to ntfy.sh (HM-NTFY-IPV6-NOROUTE, 2026-07-07). Traced this
as the source of the large majority of trader_error.log's ~13,300 "ntfy
failed" lines this week. Fixed by routing through the already-hardened
send_alert() instead of a second, separate implementation of the same POST.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_send_ntfy_routes_through_hardened_send_alert():
    import engine.long_range_sensors as lrs

    with patch("engine.alert_channels.send_alert") as mock_send:
        lrs.send_ntfy("WHALE: SPY 5x volume", priority="high")

    mock_send.assert_called_once()
    _, kwargs = mock_send.call_args
    assert kwargs["message"] == "WHALE: SPY 5x volume"
    assert kwargs["audience"] == "crew"
    assert kwargs["bypass_rate_limit"] is True


def test_send_ntfy_urgent_maps_to_red_alert():
    import engine.long_range_sensors as lrs
    from engine.alert_channels import AlertLevel

    with patch("engine.alert_channels.send_alert") as mock_send:
        lrs.send_ntfy("MEGA WHALE: SPY 20x volume", priority="urgent")

    _, kwargs = mock_send.call_args
    assert kwargs["level"] == AlertLevel.RED_ALERT


def test_send_ntfy_high_maps_to_warning():
    import engine.long_range_sensors as lrs
    from engine.alert_channels import AlertLevel

    with patch("engine.alert_channels.send_alert") as mock_send:
        lrs.send_ntfy("WHALE: QQQ 5x volume", priority="high")

    _, kwargs = mock_send.call_args
    assert kwargs["level"] == AlertLevel.WARNING


def test_send_ntfy_swallows_send_alert_exception():
    """A failure inside send_alert must not propagate -- callers (scan_for_whales)
    must keep running even if one alert send raises."""
    import engine.long_range_sensors as lrs

    with patch("engine.alert_channels.send_alert", side_effect=RuntimeError("boom")):
        lrs.send_ntfy("WHALE: TSLA 5x volume", priority="high")  # must not raise
