"""HM-NTFY-IPV6-NOROUTE-RIKER-FIX 2026-07-10.

engine/riker_synthesis.py::_ntfy() had its own unprotected requests.post()
call -- confirmed hitting this box's lack of an IPv6 route to ntfy.sh via
real evidence in its own log (21 failures). Fixed by delegating to the
already-hardened engine.alert_channels._send_ntfy() instead of a separate
implementation of the same POST.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_ntfy_routes_through_hardened_send_ntfy():
    import engine.riker_synthesis as rs

    with patch("engine.alert_channels._send_ntfy") as mock_send:
        rs._ntfy("Fleet Alert", "3 high-conf signals", priority="high")

    mock_send.assert_called_once()
    _, kwargs = mock_send.call_args
    assert kwargs["title"] == "Fleet Alert"
    assert kwargs["message"] == "3 high-conf signals"
    assert kwargs["priority"] == "high"
    assert kwargs["topic"] == rs.NTFY_TOPIC


def test_ntfy_default_priority():
    import engine.riker_synthesis as rs

    with patch("engine.alert_channels._send_ntfy") as mock_send:
        rs._ntfy("t", "b")

    assert mock_send.call_args.kwargs["priority"] == "default"


def test_ntfy_swallows_exception():
    """A failure inside _send_ntfy must not propagate."""
    import engine.riker_synthesis as rs

    with patch("engine.alert_channels._send_ntfy", side_effect=RuntimeError("boom")):
        rs._ntfy("t", "b")  # must not raise
