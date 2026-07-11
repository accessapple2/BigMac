"""HM-NTFY-IPV6-NOROUTE-ENGINE-NTFY-FIX 2026-07-10.

engine/ntfy.py::_send() had its own unprotected urllib.request POST --
a third exposed sender (alongside the already-fixed engine/long_range_
sensors.py) never given the IPv4-force fix applied to engine/
alert_channels.py::_send_ntfy() for this box's confirmed lack of an IPv6
route to ntfy.sh (HM-NTFY-IPV6-NOROUTE, 2026-07-07). Fixed by delegating
to the already-hardened _send_ntfy() instead of a third, separate
implementation of the same POST -- and to avoid two independent locks
racing to monkeypatch the same process-global socket.getaddrinfo from
different threads.

Tests call _send() directly (not _fire()/_fire_pg()) to exercise the
fixed logic synchronously, without the daemon-thread indirection.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_send_routes_through_hardened_send_ntfy():
    import engine.ntfy as ntfy_mod

    with patch("engine.alert_channels._send_ntfy") as mock_send:
        ntfy_mod._send("Ollie BUY SPY", "$700 x 1", priority=ntfy_mod.P_HIGH, tags="buy")

    mock_send.assert_called_once()
    _, kwargs = mock_send.call_args
    assert kwargs["title"] == "Ollie BUY SPY"
    assert kwargs["message"] == "$700 x 1"
    assert kwargs["priority"] == "high"
    assert kwargs["tags"] == "buy"
    assert kwargs["topic"] == ntfy_mod.NTFY_TOPIC


def test_send_priority_mapping_covers_all_five_levels():
    import engine.ntfy as ntfy_mod

    expected = {
        ntfy_mod.P_MIN: "min",
        ntfy_mod.P_LOW: "low",
        ntfy_mod.P_DEFAULT: "default",
        ntfy_mod.P_HIGH: "high",
        ntfy_mod.P_MAX: "urgent",
    }
    for level, label in expected.items():
        with patch("engine.alert_channels._send_ntfy") as mock_send:
            ntfy_mod._send("t", "b", priority=level)
        assert mock_send.call_args.kwargs["priority"] == label


def test_send_pg_topic_override_is_honored():
    import engine.ntfy as ntfy_mod

    with patch("engine.alert_channels._send_ntfy") as mock_send:
        ntfy_mod._send("t", "b", topic=ntfy_mod.NTFY_PROVING_GROUND_TOPIC)

    assert mock_send.call_args.kwargs["topic"] == ntfy_mod.NTFY_PROVING_GROUND_TOPIC


def test_send_swallows_exception():
    """A failure inside _send_ntfy (or the import itself) must not propagate --
    ntfy failures must never crash trading logic."""
    import engine.ntfy as ntfy_mod

    with patch("engine.alert_channels._send_ntfy", side_effect=RuntimeError("boom")):
        ntfy_mod._send("t", "b")  # must not raise


def test_fire_pg_spawns_thread_targeting_proving_ground_topic():
    """_fire_pg() must still hand off to a daemon thread (unchanged async
    contract) while routing to the dedicated proving-ground topic."""
    import engine.ntfy as ntfy_mod

    with patch("engine.alert_channels._send_ntfy") as mock_send:
        ntfy_mod._fire_pg("t", "b")
        # _fire_pg spawns a daemon thread; give it a moment to run.
        import time
        for _ in range(50):
            if mock_send.called:
                break
            time.sleep(0.01)

    assert mock_send.called
    assert mock_send.call_args.kwargs["topic"] == ntfy_mod.NTFY_PROVING_GROUND_TOPIC
