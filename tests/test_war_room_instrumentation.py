"""tests/test_war_room_instrumentation.py — HM-WAR-ROOM-LATENCY Layer 1 regression tests.

Covers the cycle-duration instrumentation added to ``main._war_room_thread``
via the module-level helper ``main._emit_wr_duration(wall_seconds)``. The
helper is the entire Layer 1 ship surface — extracting it from the daemon
thread closure is what makes the behavior testable without spinning up the
War Room providers.

Asserted behaviors:

  - ``main._WR_STALL_THRESHOLD_S`` is exactly 750s (12.5 min) -- raised from
    600s by HM-WR-STALL-ALARM-RATE-LIMIT (2026-05-20) to cut false-alarm
    volume after a genuine 1175s cycle with all providers individually
    healthy (91-202s each) showed 600s was too tight.
  - Every cycle (regardless of duration) emits a ``[WR-DUR] cycle wall=…s``
    line via ``console.log`` so trader.log captures the wall-clock.
  - Cycles at or under the threshold do NOT call
    ``engine.alert_channels.send_alert`` — strict greater-than semantics.
  - Cycles over the threshold call ``send_alert`` exactly once with
    ``level=AlertLevel.WARNING`` (NTFY priority=high, routes to
    NTFY_ADMIN_TOPIC=ollietrades-admin) and
    ``alert_type='war_room_slow_cycle'`` — the rate-limit class.
  - The ``[WR-STALL]`` message includes the wall-clock in minutes.
  - A failure inside ``send_alert`` is caught so it cannot crash the daemon
    thread or latch ``_war_room_running``.

Run from project root:
    venv/bin/python3 -m pytest tests/test_war_room_instrumentation.py -v
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


class WarRoomInstrumentationTests(unittest.TestCase):
    """Layer 1 surface: ``main._emit_wr_duration`` + ``main._WR_STALL_THRESHOLD_S``."""

    @classmethod
    def setUpClass(cls) -> None:
        # Single import — module-level state is fine; tests only patch attrs.
        import main  # noqa: WPS433 — testing main.py directly
        cls.main = main

    def setUp(self) -> None:
        # HM-TEST-ISOLATION-2026-08-29: _wr_stall_last_ntfy is a real,
        # persistent module-level float (main.py's own 1-hour NTFY rate
        # limit, HM-WR-STALL-ALARM-RATE-LIMIT) -- mocking send_alert does
        # NOT prevent _emit_wr_duration from updating it on every fired
        # stall. Without a reset, whichever test fires a stall first
        # permanently consumes the hour-window for every later test in this
        # same process (pytest runs the whole suite in one process), making
        # every subsequent "does it fire" assertion see 0 calls regardless
        # of wall_seconds. Reset before each test so they're independent.
        self.main._wr_stall_last_ntfy = 0.0

    # ---- threshold contract --------------------------------------------------

    def test_threshold_constant_is_750s(self) -> None:
        """12.5-minute stall threshold (HM-WR-STALL-ALARM-RATE-LIMIT,
        2026-05-20 -- raised from 600s to cut false-alarm volume)."""
        self.assertEqual(self.main._WR_STALL_THRESHOLD_S, 750)

    # ---- [WR-DUR] log emission ----------------------------------------------

    def test_dur_log_emitted_under_threshold(self) -> None:
        """A 120.5s cycle emits ``[WR-DUR] cycle wall=120.5s`` via console.log."""
        with mock.patch.object(self.main, "console") as mock_console, \
             mock.patch("engine.alert_channels.send_alert") as mock_alert:
            self.main._emit_wr_duration(120.5)
            log_lines = [c.args[0] for c in mock_console.log.call_args_list if c.args]
            self.assertTrue(
                any("[WR-DUR]" in line and "120.5" in line for line in log_lines),
                f"expected [WR-DUR] log with 120.5s; got: {log_lines}",
            )
            mock_alert.assert_not_called()

    def test_dur_log_emitted_even_when_stalled(self) -> None:
        """Stall cycles ALSO emit [WR-DUR] — NTFY is additive, not replacement."""
        with mock.patch.object(self.main, "console") as mock_console, \
             mock.patch("engine.alert_channels.send_alert"):
            self.main._emit_wr_duration(900.0)
            log_lines = [c.args[0] for c in mock_console.log.call_args_list if c.args]
            self.assertTrue(
                any("[WR-DUR]" in line for line in log_lines),
                f"expected [WR-DUR] log even when stalled; got: {log_lines}",
            )

    # ---- [WR-STALL] NTFY semantics ------------------------------------------

    def test_no_stall_alert_under_threshold(self) -> None:
        """1-min cycle never calls send_alert."""
        with mock.patch.object(self.main, "console"), \
             mock.patch("engine.alert_channels.send_alert") as mock_alert:
            self.main._emit_wr_duration(60.0)
            mock_alert.assert_not_called()

    def test_no_stall_alert_at_exactly_threshold(self) -> None:
        """Exactly 750s does NOT fire — strict greater-than semantics."""
        with mock.patch.object(self.main, "console"), \
             mock.patch("engine.alert_channels.send_alert") as mock_alert:
            self.main._emit_wr_duration(750.0)
            mock_alert.assert_not_called()

    def test_stall_alert_fires_above_threshold(self) -> None:
        """800s (>750s threshold) fires WARNING NTFY with the right alert_type."""
        with mock.patch.object(self.main, "console"), \
             mock.patch("engine.alert_channels.send_alert") as mock_alert:
            self.main._emit_wr_duration(800.0)
            mock_alert.assert_called_once()
            kwargs = mock_alert.call_args.kwargs
            self.assertEqual(
                kwargs.get("level"), "warning",
                f"AlertLevel.WARNING == 'warning' (priority=high); got {kwargs.get('level')!r}",
            )
            self.assertEqual(
                kwargs.get("alert_type"), "war_room_slow_cycle",
                "alert_type is the rate-limit dedup class; must be stable",
            )
            msg = kwargs.get("message", "")
            self.assertIn("[WR-STALL]", msg)
            self.assertIn("13.3min", msg)

    # ---- failure containment ------------------------------------------------

    def test_ntfy_failure_does_not_crash_helper(self) -> None:
        """If send_alert raises, _emit_wr_duration must not propagate."""
        with mock.patch.object(self.main, "console") as mock_console, \
             mock.patch(
                 "engine.alert_channels.send_alert",
                 side_effect=RuntimeError("ntfy down"),
             ):
            try:
                self.main._emit_wr_duration(800.0)  # > 750s threshold, must reach the NTFY dispatch path
            except Exception as e:
                self.fail(
                    f"_emit_wr_duration must absorb NTFY failures (caller is "
                    f"_war_room_thread daemon finally-block); leaked "
                    f"{type(e).__name__}: {e!r}"
                )
            log_lines = [c.args[0] for c in mock_console.log.call_args_list if c.args]
            self.assertTrue(
                any("[WR-STALL] NTFY dispatch failed" in line for line in log_lines),
                f"expected error-path log line; got: {log_lines}",
            )


if __name__ == "__main__":
    unittest.main()
