"""tests/test_event_tape_staleness_watchdog.py — HM-EVENT-TAPE-STALENESS-WATCHDOG-2026-07-09.

Covers a live-incident finding (2026-07-09): the event-tape detector thread
(engine/event_tape.py::_run_detector_loop) went silently dead for 21.5h+ --
cycles=0 the entire time, no exception ever logged, no alarm anywhere to catch
it. Root-caused to a lazy `from engine.risk_manager import RiskManager` import
executed fresh inside the daemon thread's first loop iteration, racing against
sibling daemon threads' own startup imports (a plausible CPython import-lock
deadlock). Fixed by hoisting the import to module scope.

These tests cover the belt-and-suspenders half of the fix: a staleness
watchdog that pages if the detector loop hasn't completed a cycle in >15min
during market hours, so a future stall of this kind (whatever the cause)
gets caught automatically instead of requiring a manual dashboard read.

Deliberately alarms on CYCLE staleness, not zero-EVENTS: _run_heartbeat's own
docstring documents "no events fire" as an expected, normal state during quiet
market hours, so an events-based alarm would false-page on legitimately quiet
stretches. Cycle staleness is the metric that actually failed here.
"""
from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import engine.event_tape as event_tape


class DetectorStalenessWatchdogTests(unittest.TestCase):
    def setUp(self) -> None:
        # Isolate module state between tests.
        self._orig_stats = dict(event_tape._stats)
        event_tape._stats = dict(self._orig_stats)
        event_tape._staleness_alert_sent = False

    def tearDown(self) -> None:
        event_tape._stats = self._orig_stats
        event_tape._staleness_alert_sent = False

    def test_no_alert_outside_market_hours(self) -> None:
        event_tape._stats["last_cycle_at"] = None
        with patch.object(event_tape.RiskManager, "is_market_hours", return_value=False), \
             patch("engine.alert_channels.send_alert") as mock_alert:
            event_tape._check_detector_staleness()
        mock_alert.assert_not_called()
        self.assertFalse(event_tape._staleness_alert_sent)

    def test_no_alert_when_cycle_is_fresh(self) -> None:
        fresh = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
        event_tape._stats["last_cycle_at"] = fresh
        with patch.object(event_tape.RiskManager, "is_market_hours", return_value=True), \
             patch("engine.alert_channels.send_alert") as mock_alert:
            event_tape._check_detector_staleness()
        mock_alert.assert_not_called()
        self.assertFalse(event_tape._staleness_alert_sent)

    def test_alert_fires_when_never_cycled(self) -> None:
        # Reproduces the actual 2026-07-09 incident: last_cycle_at stays
        # None forever because the loop never completed a single iteration.
        event_tape._stats["last_cycle_at"] = None
        with patch.object(event_tape.RiskManager, "is_market_hours", return_value=True), \
             patch("engine.alert_channels.send_alert") as mock_alert:
            event_tape._check_detector_staleness()
        mock_alert.assert_called_once()
        self.assertTrue(event_tape._staleness_alert_sent)

    def test_alert_fires_when_stale_past_threshold(self) -> None:
        stale = (datetime.now(timezone.utc) - timedelta(minutes=20)).isoformat()
        event_tape._stats["last_cycle_at"] = stale
        with patch.object(event_tape.RiskManager, "is_market_hours", return_value=True), \
             patch("engine.alert_channels.send_alert") as mock_alert:
            event_tape._check_detector_staleness()
        mock_alert.assert_called_once()
        self.assertTrue(event_tape._staleness_alert_sent)

    def test_alert_is_edge_triggered_not_repeated(self) -> None:
        stale = (datetime.now(timezone.utc) - timedelta(minutes=20)).isoformat()
        event_tape._stats["last_cycle_at"] = stale
        with patch.object(event_tape.RiskManager, "is_market_hours", return_value=True), \
             patch("engine.alert_channels.send_alert") as mock_alert:
            event_tape._check_detector_staleness()
            event_tape._check_detector_staleness()
            event_tape._check_detector_staleness()
        mock_alert.assert_called_once()

    def test_alert_auto_clears_on_recovery(self) -> None:
        stale = (datetime.now(timezone.utc) - timedelta(minutes=20)).isoformat()
        event_tape._stats["last_cycle_at"] = stale
        with patch.object(event_tape.RiskManager, "is_market_hours", return_value=True), \
             patch("engine.alert_channels.send_alert") as mock_alert:
            event_tape._check_detector_staleness()
            self.assertTrue(event_tape._staleness_alert_sent)

            fresh = datetime.now(timezone.utc).isoformat()
            event_tape._stats["last_cycle_at"] = fresh
            event_tape._check_detector_staleness()
            self.assertFalse(event_tape._staleness_alert_sent)
        mock_alert.assert_called_once()  # still only the one alert, no re-fire on recovery


class RiskManagerImportIsModuleLevelTests(unittest.TestCase):
    """Proves the fix for the actual root cause: RiskManager must be a
    module-level name in event_tape, not re-imported inside the thread loop
    on every startup (the plausible import-lock deadlock site)."""

    def test_risk_manager_is_module_level_attribute(self) -> None:
        self.assertTrue(hasattr(event_tape, "RiskManager"))

    def test_detector_loop_source_has_no_lazy_risk_manager_import(self) -> None:
        import inspect
        src = inspect.getsource(event_tape._run_detector_loop)
        self.assertNotIn("import RiskManager", src)
        self.assertNotIn("from engine.risk_manager", src)


if __name__ == "__main__":
    unittest.main()
