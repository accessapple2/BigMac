"""tests/test_war_room_layer_2a_budget.py — HM-WAR-ROOM-LATENCY Layer 2a v1 regression tests.

Mirrors the Layer 1 pattern at tests/test_war_room_instrumentation.py: tests
the module-level helper ``main._emit_wr_budget_exceeded(wall_seconds)`` plus
the ``main._WR_CYCLE_BUDGET_S`` env-var-backed constant. No War Room providers
are spun up; tests patch ``main.console.log`` to capture emissions.

Asserted behaviors:

  - ``main._WR_CYCLE_BUDGET_S`` defaults to 925s when env var unset.
  - The expression backing it (``int(os.getenv("WAR_ROOM_CYCLE_BUDGET_S","925"))``)
    correctly parses the env var when set (verified via the same expression
    pattern; module-reload avoided to keep tests fast and side-effect-free).
  - ``_emit_wr_budget_exceeded(wall_seconds)`` emits ``[WR-BUDGET-EXCEEDED]``
    via ``console.log`` only when ``wall_seconds > _WR_CYCLE_BUDGET_S``
    (strict greater-than, matches Layer 1's stall-threshold semantics).
  - Emission is log-only by design — does NOT call ``send_alert`` (distinct
    from ``[WR-STALL]`` which routes through engine.alert_channels). This is
    the v1 instrumentation-only ship per project_hm_layer_2a_design.md.
  - Emission message includes wall, budget, and over_by values for telemetry.
  - Helper is crash-safe: an exception inside ``console.log`` does not
    propagate (matches Layer 1's _emit_wr_duration daemon-thread contract).

Run from project root:
    venv/bin/python3 -m pytest tests/test_war_room_layer_2a_budget.py -v
"""
from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest import mock

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


class WarRoomLayer2aBudgetTests(unittest.TestCase):
    """Layer 2a v1 surface: ``main._emit_wr_budget_exceeded`` + ``main._WR_CYCLE_BUDGET_S``."""

    @classmethod
    def setUpClass(cls) -> None:
        # Single import — module-level state is fine; tests only patch attrs.
        import main  # noqa: WPS433 — testing main.py directly
        cls.main = main

    # ── threshold contract ────────────────────────────────────────────────

    def test_budget_constant_default_is_925s(self) -> None:
        """When WAR_ROOM_CYCLE_BUDGET_S is unset at import time, default is 925."""
        # If the test process happens to have the env var set, this test
        # documents that behavior rather than guarding it — but in CI/dev
        # without the env var, the default should be 925.
        if os.environ.get("WAR_ROOM_CYCLE_BUDGET_S"):
            self.skipTest("WAR_ROOM_CYCLE_BUDGET_S set in test env; default check skipped")
        self.assertEqual(self.main._WR_CYCLE_BUDGET_S, 925)

    def test_env_var_parses_as_int(self) -> None:
        """The underlying os.getenv expression parses the env var as int."""
        # Test the exact expression used in main.py without module reload.
        with mock.patch.dict(os.environ, {"WAR_ROOM_CYCLE_BUDGET_S": "100"}):
            parsed = int(os.getenv("WAR_ROOM_CYCLE_BUDGET_S", "925"))
            self.assertEqual(parsed, 100)
        # Sanity: with no env, default kicks in
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("WAR_ROOM_CYCLE_BUDGET_S", None)
            parsed_default = int(os.getenv("WAR_ROOM_CYCLE_BUDGET_S", "925"))
            self.assertEqual(parsed_default, 925)

    # ── log-emission contract (under / at / over budget) ─────────────────

    def test_no_log_when_wall_under_budget(self) -> None:
        """Cycles at or under the budget produce NO [WR-BUDGET-EXCEEDED] log line."""
        with mock.patch.object(self.main, "_WR_CYCLE_BUDGET_S", 925), \
             mock.patch.object(self.main, "console") as mock_console:
            self.main._emit_wr_budget_exceeded(800.0)
            mock_console.log.assert_not_called()

    def test_no_log_at_exactly_budget(self) -> None:
        """Strict greater-than: wall == budget does NOT trigger the log."""
        with mock.patch.object(self.main, "_WR_CYCLE_BUDGET_S", 925), \
             mock.patch.object(self.main, "console") as mock_console:
            self.main._emit_wr_budget_exceeded(925.0)
            mock_console.log.assert_not_called()

    def test_log_fires_when_wall_over_budget(self) -> None:
        """Cycles strictly above budget emit exactly one [WR-BUDGET-EXCEEDED] line."""
        with mock.patch.object(self.main, "_WR_CYCLE_BUDGET_S", 925), \
             mock.patch.object(self.main, "console") as mock_console:
            self.main._emit_wr_budget_exceeded(1000.0)
            mock_console.log.assert_called_once()
            call_args = mock_console.log.call_args[0][0]
            self.assertIn("[WR-BUDGET-EXCEEDED]", call_args)

    def test_log_message_includes_wall_budget_over_by(self) -> None:
        """The emitted line carries wall, budget, and over_by for downstream telemetry."""
        with mock.patch.object(self.main, "_WR_CYCLE_BUDGET_S", 925), \
             mock.patch.object(self.main, "console") as mock_console:
            self.main._emit_wr_budget_exceeded(1100.5)
            msg = mock_console.log.call_args[0][0]
            self.assertIn("1100.5", msg)
            self.assertIn("925", msg)
            # over_by = 1100.5 - 925 = 175.5
            self.assertIn("175.5", msg)

    # ── log-only contract (no NTFY) ──────────────────────────────────────

    def test_log_only_no_ntfy_dispatch(self) -> None:
        """Layer 2a v1 is log-only — must NOT call engine.alert_channels.send_alert.

        Distinct from Layer 1's [WR-STALL] which DOES route through send_alert
        (and is per-process-lifetime rate-limited; see
        project_hm_wr_stall_alarm_rate_limit). v1 stays observability-only
        per project_hm_layer_2a_design.md decision.
        """
        with mock.patch.object(self.main, "_WR_CYCLE_BUDGET_S", 925), \
             mock.patch.object(self.main, "console"), \
             mock.patch("engine.alert_channels.send_alert") as mock_send:
            self.main._emit_wr_budget_exceeded(1500.0)
            mock_send.assert_not_called()

    # ── crash-safe contract ──────────────────────────────────────────────

    def test_emission_does_not_crash_helper(self) -> None:
        """If console.log raises, the helper swallows the exception (daemon-safe)."""
        with mock.patch.object(self.main, "_WR_CYCLE_BUDGET_S", 925), \
             mock.patch.object(self.main, "console") as mock_console:
            mock_console.log.side_effect = RuntimeError("simulated I/O failure")
            # Must not raise — daemon thread's finally clears _war_room_running
            try:
                self.main._emit_wr_budget_exceeded(1000.0)
            except Exception as e:
                self.fail(f"_emit_wr_budget_exceeded propagated exception: {e!r}")


if __name__ == "__main__":
    unittest.main()
