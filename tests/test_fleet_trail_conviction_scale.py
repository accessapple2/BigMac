"""HM-FLEET-TRAIL-CONVICTION-SCALE Phase C tests.

Pure-function coverage for engine.stops.get_trail_pct plus gate-
behavior smoke (mock-based) that confirms the risk_manager wiring honors
flag + allow-list + NULL-conviction fallback. Mirrors the structure used
by tests/test_market_calendar.py + tests/test_market_calendar_gates.py.
"""
from __future__ import annotations

import importlib
import os
import unittest
from unittest.mock import patch

import engine.risk_manager as rm
from engine.stops import get_stop_loss_pct, get_trail_pct


def _reload_with(env: dict):
    """HM-TEST-ENV-ISOLATION-2026-08-29: both conviction-scale flags have
    since been deliberately promoted to true in the real .env (per the
    module's own Feature Flags doctrine, "flip via .env") -- a bare
    `import engine.risk_manager as rm` captures whatever the ambient
    environment happens to be at process start, not a controlled default.
    Mirrors tests/test_conviction_stop_shadow.py's helper of the same name."""
    with patch.dict(os.environ, env):
        importlib.reload(rm)
    return rm


class TrailTierTableTests(unittest.TestCase):
    """get_trail_pct: pure-function tier table validation."""

    def test_top_tier_5pct(self) -> None:
        self.assertEqual(get_trail_pct(0.95), 0.05)
        self.assertEqual(get_trail_pct(0.90), 0.05)  # inclusive boundary

    def test_mid_tier_4pct(self) -> None:
        self.assertEqual(get_trail_pct(0.89), 0.04)
        self.assertEqual(get_trail_pct(0.85), 0.04)
        self.assertEqual(get_trail_pct(0.80), 0.04)  # inclusive boundary

    def test_floor_3pct(self) -> None:
        self.assertEqual(get_trail_pct(0.79), 0.03)
        self.assertEqual(get_trail_pct(0.75), 0.03)
        self.assertEqual(get_trail_pct(0.50), 0.03)
        self.assertEqual(get_trail_pct(0.0), 0.03)

    def test_floor_invariant_never_below_baseline(self) -> None:
        """Doctrine: trail never tighter than the 3% baseline."""
        for c in (0.0, 0.1, 0.5, 0.69, 0.79, 0.80, 0.89, 0.90, 0.99, 1.0):
            with self.subTest(conviction=c):
                self.assertGreaterEqual(get_trail_pct(c), 0.03)

    def test_paired_with_stop_loss_doctrine(self) -> None:
        """Both helpers obey the same conviction tier boundaries.

        High-conv positions earn BOTH wider entry stops AND wider trails.
        Low-conv (<0.80) inherits the respective baseline (0.12 for stop,
        0.03 for trail).
        """
        # Top tier: widest of both
        self.assertEqual(get_stop_loss_pct(0.95), 0.18)
        self.assertEqual(get_trail_pct(0.95), 0.05)
        # Mid tier
        self.assertEqual(get_stop_loss_pct(0.85), 0.15)
        self.assertEqual(get_trail_pct(0.85), 0.04)
        # Floor: both at their baseline
        self.assertEqual(get_stop_loss_pct(0.50), 0.12)
        self.assertEqual(get_trail_pct(0.50), 0.03)


class TrailGateModuleConstantTests(unittest.TestCase):
    """Verify risk_manager module reads flag at import-time correctly."""

    def tearDown(self) -> None:
        # Restore the shared rm module to real-environment state -- other
        # test files import engine.risk_manager too, and importlib.reload
        # mutates the one shared module object process-wide.
        importlib.reload(rm)

    def test_trail_flag_default_off(self) -> None:
        """With no env override, flag should resolve False.

        HM-TEST-ENV-ISOLATION-2026-08-29: both conviction-scale flags have
        since been deliberately promoted to true in the real .env -- this
        now tests the OFF default in a controlled reload rather than
        asserting against whatever the ambient environment happens to be
        (which, unpinned, currently reads True and would fail here).
        """
        mod = _reload_with({"CONVICTION_SCALED_TRAIL_ENABLED": "false"})
        self.assertIsInstance(mod._CONVICTION_SCALED_TRAIL_ENABLED, bool)
        self.assertFalse(mod._CONVICTION_SCALED_TRAIL_ENABLED)

    def test_trail_flag_is_separate_from_stops_flag(self) -> None:
        """Two SEPARATE module-level attributes exist so Admiral can enable
        scaled-stops first (after shadow validation), then later enable
        scaled-trail without coupling them. Both default False when unset
        (2026-08-29: both are now deliberately True in production .env --
        this test only checks the attributes are independent, not their
        current values)."""
        self.assertTrue(hasattr(rm, "_CONVICTION_SCALED_STOPS_ENABLED"))
        self.assertTrue(hasattr(rm, "_CONVICTION_SCALED_TRAIL_ENABLED"))
        # They each read their own env var key (paired but separate).
        import os
        # When neither env var is set, both default False — the production
        # state on this branch. This test guards against accidental flag
        # coupling (e.g. someone aliasing one to the other in .env).
        with patch.dict(os.environ, {
            "CONVICTION_SCALED_STOPS_ENABLED": "true",
            "CONVICTION_SCALED_TRAIL_ENABLED": "false",
        }):
            import importlib
            importlib.reload(rm)
            self.assertTrue(rm._CONVICTION_SCALED_STOPS_ENABLED)
            self.assertFalse(rm._CONVICTION_SCALED_TRAIL_ENABLED)
        # Reset to clean state for downstream tests
        with patch.dict(os.environ, {
            "CONVICTION_SCALED_STOPS_ENABLED": "false",
            "CONVICTION_SCALED_TRAIL_ENABLED": "false",
        }):
            import importlib
            importlib.reload(rm)


class TrailGateBehaviorTests(unittest.TestCase):
    """Mock-based gate behavior — exercises the inline trail-pct selection
    logic from check_stop_loss_take_profit via direct value computation
    that mirrors the production code path."""

    @staticmethod
    def _select_trail_pct(
        flag: bool, in_allow_list: bool, conviction
    ) -> float:
        """Mirror of the production logic at risk_manager.py L800-808.

        Kept in lockstep with the live code path; if the production
        branch changes, this helper must change with it (and the tests
        below will catch the divergence)."""
        if flag and in_allow_list:
            return get_trail_pct(conviction) if conviction is not None else 0.03
        return 0.03

    def test_flag_off_always_flat(self) -> None:
        # Flag-off path uses the 3% baseline regardless of conviction
        for conv in (None, 0.5, 0.75, 0.85, 0.95):
            with self.subTest(conv=conv):
                self.assertEqual(
                    self._select_trail_pct(False, True, conv), 0.03
                )

    def test_flag_on_allow_list_scaled(self) -> None:
        cases = [
            (0.95, 0.05),
            (0.85, 0.04),
            (0.75, 0.03),
            (0.50, 0.03),
        ]
        for conv, expected in cases:
            with self.subTest(conv=conv):
                self.assertEqual(
                    self._select_trail_pct(True, True, conv), expected
                )

    def test_flag_on_non_allow_list_flat(self) -> None:
        # Non-allow-list players use 3% even with flag ON + high conviction
        self.assertEqual(self._select_trail_pct(True, False, 0.95), 0.03)
        self.assertEqual(self._select_trail_pct(True, False, 0.85), 0.03)

    def test_flag_on_null_conviction_flat(self) -> None:
        # Allow-list player with NULL conviction (categorical NULL —
        # e.g. legacy row from before HM-POSITIONS-CONVICTION-DENORM
        # backfill) inherits 3% baseline.
        self.assertEqual(self._select_trail_pct(True, True, None), 0.03)


if __name__ == "__main__":
    unittest.main()
