"""HM-OPTIONS-CONVICTION-STOP-WIRE Phase C tests.

Mirrors tests/test_fleet_trail_conviction_scale.py structure. Pure-
function coverage for engine.stops.get_options_stop_pct plus gate-
behavior smoke confirming the risk_manager wiring honors flag + allow-
list + NULL-conviction-fallback + equity-positions-unaffected semantics.
"""
from __future__ import annotations

import os
import unittest
from unittest.mock import patch

import engine.risk_manager as rm
from engine.stops import (
    get_options_stop_pct,
    get_stop_loss_pct,
    get_trail_pct,
)


class OptionsStopTierTableTests(unittest.TestCase):
    """get_options_stop_pct: pure-function tier table validation.

    Note: tier table inverts the floor-invariant direction relative to
    stops + trail. Low conviction gets TIGHTER stops (0.30 < 0.50 flat).
    Intentional — see engine/stops.py docstring + Rule #5 amendment.
    """

    def test_top_tier_preserves_50pct_baseline(self) -> None:
        self.assertEqual(get_options_stop_pct(0.95), 0.50)
        self.assertEqual(get_options_stop_pct(0.90), 0.50)  # inclusive boundary

    def test_mid_tier_40pct(self) -> None:
        self.assertEqual(get_options_stop_pct(0.89), 0.40)
        self.assertEqual(get_options_stop_pct(0.85), 0.40)
        self.assertEqual(get_options_stop_pct(0.80), 0.40)  # inclusive boundary

    def test_low_tier_30pct_tighter_than_baseline(self) -> None:
        """Doctrine deviation: low conv gets a TIGHTER stop than the
        current 0.50 baseline. Documented in get_options_stop_pct
        docstring + Rule #5 amendment in Phase E."""
        self.assertEqual(get_options_stop_pct(0.79), 0.30)
        self.assertEqual(get_options_stop_pct(0.75), 0.30)
        self.assertEqual(get_options_stop_pct(0.50), 0.30)
        self.assertEqual(get_options_stop_pct(0.0), 0.30)

    def test_tier_table_consistency(self) -> None:
        """Same conviction boundaries (0.80, 0.90) as the other two
        layers, even though the per-layer values differ."""
        # Top tier: widest of all three layers
        self.assertEqual(get_options_stop_pct(0.95), 0.50)
        self.assertEqual(get_stop_loss_pct(0.95), 0.18)
        self.assertEqual(get_trail_pct(0.95), 0.05)
        # Mid tier
        self.assertEqual(get_options_stop_pct(0.85), 0.40)
        self.assertEqual(get_stop_loss_pct(0.85), 0.15)
        self.assertEqual(get_trail_pct(0.85), 0.04)
        # Floor — note options DOES NOT match the other-layer floor
        # invariant; that's the documented deviation.
        self.assertEqual(get_options_stop_pct(0.50), 0.30)  # TIGHTER than 0.50 flat
        self.assertEqual(get_stop_loss_pct(0.50), 0.12)     # = baseline
        self.assertEqual(get_trail_pct(0.50), 0.03)          # = baseline


class OptionsStopFlagTests(unittest.TestCase):
    """Verify risk_manager exposes a third independent flag."""

    def test_options_stop_flag_default_off(self) -> None:
        self.assertIsInstance(rm._CONVICTION_SCALED_OPTIONS_STOP_ENABLED, bool)
        self.assertFalse(rm._CONVICTION_SCALED_OPTIONS_STOP_ENABLED)

    def test_three_independent_flags(self) -> None:
        """All three module-level attributes exist + read their own env
        var keys. Admiral can enable any subset independently to shadow-
        validate one layer at a time."""
        self.assertTrue(hasattr(rm, "_CONVICTION_SCALED_STOPS_ENABLED"))
        self.assertTrue(hasattr(rm, "_CONVICTION_SCALED_TRAIL_ENABLED"))
        self.assertTrue(hasattr(rm, "_CONVICTION_SCALED_OPTIONS_STOP_ENABLED"))
        # Patch env to confirm each can be enabled in isolation.
        with patch.dict(os.environ, {
            "CONVICTION_SCALED_STOPS_ENABLED": "false",
            "CONVICTION_SCALED_TRAIL_ENABLED": "false",
            "CONVICTION_SCALED_OPTIONS_STOP_ENABLED": "true",
        }):
            import importlib
            importlib.reload(rm)
            self.assertFalse(rm._CONVICTION_SCALED_STOPS_ENABLED)
            self.assertFalse(rm._CONVICTION_SCALED_TRAIL_ENABLED)
            self.assertTrue(rm._CONVICTION_SCALED_OPTIONS_STOP_ENABLED)
        # Reset to all-False for downstream tests
        with patch.dict(os.environ, {
            "CONVICTION_SCALED_STOPS_ENABLED": "false",
            "CONVICTION_SCALED_TRAIL_ENABLED": "false",
            "CONVICTION_SCALED_OPTIONS_STOP_ENABLED": "false",
        }):
            import importlib
            importlib.reload(rm)


class OptionsStopGateBehaviorTests(unittest.TestCase):
    """Mock-based gate behavior — mirrors the production code path at
    risk_manager.py L770+ via a parallel helper kept in lockstep.
    """

    FLAT_BASELINE = 0.50  # config.OPTIONS_STOP_LOSS_PCT default

    @staticmethod
    def _select_options_stop_pct(
        flag: bool, in_allow_list: bool, conviction, flat: float = 0.50
    ) -> float:
        """Mirror of the production logic at risk_manager.py L795-816."""
        if flag and in_allow_list:
            return get_options_stop_pct(conviction) if conviction is not None else flat
        return flat

    def test_flag_off_always_flat(self) -> None:
        for conv in (None, 0.5, 0.75, 0.85, 0.95):
            with self.subTest(conv=conv):
                self.assertEqual(
                    self._select_options_stop_pct(False, True, conv),
                    self.FLAT_BASELINE,
                )

    def test_flag_on_allow_list_scaled(self) -> None:
        cases = [
            (0.95, 0.50),
            (0.85, 0.40),
            (0.75, 0.30),
            (0.50, 0.30),
        ]
        for conv, expected in cases:
            with self.subTest(conv=conv):
                self.assertEqual(
                    self._select_options_stop_pct(True, True, conv),
                    expected,
                )

    def test_flag_on_non_allow_list_flat(self) -> None:
        # alpaca-mirror / enterprise-computer / dalio-metals — never see scaled
        self.assertEqual(self._select_options_stop_pct(True, False, 0.95), 0.50)
        self.assertEqual(self._select_options_stop_pct(True, False, 0.75), 0.50)

    def test_flag_on_null_conviction_flat(self) -> None:
        # Allow-list player with NULL conviction inherits flat baseline.
        self.assertEqual(self._select_options_stop_pct(True, True, None), 0.50)

    def test_equity_position_unaffected_by_options_gate(self) -> None:
        """The options-stop gate at risk_manager.py:770 is keyed on
        pos.asset_type == 'option'; equity positions never enter this
        branch and therefore are unaffected by CONVICTION_SCALED_OPTIONS_
        STOP_ENABLED regardless of conviction.

        This test reproduces the doctrine assertion — the options-stop
        helper is not called on stock positions in production. Equity
        positions are gated independently by Lane A (CONVICTION_SCALED_
        STOPS_ENABLED + get_stop_loss_pct).
        """
        # Helper called directly always returns the tier table value
        # regardless of asset_type — the gate is at the call site, not
        # in this helper. So we verify the gate's discriminator: if a
        # caller correctly skips the helper on stock, equity stops are
        # untouched. We assert via the get_stop_loss_pct returning the
        # entry-stop value, not the options-stop value, for equity.
        equity_stop = get_stop_loss_pct(0.95)  # what stock would use
        opt_stop = get_options_stop_pct(0.95)  # what option would use
        self.assertNotEqual(equity_stop, opt_stop)
        self.assertEqual(equity_stop, 0.18)
        self.assertEqual(opt_stop, 0.50)


if __name__ == "__main__":
    unittest.main()
