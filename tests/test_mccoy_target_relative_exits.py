"""HM-MCCOY-TARGET-RELATIVE 2026-07-02 — unit tests for the pure
target-relative tier functions in engine/crew_scanner.py.

Scope mirrors tests/test_proving_ground_evaluator.py's approach: test the
pure functions in isolation, no DB/network I/O.
"""
from __future__ import annotations

import unittest

from engine.crew_scanner import (
    _parse_signal_stop_target,
    _target_relative_tiers,
    MCCOY_GLOBAL_HARD_STOP_PCT,
    MCCOY_T1_TARGET_FRACTION,
    MCCOY_T2_TARGET_FRACTION,
)


class ParseSignalStopTargetTests(unittest.TestCase):
    def test_parses_real_production_format(self) -> None:
        reasoning = (
            "[EVENTS-BUS-CONSUMER] dispatched from signals_v2 "
            "[AUTO-STOP: -8% from entry] [AUTO-TARGET: +16% from entry]"
        )
        result = _parse_signal_stop_target(reasoning)
        self.assertEqual(result, (0.08, 0.16))

    def test_parses_wider_stop_target_pair(self) -> None:
        reasoning = "[AUTO-STOP: -18% from entry] [AUTO-TARGET: +36% from entry]"
        result = _parse_signal_stop_target(reasoning)
        self.assertEqual(result, (0.18, 0.36))

    def test_returns_none_for_missing_pattern(self) -> None:
        self.assertIsNone(_parse_signal_stop_target("some other reasoning text"))

    def test_returns_none_for_empty_string(self) -> None:
        self.assertIsNone(_parse_signal_stop_target(""))

    def test_returns_none_for_none_input(self) -> None:
        self.assertIsNone(_parse_signal_stop_target(None))


class TargetRelativeTiersTests(unittest.TestCase):
    # ── the core invariant: T1 must never be tighter than the effective stop ──
    def test_invariant_t1_never_below_stop_at_2to1_ratio(self) -> None:
        # Real production ratio: target = 2x stop (e.g. -8%/+16%). Naive 40%
        # of target = 6.4%, which is BELOW the 8% stop -- the clamp must win.
        tiers = _target_relative_tiers(target_pct=0.16, effective_stop_pct=0.08)
        self.assertGreaterEqual(tiers["t1_pct"], 0.08)
        self.assertEqual(tiers["t1_pct"], 0.08, "clamp should bind exactly at 2:1")

    def test_invariant_holds_across_all_observed_production_ratios(self) -> None:
        # -8/+16, -12/+24, -15/+30, -18/+36 -- all real signals seen this session.
        for stop, target in [(0.08, 0.16), (0.12, 0.24), (0.15, 0.30), (0.18, 0.36)]:
            with self.subTest(stop=stop, target=target):
                tiers = _target_relative_tiers(target_pct=target, effective_stop_pct=stop)
                self.assertGreaterEqual(
                    tiers["t1_pct"], stop,
                    f"T1 {tiers['t1_pct']} must be >= stop {stop} -- never risk more than banked at T1",
                )

    def test_wider_ratio_lets_raw_fraction_take_over(self) -> None:
        # If target/stop ratio ever widens beyond 2:1 (e.g. 3:1: -8%/+24%),
        # 40% of target (9.6%) exceeds the 8% stop -- clamp shouldn't bind.
        tiers = _target_relative_tiers(target_pct=0.24, effective_stop_pct=0.08)
        self.assertAlmostEqual(tiers["t1_pct"], MCCOY_T1_TARGET_FRACTION * 0.24)
        self.assertGreater(tiers["t1_pct"], 0.08)

    def test_t2_always_strictly_above_t1(self) -> None:
        for target in (0.16, 0.24, 0.30, 0.36):
            with self.subTest(target=target):
                tiers = _target_relative_tiers(target_pct=target)
                self.assertGreater(tiers["t2_pct"], tiers["t1_pct"])

    def test_default_effective_stop_matches_module_constant(self) -> None:
        tiers = _target_relative_tiers(target_pct=0.16)
        self.assertEqual(tiers["t1_pct"], MCCOY_GLOBAL_HARD_STOP_PCT)

    def test_t2_is_70pct_of_target_when_unclamped(self) -> None:
        tiers = _target_relative_tiers(target_pct=0.36, effective_stop_pct=0.08)
        self.assertAlmostEqual(tiers["t2_pct"], MCCOY_T2_TARGET_FRACTION * 0.36)


if __name__ == "__main__":
    unittest.main()
