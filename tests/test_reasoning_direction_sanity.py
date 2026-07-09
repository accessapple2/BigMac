"""tests/test_reasoning_direction_sanity.py — HM-REASONING-DIRECTION-SANITY-2026-07-09.

Covers a live-incident finding (2026-07-09): a HIMS signal's reasoning said
"price break below the opening range low on high volume. This bearish
breakout indicates potential reversal" but was labeled BUY_CALL (bullish).
Traced to the LLM's own free-text reasoning contradicting itself -- the
deterministic breakout-detection prompt injection (engine/volatility_breakout.py)
correctly labels BEAR as "broke below ... opening range low" and never
transforms it; nothing in the codebase flips the sign.

engine.providers.base.check_reasoning_direction_conflict() is a cheap,
visibility-only sanity check wired into parse_decision(): flags (logs) a
mismatch, never blocks. Deliberately narrow (only the ORB-style phrases this
codebase's own prompt injection uses) to keep the false-positive rate
manageable for something purely advisory.
"""
from __future__ import annotations

import unittest

from engine.providers.base import check_reasoning_direction_conflict


class ReasoningDirectionConflictTests(unittest.TestCase):
    def test_reproduces_the_actual_hims_incident(self) -> None:
        reasoning = (
            "price break below the opening range low on high volume. "
            "This bearish breakout indicates potential reversal, but smart "
            "money flow is strongly buying calls."
        )
        result = check_reasoning_direction_conflict("BUY_CALL", reasoning)
        self.assertIsNotNone(result)
        self.assertIn("BEARISH", result)
        self.assertIn("BUY_CALL", result)

    def test_bullish_breakout_with_bullish_action_no_conflict(self) -> None:
        reasoning = "Price broke above $150.00 opening range high on 3.2x volume."
        self.assertIsNone(check_reasoning_direction_conflict("BUY_CALL", reasoning))

    def test_bearish_breakout_with_bearish_action_no_conflict(self) -> None:
        reasoning = "Price broke below $148.00 opening range low on 2.8x volume."
        self.assertIsNone(check_reasoning_direction_conflict("BUY_PUT", reasoning))

    def test_bullish_breakout_with_short_action_flags(self) -> None:
        reasoning = "Strong bullish breakout above opening range high, high conviction."
        result = check_reasoning_direction_conflict("SHORT", reasoning)
        self.assertIsNotNone(result)
        self.assertIn("BULLISH", result)

    def test_no_directional_language_no_conflict(self) -> None:
        reasoning = "Strong earnings beat, raising guidance, institutional accumulation."
        self.assertIsNone(check_reasoning_direction_conflict("BUY_CALL", reasoning))

    def test_hold_action_never_flagged(self) -> None:
        reasoning = "Price broke below opening range low, bearish breakout."
        self.assertIsNone(check_reasoning_direction_conflict("HOLD", reasoning))

    def test_mixed_signals_both_phrases_present_does_not_flag(self) -> None:
        """A thesis that explicitly references BOTH a breakdown and a
        breakout (e.g. discussing a reversal) is ambiguous, not a clear
        contradiction -- deliberately conservative to avoid noise on
        legitimate mixed-signal theses."""
        reasoning = (
            "Price broke below opening range low initially but then broke "
            "back above opening range high, confirming reversal."
        )
        self.assertIsNone(check_reasoning_direction_conflict("BUY_CALL", reasoning))

    def test_empty_reasoning_no_conflict(self) -> None:
        self.assertIsNone(check_reasoning_direction_conflict("BUY_CALL", ""))

    def test_case_insensitive_matching(self) -> None:
        reasoning = "PRICE BROKE BELOW OPENING RANGE LOW ON HIGH VOLUME."
        result = check_reasoning_direction_conflict("BUY_CALL", reasoning)
        self.assertIsNotNone(result)


if __name__ == "__main__":
    unittest.main()
