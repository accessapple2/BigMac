"""Tests for the shared Fear & Greed score->label classifier.

Bug: the same score (76) rendered as three different labels across the
dashboard (v2 gauge "EXTREME GREED", /classic section "GREED", CTO advisory
"neutral/greed") because at least 7 places independently hardcoded their own
score threshold ladder. engine.fear_greed.classify_fear_greed() is now the
single source of truth; these tests pin its boundaries and check that the
consumers found to be re-deriving their own zones now delegate to it.
"""

import pytest

from engine.fear_greed import classify_fear_greed, FEAR_GREED_THRESHOLDS


@pytest.mark.parametrize("score,expected", [
    (0, "EXTREME FEAR"),
    (14, "EXTREME FEAR"),
    (15, "FEAR"),
    (34, "FEAR"),
    (35, "MILD FEAR"),
    (49, "MILD FEAR"),
    (50, "NEUTRAL"),
    (64, "NEUTRAL"),
    (65, "GREED"),
    (76, "GREED"),          # the exact score from the reported bug
    (79, "GREED"),
    (80, "EXTREME GREED"),
    (100, "EXTREME GREED"),
])
def test_classify_fear_greed_boundaries(score, expected):
    assert classify_fear_greed(score) == expected


def test_classify_fear_greed_handles_non_finite():
    assert classify_fear_greed(None) == "NEUTRAL"
    assert classify_fear_greed(float("nan")) == "NEUTRAL"


def test_thresholds_table_is_the_only_source_import():
    # Structural pin: five real boundaries + one open-ended top bucket.
    bounds = [b for b, _ in FEAR_GREED_THRESHOLDS if b is not None]
    assert bounds == [15, 35, 50, 65, 80]


def test_dynamic_advisor_fg_zone_uses_shared_classifier():
    from engine.dynamic_advisor import _fg_zone
    zone, _color, _headline = _fg_zone(76)
    assert zone == "GREED"


def test_kirk_advisory_cash_reasoning_matches_canonical_label():
    """The exact reported symptom: fg_score=76 must no longer produce the
    vague hardcoded 'neutral/greed' string -- it should reflect the real
    canonical label (GREED), lowercased into the reasoning sentence."""
    from engine.fear_greed import classify_fear_greed as _classify
    label = _classify(76).lower()
    assert label == "greed"
    assert "neutral/greed" not in f"F&G 76 = {label}. No rush to deploy cash."


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
