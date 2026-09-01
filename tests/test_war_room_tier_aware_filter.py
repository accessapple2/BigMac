"""tests/test_war_room_tier_aware_filter.py — HM-WAR-ROOM-TIER-AWARE-2026-09-01.

Live case: Worf (qwen3-8b-flash) was removed from every main.py scan tier
on 2026-05-29 (benched, ADVISORY_CREW) but engine.war_room's debate
eligibility filter only checked is_paused/is_active/halt_mode -- it had no
concept of scan tiers or ADVISORY_CREW, so Worf kept debating (389x on
2026-09-01) despite being structurally unable to ever reach
save_signal()/decision_audit. Pure wasted GPU on a 16GB box already
fighting Ollama model-swap thrash.

These tests cover the fix by source inspection (run_war_room is a long,
side-effecting function with live LLM/DB calls not worth mocking end-to-end
here) plus a direct check on the imported exclusion set itself.
"""
from __future__ import annotations

import inspect
import unittest

import engine.war_room as war_room
from engine.crew_specialization import ADVISORY_CREW


class AdvisoryCrewImportTests(unittest.TestCase):
    def test_advisory_crew_ids_loaded(self) -> None:
        """The live ADVISORY_CREW list must actually be loaded, not silently
        empty (which would make the tier-aware filter a no-op)."""
        self.assertGreater(len(war_room._ADVISORY_CREW_IDS), 0)

    def test_advisory_crew_ids_matches_source_list(self) -> None:
        self.assertEqual(war_room._ADVISORY_CREW_IDS, frozenset(ADVISORY_CREW))

    def test_worf_is_excluded(self) -> None:
        """Regression guard for the exact live incident this fix addresses."""
        self.assertIn("qwen3-8b-flash", war_room._ADVISORY_CREW_IDS)

    def test_designed_advisory_agents_also_covered(self) -> None:
        """Agents that were already in _WAR_ROOM_SKIP for unrelated reasons
        (dedup/specialist) are also in ADVISORY_CREW -- this fix must not
        remove or duplicate-conflict with that existing exclusion."""
        for pid in ("energy-arnold", "options-sosnoff", "dalio-metals", "super-agent"):
            self.assertIn(pid, war_room._ADVISORY_CREW_IDS)
            self.assertIn(pid, war_room._WAR_ROOM_SKIP)


class EligibilityFilterSourceTests(unittest.TestCase):
    """Source-inspection guard: both eligibility-filter sites inside
    run_war_room must check _ADVISORY_CREW_IDS -- there are two independent
    filter sites (expected-roster precompute + actual eligibility loop) and
    a fix that only touches one is an incomplete fix."""

    def test_run_war_room_checks_advisory_crew_ids(self) -> None:
        src = inspect.getsource(war_room.run_war_room)
        occurrences = src.count("_ADVISORY_CREW_IDS")
        # 1 reference per filter site (2 sites) = 2, not counting the
        # module-level definition (outside this function's source).
        self.assertGreaterEqual(occurrences, 2)


if __name__ == "__main__":
    unittest.main()
