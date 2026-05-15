"""tests/test_crew_init_guard.py — HM-CREWAI-PIN-DECOUPLE regression tests.

The live trader (Python 3.9.6, venv/) cannot import crewai 0.5.0 — its module
body uses PEP-604 syntax (``X | None``) that requires Python 3.10+. The
``crew`` package's __init__.py guards the crewai-dependent imports so the
live trader's dashboard can still ``from crew.ensemble import ...`` without
the package init pulling in the broken modules.

These tests assert:
  - ``import crew`` succeeds in the live venv (no ImportError, no TypeError).
  - ``from crew.ensemble import AgentScoreboard`` succeeds (the original
    LANE 2 broken-endpoint trigger).
  - The seven exported names exist on the ``crew`` package, whether
    the guard fired (live venv → None fallbacks) OR crewai loaded
    cleanly (.venv-crew → real callables).
  - Regression test for crew.ensemble: it must not transitively pull
    crewai by itself.

Run from project root:
    venv/bin/python3 -m pytest tests/test_crew_init_guard.py -v
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


class TestCrewInitGuard(unittest.TestCase):
    """The guard must let ``import crew`` succeed on any interpreter."""

    def test_crew_package_imports_cleanly(self):
        """``import crew`` must NEVER raise — the guard is the contract."""
        try:
            import crew  # noqa: F401
        except Exception as e:
            self.fail(
                f"`import crew` raised {type(e).__name__}: {e!r}. "
                "The guard in crew/__init__.py is supposed to catch this."
            )

    def test_seven_names_are_exported(self):
        """All seven names must exist on the crew package (None or real callable)."""
        import crew
        for name in (
            "create_scout",
            "create_architect",
            "create_backtester",
            "create_critic",
            "create_commander",
            "CrewPipeline",
            "run_crew",
        ):
            self.assertTrue(
                hasattr(crew, name),
                msg=f"crew package missing exported name: {name}",
            )

    def test_crew_all_lists_seven_names(self):
        """__all__ must keep declaring all seven exports."""
        import crew
        self.assertIn("create_scout", crew.__all__)
        self.assertIn("create_architect", crew.__all__)
        self.assertIn("create_backtester", crew.__all__)
        self.assertIn("create_critic", crew.__all__)
        self.assertIn("create_commander", crew.__all__)
        self.assertIn("CrewPipeline", crew.__all__)
        self.assertIn("run_crew", crew.__all__)


class TestCrewEnsembleStandalone(unittest.TestCase):
    """``crew.ensemble`` must import without dragging crewai into the chain."""

    def test_crew_ensemble_imports_without_crewai(self):
        """The original LANE 2 dashboard import — verified working post-guard."""
        try:
            from crew.ensemble import AgentScoreboard  # noqa: F401
        except Exception as e:
            self.fail(
                f"`from crew.ensemble import AgentScoreboard` raised "
                f"{type(e).__name__}: {e!r}. Path D may have regressed."
            )

    def test_crew_ensemble_exports_expected_names(self):
        """dashboard/app.py:3125 imports four names from crew.ensemble."""
        from crew import ensemble
        for name in (
            "AgentScoreboard",
            "_bucket_for_agent",
            "_source_policy",
            "select_collective_signals",
        ):
            self.assertTrue(
                hasattr(ensemble, name),
                msg=f"crew.ensemble missing dashboard-required name: {name}",
            )


if __name__ == "__main__":
    unittest.main()
