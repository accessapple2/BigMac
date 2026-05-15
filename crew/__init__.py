"""CrewAI strategy-writing agents for USS TradeMinds.

HM-CREWAI-PIN-DECOUPLE (2026-05-15): the live trader runs on Python 3.9.6
with crewai==0.5.0, but crewai 0.5.0's import chain crashes on Python 3.9
(PEP-604 ``X | None`` syntax in crewai/utilities/rpm_controller.py). The
crewai-dependent strategy crew runs in ``main_crew.py`` against the
separate ``.venv-crew`` (Python 3.12 + crewai 1.11.1), where these imports
succeed.

The eager imports below are guarded so the LIVE trader's dashboard can do
``from crew.ensemble import ...`` without the package init pulling
``crew.agents`` / ``crew.pipeline`` into the chain. In the live venv all
seven exported names fall back to ``None``; in ``.venv-crew`` they resolve
to the real functions and classes.

Callers that need the real symbols must run inside ``.venv-crew``. The
live trader does not call any of these — verified 2026-05-15 via repo-wide
grep across engine/, main.py, dashboard/, scrapers/, scripts/, agents/,
shared/, etc.
"""

try:
    from crew.agents import (
        create_scout,
        create_architect,
        create_backtester,
        create_critic,
        create_commander,
    )
except ImportError:
    create_scout = create_architect = create_backtester = None
    create_critic = create_commander = None

try:
    from crew.pipeline import CrewPipeline, run_crew
except (ImportError, TypeError):
    # ImportError: crewai package missing.
    # TypeError: crewai imported but its module body uses Python 3.10+ syntax
    # that fails to compile on 3.9 (e.g. ``threading.Timer | None``).
    CrewPipeline = None
    run_crew = None

__all__ = [
    "create_scout",
    "create_architect",
    "create_backtester",
    "create_critic",
    "create_commander",
    "CrewPipeline",
    "run_crew",
]
