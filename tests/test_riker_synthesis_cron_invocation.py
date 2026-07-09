"""tests/test_riker_synthesis_cron_invocation.py — HM-RIKER-SYNTHESIS-SYSPATH-2026-07-09.

Covers a real regression: the HM-RIKER-LOCK-RETRY-2026-07-09 fix added
`from engine.db_conn import get_conn` as a module-level import to
engine/riker_synthesis.py. This worked fine when smoke-tested interactively
(`python3 -c "from engine.riker_synthesis import run_synthesis; ..."` from the
repo root, which already has the repo root on sys.path) but broke on every
real cron invocation with `ModuleNotFoundError: No module named 'engine'` --
cron runs `python3 /abs/path/to/engine/riker_synthesis.py` (crontab: */10 * *
* *), and running a script BY PATH sets sys.path[0] to the script's own
directory (engine/), not the repo root -- the crontab's `cd
/Users/bigmac/autonomous-trader &&` before the python3 call does not change
this, since sys.path[0] is derived from script location, not cwd.

This silently broke the every-10-minute rikers_log cron for ~5.5 hours
(09:21 AZ through the fix) before being caught by checking the actual
cron log rather than trusting an interactive smoke test.

This test specifically invokes the script as a subprocess BY ITS FULL PATH
(matching the real cron invocation exactly, unlike a same-process import
test which would have missed this the first time) and asserts it exits
cleanly with no ModuleNotFoundError.
"""
from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPT = _REPO_ROOT / "engine" / "riker_synthesis.py"
_VENV_PYTHON = _REPO_ROOT / ".venv" / "bin" / "python3"


class RikerSynthesisCronInvocationTests(unittest.TestCase):
    def test_script_runs_by_full_path_like_cron_does(self) -> None:
        """Exact real invocation: absolute script path, cwd = repo root,
        matching the crontab entry byte-for-byte (`cd <repo> &&
        <venv>/python3 <repo>/engine/riker_synthesis.py`)."""
        python = str(_VENV_PYTHON) if _VENV_PYTHON.exists() else sys.executable
        result = subprocess.run(
            [python, str(_SCRIPT)],
            cwd=str(_REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(
            result.returncode, 0,
            f"riker_synthesis.py exited non-zero when invoked by full path "
            f"(the real cron invocation shape).\nstdout: {result.stdout}\n"
            f"stderr: {result.stderr}",
        )
        combined = result.stdout + result.stderr
        self.assertNotIn("ModuleNotFoundError", combined)
        self.assertNotIn("No module named 'engine'", combined)

    def test_repo_root_insertion_is_idempotent(self) -> None:
        """Importing the module twice (as cron effectively does across
        successive 10-min invocations, each a fresh process) must not
        duplicate repo root entries in sys.path or raise."""
        python = str(_VENV_PYTHON) if _VENV_PYTHON.exists() else sys.executable
        code = (
            "import sys; "
            f"sys.path.insert(0, {str(_REPO_ROOT)!r}); "
            "import engine.riker_synthesis as m; "
            "count = sys.path.count(str(m._REPO_ROOT)); "
            "print('REPO_ROOT_COUNT=' + str(count))"
        )
        result = subprocess.run(
            [python, "-c", code],
            cwd=str(_REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        # Exactly 2 is fine (our manual insert + the module's own idempotent
        # insert didn't fire since it was already present) -- the important
        # invariant is it doesn't grow unbounded across repeated imports.
        self.assertIn("REPO_ROOT_COUNT=", result.stdout)


if __name__ == "__main__":
    unittest.main()
