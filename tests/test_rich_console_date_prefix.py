"""tests/test_rich_console_date_prefix.py — HM-LOG-DATE-PREFIX regression tests.

Covers the Rich Console monkey-patch in ``engine/_rich_patch.py`` that injects
an ISO date prefix into ``console.log(...)`` output for every Console()
instance constructed across the codebase (~141 sites, no central wrapper).

The patch overrides ``rich.console.Console.__init__`` to set
``log_time_format="[%Y-%m-%d %H:%M:%S]"`` whenever the caller did not pass
``log_time_format`` themselves. Existing explicit overrides are preserved.

Asserted behaviors:

  - Importing ``engine._rich_patch`` replaces ``Console.__init__`` with the
    patched version (test 1).
  - Existing ``Console(log_time_format=...)`` callers are NOT overridden —
    the patch only injects when the caller's value is absent (test 2).
  - ``console.log("test")`` output contains the ``[YYYY-MM-DD HH:MM:SS]``
    format prefix once the patch is active (test 3).
  - The injected prefix matches the expected ISO regex with today's date
    (test 4).
  - ``apply_patch()`` is idempotent — calling it twice does not double-wrap
    ``Console.__init__`` (test 5).

Run from project root:

    venv/bin/python3 -m pytest tests/test_rich_console_date_prefix.py -v
"""

from __future__ import annotations

import io
import re
import sys
import unittest
from datetime import datetime
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


_ISO_PREFIX_RE = re.compile(r"\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\]")


class RichConsoleDatePrefixTests(unittest.TestCase):
    """Surface: ``engine/_rich_patch.py`` monkey-patch of Console.__init__."""

    def test_1_import_replaces_console_init(self) -> None:
        """Importing engine._rich_patch replaces rich.console.Console.__init__."""
        import rich.console

        # Capture the (already-patched-or-not) state before import.
        # If the patch module has already been imported by a prior test, the
        # current Console.__init__ is the patched one — verify against the
        # module's _patched_init reference.
        import engine._rich_patch as patch

        self.assertIs(
            rich.console.Console.__init__,
            patch._patched_init,
            "Console.__init__ should be replaced by engine._rich_patch._patched_init",
        )
        self.assertIsNot(
            patch._ORIGINAL_INIT,
            patch._patched_init,
            "engine._rich_patch must store the pre-patch __init__ separately",
        )

    def test_2_explicit_log_time_format_preserved(self) -> None:
        """Caller-supplied log_time_format is not overridden by the patch."""
        import engine._rich_patch  # noqa: F401 — ensure patch applied

        from rich.console import Console

        buf = io.StringIO()
        c = Console(
            file=buf,
            force_terminal=False,
            no_color=True,
            log_time_format="[CUSTOM]",
        )
        c.log("test message")
        output = buf.getvalue()

        self.assertIn(
            "[CUSTOM]",
            output,
            f"Caller's explicit log_time_format must be preserved; got {output!r}",
        )
        # The default ISO prefix must NOT be applied when caller overrode it.
        self.assertIsNone(
            _ISO_PREFIX_RE.search(output),
            f"ISO prefix injected over caller's explicit format: {output!r}",
        )

    def test_3_default_console_emits_iso_prefix(self) -> None:
        """A Console() constructed with no log_time_format gets the ISO prefix."""
        import engine._rich_patch  # noqa: F401 — ensure patch applied

        from rich.console import Console

        buf = io.StringIO()
        c = Console(file=buf, force_terminal=False, no_color=True)
        c.log("test message")
        output = buf.getvalue()

        self.assertIsNotNone(
            _ISO_PREFIX_RE.search(output),
            f"Expected ISO prefix [YYYY-MM-DD HH:MM:SS] in console.log output; got {output!r}",
        )

    def test_4_iso_prefix_matches_today(self) -> None:
        """The injected ISO prefix carries today's date in [YYYY-MM-DD HH:MM:SS] form."""
        import engine._rich_patch  # noqa: F401 — ensure patch applied

        from rich.console import Console

        buf = io.StringIO()
        c = Console(file=buf, force_terminal=False, no_color=True)
        c.log("test message")
        output = buf.getvalue()

        match = _ISO_PREFIX_RE.search(output)
        self.assertIsNotNone(match, f"No ISO prefix in {output!r}")
        # Validate the date portion is parseable and matches today's date.
        today_str = datetime.now().strftime("%Y-%m-%d")
        self.assertIn(
            today_str,
            match.group(0),
            f"ISO prefix did not contain today's date {today_str!r}; got {match.group(0)!r}",
        )

    def test_5_apply_patch_is_idempotent(self) -> None:
        """Calling apply_patch() twice does not double-wrap Console.__init__."""
        import rich.console

        import engine._rich_patch as patch

        # First apply (already applied at import time).
        first_init = rich.console.Console.__init__
        self.assertIs(
            first_init,
            patch._patched_init,
            "Pre-condition: Console.__init__ should already be _patched_init",
        )

        # Re-apply — must NOT wrap _patched_init around itself.
        patch.apply_patch()
        second_init = rich.console.Console.__init__
        self.assertIs(
            second_init,
            patch._patched_init,
            "After re-apply, Console.__init__ should still be _patched_init (not wrapped)",
        )

        # The stored original must remain the true pre-patch init, not the patched one.
        self.assertIsNot(
            patch._ORIGINAL_INIT,
            patch._patched_init,
            "Re-applying the patch must not overwrite _ORIGINAL_INIT with _patched_init",
        )

        # Sanity: a fresh Console still produces a single ISO prefix (not two stacked).
        from rich.console import Console

        buf = io.StringIO()
        c = Console(file=buf, force_terminal=False, no_color=True)
        c.log("test message")
        output = buf.getvalue()
        matches = _ISO_PREFIX_RE.findall(output)
        self.assertEqual(
            len(matches),
            1,
            f"Expected exactly one ISO prefix after idempotent re-apply; got {matches!r} in {output!r}",
        )


if __name__ == "__main__":
    unittest.main()
