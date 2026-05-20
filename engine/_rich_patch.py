"""engine/_rich_patch.py — Rich Console monkey-patch (HM-LOG-DATE-PREFIX).

Inject an ISO ``[YYYY-MM-DD HH:MM:SS]`` prefix into Rich ``console.log(...)``
output across the entire codebase by patching ``rich.console.Console.__init__``.

Background
==========

OllieTrades has ~141 decentralized ``Console()`` init sites with no central
wrapper. Rich's default ``log_time_format`` is ``"[%X]"`` which emits a
time-only prefix (``[HH:MM:SS]``). Every weekly review section that needs to
date-slice ``trader.log`` lines is blocked by the missing date.

Adding the ``[YYYY-MM-DD HH:MM:SS]`` prefix to every log line by editing
each call site would normally require touching 141 files. Per the
``HM-CONSOLE-INIT`` memory lesson (2026-05-13), every touched module also
requires a runtime smoke before commit — a 141-file diff is a multi-day ship.

Instead, this module monkey-patches ``rich.console.Console.__init__`` once,
at import time, to inject ``log_time_format="[%Y-%m-%d %H:%M:%S]"`` whenever
the caller did not specify one. Existing callers that pass their own
``log_time_format`` are unaffected.

Trade-offs accepted
===================

- **Non-idiomatic Python** (monkey-patch of a third-party class).
- A future Rich major-version upgrade could change the ``Console.__init__``
  signature and break the patching. The failure mode is visible: logs revert
  to time-only output (not silent corruption).
- Surprise factor for new readers ("where does the date prefix come from?").

Mitigations
===========

- Failure mode is visible (logs revert to time-only) — not silent.
- ``rich.console.Console.__init__`` has had a stable signature for years.
- This module docstring + the ``main.py`` import comment document the
  decision so a new reader can follow the trail.
- Cleanup ticket filed: ``HM-RICH-CONSOLE-FACTORY`` — post-migration work to
  introduce a project Console factory and remove the monkey-patch.

Activation
==========

Import this module ONCE at the top of ``main.py``, BEFORE any other engine
imports. The patch applies to all subsequent ``Console()`` instantiations
across the codebase at their module-load time.

Rollback
========

Remove the ``import engine._rich_patch`` line in ``main.py``. The file
becomes unused. No data migration or special trader restart sequence
needed beyond the next regular restart.
"""

from __future__ import annotations

from typing import Any

import rich.console

_DESIRED_FORMAT: str = "[%Y-%m-%d %H:%M:%S]"

_ORIGINAL_INIT = rich.console.Console.__init__

_PATCH_APPLIED: bool = False


def _patched_init(self: rich.console.Console, *args: Any, **kwargs: Any) -> None:
    """Wrapped Console.__init__ that injects log_time_format when absent."""
    if "log_time_format" not in kwargs:
        kwargs["log_time_format"] = _DESIRED_FORMAT
    _ORIGINAL_INIT(self, *args, **kwargs)


def apply_patch() -> None:
    """Install the patched ``__init__`` onto ``rich.console.Console``.

    Idempotent: subsequent calls after the first are no-ops. The check
    against ``_PATCH_APPLIED`` prevents wrapping ``_patched_init`` around
    itself, which would otherwise corrupt ``_ORIGINAL_INIT`` on re-apply.
    """
    global _PATCH_APPLIED
    if _PATCH_APPLIED:
        return
    rich.console.Console.__init__ = _patched_init  # type: ignore[method-assign]
    _PATCH_APPLIED = True


apply_patch()
