"""HM-FALSE-RED-ALERT 2026-09-09: set before any test module imports, so it
covers module-level side effects during collection too, not just the
window `PYTEST_CURRENT_TEST` is set for during an individual test's call.
See engine/alert_channels.py::_under_pytest() for the guard this enables.
"""
import os

os.environ.setdefault("OT_ALERTS_DISABLED", "1")
