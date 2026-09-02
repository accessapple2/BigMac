"""tests/test_gex_staleness_marker.py -- HM-GEX-STALENESS-MARKER-2026-09-01.

Captain flagged /api/market/gex's `as_of` still reading 2026-07-21 (frozen
since HM-GEX-RETIRED, the Polygon options-chain 403) and rendering
identically to a genuinely fresh snapshot. dashboard/app.py::_gex_age_days()
computes age-from-as_of once, server-side, so gex_all() (multi-ticker) and
gex_ticker() (single-ticker) both expose age_days/stale without either
consumer needing its own clock math.

Only exercises the pure helper -- no DB, no network, no app startup.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import dashboard.app as app_module  # noqa: E402


def test_none_input_returns_none():
    assert app_module._gex_age_days(None) is None
    assert app_module._gex_age_days("") is None


def test_unparseable_string_returns_none():
    assert app_module._gex_age_days("not-a-timestamp") is None


def test_fresh_timestamp_is_near_zero_and_not_stale():
    now = datetime.now(timezone.utc)
    age = app_module._gex_age_days(now.isoformat())
    assert age is not None
    assert 0.0 <= age < 0.01


def test_frozen_2026_07_21_snapshot_is_far_stale():
    """The exact live symptom: a snapshot frozen since HM-GEX-RETIRED must
    read as stale, not silently pass through as current."""
    age = app_module._gex_age_days("2026-07-21T13:05:00")
    assert age is not None
    assert age > 40  # weeks stale relative to 2026-09-01


def test_space_separator_format_parses_same_as_t_separator():
    """This pipeline's writers disagree on separator (' ' vs 'T') -- both
    must resolve to the same age."""
    dt = datetime.now(timezone.utc) - timedelta(days=2)
    space_form = dt.strftime("%Y-%m-%d %H:%M:%S")
    t_form = dt.isoformat()
    age_space = app_module._gex_age_days(space_form)
    age_t = app_module._gex_age_days(t_form)
    assert age_space is not None and age_t is not None
    assert abs(age_space - age_t) < 0.01


def test_stale_threshold_one_day():
    just_under = datetime.now(timezone.utc) - timedelta(hours=23)
    just_over = datetime.now(timezone.utc) - timedelta(hours=25)
    age_under = app_module._gex_age_days(just_under.isoformat())
    age_over = app_module._gex_age_days(just_over.isoformat())
    assert age_under < 1.0
    assert age_over >= 1.0


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-q"]))
