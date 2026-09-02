"""tests/test_gex_staleness_marker.py -- HM-GEX-STALENESS-MARKER-2026-09-01.

Captain flagged /api/market/gex's `as_of` still reading 2026-07-21 (frozen
since HM-GEX-RETIRED, the Polygon options-chain 403) and rendering
identically to a genuinely fresh snapshot. dashboard/app.py::_gex_age_days()
computes age-from-as_of once, server-side, so gex_all() (multi-ticker) and
gex_ticker() (single-ticker) both expose age_days/stale without either
consumer needing its own clock math.

HM-GEX-STALENESS-MARKER-2026-09-01 part 2 (same day, follow-up): the first
pass covered gex_all()/gex_ticker() only -- 2 of ~6 fossil-reading dashboard
endpoints. /api/gex-overlay/levels (the dashboard's highest-traffic GEX
endpoint by a wide margin -- 147 hits vs 2-10 for the others in a live
trader.log sample, because index.html polls it every 15 min per open tab)
and /api/gex-overlay/heatmap now carry the same age_days/stale fields too,
tested below by mocking _canonical_gex_cached directly (no network/DB).

/api/chart-data's embedded gex_levels block got the same fields (see
dashboard/app.py's "GEX levels" section, ~line 16287) but isn't given a
dedicated endpoint-level test here -- that endpoint's other sections
(candles, indicators, battle station, trades, convergences) each make live
Alpaca/DB calls unrelated to GEX, making a full invocation expensive and
flaky for a one-field addition. It reuses the exact same _gex_age_days/
_gex_is_stale functions already exercised below and in
test_gex_freshness_gate.py -- verified by code inspection instead.

Only exercises the pure helper + the two overlay endpoints -- no DB, no
network, no app startup.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

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


def test_gex_overlay_levels_carries_staleness_fields():
    stale_asof = "2026-07-21T13:05:00"
    fake = {"underlying": "SPY", "spot": 740.0, "gamma_flip": 730.0,
            "king_node": 750.0, "put_wall": 700.0, "call_wall": 780.0,
            "_asof": stale_asof, "_src": "daily-flow_gex.db"}
    with patch.object(app_module, "_canonical_gex_cached", return_value=fake):
        result = app_module.gex_overlay_levels("SPY")
    assert result["as_of"] == stale_asof
    assert result["age_days"] is not None and result["age_days"] > 40
    assert result["stale"] is True


def test_gex_overlay_levels_fresh_is_not_stale():
    fresh_asof = datetime.now(timezone.utc).isoformat()
    fake = {"underlying": "SPY", "spot": 740.0, "gamma_flip": 730.0,
            "king_node": 750.0, "put_wall": 700.0, "call_wall": 780.0,
            "_asof": fresh_asof, "_src": "intraday-cache"}
    with patch.object(app_module, "_canonical_gex_cached", return_value=fake):
        result = app_module.gex_overlay_levels("SPY")
    assert result["stale"] is False


def test_gex_overlay_heatmap_carries_staleness_fields():
    stale_asof = "2026-07-21T13:05:00"
    fake = {"underlying": "SPY", "strikes": [{"strike": 740.0, "net_gex": 1.0}],
            "_asof": stale_asof, "_src": "daily-flow_gex.db"}
    with patch.object(app_module, "_canonical_gex_cached", return_value=fake):
        result = app_module.gex_overlay_heatmap("SPY")
    assert result["as_of"] == stale_asof
    assert result["stale"] is True
    assert result["count"] == 1


def test_gex_overlay_endpoints_propagate_error_unchanged():
    """An upstream error must still short-circuit before the staleness
    fields are computed -- no behavior change to the existing error path."""
    with patch.object(app_module, "_canonical_gex_cached",
                       return_value={"error": "no cached GEX available", "pending": False}):
        levels = app_module.gex_overlay_levels("SPY")
        heatmap = app_module.gex_overlay_heatmap("SPY")
    assert levels == {"error": "no cached GEX available", "pending": False}
    assert heatmap["error"] == "no cached GEX available"
    assert "stale" not in heatmap


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-q"]))
