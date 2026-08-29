"""HM-BRIDGE-SEASON-PNL 2026-08-29 — fleet-level season P&L rollup.

dashboard.app._fleet_season_rollup() sums each leaderboard row's
season_overlay.season_pnl into a single fleet-level figure for the header
tiles (bridge-v2's "S{season} P&L" vital + /classic's FLEET P&L strip).
Pure function over already-computed rows (no DB) -- these tests cover the
math directly with synthetic season_overlay dicts, per the ticket's two
named edge cases: an agent with no season trades contributes 0, and a
halted agent still counts (no halt_mode filtering happens in the rollup
itself -- that's leaderboard()'s job upstream, same convention the
existing total_pnl/day_pnl rollup in fleet_pnl() already follows).
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _row(player_id, season_pnl, season_baseline, halt_mode="active"):
    return {
        "player_id": player_id,
        "halt_mode": halt_mode,
        "season_overlay": {
            "season_pnl": season_pnl,
            "season_baseline": season_baseline,
        },
    }


def test_basic_sum_and_return_pct():
    import dashboard.app as app_module

    rows = [
        _row("mccoy", 500.0, 10000.0),
        _row("dax", -200.0, 10000.0),
        _row("troi", 150.0, 10000.0),
    ]
    result = app_module._fleet_season_rollup(rows)
    assert result["season_pnl"] == 450.0
    assert result["season_baseline"] == 30000.0
    assert result["season_return_pct"] == 1.5  # 450 / 30000 * 100


def test_agent_with_no_season_trades_contributes_zero():
    import dashboard.app as app_module

    rows = [
        _row("mccoy", 500.0, 10000.0),
        _row("fresh-agent", 0.0, 10000.0),  # no season trades yet -- flat
    ]
    result = app_module._fleet_season_rollup(rows)
    assert result["season_pnl"] == 500.0
    assert result["season_baseline"] == 20000.0
    assert result["season_return_pct"] == 2.5


def test_halted_agent_still_counts_per_existing_convention():
    """Rollup does no halt_mode filtering of its own -- rows are whatever
    leaderboard() already selected (FLEET_ACTIVE roster, halted or not),
    same as the pre-existing total_pnl/day_pnl sums in fleet_pnl()."""
    import dashboard.app as app_module

    rows = [
        _row("mccoy", 500.0, 10000.0, halt_mode="active"),
        _row("halted-agent", -300.0, 10000.0, halt_mode="exit_only"),
    ]
    result = app_module._fleet_season_rollup(rows)
    assert result["season_pnl"] == 200.0
    assert result["season_baseline"] == 20000.0


def test_empty_rows_no_division_by_zero():
    import dashboard.app as app_module

    result = app_module._fleet_season_rollup([])
    assert result["season_pnl"] == 0.0
    assert result["season_baseline"] == 0.0
    assert result["season_return_pct"] == 0.0


def test_row_missing_season_overlay_defensive_zero():
    """A row without season_overlay at all (shouldn't happen -- every
    leaderboard row gets one -- but the rollup must not crash on it)."""
    import dashboard.app as app_module

    rows = [
        _row("mccoy", 500.0, 10000.0),
        {"player_id": "no-overlay", "halt_mode": "active"},
    ]
    result = app_module._fleet_season_rollup(rows)
    assert result["season_pnl"] == 500.0
    assert result["season_baseline"] == 10000.0


def test_rounding_to_two_decimals():
    import dashboard.app as app_module

    rows = [_row("mccoy", 100.005, 10000.0)]
    result = app_module._fleet_season_rollup(rows)
    assert result["season_pnl"] == round(100.005, 2)
