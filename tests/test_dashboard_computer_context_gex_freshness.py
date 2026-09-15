"""HM-GEX-CONSUMER-BATCH 2026-09-14 (item 10 of the relay §10 batch) --
the SPY GEX line in Captain Archer's ship-status context must not present a
stale snapshot as current.

Live finding (relay_2026-09-14_screened_scan_silence_trace.md §10):
dashboard/app.py::_build_computer_context read
gex_calculator.get_latest_snapshot("SPY") at any age and printed
gex.get('regime', '?') from gex_snapshots, which has no regime column -- so the
line was always "GEX: ? | $xB" with an unaged total. The line now comes from
_computer_context_gex_line() (canonical_gex tier 0, 30-min bar); the rest of
_build_computer_context queries the live trader.db and is not called here.
"""
from __future__ import annotations

import pytest

import dashboard.app as app
import engine.canonical_gex as cg
import gex_calculator

STALE_ALPACA_ROW = {
    "symbol": "SPY", "timestamp": "2026-09-14T12:46:36.255165", "spot_price": 759.0,
    "total_gex": -7825951917.18, "put_wall": 760.0, "call_wall": 775.0, "gamma_flip": 766.0,
    "created_at": "2026-09-14 19:46:36", "levels_json": "[]", "levels": [],
}


@pytest.fixture
def sources(monkeypatch):
    state = {"tier0": {}}
    monkeypatch.setattr(gex_calculator, "get_latest_snapshot", lambda sym: dict(STALE_ALPACA_ROW))
    monkeypatch.setattr(cg, "_alpaca_snapshot_fresh", lambda sym: state["tier0"].get(sym))
    return state


def test_stale_snapshot_is_stated_unavailable(sources):
    line = app._computer_context_gex_line()
    assert line.startswith("GEX: UNAVAILABLE")
    assert "-7.8B" not in line


def test_fresh_snapshot_carries_a_real_regime(sources):
    sources["tier0"]["SPY"] = {
        "underlying": "SPY", "spot": 759.33, "total_gex": -12767156387.08, "gamma_flip": 766.31,
        "call_wall": 775.0, "put_wall": 750.0, "king_node": 775.0,
        "regime": "SHORT GAMMA · volatile (spot below flip)", "_src": "alpaca",
    }
    assert app._computer_context_gex_line() == "GEX: SHORT GAMMA · volatile (spot below flip) | $-12.8B"
