"""HM-GEX-CONSUMER-BATCH 2026-09-14 (item 6 of the relay §10 batch) --
kirk_advisory._get_gex_context must not report a stale snapshot's regime or put
wall as current.

Live finding (relay_2026-09-14_screened_scan_silence_trace.md §10): it read
gex_calculator.get_latest_snapshot("SPY") with no age check. Now
canonical_gex_if_fresh("SPY"); stale -> ("unknown", 0), the existing no-data
result.

Both sources are stubbed at the function the old and new code each call, so
the same tests run against both.
"""
from __future__ import annotations

import pytest

import engine.canonical_gex as cg
import engine.kirk_advisory as ka
import gex_calculator

STALE_ALPACA_ROW = {
    "symbol": "SPY", "spot_price": 742.09, "total_gex": 846063467.9, "gamma_flip": 752.0,
    "call_wall": 748.0, "put_wall": 607.0, "max_gamma_strike": 740.0,
    "created_at": "2026-07-21 20:05:20", "levels": [],
}


@pytest.fixture
def sources(monkeypatch):
    state = {"canonical": None}
    monkeypatch.setattr(gex_calculator, "get_latest_snapshot", lambda sym: dict(STALE_ALPACA_ROW))
    monkeypatch.setattr(cg, "canonical_gex_if_fresh", lambda sym: state["canonical"])
    return state


def test_stale_snapshot_reports_unknown_regime_and_no_put_wall(sources):
    assert ka._get_gex_context() == ("unknown", 0)


def test_fresh_canonical_snapshot_drives_regime_and_put_wall(sources):
    sources["canonical"] = {
        "underlying": "SPY", "spot": 759.33, "total_gex": -12767156387.08, "gamma_flip": 766.31,
        "call_wall": 775.0, "put_wall": 750.0, "king_node": 775.0, "_src": "alpaca",
    }
    assert ka._get_gex_context() == ("volatile", 750.0)


def test_reader_exception_degrades_to_unknown(sources, monkeypatch):
    def _boom(sym):
        raise RuntimeError("db locked")

    monkeypatch.setattr(cg, "canonical_gex_if_fresh", _boom)
    assert ka._get_gex_context() == ("unknown", 0)


def test_deploy_reasoning_never_names_a_zero_put_wall():
    text = ka._cash_deploy_reasoning(30, 31.2, 0)
    assert "$0" not in text and "put wall $" not in text
    assert "no fresh GEX put wall" in text


def test_deploy_reasoning_names_a_fresh_put_wall():
    assert "near put wall $750." in ka._cash_deploy_reasoning(30, 31.2, 750.0)
