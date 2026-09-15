"""HM-GEX-CONSUMER-BATCH 2026-09-14 (item 7 of the relay §10 batch) --
kirk_briefing.gather_gex must not report a stale snapshot as the briefing's GEX.

Live finding (relay_2026-09-14_screened_scan_silence_trace.md §10): gather_gex
read gex_calculator.get_latest_snapshot("SPY") with no age check, and it feeds
the Kirk daily briefing (5 crontab lines). Now canonical_gex_if_fresh("SPY");
stale -> {}, the function's existing degraded result.

Both sources are stubbed at the function the old and new code each call.
"""
from __future__ import annotations

import pytest

import engine.canonical_gex as cg
import gex_calculator
import kirk_briefing as kb

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


def test_stale_snapshot_gives_the_briefing_no_gex(sources):
    assert kb.gather_gex() == {}


def test_fresh_canonical_snapshot_is_reported(sources):
    sources["canonical"] = {
        "underlying": "SPY", "spot": 759.33, "total_gex": -12767156387.08, "gamma_flip": 766.31,
        "call_wall": 775.0, "put_wall": 750.0, "king_node": 775.0, "_src": "alpaca",
    }
    assert kb.gather_gex() == {
        "regime": "negative/volatile", "total_gex": -12767156387.08,
        "put_wall": 750.0, "call_wall": 775.0,
    }
