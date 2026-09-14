"""HM-GEX-PROMPT-FRESHNESS 2026-09-14 — the GEX OVERLAY block injected into
every agent's scan prompt must never present a stale level as current.

Live finding: engine/gex_overlay.get_gex_context_for_prompt() bypassed
engine.canonical_gex entirely. It read the dead Polygon intraday cache, then
fell straight to flow_gex.db's frozen 2026-07-21 row with no age check. So
every McCoy prompt since the 2026-09-11 num_ctx fix said
"GEX OVERLAY (updated 20:05) — SPY: Gamma Flip $752 | Put Wall $607 |
Call Wall $748" while SPY traded ~$760. Against FlashAlpha's 2026-09-14
recap that fails on both net-GEX sign and call-wall side. The fresh Alpaca
snapshot (tier 0 of canonical_gex) was never consulted.

Tiers are stubbed at the source functions (Alpaca tier-0 reader, Polygon
intraday cache, flow_gex.db reader) so the same test runs against the old
and new block builder."""
from __future__ import annotations

import sys
import types
from datetime import datetime, timedelta, timezone

import pytest

import engine
import engine.canonical_gex as cg
import engine.gex_overlay as go

FOSSIL = {
    "SPY": {"underlying": "SPY", "spot": 742.09, "total_gex": 846063467.9, "gamma_flip": 752.0,
            "call_wall": 748.0, "put_wall": 607.0, "king_node": 740.0,
            "regime": "SHORT GAMMA · volatile (spot below flip)",
            "_asof": "2026-07-21 20:05:20", "_src": "daily-flow_gex.db"},
    "QQQ": {"underlying": "QQQ", "spot": 696.06, "total_gex": -183458428.1, "gamma_flip": 716.47,
            "call_wall": 710.0, "put_wall": 619.0, "king_node": 710.0,
            "regime": "SHORT GAMMA · volatile (spot below flip)",
            "_asof": "2026-07-21 20:05:38", "_src": "daily-flow_gex.db"},
}


def _utc_minutes_ago(minutes: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")


@pytest.fixture
def tiers(monkeypatch):
    state = {"alpaca": {}}
    fake_ofg = types.ModuleType("engine.options_flow_gex")
    fake_ofg.get_latest = lambda: {"data": {}, "ts": None}

    def _no_live_compute(sym):
        raise RuntimeError("live compute must not be reached in this test")

    fake_ofg.compute_gex = _no_live_compute
    monkeypatch.setitem(sys.modules, "engine.options_flow_gex", fake_ofg)
    monkeypatch.setattr(engine, "options_flow_gex", fake_ofg, raising=False)
    monkeypatch.setattr(cg, "_alpaca_snapshot_fresh", lambda sym: state["alpaca"].get(sym))
    monkeypatch.setattr(cg, "latest_snapshot", lambda sym: dict(FOSSIL[sym]) if sym in FOSSIL else None)
    return state


def test_fossil_levels_never_reach_the_prompt(tiers):
    block = go.get_gex_context_for_prompt()
    for stale_level in ("$748", "$752", "$607", "$740", "$716", "$619", "$710"):
        assert stale_level not in block
    assert "20:05" not in block
    assert "SPY: GEX UNAVAILABLE" in block and "QQQ: GEX UNAVAILABLE" in block


def test_fresh_alpaca_snapshot_is_used_and_labelled_with_age_and_source(tiers):
    tiers["alpaca"]["SPY"] = {
        "underlying": "SPY", "spot": 759.33, "total_gex": -12767156387.08, "gamma_flip": 766.31,
        "call_wall": 775.0, "put_wall": 750.0, "king_node": 775.0,
        "regime": "SHORT GAMMA · volatile (spot below flip)",
        "_asof": _utc_minutes_ago(12), "_src": "alpaca",
    }
    block = go.get_gex_context_for_prompt()
    spy = next(line for line in block.splitlines() if line.startswith("SPY"))
    assert "Gamma Flip $766" in spy and "Call Wall $775" in spy and "Put Wall $750" in spy
    assert "source alpaca" in spy
    assert "12 min old" in spy or "11 min old" in spy or "13 min old" in spy
    assert "QQQ: GEX UNAVAILABLE" in block  # QQQ only has the fossil


def test_snapshot_just_under_the_shared_gate_is_shown_with_its_age(tiers, monkeypatch):
    recent_daily = dict(FOSSIL["SPY"], _asof=_utc_minutes_ago(600))
    monkeypatch.setattr(cg, "latest_snapshot", lambda sym: dict(recent_daily) if sym == "SPY" else None)
    block = go.get_gex_context_for_prompt()
    spy = next(line for line in block.splitlines() if line.startswith("SPY"))
    assert "Call Wall $748" in spy and "600 min old" in spy


def test_reader_exception_degrades_to_unavailable(tiers, monkeypatch):
    def _boom(sym):
        raise RuntimeError("db locked")

    monkeypatch.setattr(cg, "canonical_gex_if_fresh", _boom)
    block = go.get_gex_context_for_prompt()
    assert "SPY: GEX UNAVAILABLE" in block and "$748" not in block
