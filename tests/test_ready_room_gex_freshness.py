"""HM-GEX-CONSUMER-BATCH 2026-09-14 (item 8 of the relay §10 batch) --
ready_room's DB-snapshot fallback must not build a briefing from a stale snapshot.

Live finding (relay_2026-09-14_screened_scan_silence_trace.md §10): when the live
Alpaca compute failed, generate_ready_room_briefing() took the newest
gex_snapshots row at any age. The canonical overlay only replaces walls when
canonical is fresh, so the stale spot/walls/flip reached Troi and bridge_vote.

Every call past the GEX step (_get_vix, _get_pc_ratio_cboe) is stubbed to raise,
so the old code fails loudly here instead of reaching the network.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

import engine.canonical_gex as cg
import engine.ready_room as rr
import gex_calculator


def _utc_minutes_ago(minutes: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")


def _snapshot_row(created_at: str) -> dict:
    return {
        "symbol": "SPY", "spot_price": 742.09, "timestamp": "2026-07-21T13:05:20",
        "levels": [{"strike": 748.0, "net_gex": 1.0, "call_gex": 1.0, "put_gex": 0.0}],
        "max_gamma_strike": 740.0, "zero_gamma_level": 752.0, "put_wall": 607.0,
        "call_wall": 748.0, "gamma_flip": 752.0, "total_gex": 846063467.9,
        "created_at": created_at,
    }


@pytest.fixture
def briefing_env(monkeypatch):
    state = {"snapshot": None}

    def _live_compute_down(sym, force=False):
        raise RuntimeError("Alpaca options chain unavailable")

    def _past_gex_step(*args, **kwargs):
        raise RuntimeError("briefing continued past the GEX step on a stale snapshot")

    monkeypatch.setattr(gex_calculator, "compute_gex_sync", _live_compute_down)
    monkeypatch.setattr(gex_calculator, "get_latest_snapshot", lambda sym: state["snapshot"])
    monkeypatch.setattr(cg, "canonical_gex_if_fresh", lambda sym: None)
    monkeypatch.setattr(rr, "_init_db", lambda: None)
    monkeypatch.setattr(rr, "_CACHE", {})
    monkeypatch.setattr(rr, "_get_vix", _past_gex_step)
    monkeypatch.setattr(rr, "_get_pc_ratio_cboe", _past_gex_step)
    return state


def test_stale_snapshot_fallback_yields_gex_unavailable(briefing_env):
    briefing_env["snapshot"] = _snapshot_row("2026-07-21 20:05:20")
    result = rr.generate_ready_room_briefing(force=True)
    assert result.get("briefing") is None
    assert "GEX data unavailable" in result.get("error", "")


def test_fresh_snapshot_profile_is_used(briefing_env):
    briefing_env["snapshot"] = _snapshot_row(_utc_minutes_ago(10))
    profile = rr._fresh_snapshot_profile("SPY")
    assert profile is not None and profile.put_wall == 607.0


@pytest.mark.parametrize("created_at", ["2026-07-21 20:05:20", None, "garbage"])
def test_stale_missing_or_garbage_created_at_is_not_used(briefing_env, created_at):
    briefing_env["snapshot"] = _snapshot_row(created_at)
    assert rr._fresh_snapshot_profile("SPY") is None


def test_snapshot_just_past_the_30_minute_bar_is_not_used(briefing_env):
    briefing_env["snapshot"] = _snapshot_row(_utc_minutes_ago(45))
    assert rr._fresh_snapshot_profile("SPY") is None
