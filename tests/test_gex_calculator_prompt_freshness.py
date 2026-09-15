"""HM-GEX-CONSUMER-BATCH 2026-09-14 (item 5 of the relay §10 batch) --
gex_calculator.build_alpaca_gex_prompt_section (injected per symbol into agent
prompts by engine/providers/base.py) must refuse a stale snapshot.

Live finding (relay_2026-09-14_screened_scan_silence_trace.md §10): the block
labelled its age "[Nm old]" but served the levels at any age, from either the
in-memory cache or the DB snapshot. Now gated on the tier-0 30-min bar; stale
or unparseable -> "", same as a symbol never computed.

profile.timestamp is naive LOCAL time (compute path writes
datetime.now().isoformat()); timestamps here are built the same way.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

import gex_calculator as gc


def _local_naive_minutes_ago(minutes: float) -> str:
    return (datetime.now() - timedelta(minutes=minutes)).isoformat()


def _profile(timestamp: str) -> gc.GEXProfile:
    return gc.GEXProfile(
        symbol="SPY", spot_price=759.33, timestamp=timestamp, levels=["level"],
        max_gamma_strike=775.0, zero_gamma_level=766.31, put_wall=750.0, call_wall=775.0,
        gamma_flip=766.31, total_gex=-12767156387.08,
    )


def _snapshot_row(timestamp: str) -> dict:
    return {
        "symbol": "SPY", "spot_price": 742.09, "timestamp": timestamp,
        "levels": [{"strike": 748.0, "net_gex": 1.0, "call_gex": 1.0, "put_gex": 0.0}],
        "max_gamma_strike": 740.0, "zero_gamma_level": 752.0, "put_wall": 607.0,
        "call_wall": 748.0, "gamma_flip": 752.0, "total_gex": 846063467.9,
    }


@pytest.fixture
def sources(monkeypatch):
    state = {"snapshot": None}
    monkeypatch.setattr(gc, "_cache", {})
    monkeypatch.setattr(gc, "get_latest_snapshot", lambda sym: state["snapshot"])
    return state


def _cache(profile: gc.GEXProfile) -> None:
    gc._cache[profile.symbol] = {"profile": profile}


def test_stale_cached_profile_is_refused(sources):
    _cache(_profile("2026-07-21T13:05:20"))
    assert gc.build_alpaca_gex_prompt_section("SPY") == ""


def test_stale_db_snapshot_is_refused(sources):
    sources["snapshot"] = _snapshot_row("2026-07-21T13:05:20")
    block = gc.build_alpaca_gex_prompt_section("SPY")
    assert block == ""
    assert "$748" not in block and "$607" not in block


def test_profile_just_past_the_30_minute_bar_is_refused(sources):
    _cache(_profile(_local_naive_minutes_ago(45)))
    assert gc.build_alpaca_gex_prompt_section("SPY") == ""


def test_unparseable_timestamp_is_refused(sources):
    _cache(_profile("not-a-timestamp"))
    assert gc.build_alpaca_gex_prompt_section("SPY") == ""


def test_fresh_profile_is_served_with_its_age(sources):
    _cache(_profile(_local_naive_minutes_ago(10)))
    block = gc.build_alpaca_gex_prompt_section("SPY")
    assert "Call Wall (Resistance): $775" in block and "Put Wall (Support): $750" in block
    assert "[10m old]" in block or "[9m old]" in block
