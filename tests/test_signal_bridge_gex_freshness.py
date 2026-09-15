"""HM-GEX-CONSUMER-BATCH 2026-09-14 (item 9 of the relay §10 batch) --
signal_bridge._w3_context must not stamp stale GEX levels onto shadow signals.

Live finding (relay_2026-09-14_screened_scan_silence_trace.md §10): _w3_context
read flow_gex.db's newest gex_snapshots row at any age (frozen 2026-07-21) into
every shadow signal's W3 context. GEX levels now come from canonical_gex tier 0
under its 30-min bar; the flow lean read from flow_aggregates is unchanged.

flow_gex.db is a temp file holding the stale row, and tier 0 is stubbed at its
source, so the same tests run against the old and new code.
"""
from __future__ import annotations

import sqlite3

import pytest

import engine.canonical_gex as cg
import engine.signal_bridge as sb


@pytest.fixture
def w3(tmp_path, monkeypatch):
    db = tmp_path / "flow_gex_test.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE gex_snapshots (underlying TEXT, asof TEXT, gamma_flip REAL, "
                 "call_wall REAL, put_wall REAL, regime TEXT)")
    conn.execute("CREATE TABLE flow_aggregates (underlying TEXT, asof TEXT, lean TEXT)")
    conn.execute("INSERT INTO gex_snapshots VALUES ('SPY', '2026-07-21 20:05:20', 752.0, 748.0, 607.0, "
                 "'SHORT GAMMA · volatile (spot below flip)')")
    conn.execute("INSERT INTO flow_aggregates VALUES ('SPY', '2026-07-21 20:05:20', 'bullish')")
    conn.commit()
    conn.close()
    monkeypatch.setattr(sb, "FLOW_GEX_DB", str(db))

    state = {"tier0": {}}
    monkeypatch.setattr(cg, "_alpaca_snapshot_fresh", lambda sym: state["tier0"].get(sym))
    return state


def test_stale_flow_gex_levels_never_reach_the_shadow_signal(w3):
    ctx = sb._w3_context("SPY")
    assert ctx["gamma_flip"] is None and ctx["call_wall"] is None and ctx["put_wall"] is None
    assert ctx["regime"] is None


def test_fresh_tier0_levels_are_used(w3):
    w3["tier0"]["SPY"] = {
        "underlying": "SPY", "spot": 759.33, "total_gex": -12767156387.08, "gamma_flip": 766.31,
        "call_wall": 775.0, "put_wall": 750.0, "king_node": 775.0,
        "regime": "SHORT GAMMA · volatile (spot below flip)", "_src": "alpaca",
    }
    ctx = sb._w3_context("SPY")
    assert (ctx["gamma_flip"], ctx["call_wall"], ctx["put_wall"]) == (766.31, 775.0, 750.0)


def test_flow_lean_is_still_read(w3):
    assert sb._w3_context("SPY")["lean"] == "bullish"
