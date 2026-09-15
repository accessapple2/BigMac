"""HM-GEX-CONSUMER-BATCH 2026-09-14 (item 3 of the relay §10 batch) --
super_trader._gex_multiplier must not scale confidence off a stale gex_levels row.

Live finding (relay_2026-09-14_screened_scan_silence_trace.md §10):
_gex_multiplier read the newest gex_levels row with no age check. Every row in
trader.db is from 2026-05-23..05-30, so a symbol whose newest row said BULLISH
got a 1.10x BUY confidence multiplier off May's composite. canonical_gex has no
composite score, so the row is gated on the tier-0 30-min bar instead.

calc_time is naive LOCAL time (gex_overlay._save_gex_levels writes
datetime.now().isoformat()); timestamps here are built the same way.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta

import pytest

import engine.super_trader as st


def _local_naive_minutes_ago(minutes: float) -> str:
    return (datetime.now() - timedelta(minutes=minutes)).isoformat()


@pytest.fixture
def gex_db(tmp_path, monkeypatch):
    db = tmp_path / "super_trader_test.db"
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE gex_levels (id INTEGER PRIMARY KEY, symbol TEXT, calc_time TIMESTAMP, "
        "composite_score REAL, composite_signal TEXT)"
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(st, "TRADER_DB", str(db))

    def _insert(calc_time: str, score: float, signal: str, symbol: str = "SPY") -> None:
        c = sqlite3.connect(db)
        c.execute(
            "INSERT INTO gex_levels (symbol, calc_time, composite_score, composite_signal) VALUES (?,?,?,?)",
            (symbol, calc_time, score, signal),
        )
        c.commit()
        c.close()

    return _insert


def test_stale_bullish_row_leaves_confidence_unscaled(gex_db):
    gex_db("2026-05-30T13:06:05.316829", 64.0, "BULLISH")
    assert st._gex_multiplier("SPY", "BUY") == 1.0


def test_stale_bearish_row_leaves_confidence_unscaled(gex_db):
    gex_db("2026-05-30T13:06:05.316829", 64.0, "BEARISH")
    assert st._gex_multiplier("SPY", "BUY") == 1.0


def test_row_hours_old_is_stale_even_though_naive_as_utc_would_call_it_fresh(gex_db):
    gex_db(_local_naive_minutes_ago(180), 64.0, "BULLISH")
    assert st._gex_multiplier("SPY", "BUY") == 1.0


def test_fresh_bullish_row_still_boosts(gex_db):
    gex_db(_local_naive_minutes_ago(5), 64.0, "BULLISH")
    assert st._gex_multiplier("SPY", "BUY") == 1.10


def test_fresh_bearish_row_still_dampens(gex_db):
    gex_db(_local_naive_minutes_ago(5), 64.0, "BEARISH")
    assert st._gex_multiplier("SPY", "BUY") == 0.85


def test_non_buy_is_always_neutral(gex_db):
    gex_db(_local_naive_minutes_ago(5), 64.0, "BULLISH")
    assert st._gex_multiplier("SPY", "SELL") == 1.0
