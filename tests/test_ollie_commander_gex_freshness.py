"""HM-GEX-CONSUMER-BATCH 2026-09-14 (item 2 of the relay §10 batch) --
ollie_commander._get_gex_pts must not score a trade against a stale gex_levels row.

Live finding (relay_2026-09-14_screened_scan_silence_trace.md §10): _get_gex_pts
read the newest gex_levels row with no age check. Every row in trader.db is
from 2026-05-30 13:06, so every approve_or_reject() since scored 0-0.4 crew
points off May's composite. canonical_gex has no composite score, so the row
is gated on the tier-0 30-min bar instead.

calc_time is naive LOCAL time (gex_overlay._save_gex_levels writes
datetime.now().isoformat()); timestamps here are built the same way so the
tests hold in any host time zone.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta

import pytest

import engine.canonical_gex as cg
import engine.ollie_commander as oc


def _local_naive_minutes_ago(minutes: float) -> str:
    return (datetime.now() - timedelta(minutes=minutes)).isoformat()


@pytest.fixture
def gex_db(tmp_path, monkeypatch):
    db = tmp_path / "ollie_commander_test.db"
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE gex_levels (id INTEGER PRIMARY KEY, symbol TEXT, calc_time TIMESTAMP, "
        "composite_score REAL, composite_signal TEXT)"
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(oc, "TRADER_DB", str(db))

    def _insert(calc_time: str, score: float, signal: str, symbol: str = "SPY") -> None:
        c = sqlite3.connect(db)
        c.execute(
            "INSERT INTO gex_levels (symbol, calc_time, composite_score, composite_signal) VALUES (?,?,?,?)",
            (symbol, calc_time, score, signal),
        )
        c.commit()
        c.close()

    return _insert


def test_stale_bullish_row_scores_neutral(gex_db):
    gex_db("2026-05-30T13:06:05.316829", 64.0, "BULLISH")
    assert oc._get_gex_pts("SPY", "BUY") == 0.2


def test_stale_bearish_row_does_not_zero_the_trade(gex_db):
    gex_db("2026-05-30T13:06:05.316829", 64.0, "BEARISH")
    assert oc._get_gex_pts("SPY", "BUY") == 0.2


def test_row_hours_old_is_stale_even_though_naive_as_utc_would_call_it_fresh(gex_db):
    gex_db(_local_naive_minutes_ago(180), 64.0, "BULLISH")
    assert oc._get_gex_pts("SPY", "BUY") == 0.2


def test_fresh_bullish_row_still_scores(gex_db):
    gex_db(_local_naive_minutes_ago(5), 64.0, "BULLISH")
    assert oc._get_gex_pts("SPY", "BUY") == 0.4


def test_fresh_bearish_row_still_opposes(gex_db):
    gex_db(_local_naive_minutes_ago(5), 64.0, "BEARISH")
    assert oc._get_gex_pts("SPY", "BUY") == 0.0


# ── canonical_gex.gex_levels_row_is_fresh ────────────────────────────────────

@pytest.mark.parametrize("calc_time, fresh", [
    (None, False),
    ("", False),
    ("not-a-timestamp", False),
    ("2026-05-30T13:06:05.316829", False),
    ("2026-05-30 13:06:05", False),
])
def test_gex_levels_row_is_fresh_rejects_missing_garbage_and_old(calc_time, fresh):
    assert cg.gex_levels_row_is_fresh(calc_time) is fresh


def test_gex_levels_row_is_fresh_reads_naive_as_local():
    assert cg.gex_levels_row_is_fresh(_local_naive_minutes_ago(10)) is True
    assert cg.gex_levels_row_is_fresh(_local_naive_minutes_ago(45)) is False
