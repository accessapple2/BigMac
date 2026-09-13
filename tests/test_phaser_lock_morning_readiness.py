"""HM-PHASER-LOCK-SCAN-RACE-2026-09-13 — Trade of the Day must wait for today's
strategy_signals batch instead of firing at a fixed 06:15 that the batch can land after."""
from __future__ import annotations

import json
import sqlite3

import pytest

import engine.phaser_lock as pl

T = "2026-09-11"


def _m(h: int, mi: int) -> int:
    return h * 60 + mi


# ── decision ────────────────────────────────────────────────────────────────

def test_replay_2026_09_11_race():
    # fired 06:18 with no rows yet (the bug) -> must wait; batch landed 06:26 -> fire
    assert pl.morning_fire_decision(_m(6, 18), 0, False) is None
    assert pl.morning_fire_decision(_m(6, 28), 154, False) == "ready"


@pytest.mark.parametrize("now, rows, produced, expected", [
    (_m(6, 11), 500, False, None),        # before window
    (_m(6, 12), 500, False, "ready"),     # window opens
    (_m(6, 39), 0, False, None),          # still waiting
    (_m(6, 40), 0, False, "fallback"),    # scan never landed -> fail-closed report
    (_m(6, 44), 0, False, "fallback"),
    (_m(6, 45), 500, False, None),        # window closed
    (_m(6, 20), 500, True, None),         # already produced today
])
def test_decision_table(now, rows, produced, expected):
    assert pl.morning_fire_decision(now, rows, produced) == expected


# ── inputs ──────────────────────────────────────────────────────────────────

def test_today_setup_row_count(tmp_path, monkeypatch):
    db = tmp_path / "trader.db"
    c = sqlite3.connect(db)
    c.execute("""CREATE TABLE strategy_signals (scan_date TEXT, ticker TEXT,
                 entry_price REAL, stop_price REAL, target_price REAL)""")
    c.executemany("INSERT INTO strategy_signals VALUES (?,?,?,?,?)", [
        (T, "AAA", 10, 9, 12),
        (T, "BBB", 20, 18, 24),
        (T, "CCC", 0, 9, 12),            # no usable triple
        ("2026-09-10", "DDD", 10, 9, 12),
    ])
    c.commit()
    c.close()
    monkeypatch.setattr(pl, "TRADER_DB", str(db))
    assert pl.today_setup_row_count(T) == 2
    assert pl.today_setup_row_count("2026-09-12") == 0


def test_today_setup_row_count_missing_table_is_zero(tmp_path, monkeypatch):
    monkeypatch.setattr(pl, "TRADER_DB", str(tmp_path / "empty.db"))
    assert pl.today_setup_row_count(T) == 0


@pytest.mark.parametrize("payload, expected", [
    ({"date": T, "scanned": 68}, True),
    ({"date": T, "scanned": 0}, False),          # the stale 9/11 file shape
    ({"date": "2026-09-10", "scanned": 68}, False),
    (None, False),                               # no file
])
def test_produced_pick_today(tmp_path, monkeypatch, payload, expected):
    path = tmp_path / "pick.json"
    if payload is not None:
        path.write_text(json.dumps(payload))
    monkeypatch.setattr(pl, "PICK_JSON", str(path))
    assert pl.produced_pick_today(T) is expected
