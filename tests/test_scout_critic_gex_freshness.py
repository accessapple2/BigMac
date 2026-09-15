"""HM-GEX-CONSUMER-BATCH 2026-09-14 (item 4 of the relay §10 batch) --
scout_critic._gather_scout_brief must not hand the Critic a stale gex_levels row
as current.

Live finding (relay_2026-09-14_screened_scan_silence_trace.md §10): the Scout
brief read the newest gex_levels row with no age check and wrote its flip and
walls into the Critic's prompt. Every row in trader.db is 2026-05-23..05-30.
Stale or missing now yields an explicit UNAVAILABLE line, same wording as the
fleet prompt block.

yfinance is stubbed so the earnings section makes no network call.
"""
from __future__ import annotations

import sqlite3
import sys
import types
from datetime import datetime, timedelta

import pytest

import engine.scout_critic as sc

STALE_CALC_TIME = "2026-05-30T13:06:03.475811"


def _local_naive_minutes_ago(minutes: float) -> str:
    return (datetime.now() - timedelta(minutes=minutes)).isoformat()


@pytest.fixture
def scout(tmp_path, monkeypatch):
    fake_yf = types.ModuleType("yfinance")

    class _NoEarnings:
        earnings_dates = None

        def __init__(self, symbol):
            pass

    fake_yf.Ticker = _NoEarnings
    monkeypatch.setitem(sys.modules, "yfinance", fake_yf)

    db = tmp_path / "scout_critic_test.db"
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE gex_levels (id INTEGER PRIMARY KEY, symbol TEXT, calc_time TIMESTAMP, "
        "composite_score REAL, composite_signal TEXT, gamma_flip REAL, call_wall REAL, put_wall REAL)"
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(sc, "TRADER_DB", str(db))

    def _insert(calc_time: str, symbol: str = "MSI") -> None:
        c = sqlite3.connect(db)
        c.execute(
            "INSERT INTO gex_levels (symbol, calc_time, composite_score, composite_signal, "
            "gamma_flip, call_wall, put_wall) VALUES (?,?,?,?,?,?,?)",
            (symbol, calc_time, 66.5, "BULLISH", 452.5, 470.0, 440.0),
        )
        c.commit()
        c.close()

    return _insert


def _gex_line(brief: str) -> str:
    return next(line for line in brief.splitlines() if line.startswith("GEX"))


def test_stale_row_never_reaches_the_critic(scout):
    scout(STALE_CALC_TIME)
    brief = sc._gather_scout_brief("MSI", {})
    for stale_level in ("452.5", "470.0", "440.0", "BULLISH"):
        assert stale_level not in brief
    assert "GEX: UNAVAILABLE" in _gex_line(brief)


def test_missing_row_is_stated_not_silently_omitted(scout):
    brief = sc._gather_scout_brief("MSI", {})
    assert "GEX: UNAVAILABLE" in _gex_line(brief)


def test_row_hours_old_is_stale_even_though_naive_as_utc_would_call_it_fresh(scout):
    scout(_local_naive_minutes_ago(180))
    assert "GEX: UNAVAILABLE" in _gex_line(sc._gather_scout_brief("MSI", {}))


def test_fresh_row_is_shown_with_its_as_of_time(scout):
    calc_time = _local_naive_minutes_ago(5)
    scout(calc_time)
    line = _gex_line(sc._gather_scout_brief("MSI", {}))
    assert "gamma_flip=452.5" in line and "call_wall=470.0" in line and "put_wall=440.0" in line
    assert f"as of {calc_time}" in line
