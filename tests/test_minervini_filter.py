"""Tests for HM-MINERVINI-TREND-FILTER (engine/minervini_filter.py).

Validates:
  - _sma basic correctness
  - _evaluate_conditions: perfect uptrend → 8/8, flat → near-zero,
    short history → bars_used=0
  - Each condition individually rejects when its premise breaks
  - _persist_results full-table rewrite
  - run_minervini_scan end-to-end with mocked bars + rs_rank LEFT JOIN
  - rs_pass populated from seeded rs_rank row

Run:
    PYTHONPATH=. pytest tests/test_minervini_filter.py -v
"""
from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine import minervini_filter as mv  # noqa: E402

MINERVINI_SCHEMA = (
    ROOT / "scripts" / "migrations" / "add_minervini_trend_table.sql"
).read_text()
RS_SCHEMA = (ROOT / "scripts" / "migrations" / "add_rs_rank_table.sql").read_text()


@pytest.fixture
def db_path(tmp_path):
    p = tmp_path / "test_trader.db"
    conn = sqlite3.connect(str(p))
    conn.executescript(MINERVINI_SCHEMA)
    conn.executescript(RS_SCHEMA)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS scan_universe (symbol TEXT PRIMARY KEY)"
    )
    conn.commit()
    conn.close()
    return str(p)


def _make_df(closes: np.ndarray, hl_pad: float = 0.5) -> pd.DataFrame:
    closes = np.asarray(closes, dtype=float)
    return pd.DataFrame(
        {
            "Open": closes,
            "High": closes + hl_pad,
            "Low": closes - hl_pad,
            "Close": closes,
            "Volume": np.full(len(closes), 1_000_000.0),
        }
    )


def test_sma_basic():
    arr = np.arange(1, 11, dtype=float)
    out = mv._sma(arr, 3)
    # First 2 NaN, then [2, 3, 4, 5, 6, 7, 8, 9]
    assert np.isnan(out[0])
    assert np.isnan(out[1])
    assert out[-1] == pytest.approx(9.0)


def test_evaluate_perfect_uptrend_passes_all_8():
    closes = np.linspace(100, 150, 260)  # +50% over 260 bars
    df = _make_df(closes)
    r = mv._evaluate_conditions(df)
    assert r["template_score"] == 8
    assert r["template_pass"] is True
    assert r["bars_used"] == 260
    for i in range(1, 9):
        assert r[f"cond{i}"] is True, f"cond{i} failed"


def test_evaluate_flat_series_fails_most():
    closes = np.full(260, 100.0)
    df = _make_df(closes)
    r = mv._evaluate_conditions(df)
    # Flat → no rising SMA200, price not > smaller-window SMAs, etc.
    assert r["template_score"] <= 3
    assert r["template_pass"] is False


def test_evaluate_short_history_returns_empty():
    df = _make_df(np.linspace(100, 110, 100))
    r = mv._evaluate_conditions(df)
    assert r["template_score"] == 0
    assert r["template_pass"] is False
    assert r["bars_used"] == 0


def test_cond7_rejects_below_25pct_from_high():
    # 252 bars: ramp up to 200 (52w high), then drop to 130 (35% below high)
    ramp = np.linspace(100, 200, 200)
    drop = np.linspace(200, 130, 60)
    closes = np.concatenate([ramp, drop])
    df = _make_df(closes)
    r = mv._evaluate_conditions(df)
    assert r["cond7"] is False  # 130 vs 200 high → 35% below, > 25%
    assert r["template_pass"] is False


def test_cond8_rejects_near_52w_low():
    # 252 bars: drop to 50 (52w low), then small bounce to 60 (20% above low)
    drop = np.linspace(100, 50, 200)
    bounce = np.linspace(50, 60, 60)
    closes = np.concatenate([drop, bounce])
    df = _make_df(closes)
    r = mv._evaluate_conditions(df)
    assert r["cond8"] is False  # 60 vs 50 low → 20% above, < 30%
    assert r["template_pass"] is False


def test_cond4_rejects_flat_sma200():
    # 260 perfectly flat bars → sma200[today] == sma200[22d ago] (cond4
    # is strict ">", not ">=", so flat fails by design).
    closes = np.full(260, 100.0)
    df = _make_df(closes)
    r = mv._evaluate_conditions(df)
    assert r["cond4"] is False
    assert r["template_pass"] is False


def test_persist_full_rewrite(db_path):
    # Seed an old row
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO minervini_trend "
        "(symbol, computed_at, template_score, template_pass, rs_pass, "
        " conds_json, price_at_scan, high_52w, low_52w, bars_used) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("OLD", "2026-01-01T00:00:00", 5, 0, 0, "{}", 50.0, 60.0, 40.0, 260),
    )
    conn.commit()
    conn.close()

    fresh = [
        {
            "symbol": "NEW",
            "computed_at": "2026-05-24T00:00:00",
            "template_score": 8,
            "template_pass": True,
            "rs_pass": True,
            "conds_json": "{}",
            "price_at_scan": 150.0,
            "high_52w": 160.0,
            "low_52w": 80.0,
            "bars_used": 260,
        }
    ]
    n = mv._persist_results(fresh, db_path=db_path)
    assert n == 1
    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT symbol FROM minervini_trend").fetchall()
    conn.close()
    assert [r[0] for r in rows] == ["NEW"]


def test_persist_skips_below_min_bars(db_path):
    fresh = [
        {
            "symbol": "SHORT",
            "computed_at": "2026-05-24T00:00:00",
            "template_score": 8,
            "template_pass": True,
            "rs_pass": True,
            "conds_json": "{}",
            "price_at_scan": 100.0,
            "high_52w": 110.0,
            "low_52w": 80.0,
            "bars_used": 100,  # < _MIN_BARS_REQUIRED=252
        }
    ]
    n = mv._persist_results(fresh, db_path=db_path)
    assert n == 0


def test_run_minervini_scan_end_to_end(db_path, monkeypatch):
    # Seed scan_universe + rs_rank for the rs_pass LEFT JOIN
    conn = sqlite3.connect(db_path)
    for s in ["UP", "FLAT"]:
        conn.execute("INSERT INTO scan_universe (symbol) VALUES (?)", (s,))
    conn.execute(
        "INSERT INTO rs_rank "
        "(symbol, computed_at, rs_return_pct, rs_vs_spy_pct, rs_rank, bars_used) "
        "VALUES ('UP', '2026-05-24', 25.0, 17.0, 85, 60)"
    )
    conn.commit()
    conn.close()

    up_closes = np.linspace(100, 150, 260)
    flat_closes = np.full(260, 100.0)
    bars = {"UP": _make_df(up_closes), "FLAT": _make_df(flat_closes)}

    monkeypatch.setattr(mv, "_DB_PATH", db_path)
    monkeypatch.setattr(mv, "_load_universe", lambda db_path=None: ["UP", "FLAT"])
    monkeypatch.setattr(
        "engine.market_data.get_bulk_daily_ohlcv",
        lambda symbols, range_str="3mo": bars,
    )
    monkeypatch.setattr(mv, "_last_result", None)
    monkeypatch.setattr(mv, "_last_scan_ts", 0.0)

    result = mv.run_minervini_scan(force=True)
    assert result["scanned"] == 2
    assert result["passing"] == 1  # UP passes, FLAT fails
    assert "UP" in result["top_pass_symbols"]

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = {r["symbol"]: r for r in conn.execute(
        "SELECT * FROM minervini_trend").fetchall()}
    conn.close()
    assert rows["UP"]["template_pass"] == 1
    assert rows["UP"]["template_score"] == 8
    assert rows["UP"]["rs_pass"] == 1  # rs_rank=85 ≥ 70
    assert rows["FLAT"]["template_pass"] == 0
    assert rows["FLAT"]["rs_pass"] == 0  # no rs_rank seeded for FLAT
    # conds_json round-trips
    conds = json.loads(rows["UP"]["conds_json"])
    assert all(conds[f"cond{i}"] is True for i in range(1, 9))
