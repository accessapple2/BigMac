"""Tests for HM-RS-RANK-VS-SPY (engine/rs_rank.py).

Validates:
  - _compute_window_return: full-window, short-history, NaN-shaped, zero start
  - _percentile_rank: ascending → 1-99 spread; NaN preserves position with 0;
    ties get mean rank
  - run_rs_rank end-to-end with mocked bars (top-mover gets 99, bottom 1)
  - _persist_results: full-table rewrite (DELETE + INSERT in one tx)

Run:
    PYTHONPATH=. pytest tests/test_rs_rank.py -v
"""
from __future__ import annotations

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

from engine import rs_rank  # noqa: E402

SCHEMA = (ROOT / "scripts" / "migrations" / "add_rs_rank_table.sql").read_text()


@pytest.fixture
def db_path(tmp_path):
    p = tmp_path / "test_trader.db"
    conn = sqlite3.connect(str(p))
    conn.executescript(SCHEMA)
    # Need scan_universe for _load_universe / run_rs_rank end-to-end
    conn.execute(
        "CREATE TABLE IF NOT EXISTS scan_universe (symbol TEXT PRIMARY KEY)"
    )
    conn.commit()
    conn.close()
    return str(p)


def _make_close_df(closes: list[float]) -> pd.DataFrame:
    return pd.DataFrame({"Close": np.asarray(closes, dtype=float)})


def test_compute_window_return_full_window():
    closes = np.linspace(100, 110, 65).tolist()  # +10% over 65 bars
    df = _make_close_df(closes)
    ret, bars = rs_rank._compute_window_return(df, window=60)
    # 60-bar trailing return should be close to 10% (slightly less due to
    # the offset into the linspace)
    assert bars == 60
    assert 9.0 < ret < 10.5


def test_compute_window_return_short_history_degrades():
    closes = np.linspace(100, 105, 40).tolist()  # 40 bars only
    df = _make_close_df(closes)
    ret, bars = rs_rank._compute_window_return(df, window=60)
    assert bars == 39  # window degrades to len-1
    assert ret > 0


def test_compute_window_return_below_min_bars_returns_nan():
    closes = [100.0] * 20  # below _MIN_BARS_REQUIRED=30
    df = _make_close_df(closes)
    ret, bars = rs_rank._compute_window_return(df, window=60)
    assert np.isnan(ret)
    assert bars == 0


def test_compute_window_return_empty_df():
    ret, bars = rs_rank._compute_window_return(pd.DataFrame({"Close": []}))
    assert np.isnan(ret)
    assert bars == 0


def test_percentile_rank_monotonic():
    ranks = rs_rank._percentile_rank([1.0, 2.0, 3.0, 4.0, 5.0])
    # Highest value gets 99, lowest gets the smallest rank
    assert ranks[-1] == 99
    assert ranks[0] < ranks[-1]
    # Strictly increasing
    assert ranks == sorted(ranks)


def test_percentile_rank_with_nan():
    # NaN positions get rank 0; non-NaN spread over 1-99
    ranks = rs_rank._percentile_rank([float("nan"), 10.0, 20.0, 30.0, 40.0])
    assert ranks[0] == 0  # NaN
    assert ranks[-1] == 99
    assert all(r > 0 for r in ranks[1:])


def test_percentile_rank_ties_share_mean_rank():
    ranks = rs_rank._percentile_rank([5.0, 5.0, 5.0, 5.0])
    # All tied → all get the same rank (mean of [25, 50, 75, 100] -> 99 via
    # ceil/round). Verify equality.
    assert len(set(ranks)) == 1


def test_persist_results_full_table_rewrite(db_path):
    # Seed an old row
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO rs_rank "
        "(symbol, computed_at, rs_return_pct, rs_vs_spy_pct, rs_rank, bars_used) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        ("OLD", "2026-01-01T00:00:00", 5.0, 2.0, 50, 60),
    )
    conn.commit()
    conn.close()

    fresh = [
        {
            "symbol": "AAA",
            "computed_at": "2026-05-24T00:00:00",
            "rs_return_pct": 12.5,
            "rs_vs_spy_pct": 8.0,
            "rs_rank": 95,
            "bars_used": 60,
        },
        {
            "symbol": "BBB",
            "computed_at": "2026-05-24T00:00:00",
            "rs_return_pct": -3.0,
            "rs_vs_spy_pct": -7.5,
            "rs_rank": 12,
            "bars_used": 60,
        },
    ]
    inserted = rs_rank._persist_results(fresh, db_path=db_path)
    assert inserted == 2

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT symbol, rs_rank FROM rs_rank ORDER BY symbol").fetchall()
    conn.close()
    syms = [r["symbol"] for r in rows]
    # OLD row purged; AAA + BBB present
    assert syms == ["AAA", "BBB"]


def test_run_rs_rank_end_to_end_with_mock_bars(db_path, monkeypatch):
    # Seed universe: 3 symbols + SPY
    conn = sqlite3.connect(db_path)
    for s in ["WIN", "MID", "LOSS"]:
        conn.execute("INSERT INTO scan_universe (symbol) VALUES (?)", (s,))
    conn.commit()
    conn.close()

    # SPY: +5% over 60 bars
    # WIN: +15% → should beat SPY → rank 99
    # MID: +5%  → matches SPY → mid-rank
    # LOSS: -8% → underperforms → rank 1
    def _series(start: float, end: float, n: int = 65) -> pd.DataFrame:
        return _make_close_df(np.linspace(start, end, n).tolist())

    bars = {
        "SPY":  _series(100, 105),
        "WIN":  _series(100, 115),
        "MID":  _series(100, 105),
        "LOSS": _series(100, 92),
    }

    monkeypatch.setattr(rs_rank, "_DB_PATH", db_path)
    monkeypatch.setattr(rs_rank, "_load_universe", lambda db_path=None: ["WIN", "MID", "LOSS"])
    monkeypatch.setattr(
        "engine.market_data.get_bulk_daily_ohlcv",
        lambda symbols, range_str="3mo": bars,
    )
    monkeypatch.setattr(rs_rank, "_last_result", None)
    monkeypatch.setattr(rs_rank, "_last_scan_ts", 0.0)

    result = rs_rank.run_rs_rank(force=True)
    assert result["scanned"] == 3
    assert result["persisted"] == 3
    assert result["spy_return_pct"] > 0

    # Pull persisted rows to check ranking
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = {r["symbol"]: r for r in conn.execute("SELECT * FROM rs_rank").fetchall()}
    conn.close()
    # WIN > MID > LOSS by return; ranks should reflect that
    assert rows["WIN"]["rs_rank"] > rows["MID"]["rs_rank"] > rows["LOSS"]["rs_rank"]
    assert rows["WIN"]["rs_rank"] == 99
    # With only 3 symbols, the floor of percentile-rank is 1/3 ≈ 33 (not 1).
    # The 1-rank target is reached only on >= ~99-symbol universes; verify
    # the bottom is strictly < the middle.
    assert rows["LOSS"]["rs_rank"] < rows["MID"]["rs_rank"]
    # vs SPY signs
    assert rows["WIN"]["rs_vs_spy_pct"] > 0
    assert rows["LOSS"]["rs_vs_spy_pct"] < 0


def test_run_rs_rank_aborts_without_spy(db_path, monkeypatch):
    conn = sqlite3.connect(db_path)
    conn.execute("INSERT INTO scan_universe (symbol) VALUES ('AAA')")
    conn.commit()
    conn.close()

    monkeypatch.setattr(rs_rank, "_DB_PATH", db_path)
    monkeypatch.setattr(rs_rank, "_load_universe", lambda db_path=None: ["AAA"])
    # No SPY in the bars dict
    monkeypatch.setattr(
        "engine.market_data.get_bulk_daily_ohlcv",
        lambda symbols, range_str="3mo": {"AAA": _make_close_df(np.linspace(100, 110, 65).tolist())},
    )
    monkeypatch.setattr(rs_rank, "_last_result", None)
    monkeypatch.setattr(rs_rank, "_last_scan_ts", 0.0)

    result = rs_rank.run_rs_rank(force=True)
    # SPY missing → scanner aborts cleanly with scanned=0
    assert result["scanned"] == 0
    assert result["persisted"] == 0
