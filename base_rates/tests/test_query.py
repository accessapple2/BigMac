"""End-to-end test: build a small DB, run base_rate(), check aggregates."""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pytest

from base_rates.migrate import migrate
from base_rates.query import base_rate


def _seed_rows(db_path: str):
    """Seed with: today + 5 historical analogs (same bucket vector)
    and 3 non-analogs (different bucket vector)."""
    today_buckets = (5, 2, 1, 1, 2, 1)  # b_move, b_rsi, b_rsi_slope, b_vix, b_vix_move, b_trend
    other_buckets = (0, 0, 0, 0, 0, 0)

    rows = []
    # today (no fwd return — it's the as-of)
    rows.append(("AAPL", "2026-04-30", 200.0, 0.04, 65.0, 1.5, 18.0, 0.02, 1, None, None,
                 *today_buckets))
    # 5 analogs with various forward outcomes
    rows.append(("AAPL", "2024-01-15", 180.0, 0.04, 65.0, 1.5, 18.0, 0.02, 1,  0.03, -0.02, *today_buckets))
    rows.append(("AAPL", "2023-06-10", 170.0, 0.04, 65.0, 1.5, 18.0, 0.02, 1,  0.05, -0.01, *today_buckets))
    rows.append(("AAPL", "2022-11-01", 150.0, 0.04, 65.0, 1.5, 18.0, 0.02, 1, -0.02, -0.04, *today_buckets))
    rows.append(("AAPL", "2021-08-20", 140.0, 0.04, 65.0, 1.5, 18.0, 0.02, 1,  0.01, -0.03, *today_buckets))
    rows.append(("AAPL", "2020-03-15", 130.0, 0.04, 65.0, 1.5, 18.0, 0.02, 1,  0.04, -0.02, *today_buckets))
    # 3 non-analogs (should NOT be included)
    rows.append(("AAPL", "2019-05-01", 100.0, -0.05, 25.0, -2.0, 30.0, 0.10, 0, -0.06, -0.08, *other_buckets))
    rows.append(("AAPL", "2018-12-12", 90.0, -0.05, 25.0, -2.0, 30.0, 0.10, 0, -0.04, -0.06, *other_buckets))
    rows.append(("AAPL", "2018-04-04", 95.0, -0.05, 25.0, -2.0, 30.0, 0.10, 0, -0.02, -0.05, *other_buckets))

    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            """INSERT INTO base_rate_features
               (symbol, date, close, pct_change, rsi14, rsi_slope,
                vix_close, vix_pct_change, spy_above_200,
                fwd_5d_return, fwd_5d_maxdd,
                b_move, b_rsi, b_rsi_slope, b_vix, b_vix_move, b_trend)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            rows,
        )
        conn.commit()


def test_base_rate_returns_correct_aggregates():
    with tempfile.TemporaryDirectory() as td:
        db = str(Path(td) / "test.db")
        migrate(db)
        _seed_rows(db)

        r = base_rate("AAPL", db_path=db, as_of="2026-04-30", min_n=3)

        # 5 historical analogs were seeded (the 3 non-analogs should be filtered)
        assert r.n_matches == 5
        # Win rate: 4 of 5 are positive
        assert abs(r.win_rate - 0.8) < 1e-9
        # Median of [0.03, 0.05, -0.02, 0.01, 0.04] = 0.03
        assert abs(r.median_5d - 0.03) < 1e-9


def test_base_rate_warns_on_low_n():
    with tempfile.TemporaryDirectory() as td:
        db = str(Path(td) / "test.db")
        migrate(db)
        _seed_rows(db)

        r = base_rate("AAPL", db_path=db, as_of="2026-04-30", min_n=30)
        # 5 < 30, should warn
        assert any("min_n" in w for w in r.warnings)


def test_base_rate_zero_matches_when_unprecedented():
    with tempfile.TemporaryDirectory() as td:
        db = str(Path(td) / "test.db")
        migrate(db)
        # Only seed today, no history
        with sqlite3.connect(db) as conn:
            conn.execute(
                """INSERT INTO base_rate_features
                   (symbol, date, close, pct_change, rsi14, rsi_slope,
                    vix_close, vix_pct_change, spy_above_200,
                    fwd_5d_return, fwd_5d_maxdd,
                    b_move, b_rsi, b_rsi_slope, b_vix, b_vix_move, b_trend)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                ("AAPL", "2026-04-30", 200.0, 0.04, 65.0, 1.5, 18.0, 0.02, 1,
                 None, None, 5, 2, 1, 1, 2, 1),
            )
            conn.commit()

        r = base_rate("AAPL", db_path=db, as_of="2026-04-30")
        assert r.n_matches == 0
        assert any("unprecedented" in w for w in r.warnings)


def test_base_rate_missing_symbol():
    with tempfile.TemporaryDirectory() as td:
        db = str(Path(td) / "test.db")
        migrate(db)
        r = base_rate("NOPE", db_path=db, as_of="2026-04-30")
        assert r.n_matches == 0
        assert any("no row found" in w for w in r.warnings)
