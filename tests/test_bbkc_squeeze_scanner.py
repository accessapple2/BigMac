"""Tests for HM-SQUEEZE-BBKC-COMPRESSION (engine/bbkc_squeeze_scanner.py).

Validates:
  - _compute_bands shape + NaN-pad
  - _detect_squeeze_run: no squeeze, tight squeeze, BB-poke resets count
  - Tier mapping (5d→WATCH, 10d→ALERT, 20d→PRIORITY); composite 5pts/day
  - Insufficient bars → skipped, no exception
  - _persist_results writes kind='bbkc'; backwards-compat with existing
    short_interest rows
  - Dedupe within _DEDUPE_HOURS: same-symbol re-insert only on tier upgrade
  - run_scan with mocked get_bulk_daily_ohlcv (1 in squeeze, 2 not, 1 short)

Run:
    .venv/bin/python -m pytest tests/test_bbkc_squeeze_scanner.py -v
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine import bbkc_squeeze_scanner as bbkc  # noqa: E402

BASE_SCHEMA = (ROOT / "scripts" / "migrations" / "add_squeeze_watch_table.sql").read_text()
BBKC_SCHEMA = (ROOT / "scripts" / "migrations" / "add_squeeze_watch_bbkc.sql").read_text()


@pytest.fixture
def db_path(tmp_path):
    p = tmp_path / "test_trader.db"
    conn = sqlite3.connect(str(p))
    conn.executescript(BASE_SCHEMA)
    conn.executescript(BBKC_SCHEMA)
    conn.commit()
    conn.close()
    return str(p)


def _make_df(closes, hi_pad=0.5, lo_pad=0.5):
    closes = np.asarray(closes, dtype=float)
    return pd.DataFrame(
        {
            "Open": closes,
            "High": closes + hi_pad,
            "Low": closes - lo_pad,
            "Close": closes,
            "Volume": np.full(len(closes), 1_000_000.0),
        }
    )


def test_compute_bands_shape_and_nan_pad():
    closes = 100 + np.cumsum(np.random.RandomState(0).randn(30) * 0.5)
    df = _make_df(closes)
    bands = bbkc._compute_bands(df)
    assert list(bands.columns) == ["bb_upper", "bb_lower", "kc_upper", "kc_lower", "bb_mid"]
    assert len(bands) == 30
    # First 19 rows should be NaN (period=20)
    assert bands["bb_upper"].iloc[:19].isna().all()
    assert bands["bb_upper"].iloc[19:].notna().all()


def test_detect_no_squeeze_on_wide_swings():
    # Big swings → BB wider than KC most of the time
    rng = np.random.RandomState(1)
    closes = 100 + np.cumsum(rng.randn(40) * 5.0)
    df = _make_df(closes, hi_pad=1.0, lo_pad=1.0)
    in_sq, duration, _, _, _ = bbkc._detect_squeeze_run(df)
    assert not in_sq
    assert duration == 0


def test_detect_squeeze_on_tight_series():
    # Very low volatility → BB inside KC
    rng = np.random.RandomState(2)
    closes = 100 + np.cumsum(rng.randn(40) * 0.05)
    df = _make_df(closes, hi_pad=0.1, lo_pad=0.1)
    in_sq, duration, bb_w, kc_w, _ = bbkc._detect_squeeze_run(df)
    assert in_sq
    assert duration >= bbkc._MIN_PERSIST_DAYS
    assert kc_w > bb_w > 0.0


def test_detect_squeeze_run_resets_on_bb_poke():
    # 30 tight bars, then one giant pop, then 5 tight bars → run = 5
    rng = np.random.RandomState(3)
    tight_before = 100 + np.cumsum(rng.randn(30) * 0.05)
    pop = np.array([tight_before[-1] + 10.0])
    tight_after = pop[-1] + np.cumsum(rng.randn(5) * 0.05)
    closes = np.concatenate([tight_before, pop, tight_after])
    df = _make_df(closes, hi_pad=0.1, lo_pad=0.1)
    in_sq, duration, _, _, _ = bbkc._detect_squeeze_run(df)
    # The pop bar widens BB way outside KC → squeeze run resets after it
    assert duration <= 6  # tolerant: small numerical wobble in the post-pop window


def test_insufficient_bars_returns_zero():
    df = _make_df([100.0] * 10)
    in_sq, duration, _, _, _ = bbkc._detect_squeeze_run(df)
    assert not in_sq
    assert duration == 0


def test_tier_mapping():
    assert bbkc._tier_for_duration(4) == "WATCH"  # below persist; mapping still works
    assert bbkc._tier_for_duration(5) == "WATCH"
    assert bbkc._tier_for_duration(9) == "WATCH"
    assert bbkc._tier_for_duration(10) == "ALERT"
    assert bbkc._tier_for_duration(19) == "ALERT"
    assert bbkc._tier_for_duration(20) == "PRIORITY"
    assert bbkc._tier_for_duration(50) == "PRIORITY"


def test_composite_for_duration():
    assert bbkc._composite_for_duration(5) == 25.0
    assert bbkc._composite_for_duration(10) == 50.0
    assert bbkc._composite_for_duration(20) == 100.0
    assert bbkc._composite_for_duration(30) == 100.0  # saturated


def test_persist_writes_bbkc_kind(db_path):
    results = [
        {
            "symbol": "AAA",
            "duration_days": 12,
            "bb_width_pct": 2.1,
            "kc_width_pct": 4.5,
            "tightness": 0.53,
            "last_close": 50.0,
        }
    ]
    summary = bbkc._persist_results(results, db_path=db_path)
    assert summary["inserted"] == 1
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT symbol, kind, bbkc_duration_days, threshold_tier, composite_score "
        "FROM squeeze_watch"
    ).fetchall()
    conn.close()
    assert len(rows) == 1
    assert rows[0]["symbol"] == "AAA"
    assert rows[0]["kind"] == "bbkc"
    assert rows[0]["bbkc_duration_days"] == 12
    assert rows[0]["threshold_tier"] == "ALERT"
    assert rows[0]["composite_score"] == 60.0


def test_persist_skips_below_min_duration(db_path):
    results = [{"symbol": "TOOSHORT", "duration_days": 3, "bb_width_pct": 1.0,
                "kc_width_pct": 2.0, "tightness": 0.5, "last_close": 10.0}]
    summary = bbkc._persist_results(results, db_path=db_path)
    assert summary["inserted"] == 0


def test_dedupe_same_tier_skipped_higher_tier_upgrades(db_path):
    base = {"bb_width_pct": 1.0, "kc_width_pct": 2.0, "tightness": 0.5,
            "last_close": 50.0}
    bbkc._persist_results(
        [{"symbol": "DDD", "duration_days": 7, **base}], db_path=db_path
    )
    # Same tier (WATCH) within dedupe window → skipped
    s = bbkc._persist_results(
        [{"symbol": "DDD", "duration_days": 8, **base}], db_path=db_path
    )
    assert s["skipped_dedup"] == 1
    # Higher tier (ALERT) → inserted
    s = bbkc._persist_results(
        [{"symbol": "DDD", "duration_days": 12, **base}], db_path=db_path
    )
    assert s["inserted"] == 1
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT threshold_tier FROM squeeze_watch WHERE symbol='DDD' "
        "ORDER BY scan_ts, id"  # HM-SQUEEZE-TIEBREAK: scan_ts is second-precision, ties are real
    ).fetchall()
    conn.close()
    assert [r[0] for r in rows] == ["WATCH", "ALERT"]


def test_short_interest_rows_unaffected_by_bbkc_persist(db_path):
    # Seed a short_interest row directly
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO squeeze_watch "
        "(symbol, scan_ts, composite_score, threshold_tier, price_at_scan, "
        " kind) VALUES (?, ?, ?, ?, ?, ?)",
        ("SHRT", datetime.now(timezone.utc).isoformat(), 80.0,
         "ALERT", 25.0, "short_interest"),
    )
    conn.commit()
    conn.close()

    # Persist a bbkc row for the SAME symbol — should NOT dedupe against
    # the short_interest row, because dedupe is scoped to kind='bbkc'.
    summary = bbkc._persist_results(
        [{"symbol": "SHRT", "duration_days": 11, "bb_width_pct": 1.0,
          "kc_width_pct": 2.0, "tightness": 0.5, "last_close": 25.0}],
        db_path=db_path,
    )
    assert summary["inserted"] == 1

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT kind, threshold_tier FROM squeeze_watch "
        "WHERE symbol='SHRT' ORDER BY kind"
    ).fetchall()
    conn.close()
    kinds = sorted([r["kind"] for r in rows])
    assert kinds == ["bbkc", "short_interest"]


def test_run_scan_end_to_end_with_mock_bars(db_path, monkeypatch):
    # Two symbols: one in tight squeeze, one not.
    rng = np.random.RandomState(7)
    tight_closes = 100 + np.cumsum(rng.randn(40) * 0.05)
    wide_closes = 50 + np.cumsum(rng.randn(40) * 4.0)
    bars = {
        "TGHT": _make_df(tight_closes, hi_pad=0.1, lo_pad=0.1),
        "WIDE": _make_df(wide_closes, hi_pad=1.0, lo_pad=1.0),
    }

    monkeypatch.setattr(bbkc, "_DB_PATH", db_path)
    monkeypatch.setattr(
        bbkc,
        "_load_universe",
        lambda db_path=None: ["TGHT", "WIDE"],
    )
    monkeypatch.setattr(
        "engine.market_data.get_bulk_daily_ohlcv",
        lambda symbols, range_str="3mo": bars,
    )
    # Reset module-level caches so each test is independent
    monkeypatch.setattr(bbkc, "_last_result", None)
    monkeypatch.setattr(bbkc, "_last_scan_ts", 0.0)
    monkeypatch.setattr(bbkc, "_ntfy_fired_classes", set())

    result = bbkc.run_scan(force=True)
    syms = [r["symbol"] for r in result["results"]]
    assert "TGHT" in syms
    assert "WIDE" not in syms
    assert result["watch_persist"]["inserted"] >= 1
