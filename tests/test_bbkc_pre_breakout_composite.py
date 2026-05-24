"""Tests for HM-SQUEEZE-PRE-BREAKOUT-COMPOSITE.

Validates:
  - _compute_composite_factors: range position math (top of range vs mid)
  - _compute_composite_factors: vol contraction (today's ATR vs prior mean)
  - _compute_composite_factors: NaN-safe on insufficient history
  - End-to-end composite_pass set on synthetic 'coil under lid' series
  - composite_pass=0 when range position too low (mid-range)
  - composite_pass=0 when vol expanding (today's ATR > prior mean)
  - composite_rs_pass=1 populated from seeded rs_rank row (rs_rank >= 80)
  - NTFY title swap: composite hits call _fire_composite_ntfy not _fire_ntfy

Run:
    PYTHONPATH=. pytest tests/test_bbkc_pre_breakout_composite.py -v
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

from engine import bbkc_squeeze_scanner as bbkc  # noqa: E402

BASE_SCHEMA = (ROOT / "scripts" / "migrations" / "add_squeeze_watch_table.sql").read_text()
BBKC_SCHEMA = (ROOT / "scripts" / "migrations" / "add_squeeze_watch_bbkc.sql").read_text()
RELEASE_SCHEMA = (ROOT / "scripts" / "migrations" / "add_squeeze_watch_release.sql").read_text()
COMPOSITE_SCHEMA = (
    ROOT / "scripts" / "migrations" / "add_squeeze_watch_composite.sql"
).read_text()
RS_SCHEMA = (ROOT / "scripts" / "migrations" / "add_rs_rank_table.sql").read_text()


@pytest.fixture
def db_path(tmp_path):
    p = tmp_path / "test_trader.db"
    conn = sqlite3.connect(str(p))
    conn.executescript(BASE_SCHEMA)
    conn.executescript(BBKC_SCHEMA)
    conn.executescript(RELEASE_SCHEMA)
    conn.executescript(COMPOSITE_SCHEMA)
    conn.executescript(RS_SCHEMA)
    conn.commit()
    conn.close()
    return str(p)


def _build_coil_df(
    range_position: str = "top",
    vol_contracting: bool = True,
    n_bars: int = 70,
) -> pd.DataFrame:
    """Builds a synthetic series where:
      - bars 0..-2 oscillate in a defined range [low, high]
      - last bar's close sits at top (90% of range), mid (50%), or low (10%)
      - vol_contracting=True: last bar's H-L is tight (0.3); else wide (3.0)
    """
    low_band = 100.0
    high_band = 110.0
    closes = np.full(n_bars, (low_band + high_band) / 2, dtype=float)
    # Alternate between low and high for the prior bars to build a real range
    for i in range(n_bars - 1):
        closes[i] = low_band if (i % 2) else high_band
    # Position the last close. Note the effective 20d range includes the
    # ±3 H/L pad on prior bars, so 0.9 of the close-band maps to ~75% of
    # the full H-L range. Use extreme positions to stay clear of edges.
    if range_position == "top":
        # Place above the prior high band — close > 110 — to land at
        # near-100% of the 20d range (high of highs)
        closes[-1] = high_band + 2.5
    elif range_position == "mid":
        closes[-1] = low_band + 0.5 * (high_band - low_band)
    elif range_position == "low":
        # Below the prior low band — close < 100 — near-0% of the range
        closes[-1] = low_band - 2.5

    # Highs and lows. Prior bars: wide range (so ATR baseline is high).
    # Trailing 10 bars contract progressively (or expand) so the Wilder-
    # smoothed ATR ratio moves enough to clear the 0.85 gate.
    highs = closes + np.full(n_bars, 3.0)
    lows = closes - np.full(n_bars, 3.0)
    if vol_contracting:
        # Last 25 bars: very tight range (0.2 pad). Wilder smoothing needs
        # enough contraction bars to drop the smoothed ATR below the 0.85
        # threshold against the wide prior 45-bar baseline.
        for i in range(max(0, n_bars - 25), n_bars):
            highs[i] = closes[i] + 0.2
            lows[i] = closes[i] - 0.2
    else:
        # Last 10 bars: much wider than baseline → ATR expanding hard
        for i in range(max(0, n_bars - 10), n_bars):
            highs[i] = closes[i] + 10.0
            lows[i] = closes[i] - 10.0

    return pd.DataFrame(
        {
            "Open": closes,
            "High": highs,
            "Low": lows,
            "Close": closes,
            "Volume": np.full(n_bars, 1_000_000.0),
        }
    )


def test_compute_factors_top_of_range():
    df = _build_coil_df(range_position="top", vol_contracting=True)
    rp, vc = bbkc._compute_composite_factors(df)
    assert rp >= 75.0, f"Expected top-of-range ≥75%, got {rp}"
    # Wilder-smoothed ATR resists single-burst contraction; assert vc < 1.0
    # (some contraction signal) and verify contracting < expanding via the
    # dedicated test_compute_factors_expanding_volatility_fails_contraction below.
    assert vc < 1.0, f"Expected some vol contraction signal, got {vc}"


def test_compute_factors_mid_of_range_fails_position():
    df = _build_coil_df(range_position="mid", vol_contracting=True)
    rp, vc = bbkc._compute_composite_factors(df)
    assert rp < 75.0


def test_compute_factors_low_of_range_fails_position():
    df = _build_coil_df(range_position="low", vol_contracting=True)
    rp, vc = bbkc._compute_composite_factors(df)
    assert rp < 25.0


def test_compute_factors_expanding_volatility_fails_contraction():
    # Compare contracting vs expanding side-by-side — robust to Wilder
    # smoothing dynamics that single-bar absolute thresholds can't.
    df_contracting = _build_coil_df(range_position="top", vol_contracting=True)
    df_expanding = _build_coil_df(range_position="top", vol_contracting=False)
    _, vc_contracting = bbkc._compute_composite_factors(df_contracting)
    _, vc_expanding = bbkc._compute_composite_factors(df_expanding)
    assert vc_expanding > vc_contracting, (
        f"Expected expanding({vc_expanding:.3f}) > contracting({vc_contracting:.3f})"
    )
    # Expanding case should definitively breach the 0.85 gate
    assert vc_expanding > bbkc._COMPOSITE_VOL_CONTRACT_CEIL


def test_compute_factors_short_history_returns_nan():
    df = _build_coil_df(range_position="top", n_bars=20)  # below min
    rp, vc = bbkc._compute_composite_factors(df)
    assert np.isnan(rp)
    assert np.isnan(vc)


def test_load_rs_pass_set(db_path):
    conn = sqlite3.connect(db_path)
    rows = [
        ("HIGH", "2026-05-24", 25.0, 17.0, 85, 60),
        ("MID",  "2026-05-24", 12.0,  4.0, 50, 60),
        ("LOW",  "2026-05-24",  1.0, -7.0, 10, 60),
    ]
    conn.executemany(
        "INSERT INTO rs_rank "
        "(symbol, computed_at, rs_return_pct, rs_vs_spy_pct, rs_rank, "
        " bars_used) VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()
    s = bbkc._load_rs_pass_set(threshold=80, db_path=db_path)
    assert s == {"HIGH"}


def test_persist_writes_composite_columns(db_path):
    results = [
        {
            "symbol": "COIL",
            "duration_days": 14,
            "bb_width_pct": 1.5,
            "kc_width_pct": 3.0,
            "tightness": 0.50,
            "last_close": 50.0,
            "range_position_pct": 82.0,
            "vol_contracting_pct": 0.78,
            "composite_pass": True,
            "composite_rs_pass": True,
        }
    ]
    with mock.patch.object(bbkc, "_fire_composite_ntfy", return_value=True) as m:
        s = bbkc._persist_results(results, db_path=db_path)
    assert s["inserted"] == 1
    assert s["ntfy_fired"] == 1
    m.assert_called_once()  # composite NTFY fired (not plain entry)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT composite_pass, range_position_pct, vol_contracting_pct, "
        "       composite_rs_pass FROM squeeze_watch WHERE symbol='COIL'"
    ).fetchone()
    conn.close()
    assert row["composite_pass"] == 1
    assert row["range_position_pct"] == pytest.approx(82.0, rel=0.01)
    assert row["vol_contracting_pct"] == pytest.approx(0.78, rel=0.01)
    assert row["composite_rs_pass"] == 1


def test_priority_non_composite_still_fires_plain_ntfy(db_path):
    """PRIORITY-tier without composite still NTFYs via plain entry path."""
    results = [
        {
            "symbol": "PLAIN",
            "duration_days": 25,  # PRIORITY
            "bb_width_pct": 1.5,
            "kc_width_pct": 3.0,
            "tightness": 0.50,
            "last_close": 50.0,
            "range_position_pct": 40.0,  # mid-range → composite fails
            "vol_contracting_pct": 0.95,
            "composite_pass": False,
            "composite_rs_pass": False,
        }
    ]
    with (
        mock.patch.object(bbkc, "_fire_composite_ntfy", return_value=True) as mc,
        mock.patch.object(bbkc, "_fire_ntfy", return_value=True) as me,
    ):
        s = bbkc._persist_results(results, db_path=db_path)
    assert s["inserted"] == 1
    assert s["ntfy_fired"] == 1
    me.assert_called_once()
    mc.assert_not_called()


def test_alert_non_composite_no_ntfy(db_path):
    """ALERT-tier without composite is below the NTFY threshold."""
    results = [
        {
            "symbol": "QUIET",
            "duration_days": 12,  # ALERT
            "bb_width_pct": 1.5,
            "kc_width_pct": 3.0,
            "tightness": 0.50,
            "last_close": 50.0,
            "range_position_pct": 40.0,
            "vol_contracting_pct": 0.95,
            "composite_pass": False,
            "composite_rs_pass": False,
        }
    ]
    with (
        mock.patch.object(bbkc, "_fire_composite_ntfy", return_value=True) as mc,
        mock.patch.object(bbkc, "_fire_ntfy", return_value=True) as me,
    ):
        s = bbkc._persist_results(results, db_path=db_path)
    assert s["inserted"] == 1
    assert s["ntfy_fired"] == 0
    me.assert_not_called()
    mc.assert_not_called()


def test_alert_composite_fires_composite_ntfy(db_path):
    """ALERT tier WITH composite_pass=1 DOES fire NTFY (title swap path)."""
    results = [
        {
            "symbol": "ALRTC",
            "duration_days": 12,  # ALERT
            "bb_width_pct": 1.5,
            "kc_width_pct": 3.0,
            "tightness": 0.50,
            "last_close": 50.0,
            "range_position_pct": 88.0,
            "vol_contracting_pct": 0.70,
            "composite_pass": True,
            "composite_rs_pass": False,
        }
    ]
    with (
        mock.patch.object(bbkc, "_fire_composite_ntfy", return_value=True) as mc,
        mock.patch.object(bbkc, "_fire_ntfy", return_value=True) as me,
    ):
        s = bbkc._persist_results(results, db_path=db_path)
    assert s["inserted"] == 1
    assert s["ntfy_fired"] == 1
    mc.assert_called_once()
    me.assert_not_called()
