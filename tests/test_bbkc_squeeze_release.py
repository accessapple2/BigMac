"""Tests for HM-SQUEEZE-RELEASE-DETECT (engine/bbkc_squeeze_scanner.py
release-detection pass).

Validates:
  - _detect_release direction logic (up / down / both)
  - Volume gate (returns released=True with low volume; caller filters NTFY)
  - No release while still in squeeze
  - _scan_for_releases marks row + skips already-released rows
  - Idempotent re-run does not re-update or re-NTFY
  - Already-released rows excluded from the scan query

Run:
    PYTHONPATH=. pytest tests/test_bbkc_squeeze_release.py -v
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


@pytest.fixture
def db_path(tmp_path):
    p = tmp_path / "test_trader.db"
    conn = sqlite3.connect(str(p))
    conn.executescript(BASE_SCHEMA)
    conn.executescript(BBKC_SCHEMA)
    conn.executescript(RELEASE_SCHEMA)
    conn.commit()
    conn.close()
    return str(p)


def _tight_then_pop_df(direction: str, vol_spike: float = 4.0) -> pd.DataFrame:
    """Build 25 tight bars + 1 pop bar in the given direction."""
    rng = np.random.RandomState(11)
    tight = 100 + np.cumsum(rng.randn(25) * 0.05)
    pop = tight[-1] + (3.0 if direction == "up" else -3.0)
    closes = np.concatenate([tight, [pop]])
    if direction == "up":
        highs = closes + np.array([0.1] * 25 + [3.5])
        lows = closes - np.array([0.1] * 26)
    else:
        highs = closes + np.array([0.1] * 26)
        lows = closes - np.array([0.1] * 25 + [3.5])
    vols = np.array([1_000_000.0] * 25 + [1_000_000.0 * vol_spike])
    return pd.DataFrame(
        {"Open": closes, "High": highs, "Low": lows, "Close": closes, "Volume": vols}
    )


def test_detect_release_bullish_with_volume():
    df = _tight_then_pop_df("up", vol_spike=4.0)
    released, direction, vol_ratio, last_close, excess = bbkc._detect_release(df)
    assert released is True
    assert direction == "up"
    assert vol_ratio == pytest.approx(4.0, rel=0.01)
    assert last_close > 100
    assert excess > 0


def test_detect_release_bearish():
    df = _tight_then_pop_df("down", vol_spike=3.0)
    released, direction, vol_ratio, _, _ = bbkc._detect_release(df)
    assert released is True
    assert direction == "down"
    assert vol_ratio == pytest.approx(3.0, rel=0.01)


def test_detect_no_release_while_still_in_squeeze():
    rng = np.random.RandomState(7)
    closes = 100 + np.cumsum(rng.randn(30) * 0.05)
    df = pd.DataFrame(
        {
            "Open": closes,
            "High": closes + 0.1,
            "Low": closes - 0.1,
            "Close": closes,
            "Volume": np.full(30, 1_000_000.0),
        }
    )
    released, direction, _, _, _ = bbkc._detect_release(df)
    assert released is False
    assert direction is None


def test_detect_release_low_volume_still_marks_but_under_gate():
    # Vol ratio 0.5× — release detected but below 2.0 gate; caller should
    # filter NTFY downstream. Detection itself does NOT gate on volume.
    df = _tight_then_pop_df("up", vol_spike=0.5)
    released, direction, vol_ratio, _, _ = bbkc._detect_release(df)
    assert released is True
    assert direction == "up"
    assert vol_ratio < bbkc._RELEASE_VOL_GATE


def test_scan_for_releases_updates_row(db_path, monkeypatch):
    # Seed an unreleased ALERT row for AAA
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO squeeze_watch "
        "(symbol, scan_ts, composite_score, threshold_tier, price_at_scan, "
        " kind, bbkc_duration_days) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("AAA", datetime.now(timezone.utc).isoformat(), 60.0, "ALERT", 100.0,
         "bbkc", 12),
    )
    conn.commit()
    conn.close()

    bars = {"AAA": _tight_then_pop_df("up", vol_spike=4.0)}
    monkeypatch.setattr(bbkc, "_ntfy_fired_classes", set())
    # Block real NTFY HTTP calls
    with mock.patch.object(bbkc, "_fire_release_ntfy", return_value=True) as m:
        summary = bbkc._scan_for_releases(
            bars, db_path=db_path, remaining_ntfy_budget=5
        )
    assert summary["detected"] == 1
    assert summary["ntfy_fired"] == 1
    m.assert_called_once()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT released_at, release_direction, release_volume_ratio, "
        "       release_close FROM squeeze_watch WHERE symbol='AAA'"
    ).fetchone()
    conn.close()
    assert row["released_at"] is not None
    assert row["release_direction"] == "up"
    assert row["release_volume_ratio"] == pytest.approx(4.0, rel=0.01)
    assert row["release_close"] > 100


def test_scan_for_releases_idempotent(db_path, monkeypatch):
    # Seed an unreleased PRIORITY row
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO squeeze_watch "
        "(symbol, scan_ts, composite_score, threshold_tier, price_at_scan, "
        " kind, bbkc_duration_days) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("BBB", datetime.now(timezone.utc).isoformat(), 100.0, "PRIORITY",
         100.0, "bbkc", 25),
    )
    conn.commit()
    conn.close()

    bars = {"BBB": _tight_then_pop_df("down", vol_spike=2.5)}
    monkeypatch.setattr(bbkc, "_ntfy_fired_classes", set())
    with mock.patch.object(bbkc, "_fire_release_ntfy", return_value=True) as m:
        s1 = bbkc._scan_for_releases(bars, db_path=db_path, remaining_ntfy_budget=5)
        s2 = bbkc._scan_for_releases(bars, db_path=db_path, remaining_ntfy_budget=5)
    assert s1["detected"] == 1
    assert s1["ntfy_fired"] == 1
    # Second pass — row already has released_at IS NOT NULL → excluded
    assert s2["detected"] == 0
    assert s2["ntfy_fired"] == 0
    assert m.call_count == 1


def test_scan_for_releases_below_vol_gate_no_ntfy(db_path, monkeypatch):
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO squeeze_watch "
        "(symbol, scan_ts, composite_score, threshold_tier, price_at_scan, "
        " kind, bbkc_duration_days) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("LOW", datetime.now(timezone.utc).isoformat(), 60.0, "ALERT", 100.0,
         "bbkc", 14),
    )
    conn.commit()
    conn.close()

    bars = {"LOW": _tight_then_pop_df("up", vol_spike=1.2)}  # 1.2 < 2.0 gate
    monkeypatch.setattr(bbkc, "_ntfy_fired_classes", set())
    with mock.patch.object(bbkc, "_fire_release_ntfy", return_value=True) as m:
        summary = bbkc._scan_for_releases(
            bars, db_path=db_path, remaining_ntfy_budget=5
        )
    assert summary["detected"] == 1
    assert summary["ntfy_fired"] == 0
    assert summary["skipped"] >= 1
    m.assert_not_called()
    # Row still gets released_at — release happened, just no NTFY
    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT released_at, release_direction FROM squeeze_watch "
        "WHERE symbol='LOW'"
    ).fetchone()
    conn.close()
    assert row[0] is not None
    assert row[1] == "up"


def test_scan_for_releases_watch_tier_ignored(db_path, monkeypatch):
    # WATCH tier rows should NOT be release-scanned (only ALERT/PRIORITY).
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO squeeze_watch "
        "(symbol, scan_ts, composite_score, threshold_tier, price_at_scan, "
        " kind, bbkc_duration_days) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("WTCH", datetime.now(timezone.utc).isoformat(), 30.0, "WATCH", 100.0,
         "bbkc", 6),
    )
    conn.commit()
    conn.close()

    bars = {"WTCH": _tight_then_pop_df("up", vol_spike=4.0)}
    monkeypatch.setattr(bbkc, "_ntfy_fired_classes", set())
    summary = bbkc._scan_for_releases(
        bars, db_path=db_path, remaining_ntfy_budget=5
    )
    assert summary["detected"] == 0
    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT released_at FROM squeeze_watch WHERE symbol='WTCH'"
    ).fetchone()
    conn.close()
    assert row[0] is None  # untouched
