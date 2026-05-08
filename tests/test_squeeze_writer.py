"""Tests for HM-AO-β squeeze_watch persistence in engine/squeeze_scanner.py.

Verifies:
  - Results below _MIN_PERSIST_SCORE are not written
  - Composite score is score * 10
  - Tier mapping: WATCH 50-74, ALERT 75-89, PRIORITY 90+
  - 24h dedupe: same-symbol re-insert only when tier upgrades
  - Quiet-hours flag mark (ntfy_deferred)
  - Failures (bad row) are caught, never raised

Run:
    python3 -m pytest tests/test_squeeze_writer.py -v
"""

from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Import the module after path is set
from engine import squeeze_scanner as ss  # noqa: E402


SCHEMA_SQL = (ROOT / "scripts" / "migrations" / "add_squeeze_watch_table.sql").read_text()


@pytest.fixture
def db_path(tmp_path):
    p = tmp_path / "test_trader.db"
    conn = sqlite3.connect(str(p))
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    conn.close()
    return str(p)


def _row(score: int, ticker: str = "TEST", **overrides):
    base = {
        "ticker": ticker,
        "short_interest_pct": 25.0,
        "float_m": 8.0,
        "days_to_cover": 4.0,
        "vol_ratio": 3.0,
        "price": 12.5,
        "day_change_pct": 1.2,
        "rsi": 45.0,
        "above_10d_high": True,
        "score": score,
    }
    base.update(overrides)
    return base


def _select_all(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM squeeze_watch ORDER BY id").fetchall()
    conn.close()
    return rows


def test_below_min_score_not_written(db_path):
    summary = ss._persist_results([_row(3, "AAA"), _row(4, "BBB")], db_path=db_path)
    assert summary["inserted"] == 0
    assert _select_all(db_path) == []


def test_score_to_composite_mapping(db_path):
    # score 5 → 50 (WATCH), 8 → 80 (ALERT), 10 → 100 (PRIORITY)
    with mock.patch.object(ss, "_is_quiet_hours_et", return_value=True):
        # quiet hours so we don't actually post ntfy in tests
        ss._persist_results(
            [_row(5, "WAT"), _row(8, "ALR"), _row(10, "PRI")],
            db_path=db_path,
        )
    rows = {r["symbol"]: r for r in _select_all(db_path)}
    assert rows["WAT"]["composite_score"] == 50.0
    assert rows["WAT"]["threshold_tier"] == "WATCH"
    assert rows["ALR"]["composite_score"] == 80.0
    assert rows["ALR"]["threshold_tier"] == "ALERT"
    assert rows["PRI"]["composite_score"] == 100.0
    assert rows["PRI"]["threshold_tier"] == "PRIORITY"


def test_tier_thresholds_boundaries(db_path):
    # exact boundary scores
    with mock.patch.object(ss, "_is_quiet_hours_et", return_value=True):
        ss._persist_results(
            [
                _row(7, "S07"),  # 70 → WATCH (boundary)
                _row(8, "S08"),  # 80 → ALERT (75 boundary)
                _row(9, "S09"),  # 90 → PRIORITY (boundary)
            ],
            db_path=db_path,
        )
    rows = {r["symbol"]: r["threshold_tier"] for r in _select_all(db_path)}
    assert rows["S07"] == "WATCH"
    assert rows["S08"] == "ALERT"
    assert rows["S09"] == "PRIORITY"


def test_dedupe_same_tier_skipped(db_path):
    with mock.patch.object(ss, "_is_quiet_hours_et", return_value=True):
        ss._persist_results([_row(7, "DUP")], db_path=db_path)
        s2 = ss._persist_results([_row(7, "DUP")], db_path=db_path)
    assert s2["inserted"] == 0
    assert s2["skipped_dedup"] == 1
    assert len(_select_all(db_path)) == 1


def test_dedupe_lower_tier_skipped(db_path):
    with mock.patch.object(ss, "_is_quiet_hours_et", return_value=True):
        ss._persist_results([_row(9, "SYM")], db_path=db_path)  # PRIORITY
        s2 = ss._persist_results([_row(6, "SYM")], db_path=db_path)  # WATCH
    assert s2["inserted"] == 0
    assert s2["skipped_dedup"] == 1


def test_dedupe_upgrade_inserts_new_row(db_path):
    with mock.patch.object(ss, "_is_quiet_hours_et", return_value=True):
        ss._persist_results([_row(6, "UP")], db_path=db_path)  # WATCH
        s2 = ss._persist_results([_row(9, "UP")], db_path=db_path)  # PRIORITY
    assert s2["inserted"] == 1
    rows = _select_all(db_path)
    assert len(rows) == 2
    assert rows[0]["threshold_tier"] == "WATCH"
    assert rows[1]["threshold_tier"] == "PRIORITY"


def test_quiet_hours_marks_deferred_for_priority_only(db_path):
    with mock.patch.object(ss, "_is_quiet_hours_et", return_value=True):
        ss._persist_results(
            [_row(6, "WLO"), _row(8, "ALH"), _row(10, "PHI")],
            db_path=db_path,
        )
    rows = {r["symbol"]: r for r in _select_all(db_path)}
    # Only PRIORITY rows are marked deferred — WATCH/ALERT have no ntfy
    # path so no defer flag is needed.
    assert rows["WLO"]["ntfy_deferred"] == 0
    assert rows["ALH"]["ntfy_deferred"] == 0
    assert rows["PHI"]["ntfy_deferred"] == 1


def test_no_signals_table_writes(db_path):
    """Sacred rule: scanner must not write to signals table."""
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS signals "
        "(id INTEGER PRIMARY KEY, player_id TEXT, symbol TEXT, signal TEXT)"
    )
    conn.commit()
    conn.close()

    with mock.patch.object(ss, "_is_quiet_hours_et", return_value=True):
        ss._persist_results([_row(10, "ANY")], db_path=db_path)

    conn = sqlite3.connect(db_path)
    n = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
    conn.close()
    assert n == 0


def test_bad_row_does_not_raise(db_path):
    """Malformed row (missing ticker) is skipped silently."""
    with mock.patch.object(ss, "_is_quiet_hours_et", return_value=True):
        # Mix one good, one missing-ticker row
        s = ss._persist_results(
            [_row(8, ""), _row(9, "GOOD")],
            db_path=db_path,
        )
    rows = _select_all(db_path)
    assert len(rows) == 1
    assert rows[0]["symbol"] == "GOOD"
    assert s["inserted"] == 1


def test_tier_helpers():
    assert ss._tier_for_composite(49) == "WATCH"
    assert ss._tier_for_composite(50) == "WATCH"
    assert ss._tier_for_composite(74) == "WATCH"
    assert ss._tier_for_composite(75) == "ALERT"
    assert ss._tier_for_composite(89) == "ALERT"
    assert ss._tier_for_composite(90) == "PRIORITY"
    assert ss._tier_for_composite(100) == "PRIORITY"
    assert ss._tier_rank("PRIORITY") > ss._tier_rank("ALERT") > ss._tier_rank("WATCH")
