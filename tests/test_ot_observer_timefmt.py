"""ot_observer timestamp labelling: every timestamp leaves as UTC and MST, both labelled.

Three misreads in the week of 2026-09-08 came from a UTC column compared against a local
date. Bases below were verified against the writers on 2026-09-14.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ot_observer_testkit  # noqa: E402,F401

from ot_observer import timefmt as tf  # noqa: E402


def test_label_carries_both_zones_with_labels():
    assert tf.label(datetime(2026, 9, 15, 3, 37, 36, tzinfo=timezone.utc)) == {
        "utc": "2026-09-15T03:37:36Z",
        "mst": "2026-09-14 20:37:36 MST",
    }


def test_utc_column_value():
    assert tf.from_db("2026-09-14 19:55:58", "utc") == {
        "utc": "2026-09-14T19:55:58Z",
        "mst": "2026-09-14 12:55:58 MST",
        "stored_as": "utc",
        "raw": "2026-09-14 19:55:58",
    }


def test_mst_local_column_value():
    assert tf.from_db("2026-08-31T11:26:16.822232", "mst_local") == {
        "utc": "2026-08-31T18:26:16Z",
        "mst": "2026-08-31 11:26:16 MST",
        "stored_as": "mst_local",
        "raw": "2026-08-31T11:26:16.822232",
    }


def test_halted_at_basis_is_read_per_value():
    """fleet_lifecycle.py writes datetime.now().isoformat() (local, 'T' + fraction);
    proving_ground and manual halts write CURRENT_TIMESTAMP (UTC, space-separated)."""
    local = tf.from_db("2026-08-31T11:26:16.822232", "halted_at")
    utc = tf.from_db("2026-06-24 03:07:17", "halted_at")
    assert local["stored_as"] == "mst_local" and local["utc"] == "2026-08-31T18:26:16Z"
    assert utc["stored_as"] == "utc" and utc["mst"] == "2026-06-23 20:07:17 MST"


def test_null_stays_null():
    assert tf.from_db(None, "utc") is None


def test_unparseable_value_is_marked_not_guessed():
    assert tf.from_db("garbage", "utc") == {
        "utc": None, "mst": None, "stored_as": "unparseable", "raw": "garbage",
    }


def test_log_mst_value_converts_to_utc():
    assert tf.from_log_mst(datetime(2026, 9, 14, 20, 16, 0)) == {
        "utc": "2026-09-15T03:16:00Z",
        "mst": "2026-09-14 20:16:00 MST",
        "stored_as": "mst_local",
    }


def test_mst_is_utc_minus_7_all_year():
    """Arizona does not observe DST."""
    assert tf.from_db("2026-01-15 12:00:00", "utc")["mst"] == "2026-01-15 05:00:00 MST"
    assert tf.from_db("2026-07-15 12:00:00", "utc")["mst"] == "2026-07-15 05:00:00 MST"
