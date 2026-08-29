"""tests/test_status_page_heartbeat_sentinel.py — HM-STATUSPAGE-FREEZE-2026-08-29.

status.ollietrades.com's "Last checked" froze ~11.5h (Fri 21:54 -> Sat
09:25) because scripts/status_page.py had no independent heartbeat --
only computed a fresh timestamp on each HTTP request. Fixed with a
background thread that persists its own check result to a JSON sidecar
on a fixed 5-min cadence, and scripts/hm_ops_sentinel.py::
check_status_page_heartbeat() now watches THAT file's age (not the live
page, which would trivially look fresh the instant anything -- including
the sentinel's own probe -- requests it).

These tests exercise check_status_page_heartbeat() against a temp
heartbeat file, patching STATUS_PAGE_HEARTBEAT_PATH so nothing touches
the real one.
"""
from __future__ import annotations

import json
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts import hm_ops_sentinel as sentinel  # noqa: E402


def _write_heartbeat(path: Path, age_min: float) -> None:
    checked_dt = datetime.now(timezone.utc) - timedelta(minutes=age_min)
    path.write_text(json.dumps({
        "bigmac": True, "trader": True, "tunnel": True,
        "checked_at": checked_dt.strftime("%Y-%m-%d %H:%M:%S UTC"),
    }))


def test_fresh_heartbeat_no_alert():
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "heartbeat.json"
        _write_heartbeat(path, age_min=1.0)
        with patch.object(sentinel, "STATUS_PAGE_HEARTBEAT_PATH", path):
            alerts: list = []
            result = sentinel.check_status_page_heartbeat(alerts)
        assert alerts == []
        assert result["status_page_heartbeat_age_min"] < 2.0


def test_stale_heartbeat_fires_red_alert():
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "heartbeat.json"
        _write_heartbeat(path, age_min=20.0)  # > 15 min threshold
        with patch.object(sentinel, "STATUS_PAGE_HEARTBEAT_PATH", path):
            alerts: list = []
            result = sentinel.check_status_page_heartbeat(alerts)
        assert len(alerts) == 1
        level_kw, alert_type, message, metric = alerts[0]
        assert level_kw == "red_alert"
        assert alert_type == "sentinel_status_page_heartbeat_stale"
        assert metric > 15.0
        assert result["status_page_heartbeat_age_min"] > 15.0


def test_heartbeat_just_under_threshold_no_alert():
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "heartbeat.json"
        _write_heartbeat(path, age_min=10.0)  # < 15 min threshold
        with patch.object(sentinel, "STATUS_PAGE_HEARTBEAT_PATH", path):
            alerts: list = []
            sentinel.check_status_page_heartbeat(alerts)
        assert alerts == []


def test_missing_file_fires_warning_not_red_alert():
    """First 5 min after a restart is routine, not an incident -- WARNING,
    not RED_ALERT, and a message that says so."""
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "does_not_exist.json"
        with patch.object(sentinel, "STATUS_PAGE_HEARTBEAT_PATH", path):
            alerts: list = []
            result = sentinel.check_status_page_heartbeat(alerts)
        assert len(alerts) == 1
        level_kw, alert_type, message, metric = alerts[0]
        assert level_kw == "warning"
        assert alert_type == "sentinel_status_page_heartbeat_missing"
        assert metric is None
        assert result["status_page_heartbeat_age_min"] is None


def test_corrupt_json_does_not_crash():
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "heartbeat.json"
        path.write_text("{not valid json")
        with patch.object(sentinel, "STATUS_PAGE_HEARTBEAT_PATH", path):
            alerts: list = []
            result = sentinel.check_status_page_heartbeat(alerts)
        assert alerts == []  # swallowed, logged to stderr, not alerted
        assert result["status_page_heartbeat_age_min"] is None
