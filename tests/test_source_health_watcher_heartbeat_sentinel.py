"""tests/test_source_health_watcher_heartbeat_sentinel.py — OPS TRIAGE item 2.

scripts/source_health_watcher.py died 2026-07-22 (same stand-down, same
root cause as HM-GEX-COLLECTOR-DEAD) and stayed dead 54,682 minutes.
main.py's own in-process dead-man's-switch (_bg_source_health_dms) had
been firing correctly the entire time -- but at AlertLevel.WARNING, which
DECOM-SILENCE (2026-07-19) mutes. check_source_health_watcher_heartbeat()
fires RED_ALERT specifically so it reaches Pushover regardless.
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
    last_run_ts = (datetime.now(timezone.utc) - timedelta(minutes=age_min)).timestamp()
    path.write_text(json.dumps({
        "watcher": "source_health_watcher", "last_run": last_run_ts,
        "last_run_iso": "x", "sources_checked": 16,
    }))


def test_fresh_heartbeat_no_alert():
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "heartbeat.json"
        _write_heartbeat(path, age_min=1.0)
        with patch.object(sentinel, "SOURCE_HEALTH_HEARTBEAT_PATH", path):
            alerts: list = []
            result = sentinel.check_source_health_watcher_heartbeat(alerts)
        assert alerts == []
        assert result["source_health_heartbeat_age_min"] < 2.0


def test_stale_heartbeat_fires_red_alert_not_warning():
    """Must be RED_ALERT specifically -- WARNING is exactly what already
    existed (main.py's in-process check) and was silently muted by
    DECOM-SILENCE for 38 days. A WARNING here would repeat the same gap."""
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "heartbeat.json"
        _write_heartbeat(path, age_min=40.0)  # > 35 min threshold
        with patch.object(sentinel, "SOURCE_HEALTH_HEARTBEAT_PATH", path):
            alerts: list = []
            result = sentinel.check_source_health_watcher_heartbeat(alerts)
        assert len(alerts) == 1
        level_kw, alert_type, message, metric = alerts[0]
        assert level_kw == "red_alert"
        assert alert_type == "sentinel_source_health_watcher_heartbeat_stale"
        assert metric > 35.0
        assert result["source_health_heartbeat_age_min"] > 35.0


def test_heartbeat_just_under_threshold_no_alert():
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "heartbeat.json"
        _write_heartbeat(path, age_min=30.0)
        with patch.object(sentinel, "SOURCE_HEALTH_HEARTBEAT_PATH", path):
            alerts: list = []
            sentinel.check_source_health_watcher_heartbeat(alerts)
        assert alerts == []


def test_missing_file_fires_warning_not_red_alert():
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "does_not_exist.json"
        with patch.object(sentinel, "SOURCE_HEALTH_HEARTBEAT_PATH", path):
            alerts: list = []
            result = sentinel.check_source_health_watcher_heartbeat(alerts)
        assert len(alerts) == 1
        level_kw, alert_type, message, metric = alerts[0]
        assert level_kw == "warning"
        assert alert_type == "sentinel_source_health_watcher_heartbeat_missing"
        assert result["source_health_heartbeat_age_min"] is None


def test_corrupt_json_does_not_crash():
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "heartbeat.json"
        path.write_text("{not valid json")
        with patch.object(sentinel, "SOURCE_HEALTH_HEARTBEAT_PATH", path):
            alerts: list = []
            result = sentinel.check_source_health_watcher_heartbeat(alerts)
        assert alerts == []
        assert result["source_health_heartbeat_age_min"] is None
