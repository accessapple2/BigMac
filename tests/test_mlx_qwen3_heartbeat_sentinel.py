"""tests/test_mlx_qwen3_heartbeat_sentinel.py — HM-MLX-QWEN3-REVIVAL-2026-08-29.

mlx-qwen3's local MLX server died 2026-07-18 with zero supervision (no
launchd, no cron) and stayed dead six weeks. Revived under
com.ollietrades.mlx-qwen3.plist (KeepAlive); check_mlx_qwen3_heartbeat
watches scripts/mlx_qwen3_probe.py's heartbeat on a different mechanism
than the server itself, per the "alarm must not share a failure mode
with what it watches" doctrine.
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


def _write_heartbeat(path: Path, age_min: float, healthy: bool = True, detail=None) -> None:
    last_run_ts = (datetime.now(timezone.utc) - timedelta(minutes=age_min)).timestamp()
    path.write_text(json.dumps({
        "watcher": "mlx_qwen3_probe", "last_run": last_run_ts, "last_run_iso": "x",
        "healthy": healthy, "latency_ms": 12.3, "detail": detail,
    }))


def test_fresh_healthy_heartbeat_no_alert():
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "heartbeat.json"
        _write_heartbeat(path, age_min=1.0, healthy=True)
        with patch.object(sentinel, "MLX_QWEN3_HEARTBEAT_PATH", path):
            alerts: list = []
            result = sentinel.check_mlx_qwen3_heartbeat(alerts)
        assert alerts == []
        assert result["healthy"] is True


def test_stale_heartbeat_fires_red_alert():
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "heartbeat.json"
        _write_heartbeat(path, age_min=25.0, healthy=True)  # > 20 min threshold
        with patch.object(sentinel, "MLX_QWEN3_HEARTBEAT_PATH", path):
            alerts: list = []
            result = sentinel.check_mlx_qwen3_heartbeat(alerts)
        assert len(alerts) == 1
        level_kw, alert_type, message, metric = alerts[0]
        assert level_kw == "red_alert"
        assert alert_type == "sentinel_mlx_qwen3_heartbeat_stale"
        assert metric > 20.0
        assert result["mlx_qwen3_heartbeat_age_min"] > 20.0


def test_fresh_but_unhealthy_fires_warning_not_red_alert():
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "heartbeat.json"
        _write_heartbeat(path, age_min=1.0, healthy=False, detail="ConnectionRefusedError")
        with patch.object(sentinel, "MLX_QWEN3_HEARTBEAT_PATH", path):
            alerts: list = []
            result = sentinel.check_mlx_qwen3_heartbeat(alerts)
        assert len(alerts) == 1
        level_kw, alert_type, message, metric = alerts[0]
        assert level_kw == "warning"
        assert alert_type == "sentinel_mlx_qwen3_unhealthy"
        assert "ConnectionRefusedError" in message
        assert result["healthy"] is False


def test_stale_and_unhealthy_only_fires_stale_alert():
    """Staleness is the more serious finding -- don't double-fire when both conditions hold."""
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "heartbeat.json"
        _write_heartbeat(path, age_min=30.0, healthy=False)
        with patch.object(sentinel, "MLX_QWEN3_HEARTBEAT_PATH", path):
            alerts: list = []
            sentinel.check_mlx_qwen3_heartbeat(alerts)
        assert len(alerts) == 1
        assert alerts[0][1] == "sentinel_mlx_qwen3_heartbeat_stale"


def test_heartbeat_just_under_threshold_no_alert():
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "heartbeat.json"
        _write_heartbeat(path, age_min=15.0, healthy=True)
        with patch.object(sentinel, "MLX_QWEN3_HEARTBEAT_PATH", path):
            alerts: list = []
            sentinel.check_mlx_qwen3_heartbeat(alerts)
        assert alerts == []


def test_missing_file_fires_warning_not_red_alert():
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "does_not_exist.json"
        with patch.object(sentinel, "MLX_QWEN3_HEARTBEAT_PATH", path):
            alerts: list = []
            result = sentinel.check_mlx_qwen3_heartbeat(alerts)
        assert len(alerts) == 1
        level_kw, alert_type, message, metric = alerts[0]
        assert level_kw == "warning"
        assert alert_type == "sentinel_mlx_qwen3_heartbeat_missing"
        assert result["mlx_qwen3_heartbeat_age_min"] is None


def test_corrupt_json_does_not_crash():
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "heartbeat.json"
        path.write_text("{not valid json")
        with patch.object(sentinel, "MLX_QWEN3_HEARTBEAT_PATH", path):
            alerts: list = []
            result = sentinel.check_mlx_qwen3_heartbeat(alerts)
        assert alerts == []
        assert result["mlx_qwen3_heartbeat_age_min"] is None
