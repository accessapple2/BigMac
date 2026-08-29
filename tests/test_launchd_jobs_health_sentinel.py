"""tests/test_launchd_jobs_health_sentinel.py — freshness coverage for the
18 com.ollietrades.* LaunchAgents reactivated 2026-08-29 after the
2026-07-22 stand-down.

check_launchd_jobs_health() has two independent failure modes to catch:
drifting back into launchd's disabled state, and staying enabled+loaded
but silently no longer firing (log gone stale past its own cadence-scaled
ceiling). Mirrors check_cron_missing_scripts' "no unwatched watchers"
philosophy for launchd instead of crontab.
"""
from __future__ import annotations

import subprocess
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts import hm_ops_sentinel as sentinel  # noqa: E402


def _fake_run(stdout: str):
    proc = MagicMock()
    proc.stdout = stdout
    return proc


def test_all_healthy_no_alerts():
    with tempfile.TemporaryDirectory() as d:
        registry = {}
        for label, (rel, ceiling) in sentinel.LAUNCHD_JOB_REGISTRY.items():
            p = Path(d) / f"{label}.log"
            p.write_text("ok\n")
            registry[label] = (f"{label}.log", ceiling)
        with patch.object(sentinel, "ROOT", Path(d)), \
             patch.object(sentinel, "LAUNCHD_JOB_REGISTRY", registry), \
             patch("subprocess.run", return_value=_fake_run("")):
            alerts: list = []
            result = sentinel.check_launchd_jobs_health(alerts)
        assert result["disabled_again"] == []
        assert result["stale"] == []
        assert alerts == []


def test_disabled_again_fires_and_skips_staleness():
    with tempfile.TemporaryDirectory() as d:
        registry = {"universe-refresh": ("universe-refresh.log", 216.0)}
        cron_out = '\t\t"com.ollietrades.universe-refresh" => disabled\n'
        with patch.object(sentinel, "ROOT", Path(d)), \
             patch.object(sentinel, "LAUNCHD_JOB_REGISTRY", registry), \
             patch("subprocess.run", return_value=_fake_run(cron_out)):
            alerts: list = []
            result = sentinel.check_launchd_jobs_health(alerts)
        assert result["disabled_again"] == ["universe-refresh"]
        assert result["stale"] == []  # skipped -- disabled is reported instead
        assert len(alerts) == 1
        assert alerts[0][1] == "sentinel_launchd_job_disabled_again"


def test_stale_log_fires():
    with tempfile.TemporaryDirectory() as d:
        registry = {"nightly-backtest": ("nightly_backtest.log", 30.0)}
        p = Path(d) / "nightly_backtest.log"
        p.write_text("old\n")
        old_time = time.time() - (40 * 3600)  # 40h old, past the 30h ceiling
        import os
        os.utime(p, (old_time, old_time))
        with patch.object(sentinel, "ROOT", Path(d)), \
             patch.object(sentinel, "LAUNCHD_JOB_REGISTRY", registry), \
             patch("subprocess.run", return_value=_fake_run("")):
            alerts: list = []
            result = sentinel.check_launchd_jobs_health(alerts)
        assert len(result["stale"]) == 1
        assert result["stale"][0]["label"] == "nightly-backtest"
        assert len(alerts) == 1
        assert alerts[0][1] == "sentinel_launchd_job_stale"


def test_missing_log_not_flagged():
    """A job just reactivated today with no log yet isn't stale -- it's new."""
    with tempfile.TemporaryDirectory() as d:
        registry = {"scotty": ("scotty.out.log", 48.0)}
        with patch.object(sentinel, "ROOT", Path(d)), \
             patch.object(sentinel, "_ALT_LOG_ROOT", Path(d) / "nonexistent"), \
             patch.object(sentinel, "LAUNCHD_JOB_REGISTRY", registry), \
             patch("subprocess.run", return_value=_fake_run("")):
            alerts: list = []
            result = sentinel.check_launchd_jobs_health(alerts)
        assert result["stale"] == []
        assert alerts == []


def test_launchctl_failure_does_not_crash():
    with tempfile.TemporaryDirectory() as d:
        registry = {"scotty": ("scotty.out.log", 48.0)}
        with patch.object(sentinel, "ROOT", Path(d)), \
             patch.object(sentinel, "LAUNCHD_JOB_REGISTRY", registry), \
             patch("subprocess.run", side_effect=subprocess.TimeoutExpired("launchctl", 10)):
            alerts: list = []
            result = sentinel.check_launchd_jobs_health(alerts)
        assert result["checked"] == 1
        assert alerts == []
