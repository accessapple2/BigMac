"""tests/test_hm_ops_sentinel_acks.py — HM-SENTINEL-ACK coverage.

Exercises scripts/hm_ops_sentinel.py::_is_suppressed (the pure suppression
decision) and scripts/hm_sentinel_ack.py's ack/unack/list roundtrip against
a temp acks file. Confirms:

  1. No ack -> alert always fires.
  2. Ack with no ceiling -> suppressed regardless of metric magnitude
     (permanent suppression until unacked).
  3. Ack with a ceiling, metric under it -> suppressed. This is the literal
     reported scenario: signals_v2 queue-depth WARNING, acked while the
     oldest-pending row reads 0.0 elapsed MARKET hours (Friday-evening
     backlog sitting through a closed weekend, per
     engine.market_calendar.market_hours_elapsed) against a 13-market-hour
     ceiling (~2 trading sessions) -- must be suppressed.
  4. Ack with a ceiling, metric over it -> fires anyway (simulated
     escalation past the ceiling, e.g. Monday's session comes and goes and
     the row still hasn't drained).
  5. hm_sentinel_ack.py ack/unack round-trips through the acks file on disk.

Run from project root:
    .venv/bin/python3 -m pytest tests/test_hm_ops_sentinel_acks.py -v
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts import hm_ops_sentinel as sentinel  # noqa: E402
from scripts import hm_sentinel_ack as ack_cli  # noqa: E402


def test_no_ack_always_fires():
    assert sentinel._is_suppressed("sentinel_signals_v2_queue", 0.0, {}) is False


def test_ack_with_no_ceiling_suppresses_regardless_of_metric():
    acks = {"sentinel_signals_v2_queue": {"ceiling": None, "acked_at": "x", "acked_by": "y"}}
    assert sentinel._is_suppressed("sentinel_signals_v2_queue", 0.0, acks) is True
    assert sentinel._is_suppressed("sentinel_signals_v2_queue", 9999.0, acks) is True


def test_ack_under_ceiling_suppresses_the_reported_alert():
    """The literal reported 5:20 PM scenario: signals_v2 queue-depth WARNING
    acked while the oldest-pending row's elapsed MARKET time is 0.0h (a
    Friday-evening backlog sitting through the closed weekend) against a
    13-market-hour ceiling (~2 trading sessions) -- must be suppressed."""
    acks = {"sentinel_signals_v2_queue": {"ceiling": 13.0, "acked_at": "x", "acked_by": "y"}}
    assert sentinel._is_suppressed("sentinel_signals_v2_queue", 0.0, acks) is True


def test_ack_ceiling_breach_still_fires():
    """Simulated escalation: two trading sessions' worth of market time pass
    post-ack (e.g. Monday and Tuesday both close) and the row still hasn't
    drained -- must fire despite the ack, so acking never creates a
    permanent blind spot."""
    acks = {"sentinel_signals_v2_queue": {"ceiling": 13.0, "acked_at": "x", "acked_by": "y"}}
    assert sentinel._is_suppressed("sentinel_signals_v2_queue", 14.0, acks) is False


def test_ack_is_per_alert_type():
    acks = {"sentinel_signals_v2_queue": {"ceiling": None, "acked_at": "x", "acked_by": "y"}}
    assert sentinel._is_suppressed("sentinel_fd_red", 300.0, acks) is False


def test_main_dispatches_only_unsuppressed_alerts():
    """End-to-end: two alerts fire from the checks, one is acked-and-under-
    ceiling (suppressed), the other is unacked (must dispatch).

    HM-TEST-ENV-ISOLATION-2026-08-29: every check main() calls must be
    mocked here, not just the two under direct test -- main() now also
    runs check_collector_freshness/check_status_page_heartbeat/
    check_source_health_watcher_heartbeat/check_cron_missing_scripts
    against the REAL live environment (real files, real crontab) when
    left unpatched, and OPS TRIAGE item 2's check_cron_missing_scripts in
    particular found this test's own dev box has genuinely broken cron
    entries -- which leaked a real, unplanned alert into this test's
    strict alert-set assertion. Pin every check to a known-quiet return so
    this test is hermetic regardless of what's actually broken on the
    machine running it.

    HM-ALERT-COOLDOWN-2026-09-03: also pins check_disk_space (the real
    disk crossed DISK_WARN_PCT the same week this test was written) and
    patches STATE_PATH -- main() now persists a per-alert-type cooldown
    there, which would otherwise read/write the real
    data/.hm_ops_sentinel_state.json.
    """
    with tempfile.TemporaryDirectory() as tmp:
        acks_path = Path(tmp) / "acks.json"
        acks_path.write_text(json.dumps({
            "sentinel_signals_v2_queue": {"ceiling": 13.0, "acked_at": "x", "acked_by": "y"},
        }))
        state_path = Path(tmp) / "state.json"  # HM-ALERT-COOLDOWN: don't touch the real state file
        with patch.object(sentinel, "ACKS_PATH", acks_path):
            with patch.object(sentinel, "STATE_PATH", state_path):
                with patch.object(sentinel, "check_fd_count", side_effect=lambda alerts: alerts.append(
                    ("warning", "sentinel_fd_warn", "fd high", 200.0)) or {"pid": 1, "fd_count": 200}):
                    with patch.object(sentinel, "check_lock_errors", return_value={"lock_errors": 0}):
                        with patch.object(sentinel, "check_signals_v2_queue", side_effect=lambda alerts: alerts.append(
                            ("warning", "sentinel_signals_v2_queue", "queue stale", 0.0)) or {"pending": 140, "oldest": "x", "oldest_age_hours": 45.9, "oldest_age_market_hours": 0.0}):
                            with patch.object(sentinel, "check_collector_freshness", return_value={"checked": False, "reason": "market closed"}):
                                with patch.object(sentinel, "check_status_page_heartbeat", return_value={"status_page_heartbeat_age_min": 1.0}):
                                    with patch.object(sentinel, "check_source_health_watcher_heartbeat", return_value={"source_health_heartbeat_age_min": 1.0}):
                                        with patch.object(sentinel, "check_mlx_qwen3_heartbeat", return_value={"mlx_qwen3_heartbeat_age_min": 1.0, "healthy": True}):
                                            with patch.object(sentinel, "check_cron_missing_scripts", return_value={"scanned": 0, "broken": []}):
                                                with patch.object(sentinel, "check_launchd_jobs_health", return_value={"checked": 0, "skipped_by_ledger": [], "stale": []}):
                                                    with patch.object(sentinel, "check_fleet_lifecycle_drift", return_value={"job_drift": [], "agent_drift": [], "overdue": []}):
                                                        with patch.object(sentinel, "check_disk_space", return_value={"disk_pct": 50.0, "disk_free_gb": 500.0}):
                                                            with patch.object(sentinel, "_dispatch") as mock_dispatch:
                                                                rc = sentinel.main()

    assert rc == 2  # sentinel_fd_warn fired unsuppressed
    dispatched_types = {a[1] for a in mock_dispatch.call_args[0][0]}
    assert dispatched_types == {"sentinel_fd_warn"}
    assert "sentinel_signals_v2_queue" not in dispatched_types


def test_ack_cli_roundtrip(tmp_path):
    acks_path = tmp_path / "acks.json"
    with patch.object(ack_cli, "ACKS_PATH", acks_path):
        ack_cli.cmd_ack(_ns(alert_type="sentinel_signals_v2_queue", ceiling=13.0,
                             note="known weekend residual", by="Admiral"))
        acks = ack_cli._load()
        assert acks["sentinel_signals_v2_queue"]["ceiling"] == 13.0
        assert acks["sentinel_signals_v2_queue"]["note"] == "known weekend residual"

        ack_cli.cmd_unack(_ns(alert_type="sentinel_signals_v2_queue"))
        acks = ack_cli._load()
        assert "sentinel_signals_v2_queue" not in acks


class _ns:
    """Minimal argparse.Namespace stand-in for direct cmd_* calls."""

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
