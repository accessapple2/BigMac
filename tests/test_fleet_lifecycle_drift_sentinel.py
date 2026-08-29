"""tests/test_fleet_lifecycle_drift_sentinel.py — HM-FLEET-LIFECYCLE-2026-08-29.

check_fleet_lifecycle_drift() is the doctrine's "manual plist/cron edits to
fleet jobs become a sentinel finding of their own" enforcement: live state
(launchctl print-disabled, ai_players.halt_mode) vs. the ledger's latest
recorded action for the same target, plus overdue resume_by/review_by dates
on paused targets.
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts import hm_ops_sentinel as sentinel  # noqa: E402


def _make_db(tmp_path) -> Path:
    db_path = tmp_path / "trader.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""CREATE TABLE ai_players (id TEXT PRIMARY KEY, halt_mode TEXT)""")
    conn.execute("""CREATE TABLE fleet_lifecycle_ledger (
        id INTEGER PRIMARY KEY AUTOINCREMENT, target_type TEXT, target_name TEXT,
        action TEXT, reason TEXT, order_doc TEXT, resume_by TEXT, review_by TEXT,
        backfilled INTEGER DEFAULT 0, created_at TEXT DEFAULT (datetime('now')),
        created_by TEXT DEFAULT 'test')""")
    conn.commit()
    conn.close()
    return db_path


def _fake_run(stdout: str):
    proc = MagicMock()
    proc.stdout = stdout
    return proc


def test_job_drift_detected_ledger_active_but_live_disabled(tmp_path):
    db_path = _make_db(tmp_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute("INSERT INTO fleet_lifecycle_ledger (target_type, target_name, action, reason) "
                 "VALUES ('job','universe-refresh','revive','test')")
    conn.commit()
    conn.close()
    with patch.object(sentinel, "DB_PATH", db_path), \
         patch.object(sentinel, "_resolve_job_label", return_value="com.ollietrades.universe-refresh"), \
         patch("subprocess.run", return_value=_fake_run('"com.ollietrades.universe-refresh" => disabled\n')):
        alerts: list = []
        result = sentinel.check_fleet_lifecycle_drift(alerts)
    assert len(result["job_drift"]) == 1
    assert result["job_drift"][0]["name"] == "universe-refresh"
    assert any(a[1] == "sentinel_lifecycle_drift" for a in alerts)


def test_job_no_drift_when_states_agree(tmp_path):
    db_path = _make_db(tmp_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute("INSERT INTO fleet_lifecycle_ledger (target_type, target_name, action, reason) "
                 "VALUES ('job','universe-refresh','revive','test')")
    conn.commit()
    conn.close()
    with patch.object(sentinel, "DB_PATH", db_path), \
         patch.object(sentinel, "_resolve_job_label", return_value="com.ollietrades.universe-refresh"), \
         patch("subprocess.run", return_value=_fake_run("")):
        alerts: list = []
        result = sentinel.check_fleet_lifecycle_drift(alerts)
    assert result["job_drift"] == []
    assert alerts == []


def test_job_unresolvable_label_not_flagged(tmp_path):
    """The documented 'crew' orphan -- no plist exists, can't verify, must
    not guess a wrong label and false-positive."""
    db_path = _make_db(tmp_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute("INSERT INTO fleet_lifecycle_ledger (target_type, target_name, action, reason, backfilled) "
                 "VALUES ('job','crew','retire','orphan test',1)")
    conn.commit()
    conn.close()
    with patch.object(sentinel, "DB_PATH", db_path), \
         patch.object(sentinel, "_resolve_job_label", return_value=None), \
         patch("subprocess.run", return_value=_fake_run("")):
        alerts: list = []
        result = sentinel.check_fleet_lifecycle_drift(alerts)
    assert result["job_drift"] == []
    assert alerts == []


def test_agent_drift_detected(tmp_path):
    db_path = _make_db(tmp_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute("INSERT INTO ai_players (id, halt_mode) VALUES ('quark-ic', 'active')")
    conn.execute("INSERT INTO fleet_lifecycle_ledger (target_type, target_name, action, reason) "
                 "VALUES ('agent','quark-ic','retire','test')")
    conn.commit()
    conn.close()
    with patch.object(sentinel, "DB_PATH", db_path), \
         patch("subprocess.run", return_value=_fake_run("")):
        alerts: list = []
        result = sentinel.check_fleet_lifecycle_drift(alerts)
    assert len(result["agent_drift"]) == 1
    assert result["agent_drift"][0]["name"] == "quark-ic"
    assert result["agent_drift"][0]["expected_halt_mode"] == "full"
    assert result["agent_drift"][0]["live_halt_mode"] == "active"
    assert any(a[1] == "sentinel_lifecycle_drift" for a in alerts)


def test_agent_removed_from_roster_not_flagged(tmp_path):
    """A ledger entry for an agent that no longer has an ai_players row
    (e.g. a very old backfilled entry) is not a drift finding."""
    db_path = _make_db(tmp_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute("INSERT INTO fleet_lifecycle_ledger (target_type, target_name, action, reason) "
                 "VALUES ('agent','long-gone-agent','halt','test')")
    conn.commit()
    conn.close()
    with patch.object(sentinel, "DB_PATH", db_path), \
         patch("subprocess.run", return_value=_fake_run("")):
        alerts: list = []
        result = sentinel.check_fleet_lifecycle_drift(alerts)
    assert result["agent_drift"] == []


def test_overdue_review_by_fires(tmp_path):
    db_path = _make_db(tmp_path)
    conn = sqlite3.connect(str(db_path))
    past = (date.today() - timedelta(days=5)).isoformat()
    conn.execute("INSERT INTO fleet_lifecycle_ledger (target_type, target_name, action, reason, review_by) "
                 "VALUES ('job','crusher','halt','test',?)", (past,))
    conn.commit()
    conn.close()
    with patch.object(sentinel, "DB_PATH", db_path), \
         patch.object(sentinel, "_resolve_job_label", return_value=None), \
         patch("subprocess.run", return_value=_fake_run("")):
        alerts: list = []
        result = sentinel.check_fleet_lifecycle_drift(alerts)
    assert len(result["overdue"]) == 1
    assert result["overdue"][0]["name"] == "crusher"
    assert any(a[1] == "sentinel_lifecycle_review_overdue" for a in alerts)


def test_review_by_in_future_not_overdue(tmp_path):
    db_path = _make_db(tmp_path)
    conn = sqlite3.connect(str(db_path))
    future = (date.today() + timedelta(days=30)).isoformat()
    conn.execute("INSERT INTO fleet_lifecycle_ledger (target_type, target_name, action, reason, review_by) "
                 "VALUES ('job','crusher','halt','test',?)", (future,))
    conn.commit()
    conn.close()
    with patch.object(sentinel, "DB_PATH", db_path), \
         patch.object(sentinel, "_resolve_job_label", return_value=None), \
         patch("subprocess.run", return_value=_fake_run("")):
        alerts: list = []
        result = sentinel.check_fleet_lifecycle_drift(alerts)
    assert result["overdue"] == []
    assert alerts == []


def test_launchd_jobs_health_skips_paused_targets(tmp_path):
    db_path = _make_db(tmp_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute("INSERT INTO fleet_lifecycle_ledger (target_type, target_name, action, reason) "
                 "VALUES ('job','universe-refresh','halt','intentionally paused')")
    conn.commit()
    conn.close()
    registry = {"universe-refresh": ("nonexistent.log", 1.0)}
    with patch.object(sentinel, "DB_PATH", db_path), \
         patch.object(sentinel, "LAUNCHD_JOB_REGISTRY", registry):
        alerts: list = []
        result = sentinel.check_launchd_jobs_health(alerts)
    assert result["checked"] == 0
    assert result["skipped_by_ledger"] == ["universe-refresh"]
    assert alerts == []
