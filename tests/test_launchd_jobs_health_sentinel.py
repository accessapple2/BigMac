"""tests/test_launchd_jobs_health_sentinel.py — freshness coverage for the
18 com.ollietrades.* LaunchAgents reactivated 2026-08-29 after the
2026-07-22 stand-down.

check_launchd_jobs_health() is purely a staleness check now (each job's
log mtime vs. a cadence-scaled ceiling) -- it skips any target the
fleet_lifecycle_ledger's latest entry marks halt/bench/shadow/retire, on
the theory that an intentionally-off job going log-stale is not a
finding, it's the plan working. Whether the *live* launchd state actually
matches the ledger is check_fleet_lifecycle_drift's job (see
tests/test_fleet_lifecycle_drift_sentinel.py), not this file's.
"""
from __future__ import annotations

import sqlite3
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts import hm_ops_sentinel as sentinel  # noqa: E402


def _make_ledger_db(tmp_path: Path, rows: list[tuple[str, str]]) -> Path:
    """rows: list of (target_name, action) for target_type='job'."""
    db_path = tmp_path / "trader.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""CREATE TABLE fleet_lifecycle_ledger (
        id INTEGER PRIMARY KEY AUTOINCREMENT, target_type TEXT, target_name TEXT,
        action TEXT, reason TEXT, order_doc TEXT, resume_by TEXT, review_by TEXT,
        backfilled INTEGER DEFAULT 0, created_at TEXT DEFAULT (datetime('now')),
        created_by TEXT DEFAULT 'test')""")
    for name, action in rows:
        conn.execute("INSERT INTO fleet_lifecycle_ledger (target_type, target_name, action, reason) "
                     "VALUES ('job', ?, ?, 'test')", (name, action))
    conn.commit()
    conn.close()
    return db_path


def test_all_healthy_no_alerts():
    with tempfile.TemporaryDirectory() as d:
        registry = {}
        for label, (rel, ceiling) in sentinel.LAUNCHD_JOB_REGISTRY.items():
            p = Path(d) / f"{label}.log"
            p.write_text("ok\n")
            registry[label] = (f"{label}.log", ceiling)
        db_path = _make_ledger_db(Path(d), [(label, "revive") for label in registry])
        with patch.object(sentinel, "ROOT", Path(d)), \
             patch.object(sentinel, "DB_PATH", db_path), \
             patch.object(sentinel, "LAUNCHD_JOB_REGISTRY", registry):
            alerts: list = []
            result = sentinel.check_launchd_jobs_health(alerts)
        assert result["stale"] == []
        assert result["skipped_by_ledger"] == []
        assert alerts == []


def test_ledger_halted_target_skipped_not_stale():
    """A job the ledger says is intentionally halted must never be flagged
    stale just because its log hasn't been touched -- that's the plan
    working, not a failure."""
    with tempfile.TemporaryDirectory() as d:
        registry = {"crusher": ("crusher.log", 1.0)}  # tiny ceiling, would trip immediately if not skipped
        db_path = _make_ledger_db(Path(d), [("crusher", "halt")])
        with patch.object(sentinel, "ROOT", Path(d)), \
             patch.object(sentinel, "DB_PATH", db_path), \
             patch.object(sentinel, "LAUNCHD_JOB_REGISTRY", registry):
            alerts: list = []
            result = sentinel.check_launchd_jobs_health(alerts)
        assert result["checked"] == 0
        assert result["skipped_by_ledger"] == ["crusher"]
        assert result["stale"] == []
        assert alerts == []


def test_stale_log_fires():
    with tempfile.TemporaryDirectory() as d:
        registry = {"nightly-backtest": ("nightly_backtest.log", 30.0)}
        p = Path(d) / "nightly_backtest.log"
        p.write_text("old\n")
        old_time = time.time() - (40 * 3600)  # 40h old, past the 30h ceiling
        import os
        os.utime(p, (old_time, old_time))
        db_path = _make_ledger_db(Path(d), [("nightly-backtest", "revive")])
        with patch.object(sentinel, "ROOT", Path(d)), \
             patch.object(sentinel, "DB_PATH", db_path), \
             patch.object(sentinel, "LAUNCHD_JOB_REGISTRY", registry):
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
        db_path = _make_ledger_db(Path(d), [("scotty", "revive")])
        with patch.object(sentinel, "ROOT", Path(d)), \
             patch.object(sentinel, "DB_PATH", db_path), \
             patch.object(sentinel, "_ALT_LOG_ROOT", Path(d) / "nonexistent"), \
             patch.object(sentinel, "LAUNCHD_JOB_REGISTRY", registry):
            alerts: list = []
            result = sentinel.check_launchd_jobs_health(alerts)
        assert result["stale"] == []
        assert alerts == []


def test_no_ledger_entry_defaults_to_checked():
    """A registered job with no ledger row at all (not yet backfilled)
    still gets staleness coverage -- fail toward checking, not silence."""
    with tempfile.TemporaryDirectory() as d:
        registry = {"scotty": ("scotty.out.log", 48.0)}
        db_path = _make_ledger_db(Path(d), [])  # empty ledger
        with patch.object(sentinel, "ROOT", Path(d)), \
             patch.object(sentinel, "DB_PATH", db_path), \
             patch.object(sentinel, "_ALT_LOG_ROOT", Path(d) / "nonexistent"), \
             patch.object(sentinel, "LAUNCHD_JOB_REGISTRY", registry):
            alerts: list = []
            result = sentinel.check_launchd_jobs_health(alerts)
        assert result["checked"] == 1
        assert result["skipped_by_ledger"] == []


def test_mass_outage_fires_when_half_or_more_go_quiet_together():
    """HM-GUI-DOMAIN-OUTAGE-2026-09-04: a WindowServer crash can unload
    every LaunchAgent at once -- individual ceilings (30h-216h) are too
    slow to catch that quickly. A uniform, ceiling-independent window
    should fire a single RED_ALERT once at least half the registry has
    gone quiet inside it, regardless of any one job's own ceiling."""
    with tempfile.TemporaryDirectory() as d:
        # 5 jobs, all with generous ceilings that individually would NOT
        # yet flag "stale" -- but all five have been silent 7h, past the
        # 6h mass-outage window (and past MASS_OUTAGE_MIN_COUNT=5).
        registry = {
            "universe-refresh": ("universe-refresh.log", 216.0),
            "model-watcher": ("model-watcher.log", 216.0),
            "uhura-watch": ("uhura-watch.log", 192.0),
            "scotty": ("scotty.log", 48.0),
            "danelfin-update": ("danelfin-update.log", 216.0),
        }
        quiet_time = time.time() - (7 * 3600)
        import os
        for label, (rel, _) in registry.items():
            p = Path(d) / rel
            p.write_text("old\n")
            os.utime(p, (quiet_time, quiet_time))
        db_path = _make_ledger_db(Path(d), [(label, "revive") for label in registry])
        with patch.object(sentinel, "ROOT", Path(d)), \
             patch.object(sentinel, "DB_PATH", db_path), \
             patch.object(sentinel, "LAUNCHD_JOB_REGISTRY", registry):
            alerts: list = []
            result = sentinel.check_launchd_jobs_health(alerts)
        assert result["stale"] == []  # none crossed their OWN ceiling
        assert result["mass_outage"] is not None
        assert result["mass_outage"]["quiet_count"] == 5
        mass_alerts = [a for a in alerts if a[1] == "sentinel_launchd_mass_outage"]
        assert len(mass_alerts) == 1
        assert mass_alerts[0][0] == "red_alert"


def test_a_few_individually_quiet_jobs_do_not_trigger_mass_outage():
    """Ordinary partial staleness (one or two jobs, not the whole fleet)
    must stay a per-job WARNING, not escalate to the mass-outage RED_ALERT."""
    with tempfile.TemporaryDirectory() as d:
        registry = {
            "nightly-backtest": ("nightly_backtest.log", 30.0),
            "uhura-watch": ("uhura_watch.log", 192.0),
            "scotty": ("scotty.log", 48.0),
            "model-watcher": ("model_watcher.log", 216.0),
        }
        import os
        now = time.time()
        for label, (rel, _) in registry.items():
            p = Path(d) / rel
            p.write_text("x\n")
            os.utime(p, (now, now))  # everyone fresh...
        # ...except one, past ITS OWN ceiling but alone, not a mass event.
        stale_path = Path(d) / "nightly_backtest.log"
        old_time = now - (40 * 3600)
        os.utime(stale_path, (old_time, old_time))
        db_path = _make_ledger_db(Path(d), [(label, "revive") for label in registry])
        with patch.object(sentinel, "ROOT", Path(d)), \
             patch.object(sentinel, "DB_PATH", db_path), \
             patch.object(sentinel, "LAUNCHD_JOB_REGISTRY", registry):
            alerts: list = []
            result = sentinel.check_launchd_jobs_health(alerts)
        assert len(result["stale"]) == 1
        assert result["mass_outage"] is None
        assert [a for a in alerts if a[1] == "sentinel_launchd_mass_outage"] == []


def test_small_registry_does_not_trigger_mass_outage_on_fraction_alone():
    """A single stale job out of a tiny (e.g. 1-job) registry is trivially
    "100% quiet" by fraction -- MASS_OUTAGE_MIN_COUNT must stop that from
    reading as a fleet-wide outage. Real registry is 15-17 jobs; this only
    matters for degenerate small-N cases like a scoped test or a
    near-empty ledger."""
    with tempfile.TemporaryDirectory() as d:
        registry = {"nightly-backtest": ("nightly_backtest.log", 30.0)}
        p = Path(d) / "nightly_backtest.log"
        p.write_text("old\n")
        import os
        old_time = time.time() - (40 * 3600)
        os.utime(p, (old_time, old_time))
        db_path = _make_ledger_db(Path(d), [("nightly-backtest", "revive")])
        with patch.object(sentinel, "ROOT", Path(d)), \
             patch.object(sentinel, "DB_PATH", db_path), \
             patch.object(sentinel, "LAUNCHD_JOB_REGISTRY", registry):
            alerts: list = []
            result = sentinel.check_launchd_jobs_health(alerts)
        assert result["mass_outage"] is None
        assert [a for a in alerts if a[1] == "sentinel_launchd_mass_outage"] == []


def test_db_unreadable_does_not_crash():
    with tempfile.TemporaryDirectory() as d:
        registry = {"scotty": ("scotty.out.log", 48.0)}
        with patch.object(sentinel, "ROOT", Path(d)), \
             patch.object(sentinel, "DB_PATH", Path(d) / "does_not_exist" / "trader.db"), \
             patch.object(sentinel, "_ALT_LOG_ROOT", Path(d) / "nonexistent"), \
             patch.object(sentinel, "LAUNCHD_JOB_REGISTRY", registry):
            alerts: list = []
            result = sentinel.check_launchd_jobs_health(alerts)
        assert result["checked"] == 1
        assert alerts == []
