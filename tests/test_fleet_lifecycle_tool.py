"""tests/test_fleet_lifecycle_tool.py — scripts/fleet_lifecycle.py,
HM-FLEET-LIFECYCLE-2026-08-29.

Exercises the CLI's validation, atomicity (order doc + live change +
ledger row all together, or none of them), and target-type auto-detection
against a temp DB and mocked launchd/plist filesystem -- never touches
the real trader.db or real launchctl.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts import fleet_lifecycle as fl  # noqa: E402


def _make_db(tmp_path: Path, agents: list[str] | None = None) -> Path:
    db_path = tmp_path / "trader.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""CREATE TABLE ai_players (
        id TEXT PRIMARY KEY, halt_mode TEXT, halt_reason TEXT, halted_at TEXT)""")
    for a in (agents or []):
        conn.execute("INSERT INTO ai_players (id, halt_mode) VALUES (?, 'active')", (a,))
    conn.execute("""CREATE TABLE fleet_lifecycle_ledger (
        id INTEGER PRIMARY KEY AUTOINCREMENT, target_type TEXT, target_name TEXT,
        action TEXT, reason TEXT, order_doc TEXT, resume_by TEXT, review_by TEXT,
        backfilled INTEGER DEFAULT 0, created_at TEXT DEFAULT (datetime('now')),
        created_by TEXT DEFAULT 'test')""")
    # HM-FLEET-LIFECYCLE-REVIVE-RATING-2026-09-01: minimal agent_ratings shape,
    # matches the real schema's columns _ensure_fresh_rating_on_resume writes.
    conn.execute("""CREATE TABLE agent_ratings (
        id INTEGER PRIMARY KEY AUTOINCREMENT, player_id TEXT, period TEXT,
        total_trades INTEGER, wins INTEGER, losses INTEGER, win_rate REAL,
        total_pnl REAL, avg_win REAL, avg_loss REAL, profit_factor REAL,
        best_trade REAL, worst_trade REAL, consecutive_losses INTEGER,
        consecutive_wins INTEGER, avg_confidence REAL, pass_rate REAL,
        volume_accuracy REAL, rating TEXT, rating_score REAL,
        timestamp TEXT DEFAULT (datetime('now')))""")
    conn.commit()
    conn.close()
    return db_path


def _patch_never_rated(**kw):
    """Default mock for calculate_rating in tests that exercise a
    RESUME_ACTION but don't care about the rating recompute specifically --
    keeps every existing revive/active test hermetic (no real DB touch)
    without changing what they're actually testing. Returns N/A, same as
    a freshly-revived agent with no current-season trades yet."""
    return patch("engine.agent_ratings.calculate_rating",
                 return_value={"rating": "N/A", "rating_score": 0.0}, **kw)


class _Args:
    def __init__(self, **kw):
        self.type = None
        self.resume_by = None
        self.review_by = None
        self.dry_run = False
        self.__dict__.update(kw)


def test_agent_halt_writes_order_doc_ledger_and_db(tmp_path):
    db_path = _make_db(tmp_path, agents=["quark-ic"])
    orders_dir = tmp_path / "orders"
    with patch.object(fl, "DB_PATH", db_path), patch.object(fl, "ORDERS_DIR", orders_dir):
        rc = fl.cmd_apply(_Args(action="halt", name="quark-ic",
                                 reason="test halt", review_by="2026-12-31"))
    assert rc == 0
    docs = list(orders_dir.glob("*.md"))
    assert len(docs) == 1
    assert "test halt" in docs[0].read_text()
    assert "Review-by: 2026-12-31" in docs[0].read_text()

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM ai_players WHERE id='quark-ic'").fetchone()
    assert row["halt_mode"] == "full"
    assert "HALT" in row["halt_reason"]

    ledger = conn.execute("SELECT * FROM fleet_lifecycle_ledger WHERE target_name='quark-ic'").fetchone()
    assert ledger["action"] == "halt"
    assert ledger["review_by"] == "2026-12-31"
    assert ledger["backfilled"] == 0


def test_agent_revive_clears_halt_reason(tmp_path):
    db_path = _make_db(tmp_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute("INSERT INTO ai_players (id, halt_mode, halt_reason) VALUES ('quark-ic', 'full', 'old reason')")
    conn.commit()
    conn.close()
    orders_dir = tmp_path / "orders"
    with patch.object(fl, "DB_PATH", db_path), patch.object(fl, "ORDERS_DIR", orders_dir), \
         _patch_never_rated():
        rc = fl.cmd_apply(_Args(action="revive", name="quark-ic", reason="passing audition"))
    assert rc == 0
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM ai_players WHERE id='quark-ic'").fetchone()
    assert row["halt_mode"] == "active"
    assert row["halt_reason"] is None


def test_pause_action_requires_a_date(tmp_path):
    db_path = _make_db(tmp_path, agents=["quark-ic"])
    orders_dir = tmp_path / "orders"
    with patch.object(fl, "DB_PATH", db_path), patch.object(fl, "ORDERS_DIR", orders_dir):
        rc = fl.cmd_apply(_Args(action="halt", name="quark-ic", reason="test"))
    assert rc == 1
    assert list(orders_dir.glob("*.md")) == []  # refused before writing anything


def test_blank_reason_rejected(tmp_path):
    db_path = _make_db(tmp_path, agents=["quark-ic"])
    orders_dir = tmp_path / "orders"
    with patch.object(fl, "DB_PATH", db_path), patch.object(fl, "ORDERS_DIR", orders_dir):
        rc = fl.cmd_apply(_Args(action="revive", name="quark-ic", reason="   "))
    assert rc == 1


def test_bench_rejected_for_job_target_type(tmp_path):
    db_path = _make_db(tmp_path)
    orders_dir = tmp_path / "orders"
    with patch.object(fl, "DB_PATH", db_path), patch.object(fl, "ORDERS_DIR", orders_dir), \
         patch.object(fl, "_resolve_job_label", return_value="com.ollietrades.some-job"):
        rc = fl.cmd_apply(_Args(action="bench", name="some-job", type="job",
                                 reason="test", review_by="2026-12-31"))
    assert rc == 1
    assert list(orders_dir.glob("*.md")) == []


def test_unknown_target_without_type_errors(tmp_path):
    db_path = _make_db(tmp_path)
    with patch.object(fl, "DB_PATH", db_path), \
         patch.object(fl, "LAUNCHAGENTS_DIR", tmp_path / "nonexistent"):
        rc = fl.cmd_apply(_Args(action="halt", name="nope", reason="test", review_by="2026-12-31"))
    assert rc == 1


def test_job_live_change_failure_marks_doc_failed_no_ledger_row(tmp_path):
    db_path = _make_db(tmp_path)
    orders_dir = tmp_path / "orders"
    la_dir = tmp_path / "LaunchAgents"
    la_dir.mkdir()
    (la_dir / "com.ollietrades.flaky-job.plist").write_text("<plist/>")
    with patch.object(fl, "DB_PATH", db_path), patch.object(fl, "ORDERS_DIR", orders_dir), \
         patch.object(fl, "LAUNCHAGENTS_DIR", la_dir), \
         patch("subprocess.run", side_effect=RuntimeError("launchctl exploded")):
        rc = fl.cmd_apply(_Args(action="revive", name="flaky-job", type="job", reason="test"))
    assert rc == 1
    docs = list(orders_dir.glob("*.md"))
    assert len(docs) == 1
    assert "FAILED" in docs[0].read_text()
    conn = sqlite3.connect(str(db_path))
    n = conn.execute("SELECT COUNT(*) FROM fleet_lifecycle_ledger").fetchone()[0]
    assert n == 0  # no ledger row for a change that didn't actually happen


def test_dry_run_touches_nothing(tmp_path):
    db_path = _make_db(tmp_path, agents=["quark-ic"])
    orders_dir = tmp_path / "orders"
    with patch.object(fl, "DB_PATH", db_path), patch.object(fl, "ORDERS_DIR", orders_dir):
        rc = fl.cmd_apply(_Args(action="revive", name="quark-ic", reason="test", dry_run=True))
    assert rc == 0
    assert list(orders_dir.glob("*.md")) == []
    conn = sqlite3.connect(str(db_path))
    assert conn.execute("SELECT COUNT(*) FROM fleet_lifecycle_ledger").fetchone()[0] == 0
    row = conn.execute("SELECT halt_mode FROM ai_players WHERE id='quark-ic'").fetchone()
    assert row[0] == "active"  # never touched


def test_retire_action_writes_tombstone_language(tmp_path):
    db_path = _make_db(tmp_path, agents=["mccoy-bps"])
    orders_dir = tmp_path / "orders"
    with patch.object(fl, "DB_PATH", db_path), patch.object(fl, "ORDERS_DIR", orders_dir):
        rc = fl.cmd_apply(_Args(action="retire", name="mccoy-bps", reason="never profitable"))
    assert rc == 0
    doc = list(orders_dir.glob("*.md"))[0].read_text()
    assert "TOMBSTONE" in doc
    assert "permanent" in doc.lower()


def test_status_reports_latest_ledger_row(tmp_path):
    db_path = _make_db(tmp_path, agents=["quark-ic"])
    orders_dir = tmp_path / "orders"
    with patch.object(fl, "DB_PATH", db_path), patch.object(fl, "ORDERS_DIR", orders_dir), \
         _patch_never_rated():
        fl.cmd_apply(_Args(action="halt", name="quark-ic", reason="first", review_by="2026-09-01"))
        fl.cmd_apply(_Args(action="revive", name="quark-ic", reason="second"))
        rc = fl.cmd_status(_Args(name="quark-ic"))
    assert rc == 0


# ── HM-FLEET-LIFECYCLE-REVIVE-RATING-2026-09-01 ─────────────────────────────
# fleet_lifecycle.py's revive path never touched agent_ratings at all --
# combined with the BENCH gate's 30-day staleness fail-open (0730aec, same
# day) and its pre-existing "never-rated fails open" rule, a revived D/E
# agent or a never-rated agent both landed back trading with zero
# rating-based protection. Fail CLOSED instead.

def test_revive_uses_real_rating_when_recompute_succeeds(tmp_path):
    """A revived agent that already has enough clean current-season trades
    (calculate_rating returns a real grade, not N/A) needs no synthetic
    override -- the real computed rating is what should govern."""
    db_path = _make_db(tmp_path, agents=["quark-ic"])
    orders_dir = tmp_path / "orders"
    with patch.object(fl, "DB_PATH", db_path), patch.object(fl, "ORDERS_DIR", orders_dir), \
         patch("engine.agent_ratings.calculate_rating",
                return_value={"rating": "B", "rating_score": 75.0}):
        rc = fl.cmd_apply(_Args(action="revive", name="quark-ic", reason="test"))
    assert rc == 0
    conn = sqlite3.connect(str(db_path))
    # No synthetic probation row inserted -- calculate_rating (mocked here,
    # real in production) is responsible for its own insert when it computes
    # a genuine rating; this tool must not duplicate or override that.
    n = conn.execute("SELECT COUNT(*) FROM agent_ratings WHERE player_id='quark-ic'").fetchone()[0]
    assert n == 0


def test_revive_inserts_probation_row_when_never_rated(tmp_path):
    """No current-season trades yet (the normal case right after a revive)
    -- calculate_rating returns N/A and (confirmed by reading it) never
    inserts a row in that case. This tool must fail closed: insert a
    conservative, clearly-synthetic D/0 probation row so the BENCH gate has
    fresh data to block new entries on."""
    db_path = _make_db(tmp_path, agents=["quark-ic"])
    orders_dir = tmp_path / "orders"
    with patch.object(fl, "DB_PATH", db_path), patch.object(fl, "ORDERS_DIR", orders_dir), \
         _patch_never_rated():
        rc = fl.cmd_apply(_Args(action="revive", name="quark-ic", reason="test"))
    assert rc == 0
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM agent_ratings WHERE player_id='quark-ic'").fetchone()
    assert row is not None
    assert row["rating"] == "D"
    assert row["rating_score"] == 0.0
    assert row["period"] == "alltime"


def test_revive_inserts_probation_row_when_recompute_raises(tmp_path):
    """A rating-system hiccup during recompute must never block a
    legitimate revive, but it must also never silently skip the probation
    insert -- fail closed even when the recompute attempt itself errors."""
    db_path = _make_db(tmp_path, agents=["quark-ic"])
    orders_dir = tmp_path / "orders"
    with patch.object(fl, "DB_PATH", db_path), patch.object(fl, "ORDERS_DIR", orders_dir), \
         patch("engine.agent_ratings.calculate_rating",
                side_effect=RuntimeError("db locked")):
        rc = fl.cmd_apply(_Args(action="revive", name="quark-ic", reason="test"))
    assert rc == 0  # the revive itself must still succeed
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM agent_ratings WHERE player_id='quark-ic'").fetchone()
    assert row is not None
    assert row["rating"] == "D"


def test_active_action_also_gets_rating_check(tmp_path):
    """RESUME_ACTIONS = {'revive', 'active'} -- both re-enter an agent into
    a state where it could trade, both must get the same protection."""
    db_path = _make_db(tmp_path, agents=["quark-ic"])
    orders_dir = tmp_path / "orders"
    with patch.object(fl, "DB_PATH", db_path), patch.object(fl, "ORDERS_DIR", orders_dir), \
         _patch_never_rated():
        rc = fl.cmd_apply(_Args(action="active", name="quark-ic", reason="test"))
    assert rc == 0
    conn = sqlite3.connect(str(db_path))
    n = conn.execute("SELECT COUNT(*) FROM agent_ratings WHERE player_id='quark-ic'").fetchone()[0]
    assert n == 1


def test_halt_action_does_not_touch_ratings(tmp_path):
    """A pause-type action (halt/bench/shadow) isn't re-entering the agent
    into trading -- must not touch agent_ratings at all."""
    db_path = _make_db(tmp_path, agents=["quark-ic"])
    orders_dir = tmp_path / "orders"
    with patch.object(fl, "DB_PATH", db_path), patch.object(fl, "ORDERS_DIR", orders_dir), \
         patch("engine.agent_ratings.calculate_rating") as mock_calc:
        rc = fl.cmd_apply(_Args(action="halt", name="quark-ic", reason="test",
                                 review_by="2026-12-31"))
    assert rc == 0
    mock_calc.assert_not_called()
    conn = sqlite3.connect(str(db_path))
    n = conn.execute("SELECT COUNT(*) FROM agent_ratings").fetchone()[0]
    assert n == 0
