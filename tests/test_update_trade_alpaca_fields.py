"""HM-SILENT-CATCH-SWEEP (2026-07-07) — _update_trade_alpaca_fields regression test.

Real bug found during the sweep: `UPDATE trades ... ORDER BY executed_at DESC
LIMIT 1` is NOT valid SQLite syntax without the SQLITE_ENABLE_UPDATE_DELETE_LIMIT
compile flag (not present in Python's bundled sqlite3) -- this statement raised
`OperationalError: near "ORDER": syntax error` on every single call since it
was written, silently swallowed by a bare `except Exception: pass`. Confirmed
empirically against the live DB: zero trades rows anywhere had
alpaca_status='submitted'. Fixed with the standard SQLite "target the most
recent matching row via a rowid subquery" idiom -- this test proves the fix
targets exactly the intended row and would fail loudly (not silently) if the
syntax regressed.
"""
from __future__ import annotations

import sqlite3
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine import paper_trader as pt  # noqa: E402


def _make_trades_db(path):
    conn = sqlite3.connect(path)
    conn.execute("""CREATE TABLE trades (
        id INTEGER PRIMARY KEY,
        player_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        action TEXT NOT NULL,
        alpaca_order_id TEXT,
        alpaca_status TEXT,
        execution_type TEXT DEFAULT 'simulated',
        executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.commit()
    conn.close()


def test_update_trade_alpaca_fields_targets_most_recent_matching_row():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = str(Path(tmp) / "test_trader.db")
        _make_trades_db(db_path)
        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO trades (player_id, symbol, action, executed_at) "
            "VALUES ('p','SYM','BUY','2026-01-01T00:00:00')"
        )
        conn.execute(
            "INSERT INTO trades (player_id, symbol, action, executed_at) "
            "VALUES ('p','SYM','BUY','2026-01-02T00:00:00')"
        )  # more recent -- this one should get updated
        conn.execute(
            "INSERT INTO trades (player_id, symbol, action, executed_at) "
            "VALUES ('p','OTHER','BUY','2026-01-03T00:00:00')"
        )  # different symbol -- must NOT be touched
        conn.commit()
        conn.close()

        with patch.object(pt, "DB", db_path):
            pt._update_trade_alpaca_fields("p", "SYM", "oid123", "alpaca_paper")

        conn = sqlite3.connect(db_path)
        rows = conn.execute(
            "SELECT symbol, executed_at, alpaca_order_id, alpaca_status, execution_type "
            "FROM trades ORDER BY id"
        ).fetchall()
        conn.close()

    assert rows[0] == ("SYM", "2026-01-01T00:00:00", None, None, "simulated"), (
        "older SYM trade must be untouched"
    )
    assert rows[1] == ("SYM", "2026-01-02T00:00:00", "oid123", "submitted", "alpaca_paper"), (
        "the MOST RECENT matching trade must get the Alpaca fields stamped -- "
        "this is the exact statement that silently failed on every call before "
        "the fix (invalid `UPDATE ... ORDER BY ... LIMIT` syntax)"
    )
    assert rows[2] == ("OTHER", "2026-01-03T00:00:00", None, None, "simulated"), (
        "different-symbol trade must never be touched"
    )


def test_update_trade_alpaca_fields_no_match_is_a_silent_noop():
    """No matching trade (e.g. race with the INSERT) must not raise -- 0 rows
    affected is a legitimate outcome, not an error."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = str(Path(tmp) / "test_trader.db")
        _make_trades_db(db_path)
        with patch.object(pt, "DB", db_path):
            pt._update_trade_alpaca_fields("nobody", "NOPE", "oid", "alpaca_paper")
        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        conn.close()
    assert count == 0
