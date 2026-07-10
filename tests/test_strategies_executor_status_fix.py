"""HM-STRATEGIES-EXECUTOR-STATUS-NEVER-SET 2026-07-10.

strategies/executor.py::_increment_closed() is the only live close path for
bull_spread_v1 / bull_call_spread_v1 / bear_put_spread_v1 (scheduled every
tick in main.py) and never set `status='closed'` -- only `exec_status`.
Every P&L/win-rate query in the system filters on `status='closed'`, so a
position closed via this path was permanently invisible to reporting. This
covers the fix: status now flips to 'closed' (alongside exec_status) exactly
when the position is fully closed, and stays 'open' on a partial close.

pnl/exit_credit_debit computation is deliberately NOT covered here -- see
docs/XO_BACKLOG.md HM-STRATEGIES-EXECUTOR-STATUS-NEVER-SET for why (MLEG
close fill-price sign convention is unverified; pnl staying NULL is
existing, tolerated behavior, not a regression).
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _build_db(tmp_path, contracts=2, contracts_closed_so_far=0):
    db_path = tmp_path / "test_trader.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""CREATE TABLE options_trades (
        id INTEGER PRIMARY KEY,
        book_tag TEXT DEFAULT 'fleet',
        agent_id TEXT, structure TEXT, symbol TEXT,
        status TEXT DEFAULT 'open',
        exec_status TEXT DEFAULT 'pending',
        contracts INTEGER DEFAULT 1,
        contracts_closed_so_far INTEGER DEFAULT 0,
        exit_date TEXT, exit_reason TEXT, pnl REAL, exit_credit_debit REAL
    )""")
    conn.execute(
        "INSERT INTO options_trades (id, agent_id, structure, symbol, status, "
        "exec_status, contracts, contracts_closed_so_far) "
        "VALUES (1, 'strategy:bull_spread_v1', 'bull_put_spread', 'ABC', 'open', "
        "'pending', ?, ?)",
        (contracts, contracts_closed_so_far),
    )
    conn.commit()
    conn.close()
    return db_path


def test_full_close_sets_status_closed_and_exit_date(tmp_path):
    import strategies.executor as ex

    db_path = _build_db(tmp_path, contracts=2, contracts_closed_so_far=0)
    with patch.object(ex, "DB_PATH", db_path):
        ex._increment_closed(position_id=1, count=2, reason="test_full_close")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM options_trades WHERE id=1").fetchone()
    conn.close()

    assert row["status"] == "closed"
    assert row["exec_status"] == "closed"
    assert row["exit_date"] is not None
    assert row["exit_reason"] == "test_full_close"
    # pnl is deliberately untouched by this fix -- still NULL.
    assert row["pnl"] is None


def test_partial_close_leaves_status_open(tmp_path):
    import strategies.executor as ex

    db_path = _build_db(tmp_path, contracts=4, contracts_closed_so_far=0)
    with patch.object(ex, "DB_PATH", db_path):
        ex._increment_closed(position_id=1, count=1, reason="test_partial_close")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM options_trades WHERE id=1").fetchone()
    conn.close()

    assert row["status"] == "open"
    assert row["exec_status"] == "open"
    assert row["exit_date"] is None
    assert row["contracts_closed_so_far"] == 1


def test_second_partial_close_completes_and_sets_status_closed(tmp_path):
    import strategies.executor as ex

    db_path = _build_db(tmp_path, contracts=4, contracts_closed_so_far=3)
    with patch.object(ex, "DB_PATH", db_path):
        ex._increment_closed(position_id=1, count=1, reason="final_leg")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM options_trades WHERE id=1").fetchone()
    conn.close()

    assert row["status"] == "closed"
    assert row["exec_status"] == "closed"
    assert row["contracts_closed_so_far"] == 4
