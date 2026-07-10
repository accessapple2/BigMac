"""HM-SWINGDESK-CLOSE-PHANTOM-ROW 2026-07-10.

swingdesk/spread_executor.py::submit_spread(action='close') used to fall
through to _persist() unconditionally -- the same function used to record a
new OPEN position -- creating a brand-new row for the close order instead of
closing the original. The original stayed status='open' forever (invisible
to every P&L/win-rate query) and a second row appeared, indistinguishable
from a fresh position. Confirmed against real 2026-06-11 data (options_trades
ids 93-95: three near-identical CEG bear_put_spread rows from repeated
manual test submissions, one of which is still status='open' despite
exec_status='expired').

Covers the fix at the DB-mutation level (_find_open_position_id,
_close_original_position) rather than through the full submit_spread() ->
Alpaca order flow, since that requires a live/mocked broker client and the
bug lives entirely in local bookkeeping, not order submission.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _legs_json(strikes=(245.0, 230.0)):
    return json.dumps([
        {"symbol": f"CEG260710P{int(strikes[0] * 1000):08d}", "side": "long",
         "type": "put", "strike": strikes[0], "ratio_qty": 1},
        {"symbol": f"CEG260710P{int(strikes[1] * 1000):08d}", "side": "short",
         "type": "put", "strike": strikes[1], "ratio_qty": 1},
    ])


def _occ_symbols(strikes=(245.0, 230.0)):
    return [f"CEG260710P{int(strikes[0] * 1000):08d}",
            f"CEG260710P{int(strikes[1] * 1000):08d}"]


def _build_db(tmp_path):
    db_path = tmp_path / "test_trader.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""CREATE TABLE options_trades (
        id INTEGER PRIMARY KEY,
        book_tag TEXT DEFAULT 'fleet',
        agent_id TEXT, strategy_id TEXT, structure TEXT, symbol TEXT,
        legs_json TEXT,
        status TEXT DEFAULT 'open', exec_status TEXT DEFAULT 'pending',
        entry_date TEXT, exit_date TEXT, exit_reason TEXT,
        pnl REAL, exit_credit_debit REAL, broker_order_id TEXT
    )""")
    conn.commit()
    conn.close()
    return db_path


def test_find_open_position_id_matches_symbol_strategy_and_legs(tmp_path):
    import swingdesk.spread_executor as se

    db_path = _build_db(tmp_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT INTO options_trades (id, symbol, strategy_id, legs_json, status) "
        "VALUES (1, 'CEG', 'swingdesk_manual', ?, 'open')", (_legs_json(),))
    conn.commit()
    conn.close()

    with patch.object(se, "_DB", db_path):
        found = se._find_open_position_id("CEG", "swingdesk_manual", _occ_symbols())

    assert found == 1


def test_find_open_position_id_ignores_closed_rows(tmp_path):
    import swingdesk.spread_executor as se

    db_path = _build_db(tmp_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT INTO options_trades (id, symbol, strategy_id, legs_json, status) "
        "VALUES (1, 'CEG', 'swingdesk_manual', ?, 'closed')", (_legs_json(),))
    conn.commit()
    conn.close()

    with patch.object(se, "_DB", db_path):
        found = se._find_open_position_id("CEG", "swingdesk_manual", _occ_symbols())

    assert found is None


def test_find_open_position_id_returns_none_when_no_match(tmp_path):
    import swingdesk.spread_executor as se

    db_path = _build_db(tmp_path)
    with patch.object(se, "_DB", db_path):
        found = se._find_open_position_id("CEG", "swingdesk_manual", _occ_symbols())

    assert found is None


def test_find_open_position_id_picks_most_recent_of_duplicates(tmp_path):
    """Reproduces the real 2026-06-11 scenario (ids 93/94/95): the same
    spread submitted more than once, more than one row landing status='open'.
    Closing should hit the most recent, not an arbitrary/earliest one."""
    import swingdesk.spread_executor as se

    db_path = _build_db(tmp_path)
    conn = sqlite3.connect(str(db_path))
    for trade_id in (93, 94, 95):
        conn.execute(
            "INSERT INTO options_trades (id, symbol, strategy_id, legs_json, status) "
            "VALUES (?, 'CEG', 'swingdesk_manual', ?, 'open')", (trade_id, _legs_json()))
    conn.commit()
    conn.close()

    with patch.object(se, "_DB", db_path):
        found = se._find_open_position_id("CEG", "swingdesk_manual", _occ_symbols())

    assert found == 95


def test_close_original_position_updates_in_place_no_new_row(tmp_path):
    import swingdesk.spread_executor as se

    db_path = _build_db(tmp_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT INTO options_trades (id, symbol, strategy_id, legs_json, status, "
        "exec_status) VALUES (1, 'CEG', 'swingdesk_manual', ?, 'open', 'filled')",
        (_legs_json(),))
    conn.commit()
    conn.close()

    with patch.object(se, "_DB", db_path):
        trade_id = se._close_original_position(
            "CEG", "swingdesk_manual", _occ_symbols(), "close-order-123")

    assert trade_id == 1

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM options_trades").fetchall()
    conn.close()

    # Exactly one row -- the close must NOT have inserted a phantom duplicate.
    assert len(rows) == 1
    row = rows[0]
    assert row["status"] == "closed"
    assert row["exit_date"] is not None
    assert "close-order-123" in row["exit_reason"]
    # pnl is deliberately untouched by this fix -- still NULL.
    assert row["pnl"] is None


def test_close_original_position_returns_none_when_no_open_row(tmp_path):
    import swingdesk.spread_executor as se

    db_path = _build_db(tmp_path)
    with patch.object(se, "_DB", db_path):
        trade_id = se._close_original_position(
            "CEG", "swingdesk_manual", _occ_symbols(), "close-order-456")

    assert trade_id is None
