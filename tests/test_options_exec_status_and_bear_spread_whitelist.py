"""Two fixes from the S6 2026-07-10 MLEG sweep, covered together since both
were shipped in the same pass:

1. HM-OPTIONS-EXEC-CLOSE-EXEC-STATUS-NEVER-SET: engine/options_exec.py::
   close_options_trade() updated status/pnl/exit_date on close but never
   set exec_status. Three call sites gate on exec_status='open' alone
   (not status): strategies/exit_manager.py::fetch_open_strategy_positions,
   and the dedup checks in bull_spread_v1.py/bull_call_spread_v1.py.
   Confirmed live: options_trades id 28 (bull_spread_v1, SPY) closed
   2026-05-22 but stayed exec_status='open', silently blocking every new
   SPY entry for both strategies for 7+ weeks.

2. HM-EXECUTOR-STRUCTURE-WHITELIST-GAP: strategies/executor.py::
   _execute_live()'s structure whitelist was hardcoded to
   ("bull_call_spread", "bull_put_spread") -- but bear_put_spread_v1.py
   emits "bear_put_spread"/"bear_call_spread", neither on the list, so
   100% of that strategy's signals were rejected before submission since
   it was wired up (confirmed: zero options_trades rows for
   strategy_id='bear_put_spread_v1', ever).
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ─── close_options_trade() exec_status fix ─────────────────────────────────

def _build_options_exec_db(tmp_path):
    db_path = tmp_path / "test_trader.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""CREATE TABLE options_trades (
        id INTEGER PRIMARY KEY,
        book_tag TEXT DEFAULT 'fleet',
        status TEXT DEFAULT 'open', exec_status TEXT DEFAULT 'open',
        entry_credit_debit REAL, max_loss REAL,
        exit_date TEXT, exit_credit_debit REAL, pnl REAL, pnl_pct REAL,
        exit_reason TEXT
    )""")
    conn.execute("""CREATE TABLE options_books (
        book_tag TEXT PRIMARY KEY, current_cash REAL DEFAULT 0,
        wins INTEGER DEFAULT 0, losses INTEGER DEFAULT 0
    )""")
    conn.execute(
        "INSERT INTO options_trades (id, status, exec_status, entry_credit_debit, "
        "max_loss) VALUES (1, 'open', 'open', 2.0, -100.0)")
    conn.execute("INSERT INTO options_books (book_tag, current_cash) VALUES ('fleet', 1000.0)")
    conn.commit()
    conn.close()
    return db_path


def test_close_options_trade_sets_exec_status_closed(tmp_path):
    import engine.options_exec as oe

    db_path = _build_options_exec_db(tmp_path)
    with patch.object(oe, "DB_PATH", str(db_path)):
        pnl = oe.close_options_trade(
            trade_id=1,
            exit_legs=[{"exit_price": 0.5, "qty": 1}],
            exit_reason="test_close",
        )

    assert pnl is not None
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM options_trades WHERE id=1").fetchone()
    conn.close()

    assert row["status"] == "closed"
    assert row["exec_status"] == "closed"
    assert row["exit_date"] is not None
    assert row["pnl"] is not None


def test_close_options_trade_only_affects_open_rows(tmp_path):
    """Sanity check the existing WHERE status='open' guard still works --
    the fix must not touch already-closed rows."""
    import engine.options_exec as oe

    db_path = _build_options_exec_db(tmp_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute("UPDATE options_trades SET status='closed' WHERE id=1")
    conn.commit()
    conn.close()

    with patch.object(oe, "DB_PATH", str(db_path)):
        pnl = oe.close_options_trade(
            trade_id=1, exit_legs=[{"exit_price": 0.5, "qty": 1}],
            exit_reason="should_not_apply")

    assert pnl is None


# ─── strategies/executor.py structure whitelist fix ────────────────────────

def _signal(structure: str):
    from strategies.base import StrategySignal
    return StrategySignal(
        strategy_id="bear_put_spread_v1",
        ticker="SPY",
        action="open",
        asset_type="spread",
        direction="bear",
        max_risk_usd=500.0,
        payload={
            "structure": structure,
            "contracts": 1,
            "long_leg": {"action": "buy", "option_type": "put", "strike": 600.0,
                        "expiration": "2026-08-15", "premium": 5.0},
            "short_leg": {"action": "sell", "option_type": "put", "strike": 595.0,
                         "expiration": "2026-08-15", "premium": 3.0},
        },
    )


def test_bear_put_spread_no_longer_rejected_by_whitelist():
    import strategies.executor as ex

    with patch("engine.alpaca_options.submit_vertical_spread",
              return_value={"order_id": "fake-order-1"}), \
         patch.object(ex, "_record_options_trade", return_value=42):
        result = ex._execute_live(_signal("bear_put_spread"), signal_id=1)

    assert result.status == "executed"
    assert result.options_trade_id == 42


def test_bear_call_spread_no_longer_rejected_by_whitelist():
    import strategies.executor as ex

    with patch("engine.alpaca_options.submit_vertical_spread",
              return_value={"order_id": "fake-order-2"}), \
         patch.object(ex, "_record_options_trade", return_value=43):
        result = ex._execute_live(_signal("bear_call_spread"), signal_id=2)

    assert result.status == "executed"


def test_unknown_structure_still_rejected():
    """The whitelist gap fix must not turn into an open door -- a genuinely
    unrecognized structure must still be rejected."""
    import strategies.executor as ex

    result = ex._execute_live(_signal("iron_condor"), signal_id=3)

    assert result.status == "rejected"
    assert "Unknown structure" in result.reason
