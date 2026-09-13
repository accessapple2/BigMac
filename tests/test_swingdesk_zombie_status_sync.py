"""HM-SWINGDESK-ZOMBIE-OPEN-ROWS 2026-07-10.

swingdesk/spread_executor.py::poll_fill() correctly synced exec_status to
match the broker's order state, but never touched `status` -- so an order
that died (canceled/expired/rejected) WITHOUT ever filling stayed
status='open' forever, even though it never put on a real position. This is
the OPEN-side counterpart to HM-SWINGDESK-CLOSE-PHANTOM-ROW (the close
side). Confirmed live: options_trades ids 89 and 93 were broker-verified
(filled_qty=0 on both) as dead unfilled orders from 2026-06-11, yet still
showed status='open' -- inflating /api/options/book-summary's fleet
open_positions count from the true 0 to 2.

No pnl/fill-price ambiguity applies to this fix: filled_qty=0 means there is
nothing to price, unlike the close-side pnl question (still open, see
docs/XO_BACKLOG.md).
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _build_db(tmp_path, status="open", exec_status="pending"):
    db_path = tmp_path / "test_trader.db"
    conn = sqlite3.connect(str(db_path))
    # HM-OPTIONS-REAL-FILLS-2026-09-12: added entry_credit_debit/
    # restatement_basis/exit_date -- poll_fill() now writes the first two
    # back on a confirmed open-side fill (see that function's own fix
    # note), and this fixture needs to carry the real table's columns for
    # that code path to run at all, not just the two columns this file's
    # original fix cared about.
    conn.execute("""CREATE TABLE options_trades (
        id INTEGER PRIMARY KEY,
        broker_order_id TEXT,
        status TEXT DEFAULT 'open', exec_status TEXT DEFAULT 'pending',
        entry_credit_debit REAL, restatement_basis TEXT, exit_date TEXT
    )""")
    conn.execute(
        "INSERT INTO options_trades (id, broker_order_id, status, exec_status) "
        "VALUES (1, 'order-abc', ?, ?)", (status, exec_status))
    conn.commit()
    conn.close()
    return db_path


def _fake_client(order):
    return SimpleNamespace(get_order_by_id=lambda oid: order)


def _row(db_path):
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM options_trades WHERE id=1").fetchone()
    conn.close()
    return row


def test_canceled_never_filled_syncs_status_to_canceled(tmp_path):
    import swingdesk.spread_executor as se

    db_path = _build_db(tmp_path)
    order = SimpleNamespace(status="canceled", filled_avg_price=None, filled_qty=0)

    with patch.object(se, "_DB", db_path), \
         patch.object(se, "_get_paper_client", return_value=_fake_client(order)):
        result = se.poll_fill("order-abc")

    assert result["ok"] is True
    row = _row(db_path)
    assert row["status"] == "canceled"
    assert row["exec_status"] == "canceled"


def test_expired_never_filled_syncs_status_to_expired(tmp_path):
    import swingdesk.spread_executor as se

    db_path = _build_db(tmp_path)
    order = SimpleNamespace(status="expired", filled_avg_price=None, filled_qty=0)

    with patch.object(se, "_DB", db_path), \
         patch.object(se, "_get_paper_client", return_value=_fake_client(order)):
        se.poll_fill("order-abc")

    row = _row(db_path)
    assert row["status"] == "expired"
    assert row["exec_status"] == "expired"


def test_filled_order_leaves_status_untouched(tmp_path):
    """A real, live position -- status must NOT be touched by fill polling
    (lifecycle status is owned by the close path)."""
    import swingdesk.spread_executor as se

    db_path = _build_db(tmp_path, status="open", exec_status="pending")
    order = SimpleNamespace(status="filled", filled_avg_price=1.5, filled_qty=1)

    with patch.object(se, "_DB", db_path), \
         patch.object(se, "_get_paper_client", return_value=_fake_client(order)):
        se.poll_fill("order-abc")

    row = _row(db_path)
    assert row["status"] == "open"


def test_filled_order_writes_back_real_entry_credit_debit(tmp_path):
    """HM-OPTIONS-REAL-FILLS-2026-09-12: a confirmed fill must overwrite the
    pre-trade quoted entry_credit_debit with the real Alpaca fill, and stamp
    restatement_basis so this row is known-real going forward."""
    import swingdesk.spread_executor as se

    db_path = _build_db(tmp_path, status="open", exec_status="pending")
    order = SimpleNamespace(status="filled", filled_avg_price=1.42, filled_qty=1)

    with patch.object(se, "_DB", db_path), \
         patch.object(se, "_get_paper_client", return_value=_fake_client(order)):
        result = se.poll_fill("order-abc")

    assert result["ok"] is True
    row = _row(db_path)
    assert row["entry_credit_debit"] == 1.42
    assert row["restatement_basis"] == "real_fill"


def test_repolling_an_already_recorded_fill_does_not_overwrite_it(tmp_path):
    """Idempotency: _refresh_fills() re-polls every candidate row on a cycle
    -- once a real fill is recorded, a later poll (even with a slightly
    different price on some hypothetical re-check) must not clobber it."""
    import swingdesk.spread_executor as se

    db_path = _build_db(tmp_path, status="open", exec_status="pending")
    order = SimpleNamespace(status="filled", filled_avg_price=1.42, filled_qty=1)
    with patch.object(se, "_DB", db_path), \
         patch.object(se, "_get_paper_client", return_value=_fake_client(order)):
        se.poll_fill("order-abc")

    order2 = SimpleNamespace(status="filled", filled_avg_price=9.99, filled_qty=1)
    with patch.object(se, "_DB", db_path), \
         patch.object(se, "_get_paper_client", return_value=_fake_client(order2)):
        se.poll_fill("order-abc")

    row = _row(db_path)
    assert row["entry_credit_debit"] == 1.42  # unchanged by the second poll
    assert row["exec_status"] == "filled"


def test_partially_filled_then_canceled_leaves_status_untouched(tmp_path):
    """A real partial position exists -- must NOT be marked dead even though
    the order's terminal state is 'canceled'."""
    import swingdesk.spread_executor as se

    db_path = _build_db(tmp_path, status="open", exec_status="partially_filled")
    order = SimpleNamespace(status="canceled", filled_avg_price=1.2, filled_qty=1)

    with patch.object(se, "_DB", db_path), \
         patch.object(se, "_get_paper_client", return_value=_fake_client(order)):
        se.poll_fill("order-abc")

    row = _row(db_path)
    assert row["status"] == "open"
    assert row["exec_status"] == "canceled"


def test_still_working_order_leaves_status_untouched(tmp_path):
    """Order hasn't died yet -- still working, not dead-unfilled."""
    import swingdesk.spread_executor as se

    db_path = _build_db(tmp_path, status="open", exec_status="pending")
    order = SimpleNamespace(status="accepted", filled_avg_price=None, filled_qty=0)

    with patch.object(se, "_DB", db_path), \
         patch.object(se, "_get_paper_client", return_value=_fake_client(order)):
        se.poll_fill("order-abc")

    row = _row(db_path)
    assert row["status"] == "open"
    assert row["exec_status"] == "accepted"
