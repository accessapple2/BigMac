"""tests/test_battle_station_0dte_real_fills.py -- HM-OPTIONS-REAL-FILLS-2026-09-12.

battle_station_0dte.py had never submitted a real Alpaca order in its
history (2 trades ever, both quote-only, no broker_order_id) -- same
pure-simulation pattern as options-sosnoff/shadow-qwen35-csp. Wires
_submit_real_order() through the already-proven submit_single_option()
machinery. Covers only the new function's three outcomes; no live Alpaca
calls (submit_single_option is mocked).
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import engine.battle_station_0dte as bs0  # noqa: E402


def test_real_fill_confirmed_overrides_quoted_price():
    with patch("engine.alpaca_options.submit_single_option") as m:
        m.return_value = {"success": True, "order_id": "o1", "filled_avg_price": 2.35}
        price, order_id, basis = bs0._submit_real_order("SPY260912C00600000", quoted_price=2.50)
    assert price == 2.35
    assert order_id == "o1"
    assert basis == "real_fill"


def test_order_submitted_but_fill_not_confirmed_falls_back_to_quote():
    """Poll timeout -- order_id is still worth recording (a real order did
    go out), but the price must stay the quote until a fill is confirmed."""
    with patch("engine.alpaca_options.submit_single_option") as m:
        m.return_value = {"success": True, "order_id": "o2", "filled_avg_price": None}
        price, order_id, basis = bs0._submit_real_order("SPY260912C00600000", quoted_price=2.50)
    assert price == 2.50
    assert order_id == "o2"
    assert basis is None


def test_skipped_or_errored_order_falls_back_to_quote_with_no_order_id():
    with patch("engine.alpaca_options.submit_single_option") as m:
        m.return_value = {"skipped": True, "reason": "Alpaca not connected"}
        price, order_id, basis = bs0._submit_real_order("SPY260912C00600000", quoted_price=2.50)
    assert price == 2.50
    assert order_id is None
    assert basis is None


def test_exception_falls_back_to_quote_never_raises():
    with patch("engine.alpaca_options.submit_single_option", side_effect=RuntimeError("boom")):
        price, order_id, basis = bs0._submit_real_order("SPY260912C00600000", quoted_price=2.50)
    assert price == 2.50
    assert order_id is None
    assert basis is None


def test_open_trade_stores_broker_order_id_and_restatement_basis(tmp_path, monkeypatch):
    db_path = tmp_path / "test_trader.db"
    monkeypatch.setattr(bs0, "DB_PATH", str(db_path))
    bs0._initialized = False
    bs0._init()
    trade_id = bs0._open_trade(
        "call", 600.0, "2026-09-12", 2.35, 599.0, 590.0, 605.0, 15.0, 10.0,
        "test reason", broker_order_id="o1", restatement_basis="real_fill",
    )
    import sqlite3
    conn = sqlite3.connect(str(db_path))
    row = conn.execute(
        "SELECT entry_price, broker_order_id, restatement_basis FROM battle_station_trades WHERE id=?",
        (trade_id,),
    ).fetchone()
    conn.close()
    assert row == (2.35, "o1", "real_fill")
