"""tests/test_options_real_fills.py -- HM-OPTIONS-REAL-FILLS-2026-09-12.

Covers the options real-fill writeback pass:
1. strategies/executor.py's canonical-schema close bugs, found and fixed
   while building this -- the close path checked leg.get("action")=="buy"/
   "sell", a key the canonical legs_json schema (2026-05-17) never writes
   (real rows use "side": "long"/"short"), and _occ_symbol() read a
   per-leg "expiration"/"option_type" that also doesn't exist in the
   canonical shape (expiration lives once on the options_trades row;
   the key is "type"). Confirmed via real stored data (options_trades id
   140) before fixing, not assumed -- every real 2-leg close since the
   schema changed silently fell through to a per-leg fallback with the
   identical bug, so the atomic MLEG close has never actually fired.
2. _record_options_trade()'s real-fill writeback: a real Alpaca fill
   magnitude, signed by the spread's STRUCTURAL debit/credit type (a
   mathematical property -- bull_call/bear_put spreads are always a net
   debit, bull_put/bear_call spreads always a net credit), not by
   Alpaca's own unverified MLEG sign convention.
3. _close_live()'s equivalent close-side pnl computation, using the
   opposite-of-entry structural sign (closing a debit spread returns
   money, closing a credit spread costs money) -- the same relationship
   the restatement script's own hand-verified formula already uses.

No live Alpaca calls -- everything that would call the broker is mocked.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import strategies.executor as ex  # noqa: E402
from strategies.base import StrategySignal  # noqa: E402


# ── 1. Schema-mismatch fixes ────────────────────────────────────────────

def test_occ_symbol_reads_canonical_type_key_and_explicit_expiration():
    leg = {"side": "long", "type": "put", "strike": 750.0, "qty": 1, "entry_price": 4.02}
    assert ex._occ_symbol("SPY", leg, expiration="2026-07-24") == "SPY260724P00750000"


def test_occ_symbol_still_accepts_legacy_leg_shaped_expiration_and_option_type():
    """Open-side payload legs (strategy signal shape) still carry their own
    expiration/option_type -- must keep working unchanged."""
    leg = {"action": "buy", "option_type": "call", "strike": 700.0, "expiration": "2026-04-25"}
    assert ex._occ_symbol("SPY", leg) == "SPY260425C00700000"


def test_close_legs_individually_uses_side_not_action():
    """The exact real-data shape (options_trades id 140's legs_json) --
    would have defaulted BOTH legs to 'buy'/sell-to-close under the old
    leg.get('action','buy') check."""
    legs = [
        {"side": "long", "type": "put", "strike": 750.0, "qty": 1, "entry_price": 4.02},
        {"side": "short", "type": "put", "strike": 755.0, "qty": 1, "entry_price": 5.72},
    ]
    close_pos_calls = []
    submit_calls = []

    def fake_close_options_position(player_id, contract_symbol, qty):
        close_pos_calls.append(contract_symbol)
        return {"success": True, "order_id": "o1", "filled_avg_price": 3.5}

    def fake_submit_single_option(player_id, contract_symbol, qty, side):
        submit_calls.append((contract_symbol, side))
        return {"success": True, "order_id": "o2", "filled_avg_price": 4.5}

    out = ex._close_legs_individually(
        legs, "SPY", "strategy:bull_spread_v1", 1,
        fake_close_options_position, fake_submit_single_option,
        expiration="2026-07-24",
    )
    assert len(out) == 2
    # Long leg -> sell-to-close via close_options_position
    assert close_pos_calls == ["SPY260724P00750000"]
    # Short leg -> buy-to-close via submit_single_option(side='buy')
    assert submit_calls == [("SPY260724P00755000", "buy")]


# ── 2. Open-side real-fill writeback, structural sign ──────────────────

def _signal(structure: str, strategy_id: str = "bull_spread_v1") -> StrategySignal:
    return StrategySignal(
        strategy_id=strategy_id,
        ticker="SPY",
        action="open",
        asset_type="spread",
        direction="bull",
        max_risk_usd=500.0,
        exit_tag="test",
        payload={
            "structure": structure,
            "contracts": 1,
            "long_leg": {"action": "buy", "option_type": "put", "strike": 750.0,
                         "expiration": "2026-07-24", "premium": 4.02},
            "short_leg": {"action": "sell", "option_type": "put", "strike": 755.0,
                          "expiration": "2026-07-24", "premium": 5.72},
            "net_debit": 0.0,
            "net_credit": 1.70,  # the pre-trade quote -- must NOT be used when a real fill exists
        },
    )


@pytest.fixture
def executor_db(tmp_path, monkeypatch):
    db_path = tmp_path / "test_trader.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""CREATE TABLE options_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        book_tag TEXT DEFAULT 'fleet', agent_id TEXT, symbol TEXT, structure TEXT,
        expiration TEXT, legs_json TEXT, entry_credit_debit REAL, entry_date TEXT,
        strategy_id TEXT, exit_tag TEXT, broker_order_id TEXT, signal_id INTEGER,
        exec_status TEXT, restatement_basis TEXT,
        status TEXT DEFAULT 'open', contracts INTEGER DEFAULT 1,
        contracts_closed_so_far INTEGER DEFAULT 0,
        exit_date TEXT, exit_reason TEXT, pnl REAL, exit_credit_debit REAL
    )""")
    conn.commit()
    conn.close()
    monkeypatch.setattr(ex, "DB_PATH", db_path)
    return db_path


def _row(db_path, rid):
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM options_trades WHERE id=?", (rid,)).fetchone()
    conn.close()
    return dict(row)


def test_debit_structure_real_fill_is_negative_entry_credit_debit(executor_db):
    """bull_put_spread's own payload above is a CREDIT structure; use
    bull_call_spread here to exercise the debit branch."""
    rid = ex._record_options_trade(_signal("bull_call_spread"), "order123", None, fill_price=2.15)
    row = _row(executor_db, rid)
    assert row["entry_credit_debit"] == pytest.approx(-2.15)
    assert row["restatement_basis"] == "real_fill"


def test_credit_structure_real_fill_is_positive_entry_credit_debit(executor_db):
    rid = ex._record_options_trade(_signal("bull_put_spread"), "order123", None, fill_price=1.65)
    row = _row(executor_db, rid)
    assert row["entry_credit_debit"] == pytest.approx(1.65)
    assert row["restatement_basis"] == "real_fill"


def test_no_fill_falls_back_to_pretrade_quote_unchanged(executor_db):
    """Poll timeout / order still working -- behavior identical to before this fix."""
    rid = ex._record_options_trade(_signal("bull_put_spread"), "order123", None, fill_price=None)
    row = _row(executor_db, rid)
    assert row["entry_credit_debit"] == pytest.approx(1.70)  # net_credit - net_debit from payload
    assert row["restatement_basis"] is None


# ── 3. Close-side real-fill writeback, structural sign ──────────────────

class _Intent:
    def __init__(self, position_id, contracts_to_close, reason="test_exit"):
        self.position_id = position_id
        self.contracts_to_close = contracts_to_close
        self.reason = reason


def _insert_open_position(db_path, structure, entry_credit_debit, contracts=1, contracts_closed=0):
    conn = sqlite3.connect(str(db_path))
    legs_json = (
        '[{"side": "long", "type": "put", "strike": 750.0, "qty": 1, "entry_price": 4.02},'
        ' {"side": "short", "type": "put", "strike": 755.0, "qty": 1, "entry_price": 5.72}]'
    )
    cur = conn.execute(
        "INSERT INTO options_trades (agent_id, symbol, structure, expiration, legs_json, "
        "entry_credit_debit, strategy_id, status, exec_status, contracts, contracts_closed_so_far) "
        "VALUES ('strategy:bull_spread_v1', 'SPY', ?, '2026-07-24', ?, ?, 'bull_spread_v1', "
        "'open', 'open', ?, ?)",
        (structure, legs_json, entry_credit_debit, contracts, contracts_closed),
    )
    conn.commit()
    rid = cur.lastrowid
    conn.close()
    return rid


def test_full_close_credit_structure_computes_real_pnl(executor_db):
    """bull_put_spread opened at +1.70 credit; a real close fill of 0.50
    (buying it back) is a debit on close -- pnl = 1.70 + (-0.50) = 1.20."""
    rid = _insert_open_position(executor_db, "bull_put_spread", entry_credit_debit=1.70)
    with patch("engine.alpaca_options.close_options_position") as m_close_pos, \
         patch("engine.alpaca_options.submit_single_option") as m_submit, \
         patch("engine.alpaca_options.close_vertical_spread") as m_close_spread:
        m_close_spread.return_value = {"success": True, "order_id": "c1", "filled_avg_price": 0.50}
        ex._close_live(_Intent(rid, contracts_to_close=1))
    row = _row(executor_db, rid)
    assert row["exit_credit_debit"] == pytest.approx(-0.50)
    assert row["pnl"] == pytest.approx(1.20)
    assert row["restatement_basis"] == "real_fill"


def test_full_close_debit_structure_computes_real_pnl(executor_db):
    """bull_call_spread opened at -2.15 debit; a real close fill of 3.00
    (selling it back) is a credit on close -- pnl = -2.15 + 3.00 = 0.85."""
    rid = _insert_open_position(executor_db, "bull_call_spread", entry_credit_debit=-2.15)
    with patch("engine.alpaca_options.close_options_position") as m_close_pos, \
         patch("engine.alpaca_options.submit_single_option") as m_submit, \
         patch("engine.alpaca_options.close_vertical_spread") as m_close_spread:
        m_close_spread.return_value = {"success": True, "order_id": "c1", "filled_avg_price": 3.00}
        ex._close_live(_Intent(rid, contracts_to_close=1))
    row = _row(executor_db, rid)
    assert row["exit_credit_debit"] == pytest.approx(3.00)
    assert row["pnl"] == pytest.approx(0.85)
    assert row["restatement_basis"] == "real_fill"


def test_partial_close_does_not_compute_pnl(executor_db):
    """Only a FULL close gets a real pnl -- a partial close's real economics
    aren't fully known yet (position still open)."""
    rid = _insert_open_position(executor_db, "bull_put_spread", entry_credit_debit=1.70, contracts=2)
    with patch("engine.alpaca_options.close_options_position"), \
         patch("engine.alpaca_options.submit_single_option"), \
         patch("engine.alpaca_options.close_vertical_spread") as m_close_spread:
        m_close_spread.return_value = {"success": True, "order_id": "c1", "filled_avg_price": 0.50}
        ex._close_live(_Intent(rid, contracts_to_close=1))  # 1 of 2 -- partial
    row = _row(executor_db, rid)
    assert row["pnl"] is None
    assert row["exit_credit_debit"] is None


def test_close_with_no_confirmed_fill_leaves_pnl_null(executor_db):
    """Poll timeout on close -- same tolerated behavior as before this fix."""
    rid = _insert_open_position(executor_db, "bull_put_spread", entry_credit_debit=1.70)
    with patch("engine.alpaca_options.close_options_position"), \
         patch("engine.alpaca_options.submit_single_option"), \
         patch("engine.alpaca_options.close_vertical_spread") as m_close_spread:
        m_close_spread.return_value = {"success": True, "order_id": "c1", "filled_avg_price": None}
        ex._close_live(_Intent(rid, contracts_to_close=1))
    row = _row(executor_db, rid)
    assert row["pnl"] is None
