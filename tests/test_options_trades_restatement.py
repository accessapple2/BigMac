"""tests/test_options_trades_restatement.py -- HM-OPTIONS-TRADES-
RESTATEMENT-GAP-2026-09-12.

The 2026-09-11 restatement (dry-dock B6) filtered on exit_date, but the
pricing bug it restates lives in ENTRY pricing (wheel_strategy.py /
shadow_csp.py computed entry_credit_debit from a synthetic vix/500 formula
at OPEN time) -- a row opened inside the bug window that happened to close
a day or more after 2026-07-07 was silently skipped. This file guards the
two things that fix depended on: the corrected row-selection SQL, and that
it's idempotent against already-restated rows (never re-touches the
original 120).

Does not hit the real Alpaca API -- detect_mult() and occ_symbol() are pure
functions, tested directly; the SQL selection is tested against a throwaway
sqlite file, not data/trader.db.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts.options_trades_restatement import detect_mult, occ_symbol  # noqa: E402


def test_occ_symbol_format():
    assert occ_symbol("SPY", "2026-07-25", "put", 646.67) == "SPY260725P00646670"
    assert occ_symbol("SOXL", "2026-08-01", "call", 100.0) == "SOXL260801C00100000"


def test_detect_mult_hundred_for_csp_convention():
    legs = [{"side": "short", "type": "put", "strike": 646.67, "qty": 1, "entry_price": 27.78}]
    assert detect_mult(2778.0, legs) == 100.0


def test_detect_mult_one_for_equity_spread_convention():
    legs = [
        {"side": "long", "type": "put", "strike": 743.0, "qty": 1, "entry_price": 4.56},
        {"side": "short", "type": "put", "strike": 748.0, "qty": 1, "entry_price": 6.38},
    ]
    # entry_credit_debit = +6.38 - 4.56 = 1.82 (mult=1, no qty/100 scaling)
    assert detect_mult(1.82, legs) == 1.0


def test_detect_mult_none_when_neither_matches():
    legs = [{"side": "short", "type": "put", "strike": 100.0, "qty": 1, "entry_price": 1.0}]
    assert detect_mult(-3.0, legs) is None  # the real HM-DRYDOCK "TEST" row shape


@pytest.fixture
def temp_options_db(tmp_path):
    db_path = tmp_path / "test_trader.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE options_trades (
            id INTEGER PRIMARY KEY, structure TEXT, symbol TEXT,
            entry_date TEXT, exit_date TEXT, expiration TEXT,
            entry_credit_debit REAL, exit_credit_debit REAL, pnl REAL,
            status TEXT, exit_reason TEXT, legs_json TEXT,
            restatement_basis TEXT
        )
    """)
    rows = [
        # Already restated by the original pass -- must NOT be re-selected.
        (1, "csp", "SPY", "2026-04-22T10:00:00", "2026-05-05T10:00:00", "2026-05-10",
         100.0, -50.0, 50.0, "closed", "expired_otm", "[]", "real_alpaca_bar"),
        # The actual gap: entered inside the bug window, exited AFTER
        # 2026-07-07 -- the original exit_date-only filter missed this.
        (2, "csp", "SPY", "2026-06-26T04:30:09", "2026-07-08T00:28:56", "2026-07-25",
         2778.0, -1506.28, 1271.72, "closed", "time_stop_21dte",
         '[{"side":"short","type":"put","strike":646.67,"qty":1,"entry_price":27.78}]', None),
        # Entered AFTER the fix -- must NOT be selected (no bug exposure).
        (3, "bull_put_spread", "SPY", "2026-07-13T08:26:06", "2026-08-25T00:22:11", "2026-07-24",
         1.6955, 0.0, 1.6955, "closed", "expired_otm", "[]", None),
    ]
    conn.executemany(
        "INSERT INTO options_trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", rows
    )
    conn.commit()
    conn.close()
    return db_path


def _select_ids(db_path) -> list[int]:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    ids = [r[0] for r in conn.execute(
        "SELECT id FROM options_trades "
        "WHERE (entry_date < '2026-07-07' OR exit_date < '2026-07-07' OR exit_date IS NULL) "
        "AND (restatement_basis IS NULL OR restatement_basis = '') "
        "ORDER BY id"
    ).fetchall()]
    conn.close()
    return ids


def test_selection_catches_entry_side_bug_exposure_missed_by_exit_date_filter(temp_options_db):
    assert _select_ids(temp_options_db) == [2]


def test_selection_skips_already_restated_rows(temp_options_db):
    ids = _select_ids(temp_options_db)
    assert 1 not in ids  # already has restatement_basis='real_alpaca_bar'


def test_selection_skips_rows_entered_after_the_fix(temp_options_db):
    ids = _select_ids(temp_options_db)
    assert 3 not in ids  # entered 2026-07-13, past the bug window entirely
