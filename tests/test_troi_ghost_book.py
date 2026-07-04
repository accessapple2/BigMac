"""HM-TROI-GHOST-BOOK-2026-07-04 tests.

Isolated temp SQLite DB throughout -- never the live trader.db, so these
stay correct regardless of the real ghost book's state on any given day.
"""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import engine.troi_ghost_book as gb


def _make_source_db(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE options_trades (id INTEGER PRIMARY KEY, agent_id TEXT, "
        "symbol TEXT, structure TEXT, status TEXT, legs_json TEXT, "
        "entry_credit_debit REAL, exit_credit_debit REAL, pnl REAL, "
        "expiration TEXT, entry_date TEXT, exit_date TEXT, exit_reason TEXT)"
    )
    conn.commit()
    conn.close()


def _insert_trimmed_leg(path: str, trade_id: int, symbol: str, strike: float,
                        entry_premium: float, exit_premium: float,
                        expiration: str, entry_date: str = "2026-06-01",
                        exit_date: str = "2026-07-04", qty: int = 1) -> float:
    legs = json.dumps([{"side": "short", "type": "put", "strike": strike,
                        "qty": qty, "entry_price": entry_premium}])
    entry_credit = entry_premium * 100 * qty
    exit_debit = -(exit_premium * 100 * qty)  # stored negative, matches close_options_trade
    pnl = entry_credit - abs(exit_debit)
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO options_trades (id, agent_id, symbol, structure, status, legs_json, "
        "entry_credit_debit, exit_credit_debit, pnl, expiration, entry_date, exit_date, exit_reason) "
        "VALUES (?, 'options-sosnoff', ?, 'csp', 'closed', ?, ?, ?, ?, ?, ?, ?, ?)",
        (trade_id, symbol, legs, entry_credit, exit_debit, pnl, expiration,
         entry_date, exit_date, gb.SOURCE_EXIT_REASON),
    )
    conn.commit()
    conn.close()
    return pnl


class GhostBookSeedTests(unittest.TestCase):

    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        _make_source_db(self.db_path)
        self._db_patch = patch.object(gb, "DB_PATH", self.db_path)
        self._db_patch.start()

    def tearDown(self):
        self._db_patch.stop()
        os.remove(self.db_path)

    def test_seed_creates_one_row_per_trimmed_leg(self):
        _insert_trimmed_leg(self.db_path, 1, "QQQ", 630.0, 28.0, 15.0, "2026-08-01")
        _insert_trimmed_leg(self.db_path, 2, "SOXL", 180.0, 8.0, 3.0, "2026-08-01")
        res = gb.seed_ghost_book()
        self.assertEqual(res["seeded"], 2)
        conn = sqlite3.connect(self.db_path)
        n = conn.execute("SELECT COUNT(*) FROM ghost_csp_book").fetchone()[0]
        conn.close()
        self.assertEqual(n, 2)

    def test_seed_only_picks_up_correctly_tagged_exit_reason(self):
        _insert_trimmed_leg(self.db_path, 1, "QQQ", 630.0, 28.0, 15.0, "2026-08-01")
        # A different close, e.g. a real time_stop exit -- must NOT be picked up.
        conn = sqlite3.connect(self.db_path)
        legs = json.dumps([{"side": "short", "type": "put", "strike": 60.0, "qty": 1, "entry_price": 3.0}])
        conn.execute(
            "INSERT INTO options_trades (id, agent_id, symbol, structure, status, legs_json, "
            "entry_credit_debit, exit_credit_debit, pnl, expiration, entry_date, exit_date, exit_reason) "
            "VALUES (99, 'options-sosnoff', 'TQQQ', 'csp', 'closed', ?, 300, -150, 150, "
            "'2026-08-01', '2026-06-01', '2026-07-01', 'time_stop_21dte')",
            (legs,),
        )
        conn.commit()
        conn.close()
        res = gb.seed_ghost_book()
        self.assertEqual(res["seeded"], 1)  # only the correctly-tagged one

    def test_seed_is_idempotent(self):
        _insert_trimmed_leg(self.db_path, 1, "QQQ", 630.0, 28.0, 15.0, "2026-08-01")
        gb.seed_ghost_book()
        res2 = gb.seed_ghost_book()
        self.assertEqual(res2["seeded"], 0)
        self.assertEqual(res2["skipped_existing"], 1)

    def test_seeded_row_preserves_trim_pnl_and_prices(self):
        expected_pnl = _insert_trimmed_leg(self.db_path, 1, "QQQ", 630.0, 28.0, 15.0, "2026-08-01")
        gb.seed_ghost_book()
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM ghost_csp_book WHERE source_trade_id=1").fetchone()
        conn.close()
        self.assertEqual(row["symbol"], "QQQ")
        self.assertEqual(row["strike"], 630.0)
        self.assertAlmostEqual(row["trim_close_price"], 15.0)
        self.assertAlmostEqual(row["trim_pnl"], expected_pnl)
        self.assertEqual(row["status"], "open")


class GhostBookMarkTests(unittest.TestCase):

    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        _make_source_db(self.db_path)
        self._db_patch = patch.object(gb, "DB_PATH", self.db_path)
        self._db_patch.start()

    def tearDown(self):
        self._db_patch.stop()
        os.remove(self.db_path)

    def _seed_one(self, symbol="QQQ", strike=630.0, entry_premium=28.0,
                 exit_premium=15.0, expiration="2026-08-01"):
        _insert_trimmed_leg(self.db_path, 1, symbol, strike, entry_premium, exit_premium, expiration)
        gb.seed_ghost_book()

    def test_mark_pre_expiry_updates_fields_stays_open(self):
        self._seed_one(expiration="2026-08-01")
        from datetime import date
        with patch("engine.market_data.get_stock_price", return_value={"price": 650.0}):
            res = gb.run_ghost_mark(as_of=date(2026, 7, 10))
        self.assertEqual(res["marked"], 1)
        self.assertEqual(res["assigned"], 0)
        self.assertEqual(res["expired_otm"], 0)
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM ghost_csp_book WHERE source_trade_id=1").fetchone()
        conn.close()
        self.assertEqual(row["status"], "open")
        self.assertEqual(row["last_underlying_close"], 650.0)
        self.assertAlmostEqual(row["last_distance_to_strike_pct"], (650.0 - 630.0) / 630.0 * 100, places=2)

    def test_mark_at_expiry_otm_resolves_expired_full_premium(self):
        self._seed_one(strike=630.0, entry_premium=28.0, expiration="2026-08-01")
        from datetime import date
        # Close ABOVE strike at expiry -> put expires worthless, full premium kept.
        with patch("engine.market_data.get_stock_price", return_value={"price": 650.0}):
            res = gb.run_ghost_mark(as_of=date(2026, 8, 1))
        self.assertEqual(res["expired_otm"], 1)
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM ghost_csp_book WHERE source_trade_id=1").fetchone()
        conn.close()
        self.assertEqual(row["status"], "expired_otm")
        self.assertAlmostEqual(row["ghost_pnl"], 28.0 * 100)  # full entry credit

    def test_mark_at_expiry_itm_resolves_assigned_with_intrinsic_loss(self):
        self._seed_one(strike=630.0, entry_premium=28.0, expiration="2026-08-01")
        from datetime import date
        # Close BELOW strike at expiry -> assigned, intrinsic loss vs premium collected.
        with patch("engine.market_data.get_stock_price", return_value={"price": 600.0}):
            res = gb.run_ghost_mark(as_of=date(2026, 8, 1))
        self.assertEqual(res["assigned"], 1)
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM ghost_csp_book WHERE source_trade_id=1").fetchone()
        conn.close()
        self.assertEqual(row["status"], "assigned")
        expected_pnl = 28.0 * 100 - (630.0 - 600.0) * 100  # entry_credit - intrinsic*100*qty
        self.assertAlmostEqual(row["ghost_pnl"], expected_pnl)
        self.assertEqual(row["ghost_assigned_price"], 600.0)

    def test_mark_past_expiry_still_resolves(self):
        """A leg whose expiry already passed before the first mark ever ran
        (e.g. the cron was down) must still resolve correctly, not get stuck."""
        self._seed_one(strike=630.0, entry_premium=28.0, expiration="2026-07-01")
        from datetime import date
        with patch("engine.market_data.get_stock_price", return_value={"price": 640.0}):
            res = gb.run_ghost_mark(as_of=date(2026, 7, 10))
        self.assertEqual(res["expired_otm"], 1)

    def test_no_price_available_counts_as_error_not_crash(self):
        self._seed_one(expiration="2026-08-01")
        from datetime import date
        with patch("engine.market_data.get_stock_price", return_value={"price": None}):
            res = gb.run_ghost_mark(as_of=date(2026, 7, 10))
        self.assertEqual(res["errors"], 1)
        self.assertEqual(res["marked"], 0)

    def test_mark_never_raises_on_exception(self):
        self._seed_one(expiration="2026-08-01")
        from datetime import date
        with patch("engine.market_data.get_stock_price", side_effect=RuntimeError("boom")):
            try:
                res = gb.run_ghost_mark(as_of=date(2026, 7, 10))
            except Exception as e:
                self.fail(f"run_ghost_mark raised: {e!r}")
        self.assertGreaterEqual(res["errors"], 1)


class GhostWorstCaseAndDeltaTests(unittest.TestCase):

    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        _make_source_db(self.db_path)
        self._db_patch = patch.object(gb, "DB_PATH", self.db_path)
        self._db_patch.start()

    def tearDown(self):
        self._db_patch.stop()
        os.remove(self.db_path)

    def test_worst_case_only_counts_still_open_legs(self):
        _insert_trimmed_leg(self.db_path, 1, "SOXL", 180.0, 8.0, 3.0, "2026-08-01")
        gb.seed_ghost_book()
        from datetime import date
        with patch("engine.market_data.get_stock_price", return_value={"price": 190.0}):
            gb.run_ghost_mark(as_of=date(2026, 7, 10))  # pre-expiry mark, stays open
        wc = gb.ghost_worst_case()
        self.assertEqual(wc["n_open"], 1)
        # SOXL is 3x leveraged -> -60% shock: 190 * 0.4 = 76, well below strike 180 -> assigned in worst case
        self.assertIn("SOXL", wc["per_symbol"])
        self.assertGreater(wc["total"], 0)

    def test_worst_case_excludes_resolved_legs(self):
        _insert_trimmed_leg(self.db_path, 1, "SOXL", 180.0, 8.0, 3.0, "2026-07-01")
        gb.seed_ghost_book()
        from datetime import date
        with patch("engine.market_data.get_stock_price", return_value={"price": 200.0}):
            gb.run_ghost_mark(as_of=date(2026, 7, 10))  # past expiry, resolves expired_otm
        wc = gb.ghost_worst_case()
        self.assertEqual(wc["n_open"], 0)
        self.assertEqual(wc["total"], 0)

    def test_vs_trim_delta_positive_when_ghost_beats_trim(self):
        # Trimmed at a debit (small pnl); ghost ultimately expires OTM (full premium) -> ghost wins.
        _insert_trimmed_leg(self.db_path, 1, "QQQ", 630.0, 28.0, 20.0, "2026-07-01")  # trim pnl = (28-20)*100=800
        gb.seed_ghost_book()
        from datetime import date
        with patch("engine.market_data.get_stock_price", return_value={"price": 650.0}):
            gb.run_ghost_mark(as_of=date(2026, 7, 10))  # past expiry, OTM -> ghost_pnl = 2800
        summary = gb.ghost_vs_trim_summary()
        self.assertEqual(summary["resolved_legs"], 1)
        self.assertAlmostEqual(summary["resolved_trim_total"], 800.0)
        self.assertAlmostEqual(summary["resolved_ghost_total"], 2800.0)
        self.assertAlmostEqual(summary["resolved_delta"], 2000.0)  # ghost beat trim by $2000


if __name__ == "__main__":
    unittest.main()
