"""tests/test_capitol_trades_signal_dedup.py — HM-CAPITOL-DEDUP-SIGNAL-LEVEL-2026-07-09.

Covers a live-incident finding (2026-07-09): a Capitol Trades disclosure for
MU (Sheldon Whitehouse) re-fired an identical BUY signal 15 times in one
session (06:56-15:16 UTC, every ~2-15 min), every single one gate_result=
TRADE_REJECTED. The existing dedup in engine/crew_scanner.py::_scan_rules_agent
(around line 3112) only checked the `trades` table for an already-EXECUTED
buy -- since MU's buy() never succeeded, that check always found nothing and
the identical signal kept re-emitting forever.

The fix adds a second dedup check against `crew_decisions` (which
_log_decision() writes unconditionally, including gate-rejected decisions)
for an already-emitted BUY-action row today for (player, symbol) -- dedup at
the signal-EMISSION level, not just the execution-SUCCESS level.

These tests exercise the exact SQL semantics of the fix directly against an
isolated temp DB (the enclosing function has many gate dependencies that
would require heavy mocking to invoke end-to-end; the dedup query itself is
the load-bearing logic and is what actually changed).
"""
from __future__ import annotations

import sqlite3
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _make_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "capitol_dedup_test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE crew_decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT, agent_name TEXT, player_id TEXT,
            action TEXT, symbol TEXT, confidence INTEGER,
            reason TEXT, market_data TEXT, gate_result TEXT, executed INTEGER
        )
    """)
    conn.commit()
    conn.close()
    return db_path


# Mirrors the exact query added to _scan_rules_agent's dedup block.
_DEDUP_QUERY = (
    "SELECT 1 FROM crew_decisions "
    "WHERE player_id=? AND symbol=? AND action LIKE 'BUY%' "
    "AND date(timestamp)=? LIMIT 1"
)


class SignalLevelDedupQueryTests(unittest.TestCase):
    def setUp(self) -> None:
        import tempfile
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = _make_db(Path(self._tmpdir.name))
        self.conn = sqlite3.connect(str(self.db_path))

    def tearDown(self) -> None:
        self.conn.close()
        self._tmpdir.cleanup()

    def _insert_decision(self, player_id, action, symbol, gate_result,
                          executed, when: datetime) -> None:
        self.conn.execute(
            "INSERT INTO crew_decisions "
            "(timestamp, agent_name, player_id, action, symbol, confidence, "
            " reason, market_data, gate_result, executed) "
            "VALUES (?, 'Capitol Trades', ?, ?, ?, 80, 'test', '{}', ?, ?)",
            (when.isoformat(), player_id, action, symbol, gate_result, int(executed)),
        )
        self.conn.commit()

    def test_dedup_matches_prior_trade_rejected_buy_same_day(self) -> None:
        """The exact reproduced incident: a TRADE_REJECTED buy earlier today
        must be found by the dedup query, blocking re-emission."""
        today = datetime.now(timezone.utc)
        self._insert_decision("capitol-trades", "BUY", "MU", "TRADE_REJECTED", False, today)

        row = self.conn.execute(
            _DEDUP_QUERY, ("capitol-trades", "MU", str(today.date()))
        ).fetchone()
        self.assertIsNotNone(row, "TRADE_REJECTED buy earlier today must still dedup")

    def test_dedup_matches_prior_executed_buy_same_day(self) -> None:
        today = datetime.now(timezone.utc)
        self._insert_decision("capitol-trades", "BUY", "MU", "EXECUTED", True, today)

        row = self.conn.execute(
            _DEDUP_QUERY, ("capitol-trades", "MU", str(today.date()))
        ).fetchone()
        self.assertIsNotNone(row)

    def test_no_dedup_when_no_prior_decision(self) -> None:
        today = datetime.now(timezone.utc)
        row = self.conn.execute(
            _DEDUP_QUERY, ("capitol-trades", "MU", str(today.date()))
        ).fetchone()
        self.assertIsNone(row)

    def test_pass_decisions_do_not_falsely_dedup(self) -> None:
        """A PASS row (e.g. from an earlier dedup hit, or an unrelated gate
        block) must not itself satisfy the BUY-action dedup match -- only a
        row where the emitted action was actually a BUY variant counts."""
        today = datetime.now(timezone.utc)
        self._insert_decision("capitol-trades", "PASS", "MU", "MANDATE_BLOCKED", False, today)

        row = self.conn.execute(
            _DEDUP_QUERY, ("capitol-trades", "MU", str(today.date()))
        ).fetchone()
        self.assertIsNone(row)

    def test_different_symbol_does_not_dedup(self) -> None:
        today = datetime.now(timezone.utc)
        self._insert_decision("capitol-trades", "BUY", "NVDA", "TRADE_REJECTED", False, today)

        row = self.conn.execute(
            _DEDUP_QUERY, ("capitol-trades", "MU", str(today.date()))
        ).fetchone()
        self.assertIsNone(row)

    def test_different_player_does_not_dedup(self) -> None:
        """capitol-trades must not be blocked by a different player's
        identical-looking BUY on the same symbol -- dedup is deliberately
        scoped to this specific source's re-fire problem, not global."""
        today = datetime.now(timezone.utc)
        self._insert_decision("ollama-plutus", "BUY", "MU", "TRADE_REJECTED", False, today)

        row = self.conn.execute(
            _DEDUP_QUERY, ("capitol-trades", "MU", str(today.date()))
        ).fetchone()
        self.assertIsNone(row)

    def test_prior_day_decision_does_not_dedup_today(self) -> None:
        """A fresh trading day must re-open the gate -- yesterday's
        TRADE_REJECTED buy must not permanently suppress today's signal."""
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        self._insert_decision("capitol-trades", "BUY", "MU", "TRADE_REJECTED", False, yesterday)

        today_str = str(datetime.now(timezone.utc).date())
        row = self.conn.execute(
            _DEDUP_QUERY, ("capitol-trades", "MU", today_str)
        ).fetchone()
        self.assertIsNone(row)

    def test_buy_call_variant_action_still_matches_like_pattern(self) -> None:
        today = datetime.now(timezone.utc)
        self._insert_decision("capitol-trades", "BUY_CALL", "MU", "TRADE_REJECTED", False, today)

        row = self.conn.execute(
            _DEDUP_QUERY, ("capitol-trades", "MU", str(today.date()))
        ).fetchone()
        self.assertIsNotNone(row)


if __name__ == "__main__":
    unittest.main()
