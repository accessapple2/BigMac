"""HM-PROVING-GROUND-FORMALIZE-V2 SUB-2 — state evaluator unit tests.

Focuses on the pure ``_evaluate_state`` function (state machine logic) so
the threshold semantics are pinned regardless of DB plumbing.
"""
from __future__ import annotations

import sqlite3
import unittest
from datetime import date, datetime
from unittest.mock import patch

from engine.proving_ground import (
    _evaluate_state,
    _consecutive_state_days,
    ship_kill_evaluator,
    DD_GUARD_PCT_ABS,
    SHIP_CONSECUTIVE_DAYS,
    SHIP_GO_COUNT_MIN,
    KILL_GO_COUNT_THRESHOLD,
    KILL_GO_COUNT_DAYS,
    WARN_GO_COUNT_LO,
    WARN_GO_COUNT_HI,
    WARN_DAYS,
    FORCED_EVAL_DAY,
    ESCALATION_DAYS,
    ESCALATION_REPEAT_DAYS,
    TRIAL_START,
)


def _row(d: str, go: int, dd: float, trades: int = 100) -> dict:
    return {
        "as_of_date": d,
        "go_count": go,
        "max_drawdown": dd,
        "total_trades": trades,
        "exit_status": "pending",
    }


class EvaluatorStateMachineTests(unittest.TestCase):
    # ── pending stays pending when history is short ─────────────────────
    def test_pending_stays_pending_short_history(self) -> None:
        history = [_row("2026-05-22", go=4, dd=-10.0)]
        target, m = _evaluate_state(history, "pending", trial_day=46)
        self.assertEqual(target, "pending")

    # ── warning fires when 4/6 for 5 consecutive days ───────────────────
    def test_warning_on_5_day_4of6_streak(self) -> None:
        history = [_row(f"2026-05-{20+i}", go=4, dd=-10.0) for i in range(WARN_DAYS)]
        target, m = _evaluate_state(history, "pending", trial_day=46)
        self.assertEqual(target, "warning")
        self.assertEqual(m["warning_streak"], WARN_DAYS)

    # ── ship_ready fires when SHIP conditions hold 10 days ──────────────
    def test_ship_ready_on_10_day_ship_streak(self) -> None:
        # 10 days of go_count>=5/6 AND |dd|<=15
        history = [_row(f"2026-05-{10+i}", go=5, dd=-12.0)
                   for i in range(SHIP_CONSECUTIVE_DAYS)]
        target, m = _evaluate_state(history, "warning", trial_day=46)
        self.assertEqual(target, "ship_ready")
        self.assertEqual(m["ship_streak"], SHIP_CONSECUTIVE_DAYS)

    # ── ship_ready REJECTED if dd exceeds guard on any day ──────────────
    def test_ship_blocked_by_single_dd_breach(self) -> None:
        # 9 clean days + 1 day with -16% drawdown
        history = [_row(f"2026-05-{10+i}", go=5, dd=-12.0)
                   for i in range(SHIP_CONSECUTIVE_DAYS - 1)]
        history.insert(3, _row("2026-05-13", go=5, dd=-16.0))  # breach in middle
        history = history[:SHIP_CONSECUTIVE_DAYS]
        target, m = _evaluate_state(history, "warning", trial_day=46)
        self.assertNotEqual(target, "ship_ready")

    # ── kill_warning K1: dd worse than guard past Day 60 ────────────────
    def test_kill_warning_dd_past_day60(self) -> None:
        history = [_row("2026-06-10", go=4, dd=-24.0)]
        target, m = _evaluate_state(history, "warning", trial_day=62)
        self.assertEqual(target, "kill_warning")
        self.assertEqual(m["kill_trigger"], "dd_past_day60")

    # ── kill_warning K1 DOES NOT fire before Day 60 (regression of dry-run) ──
    def test_kill_warning_dd_not_fired_before_day60(self) -> None:
        # The actual production state on 2026-05-25: dd=-24%, trial_day=46
        history = [_row(f"2026-05-{10+i}", go=4, dd=-24.0) for i in range(20)]
        target, m = _evaluate_state(history, "pending", trial_day=46)
        # Should be 'warning' (4/6 streak), NOT 'kill_warning'
        self.assertEqual(target, "warning")

    # ── kill_warning K2: go_count < 3 for 10 days ───────────────────────
    def test_kill_warning_go_count_collapse(self) -> None:
        history = [_row(f"2026-05-{10+i}", go=2, dd=-10.0)
                   for i in range(KILL_GO_COUNT_DAYS)]
        target, m = _evaluate_state(history, "warning", trial_day=46)
        self.assertEqual(target, "kill_warning")
        self.assertEqual(m["kill_trigger"], "go_count_collapse")

    # ── terminal states are sticky ──────────────────────────────────────
    def test_shipped_is_sticky(self) -> None:
        history = [_row("2026-05-22", go=2, dd=-30.0)]  # would trigger kill if not sticky
        target, m = _evaluate_state(history, "shipped", trial_day=62)
        self.assertEqual(target, "shipped")
        self.assertTrue(m["sticky"])

    def test_killed_is_sticky(self) -> None:
        history = [_row("2026-05-22", go=6, dd=-5.0)]  # would trigger ship if not sticky
        target, m = _evaluate_state(history, "killed", trial_day=46)
        self.assertEqual(target, "killed")
        self.assertTrue(m["sticky"])

    # ── kill_warning K3: trade collapse > 50% ───────────────────────────
    def test_kill_warning_trades_collapse(self) -> None:
        # Prior 10d window: 100→200 (delta=100). Recent 10d: 200→230 (delta=30 < 50).
        history = [_row("2026-05-22", go=4, dd=-10.0, trades=230)]
        history += [_row(f"2026-05-{12+i}", go=4, dd=-10.0, trades=230 - i*3)
                    for i in range(9)]
        history += [_row(f"2026-05-{2+i}", go=4, dd=-10.0, trades=200 - i*11)
                    for i in range(10)]
        target, m = _evaluate_state(history, "warning", trial_day=46)
        self.assertEqual(target, "kill_warning")
        self.assertEqual(m["kill_trigger"], "trades_collapse")


class ConsecutiveStateDaysTests(unittest.TestCase):
    """HM-PG-ESCALATION 2026-07-02: pure helper, same shape as _evaluate_state."""

    def test_counts_matching_prefix_only(self) -> None:
        history = [
            {"exit_status": "kill_warning"},
            {"exit_status": "kill_warning"},
            {"exit_status": "kill_warning"},
            {"exit_status": "pending"},
        ]
        self.assertEqual(_consecutive_state_days(history, "kill_warning"), 3)

    def test_empty_history_is_zero(self) -> None:
        self.assertEqual(_consecutive_state_days([], "kill_warning"), 0)

    def test_no_match_at_all_is_zero(self) -> None:
        history = [{"exit_status": "pending"}]
        self.assertEqual(_consecutive_state_days(history, "kill_warning"), 0)

    def test_missing_exit_status_defaults_to_pending(self) -> None:
        history = [{}]
        self.assertEqual(_consecutive_state_days(history, "pending"), 1)


def _make_pg_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE running_scorecard (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            as_of_date TEXT NOT NULL UNIQUE,
            total_trades INTEGER DEFAULT 0,
            max_drawdown REAL DEFAULT 0.0,
            go_count INTEGER DEFAULT 0,
            exit_status TEXT DEFAULT 'pending'
        );
        CREATE TABLE state_transitions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            transition_at TEXT NOT NULL DEFAULT (datetime('now')),
            from_state TEXT NOT NULL,
            to_state TEXT NOT NULL,
            trigger_metrics_json TEXT NOT NULL,
            ntfy_sent INTEGER NOT NULL DEFAULT 0
        );
    """)
    conn.commit()


class ShipKillEvaluatorPrevStateBugTests(unittest.TestCase):
    """HM-PG-ESCALATION 2026-07-02: regression tests for the root-caused bug
    (prev_state read today's own not-yet-classified row) and the new
    escalation behavior it unblocks. Mocks az_now (deterministic trial_day),
    _fire (no real NTFY sends), and _conn_trader (no real trader.db writes)
    -- these tests must never touch a live NTFY topic or the real database.
    """

    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        _make_pg_schema(self.conn)

    def tearDown(self) -> None:
        self.conn.close()

    def _seed_kill_warning_streak(self, n_days: int, start_day_offset: int) -> None:
        """Seed n_days of already-correctly-persisted kill_warning rows
        (matching the REAL production data shape confirmed this session --
        every row's exit_status genuinely says kill_warning; the bug was
        never in the data, only in how prev_state was read)."""
        for i in range(n_days):
            trial_day = start_day_offset + i
            d = (TRIAL_START.toordinal() + trial_day - 1)
            as_of = date.fromordinal(d).isoformat()
            self.conn.execute(
                "INSERT INTO running_scorecard "
                "(as_of_date, total_trades, max_drawdown, go_count, exit_status) "
                "VALUES (?, ?, ?, ?, 'kill_warning')",
                (as_of, 100 + i, -43.03, 4),
            )
        self.conn.commit()

    def test_no_spurious_transition_on_continuing_kill_warning(self) -> None:
        """The core bug: 3 days already correctly show kill_warning in the
        data; evaluating a 4th day should NOT report a fresh transition."""
        self._seed_kill_warning_streak(n_days=3, start_day_offset=61)
        fake_today = date.fromordinal(TRIAL_START.toordinal() + 63)  # trial_day 64
        # Insert today's row the way the real scorecard writer does: numeric
        # fields populated, exit_status left at its schema default.
        self.conn.execute(
            "INSERT INTO running_scorecard (as_of_date, total_trades, max_drawdown, go_count) "
            "VALUES (?, ?, ?, ?)",
            (fake_today.isoformat(), 104, -43.03, 4),
        )
        self.conn.commit()

        with patch("engine.proving_ground.az_now") as mock_now, \
             patch("engine.proving_ground._fire"), \
             patch("engine.proving_ground._conn_trader") as mock_trader:
            mock_now.return_value.date.return_value = fake_today
            mock_trader.return_value.execute.return_value.fetchone.return_value = None
            result = ship_kill_evaluator(pg_conn=self.conn)

        self.assertEqual(result["prev_state"], "kill_warning")
        self.assertEqual(result["target_state"], "kill_warning")
        self.assertFalse(result["transitioned"], (
            "prev_state bug regression: a continuing kill_warning day must "
            "not be reported as a fresh transition"
        ))
        self.assertEqual(result["consec_days"], 4)  # 3 seeded + today

    def test_todays_row_still_gets_classified_without_a_transition(self) -> None:
        """Fixing the 'always update' path: even when transitioned=False,
        today's own row must still end up with the real exit_status, not
        stuck at the schema default -- this is the regression the old
        transitioned-only UPDATE would have reintroduced."""
        self._seed_kill_warning_streak(n_days=2, start_day_offset=61)
        fake_today = date.fromordinal(TRIAL_START.toordinal() + 62)
        self.conn.execute(
            "INSERT INTO running_scorecard (as_of_date, total_trades, max_drawdown, go_count) "
            "VALUES (?, ?, ?, ?)",
            (fake_today.isoformat(), 103, -43.03, 4),
        )
        self.conn.commit()

        with patch("engine.proving_ground.az_now") as mock_now, \
             patch("engine.proving_ground._fire"), \
             patch("engine.proving_ground._conn_trader") as mock_trader:
            mock_now.return_value.date.return_value = fake_today
            mock_trader.return_value.execute.return_value.fetchone.return_value = None
            ship_kill_evaluator(pg_conn=self.conn)

        row = self.conn.execute(
            "SELECT exit_status FROM running_scorecard WHERE as_of_date = ?",
            (fake_today.isoformat(),),
        ).fetchone()
        self.assertEqual(row["exit_status"], "kill_warning")

    def test_escalation_fires_at_threshold_not_before(self) -> None:
        """ESCALATION_DAYS - 1 consecutive days must NOT escalate; the Nth
        day must."""
        n_before = ESCALATION_DAYS - 1
        self._seed_kill_warning_streak(n_days=n_before, start_day_offset=61)
        fake_today = date.fromordinal(TRIAL_START.toordinal() + 60 + n_before)

        with patch("engine.proving_ground.az_now") as mock_now, \
             patch("engine.proving_ground._fire"), \
             patch("engine.proving_ground._conn_trader") as mock_trader:
            mock_now.return_value.date.return_value = fake_today
            mock_trader.return_value.execute.return_value.fetchone.return_value = None
            # Not yet at threshold: n_before seeded days + today = ESCALATION_DAYS.
            # We want to test the day BEFORE that, so seed one fewer.
            self.conn.execute("DELETE FROM running_scorecard")
            self._seed_kill_warning_streak(n_days=n_before - 1, start_day_offset=61)
            self.conn.execute(
                "INSERT INTO running_scorecard (as_of_date, total_trades, max_drawdown, go_count) "
                "VALUES (?, ?, ?, ?)",
                (fake_today.isoformat(), 100, -43.03, 4),
            )
            self.conn.commit()
            result_before = ship_kill_evaluator(pg_conn=self.conn)

        self.assertLess(result_before["consec_days"], ESCALATION_DAYS)
        self.assertIsNone(result_before["escalation"])

        # Now the actual threshold day.
        self.conn.execute("DELETE FROM running_scorecard")
        self._seed_kill_warning_streak(n_days=ESCALATION_DAYS - 1, start_day_offset=61)
        fake_today2 = date.fromordinal(TRIAL_START.toordinal() + 60 + (ESCALATION_DAYS - 1))
        self.conn.execute(
            "INSERT INTO running_scorecard (as_of_date, total_trades, max_drawdown, go_count) "
            "VALUES (?, ?, ?, ?)",
            (fake_today2.isoformat(), 100, -43.03, 4),
        )
        self.conn.commit()

        with patch("engine.proving_ground.az_now") as mock_now, \
             patch("engine.proving_ground._fire") as mock_fire, \
             patch("engine.proving_ground._conn_trader") as mock_trader:
            mock_now.return_value.date.return_value = fake_today2
            mock_trader.return_value.execute.return_value.fetchone.return_value = {"halt_mode": "active"}
            result_at = ship_kill_evaluator(pg_conn=self.conn)

        self.assertEqual(result_at["consec_days"], ESCALATION_DAYS)
        self.assertIsNotNone(result_at["escalation"])
        self.assertTrue(result_at["escalation"]["ntfy_sent"])
        self.assertTrue(result_at["escalation"]["halt_applied"])
        self.assertTrue(mock_fire.called)

    def test_escalation_skips_halt_if_already_halted(self) -> None:
        """ollie-auto has been exit_only since the unrelated Door-1 cut --
        escalation must not report halt_applied when it's already halted,
        and must not error."""
        self._seed_kill_warning_streak(n_days=ESCALATION_DAYS - 1, start_day_offset=61)
        fake_today = date.fromordinal(TRIAL_START.toordinal() + 60 + (ESCALATION_DAYS - 1))
        self.conn.execute(
            "INSERT INTO running_scorecard (as_of_date, total_trades, max_drawdown, go_count) "
            "VALUES (?, ?, ?, ?)",
            (fake_today.isoformat(), 100, -43.03, 4),
        )
        self.conn.commit()

        with patch("engine.proving_ground.az_now") as mock_now, \
             patch("engine.proving_ground._fire"), \
             patch("engine.proving_ground._conn_trader") as mock_trader:
            mock_now.return_value.date.return_value = fake_today
            mock_trader.return_value.execute.return_value.fetchone.return_value = {"halt_mode": "exit_only"}
            result = ship_kill_evaluator(pg_conn=self.conn)

        self.assertFalse(result["escalation"]["halt_applied"])
        self.assertEqual(result["escalation"]["halt_skipped_reason"], "already_exit_only")


if __name__ == "__main__":
    unittest.main()
