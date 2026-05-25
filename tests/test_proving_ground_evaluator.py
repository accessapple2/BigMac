"""HM-PROVING-GROUND-FORMALIZE-V2 SUB-2 — state evaluator unit tests.

Focuses on the pure ``_evaluate_state`` function (state machine logic) so
the threshold semantics are pinned regardless of DB plumbing.
"""
from __future__ import annotations

import unittest
from datetime import date

from engine.proving_ground import (
    _evaluate_state,
    DD_GUARD_PCT_ABS,
    SHIP_CONSECUTIVE_DAYS,
    SHIP_GO_COUNT_MIN,
    KILL_GO_COUNT_THRESHOLD,
    KILL_GO_COUNT_DAYS,
    WARN_GO_COUNT_LO,
    WARN_GO_COUNT_HI,
    WARN_DAYS,
    FORCED_EVAL_DAY,
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


if __name__ == "__main__":
    unittest.main()
