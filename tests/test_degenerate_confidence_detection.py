"""tests/test_degenerate_confidence_detection.py — HM-DEGENERATE-CONFIDENCE-2026-07-09.

Covers a live-incident finding (2026-07-09): McCoy's (ollama-plutus) local
model collapsed to emitting EXACTLY 0.85 confidence on 100% of BUY_CALL
signals starting 2026-07-08 (45/45 that day) -- it used to vary (0.8-0.95).
Confirmed this is a real model-behavior degradation, not a code bug (nothing
in the parse path hardcodes confidence); nothing caught it automatically,
it was found by manual review.

engine/crew_scanner.py::_check_degenerate_confidence() flags any player
whose last N=10 signals all carry the identical confidence value -- fires
an alert and persists an advisory flag (confidence_reliability_flags table)
once per transition into the flagged state, and auto-clears once fresh
variation reappears. is_confidence_reliable() is the advisory read for
other systems to consult; nothing is auto-blocked by this (explicitly
advisory-only per the directive).
"""
from __future__ import annotations

import sqlite3
import unittest
from pathlib import Path
from unittest.mock import patch

import engine.crew_scanner as crew_scanner


def _make_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "degenerate_conf_test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            signal TEXT NOT NULL,
            confidence REAL,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    conn.close()
    return db_path


class DegenerateConfidenceDetectionTests(unittest.TestCase):
    def setUp(self) -> None:
        import tempfile
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = _make_db(Path(self._tmpdir.name))
        self._patcher = patch.object(crew_scanner, "DB_PATH", str(self.db_path))
        self._patcher.start()

    def tearDown(self) -> None:
        self._patcher.stop()
        self._tmpdir.cleanup()

    def _insert_signals(self, player_id: str, confidences: list[float]) -> None:
        conn = sqlite3.connect(str(self.db_path))
        for i, conf in enumerate(confidences):
            # older rows get earlier timestamps -- most recent last in the list
            conn.execute(
                "INSERT INTO signals (player_id, symbol, signal, confidence, created_at) "
                "VALUES (?, 'AAPL', 'BUY_CALL', ?, datetime('now', ?))",
                (player_id, conf, f"-{len(confidences) - i} minutes"),
            )
        conn.commit()
        conn.close()

    @patch("engine.alert_channels.send_alert")
    def test_flags_player_with_10_identical_confidences(self, mock_alert) -> None:
        self._insert_signals("mccoy-test", [0.85] * 10)

        flagged = crew_scanner._check_degenerate_confidence()

        self.assertEqual(flagged, 1)
        mock_alert.assert_called_once()
        self.assertFalse(crew_scanner.is_confidence_reliable("mccoy-test"))

    @patch("engine.alert_channels.send_alert")
    def test_does_not_flag_varying_confidence(self, mock_alert) -> None:
        self._insert_signals("spock-test", [0.8, 0.82, 0.87, 0.89, 0.9, 0.85, 0.86, 0.91, 0.84, 0.88])

        flagged = crew_scanner._check_degenerate_confidence()

        self.assertEqual(flagged, 0)
        mock_alert.assert_not_called()
        self.assertTrue(crew_scanner.is_confidence_reliable("spock-test"))

    @patch("engine.alert_channels.send_alert")
    def test_does_not_flag_fewer_than_10_signals(self, mock_alert) -> None:
        """9 identical values isn't enough history -- must not false-positive
        on a player with sparse signal history."""
        self._insert_signals("new-agent-test", [0.85] * 9)

        flagged = crew_scanner._check_degenerate_confidence()

        self.assertEqual(flagged, 0)
        mock_alert.assert_not_called()

    @patch("engine.alert_channels.send_alert")
    def test_rules_based_scanner_not_flagged_despite_identical_confidence(self, mock_alert) -> None:
        """Live-confirmed false positive during development: capitol-trades
        (RULES_SCANNERS -- deterministic weighted-score formula, not an
        LLM) flagged on its first real run for 10x confidence=0.80, its
        legitimate steady-state output for that condition set. This check
        is specifically about LLM output degradation and must exclude
        RULES_SCANNERS entirely."""
        self.assertIn("capitol-trades", crew_scanner.RULES_SCANNERS)
        self._insert_signals("capitol-trades", [0.80] * 10)

        flagged = crew_scanner._check_degenerate_confidence()

        self.assertEqual(flagged, 0)
        mock_alert.assert_not_called()
        self.assertTrue(crew_scanner.is_confidence_reliable("capitol-trades"))

    @patch("engine.alert_channels.send_alert")
    def test_edge_triggered_does_not_realert_every_cycle(self, mock_alert) -> None:
        self._insert_signals("mccoy-test", [0.85] * 10)

        crew_scanner._check_degenerate_confidence()
        crew_scanner._check_degenerate_confidence()
        crew_scanner._check_degenerate_confidence()

        mock_alert.assert_called_once()  # only the first transition alerts

    @patch("engine.alert_channels.send_alert")
    def test_auto_clears_when_variation_returns(self, mock_alert) -> None:
        self._insert_signals("mccoy-test", [0.85] * 10)
        crew_scanner._check_degenerate_confidence()
        self.assertFalse(crew_scanner.is_confidence_reliable("mccoy-test"))

        # Fresh varied signal arrives -- next 10-window is no longer degenerate.
        conn = sqlite3.connect(str(self.db_path))
        conn.execute(
            "INSERT INTO signals (player_id, symbol, signal, confidence, created_at) "
            "VALUES ('mccoy-test', 'NVDA', 'BUY_CALL', 0.72, datetime('now'))"
        )
        conn.commit()
        conn.close()

        crew_scanner._check_degenerate_confidence()
        self.assertTrue(crew_scanner.is_confidence_reliable("mccoy-test"))

    def test_never_flagged_player_is_reliable_by_default(self) -> None:
        self.assertTrue(crew_scanner.is_confidence_reliable("nobody-has-ever-heard-of-this-agent"))


if __name__ == "__main__":
    unittest.main()
