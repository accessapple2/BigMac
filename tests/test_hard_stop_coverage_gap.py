"""tests/test_hard_stop_coverage_gap.py — HM-STOP-COVERAGE-GAP-2026-07-09 regression tests.

Covers a live-incident finding (2026-07-09): engine.crew_scanner._check_hard_stops()
iterated only ACTIVE_SCANNERS + RULES_SCANNERS + ALPHA_SQUAD (10 hardcoded player
IDs), so any player outside that fixed union got zero -8% stop-loss enforcement no
matter how far a position ran against them. Live sweep that day found two players
(ollie-auto, alpaca-mirror) breaching -8% with no automated response; a manual
emergency flatten was required (see docs/XO_BACKLOG.md).

The fix (_hard_stop_eligible_players()) replaces the static union with a DB query
for every player actually holding an open stock position. These tests prove the
player-universe fix on an isolated temp DB (no network/broker I/O), then prove the
full _check_hard_stops() call chain closes a breaching position for a player who
was NOT in the old static lists (dry-run harness via mocked sell()/get_stock_price()).
"""
from __future__ import annotations

import sqlite3
import unittest
from pathlib import Path
from unittest.mock import patch

from engine.crew_scanner import (
    ACTIVE_SCANNERS,
    ALPHA_SQUAD,
    RULES_SCANNERS,
    _check_hard_stops,
    _hard_stop_eligible_players,
)

_UNCOVERED_PLAYER = "ollie-auto"  # real player id, confirmed outside the old static lists


def _make_positions_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "hard_stop_test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE positions (
            id INTEGER PRIMARY KEY,
            player_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            qty REAL,
            avg_price REAL,
            asset_type TEXT DEFAULT 'stock',
            option_type TEXT,
            strike_price REAL,
            expiry_date TEXT,
            opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            high_watermark REAL,
            conviction REAL,
            conviction_source TEXT
        )
    """)
    conn.execute(
        "INSERT INTO positions (player_id, symbol, qty, avg_price, asset_type) "
        "VALUES (?, 'ZZZZ', 1.0, 100.0, 'stock')",
        (_UNCOVERED_PLAYER,),
    )
    conn.commit()
    conn.close()
    return db_path


class HardStopEligiblePlayersTests(unittest.TestCase):
    def setUp(self) -> None:
        import tempfile
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = _make_positions_db(Path(self._tmpdir.name))

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def test_uncovered_player_confirmed_outside_old_static_lists(self) -> None:
        # Sanity check the premise: this player must NOT be in the old
        # hardcoded union, or the regression test proves nothing.
        old_union = set(ACTIVE_SCANNERS + RULES_SCANNERS + ALPHA_SQUAD)
        self.assertNotIn(_UNCOVERED_PLAYER, old_union)

    def test_dynamic_query_includes_previously_uncovered_player(self) -> None:
        with patch("engine.crew_scanner.DB_PATH", str(self.db_path)):
            players = _hard_stop_eligible_players()
        self.assertIn(_UNCOVERED_PLAYER, players)

    def test_dynamic_query_falls_back_to_static_union_on_db_error(self) -> None:
        with patch("engine.crew_scanner.DB_PATH", "/nonexistent/path/does-not-exist.db"):
            players = _hard_stop_eligible_players()
        self.assertEqual(set(players), set(ACTIVE_SCANNERS + RULES_SCANNERS + ALPHA_SQUAD))


class CheckHardStopsDryRunTests(unittest.TestCase):
    """Dry-run harness: prove a breaching position for a previously-uncovered
    player actually gets sold on the next _check_hard_stops() cycle."""

    def setUp(self) -> None:
        import tempfile
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = _make_positions_db(Path(self._tmpdir.name))

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def test_breaching_position_for_uncovered_player_gets_sold(self) -> None:
        fake_portfolio = {
            "positions": [
                {"symbol": "ZZZZ", "avg_price": 100.0, "qty": 1.0, "asset_type": "stock"},
            ]
        }
        # -8.5% -- past the -8% floor
        fake_price = {"price": 91.50}
        sell_calls = []

        def fake_sell(**kwargs):
            sell_calls.append(kwargs)
            return {"action": "SELL", "symbol": kwargs["symbol"]}

        with patch("engine.crew_scanner.DB_PATH", str(self.db_path)), \
             patch("engine.paper_trader.get_portfolio", return_value=fake_portfolio), \
             patch("engine.paper_trader.sell", side_effect=fake_sell), \
             patch("engine.market_data.get_stock_price", return_value=fake_price):
            cut = _check_hard_stops()

        self.assertEqual(cut, 1)
        self.assertEqual(len(sell_calls), 1)
        self.assertEqual(sell_calls[0]["player_id"], _UNCOVERED_PLAYER)
        self.assertEqual(sell_calls[0]["symbol"], "ZZZZ")

    def test_non_breaching_position_is_not_sold(self) -> None:
        fake_portfolio = {
            "positions": [
                {"symbol": "ZZZZ", "avg_price": 100.0, "qty": 1.0, "asset_type": "stock"},
            ]
        }
        # -7.98% -- inside the -8% floor, must NOT sell
        fake_price = {"price": 92.02}
        sell_calls = []

        def fake_sell(**kwargs):
            sell_calls.append(kwargs)
            return {"action": "SELL", "symbol": kwargs["symbol"]}

        with patch("engine.crew_scanner.DB_PATH", str(self.db_path)), \
             patch("engine.paper_trader.get_portfolio", return_value=fake_portfolio), \
             patch("engine.paper_trader.sell", side_effect=fake_sell), \
             patch("engine.market_data.get_stock_price", return_value=fake_price):
            cut = _check_hard_stops()

        self.assertEqual(cut, 0)
        self.assertEqual(len(sell_calls), 0)


if __name__ == "__main__":
    unittest.main()
