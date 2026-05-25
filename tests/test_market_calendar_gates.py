"""HM-MARKET-HOLIDAY-CALENDAR Phase B tests — gate coverage.

Verifies that the seven hard-gate sites and one soft-update site all
correctly block (or allow) based on market_calendar state. Uses
unittest.mock.patch to stub get_market_status at the engine.market_calendar
module level so all gate sites see the mocked state.
"""
from __future__ import annotations

import unittest
from datetime import datetime
from unittest.mock import patch

import pytz

import engine.market_calendar as mcal
from engine.market_calendar import MarketStatus


ET = mcal.ET


def _et(year, month, day, hour=0, minute=0) -> datetime:
    return ET.localize(datetime(year, month, day, hour, minute))


class MarketCalendarGateTests(unittest.TestCase):
    """Pin time via patching get_market_status; verify each gate's response."""

    # ── Helper: stub get_market_status to a fixed value ─────────────────

    def _stub_status(self, status: MarketStatus):
        """Returns a context manager that pins get_market_status."""
        return patch.object(mcal, "get_market_status", return_value=status)

    def _stub_status_and_holiday(self, status: MarketStatus, holiday_name=None):
        """Patches both status and holiday-name lookup (for reason strings)."""
        return [
            patch.object(mcal, "get_market_status", return_value=status),
            patch.object(mcal, "get_holiday_name", return_value=holiday_name),
        ]

    # ── 1. paper_trader.buy() blocks on closed market ───────────────────

    def test_paper_trader_buy_blocked_on_holiday(self) -> None:
        from engine import paper_trader
        with self._stub_status(MarketStatus.CLOSED_HOLIDAY), \
             patch.object(mcal, "get_holiday_name", return_value="Memorial Day"):
            result = paper_trader.buy(
                player_id="neo-matrix", symbol="QQQ", price=500.0,
                qty=1.0, confidence=0.85,
            )
        self.assertIsNone(result, "buy should return None on holiday")
        # Last rejection should mention HM-MARKET-CLOSED tag
        self.assertIn(
            "HM-MARKET-CLOSED",
            paper_trader._last_rejection.get("neo-matrix", ""),
        )

    def test_paper_trader_buy_blocked_on_weekend(self) -> None:
        from engine import paper_trader
        with self._stub_status(MarketStatus.CLOSED_WEEKEND):
            result = paper_trader.buy(
                player_id="neo-matrix", symbol="QQQ", price=500.0,
                qty=1.0, confidence=0.85,
            )
        self.assertIsNone(result)
        self.assertIn(
            "weekend",
            paper_trader._last_rejection.get("neo-matrix", "").lower(),
        )

    # ── 2. paper_trader.sell() blocks on closed market ──────────────────

    def test_paper_trader_sell_blocked_on_holiday(self) -> None:
        from engine import paper_trader
        with self._stub_status(MarketStatus.CLOSED_HOLIDAY), \
             patch.object(mcal, "get_holiday_name", return_value="Memorial Day"):
            result = paper_trader.sell(
                player_id="ollie-auto", symbol="ZM", price=100.0,
            )
        self.assertIsNone(result)
        self.assertIn(
            "HM-MARKET-CLOSED",
            paper_trader._last_rejection.get("ollie-auto", ""),
        )

    # ── 3. paper_trader.short_sell() blocks on closed market ────────────

    def test_paper_trader_short_sell_blocked_on_holiday(self) -> None:
        from engine import paper_trader
        with self._stub_status(MarketStatus.CLOSED_HOLIDAY), \
             patch.object(mcal, "get_holiday_name", return_value="Memorial Day"):
            result = paper_trader.short_sell(
                player_id="neo-matrix", symbol="TSLA", price=200.0,
                qty=1.0,
            )
        self.assertIsNone(result)

    # ── 4-6. alpaca_bridge.buy/sell/short_sell block on closed market ──

    def test_alpaca_bridge_buy_blocked(self) -> None:
        from engine.alpaca_bridge import alpaca
        with self._stub_status(MarketStatus.CLOSED_HOLIDAY), \
             patch.object(mcal, "get_holiday_name", return_value="Memorial Day"):
            result = alpaca.buy("QQQ", 1.0, agent_id="test")
        self.assertIsInstance(result, dict)
        self.assertIn("error", result)
        self.assertIn("market_closed", result["error"])

    def test_alpaca_bridge_sell_blocked(self) -> None:
        from engine.alpaca_bridge import alpaca
        with self._stub_status(MarketStatus.CLOSED_WEEKEND):
            result = alpaca.sell("ZM", 1.0, agent_id="test")
        self.assertIn("error", result)
        self.assertIn("market_closed", result["error"])

    def test_alpaca_bridge_short_sell_blocked(self) -> None:
        from engine.alpaca_bridge import alpaca
        with self._stub_status(MarketStatus.CLOSED_BEFORE_HOURS):
            result = alpaca.short_sell("TSLA", 1.0, agent_id="test")
        self.assertIn("error", result)
        self.assertIn("market_closed", result["error"])

    # ── 7. alpaca_options.execute_options_signal blocked ────────────────

    def test_alpaca_options_execute_signal_blocked(self) -> None:
        from engine.alpaca_options import execute_options_signal
        with self._stub_status(MarketStatus.CLOSED_HOLIDAY), \
             patch.object(mcal, "get_holiday_name", return_value="Memorial Day"):
            result = execute_options_signal(
                player_id="bull_call_spread_v1", action="BUY_CALL",
                symbol="QQQ", current_price=500.0,
            )
        self.assertTrue(result.get("market_closed"))
        self.assertTrue(result.get("skipped"))
        self.assertIn("market_closed", result.get("error", ""))

    # ── 8. risk_manager.is_market_hours holiday-aware ───────────────────

    def test_risk_manager_holiday_aware(self) -> None:
        from engine.risk_manager import RiskManager
        # is_market_hours imports is_us_market_holiday locally; patch the
        # source module so the lookup at call time hits the mock.
        with patch.object(mcal, "is_us_market_holiday", return_value=True):
            self.assertFalse(RiskManager.is_market_hours())

    # ── Allow-through cases: OPEN status → no block ─────────────────────

    def test_alpaca_bridge_buy_open_market_no_block(self) -> None:
        """Gate must allow when status is OPEN. The bridge will fail with
        a different error (no Alpaca creds in test env) but the
        market_closed error must NOT appear."""
        from engine.alpaca_bridge import alpaca
        with self._stub_status(MarketStatus.OPEN):
            result = alpaca.buy("QQQ", 1.0, agent_id="test")
        # Whatever the result, it must not be a market_closed block.
        if isinstance(result, dict) and "error" in result:
            self.assertNotIn("market_closed", result["error"])

    def test_alpaca_options_open_market_no_block(self) -> None:
        from engine.alpaca_options import execute_options_signal
        with self._stub_status(MarketStatus.OPEN):
            result = execute_options_signal(
                player_id="bull_call_spread_v1", action="BUY_CALL",
                symbol="QQQ", current_price=500.0,
            )
        # OPEN should NOT produce market_closed; instead the function
        # proceeds (and may fail later for other reasons like missing
        # OPTIONS_PLAYERS membership).
        self.assertNotEqual(result.get("market_closed"), True)


if __name__ == "__main__":
    unittest.main()
