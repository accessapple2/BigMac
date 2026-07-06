"""HM-DECISION-DESK-MVP-2026-07-05 tests.

Chain-join correctness tests read the REAL trader.db (read-only, no writes)
against two known historical signal_ids that are both in terminal states
(REJECTED / has a real closed-loop trade link) and will never change again.

Empty-feed and execute-path tests use an isolated temp SQLite DB via
patch.object(dashboard.app, "DB", ...) so nothing here can ever touch
production state. The execute-path tests fully mock engine.paper_trader.buy
and engine.market_data.get_stock_price -- zero live orders, guaranteed.
"""
from __future__ import annotations

import asyncio
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import dashboard.app as app_module  # noqa: E402
from fastapi import HTTPException  # noqa: E402

REAL_DB = os.path.expanduser("~/autonomous-trader/data/trader.db")

# Verified during HM-DESK-SCOPE audit (2026-07-05) -- both terminal, stable.
EXECUTED_SIGNAL_ID = 73981  # NVDA, capitol-trades; real matched trades.signal_id link (trade id 2958)
REJECTED_SIGNAL_ID = 73613  # NVDA, ollama-plutus; gate_reject_log MARKET_CLOSED entry


@unittest.skipUnless(os.path.exists(REAL_DB), "trader.db not present in this environment")
class ChainJoinCorrectnessTests(unittest.TestCase):
    """Read-only against the real trader.db."""

    def test_signal_with_real_trade_link_surfaces_the_trade(self):
        result = app_module.desk_chain(EXECUTED_SIGNAL_ID)
        self.assertEqual(result["signal_id"], EXECUTED_SIGNAL_ID)
        self.assertEqual(result["symbol"], "NVDA")
        kinds = [e["kind"] for e in result["events"]]
        self.assertIn("signal", kinds)
        self.assertIn("trade", kinds)
        trade_events = [e for e in result["events"] if e["kind"] == "trade"]
        self.assertEqual(trade_events[0]["detail"]["symbol"], "NVDA")

    def test_rejected_signal_surfaces_gate_reject_entry(self):
        result = app_module.desk_chain(REJECTED_SIGNAL_ID)
        self.assertEqual(result["status"], "REJECTED")
        gate_events = [e for e in result["events"] if e["kind"] == "gate_reject"]
        self.assertTrue(gate_events, "expected at least one gate_reject_log event")
        self.assertEqual(gate_events[0]["detail"]["gate_name"], "MARKET_CLOSED")

    def test_events_are_chronologically_ordered(self):
        result = app_module.desk_chain(REJECTED_SIGNAL_ID)
        ts_list = [app_module._desk_norm_ts(e["ts"]) for e in result["events"]]
        self.assertEqual(ts_list, sorted(ts_list))

    def test_unknown_signal_id_404s(self):
        with self.assertRaises(HTTPException) as ctx:
            app_module.desk_chain(999999999)
        self.assertEqual(ctx.exception.status_code, 404)

    def test_feed_symbol_mismatch_guard_never_shows_wrong_trade(self):
        """HM-DESK-SCOPE anomaly: 47/65 sampled trades.signal_id links point
        to a DIFFERENT symbol than their signal. desk_feed must never
        surface one of these as `trade` on the wrong signal."""
        result = app_module.desk_feed(limit=200)
        for row in result["signals"]:
            if row["trade"] is not None:
                self.assertEqual(row["trade"]["symbol"], row["symbol"])


def _make_empty_schema(path):
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE ai_players (id TEXT PRIMARY KEY, display_name TEXT, provider TEXT,
            model_id TEXT, can_trade_live INTEGER DEFAULT 0);
        CREATE TABLE signals (id INTEGER PRIMARY KEY, player_id TEXT, symbol TEXT,
            signal TEXT, confidence REAL, reasoning TEXT, asset_type TEXT DEFAULT 'stock',
            option_type TEXT, sources TEXT DEFAULT '', timeframe TEXT DEFAULT 'SWING',
            execution_status TEXT DEFAULT 'PENDING', rejection_reason TEXT, created_at TEXT);
        CREATE TABLE gate_reject_log (id INTEGER PRIMARY KEY, ts TEXT, player_id TEXT,
            symbol TEXT, gate_name TEXT, reason TEXT, signal_id INTEGER, price REAL, confidence REAL);
        CREATE TABLE decision_audit (id INTEGER PRIMARY KEY, event_type TEXT, player_id TEXT,
            symbol TEXT, signal_id INTEGER, regime TEXT, confidence REAL, gate_verdict TEXT,
            reasoning_snippet TEXT, created_at TEXT, raw_confidence REAL, meta_confidence REAL,
            confidence_modifier REAL);
        CREATE TABLE trades (id INTEGER PRIMARY KEY, player_id TEXT, symbol TEXT, action TEXT,
            qty REAL, price REAL, entry_price REAL, exit_price REAL, realized_pnl REAL,
            executed_at TEXT, signal_id INTEGER);
        CREATE TABLE options_trades (id INTEGER PRIMARY KEY, agent_id TEXT, symbol TEXT,
            structure TEXT, max_profit REAL, max_loss REAL, expiration TEXT, status TEXT,
            entry_date TEXT, signal_id INTEGER);
        """
    )
    conn.commit()
    conn.close()


class EmptyFeedTests(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        _make_empty_schema(self.db_path)
        self._patch = patch.object(app_module, "DB", self.db_path)
        self._patch.start()

    def tearDown(self):
        self._patch.stop()
        os.unlink(self.db_path)

    def test_empty_feed_returns_empty_list_not_error(self):
        result = app_module.desk_feed()
        self.assertEqual(result["signals"], [])
        self.assertEqual(result["count"], 0)

    def test_empty_feed_with_filters_still_graceful(self):
        result = app_module.desk_feed(status="EXECUTED", agent="nobody", symbol="ZZZ")
        self.assertEqual(result["count"], 0)


def _make_execute_schema(path):
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE ai_players (id TEXT PRIMARY KEY, display_name TEXT, provider TEXT,
            model_id TEXT, can_trade_live INTEGER DEFAULT 0);
        CREATE TABLE signals (id INTEGER PRIMARY KEY, player_id TEXT, symbol TEXT,
            signal TEXT, confidence REAL, reasoning TEXT, asset_type TEXT DEFAULT 'stock',
            option_type TEXT, sources TEXT DEFAULT '', timeframe TEXT DEFAULT 'SWING',
            execution_status TEXT DEFAULT 'PENDING', rejection_reason TEXT, created_at TEXT);
        """
    )
    conn.execute(
        "INSERT INTO ai_players (id, display_name, provider, model_id, can_trade_live) "
        "VALUES ('desk-manual','Admiral Desk','manual','human',0)"
    )
    conn.execute(
        "INSERT INTO signals (id, player_id, symbol, signal, confidence, reasoning, "
        "execution_status, created_at) VALUES "
        "(1,'ollama-plutus','AAPL','BUY',0.8,'test signal','PENDING','2026-07-05 00:00:00')"
    )
    conn.commit()
    conn.close()


class ExecutePathTests(unittest.TestCase):
    """Fully mocked -- engine.paper_trader.buy is never the real function
    here, so this suite can never place a live (or even real paper) order."""

    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        _make_execute_schema(self.db_path)
        self._db_patch = patch.object(app_module, "DB", self.db_path)
        self._db_patch.start()

    def tearDown(self):
        self._db_patch.stop()
        os.unlink(self.db_path)

    def _row(self):
        c = sqlite3.connect(self.db_path)
        c.row_factory = sqlite3.Row
        row = c.execute("SELECT execution_status FROM signals WHERE id=1").fetchone()
        c.close()
        return row["execution_status"]

    def test_flag_off_execute_is_inert(self):
        with patch.dict(os.environ, {"DESK_EXECUTE_ENABLED": "0"}):
            with self.assertRaises(HTTPException) as ctx:
                app_module.desk_execute_signal(1)
            self.assertEqual(ctx.exception.status_code, 403)
        self.assertEqual(self._row(), "PENDING")

    def test_flag_off_dismiss_is_inert(self):
        with patch.dict(os.environ, {"DESK_EXECUTE_ENABLED": "0"}):
            with self.assertRaises(HTTPException) as ctx:
                app_module.desk_dismiss_signal(1)
            self.assertEqual(ctx.exception.status_code, 403)
        self.assertEqual(self._row(), "PENDING")

    def test_execute_routes_through_desk_manual_and_chokepoint(self):
        mock_buy = MagicMock(return_value={"status": "filled", "qty": 1})
        mock_price = MagicMock(return_value={"price": 200.0})
        with patch.dict(os.environ, {"DESK_EXECUTE_ENABLED": "1"}), \
             patch("engine.paper_trader.buy", mock_buy), \
             patch("engine.market_data.get_stock_price", mock_price):
            result = app_module.desk_execute_signal(1)
        self.assertTrue(result["ok"])
        self.assertEqual(result["player_id"], "desk-manual")
        mock_buy.assert_called_once()
        self.assertEqual(mock_buy.call_args.args[0], "desk-manual")
        self.assertEqual(mock_buy.call_args.kwargs.get("signal_id"), 1)
        self.assertEqual(self._row(), "EXECUTED")

    def test_no_live_price_rolls_back_to_pending(self):
        mock_buy = MagicMock(return_value={"status": "filled"})
        mock_price = MagicMock(return_value=None)
        with patch.dict(os.environ, {"DESK_EXECUTE_ENABLED": "1"}), \
             patch("engine.paper_trader.buy", mock_buy), \
             patch("engine.market_data.get_stock_price", mock_price):
            with self.assertRaises(HTTPException) as ctx:
                app_module.desk_execute_signal(1)
            self.assertEqual(ctx.exception.status_code, 400)
        mock_buy.assert_not_called()
        self.assertEqual(self._row(), "PENDING")

    def test_atomic_claim_prevents_double_execute(self):
        """Simulates the race: claim the row first (as a concurrent request
        would), then a second call must see it's no longer PENDING."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("UPDATE signals SET execution_status='EXECUTING' WHERE id=1")
        conn.commit()
        conn.close()
        with patch.dict(os.environ, {"DESK_EXECUTE_ENABLED": "1"}):
            with self.assertRaises(HTTPException) as ctx:
                app_module.desk_execute_signal(1)
            self.assertEqual(ctx.exception.status_code, 400)

    def test_dismiss_flips_pending_to_dismissed(self):
        with patch.dict(os.environ, {"DESK_EXECUTE_ENABLED": "1"}):
            result = app_module.desk_dismiss_signal(1)
        self.assertTrue(result["ok"])
        self.assertEqual(self._row(), "DISMISSED")


class SSEProxyDisconnectTests(unittest.TestCase):
    """The context-stream proxy must reconnect gracefully on upstream
    failure rather than crash the generator."""

    def test_generator_emits_reconnecting_on_upstream_failure_without_crashing(self):
        async def _run():
            mock_client = MagicMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.stream = MagicMock(side_effect=ConnectionError("upstream down"))
            with patch("httpx.AsyncClient", return_value=mock_client), \
                 patch.object(app_module._asyncio, "sleep", new=AsyncMock()):
                response = await app_module.desk_context_stream()
                agen = response.body_iterator
                first = await agen.__anext__()
                second = await agen.__anext__()
                await agen.aclose()
                return first, second

        first, second = asyncio.run(_run())
        self.assertIn("desk_proxy_connected", first)
        self.assertIn("desk_proxy_reconnecting", second)


if __name__ == "__main__":
    unittest.main()
