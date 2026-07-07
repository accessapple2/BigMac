"""HM-ALERT-COLLAB-LINKS Phase 1 tests (2026-07-06).

CRUD tests use an isolated temp SQLite DB via patch.object(dashboard.app, "DB", ...)
-- same pattern as tests/test_decision_desk.py -- so nothing here touches
production trader.db. Reader-union tests import engine.dynamic_alerts directly
(small, self-contained module, no risky module-level side effects) and also
use a temp DB via monkeypatching its module-level DB constant.
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import dashboard.app as app_module  # noqa: E402
import config  # noqa: E402
from fastapi import HTTPException  # noqa: E402
from engine import dynamic_alerts as da  # noqa: E402


def _make_schema(path):
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE alert_definitions (
            id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, symbol TEXT NOT NULL,
            params_json TEXT NOT NULL, severity TEXT NOT NULL DEFAULT 'info',
            channels_json TEXT NOT NULL DEFAULT '["ntfy"]', note TEXT,
            enabled INTEGER NOT NULL DEFAULT 1, created_by TEXT NOT NULL DEFAULT 'admiral',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, last_triggered_at TIMESTAMP);
        CREATE TABLE dynamic_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL, alert_type TEXT NOT NULL,
            message TEXT NOT NULL, severity TEXT DEFAULT 'info', price REAL,
            triggered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
        """
    )
    conn.commit()
    conn.close()


class AlertDefCrudTests(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        _make_schema(self.db_path)
        self._patch = patch.object(app_module, "DB", self.db_path)
        self._patch.start()

    def tearDown(self):
        self._patch.stop()
        os.unlink(self.db_path)

    def test_create_then_list(self):
        body = app_module.AlertDefCreate(kind="price_level", symbol="aapl",
                                          params={"level": 200.0, "direction": "above"})
        result = app_module.create_alert_def(body)
        self.assertTrue(result["created"])
        rows = app_module.list_alert_defs()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["symbol"], "AAPL")  # uppercased

    def test_create_rejects_bad_kind(self):
        body = app_module.AlertDefCreate(kind="not_a_real_kind", symbol="AAPL", params={})
        with self.assertRaises(HTTPException) as ctx:
            app_module.create_alert_def(body)
        self.assertEqual(ctx.exception.status_code, 400)

    def test_create_rejects_bad_symbol(self):
        body = app_module.AlertDefCreate(kind="trendline", symbol="not-a-symbol!!", params={})
        with self.assertRaises(HTTPException) as ctx:
            app_module.create_alert_def(body)
        self.assertEqual(ctx.exception.status_code, 400)

    def test_create_rejects_bad_params(self):
        body = app_module.AlertDefCreate(kind="price_level", symbol="AAPL",
                                          params={"level": -5, "direction": "above"})
        with self.assertRaises(HTTPException) as ctx:
            app_module.create_alert_def(body)
        self.assertEqual(ctx.exception.status_code, 400)

    def test_note_is_length_capped(self):
        body = app_module.AlertDefCreate(kind="trendline", symbol="AAPL", note="x" * 500)
        app_module.create_alert_def(body)
        rows = app_module.list_alert_defs()
        self.assertLessEqual(len(rows[0]["note"]), app_module._ALERT_DEF_NOTE_MAX)

    def test_patch_toggles_enabled(self):
        new_id = app_module.create_alert_def(
            app_module.AlertDefCreate(kind="trendline", symbol="AAPL"))["id"]
        app_module.patch_alert_def(new_id, app_module.AlertDefPatch(enabled=False))
        rows = app_module.list_alert_defs(enabled_only=True)
        self.assertEqual(len(rows), 0)

    def test_patch_unknown_id_404s(self):
        with self.assertRaises(HTTPException) as ctx:
            app_module.patch_alert_def(99999, app_module.AlertDefPatch(enabled=False))
        self.assertEqual(ctx.exception.status_code, 404)

    def test_patch_validates_new_params_against_existing_kind(self):
        new_id = app_module.create_alert_def(
            app_module.AlertDefCreate(kind="rsi", symbol="AAPL",
                                       params={"direction": "oversold"}))["id"]
        with self.assertRaises(HTTPException):
            app_module.patch_alert_def(new_id, app_module.AlertDefPatch(params={"threshold": 999}))

    def test_list_filters_by_symbol(self):
        app_module.create_alert_def(app_module.AlertDefCreate(kind="trendline", symbol="AAPL"))
        app_module.create_alert_def(app_module.AlertDefCreate(kind="trendline", symbol="MSFT"))
        rows = app_module.list_alert_defs(symbol="msft")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["symbol"], "MSFT")


class DynamicAlertsReaderUnionTests(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        _make_schema(self.db_path)
        self._db_patch = patch.object(da, "DB", self.db_path)
        self._db_patch.start()
        self._flag_patch = patch.object(config, "ALERT_DEFS_ENABLED", True)
        self._flag_patch.start()
        da._alert_cooldown.clear()

    def tearDown(self):
        self._flag_patch.stop()
        self._db_patch.stop()
        os.unlink(self.db_path)

    def _insert_def(self, kind, symbol, params):
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "INSERT INTO alert_definitions (kind, symbol, params_json) VALUES (?,?,?)",
            (kind, symbol, json.dumps(params)),
        )
        conn.commit()
        conn.close()

    def test_flag_off_is_a_noop(self):
        with patch.object(config, "ALERT_DEFS_ENABLED", False):
            self._insert_def("price_level", "TSLA", {"level": 100.0, "direction": "above"})
            result = da.run_user_alert_definitions({"TSLA": {"price": 150.0}}, {"TSLA": {}})
            self.assertEqual(result, [])

    def test_price_level_fires_and_cools_down(self):
        self._insert_def("price_level", "TSLA", {"level": 100.0, "direction": "above"})
        r1 = da.run_user_alert_definitions({"TSLA": {"price": 150.0}}, {"TSLA": {}})
        self.assertEqual(len(r1), 1)
        r2 = da.run_user_alert_definitions({"TSLA": {"price": 151.0}}, {"TSLA": {}})
        self.assertEqual(r2, [])  # cooldown

    def test_rsi_oversold(self):
        self._insert_def("rsi", "NVDA", {"direction": "oversold", "threshold": 30})
        r = da.run_user_alert_definitions({"NVDA": {"price": 100.0}}, {"NVDA": {"rsi": 25}})
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0]["type"], "user_rsi")

    def test_volume_spike(self):
        self._insert_def("volume_spike", "AMD", {"threshold": 2.0})
        r = da.run_user_alert_definitions({"AMD": {"price": 50.0}}, {"AMD": {"volume_ratio": 3.5}})
        self.assertEqual(len(r), 1)

    def test_macd_cross_direction_filter(self):
        self._insert_def("macd_cross", "SPY", {"direction": "bullish", "threshold": 0.1})
        # bearish histogram should NOT fire a bullish-only definition
        r = da.run_user_alert_definitions({"SPY": {"price": 500.0}}, {"SPY": {"macd_histogram": -0.05}})
        self.assertEqual(r, [])

    def test_unknown_kind_is_skipped_not_erroring(self):
        self._insert_def("not_a_real_kind", "AAPL", {})
        r = da.run_user_alert_definitions({"AAPL": {"price": 100.0}}, {"AAPL": {}})
        self.assertEqual(r, [])

    def test_disabled_definition_is_not_evaluated(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "INSERT INTO alert_definitions (kind, symbol, params_json, enabled) VALUES (?,?,?,0)",
            ("price_level", "TSLA", json.dumps({"level": 1.0, "direction": "above"})),
        )
        conn.commit()
        conn.close()
        r = da.run_user_alert_definitions({"TSLA": {"price": 150.0}}, {"TSLA": {}})
        self.assertEqual(r, [])


if __name__ == "__main__":
    unittest.main()
