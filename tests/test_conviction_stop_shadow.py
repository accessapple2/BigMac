"""HM-FORGE Phase 1.5-D — _CONVICTION_SCALED_STOPS_SHADOW tests.

Load-bearing assertion (directive): shadow mode NEVER appends to / alters the
`actions` list returned by check_stop_loss_take_profit. The conviction-scaled
stop stays observation-only (logs to ghost_conviction_stops) until the live
actuator (_CONVICTION_SCALED_STOPS_ENABLED) is separately enabled.

Mirrors the reload-under-patched-env structure of
tests/test_fleet_trail_conviction_scale.py.
"""
from __future__ import annotations

import importlib
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import engine.risk_manager as rm


def _reload_with(env: dict):
    with patch.dict(os.environ, env):
        importlib.reload(rm)
    return rm


# A loss that trips BOTH the flat (12%) and the conviction-scaled (18% @0.95)
# stop. AI-signal player so the allow-list gate passes.
_POS = {
    "symbol": "TEST", "qty": 10, "avg_price": 100.0,
    "asset_type": "stock", "conviction": 0.95, "high_watermark": 100.0,
}
_PRICES = {"TEST": {"price": 80.0}}      # -20% pnl
_PLAYER = "ollama-plutus"


class ConvictionStopShadowTests(unittest.TestCase):
    def setUp(self) -> None:
        fd, self.db = tempfile.mkstemp(suffix=".db"); os.close(fd)

    def tearDown(self) -> None:
        try:
            os.unlink(self.db)
        except OSError:
            pass
        # restore module to clean default env for downstream tests
        _reload_with({"TRADEMINDS_DB": "", "CONVICTION_SCALED_STOPS_SHADOW": "on",
                      "CONVICTION_SCALED_STOPS_ENABLED": "false"})

    def _actions(self, shadow: str) -> list:
        mod = _reload_with({
            "TRADEMINDS_DB": self.db,
            "CONVICTION_SCALED_STOPS_SHADOW": shadow,
            "CONVICTION_SCALED_STOPS_ENABLED": "false",  # live actuator OFF
        })
        return mod.RiskManager().check_stop_loss_take_profit(
            _PLAYER, [dict(_POS)], _PRICES)

    def _ghost_rows(self) -> list:
        if not os.path.getsize(self.db):
            return []
        conn = sqlite3.connect(self.db)
        try:
            names = [r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")]
            if "ghost_conviction_stops" not in names:
                return []
            return conn.execute(
                "SELECT player_id, symbol, conviction, scaled_stop_pct, "
                "would_fire FROM ghost_conviction_stops").fetchall()
        finally:
            conn.close()

    # --- the load-bearing assertion -------------------------------------
    def test_shadow_does_not_alter_actions(self) -> None:
        """Identical inputs → identical actions whether shadow is ON or OFF.
        Proves the shadow branch has ZERO order-path effect."""
        with_shadow = self._actions("on")
        os.remove(self.db); fd, self.db = tempfile.mkstemp(suffix=".db"); os.close(fd)
        without_shadow = self._actions("off")
        self.assertEqual(with_shadow, without_shadow)

    def test_shadow_adds_no_extra_sell(self) -> None:
        """At -20% only the LIVE flat stop fires — exactly one SELL, sourced
        from the live path (reason mentions Stop-loss), never the shadow."""
        actions = self._actions("on")
        sells = [a for a in actions if a.get("action") == "SELL"]
        self.assertEqual(len(sells), 1)
        self.assertIn("Stop-loss", sells[0]["reason"])

    def test_shadow_logs_would_fire(self) -> None:
        """Shadow ON → a ghost_conviction_stops row with would_fire=1."""
        self._actions("on")
        rows = self._ghost_rows()
        self.assertEqual(len(rows), 1)
        player, symbol, conv, scaled, would = rows[0]
        self.assertEqual((player, symbol), (_PLAYER, "TEST"))
        self.assertAlmostEqual(conv, 0.95)
        self.assertAlmostEqual(scaled, 0.18)   # get_stop_loss_pct(0.95)
        self.assertEqual(would, 1)

    def test_shadow_off_writes_nothing(self) -> None:
        """Shadow OFF → no ghost table / no rows."""
        self._actions("off")
        self.assertEqual(self._ghost_rows(), [])

    def test_non_ai_player_not_shadowed(self) -> None:
        """Categorical-NULL players (alpaca-mirror) are not in the allow-list
        → never shadow-logged even with conviction present."""
        mod = _reload_with({
            "TRADEMINDS_DB": self.db,
            "CONVICTION_SCALED_STOPS_SHADOW": "on",
            "CONVICTION_SCALED_STOPS_ENABLED": "false",
        })
        mod.RiskManager().check_stop_loss_take_profit(
            "alpaca-mirror", [dict(_POS)], _PRICES)
        self.assertEqual(self._ghost_rows(), [])

    def test_shadow_flag_default_on(self) -> None:
        """Default ON (the counter-proposal default) when env unset."""
        mod = _reload_with({"CONVICTION_SCALED_STOPS_SHADOW": "on"})
        self.assertIsInstance(mod._CONVICTION_SCALED_STOPS_SHADOW, bool)
        self.assertTrue(mod._CONVICTION_SCALED_STOPS_SHADOW)
        # and the live actuator stays default-off + independent
        self.assertFalse(mod._CONVICTION_SCALED_STOPS_ENABLED)


if __name__ == "__main__":
    unittest.main()
