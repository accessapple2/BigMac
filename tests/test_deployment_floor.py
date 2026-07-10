"""Tests for engine.deployment_floor — the under-deployment advisory
(HM-DEPLOYMENT-FLOOR 2026-07-10, S6 findings Finding 1 / P1).

Incoming patch had zero test coverage. Note: this test suite pins
regime_equity_target_pct() preferring `long_equity_pct` (the real BINDING
target per engine.paper_trader._apply_regime_long_equity_cap) over
`long_equity_max_pct` (a looser backstop ceiling that rarely binds) --
confirmed correct by reading the actual enforcement code, even though the
originating findings report's "65% cap" framing was itself imprecise
(that number is long_equity_max_pct, not the real binding target).
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine import deployment_floor as df  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_done_today_flag():
    df._done_today = False
    yield
    df._done_today = False


@pytest.fixture()
def temp_db(tmp_path):
    db_path = str(tmp_path / "test_trader.db")
    conn = sqlite3.connect(db_path)
    conn.execute("""CREATE TABLE ai_players (
        id TEXT PRIMARY KEY, cash REAL, is_active INTEGER
    )""")
    conn.execute("""CREATE TABLE positions (
        id INTEGER PRIMARY KEY, player_id TEXT, symbol TEXT, qty REAL,
        avg_price REAL, asset_type TEXT
    )""")
    conn.commit()
    conn.close()
    with patch.object(df, "_db_path", return_value=db_path):
        yield db_path


def _insert_player(db_path, player_id, cash, active=1):
    conn = sqlite3.connect(db_path)
    conn.execute("INSERT INTO ai_players (id, cash, is_active) VALUES (?,?,?)", (player_id, cash, active))
    conn.commit()
    conn.close()


def _insert_position(db_path, player_id, symbol, qty, avg_price, asset_type="stock"):
    conn = sqlite3.connect(db_path)
    conn.execute("INSERT INTO positions (player_id, symbol, qty, avg_price, asset_type) VALUES (?,?,?,?,?)",
                 (player_id, symbol, qty, avg_price, asset_type))
    conn.commit()
    conn.close()


# ─── fleet_long_equity_weight() ────────────────────────────────────────────

def test_fleet_long_equity_weight_basic(temp_db):
    _insert_player(temp_db, "modelA", cash=5000.0)
    _insert_position(temp_db, "modelA", "AAPL", 10, 500.0)  # $5000 stock
    weight, total = df.fleet_long_equity_weight()
    assert total == 10000.0
    assert weight == pytest.approx(50.0)


def test_fleet_long_equity_weight_excludes_webull_and_enterprise_computer(temp_db):
    _insert_player(temp_db, "modelA", cash=1000.0)
    _insert_player(temp_db, "webull", cash=99999.0)
    _insert_player(temp_db, "enterprise-computer", cash=99999.0)
    weight, total = df.fleet_long_equity_weight()
    assert total == 1000.0  # excluded players' cash never counted


def test_fleet_long_equity_weight_options_excluded_from_long_equity(temp_db):
    _insert_player(temp_db, "modelA", cash=0.0)
    _insert_position(temp_db, "modelA", "AAPL", 10, 100.0, asset_type="stock")   # $1000 equity
    _insert_position(temp_db, "modelA", "AAPL", 5, 200.0, asset_type="option")   # $1000 option -- not equity
    weight, total = df.fleet_long_equity_weight()
    assert total == 2000.0
    assert weight == pytest.approx(50.0)  # only the $1000 stock leg counts as long equity


def test_fleet_long_equity_weight_inverse_etf_excluded_from_long_equity(temp_db):
    _insert_player(temp_db, "modelA", cash=0.0)
    _insert_position(temp_db, "modelA", "SPY", 10, 100.0)   # $1000 real long equity
    _insert_position(temp_db, "modelA", "SQQQ", 10, 100.0)  # $1000 inverse ETF -- not long equity
    weight, total = df.fleet_long_equity_weight()
    assert total == 2000.0
    assert weight == pytest.approx(50.0)


def test_fleet_long_equity_weight_none_when_no_active_players(temp_db):
    assert df.fleet_long_equity_weight() is None


def test_fleet_long_equity_weight_none_when_total_nonpositive(temp_db):
    _insert_player(temp_db, "modelA", cash=0.0)
    assert df.fleet_long_equity_weight() is None


def test_fleet_long_equity_weight_none_on_db_error():
    with patch.object(df, "_db_path", return_value="/nonexistent/path.db"):
        assert df.fleet_long_equity_weight() is None


# ─── regime_equity_target_pct() ────────────────────────────────────────────

def test_regime_equity_target_prefers_long_equity_pct_over_ceiling():
    """The real binding target per _apply_regime_long_equity_cap is
    long_equity_pct, NOT long_equity_max_pct (a looser backstop ceiling
    that only binds when it's LOWER than the target, which it normally
    isn't). BULL_CROSS in production: long_equity_pct=0.15, ceiling=0.65 --
    the correct target is 15%, not 65%."""
    with patch("engine.regime_router.get_regime_allocation",
               return_value={"long_equity_pct": 0.15, "long_equity_max_pct": 0.65}):
        result = df.regime_equity_target_pct("BULL_CROSS")
    assert result == pytest.approx(15.0)


def test_regime_equity_target_falls_back_to_matrix_ceiling_when_no_allocation_row():
    with patch("engine.regime_router.get_regime_allocation", return_value=None), \
         patch("engine.regime_router.REGIME_STRATEGY_MATRIX", {"BULL": {"long_equity_max_pct": 0.80}}):
        result = df.regime_equity_target_pct("BULL")
    assert result == pytest.approx(80.0)


def test_regime_equity_target_none_for_unknown_regime():
    assert df.regime_equity_target_pct(None) is None
    assert df.regime_equity_target_pct("") is None


def test_regime_equity_target_none_when_nothing_available():
    with patch("engine.regime_router.get_regime_allocation", return_value=None), \
         patch("engine.regime_router.REGIME_STRATEGY_MATRIX", {}):
        assert df.regime_equity_target_pct("BULL") is None


# ─── check_deployment_floor() ──────────────────────────────────────────────

def test_check_deployment_floor_out_of_scope_for_non_bull_regime(temp_db):
    with patch("engine.regime_router.get_current_regime", return_value="BEAR"), \
         patch("engine.alert_channels.send_alert") as mock_alert:
        result = df.check_deployment_floor()
    assert result == {"regime": "BEAR", "in_scope": False}
    mock_alert.assert_not_called()


def test_check_deployment_floor_no_alert_when_above_floor(temp_db):
    _insert_player(temp_db, "modelA", cash=5000.0)
    _insert_position(temp_db, "modelA", "AAPL", 10, 500.0)  # $5000/$10000 = 50%
    with patch("engine.regime_router.get_current_regime", return_value="BULL_CROSS"), \
         patch.object(df, "regime_equity_target_pct", return_value=15.0), \
         patch("engine.alert_channels.send_alert") as mock_alert:
        result = df.check_deployment_floor()
    # floor = 15% * 1/3 = 5%; actual 50% is well above -> no alert
    assert result["breached"] is False
    mock_alert.assert_not_called()


def test_check_deployment_floor_fires_alert_when_below_floor(temp_db):
    _insert_player(temp_db, "modelA", cash=9900.0)
    _insert_position(temp_db, "modelA", "AAPL", 1, 100.0)  # $100/$10000 = 1%
    with patch("engine.regime_router.get_current_regime", return_value="BULL_CROSS"), \
         patch.object(df, "regime_equity_target_pct", return_value=15.0), \
         patch("engine.alert_channels.send_alert") as mock_alert:
        result = df.check_deployment_floor()
    # floor = 15% * 1/3 = 5%; actual 1% is below -> alert fires
    assert result["breached"] is True
    mock_alert.assert_called_once()
    call_kwargs = mock_alert.call_args.kwargs
    assert call_kwargs["alert_type"] == "deployment_floor"
    assert call_kwargs["rate_limit_secs"] == 86400
    assert "source" not in call_kwargs  # HM-BUG-BATCH fix: no longer passes source="INFORMATIONAL"


def test_check_deployment_floor_none_when_no_target(temp_db):
    with patch("engine.regime_router.get_current_regime", return_value="BULL"), \
         patch.object(df, "regime_equity_target_pct", return_value=None):
        assert df.check_deployment_floor() is None


def test_check_deployment_floor_none_when_no_weight_data(temp_db):
    with patch("engine.regime_router.get_current_regime", return_value="BULL"), \
         patch.object(df, "regime_equity_target_pct", return_value=65.0):
        assert df.check_deployment_floor() is None  # no active players -> fleet_long_equity_weight() is None


def test_check_deployment_floor_alert_send_failure_does_not_crash(temp_db):
    _insert_player(temp_db, "modelA", cash=9900.0)
    _insert_position(temp_db, "modelA", "AAPL", 1, 100.0)
    with patch("engine.regime_router.get_current_regime", return_value="BULL_CROSS"), \
         patch.object(df, "regime_equity_target_pct", return_value=15.0), \
         patch("engine.alert_channels.send_alert", side_effect=RuntimeError("ntfy down")):
        result = df.check_deployment_floor()  # must not raise
    assert result["breached"] is True


# ─── run_deployment_floor_check() — scheduler gating ───────────────────────

def test_run_deployment_floor_check_skips_outside_market_hours(temp_db):
    with patch("engine.risk_manager.RiskManager.is_market_hours", return_value=False), \
         patch.object(df, "check_deployment_floor") as mock_check:
        df.run_deployment_floor_check()
    mock_check.assert_not_called()


def test_run_deployment_floor_check_skips_weekend(temp_db):
    saturday = datetime(2026, 7, 11, 8, 30)
    with patch("engine.risk_manager.RiskManager.is_market_hours", return_value="market"), \
         patch("engine.market_calendar.az_now", return_value=saturday), \
         patch.object(df, "check_deployment_floor") as mock_check:
        df.run_deployment_floor_check()
    mock_check.assert_not_called()


def test_run_deployment_floor_check_skips_outside_window(temp_db):
    too_early = datetime(2026, 7, 13, 8, 0)  # before 8:30
    with patch("engine.risk_manager.RiskManager.is_market_hours", return_value="market"), \
         patch("engine.market_calendar.az_now", return_value=too_early), \
         patch.object(df, "check_deployment_floor") as mock_check:
        df.run_deployment_floor_check()
    mock_check.assert_not_called()


def test_run_deployment_floor_check_runs_once_in_window(temp_db):
    in_window = datetime(2026, 7, 13, 8, 45)
    with patch("engine.risk_manager.RiskManager.is_market_hours", return_value="market"), \
         patch("engine.market_calendar.az_now", return_value=in_window), \
         patch.object(df, "check_deployment_floor") as mock_check:
        df.run_deployment_floor_check()
        df.run_deployment_floor_check()  # second tick same window -> must not re-check
    mock_check.assert_called_once()


def test_run_deployment_floor_check_exception_does_not_propagate(temp_db):
    in_window = datetime(2026, 7, 13, 8, 45)
    with patch("engine.risk_manager.RiskManager.is_market_hours", return_value="market"), \
         patch("engine.market_calendar.az_now", return_value=in_window), \
         patch.object(df, "check_deployment_floor", side_effect=RuntimeError("boom")):
        df.run_deployment_floor_check()  # must not raise


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
