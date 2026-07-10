"""Tests for engine.m5_allocator — the rules-based regime allocator control
seat (HM-M5-BASELINE-ALLOCATOR 2026-07-10, JPM control-arm pattern).

Incoming patch had zero test coverage; this module places real paper trades
via engine.paper_trader.buy()/sell_partial(), so it gets full coverage
before being trusted, per house doctrine for trading-adjacent code.
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine import m5_allocator as m5  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_done_today_flag():
    """_done_today is deliberate cross-call module state (restart-safe daily
    dedup) -- exactly the kind of global that leaks between tests unless
    reset. Autouse so every test in this file starts from a clean gate."""
    m5._done_today = False
    yield
    m5._done_today = False


@pytest.fixture()
def temp_db(tmp_path):
    db_path = str(tmp_path / "test_trader.db")
    conn = sqlite3.connect(db_path)
    conn.execute("""CREATE TABLE ai_players (
        id TEXT PRIMARY KEY, display_name TEXT, provider TEXT, model_id TEXT,
        cash REAL, is_active INTEGER, can_trade_live INTEGER, is_paused INTEGER,
        season INTEGER, halt_mode TEXT, halt_reason TEXT, role TEXT,
        crew_role TEXT, timeframe TEXT
    )""")
    conn.execute("""CREATE TABLE trades (
        id INTEGER PRIMARY KEY, player_id TEXT, symbol TEXT, action TEXT,
        qty REAL, price REAL, executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.commit()
    conn.close()
    with patch.object(m5, "_db_path", return_value=db_path):
        yield db_path


# ─── current_target() — regime → weight matrix ─────────────────────────────

def test_current_target_bull_cross():
    with patch("engine.regime_router.get_current_regime", return_value="BULL_CROSS"):
        weight, regime = m5.current_target()
    assert weight == 0.80
    assert regime == "BULL_CROSS"


def test_current_target_bear():
    with patch("engine.regime_router.get_current_regime", return_value="BEAR"):
        weight, regime = m5.current_target()
    assert weight == 0.20


def test_current_target_unknown_regime_defaults_to_60_40():
    with patch("engine.regime_router.get_current_regime", return_value="SOME_NEW_REGIME"):
        weight, regime = m5.current_target()
    assert weight == 0.60
    assert regime == "SOME_NEW_REGIME"


def test_current_target_none_regime_defaults_to_60_40():
    with patch("engine.regime_router.get_current_regime", return_value=None):
        weight, regime = m5.current_target()
    assert weight == 0.60
    assert regime == "UNKNOWN"


def test_all_regime_router_bear_choppy_present():
    """Cross-check claim from the patch's own commit message: BEAR_CHOPPY
    must be mapped, not silently falling to the 60/40 default."""
    assert "BEAR_CHOPPY" in m5.M5_TARGETS
    assert m5.M5_TARGETS["BEAR_CHOPPY"] == 0.20


# ─── register_player() — idempotent, ships dormant ─────────────────────────

def test_register_player_creates_paused_seat(temp_db):
    conn = sqlite3.connect(temp_db)
    result = m5.register_player(conn)
    row = conn.execute("SELECT * FROM ai_players WHERE id=?", (m5.PLAYER_ID,)).fetchone()
    conn.close()
    assert result["created"] is True
    assert row is not None
    cols = [d[0] for d in sqlite3.connect(temp_db).execute("SELECT * FROM ai_players").description]
    row_dict = dict(zip(cols, row))
    assert row_dict["is_paused"] == 1          # ships dormant
    assert row_dict["can_trade_live"] == 0     # paper only, forever
    assert row_dict["halt_mode"] == "active"
    assert row_dict["cash"] == m5.GENESIS_CAPITAL


def test_register_player_idempotent(temp_db):
    conn = sqlite3.connect(temp_db)
    m5.register_player(conn)
    conn.execute("UPDATE ai_players SET is_paused=0 WHERE id=?", (m5.PLAYER_ID,))  # simulate activation
    conn.commit()
    result = m5.register_player(conn)  # second call must NOT re-insert / reset activation
    row = conn.execute("SELECT is_paused FROM ai_players WHERE id=?", (m5.PLAYER_ID,)).fetchone()
    conn.close()
    assert result["created"] is False
    assert row[0] == 0  # activation survives a second register_player call


# ─── _seat_is_live() — fail-closed on error, requires active + not paused ──

def test_seat_is_live_true_when_active_and_not_paused(temp_db):
    conn = sqlite3.connect(temp_db)
    m5.register_player(conn)
    conn.execute("UPDATE ai_players SET is_paused=0 WHERE id=?", (m5.PLAYER_ID,))
    conn.commit()
    conn.close()
    assert m5._seat_is_live() is True


def test_seat_is_live_false_while_dormant(temp_db):
    conn = sqlite3.connect(temp_db)
    m5.register_player(conn)  # ships is_paused=1
    conn.close()
    assert m5._seat_is_live() is False


def test_seat_is_live_false_when_halted(temp_db):
    conn = sqlite3.connect(temp_db)
    m5.register_player(conn)
    conn.execute("UPDATE ai_players SET is_paused=0, halt_mode='full' WHERE id=?", (m5.PLAYER_ID,))
    conn.commit()
    conn.close()
    assert m5._seat_is_live() is False


def test_seat_is_live_false_when_row_missing(temp_db):
    assert m5._seat_is_live() is False  # no register_player() call at all


def test_seat_is_live_fails_closed_on_db_error():
    with patch.object(m5, "_db_path", return_value="/nonexistent/path/does/not/exist.db"):
        assert m5._seat_is_live() is False  # never raises, fails closed (no trade)


# ─── _traded_today() — restart-safe dedup, fails closed ────────────────────

def test_traded_today_false_when_no_trades(temp_db):
    assert m5._traded_today() is False


def test_traded_today_true_when_trade_exists(temp_db):
    conn = sqlite3.connect(temp_db)
    conn.execute("INSERT INTO trades (player_id, symbol, action, qty, price) VALUES (?,?,?,?,?)",
                 (m5.PLAYER_ID, "SPY", "BUY", 10, 500.0))
    conn.commit()
    conn.close()
    assert m5._traded_today() is True


def test_traded_today_fails_closed_on_db_error():
    """Fail closed = treat as ALREADY traded, i.e. don't fire a second trade
    if the dedup check itself is broken."""
    with patch.object(m5, "_db_path", return_value="/nonexistent/path/does/not/exist.db"):
        assert m5._traded_today() is True


# ─── run_m5_rebalance() — scheduler gating ─────────────────────────────────

def _az(hour, minute=0, weekday_dt=None):
    base = weekday_dt or datetime(2026, 7, 13, hour, minute)  # a Monday
    return base.replace(hour=hour, minute=minute)


def test_run_m5_rebalance_skips_outside_market_hours(temp_db):
    with patch("engine.risk_manager.RiskManager.is_market_hours", return_value=False), \
         patch.object(m5, "_execute_rebalance") as mock_exec:
        m5.run_m5_rebalance()
    mock_exec.assert_not_called()


def test_run_m5_rebalance_skips_weekend(temp_db):
    saturday = datetime(2026, 7, 11, 8, 0)  # a Saturday
    with patch("engine.risk_manager.RiskManager.is_market_hours", return_value="market"), \
         patch("engine.market_calendar.az_now", return_value=saturday), \
         patch.object(m5, "_execute_rebalance") as mock_exec:
        m5.run_m5_rebalance()
    mock_exec.assert_not_called()


def test_run_m5_rebalance_skips_outside_window(temp_db):
    too_early = datetime(2026, 7, 13, 6, 0)  # Monday 6am, before the 7:45-8:30 window
    with patch("engine.risk_manager.RiskManager.is_market_hours", return_value="market"), \
         patch("engine.market_calendar.az_now", return_value=too_early), \
         patch.object(m5, "_execute_rebalance") as mock_exec:
        m5.run_m5_rebalance()
    mock_exec.assert_not_called()


def test_run_m5_rebalance_skips_before_745_within_hour_7(temp_db):
    seven_thirty = datetime(2026, 7, 13, 7, 30)
    with patch("engine.risk_manager.RiskManager.is_market_hours", return_value="market"), \
         patch("engine.market_calendar.az_now", return_value=seven_thirty), \
         patch.object(m5, "_execute_rebalance") as mock_exec:
        m5.run_m5_rebalance()
    mock_exec.assert_not_called()


def test_run_m5_rebalance_skips_when_seat_dormant(temp_db):
    """register_player ships is_paused=1 by default -- must not trade."""
    conn = sqlite3.connect(temp_db)
    m5.register_player(conn)
    conn.close()
    in_window = datetime(2026, 7, 13, 8, 0)
    with patch("engine.risk_manager.RiskManager.is_market_hours", return_value="market"), \
         patch("engine.market_calendar.az_now", return_value=in_window), \
         patch.object(m5, "_execute_rebalance") as mock_exec:
        m5.run_m5_rebalance()
    mock_exec.assert_not_called()


def test_run_m5_rebalance_skips_if_already_traded_today(temp_db):
    conn = sqlite3.connect(temp_db)
    m5.register_player(conn)
    conn.execute("UPDATE ai_players SET is_paused=0 WHERE id=?", (m5.PLAYER_ID,))
    conn.execute("INSERT INTO trades (player_id, symbol, action, qty, price) VALUES (?,?,?,?,?)",
                 (m5.PLAYER_ID, "SPY", "BUY", 1, 500.0))
    conn.commit()
    conn.close()
    in_window = datetime(2026, 7, 13, 8, 0)
    with patch("engine.risk_manager.RiskManager.is_market_hours", return_value="market"), \
         patch("engine.market_calendar.az_now", return_value=in_window), \
         patch.object(m5, "_execute_rebalance") as mock_exec:
        m5.run_m5_rebalance()
    mock_exec.assert_not_called()


def test_run_m5_rebalance_executes_when_all_gates_pass(temp_db):
    conn = sqlite3.connect(temp_db)
    m5.register_player(conn)
    conn.execute("UPDATE ai_players SET is_paused=0 WHERE id=?", (m5.PLAYER_ID,))
    conn.commit()
    conn.close()
    in_window = datetime(2026, 7, 13, 8, 0)
    with patch("engine.risk_manager.RiskManager.is_market_hours", return_value="market"), \
         patch("engine.market_calendar.az_now", return_value=in_window), \
         patch.object(m5, "_execute_rebalance") as mock_exec:
        m5.run_m5_rebalance()
    mock_exec.assert_called_once()


def test_run_m5_rebalance_exception_in_execute_does_not_propagate(temp_db):
    """An error inside _execute_rebalance must not crash the scheduler tick."""
    conn = sqlite3.connect(temp_db)
    m5.register_player(conn)
    conn.execute("UPDATE ai_players SET is_paused=0 WHERE id=?", (m5.PLAYER_ID,))
    conn.commit()
    conn.close()
    in_window = datetime(2026, 7, 13, 8, 0)
    with patch("engine.risk_manager.RiskManager.is_market_hours", return_value="market"), \
         patch("engine.market_calendar.az_now", return_value=in_window), \
         patch.object(m5, "_execute_rebalance", side_effect=RuntimeError("boom")):
        m5.run_m5_rebalance()  # must not raise


# ─── _execute_rebalance() — the core allocation math ────────────────────────

def _portfolio(cash, positions=None):
    return {"cash": cash, "positions": positions or []}


def test_execute_rebalance_no_trade_within_drift_band(temp_db):
    """actual 82% vs target 80% -- within the 5pp band, must not trade."""
    portfolio = _portfolio(cash=1800.0, positions=[{"symbol": "SPY", "qty": 16.4, "avg_price": 500.0}])
    with patch.object(m5, "current_target", return_value=(0.80, "BULL_CROSS")), \
         patch("engine.market_data.get_stock_price", side_effect=lambda s: {"price": 500.0 if s == "SPY" else 100.0}), \
         patch("engine.paper_trader.get_portfolio", return_value=portfolio), \
         patch("engine.paper_trader.buy") as mock_buy, \
         patch("engine.paper_trader.sell_partial") as mock_sell:
        m5._execute_rebalance()
    mock_buy.assert_not_called()
    mock_sell.assert_not_called()


def test_execute_rebalance_buys_spy_when_underweight(temp_db):
    """0% SPY, target 80% -> must buy SPY, then top up AGG with what's left."""
    portfolio_before = _portfolio(cash=10000.0, positions=[])
    portfolio_after_spy_buy = _portfolio(cash=2000.0, positions=[{"symbol": "SPY", "qty": 16.0, "avg_price": 500.0}])
    with patch.object(m5, "current_target", return_value=(0.80, "BULL_CROSS")), \
         patch("engine.market_data.get_stock_price", side_effect=lambda s: {"price": 500.0 if s == "SPY" else 100.0}), \
         patch("engine.paper_trader.get_portfolio", side_effect=[portfolio_before, portfolio_after_spy_buy]), \
         patch("engine.paper_trader.buy", return_value={"ok": True}) as mock_buy, \
         patch("engine.paper_trader.sell_partial") as mock_sell:
        m5._execute_rebalance()
    mock_sell.assert_not_called()
    assert mock_buy.call_count == 2  # SPY leg + AGG leg
    spy_call = mock_buy.call_args_list[0]
    assert spy_call.kwargs["symbol"] == "SPY"
    agg_call = mock_buy.call_args_list[1]
    assert agg_call.kwargs["symbol"] == "AGG"


def test_execute_rebalance_sells_spy_when_overweight(temp_db):
    """100% SPY, target 20% -> must sell SPY down, then buy AGG with proceeds."""
    portfolio_before = _portfolio(cash=0.0, positions=[{"symbol": "SPY", "qty": 20.0, "avg_price": 500.0}])
    portfolio_after_sell = _portfolio(cash=8000.0, positions=[{"symbol": "SPY", "qty": 4.0, "avg_price": 500.0}])
    with patch.object(m5, "current_target", return_value=(0.20, "BEAR")), \
         patch("engine.market_data.get_stock_price", side_effect=lambda s: {"price": 500.0 if s == "SPY" else 100.0}), \
         patch("engine.paper_trader.get_portfolio", side_effect=[portfolio_before, portfolio_after_sell]), \
         patch("engine.paper_trader.sell_partial", return_value={"ok": True}) as mock_sell, \
         patch("engine.paper_trader.buy", return_value={"ok": True}) as mock_buy:
        m5._execute_rebalance()
    mock_sell.assert_called_once()
    assert mock_sell.call_args.kwargs["symbol"] == "SPY"
    mock_buy.assert_called_once()
    assert mock_buy.call_args.kwargs["symbol"] == "AGG"


def test_execute_rebalance_aborts_when_no_live_price(temp_db):
    with patch.object(m5, "current_target", return_value=(0.80, "BULL_CROSS")), \
         patch("engine.market_data.get_stock_price", return_value={"price": 0.0}), \
         patch("engine.paper_trader.get_portfolio", return_value=_portfolio(1000.0)), \
         patch("engine.paper_trader.buy") as mock_buy, \
         patch("engine.paper_trader.sell_partial") as mock_sell:
        m5._execute_rebalance()
    mock_buy.assert_not_called()
    mock_sell.assert_not_called()


def test_execute_rebalance_aborts_on_nonpositive_book(temp_db):
    with patch.object(m5, "current_target", return_value=(0.80, "BULL_CROSS")), \
         patch("engine.market_data.get_stock_price", side_effect=lambda s: {"price": 500.0}), \
         patch("engine.paper_trader.get_portfolio", return_value=_portfolio(0.0, [])), \
         patch("engine.paper_trader.buy") as mock_buy:
        m5._execute_rebalance()
    mock_buy.assert_not_called()


def test_execute_rebalance_dust_guard_skips_entirely_when_book_is_tiny(temp_db):
    """When the whole book (incl. spare cash for the AGG leg) is under the
    $50 dust guard, nothing should trade at all."""
    portfolio = _portfolio(cash=10.0, positions=[])  # $10 total book
    with patch.object(m5, "current_target", return_value=(0.60, "CAUTIOUS_BULL")), \
         patch("engine.market_data.get_stock_price", side_effect=lambda s: {"price": 500.0 if s == "SPY" else 100.0}), \
         patch("engine.paper_trader.get_portfolio", return_value=portfolio), \
         patch("engine.paper_trader.buy") as mock_buy, \
         patch("engine.paper_trader.sell_partial") as mock_sell:
        m5._execute_rebalance()
    mock_buy.assert_not_called()
    mock_sell.assert_not_called()


def test_execute_rebalance_dust_guard_skips_primary_leg_but_agg_leg_still_sweeps_spare_cash(temp_db):
    """Documents the code's actual (intentional-looking) behavior: the AGG
    "park spare cash" leg is unconditional, independent of whether the
    primary SPY leg cleared the dust guard. A primary-leg notional too small
    to bother with can still coexist with a real amount of idle cash."""
    # SPY leg: drift -26.7% * $150 = $40.05, under $50 guard -> SPY skipped.
    # AGG leg: spare = cash(100) - 1%*total(150) = 98.5, over $50 guard -> AGG fires.
    portfolio = _portfolio(cash=100.0, positions=[{"symbol": "SPY", "qty": 0.1, "avg_price": 500.0}])  # $50 SPY / $150 total = 33%
    with patch.object(m5, "current_target", return_value=(0.60, "CAUTIOUS_BULL")), \
         patch("engine.market_data.get_stock_price", side_effect=lambda s: {"price": 500.0 if s == "SPY" else 100.0}), \
         patch("engine.paper_trader.get_portfolio", return_value=portfolio), \
         patch("engine.paper_trader.buy", return_value={"ok": True}) as mock_buy, \
         patch("engine.paper_trader.sell_partial") as mock_sell:
        m5._execute_rebalance()
    mock_sell.assert_not_called()
    assert mock_buy.call_count == 1
    assert mock_buy.call_args.kwargs["symbol"] == "AGG"  # only the spare-cash leg fired, not a SPY buy


def test_execute_rebalance_blocked_buy_skips_agg_leg(temp_db):
    """If the fleet gate blocks the SPY buy (res is None, e.g. BEAR avoid-list),
    the AGG top-up leg must not fire either -- documented BEAR-regime interaction."""
    portfolio = _portfolio(cash=10000.0, positions=[])
    with patch.object(m5, "current_target", return_value=(0.80, "BULL_CROSS")), \
         patch("engine.market_data.get_stock_price", side_effect=lambda s: {"price": 500.0 if s == "SPY" else 100.0}), \
         patch("engine.paper_trader.get_portfolio", return_value=portfolio), \
         patch("engine.paper_trader.buy", return_value=None) as mock_buy, \
         patch("engine.paper_trader.sell_partial") as mock_sell:
        m5._execute_rebalance()
    assert mock_buy.call_count == 1  # only the SPY leg attempted, AGG leg never reached


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
