"""HM-GEX-CONSUMER-BATCH 2026-09-14 (item 1 of the relay §10 batch) --
battle_station must never act on, store, or report a gex_levels row as current.

Live finding (relay_2026-09-14_screened_scan_silence_trace.md §10):
battle_station read engine.gex_overlay.get_latest_gex() -> gex_levels, last
written 2026-05-30. monitor_active_options() turned that row's gamma flip into
CLOSE_NOW -> _auto_close() (paper orders); generate_morning_briefing() only
recomputed when NO row existed, so it stored May's levels as each morning's;
get_battle_station_status() reported May's regime. Dormant only because Alpaca
had no open option positions.

get_latest_gex is stubbed to return a stale row and the canonical tiers are
stubbed at their source, so the same tests run against the old and new code.
Level values are illustrative, not the real 2026-05-30 row.
"""
from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta, timezone

import pytest

import config
import engine.battle_station as bs
import engine.canonical_gex as cg
import engine.gex_overlay as go
import engine.options_utils as ou
from engine.risk_manager import RiskManager

SPOT = 759.33

STALE_GEX_LEVELS_ROW = {
    "symbol": "SPY", "calc_time": "2026-05-30T12:55:00", "spot_price": 758.0,
    "king_node": 770.0, "gamma_flip": 766.0, "put_wall": 740.0, "call_wall": 780.0,
    "gamma_walls_above": [], "gamma_walls_below": [], "total_gex": 1.2e9,
    "regime": "POSITIVE", "composite_score": 0.6, "composite_signal": "BULLISH",
    "composite_strength": "MODERATE",
}


def _utc_minutes_ago(minutes: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")


def _fresh_snapshot(**overrides) -> dict:
    snap = {
        "underlying": "SPY", "spot": SPOT, "total_gex": -12767156387.08, "gamma_flip": 766.31,
        "call_wall": 775.0, "put_wall": 750.0, "king_node": 775.0,
        "regime": "SHORT GAMMA · volatile (spot below flip)",
        "_asof": _utc_minutes_ago(12), "_src": "alpaca",
    }
    snap.update(overrides)
    return snap


def _long_call() -> dict:
    return {
        "underlying": "SPY", "option_symbol": "SPY260918C00760000", "option_type": "call",
        "strike": 760.0, "expiry_date": date.today() + timedelta(days=4),
        "unrealized_plpc": -0.05, "current_price": 4.10, "avg_entry_price": 4.32,
        "qty": 1, "qty_signed": 1,
    }


@pytest.fixture
def station(tmp_path, monkeypatch):
    state = {
        "tier0": {}, "canonical": {}, "closed": [], "positions": [_long_call()],
        "gex_levels": {"SPY": dict(STALE_GEX_LEVELS_ROW)},
    }
    monkeypatch.setattr(bs, "TRADER_DB", str(tmp_path / "battle_station_test.db"))

    monkeypatch.setattr(cg, "_alpaca_snapshot_fresh", lambda sym: state["tier0"].get(sym))
    monkeypatch.setattr(cg, "canonical_gex_if_fresh", lambda sym: state["canonical"].get(sym))
    monkeypatch.setattr(go, "get_latest_gex", lambda sym: state["gex_levels"].get(sym))

    def _no_recompute(*args, **kwargs):
        raise RuntimeError("gex_overlay recompute must not be reached in this test")

    monkeypatch.setattr(go, "calculate_gex", _no_recompute)
    monkeypatch.setattr(go, "_save_gex_levels", _no_recompute)

    monkeypatch.setattr(config, "SPREAD_CANNIBALIZATION_GUARD_ENABLED", False, raising=False)
    monkeypatch.setattr(RiskManager, "is_market_hours", staticmethod(lambda: "market"))
    monkeypatch.setattr(ou, "is_spread_leg", lambda sym: False)
    monkeypatch.setattr(bs, "_get_alpaca_options_positions", lambda: [dict(p) for p in state["positions"]])
    monkeypatch.setattr(bs, "_get_current_spot", lambda sym: SPOT)
    monkeypatch.setattr(bs, "_estimate_iv", lambda *args: 0.30)
    monkeypatch.setattr(bs, "_minutes_until_eod_close", lambda: 200.0)
    monkeypatch.setattr(bs, "_lookup_agent_for_option", lambda sym: None)
    monkeypatch.setattr(
        bs, "_get_prior_day_bars",
        lambda sym: {"high": 762.1, "low": 755.0, "close": SPOT, "vwap": 758.2},
    )
    monkeypatch.setattr(
        bs, "_auto_close", lambda pos, reason: state["closed"].append((pos["option_symbol"], reason)),
    )
    bs._last_signal.clear()
    return state


def _logged_signals() -> list[str]:
    conn = sqlite3.connect(bs.TRADER_DB)
    try:
        return [r[0] for r in conn.execute("SELECT signal FROM battle_station_log ORDER BY id")]
    finally:
        conn.close()


def _morning_gex_columns(symbol: str = "SPY") -> tuple:
    conn = sqlite3.connect(bs.TRADER_DB)
    try:
        return conn.execute(
            "SELECT gex_king, gex_flip, gex_put_wall, gex_call_wall, prior_close "
            "FROM morning_levels WHERE trade_date=? AND symbol=?",
            (date.today().isoformat(), symbol),
        ).fetchone()
    finally:
        conn.close()


# ── Position monitor ─────────────────────────────────────────────────────────

def test_stale_gex_levels_flip_never_auto_closes_a_position(station):
    # Spot 759.33 sits below the stale row's 766 flip: CLOSE_NOW on the old path.
    bs.monitor_active_options()
    assert station["closed"] == []
    assert _logged_signals() == ["HOLD"]


def test_fresh_tier0_flip_still_drives_close_now(station):
    station["gex_levels"] = {}
    station["tier0"]["SPY"] = _fresh_snapshot()
    bs.monitor_active_options()
    assert len(station["closed"]) == 1
    assert "gamma flip $766" in station["closed"][0][1]
    assert _logged_signals() == ["CLOSE_NOW"]


def test_monitor_ignores_a_daily_row_the_one_day_canonical_gate_accepts(station):
    """A flip-cross close acts within the minute, so a daily snapshot inside
    CANONICAL_GEX_MAX_AGE_DAYS is still too old to auto-close on."""
    station["canonical"]["SPY"] = _fresh_snapshot(_asof=_utc_minutes_ago(600), _src="daily-flow_gex.db")
    bs.monitor_active_options()
    assert station["closed"] == []


# ── Morning briefing ─────────────────────────────────────────────────────────

def test_morning_briefing_stores_no_stale_levels(station):
    results = bs.generate_morning_briefing(["SPY"])
    assert results["SPY"]["gex_king"] is None and results["SPY"]["gex_flip"] is None
    assert _morning_gex_columns() == (None, None, None, None, SPOT)


def test_morning_briefing_stores_fresh_canonical_levels(station):
    station["canonical"]["SPY"] = _fresh_snapshot()
    bs.generate_morning_briefing(["SPY"])
    assert _morning_gex_columns() == (775.0, 766.31, 750.0, 775.0, SPOT)


# ── Status endpoint ──────────────────────────────────────────────────────────

def test_status_regime_is_unknown_when_tier0_is_stale(station):
    station["positions"] = []
    assert bs.get_battle_station_status()["gex_regime"] == "unknown"


def test_status_regime_comes_from_fresh_tier0(station):
    station["positions"] = []
    station["tier0"]["SPY"] = _fresh_snapshot()
    assert bs.get_battle_station_status()["gex_regime"] == "SHORT GAMMA · volatile (spot below flip)"


# ── canonical_gex.alpaca_gex_if_fresh ────────────────────────────────────────

def test_alpaca_gex_if_fresh_reads_tier0_only(monkeypatch):
    def _no_lower_tiers(sym):
        raise RuntimeError("tiers 1-3 must not be reached")

    monkeypatch.setattr(cg, "canonical_gex", _no_lower_tiers)
    monkeypatch.setattr(cg, "_alpaca_snapshot_fresh", lambda sym: None)
    assert cg.alpaca_gex_if_fresh("spy") is None

    monkeypatch.setattr(cg, "_alpaca_snapshot_fresh", lambda sym: _fresh_snapshot() if sym == "SPY" else None)
    assert cg.alpaca_gex_if_fresh("spy")["gamma_flip"] == 766.31
