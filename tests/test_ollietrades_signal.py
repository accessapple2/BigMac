"""Tests for engine.ollietrades_signal — the unanimous-consensus alert gate.

Phase 1 (ghost book). Design: docs/OLLIETRADES_SIGNAL.md. Covers the pieces
explicitly named for testing: winning-model selection, directional agreement,
market-hours gating, daily cap ranking, and ledger immutability.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine import ollietrades_signal as ots  # noqa: E402

_ET = ZoneInfo("America/New_York")


@pytest.fixture()
def temp_db(tmp_path):
    db_path = str(tmp_path / "test_trader.db")
    conn = sqlite3.connect(db_path)
    conn.execute("""CREATE TABLE signals (
        id INTEGER PRIMARY KEY, player_id TEXT NOT NULL, symbol TEXT NOT NULL,
        signal TEXT NOT NULL, confidence REAL, option_type TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.commit()
    conn.close()
    with patch.object(ots, "DB", db_path):
        yield db_path


def _insert_signal(db_path, player_id, symbol, signal, confidence, option_type=None, minutes_ago=1):
    conn = sqlite3.connect(db_path)
    ts = (datetime.utcnow() - timedelta(minutes=minutes_ago)).strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        "INSERT INTO signals (player_id, symbol, signal, confidence, option_type, created_at) "
        "VALUES (?,?,?,?,?,?)",
        (player_id, symbol, signal, confidence, option_type, ts),
    )
    conn.commit()
    conn.close()


def _winner(pid, rating="A", rating_score=90, total_trades=50, return_pct=5.0, display_name=None):
    return {"player_id": pid, "display_name": display_name or pid, "rating": rating,
            "rating_score": rating_score, "total_trades": total_trades, "return_pct": return_pct}


# ─── Winning-model selection ──────────────────────────────────────────────────

def test_get_winning_models_filters_by_halt_mode():
    """A halted player must never qualify, even with a perfect rating/trades/P&L."""
    ratings = [{"player_id": "worf", "rating": "A", "rating_score": 95,
                "total_trades": 100, "display_name": "Worf"}]
    leaderboard = [{"player_id": "worf", "halt_mode": "full", "return_pct": 10.0}]
    with patch("engine.agent_ratings.fleet_report_card", return_value=ratings), \
         patch.object(ots, "_fetch_leaderboard_rows", return_value=leaderboard):
        winners = ots.get_winning_models()
    assert winners == []


def test_get_winning_models_filters_by_rating_trades_and_pnl():
    """Only players clearing ALL THREE thresholds (rating, trade count,
    positive season P&L) qualify -- never a hardcoded name list."""
    ratings = [
        {"player_id": "a_grade_thin", "rating": "A", "rating_score": 95, "total_trades": 5, "display_name": "Thin"},
        {"player_id": "c_grade_deep", "rating": "C", "rating_score": 55, "total_trades": 200, "display_name": "CGrade"},
        {"player_id": "b_grade_losing", "rating": "B", "rating_score": 70, "total_trades": 50, "display_name": "Losing"},
        {"player_id": "qualifies", "rating": "A", "rating_score": 88, "total_trades": 30, "display_name": "Qualifies"},
    ]
    leaderboard = [
        {"player_id": "a_grade_thin", "halt_mode": "active", "return_pct": 5.0},      # too few trades
        {"player_id": "c_grade_deep", "halt_mode": "active", "return_pct": 5.0},       # rating below B
        {"player_id": "b_grade_losing", "halt_mode": "active", "return_pct": -2.0},    # negative P&L
        {"player_id": "qualifies", "halt_mode": "active", "return_pct": 3.4},
    ]
    with patch("engine.agent_ratings.fleet_report_card", return_value=ratings), \
         patch.object(ots, "_fetch_leaderboard_rows", return_value=leaderboard):
        winners = ots.get_winning_models(min_rating="B", min_trades=20, min_return_pct=0.0)
    assert [w["player_id"] for w in winners] == ["qualifies"]


# ─── Directional agreement ─────────────────────────────────────────────────────

def test_find_consensus_candidates_requires_unanimity(temp_db):
    """Two winning models disagreeing on direction for the same symbol must
    NOT produce a candidate."""
    _insert_signal(temp_db, "modelA", "AAPL", "BUY", 0.9)
    _insert_signal(temp_db, "modelB", "AAPL", "SHORT", 0.85)
    winners = [_winner("modelA"), _winner("modelB")]
    candidates = ots.find_consensus_candidates(min_agreeing_models=2, winning_models=winners)
    assert candidates == []


def test_find_consensus_candidates_finds_unanimous_agreement(temp_db):
    """Two winning models both BUY the same symbol -> exactly one candidate,
    direction 'long', both models attached."""
    _insert_signal(temp_db, "modelA", "TSLA", "BUY", 0.9)
    _insert_signal(temp_db, "modelB", "TSLA", "BUY_CALL", 0.8)
    winners = [_winner("modelA"), _winner("modelB")]
    candidates = ots.find_consensus_candidates(min_agreeing_models=2, winning_models=winners)
    assert len(candidates) == 1
    assert candidates[0]["symbol"] == "TSLA"
    assert candidates[0]["direction"] == "long"
    assert {a["player_id"] for a in candidates[0]["approving_models"]} == {"modelA", "modelB"}


def test_find_consensus_candidates_hold_does_not_break_unanimity(temp_db):
    """A HOLD from a third winning model must not count against agreement --
    it's simply not an opinion either way -- but also must not count TOWARD
    the min_agreeing_models threshold."""
    _insert_signal(temp_db, "modelA", "NVDA", "BUY", 0.9)
    _insert_signal(temp_db, "modelB", "NVDA", "BUY", 0.85)
    _insert_signal(temp_db, "modelC", "NVDA", "HOLD", 0.5)
    winners = [_winner("modelA"), _winner("modelB"), _winner("modelC")]
    candidates = ots.find_consensus_candidates(min_agreeing_models=2, winning_models=winners)
    assert len(candidates) == 1
    assert {a["player_id"] for a in candidates[0]["approving_models"]} == {"modelA", "modelB"}


def test_find_consensus_candidates_requires_minimum_agreeing_models(temp_db):
    """A single winning model 'agreeing with itself' is not consensus."""
    _insert_signal(temp_db, "modelA", "SPY", "BUY", 0.95)
    winners = [_winner("modelA")]
    candidates = ots.find_consensus_candidates(min_agreeing_models=2, winning_models=winners)
    assert candidates == []


def test_find_consensus_candidates_ignores_stale_signals_outside_lookback(temp_db):
    """A signal older than the lookback window must not count."""
    _insert_signal(temp_db, "modelA", "QQQ", "BUY", 0.9, minutes_ago=200)
    _insert_signal(temp_db, "modelB", "QQQ", "BUY", 0.9, minutes_ago=200)
    winners = [_winner("modelA"), _winner("modelB")]
    candidates = ots.find_consensus_candidates(min_agreeing_models=2, lookback_minutes=60, winning_models=winners)
    assert candidates == []


# ─── Playbook matching ─────────────────────────────────────────────────────────

def test_match_playbook_bull_put_spread():
    candidate = {"symbol": "AAPL", "direction": "long",
                 "approving_models": [{"option_type": "BUY_PUT"}, {"option_type": "CSP"}]}
    assert ots.match_playbook(candidate) == "bull_put_spread"


def test_match_playbook_unmatched_returns_none():
    candidate = {"symbol": "ZZZZZ", "direction": "short",
                 "approving_models": [{"option_type": "SOME_UNKNOWN_TYPE"}]}
    assert ots.match_playbook(candidate) is None


# ─── Market-hours gate wiring ──────────────────────────────────────────────────

def test_evaluate_gate_produces_nothing_outside_market_hours(temp_db):
    """The full pipeline, frozen at 23:42 ET -- same scenario as item 8 --
    must short-circuit before ever touching winning-model/consensus logic."""
    frozen_2342_et = datetime(2026, 7, 9, 23, 42, tzinfo=_ET)
    with patch("engine.market_calendar.is_within_alert_hours", return_value=False):
        with patch.object(ots, "get_winning_models") as mock_winners:
            result = ots.evaluate_gate(now=frozen_2342_et)
    assert result["pushed"] == []
    assert result["shown_only"] == []
    assert result["gated_out"] == "market_hours"
    mock_winners.assert_not_called()  # never reached -- gate short-circuits


def test_evaluate_gate_proceeds_during_market_hours(temp_db):
    _insert_signal(temp_db, "modelA", "MSFT", "BUY", 0.9)
    _insert_signal(temp_db, "modelB", "MSFT", "BUY", 0.9)
    winners = [_winner("modelA"), _winner("modelB")]
    with patch("engine.market_calendar.is_within_alert_hours", return_value=True):
        with patch.object(ots, "get_winning_models", return_value=winners):
            result = ots.evaluate_gate(min_conviction=0.0)
    assert result["gated_out"] is None
    assert len(result["pushed"]) == 1
    assert result["pushed"][0]["symbol"] == "MSFT"


# ─── Daily cap / ranking ────────────────────────────────────────────────────────

def test_rank_and_cap_ranks_by_conviction_and_caps():
    candidates = [
        {"symbol": "LOW", "composite_conviction": 0.60},
        {"symbol": "HIGH", "composite_conviction": 0.95},
        {"symbol": "MID", "composite_conviction": 0.80},
    ]
    to_push, shown_only = ots.rank_and_cap(candidates, max_per_day=2)
    assert [c["symbol"] for c in to_push] == ["HIGH", "MID"]
    assert [c["symbol"] for c in shown_only] == ["LOW"]


def test_rank_and_cap_empty_input_is_not_an_error():
    """Silence is the expected common case -- zero candidates must not raise
    or produce spurious output."""
    to_push, shown_only = ots.rank_and_cap([], max_per_day=3)
    assert to_push == []
    assert shown_only == []


def test_evaluate_gate_daily_cap_is_a_real_daily_budget_across_cycles(temp_db):
    """HM-BUG-BATCH-2026-07-10 item 36 (scheduler wiring): rank_and_cap's
    max_per_day only ever capped a SINGLE call. Once wired to a repeating
    scheduler (every 10min during RTH), that would let a config literally
    named MAX_PUSHES_PER_DAY silently push far more than that many times in
    a real day. Two evaluate_gate() calls in the same day, each individually
    qualifying 2 candidates under max_per_day=2, must push at most 2 total
    across BOTH calls -- not 2 each."""
    _insert_signal(temp_db, "modelA", "AAPL", "BUY", 0.95)
    _insert_signal(temp_db, "modelB", "AAPL", "BUY", 0.95)
    _insert_signal(temp_db, "modelA", "MSFT", "BUY", 0.90)
    _insert_signal(temp_db, "modelB", "MSFT", "BUY", 0.90)
    winners = [_winner("modelA"), _winner("modelB")]

    with patch("engine.market_calendar.is_within_alert_hours", return_value=True), \
         patch.object(ots, "get_winning_models", return_value=winners):
        first = ots.evaluate_gate(min_conviction=0.0, max_per_day=2)
        # Simulate run_ollietrades_signal_cycle() actually logging the pushes
        # from cycle 1 -- this is what makes cycle 2 aware of the budget.
        for c in first["pushed"]:
            ots.log_to_ledger(c, "PUSHED", c["strategy"], first["gate_config"])

        second = ots.evaluate_gate(min_conviction=0.0, max_per_day=2)

    assert len(first["pushed"]) == 2  # cycle 1: full budget, nothing spent yet
    assert len(second["pushed"]) == 0  # cycle 2: budget already exhausted by cycle 1
    assert len(second["shown_only"]) == 2  # still logged, just not eligible to push


# ─── Ledger immutability ────────────────────────────────────────────────────────

def test_log_to_ledger_freezes_exact_call_time_snapshot(temp_db):
    """Every frozen column must round-trip byte-identical -- this is the
    no-repaint rule's foundation. approving_models_json specifically must
    preserve each model's rating/rating_score AT CALL TIME, not a live
    reference that could drift if the model's grade changes later."""
    candidate = {
        "symbol": "GOOGL", "direction": "long", "composite_conviction": 0.88,
        "approving_models": [
            {"player_id": "modelA", "display_name": "Model A", "action": "BUY",
             "confidence": 0.9, "option_type": None, "rating": "A", "rating_score": 92},
        ],
    }
    gate_config = {"min_rating": "B", "min_trades": 20}
    row_id = ots.log_to_ledger(
        candidate, status="SHOWN-ONLY", strategy="ollie_live_swing", gate_config=gate_config,
        entry_price=150.0, stop_price=145.0, target_price=160.0,
        context={"scanner_tier": "T1"}, dissents=[],
    )
    row = ots.get_ledger_row(row_id)
    assert row["symbol"] == "GOOGL"
    assert row["direction"] == "long"
    assert row["strategy"] == "ollie_live_swing"
    assert row["entry_price"] == 150.0
    assert row["stop_price"] == 145.0
    assert row["target_price"] == 160.0
    assert row["composite_conviction"] == 0.88
    assert row["status"] == "SHOWN-ONLY"
    assert json.loads(row["approving_models_json"]) == candidate["approving_models"]
    assert json.loads(row["gate_config_json"]) == gate_config
    assert json.loads(row["context_json"]) == {"scanner_tier": "T1"}
    assert row["outcome"] is None  # unresolved until resolve_outcomes() runs (task 37)


def test_ledger_has_no_update_path_for_frozen_columns(temp_db):
    """Structural guard: the module exposes no function that UPDATEs the
    frozen columns (symbol/direction/strategy/entry_price/stop_price/
    target_price/approving_models_json/dissents_json/context_json/
    gate_config_json) -- only log_to_ledger (INSERT) touches them. If a
    future change adds a mutator, this test's function-surface check should
    be revisited alongside it, not silently bypassed."""
    import inspect
    src = inspect.getsource(ots)
    # Any UPDATE statement in this module must only ever target the mutable
    # outcome-tracking columns (status/pushed_at/trade_id/outcome*), never
    # the frozen call-time snapshot columns.
    frozen_cols = ["entry_price", "stop_price", "target_price", "approving_models_json",
                   "gate_config_json", "context_json"]
    for line in src.splitlines():
        if "UPDATE signal_ledger" in line or ("UPDATE" in line and "signal_ledger" in line):
            for col in frozen_cols:
                assert col not in line, f"found an UPDATE touching frozen column {col}: {line!r}"


# ─── Entry/stop/target computation (task 37) ───────────────────────────────────

def _candle(time, o, h, l, c, v=1000):
    return {"time": time, "open": o, "high": h, "low": l, "close": c, "volume": v}


def test_compute_entry_stop_target_long():
    candles = [_candle("2026-07-09T14:00:00Z", 100, 101, 99, 100.0)]
    with patch("engine.market_data.get_intraday_candles", return_value=candles):
        entry, stop, target = ots.compute_entry_stop_target("AAPL", "long", stop_pct=0.05, target_r_multiple=2.0)
    assert entry == 100.0
    assert stop == 95.0    # 100 * (1 - 0.05)
    assert target == 110.0  # 100 + (100*0.05)*2.0


def test_compute_entry_stop_target_short():
    candles = [_candle("2026-07-09T14:00:00Z", 100, 101, 99, 100.0)]
    with patch("engine.market_data.get_intraday_candles", return_value=candles):
        entry, stop, target = ots.compute_entry_stop_target("AAPL", "short", stop_pct=0.05, target_r_multiple=2.0)
    assert entry == 100.0
    assert stop == 105.0
    assert target == 90.0


def test_compute_entry_stop_target_returns_none_on_no_candles():
    with patch("engine.market_data.get_intraday_candles", return_value=[]):
        assert ots.compute_entry_stop_target("AAPL", "long", 0.05, 2.0) is None


def test_compute_entry_stop_target_returns_none_on_fetch_error():
    with patch("engine.market_data.get_intraday_candles", side_effect=RuntimeError("boom")):
        assert ots.compute_entry_stop_target("AAPL", "long", 0.05, 2.0) is None


# ─── Trading-days-elapsed (resolution window) ──────────────────────────────────

def test_trading_days_elapsed_skips_weekend():
    # Thursday 2026-07-09 -> Monday 2026-07-13 (exclusive start, inclusive end):
    # Fri 07-10 and Mon 07-13 count, Sat/Sun don't -> 2
    start = datetime(2026, 7, 9, 15, 0)
    end = datetime(2026, 7, 13, 15, 0)
    assert ots._trading_days_elapsed(start, end) == 2


def test_trading_days_elapsed_same_day_is_zero():
    d = datetime(2026, 7, 9, 15, 0)
    assert ots._trading_days_elapsed(d, d) == 0


# ─── Outcome resolution walk-forward ───────────────────────────────────────────

def _ledger_row(symbol="AAPL", direction="long", created_at="2026-07-09 14:00:00",
                 entry=100.0, stop=95.0, target=110.0):
    return {"symbol": symbol, "direction": direction, "created_at": created_at,
            "entry_price": entry, "stop_price": stop, "target_price": target}


def test_resolve_one_row_win_when_target_hit():
    row = _ledger_row()
    candles = [
        _candle("2026-07-09T14:05:00Z", 100, 102, 99, 101),
        _candle("2026-07-09T14:10:00Z", 101, 111, 100, 105),  # high=111 >= target 110
    ]
    with patch("engine.market_data.get_intraday_candles", return_value=candles):
        outcome, r, detail = ots._resolve_one_row(row, datetime(2026, 7, 9, 15, 0), 5)
    assert outcome == "WIN"
    assert r == 2.0  # (110-100)/(100-95)
    assert detail["hit"] == "target"


def test_resolve_one_row_loss_when_stop_hit():
    row = _ledger_row()
    candles = [_candle("2026-07-09T14:05:00Z", 100, 101, 94, 96)]  # low=94 <= stop 95
    with patch("engine.market_data.get_intraday_candles", return_value=candles):
        outcome, r, detail = ots._resolve_one_row(row, datetime(2026, 7, 9, 15, 0), 5)
    assert outcome == "LOSS"
    assert r == -1.0
    assert detail["hit"] == "stop"


def test_resolve_one_row_same_candle_ambiguity_assumes_stop_first():
    """If a single candle's range contains BOTH stop and target, the
    conservative assumption (no tick data available) is stop-first."""
    row = _ledger_row()
    candles = [_candle("2026-07-09T14:05:00Z", 100, 115, 90, 105)]  # both 95 and 110 inside [90,115]
    with patch("engine.market_data.get_intraday_candles", return_value=candles):
        outcome, r, detail = ots._resolve_one_row(row, datetime(2026, 7, 9, 15, 0), 5)
    assert outcome == "LOSS"
    assert detail["hit"] == "stop"


def test_resolve_one_row_short_direction():
    row = _ledger_row(direction="short", entry=100.0, stop=105.0, target=90.0)
    candles = [_candle("2026-07-09T14:05:00Z", 100, 101, 89, 92)]  # low=89 <= target 90
    with patch("engine.market_data.get_intraday_candles", return_value=candles):
        outcome, r, detail = ots._resolve_one_row(row, datetime(2026, 7, 9, 15, 0), 5)
    assert outcome == "WIN"
    assert r == 2.0
    assert detail["hit"] == "target"


def test_resolve_one_row_still_pending_when_neither_hit_nor_expired():
    row = _ledger_row()
    candles = [_candle("2026-07-09T14:05:00Z", 100, 102, 99, 101)]  # neither stop nor target touched
    with patch("engine.market_data.get_intraday_candles", return_value=candles):
        # Same day, well under the 5-trading-day resolution window
        outcome, r, detail = ots._resolve_one_row(row, datetime(2026, 7, 9, 15, 30), 5)
    assert outcome is None
    assert r is None
    assert detail is None


def test_resolve_one_row_expired_unresolved_after_window():
    row = _ledger_row(created_at="2026-07-01 14:00:00")
    candles = [_candle("2026-07-08T14:05:00Z", 100, 102, 99, 101)]  # neither hit
    # 2026-07-01 (Wed) -> 2026-07-09 (Thu): 6 trading days elapsed, past a 5-day window
    with patch("engine.market_data.get_intraday_candles", return_value=candles):
        outcome, r, detail = ots._resolve_one_row(row, datetime(2026, 7, 9, 15, 0), 5)
    assert outcome == "EXPIRED_UNRESOLVED"
    assert detail["reason"] == "resolution_window_elapsed"


def test_resolve_one_row_zero_stop_distance_is_immediately_expired():
    row = _ledger_row(entry=100.0, stop=100.0, target=110.0)
    outcome, r, detail = ots._resolve_one_row(row, datetime(2026, 7, 9, 15, 0), 5)
    assert outcome == "EXPIRED_UNRESOLVED"
    assert detail["reason"] == "zero_stop_distance"


# ─── resolve_outcomes() end-to-end ─────────────────────────────────────────────

def _backdate_ledger_row(db_path, row_id, created_at_str):
    """log_to_ledger always stamps created_at via SQL datetime('now') (real
    wall-clock) -- tests that need a controlled created_at (to line up with
    fixed mocked candle timestamps regardless of what day the suite actually
    runs on) must override it directly after insert."""
    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE signal_ledger SET created_at = ? WHERE id = ?", (created_at_str, row_id))
    conn.commit()
    conn.close()


def test_resolve_outcomes_writes_outcome_and_preserves_frozen_columns(temp_db):
    """End-to-end: log a candidate with entry/stop/target, run resolve_outcomes
    against mocked candles that hit target, assert the outcome* columns are
    written AND every frozen column is byte-identical before/after (the
    no-repaint rule's actual behavioral guarantee, not just the structural
    source-scan in test_ledger_has_no_update_path_for_frozen_columns)."""
    candidate = {
        "symbol": "TSLA", "direction": "long", "composite_conviction": 0.9,
        "approving_models": [{"player_id": "modelA", "display_name": "Model A",
                               "action": "BUY", "confidence": 0.9, "option_type": None,
                               "rating": "A", "rating_score": 90}],
    }
    row_id = ots.log_to_ledger(
        candidate, status="SHOWN-ONLY", strategy="ollie_live_swing",
        gate_config={"min_rating": "B"}, entry_price=100.0, stop_price=95.0, target_price=110.0,
    )
    _backdate_ledger_row(temp_db, row_id, "2026-07-09 14:00:00")
    before = ots.get_ledger_row(row_id)

    candles = [_candle("2026-07-09T14:10:00Z", 100, 111, 100, 105)]  # target hit
    with patch("engine.market_data.get_intraday_candles", return_value=candles):
        tally = ots.resolve_outcomes(now=datetime(2026, 7, 9, 15, 0))

    after = ots.get_ledger_row(row_id)
    assert tally["WIN"] == 1
    assert after["outcome"] == "WIN"
    assert after["outcome_r_multiple"] == 2.0
    assert after["outcome_resolved_at"] is not None
    assert json.loads(after["outcome_detail_json"])["hit"] == "target"
    # Frozen columns must be byte-identical before/after.
    for col in ("symbol", "direction", "strategy", "entry_price", "stop_price", "target_price",
                "composite_conviction", "approving_models_json", "gate_config_json", "context_json"):
        assert before[col] == after[col], f"frozen column {col} changed during resolution"


def test_resolve_outcomes_skips_rows_without_entry_price(temp_db):
    """A row that never got a computed entry_price (e.g. price-fetch failure
    at log time) must be left alone by resolve_outcomes, not crash it."""
    candidate = {
        "symbol": "NVDA", "direction": "long", "composite_conviction": 0.8,
        "approving_models": [],
    }
    row_id = ots.log_to_ledger(candidate, status="SHOWN-ONLY", strategy="unmatched", gate_config={})
    tally = ots.resolve_outcomes(now=datetime(2026, 7, 9, 15, 0))
    assert tally["WIN"] == 0 and tally["LOSS"] == 0 and tally["EXPIRED_UNRESOLVED"] == 0
    row = ots.get_ledger_row(row_id)
    assert row["outcome"] is None


def test_resolve_outcomes_never_reresolves_an_already_resolved_row(temp_db):
    """outcome IS NULL is the query filter -- a resolved row must be inert
    to subsequent resolve_outcomes() calls (no-repaint at the engine level,
    not just the frozen-column level)."""
    candidate = {"symbol": "MSFT", "direction": "long", "composite_conviction": 0.9, "approving_models": []}
    row_id = ots.log_to_ledger(candidate, status="SHOWN-ONLY", strategy="unmatched", gate_config={},
                                entry_price=100.0, stop_price=95.0, target_price=110.0)
    _backdate_ledger_row(temp_db, row_id, "2026-07-09 14:00:00")
    candles = [_candle("2026-07-09T14:10:00Z", 100, 111, 100, 105)]
    with patch("engine.market_data.get_intraday_candles", return_value=candles):
        ots.resolve_outcomes(now=datetime(2026, 7, 9, 15, 0))
        first_resolved_at = ots.get_ledger_row(row_id)["outcome_resolved_at"]
        ots.resolve_outcomes(now=datetime(2026, 7, 9, 15, 30))  # run again, later
    assert ots.get_ledger_row(row_id)["outcome_resolved_at"] == first_resolved_at


# ─── run_ollietrades_signal_cycle wires entry/stop/target into the ledger ──────

def test_cycle_logs_entry_stop_target_when_price_fetch_succeeds(temp_db):
    _insert_signal(temp_db, "modelA", "AMD", "BUY", 0.9)
    _insert_signal(temp_db, "modelB", "AMD", "BUY", 0.9)
    winners = [_winner("modelA"), _winner("modelB")]
    candles = [_candle("2026-07-09T14:00:00Z", 50, 51, 49, 50.0)]
    with patch("engine.market_calendar.is_within_alert_hours", return_value=True), \
         patch.object(ots, "get_winning_models", return_value=winners), \
         patch("engine.market_data.get_intraday_candles", return_value=candles):
        ots.run_ollietrades_signal_cycle()

    conn = sqlite3.connect(temp_db)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM signal_ledger WHERE symbol = 'AMD'").fetchone()
    conn.close()
    assert row["entry_price"] == 50.0
    assert row["stop_price"] == 47.5   # 50 * (1 - 0.05)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
