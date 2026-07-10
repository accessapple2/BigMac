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


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
