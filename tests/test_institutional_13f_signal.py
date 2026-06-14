#!/usr/bin/env python3
"""Tests for SEC EDGAR 13F net-new-institutional-buyers confirmatory signal.

Network-free: covers the confirmatory rail, lean thresholds, quarter math, and the
UHURA market_vote (DB-backed, temp file) — never hits EDGAR."""

import pytest

from engine import institutional_13f_signal as m


# ── Confirmatory-only rail ────────────────────────────────────────────────

def test_sole_voter_never_counts():
    v = m.confirmatory_vote(1, "confirm")
    assert v["counts_toward_convergence"] is False
    assert v["is_trigger"] is False
    assert v["trade_permitted_on_13f_alone"] is False


def test_counts_only_with_min_fleet_votes():
    v = m.confirmatory_vote(2, "confirm")
    assert v["counts_toward_convergence"] is True
    assert v["direction"] == "BULLISH"
    v2 = m.confirmatory_vote(2, "caution")
    assert v2["direction"] == "BEARISH" and v2["counts_toward_convergence"] is True


def test_neutral_never_counts_and_min_votes_two():
    assert m.MIN_FLEET_VOTES == 2
    for fv in (0, 2, 9):
        assert m.confirmatory_vote(fv, "neutral")["counts_toward_convergence"] is False
    for lean in ("confirm", "caution", "neutral", None):
        assert m.confirmatory_vote(2, lean)["is_trigger"] is False


# ── Lean thresholds ───────────────────────────────────────────────────────

def test_lean_confirm_on_strong_accumulation():
    # +202 on prior 6068 = +3.3% >= 3% and >= MIN_ABS -> confirm
    assert m._lean(202, 6068) == "confirm"


def test_lean_caution_on_strong_distribution():
    assert m._lean(-300, 6000) == "caution"  # -5%


def test_lean_neutral_when_below_pct_or_abs_floor():
    assert m._lean(-8, 766) == "neutral"     # -1% below threshold
    assert m._lean(10, 100) == "neutral"     # +10% but abs 10 < MIN_ABS(25)
    assert m._lean(100, 0) == "neutral"      # no prior baseline


# ── Quarter math ──────────────────────────────────────────────────────────

def test_prior_quarter_rolls_year():
    assert m.prior_quarter("2026Q1") == "2025Q4"
    assert m.prior_quarter("2026Q3") == "2026Q2"


def test_filing_window_after_quarter_end():
    s, e = m.filing_window("2026Q1")     # quarter ends Mar 31
    assert s == "2026-04-01"
    assert e == "2026-05-30"             # +60 days


def test_latest_filed_quarter_lags():
    from datetime import date
    # mid-June 2026: 2026Q1 (ended Mar 31) is +50d elapsed; Q2 not yet
    assert m.latest_filed_quarter(date(2026, 6, 14)) == "2026Q1"


# ── UHURA market_vote (DB-backed) ─────────────────────────────────────────

def _seed(db, quarter, rows):
    conn = m._conn(db)
    m._ensure_schema(conn)
    import datetime as _dt
    now = "2026-06-14T00:00:00+00:00"
    for tk, lean, net in rows:
        conn.execute(
            "INSERT OR REPLACE INTO institutional_flow_13f "
            "(ticker, quarter, holders_count, new_buyers, exited_holders, net_new_buyers, "
            " lean, asof_filed, source, created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (tk, quarter, 1000, max(net, 0), max(-net, 0), net, lean, "2026-05-30", "t", now))
    conn.commit(); conn.close()


def test_market_vote_abstains_when_flag_off():
    assert m.market_vote(enabled=False) is None


def test_market_vote_abstains_when_no_fresh_quarter(tmp_path):
    db = str(tmp_path / "empty.db")
    conn = m._conn(db); m._ensure_schema(conn); conn.close()
    assert m.market_vote(enabled=True, db_path=db) is None


def test_market_vote_bull_when_accumulation_dominates(tmp_path):
    db = str(tmp_path / "bull.db")
    _seed(db, "2026Q1", [("AAA", "confirm", 200), ("BBB", "confirm", 150), ("CCC", "caution", -100)])
    mv = m.market_vote(enabled=True, db_path=db)
    assert mv is not None
    assert mv["direction"] == "BULLISH"
    assert mv["weight"] == m.CONFIRMATORY_WEIGHT == 0.5   # context-class, not 1.0
    assert mv["bull"] == 2 and mv["bear"] == 1
    assert mv["quarter"] == "2026Q1"


def test_market_vote_ignores_neutral_leans(tmp_path):
    db = str(tmp_path / "neutral.db")
    _seed(db, "2026Q1", [("AAA", "neutral", 5), ("BBB", "neutral", -3)])
    assert m.market_vote(enabled=True, db_path=db) is None  # no directional lean


def test_market_vote_watchlist_filter(tmp_path):
    db = str(tmp_path / "wl.db")
    _seed(db, "2026Q1", [("AAA", "confirm", 200)])
    assert m.market_vote(enabled=True, db_path=db, watchlist=["ZZZ"]) is None
    assert m.market_vote(enabled=True, db_path=db, watchlist=["AAA"]) is not None


def test_get_signal_returns_latest_quarter(tmp_path):
    db = str(tmp_path / "sig.db")
    _seed(db, "2025Q4", [("AAA", "caution", -90)])
    _seed(db, "2026Q1", [("AAA", "confirm", 200)])
    s = m.get_signal("AAA", db_path=db)
    assert s["quarter"] == "2026Q1" and s["vote"] == "confirm"
    assert s["is_trigger"] is False


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
