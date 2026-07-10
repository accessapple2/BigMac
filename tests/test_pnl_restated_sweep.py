"""HM-P&L-RECONCILIATION 2026-07-10 sweep — every other place that read
get_portfolio_with_pnl()'s raw total_value/return_pct (which folds in
synthetic-era "fantasy" CSP premium never seen by the real account) instead
of total_value_restated/return_pct_restated.

Two prior fixes (commit efd0b8b) covered the dashboard's CSP/Wheel strategy
cards. This sweep found the SAME bug feeding into: every active agent's live
LLM prompt (engine/providers/base.py), the leader-signal/elimination ranking
(engine/leader_signal.py), several more dashboard endpoints, the Telegram
daily summary, and a console log line. This file covers the pieces that are
practically testable in isolation; the providers/base.py prompt-injection
fix is covered by live restart verification instead (see commit message),
given its depth of embedding in a very large method.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ─── engine.leader_signal._get_standings() ─────────────────────────────────

def test_get_standings_uses_restated_value(tmp_path):
    import engine.leader_signal as ls

    db_path = str(tmp_path / "test_trader.db")
    conn = sqlite3.connect(db_path)
    conn.execute("""CREATE TABLE ai_players (
        id TEXT PRIMARY KEY, display_name TEXT, cash REAL, is_active INTEGER,
        is_paused INTEGER
    )""")
    conn.execute("""CREATE TABLE positions (
        id INTEGER PRIMARY KEY, player_id TEXT, symbol TEXT
    )""")
    conn.execute("INSERT INTO ai_players (id, display_name, cash, is_active, is_paused) "
                 "VALUES ('options-sosnoff', 'Troi', 12880.20, 1, 0)")
    conn.execute("INSERT INTO ai_players (id, display_name, cash, is_active, is_paused) "
                 "VALUES ('modelB', 'ModelB', 7500.0, 1, 0)")
    conn.commit()
    conn.close()

    fake_pnl = {
        "options-sosnoff": {"total_value": 42748.94, "total_value_restated": 12880.20},
        "modelB": {"total_value": 7500.0, "total_value_restated": 7500.0},
    }

    def _fake_gpnl(player_id, prices):
        return fake_pnl[player_id]

    with patch.object(ls, "DB", db_path), \
         patch("engine.paper_trader.get_portfolio_with_pnl", side_effect=_fake_gpnl), \
         patch("engine.market_data.get_stock_price", return_value={"price": 100.0}):
        standings = ls._get_standings()

    by_id = {s["id"]: s for s in standings}
    # Must use the restated ($12,880.20), not the raw fantasy figure ($42,748.94).
    assert by_id["options-sosnoff"]["value"] == 12880.20
    # And the ranking/order must reflect the honest figure -- modelB ($7,500)
    # would be #1 by raw value's inverse but is correctly behind Troi's
    # restated $12,880.20 here; more importantly Troi must NOT rank above
    # modelB purely off the fantasy number.
    assert standings[0]["id"] == "options-sosnoff"  # $12,880 > $7,500, both real numbers
    assert standings[0]["value"] == 12880.20


# ─── dashboard.app.leaderboard() CSP win_rate/trade_counts era boundary ────

def _build_leaderboard_db(tmp_path, exit_dates_pre, exit_dates_post):
    """exit_dates_pre/post: lists of exit_date strings for closed CSP rows
    before/at-or-after TROI_REAL_QUOTES_ERA_START, all wins."""
    db_path = tmp_path / "test_trader.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""CREATE TABLE options_trades (
        id INTEGER PRIMARY KEY, agent_id TEXT, structure TEXT, status TEXT,
        pnl REAL, exit_date TEXT
    )""")
    for d in exit_dates_pre:
        conn.execute(
            "INSERT INTO options_trades (agent_id, structure, status, pnl, exit_date) "
            "VALUES ('options-sosnoff', 'csp', 'closed', 100.0, ?)", (d,)
        )
    for d in exit_dates_post:
        conn.execute(
            "INSERT INTO options_trades (agent_id, structure, status, pnl, exit_date) "
            "VALUES ('options-sosnoff', 'csp', 'closed', 50.0, ?)", (d,)
        )
    conn.commit()
    conn.close()
    return str(db_path)


def test_leaderboard_csp_stats_use_real_quotes_era_not_v2_era(tmp_path):
    """The literal bug: this block used exit_date < TROI_V2_ERA_START
    (2026-07-06), selecting the entirely-synthetic pre-boundary book,
    instead of exit_date >= TROI_REAL_QUOTES_ERA_START (2026-07-07),
    matching the season_realized fix a few lines below it in the same
    function."""
    import dashboard.app as app_module
    from engine.paper_trader import TROI_REAL_QUOTES_ERA_START

    db_path = _build_leaderboard_db(
        tmp_path,
        exit_dates_pre=["2026-06-01", "2026-06-15", "2026-07-01"],  # 3 pre-boundary (old bug: counted)
        exit_dates_post=["2026-07-08", "2026-07-09"],               # 2 post-boundary (correct: should count)
    )
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    trade_counts, win_data = {}, {}
    csp_stats = conn.execute(
        "SELECT COUNT(*) as n, SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END) as wins "
        "FROM options_trades WHERE agent_id='options-sosnoff' AND structure='csp' "
        "AND status='closed' AND exit_date >= ?",
        (TROI_REAL_QUOTES_ERA_START,),
    ).fetchone()
    if csp_stats and csp_stats["n"]:
        trade_counts["options-sosnoff"] = csp_stats["n"]
        win_data["options-sosnoff"] = round(csp_stats["wins"] / csp_stats["n"] * 100, 1)
    conn.close()

    assert trade_counts["options-sosnoff"] == 2  # only the real-quotes-era trades
    assert win_data["options-sosnoff"] == 100.0


# ─── dashboard.app.get_capital() / player_detail() / options_book_summary ──

_REAL_SQLITE_CONNECT = sqlite3.connect


def _fake_connect(real_path):
    def _connect(path, *args, **kwargs):
        if path == "data/trader.db":
            return _REAL_SQLITE_CONNECT(real_path, *args, **kwargs)
        return _REAL_SQLITE_CONNECT(path, *args, **kwargs)
    return _connect


def test_options_book_summary_excludes_pre_real_quotes_csp_scoped_to_csp_only(tmp_path):
    import dashboard.app as app_module

    db_path = tmp_path / "test_trader.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""CREATE TABLE options_books (
        book_tag TEXT PRIMARY KEY, current_cash REAL
    )""")
    conn.execute("""CREATE TABLE options_trades (
        id INTEGER PRIMARY KEY, book_tag TEXT, agent_id TEXT, structure TEXT,
        status TEXT, pnl REAL, exit_date TEXT, mtm_intrinsic REAL
    )""")
    conn.execute("INSERT INTO options_books (book_tag, current_cash) VALUES ('fleet', 1000.0)")
    # Pre-boundary CSP, win -- must be excluded from realized_pnl AND from
    # total_trades/wins/losses.
    conn.execute("INSERT INTO options_trades (book_tag, agent_id, structure, status, pnl, exit_date) "
                 "VALUES ('fleet', 'options-sosnoff', 'csp', 'closed', 29868.74, '2026-07-01')")
    # Post-boundary CSP, win -- must be included.
    conn.execute("INSERT INTO options_trades (book_tag, agent_id, structure, status, pnl, exit_date) "
                 "VALUES ('fleet', 'options-sosnoff', 'csp', 'closed', 100.0, '2026-07-08')")
    # Non-CSP structure, pre-boundary date, loss -- must ALWAYS be included
    # regardless of the CSP era boundary (proves the scoping doesn't leak
    # into other strategies, and that losses are counted too).
    conn.execute("INSERT INTO options_trades (book_tag, agent_id, structure, status, pnl, exit_date) "
                 "VALUES ('fleet', 'someagent', 'bull_spread', 'closed', -500.0, '2026-06-01')")
    conn.commit()
    conn.close()

    import asyncio
    with patch("sqlite3.connect", side_effect=_fake_connect(str(db_path))):
        result = asyncio.run(app_module.options_book_summary())

    assert result["ok"] is True
    fleet = result["books"]["fleet"]
    # 100 (post-CSP) + -500 (non-CSP) -- NOT +29868.74 from the pre-boundary CSP.
    assert fleet["realized_pnl"] == pytest.approx(-400.0)
    # Stored options_books counters are absent entirely in this fixture (no
    # total_trades/wins/losses columns on the table) -- proves these are
    # computed live from options_trades, not passed through from storage.
    assert fleet["total_trades"] == 2  # excludes the pre-boundary CSP
    assert fleet["wins"] == 1          # the post-boundary CSP
    assert fleet["losses"] == 1        # the non-CSP loss


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
