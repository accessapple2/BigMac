"""HM-P&L-RECONCILIATION 2026-07-10 (S6 findings Finding 4).

The "CSP / Wheel" strategy card on /api/strategy/pnl and /api/performance/
summary summed ALL closed CSP history unfiltered ($35.3k live), including
the synthetic-VIX-formula era before TROI_REAL_QUOTES_ERA_START
(2026-07-07) -- none of it ever a real quote, none of it ever routed to
the real Alpaca account the headline account P&L reads from. The season
leaderboard already grades CSP agents on _csp_realized_pnl_real_quotes()
only; these two dashboard cards must use the same standard so they can't
show a bigger, more flattering number than the one that actually counts.

Fixed by filtering options_trades to (structure != 'csp' OR exit_date >=
TROI_REAL_QUOTES_ERA_START) in both queries -- scoped to CSP rows only so
a future non-CSP options strategy sharing the same code path isn't
silently affected by a pricing-era boundary that has nothing to do with it.
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


def _build_db(tmp_path):
    db_path = tmp_path / "test_trader.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""CREATE TABLE trades (
        id INTEGER PRIMARY KEY, player_id TEXT, action TEXT, season INTEGER,
        corrected_pnl REAL, realized_pnl REAL
    )""")
    conn.execute("""CREATE TABLE options_trades (
        id INTEGER PRIMARY KEY, agent_id TEXT, structure TEXT, status TEXT,
        pnl REAL, exit_date TEXT
    )""")
    # options-sosnoff: one pre-real-quotes CSP (synthetic, must be excluded)
    # and one post-real-quotes CSP (must be included).
    conn.execute(
        "INSERT INTO options_trades (agent_id, structure, status, pnl, exit_date) "
        "VALUES ('options-sosnoff', 'csp', 'closed', 29868.74, '2026-07-01')"
    )
    conn.execute(
        "INSERT INTO options_trades (agent_id, structure, status, pnl, exit_date) "
        "VALUES ('options-sosnoff', 'csp', 'closed', 250.00, '2026-07-08')"
    )
    # Non-CSP structure for the same agent -- must NEVER be filtered by the
    # CSP pricing-era boundary, regardless of its own exit_date.
    conn.execute(
        "INSERT INTO options_trades (agent_id, structure, status, pnl, exit_date) "
        "VALUES ('options-sosnoff', 'covered_call', 'closed', 500.00, '2026-06-01')"
    )
    conn.commit()
    conn.close()
    return str(db_path)


_REAL_SQLITE_CONNECT = sqlite3.connect  # captured before any patching


def _fake_connect(real_path, *a, **kw):
    def _connect(path, *args, **kwargs):
        if path == "data/trader.db":
            return _REAL_SQLITE_CONNECT(real_path, *args, **kwargs)
        return _REAL_SQLITE_CONNECT(path, *args, **kwargs)
    return _connect


def test_strategy_pnl_excludes_pre_real_quotes_csp(tmp_path):
    import dashboard.app as app_module
    db_path = _build_db(tmp_path)
    with patch("sqlite3.connect", side_effect=_fake_connect(db_path)):
        result = app_module.strategy_pnl()
    csp_bucket = result["buckets"]["csp_wheel"]
    # Only the post-real-quotes CSP ($250) + the non-CSP covered call ($500)
    # count -- the $29,868.74 synthetic-era CSP must be excluded.
    assert csp_bucket["pnl"] == pytest.approx(750.00)
    assert csp_bucket["trades"] == 2


def test_performance_summary_csp_wheel_card_excludes_pre_real_quotes_csp(tmp_path):
    import dashboard.app as app_module
    db_path = _build_db(tmp_path)
    with patch("sqlite3.connect", side_effect=_fake_connect(db_path)):
        result = app_module.performance_summary()
    csp_card = next(s for s in result["strategies"] if s["name"] == "CSP / Wheel")
    assert csp_card["realized_pnl"] == pytest.approx(750.00)
    assert csp_card["trades"] == 2


def test_strategy_pnl_all_synthetic_era_csp_yields_near_zero(tmp_path):
    """Matches the live-verified real-world state: if every closed CSP is
    still pre-real-quotes, the card must honestly show that, not the
    inflated lifetime total."""
    import dashboard.app as app_module
    db_path = tmp_path / "only_synthetic.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""CREATE TABLE trades (
        id INTEGER PRIMARY KEY, player_id TEXT, action TEXT, season INTEGER,
        corrected_pnl REAL, realized_pnl REAL
    )""")
    conn.execute("""CREATE TABLE options_trades (
        id INTEGER PRIMARY KEY, agent_id TEXT, structure TEXT, status TEXT,
        pnl REAL, exit_date TEXT
    )""")
    conn.execute(
        "INSERT INTO options_trades (agent_id, structure, status, pnl, exit_date) "
        "VALUES ('options-sosnoff', 'csp', 'closed', 29868.74, '2026-07-01')"
    )
    conn.commit()
    conn.close()
    with patch("sqlite3.connect", side_effect=_fake_connect(str(db_path))):
        result = app_module.strategy_pnl()
    assert "csp_wheel" not in result["buckets"] or result["buckets"]["csp_wheel"]["pnl"] == 0.0


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
