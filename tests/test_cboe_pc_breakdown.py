"""tests/test_cboe_pc_breakdown.py -- HM-CBOE-PC-BREAKDOWN-2026-09-12.

engine/alpha_signals.py's CBOE HTML-scrape fallback used to keep only the
Equity P/C ratio via `"Equity" in cells[0]` -- case-sensitive, and the
page's real row labels are UPPERCASE ("EQUITY PUT/CALL RATIO"), so this
never actually matched (confirmed live before fixing, not assumed) --
equity_pc silently stayed None from this path forever, and Index/ETF/Total
were fully discarded even when the page loaded fine. Fixed to match on the
real labels and capture all four aggregate rows; a loose "INDEX" substring
match would have also wrongly matched "CBOE VOLATILITY INDEX (VIX)
PUT/CALL RATIO" further down the same page, so this covers that too.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import engine.alpha_signals as als  # noqa: E402

_REAL_PAGE_SAMPLE = """
<table>
<tr><th>Ratios</th><th>Value</th></tr>
<tr><td>TOTAL PUT/CALL RATIO</td><td>0.86</td></tr>
<tr><td>INDEX PUT/CALL RATIO</td><td>1.06</td></tr>
<tr><td>EXCHANGE TRADED PRODUCTS PUT/CALL RATIO</td><td>0.99</td></tr>
<tr><td>EQUITY PUT/CALL RATIO</td><td>0.58</td></tr>
<tr><td>CBOE VOLATILITY INDEX (VIX) PUT/CALL RATIO</td><td>0.43</td></tr>
<tr><td>SPX + SPXW PUT/CALL RATIO</td><td>1.23</td></tr>
</table>
"""


def _precreate_put_call_signals_table(db_path):
    """run_put_call() also writes to this pre-existing (in the real DB)
    table -- not the thing under test here, just a fixture prerequisite so
    the rest of the function can complete normally."""
    conn = sqlite3.connect(str(db_path))
    conn.execute("""CREATE TABLE put_call_signals (
        trade_date TEXT PRIMARY KEY, equity_pc_ratio REAL, total_pc_ratio REAL,
        pc_5d_ma REAL, pc_21d_ma REAL, signal_score REAL, signal TEXT, fetched_at TEXT
    )""")
    conn.commit()
    conn.close()


def test_run_put_call_extracts_all_four_aggregate_rows_case_insensitively(tmp_path, monkeypatch):
    db_path = tmp_path / "test_alpha_signals.db"
    _precreate_put_call_signals_table(db_path)
    monkeypatch.setattr(als, "DB_PATH", db_path)
    fake_resp = MagicMock()
    fake_resp.text = _REAL_PAGE_SAMPLE
    with patch("engine.alpha_signals._yf_series_module_level", None, create=True), \
         patch.object(als, "_SESSION") as mock_session:
        mock_session.get.return_value = fake_resp
        # Force the primary (Alpaca ^PCCE/^PCCR) lookup to fail so the
        # function falls through to the HTML scrape path being tested.
        with patch("engine.market_data.get_alpaca_bars", return_value=None):
            als.run_put_call()

    conn = sqlite3.connect(str(tmp_path / "test_alpha_signals.db"))
    row = conn.execute(
        "SELECT equity_pc, index_pc, etf_pc, total_pc FROM cboe_pc_breakdown ORDER BY date DESC LIMIT 1"
    ).fetchone()
    conn.close()
    assert row == (0.58, 1.06, 0.99, 0.86)


def test_index_label_is_not_confused_with_vix_index_row(tmp_path, monkeypatch):
    """The VIX row's label also contains the word INDEX -- must not overwrite
    the real INDEX PUT/CALL RATIO value with 0.43."""
    db_path = tmp_path / "test_alpha_signals.db"
    _precreate_put_call_signals_table(db_path)
    monkeypatch.setattr(als, "DB_PATH", db_path)
    fake_resp = MagicMock()
    fake_resp.text = _REAL_PAGE_SAMPLE
    with patch.object(als, "_SESSION") as mock_session:
        mock_session.get.return_value = fake_resp
        with patch("engine.market_data.get_alpaca_bars", return_value=None):
            als.run_put_call()

    conn = sqlite3.connect(str(db_path))
    row = conn.execute("SELECT index_pc FROM cboe_pc_breakdown ORDER BY date DESC LIMIT 1").fetchone()
    conn.close()
    assert row[0] == 1.06  # not 0.43 (the VIX row)
