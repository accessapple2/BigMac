"""HM-CAPITOL-FUND-FILTER — congress copycat must skip non-equity fund tickers.

Regression test for the 2026-06-10 incident where Capitol Trades opened AFAXX
(American Funds U.S. Govt Money Market) @ $1.00 off a Tom Suozzi disclosure.
Money-market / mutual-fund share classes are cash-sweep vehicles, not equity
signals, and must be dropped before scoring.

Run: python3 -m pytest tests/test_capitol_fund_filter.py -v
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import pytest

from engine.crew_scanner import is_non_equity_ticker, capitol_rules


# ── unit: the ticker heuristic ────────────────────────────────────────────────
@pytest.mark.parametrize("ticker", [
    "AFAXX",   # the incident — American Funds U.S. Govt Money Market
    "VMFXX",   # Vanguard Federal MMF (denylist)
    "VFIAX",   # Vanguard 500 index fund share class (5-letter X pattern)
    "AGTHX",   # American Funds growth fund
    "SPAXX",   # Fidelity Govt MMF
])
def test_non_equity_funds_are_skipped(ticker):
    assert is_non_equity_ticker(ticker) is True


@pytest.mark.parametrize("ticker", [
    "META",    # req#5 known-good equity (e.g. McGuire/Moskowitz META)
    "NVDA", "AAPL", "BA", "JPM",
    "V",       # 1-letter ticker
    "X",       # 1-letter ending in X (US Steel) — must NOT be filtered
    "GOOGL",   # 5-letter equity NOT ending in X — must NOT be filtered
    "ZVZZT",   # 5-letter ending T — must NOT be filtered
    "",        # empty
])
def test_legit_equities_pass(ticker):
    assert is_non_equity_ticker(ticker) is False


# ── end-to-end: capitol_rules drops the fund, scores the equity ───────────────
def _disclosure(politician, ticker):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return {"politician": politician, "ticker": ticker, "type": "BUY",
            "asset_type": "stock", "filed_date": today}


def test_capitol_rules_skips_afaxx_scores_meta(monkeypatch, caplog):
    fake = [_disclosure("Tom Suozzi", "AFAXX"),
            _disclosure("Jared Moskowitz", "META")]
    # capitol_rules pulls disclosures via congress_scraper.get_all_congress_trades
    import engine.congress_scraper as scraper
    monkeypatch.setattr(scraper, "get_all_congress_trades", lambda *a, **k: fake)

    with caplog.at_level(logging.INFO, logger="crew_scanner"):
        res = capitol_rules({"vix": 18.0, "regime": "BULL"}, [])

    # AFAXX must be filtered out and audited at INFO
    assert "skipped non-equity: AFAXX" in caplog.text
    # The fund must never become the signal; the legit equity is scored instead
    assert res.get("symbol") != "AFAXX"
    assert res.get("symbol") == "META"
    assert res.get("action") == "BUY"
