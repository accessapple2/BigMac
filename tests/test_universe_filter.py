"""Tests for engine.universe + engine.universe_refresh trust-ETF override.

Covers:
  - HM-AO-α TRUST_ETF_OVERRIDES is the expected 5-symbol set
  - get_active_universe() includes the 5 trust ETFs after migration
  - The override constant is a frozenset (immutable)

Run:
    python3 -m pytest tests/test_universe_filter.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


EXPECTED_TRUST_ETFS = {"GLD", "GLDM", "IAU", "SIVR", "SLV"}


def test_trust_etf_overrides_constant():
    """The whitelist must contain exactly the 5 trust ETFs and be immutable."""
    from engine.universe_refresh import TRUST_ETF_OVERRIDES

    assert isinstance(TRUST_ETF_OVERRIDES, frozenset), \
        "TRUST_ETF_OVERRIDES must be a frozenset (immutable)"
    assert TRUST_ETF_OVERRIDES == EXPECTED_TRUST_ETFS, \
        f"Expected {EXPECTED_TRUST_ETFS}, got {set(TRUST_ETF_OVERRIDES)}"


def test_trust_etfs_in_active_universe():
    """After HM-AO-α migration, every trust ETF that clears the live dollar-
    volume bar is in get_active_universe() -- i.e. none are excluded due to
    ticker_type misclassification (the original HM-AO-α bug), only due to
    genuinely insufficient volume.

    HM-TEST-VOLATILE-THRESHOLD-2026-08-29: this used to hard-assert all 5
    are always included. SIVR (abrdn Physical Silver Shares, the smallest/
    least liquid of the five) verified live at ~$77-91M dollar volume
    (10-day and 3-month averages, yfinance) against a $100M bar --
    genuinely marginal, real market liquidity, not a data or classification
    bug. A fixed dollar threshold will legitimately drift a borderline
    symbol in and out over time; hard-coding "SIVR always qualifies" just
    encodes whatever was true the day this was written. Compute the
    expectation from the SAME live threshold the production code uses
    instead of assuming a fixed roster.
    """
    from engine.universe import (
        ETF_DOLLAR_VOLUME_THRESHOLD, MAX_STALENESS_DAYS, _conn,
        get_active_universe,
    )

    with _conn() as conn:
        rows = conn.execute(
            "SELECT symbol, avg_volume, avg_price FROM scan_universe "
            "WHERE symbol IN ({}) AND ticker_type='ETF' "
            "AND julianday('now') - julianday(last_updated) <= ?".format(
                ",".join("?" * len(EXPECTED_TRUST_ETFS))
            ),
            (*EXPECTED_TRUST_ETFS, MAX_STALENESS_DAYS),
        ).fetchall()
    expected_active = {
        sym for sym, vol, price in rows
        if (vol or 0) * (price or 0) >= ETF_DOLLAR_VOLUME_THRESHOLD
    }
    assert expected_active, (
        "No trust ETF cleared the live volume bar at all -- if this ever "
        "happens it's a real signal (all 5 illiquid simultaneously, or the "
        "threshold/classification broke), not routine drift."
    )

    u = set(get_active_universe())
    missing = expected_active - u
    assert not missing, (
        f"Trust ETFs that clear the ${ETF_DOLLAR_VOLUME_THRESHOLD:,.0f} "
        f"volume bar are still missing from active universe (likely a "
        f"ticker_type misclassification regression, the original HM-AO-α "
        f"bug): {missing}"
    )


def test_trust_etfs_classified_as_etf():
    """scan_universe.ticker_type for the 5 trust ETFs must be 'ETF'."""
    import sqlite3

    db = ROOT / "data" / "trader.db"
    conn = sqlite3.connect(str(db))
    rows = conn.execute(
        "SELECT symbol, ticker_type FROM scan_universe "
        "WHERE symbol IN ('GLD','GLDM','IAU','SIVR','SLV')"
    ).fetchall()
    conn.close()

    types = {sym: tt for sym, tt in rows}
    for sym in EXPECTED_TRUST_ETFS:
        assert types.get(sym) == "ETF", \
            f"{sym} expected ticker_type='ETF', got {types.get(sym)!r}"


def test_other_etfs_unchanged():
    """Non-trust ETFs (GDX, GDXJ, SILJ) must still be ETF — sanity that
    we didn't accidentally reclassify them or introduce regression."""
    import sqlite3

    db = ROOT / "data" / "trader.db"
    conn = sqlite3.connect(str(db))
    rows = conn.execute(
        "SELECT symbol, ticker_type FROM scan_universe "
        "WHERE symbol IN ('GDX','GDXJ','SILJ')"
    ).fetchall()
    conn.close()

    types = {sym: tt for sym, tt in rows}
    for sym in ("GDX", "GDXJ", "SILJ"):
        assert types.get(sym) == "ETF", \
            f"Regression: {sym} ticker_type changed to {types.get(sym)!r}"


def test_no_other_cs_with_null_market_cap_was_widened():
    """The migration WHERE clause was scoped to the 5 named symbols; this
    test asserts no other CS rows were accidentally promoted."""
    import sqlite3

    db = ROOT / "data" / "trader.db"
    conn = sqlite3.connect(str(db))
    # Sample known mega-caps that should still be CS
    rows = conn.execute(
        "SELECT symbol, ticker_type FROM scan_universe "
        "WHERE symbol IN ('AAPL','MSFT','NVDA','TSLA','MU','AMD')"
    ).fetchall()
    conn.close()

    types = {sym: tt for sym, tt in rows}
    for sym in ("AAPL", "MSFT", "NVDA", "TSLA", "MU", "AMD"):
        assert types.get(sym) == "CS", \
            f"Bulk-reclassification regression: {sym} now {types.get(sym)!r}"
