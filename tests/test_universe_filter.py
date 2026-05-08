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
    """After HM-AO-α migration, all 5 trust ETFs are in get_active_universe()."""
    from engine.universe import get_active_universe

    u = set(get_active_universe())
    missing = EXPECTED_TRUST_ETFS - u
    assert not missing, f"Trust ETFs missing from active universe: {missing}"


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
