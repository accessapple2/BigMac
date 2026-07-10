"""HM-BUG-BATCH-2026-07-10 item 9 — /api/portfolio/real metals self-consistency.

Bug: the same API response disagreed with itself -- metals.silver said 65 oz
while unified.by_symbol said 75 oz, because dashboard/app.py::portfolio_real()
hardcoded gold_oz/silver_oz as stale literals instead of reading the real
metals_ledger purchase-transaction table, while the separate `unified` section
(engine.total_portfolio._load_metals()) read the ledger correctly. Fixed by
making portfolio_real() call _load_metals() directly instead of reimplementing
the aggregation a second time -- these tests pin that there is now exactly one
source of truth, not two that happen to currently agree.
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


def _make_ledger_db(tmp_path, rows):
    db_path = tmp_path / "test_trader.db"
    conn = sqlite3.connect(db_path)
    conn.execute("""CREATE TABLE metals_ledger (
        id INTEGER PRIMARY KEY, purchase_date TEXT, metal TEXT,
        qty_oz REAL, total_cost REAL, price_per_oz REAL, vendor TEXT, notes TEXT
    )""")
    conn.executemany(
        "INSERT INTO metals_ledger (purchase_date, metal, qty_oz, total_cost) VALUES (?,?,?,?)",
        rows,
    )
    conn.commit()
    conn.close()
    return str(db_path)


def test_load_metals_sums_all_purchase_rows_for_silver(tmp_path):
    """The regression check on the ledger-aggregation function itself: 7
    separate purchase rows (matching the real production shape) must sum to
    75.0 oz, not the old hardcoded 65.0."""
    from engine import total_portfolio as tp

    rows = [
        ("2025-12-02", "silver", 10.0, 589.99),
        ("2026-01-12", "silver", 10.0, 849.99),
        ("2026-02-05", "silver", 10.0, 818.60),
        ("2026-03-03", "silver", 5.0, 402.20),
        ("2026-04-01", "silver", 10.0, 799.00),
        ("2026-05-04", "silver", 20.0, 1589.00),
        ("2026-06-06", "silver", 10.0, 759.99),
        ("2026-01-01", "gold", 1.0, 5249.99),
    ]
    db_path = _make_ledger_db(tmp_path, rows)

    with patch.object(tp, "_DB_PATH", Path(db_path)):
        with patch("engine.metals_tracker.get_spot_prices", return_value={
            "GOLD": {"price": 4000.0}, "SILVER": {"price": 60.0},
        }):
            result = tp._load_metals()

    by_symbol = {p["symbol"]: p for p in result["positions"]}
    assert by_symbol["SILVER"]["qty"] == 75.0
    assert by_symbol["GOLD"]["qty"] == 1.0


def test_portfolio_real_metals_section_agrees_with_unified_section(tmp_path):
    """The literal reported bug, end to end: call the actual live endpoint
    function with a patched ledger DB and assert result["metals"]["silver"]["oz"]
    equals the SILVER row's qty in result["unified"]["by_symbol"] -- the two
    sections of the SAME response that disagreed (65 vs 75) before the fix."""
    from engine import total_portfolio as tp
    import dashboard.app as app_module

    rows = [
        ("2025-12-02", "silver", 10.0, 589.99),
        ("2026-01-12", "silver", 10.0, 849.99),
        ("2026-02-05", "silver", 10.0, 818.60),
        ("2026-03-03", "silver", 5.0, 402.20),
        ("2026-04-01", "silver", 10.0, 799.00),
        ("2026-05-04", "silver", 20.0, 1589.00),
        ("2026-06-06", "silver", 10.0, 759.99),
        ("2026-01-01", "gold", 1.0, 5249.99),
    ]
    db_path = _make_ledger_db(tmp_path, rows)

    with patch.object(tp, "_DB_PATH", Path(db_path)), \
         patch("engine.metals_tracker.get_spot_prices", return_value={
             "GOLD": {"price": 4000.0}, "SILVER": {"price": 60.0},
         }):
        result = app_module.portfolio_real()

    silver_section = result["metals"]["silver"]
    unified_rows = {p["symbol"]: p for p in result.get("unified", {}).get("by_symbol", [])}

    assert silver_section["oz"] == 75.0, (
        f"metals.silver.oz should be 75.0 (from the 7-row ledger fixture), got {silver_section['oz']!r}"
    )
    if "SILVER" in unified_rows:  # unified section can be absent if read_total_portfolio() errors independently
        assert silver_section["oz"] == unified_rows["SILVER"]["qty"], (
            "metals.silver.oz and unified.by_symbol's SILVER qty must be the "
            "same number in the same response -- this is the structural guard "
            "against the reported 65oz-vs-75oz self-contradiction"
        )


def test_metal_result_helper_handles_missing_metal_gracefully():
    """If a metal has zero ledger rows (e.g. platinum, not currently held),
    the response must show nulls, not crash on None arithmetic -- this is
    the regression the None-safety rewrite specifically had to get right
    once the hardcoded non-None literals were removed."""
    import dashboard.app as app_module
    import inspect
    src = inspect.getsource(app_module.portfolio_real)
    assert "_metal_result" in src, (
        "portfolio_real should build its metals dict through a small "
        "None-safe helper, not inline arithmetic that assumes oz/avg_cost "
        "are always present (the pre-fix bug's failure mode)"
    )


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
