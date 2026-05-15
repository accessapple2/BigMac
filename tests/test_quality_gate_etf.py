"""tests/test_quality_gate_etf.py — Patch 1 (ETF fast-path) regression tests.

These tests stub engine.stock_fundamentals.fetch_fundamentals so the live
Yahoo API is never hit. They exercise passes_quality_gate from
engine.quality_gate against four representative symbols:

  TQQQ — 3x leveraged Nasdaq ETF       (ETF shape, RSI in band)
  IBIT — iShares Bitcoin spot ETF       (ETF shape)
  NUKZ — uranium-mining ETF             (ETF shape, top-rejected symbol)
  INTC — large-cap stock, "hold" rating (regression: stock path unchanged)

Run from project root with: venv/bin/python3 -m pytest tests/test_quality_gate_etf.py -v
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock

# Ensure project root is on sys.path so `from engine...` resolves under pytest.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from engine.quality_gate import passes_quality_gate  # noqa: E402


# Yahoo's quoteSummary modules-shape for an ETF: dict is truthy but all
# four key fields are None / "Unknown".
ETF_FUND = {
    "earnings_growth": None,
    "revenue_growth": None,
    "recommendation": None,
    "sector": "Unknown",
}

# INTC live-repro snapshot from data/scotty_qg_investigation_2026-05-14.md.
INTC_FUND = {
    "earnings_growth": None,
    "revenue_growth": 7.2,
    "recommendation": "hold",
    "sector": "Technology",
}


def _patch_fund(fund_dict):
    """Patch the fetch_fundamentals import used inside passes_quality_gate.

    The function does `from engine.stock_fundamentals import fetch_fundamentals`
    inside its body, so we patch the module attribute (not a local).
    """
    return mock.patch(
        "engine.stock_fundamentals.fetch_fundamentals",
        return_value=fund_dict,
    )


def _patch_smart_money(return_value=None):
    """Patch smart-money lookup so tests never hit the SQLite DB."""
    return mock.patch(
        "engine.smart_money.get_recent_smart_money",
        return_value=return_value or [],
    )


class TestEtfFastPath(unittest.TestCase):
    """Patch 1 deliverable — ETF-shape detection should pass these symbols."""

    def test_tqqq_passes_etf_fastpath(self):
        """TQQQ at RSI=65 (below the 70 overbought line) must pass."""
        with _patch_fund(ETF_FUND), _patch_smart_money():
            passes, score, details = passes_quality_gate("TQQQ", {"rsi": 65})
        self.assertTrue(passes, msg=f"TQQQ should pass, got details={details}")
        self.assertGreaterEqual(score, 3)
        joined = " ".join(details).lower()
        self.assertIn("etf", joined, msg=f"expected ETF-shape note, got {details}")

    def test_ibit_passes_etf_fastpath(self):
        """IBIT Bitcoin spot ETF, RSI=60 — must pass via ETF shape detector."""
        with _patch_fund(ETF_FUND), _patch_smart_money():
            passes, score, details = passes_quality_gate("IBIT", {"rsi": 60})
        self.assertTrue(passes, msg=f"IBIT should pass, got details={details}")
        self.assertGreaterEqual(score, 3)

    def test_nukz_passes_etf_fastpath(self):
        """NUKZ uranium ETF (highest-rejected symbol, RSI=44) — must pass."""
        with _patch_fund(ETF_FUND), _patch_smart_money():
            passes, score, details = passes_quality_gate("NUKZ", {"rsi": 44})
        self.assertTrue(passes, msg=f"NUKZ should pass, got details={details}")
        self.assertGreaterEqual(score, 3)

    def test_tqqq_blocked_when_overbought(self):
        """RSI >= 70 still blocks even an ETF — leveraged + hot is correct to halt."""
        with _patch_fund(ETF_FUND), _patch_smart_money():
            passes, score, details = passes_quality_gate("TQQQ", {"rsi": 82})
        self.assertFalse(passes, msg=f"TQQQ@82 should fail, got details={details}")

    def test_intc_unchanged_after_patch1(self):
        """Stocks must not be affected by the ETF shape detector.

        INTC has revenue_growth=+7.2% (real number) so the shape-detector's
        `all None` precondition fails and it falls through to the regular
        stock path. Expected: the same 1/5 score the live repro recorded.
        """
        with _patch_fund(INTC_FUND), _patch_smart_money():
            passes, score, details = passes_quality_gate("INTC", {"rsi": 73})
        self.assertFalse(passes, msg=f"INTC pre-Patch2 should fail, details={details}")
        # Regression: the ETF detector must NOT short-circuit INTC.
        joined = " ".join(details).lower()
        self.assertNotIn("etf-shape", joined,
                         msg=f"INTC must NOT trip ETF shape detector, got {details}")
        # Live-repro recorded 1/5 — assert score is in [0, 2] range to allow
        # for small drift but flag if the stock path silently shifted.
        self.assertLessEqual(score, 2,
                             msg=f"INTC score drift suggests stock-path regression: {score}")


if __name__ == "__main__":
    unittest.main()
