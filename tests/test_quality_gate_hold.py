"""tests/test_quality_gate_hold.py — Patch 2 (hold-rating partial credit) tests.

Patch 2 (HM-QG-CALIBRATION) adds two pieces of partial credit to the analyst-
consensus rung of passes_quality_gate:

  1. recommendation in {"hold","neutral"}  → score += 0.5, detail ".. (partial)"
  2. recommendation is None / ""           → score += 0.5 (was bare SKIP detail)

Outright "sell" / "underperform" still scores 0 and is marked FAIL. Strong-buy /
buy ratings are unchanged — still +1.

NOTE — score truncation contract (updated 2026-08-29):
    passes_quality_gate() used to return int(score) (engine/quality_gate.py:138),
    which made a single +0.5 partial credit invisible to the caller (internal
    3.5 -> returned 3) -- exactly the "future follow-up" flagged below as not
    yet done. HM-QG-SCORE-FLOAT-TRUNCATION has since shipped: it now returns
    round(score, 1), so 3.5 surfaces as 3.5, not 3. The pass/fail boolean is
    unaffected either way (still `int(score) >= 3` internally). The
    user-visible signal that Patch 2 fired is still also the `details` list --
    entries like "analyst=hold (partial)" vs the pre-patch "FAIL analyst=hold".

These tests stub engine.stock_fundamentals.fetch_fundamentals so the live Yahoo
API is never hit, and stub smart-money so the score is deterministic. Each
case uses fundamentals where earnings_growth and revenue_growth are real
positive numbers — that keeps the test on the stock path (not the ETF fast-
path Patch 1 short-circuits on).

Run from project root with:
    venv/bin/python3 -m pytest tests/test_quality_gate_hold.py -v
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


def _stock_fund(recommendation):
    """Stock-path fundamentals: real earnings_growth + revenue_growth so the
    ETF fast-path's `all None` guard does NOT trip. Only the recommendation
    varies, so each test isolates the analyst-rung behavior."""
    return {
        "earnings_growth": 5.0,
        "revenue_growth": 3.0,
        "recommendation": recommendation,
        "sector": "Technology",
    }


def _patch_fund(fund_dict):
    return mock.patch(
        "engine.stock_fundamentals.fetch_fundamentals",
        return_value=fund_dict,
    )


def _patch_smart_money(return_value=None):
    return mock.patch(
        "engine.smart_money.get_recent_smart_money",
        return_value=return_value or [],
    )


class TestHoldRatingPartialCredit(unittest.TestCase):
    """Patch 2 deliverable — 'hold' / 'neutral' analyst ratings earn 0.5."""

    def test_hold_rating_gets_partial_credit(self):
        """recommendation='hold' → +0.5, marked '(partial)'.

        Pre-patch: score = eg(+1) + rg(+1) + hold(0, marked FAIL) + rsi(+1)
                         = 3.0, detail says 'FAIL analyst=hold'
        Post-patch: score = eg(+1) + rg(+1) + hold(+0.5) + rsi(+1)
                          = 3.5, detail says 'analyst=hold (partial)'
        """
        with _patch_fund(_stock_fund("hold")), _patch_smart_money():
            passes, score, details = passes_quality_gate("AAPL", {"rsi": 50})
        joined = " ".join(details).lower()
        self.assertIn("partial", joined,
                      msg=f"hold rating must yield '(partial)' note; got {details}")
        self.assertNotIn("fail analyst", joined,
                         msg=f"hold rating must NOT be marked FAIL; got {details}")
        # score is un-truncated (HM-QG-SCORE-FLOAT-TRUNCATION). See module NOTE.
        self.assertEqual(score, 3.5,
                         msg=f"expected eg+rg+hold0.5+rsi = 3.5, got {score}; details={details}")
        self.assertTrue(passes)

    def test_neutral_rating_gets_partial_credit(self):
        """recommendation='neutral' is treated identically to 'hold'."""
        with _patch_fund(_stock_fund("neutral")), _patch_smart_money():
            passes, score, details = passes_quality_gate("AAPL", {"rsi": 50})
        joined = " ".join(details).lower()
        self.assertIn("partial", joined,
                      msg=f"neutral rating must yield '(partial)' note; got {details}")
        self.assertEqual(score, 3.5,
                         msg=f"expected 3.5, got {score}; details={details}")
        self.assertTrue(passes)

    def test_hold_rating_case_insensitive(self):
        """recommendation='HOLD' (uppercase) must also earn partial credit.

        Patch 2 introduces `_rec_low = (rec or '').lower()` so casing in the
        live yfinance payload doesn't break the new partial-credit branch.
        """
        with _patch_fund(_stock_fund("HOLD")), _patch_smart_money():
            passes, score, details = passes_quality_gate("AAPL", {"rsi": 50})
        joined = " ".join(details).lower()
        self.assertIn("partial", joined,
                      msg=f"HOLD (uppercase) must yield '(partial)'; got {details}")
        self.assertEqual(score, 3.5,
                         msg=f"case-insensitive hold: expected 3.5, got {score}")

    def test_no_recommendation_gets_partial_credit(self):
        """recommendation=None → +0.5 (was bare SKIP detail pre-patch).

        Pre-patch: score = eg(+1) + rg(+1) + None(0, bare SKIP) + rsi(+1)
                         = 3.0
        Post-patch: score = eg(+1) + rg(+1) + None(+0.5) + rsi(+1)
                          = 3.5
        """
        with _patch_fund(_stock_fund(None)), _patch_smart_money():
            passes, score, details = passes_quality_gate("AAPL", {"rsi": 50})
        joined = " ".join(details).lower()
        self.assertIn("skip analyst", joined,
                      msg=f"None recommendation must yield SKIP note; got {details}")
        self.assertEqual(score, 3.5,
                         msg=f"missing analyst: expected 3.5, got {score}")

    def test_buy_rating_unchanged(self):
        """Regression: 'buy' must still earn +1 (NOT downgraded to +0.5)."""
        with _patch_fund(_stock_fund("buy")), _patch_smart_money():
            passes, score, details = passes_quality_gate("AAPL", {"rsi": 50})
        joined = " ".join(details).lower()
        self.assertNotIn("partial", joined,
                         msg=f"buy must NOT be marked partial; got {details}")
        self.assertIn("analyst=buy", joined,
                      msg=f"buy rating must produce 'analyst=buy' detail; got {details}")
        self.assertEqual(score, 4.0,
                         msg=f"buy: eg+rg+buy+rsi = 4.0 expected, got {score}")
        self.assertTrue(passes)

    def test_strong_buy_rating_unchanged(self):
        """Regression: 'strong_buy' and 'overweight' still earn +1."""
        for rating in ("strong_buy", "strongbuy", "overweight"):
            with self.subTest(rating=rating):
                with _patch_fund(_stock_fund(rating)), _patch_smart_money():
                    passes, score, details = passes_quality_gate("AAPL", {"rsi": 50})
                joined = " ".join(details).lower()
                self.assertNotIn("partial", joined,
                                 msg=f"{rating!r} must NOT be partial; got {details}")
                self.assertEqual(score, 4.0,
                                 msg=f"{rating!r}: expected 4.0, got {score}")

    def test_sell_rating_still_fails(self):
        """Regression: 'sell' / 'underperform' still score 0 and are FAIL.

        Patch 2 must NOT accidentally hand partial credit to negative ratings.
        """
        for rating in ("sell", "underperform"):
            with self.subTest(rating=rating):
                with _patch_fund(_stock_fund(rating)), _patch_smart_money():
                    passes, score, details = passes_quality_gate("AAPL", {"rsi": 50})
                joined = " ".join(details).lower()
                self.assertIn(f"fail analyst={rating}", joined,
                              msg=f"{rating!r} must be marked FAIL; got {details}")
                self.assertNotIn("partial", joined,
                                 msg=f"{rating!r} must NOT be partial; got {details}")
                # Sell: eg(+1) + rg(+1) + sell(0) + rsi(+1) = 3.0 (boundary pass)
                self.assertEqual(score, 3.0,
                                 msg=f"{rating!r}: expected 3.0, got {score}")

    def test_partial_credits_combine(self):
        """Two partial credits together tip the int-truncated gate score.

        Patch 2's hold/neutral +0.5 is individually invisible (int-truncation),
        but pairs with Patch 1's SKIP-earnings_growth +0.5 to deliver a full
        point. This test proves the partial-credit mechanism actually moves
        the gate when credits combine.

        Setup (earnings_growth missing + hold rating):
            eg=None    (Patch 1 SKIP)     +0.5
            rg=+3.0    (positive)         +1
            rec='hold' (Patch 2 partial)  +0.5
            rsi=50     (below 70)         +1
            sm=[]      (no smart money)    0
            internal total                3.0 → int(3.0) = 3 → PASSES

        Without Patch 2 the hold credit would be 0, internal total 2.5,
        int(2.5)=2, gate FAILS. So this test is the only assertion in the
        file that detects regression to a pre-Patch-2 state via the integer
        return path alone (no reliance on details string matching).
        """
        fund = {
            "earnings_growth": None,
            "revenue_growth": 3.0,
            "recommendation": "hold",
            "sector": "Technology",
        }
        with _patch_fund(fund), _patch_smart_money():
            passes, score, details = passes_quality_gate("AAPL", {"rsi": 50})
        joined = " ".join(details).lower()

        self.assertTrue(passes,
                        msg=f"combined partials must tip int-gate to pass; got "
                            f"score={score}, details={details}")
        self.assertEqual(score, 3,
                         msg=f"combined partials should yield int(3.0)==3; "
                             f"got {score}, details={details}")
        self.assertIn("skip earnings_growth", joined,
                      msg=f"missing SKIP earnings detail; got {details}")
        self.assertIn("analyst=hold (partial)", joined,
                      msg=f"missing hold partial detail; got {details}")


if __name__ == "__main__":
    unittest.main()
