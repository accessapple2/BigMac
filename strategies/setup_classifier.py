"""
Setup classification for DTE selection in bull_spread_v1.

Returns one of:
  "momentum" -> short DTE (0-5 days, capture the burst)
  "pullback" -> longer DTE (10-21 days, give the bounce room)
  "neutral"  -> default DTE (10 days)

Mock mode: reads from mock_data.MOCK_SETUP.
Live mode: TODO(Task 6+), returns 'neutral' as safe default.
"""
from __future__ import annotations
from .mock_data import is_mock_mode, mock_setup_classification

DTE_BY_SETUP: dict[str, int] = {
    "momentum": 3,
    "pullback": 14,
    "neutral":  10,
}


def classify(ticker: str) -> str:
    if is_mock_mode():
        return mock_setup_classification(ticker)
    return "neutral"  # Safe default until real classifier wired


def dte_for_setup(setup: str) -> int:
    return DTE_BY_SETUP.get(setup, 10)
