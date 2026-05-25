"""HM-RISK-MANAGER-CONVICTION-STOP-WIRE 2026-05-24 — shared conviction-
scaled stop helpers.

Extracted from engine/backtester.py so the production exit-evaluator
(engine/risk_manager.py) can import without depending on a backtest-
flavored module and without importing a module-private name. Backtester
now imports from here too — single source of truth.

Callers must check conviction is not None before invoking; this function
does not have a sentinel/fallback path. NULL conviction at the call site
should route to the flat-stop default (the non-conviction-scaled path).
"""
from __future__ import annotations


def get_stop_loss_pct(conviction: float) -> float:
    """Conviction-scaled stop loss percentage.

    Wider stops for higher conviction — let winners breathe.

    Tiers (matches HM-DEEPSEEK-STOP-DISCIPLINE backtester behavior):
        conviction >= 0.90 -> 0.18  (18% stop, highest-conviction band)
        conviction >= 0.80 -> 0.15
        conviction >= 0.70 -> 0.12  (same as fleet flat default)
        conviction <  0.70 -> 0.08  (8% stop, lowest-conviction band)
    """
    if conviction >= 0.90:
        return 0.18
    elif conviction >= 0.80:
        return 0.15
    elif conviction >= 0.70:
        return 0.12
    else:
        return 0.08
