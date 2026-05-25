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

    Doctrine invariant (HM-CONVICTION-STOP-TIER-FLOOR 2026-05-25): scaled
    stops never go below 0.12 (flat baseline). Conviction-scaling is
    additive — high-conviction earns wider stops, low-conviction inherits
    baseline. We never punish low-conviction with tighter stops than the
    flat default. The original tier table returned 0.08 for conviction
    below 0.70, which actively regressed low-conviction agents in 180d
    backtest (ollama-kimi pf 9.89 -> 4.02). Floor restored 2026-05-25.

    Tiers (HM-CONVICTION-TIER-BOUNDARY-CALIBRATION Opt 1 2026-05-25):
        conviction >= 0.90 -> 0.18  (18% stop, highest-conviction band)
        conviction >= 0.80 -> 0.13  (compressed from 0.15 to reduce
                                     0.80-0.89 band hold-loser-longer regression)
        conviction <  0.80 -> 0.12  (flat baseline; floor invariant)
    """
    if conviction >= 0.90:
        return 0.18
    if conviction >= 0.80:
        return 0.13
    return 0.12
