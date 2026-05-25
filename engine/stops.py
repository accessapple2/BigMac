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


def get_options_stop_pct(conviction: float) -> float:
    """Conviction-scaled options stop-loss percentage.

    HM-OPTIONS-CONVICTION-STOP-WIRE Phase B 2026-05-25 — third paired
    implementation in the conviction-scaling trio (entry stop + fleet
    trail + options stop). Same allow-list, same flag pattern, same
    NULL-conviction-falls-back semantics as ``get_stop_loss_pct`` and
    ``get_trail_pct``.

    DOCTRINE DEVIATION (Admiral-locked): unlike the equity stops, this
    tier table inverts the floor-invariant direction. Current flat
    baseline is config.OPTIONS_STOP_LOSS_PCT (default 0.50). The brief's
    tier table:

        conviction >= 0.90 -> 0.50  (preserves current 50% baseline)
        conviction >= 0.80 -> 0.40
        conviction <  0.80 -> 0.30  (TIGHTER than current 0.50 baseline)

    Rationale (Admiral): options premium is uniquely vulnerable to theta
    decay + IV crush; tighter stops on low-conviction option bets cut
    capital risk faster, while high-conviction bets retain the existing
    50% room. This intentionally deviates from Rule #5's universal
    floor invariant; Rule #5 will be amended in Phase E to note this
    options-specific exception.

    Banked HM-CONVICTION-TIER-TABLE-CALIBRATION already covers tier-
    boundary review for all three layers.
    """
    if conviction >= 0.90:
        return 0.50
    if conviction >= 0.80:
        return 0.40
    return 0.30


def get_trail_pct(conviction: float) -> float:
    """Conviction-scaled fleet trailing-stop width.

    HM-FLEET-TRAIL-CONVICTION-SCALE Phase B 2026-05-25 — symmetric
    counterpart to ``get_stop_loss_pct``. High-conviction positions get
    a wider trail (let winners breathe past short-term pullbacks); low-
    conviction inherits the flat 3% baseline (floor invariant — same
    doctrine as the stop-loss tier floor).

    Tiers (Admiral-locked 2026-05-25):

        conviction >= 0.90 -> 0.05  (5% trail, widest band)
        conviction >= 0.80 -> 0.04  (4% trail)
        conviction <  0.80 -> 0.03  (3% trail = current flat baseline; floor invariant)

    Banked HM-CONVICTION-TIER-TABLE-CALIBRATION (already open for the
    stop-loss tier) — same boundary set (0.80/0.90) applies here; revisit
    once live shadow data accumulates.
    """
    if conviction >= 0.90:
        return 0.05
    if conviction >= 0.80:
        return 0.04
    return 0.03


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

    Tiers:
        conviction >= 0.90 -> 0.18  (18% stop, highest-conviction band)
        conviction >= 0.80 -> 0.15
        conviction <  0.80 -> 0.12  (flat baseline; floor invariant)

    Banked HM-CONVICTION-TIER-TABLE-CALIBRATION — tier boundaries
    (0.80/0.90) inferred from prior backtester logic; full calibration
    review pending more live data.
    """
    if conviction >= 0.90:
        return 0.18
    if conviction >= 0.80:
        return 0.15
    return 0.12
