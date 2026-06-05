"""
SUPER_MAX Wave 2 — Bracket Sizing Calculator
Observation-only. Never called from any execution path.
"""

from __future__ import annotations
import logging

logger = logging.getLogger(__name__)

# ── Constants (tune here, not scattered in callers) ──────────────────────────
DEFAULT_ACCOUNT_EQUITY  = 99_885.0   # Alpaca paper equity — update via caller
DEFAULT_RISK_PCT        = 0.01       # 1% per signal (W0 baseline)
MIN_STOP_DISTANCE_PCT   = 0.002      # 0.2% — below this stop is noise, SKIP
MAX_STOP_DISTANCE_PCT   = 0.08       # 8%  — wider than this, HALF size
MAX_SHARES              = 500        # hard cap — observation sanity guard
MIN_SHARES              = 1

TIER_FULL  = "FULL"
TIER_HALF  = "HALF"
TIER_SKIP  = "SKIP"


def calculate_bracket(
    entry_price: float,
    stop_price: float,
    take_profit_price: float,
    account_equity: float = DEFAULT_ACCOUNT_EQUITY,
    risk_pct: float = DEFAULT_RISK_PCT,
) -> dict:
    """
    Compute observation-only bracket sizing for one shadow signal.

    Returns a dict ready to be written into trade_signals w2_* columns.
    Never submits orders. Never touches paper_trader.
    """
    result = {
        "w2_account_risk_pct":   risk_pct,
        "w2_risk_dollars":       None,
        "w2_stop_distance_pct":  None,
        "w2_shares_or_contracts": None,
        "w2_bracket_tier":       TIER_SKIP,
        "w2_sizing_note":        "",
    }

    # ── Guard: prices must be positive ───────────────────────────────────────
    if not (entry_price > 0 and stop_price > 0 and take_profit_price > 0):
        result["w2_sizing_note"] = "SKIP: non-positive price(s)"
        return result

    # ── Guard: stop must be below entry (long only for now) ──────────────────
    if stop_price >= entry_price:
        result["w2_sizing_note"] = "SKIP: stop >= entry (short not supported in W2)"
        return result

    stop_distance_pct = (entry_price - stop_price) / entry_price
    risk_dollars      = account_equity * risk_pct
    result["w2_stop_distance_pct"] = round(stop_distance_pct, 6)
    result["w2_risk_dollars"]      = round(risk_dollars, 2)

    # ── Tier assignment ───────────────────────────────────────────────────────
    if stop_distance_pct < MIN_STOP_DISTANCE_PCT:
        result["w2_bracket_tier"] = TIER_SKIP
        result["w2_sizing_note"]  = f"SKIP: stop distance {stop_distance_pct:.3%} < min {MIN_STOP_DISTANCE_PCT:.3%}"
        return result

    if stop_distance_pct > MAX_STOP_DISTANCE_PCT:
        effective_risk = risk_dollars / 2.0
        tier = TIER_HALF
    else:
        effective_risk = risk_dollars
        tier = TIER_FULL

    # ── Share count ───────────────────────────────────────────────────────────
    raw_shares = effective_risk / (entry_price * stop_distance_pct)
    shares = max(MIN_SHARES, min(MAX_SHARES, round(raw_shares)))

    result["w2_bracket_tier"]        = tier
    result["w2_shares_or_contracts"] = shares
    result["w2_sizing_note"]         = (
        f"{tier}: {shares} sh @ ${entry_price:.2f} | "
        f"stop ${stop_price:.2f} ({stop_distance_pct:.2%}) | "
        f"target ${take_profit_price:.2f} | "
        f"risk ${effective_risk:.0f}"
    )

    logger.debug("[W2] %s", result["w2_sizing_note"])
    return result
