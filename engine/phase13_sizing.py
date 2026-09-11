"""engine/phase13_sizing.py — HM-XO-PLAN-2026-09 Phase 1.3: alpha-scaled sizing.

Spec: docs/XO_PLAN_2026-09.md, "Alpha-scaled sizing tier" + "Confidence input to
sizing" sections (verbatim copy also at data/reports/relay/relay_2026-09-11_
phase13_spec_for_review.md). Gated by config.PHASE_1_3_ALPHA_SIZING_ENABLED,
default False -- see that flag's comment in config.py for the full scope note
(player scope, what's explicitly NOT built here).

Wires into engine.paper_trader.execute_signal()'s BUY path, which currently
always calls buy() at the unconditional sizing_multiplier=1.0 default for the
Arena/McCoy call chain (confirmed 2026-09-11: the only live caller passing
sizing_multiplier explicitly today is engine/crew_scanner.py's
troi_caution_multiplier).

Tiers, from the symbol's most recent composite_alpha.composite_score
(data/alpha_signals.db):
  >= 0.6          -> 1.0 (full)  -- ONLY if calibration_map.has_calibration_
                                     evidence(regime, stated_confidence) is True
  0.3 <= x < 0.6  -> 0.5 (base)
  < 0.3 or None   -> 1.0 (unaffected -- matches buy()'s existing default,
                          i.e. Phase 1.3 does not engage at all)

Fail-closed rule (the spec's "Confidence input to sizing" section):
get_calibrated_confidence() itself stays fail-open (unchanged, per its own
module docstring) -- this module applies a STRICTER rule on top, specifically
for sizing: a (regime, confidence-bucket) with no real calibration evidence
never earns the 1.0 full tier, no matter how high composite_alpha or the raw
stated confidence are. It's capped at 0.5 (base) instead.

Every decision returns (multiplier, reason_str) -- the reason is logged by the
caller so a first live dry-run's output is auditable line-by-line, not just a
number.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from engine.calibration_map import has_calibration_evidence

ALPHA_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "alpha_signals.db"

ALPHA_FULL_TIER_MIN = 0.6
ALPHA_BASE_TIER_MIN = 0.3
FULL_TIER_MULTIPLIER = 1.0
BASE_TIER_MULTIPLIER = 0.5
UNAFFECTED_MULTIPLIER = 1.0  # matches buy()'s own existing default


def _get_composite_alpha(symbol: str) -> float | None:
    """Most recent composite_alpha.composite_score for symbol, or None if the
    symbol has no alpha-signals row at all. Same query shape as
    engine/crew_scanner.py's _get_live_alpha(), except this returns None
    (not 0.0) for "no data" -- the two cases ("alpha genuinely scored near
    zero" vs "no alpha data exists for this symbol") mean different things
    for the tier logic below and must stay distinguishable."""
    try:
        conn = sqlite3.connect(str(ALPHA_DB_PATH), timeout=5)
        row = conn.execute(
            "SELECT composite_score FROM composite_alpha WHERE symbol=? "
            "ORDER BY created_at DESC LIMIT 1",
            (symbol,),
        ).fetchone()
        conn.close()
        return float(row[0]) if row and row[0] is not None else None
    except Exception:
        return None


def compute_alpha_sizing_multiplier(
    symbol: str, regime: str, stated_confidence: float
) -> tuple[float, str]:
    """Returns (sizing_multiplier, reason) for paper_trader.buy()'s
    sizing_multiplier kwarg. Never raises -- any internal failure (DB
    unreachable, bad data) falls back to UNAFFECTED_MULTIPLIER (1.0, the
    existing default), same fail-safe posture as _get_composite_alpha()
    and get_calibrated_confidence()."""
    try:
        alpha = _get_composite_alpha(symbol)
        if alpha is None:
            return UNAFFECTED_MULTIPLIER, (
                f"no composite_alpha data for {symbol} -- Phase 1.3 does not apply"
            )
        if alpha < ALPHA_BASE_TIER_MIN:
            return UNAFFECTED_MULTIPLIER, (
                f"composite_alpha={alpha:.3f} < {ALPHA_BASE_TIER_MIN} -- "
                f"below the sizing-tier floor, Phase 1.3 does not apply"
            )
        if alpha >= ALPHA_FULL_TIER_MIN:
            if has_calibration_evidence(regime, stated_confidence):
                return FULL_TIER_MULTIPLIER, (
                    f"composite_alpha={alpha:.3f} >= {ALPHA_FULL_TIER_MIN}, "
                    f"calibration evidence present for regime={regime} "
                    f"stated_confidence={stated_confidence:.2f} -- full tier"
                )
            return BASE_TIER_MULTIPLIER, (
                f"composite_alpha={alpha:.3f} >= {ALPHA_FULL_TIER_MIN} but NO "
                f"calibration evidence for regime={regime} "
                f"stated_confidence={stated_confidence:.2f} -- fail-closed to "
                f"base tier (see docs/XO_PLAN_2026-09.md 'Confidence input to sizing')"
            )
        return BASE_TIER_MULTIPLIER, (
            f"composite_alpha={alpha:.3f} in [{ALPHA_BASE_TIER_MIN}, "
            f"{ALPHA_FULL_TIER_MIN}) -- base tier"
        )
    except Exception as e:
        return UNAFFECTED_MULTIPLIER, (
            f"phase13_sizing error ({type(e).__name__}: {e}) -- falling back "
            f"to unaffected default"
        )
