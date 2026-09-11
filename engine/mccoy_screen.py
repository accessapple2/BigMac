"""engine/mccoy_screen.py — HM-XO-PLAN-2026-09 Phase 1.2: deterministic
screen for McCoy's twice-daily top-N scan (pre-open, midday).

No LLM calls. Combines existing, already-running signals rather than
inventing new ones (RULE #1 / additive doctrine — reuse, don't
duplicate):

  1. Volume Radar (engine/volume_scanner.py::get_todays_volume_alerts) --
     the full-market (10,000-stock) volume-explosion funnel already
     running every 15 min during market hours, already named "Volume
     Radar" in its own docstrings (build_volume_radar_prompt_section).
  2. Liquidity floor -- NOT engine/universe.py::get_active_universe().
     Tried that first and it's wrong for this purpose: get_active_universe
     is a ~307-name mega-cap-only pool (market cap >= $5B, $ vol >=
     $100M/day) built for long-hold fleet coverage, and it has near-zero
     overlap with Volume Radar's actual hits by construction -- a
     volume-explosion scanner's whole point is to surface names that move
     unusually today, which skews toward small/mid caps the mega-cap
     floor excludes. Live-verified 2026-09-11: intersecting the two
     produced 0/90 candidates. Instead applies a floor sized for THIS
     purpose directly on Volume Radar's own fields: price >= MIN_PRICE
     (avoid penny-stock/manipulation-prone names) and dollar_volume >=
     MIN_DOLLAR_VOLUME (well above the observed p25 ~$59K on a live
     sample, filters genuinely illiquid noise without re-imposing the
     mega-cap bar). Tunable constants below, not hardcoded inline.
  3. Regime (engine/regime_detector.py::detect_regime) -- attached to the
     result for context/logging, not used as a hard per-symbol filter
     (regime is fleet-wide, not symbol-specific).

Cadence context (measured 2026-09-10/11, decision_audit): McCoy currently
fires via _SCAN_TIER2 every 2 hours, each firing scanning the FULL active
universe (600-900 symbols) -- ~12 firings/day, ~175 real signal_emits per
firing on average, ~2,095/day total. This screen (called twice/day
instead) at a 100-symbol cap targets under 200/day, matching the ~10x cut
Phase 1.2 specifies.

ABSTAIN posture: if Volume Radar hasn't produced enough hits yet today
(e.g. called very early, before its 15-min cadence has run a few times),
returns whatever it has rather than padding with a guess -- a short list
is honest, a padded one isn't.
"""
from __future__ import annotations

DEFAULT_LIMIT = 100

# Liquidity floor sized for a mover-screen (Volume Radar hits), not a
# long-hold mega-cap universe -- see module docstring for why
# get_active_universe()'s $100M/day bar doesn't apply here.
MIN_PRICE = 1.00
MIN_DOLLAR_VOLUME = 1_000_000.0


def get_mccoy_screened_symbols(limit: int = DEFAULT_LIMIT) -> dict:
    """Returns {"symbols": [...], "regime": <dict|None>, "n_requested": int,
    "n_found": int, "source_counts": {...}}. No LLM calls, no writes."""
    from engine.volume_scanner import get_todays_volume_alerts

    try:
        alerts = get_todays_volume_alerts(limit=max(limit * 3, 300))
    except Exception:
        alerts = []

    candidates = [
        a for a in alerts
        if (a.get("price") or 0) >= MIN_PRICE
        and (a.get("dollar_volume") or 0) >= MIN_DOLLAR_VOLUME
    ]
    candidates = sorted(candidates, key=lambda a: a.get("relative_volume") or 0, reverse=True)
    symbols = [a["symbol"] for a in candidates[:limit]]

    regime = None
    try:
        from engine.regime_detector import detect_regime
        regime = detect_regime()
    except Exception:
        pass

    return {
        "symbols": symbols,
        "regime": regime,
        "n_requested": limit,
        "n_found": len(symbols),
        "source_counts": {
            "volume_radar_raw": len(alerts),
            "post_liquidity_filter": len(candidates),
        },
    }
