"""Deployment-floor advisory — pages the Admiral when the fleet is badly
under-deployed for the current regime.

HM-DEPLOYMENT-FLOOR 2026-07-10 (S6 findings, Finding 1 / P1).

The S6 performance dig found the fleet at ~7% long equity / 93% cash in a
BULL tape (Blend E BULL_CROSS target 65%) — sitting out a +4.44% SPY run was
the bulk of the -2.95% edge vs SPY. Structural gap: the regime router only
VETOES (avoid-list); no mechanism owns "we're under-deployed for this regime."

This module is the advisory half of the fix (deliberately NOT auto-deployment,
per confirm-guarded doctrine): once per day, compare the fleet's actual
long-equity weight to the regime's Blend E target and NTFY the Admiral when
actual < FLOOR_FRACTION x target in a BULL-family regime. A human decides
what to do about it; this just makes the gap impossible to not-know.

Alert plumbing: engine.alert_channels.send_alert (WARNING -> ntfy + browser),
24h rate limit (HM-U pattern) so a persistent gap pages once a day, not once
per scheduler tick. Restart-safe by construction: the rate limiter is DB-side.

Error posture (docs/DOCTRINE.md): no silent catch; fail-loud with type+repr;
an advisory that errors must never block or crash the trader loop.
"""
from __future__ import annotations

import logging
import os
import sqlite3

from rich.console import Console

console = Console()
logger = logging.getLogger("deployment_floor")

# Alert when actual long-equity weight is below this fraction of the regime
# target (e.g. target 65%, floor 1/3 -> page below ~21.7% deployed).
FLOOR_FRACTION = 1.0 / 3.0

# Only advise in regimes where being in cash is a cost, not a virtue.
BULL_FAMILY = ("BULL", "BULL_CROSS", "BULL_LOW_VOL", "CAUTIOUS_BULL")

# Same exclusions the /api/tactical/allocation surface uses.
EXCLUDED_PLAYERS = ("webull", "enterprise-computer")
INVERSE_ETFS = {"SH", "SDS", "SPXU", "SDOW", "SQQQ", "TZA", "VXX", "DOG", "PSQ", "RWM"}

_done_today = False


def _db_path() -> str:
    return os.environ.get(
        "TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db")
    )


def fleet_long_equity_weight() -> tuple[float, float] | None:
    """(long_equity_weight_pct, total_book_value) across active seats.

    Mirrors /api/tactical/allocation's classification (avg_price valuation —
    an advisory doesn't need marks; the drift band absorbs the noise).
    Returns None on any data problem (logged loudly).
    """
    try:
        conn = sqlite3.connect(_db_path(), timeout=10)
        try:
            conn.row_factory = sqlite3.Row
            players = conn.execute(
                "SELECT id, cash FROM ai_players WHERE is_active=1 "
                "AND id NOT IN ({})".format(",".join("?" * len(EXCLUDED_PLAYERS))),
                EXCLUDED_PLAYERS,
            ).fetchall()
            if not players:
                logger.error("[deployment-floor] no active players found")
                return None
            ids = [p["id"] for p in players]
            total_cash = sum(float(p["cash"] or 0) for p in players)
            positions = conn.execute(
                "SELECT symbol, qty, avg_price, asset_type FROM positions "
                "WHERE qty>0 AND player_id IN ({})".format(",".join("?" * len(ids))),
                ids,
            ).fetchall()
        finally:
            conn.close()
    except Exception as e:
        logger.error(
            f"[deployment-floor] DB read failed: {type(e).__name__}: {e!r}"
        )
        return None

    long_equity = other_val = 0.0
    for pos in positions:
        sym = (pos["symbol"] or "").split("=")[0].upper()
        value = float(pos["qty"] or 0) * float(pos["avg_price"] or 0)
        asset_type = pos["asset_type"] or "stock"
        if asset_type == "option" or sym in INVERSE_ETFS:
            other_val += value
        else:
            long_equity += value

    total = total_cash + long_equity + other_val
    if total <= 0:
        logger.error("[deployment-floor] non-positive total book value")
        return None
    return (long_equity / total * 100.0, total)


def regime_equity_target_pct(regime: str | None) -> float | None:
    """Blend E long_equity_pct for the regime; fallback to the router matrix
    cap when no regime_allocations row exists. None if regime unknown."""
    if not regime:
        return None
    try:
        from engine.regime_router import (
            REGIME_STRATEGY_MATRIX,
            get_regime_allocation,
        )
    except Exception as e:
        logger.error(
            f"[deployment-floor] regime_router import failed: "
            f"{type(e).__name__}: {e!r}"
        )
        return None
    alloc = get_regime_allocation(regime)
    if alloc and alloc.get("long_equity_pct") is not None:
        return float(alloc["long_equity_pct"]) * (
            100.0 if float(alloc["long_equity_pct"]) <= 1.0 else 1.0
        )
    profile = REGIME_STRATEGY_MATRIX.get(regime)
    if profile and profile.get("long_equity_max_pct") is not None:
        return float(profile["long_equity_max_pct"]) * 100.0
    return None


def check_deployment_floor() -> dict | None:
    """Core check — separated for direct testing. Returns the evaluation dict
    (or None on data unavailability)."""
    from engine.regime_router import get_current_regime

    regime = get_current_regime()
    if regime not in BULL_FAMILY:
        return {"regime": regime, "in_scope": False}

    target = regime_equity_target_pct(regime)
    if target is None or target <= 0:
        logger.warning(
            f"[deployment-floor] no equity target for regime {regime!r} — skipping"
        )
        return None

    weight = fleet_long_equity_weight()
    if weight is None:
        return None
    actual_pct, total = weight

    floor_pct = target * FLOOR_FRACTION
    breached = actual_pct < floor_pct
    result = {
        "regime": regime, "in_scope": True,
        "target_pct": round(target, 1), "floor_pct": round(floor_pct, 1),
        "actual_pct": round(actual_pct, 1), "total_book": round(total, 0),
        "breached": breached,
    }
    console.log(
        f"[deployment-floor] regime={regime} actual={actual_pct:.1f}% "
        f"target={target:.0f}% floor={floor_pct:.1f}% "
        f"{'BREACHED' if breached else 'ok'}"
    )

    if breached:
        try:
            from engine.alert_channels import AlertLevel, send_alert
            send_alert(
                message=(
                    f"Fleet is {actual_pct:.1f}% long equity in {regime} — "
                    f"floor is {floor_pct:.1f}% (⅓ of the {target:.0f}% Blend E "
                    f"target). Cash drag was the #1 S6 leak (-2.95% edge). "
                    f"Book ${total:,.0f}. Advisory only — no auto-deployment."
                ),
                level=AlertLevel.WARNING,
                alert_type="deployment_floor",
                title="⚓ DEPLOYMENT FLOOR — fleet under-deployed for regime",
                audience="admin",
                rate_limit_secs=86400,  # HM-U: page at most once/day
            )
        except Exception as e:
            logger.error(
                f"[deployment-floor] alert send failed: {type(e).__name__}: {e!r}"
            )
    return result


def run_deployment_floor_check() -> None:
    """Scheduler entry point. Once per trading day, 8:30–9:30 AZ (after the
    morning entry window so the read reflects the day's deployments)."""
    global _done_today
    from engine.risk_manager import RiskManager
    from engine.market_calendar import az_now

    if not RiskManager.is_market_hours():
        return
    now = az_now()
    if now.hour == 0:
        _done_today = False
        return
    if now.weekday() >= 5:
        return
    if not (8 <= now.hour <= 9):
        return
    if now.hour == 8 and now.minute < 30:
        return
    if _done_today:
        return
    _done_today = True
    try:
        check_deployment_floor()
    except Exception as e:
        logger.error(
            f"[deployment-floor] check error: {type(e).__name__}: {e!r}"
        )
        console.log(
            f"[red][deployment-floor] check error: {type(e).__name__}: {e!r}"
        )
