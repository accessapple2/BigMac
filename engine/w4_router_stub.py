"""
SUPER_MAX Wave 4 — Router Stub (LOG-ONLY)
Gate: router stays silent until get_gate_status()['gate_open'] == True.
Never submits orders. Never touches paper_trader. Never touches execution path.
"""
from __future__ import annotations
import logging
from engine.w4_regime_expectancy import get_gate_status, get_bucket_expectancy

logger = logging.getLogger(__name__)

ROUTING_TABLE = {
    # regime_bucket → suggested structure (W4 suggestion, not execution)
    "LONG·contango·open":       "BULL_CALL_SPREAD",
    "LONG·contango·midday":     "BULL_CALL_SPREAD",
    "LONG·contango·close":      "BULL_CALL_SPREAD",
    "SHORT·contango·open":      "BEAR_PUT_SPREAD",
    "SHORT·contango·midday":    "BEAR_PUT_SPREAD",
    "LONG·backwardation·open":  "OUTRIGHT_CALL",   # volatile → wider swings
    "SHORT·backwardation·open": "OUTRIGHT_PUT",
    # Catch-all
    "default":                  "HOLD",
}


def route_signal(
    signal_id: int,
    setup_tag: str,
    regime_bucket: str,
    w3_strategy_tag: str,
) -> dict:
    """
    Log-only router. Returns routing suggestion but NEVER executes.
    Gate: if no bucket has cleared DSR/PBO/n gate → return GATE_CLOSED.
    """
    status = get_gate_status()

    if not status["gate_open"]:
        logger.debug(
            "[W4 ROUTER] GATE_CLOSED signal=%s setup=%s bucket=%s "
            "(need n>=%d per bucket; cleared=%d)",
            signal_id, setup_tag, regime_bucket,
            status["min_n_required"], status["buckets_cleared"],
        )
        return {
            "signal_id":   signal_id,
            "routing":     "GATE_CLOSED",
            "reason":      f"no bucket has cleared n>={status['min_n_required']}",
            "gate_status": status,
        }

    # Gate open — look up regime-conditional expectancy
    rows = get_bucket_expectancy(setup_tag=setup_tag, regime_bucket=regime_bucket)
    structure = ROUTING_TABLE.get(regime_bucket, ROUTING_TABLE["default"])

    result = {
        "signal_id":       signal_id,
        "regime_bucket":   regime_bucket,
        "w3_tag":          w3_strategy_tag,
        "suggested":       structure,
        "expectancy_rows": rows,
        "routing":         "LOG_ONLY — no execution",
    }

    logger.info(
        "[W4 ROUTER] LOG_ONLY signal=%s setup=%s bucket=%s → %s | E(R)=%s",
        signal_id, setup_tag, regime_bucket, structure,
        rows[0]["expectancy"] if rows else "n/a",
    )
    return result
