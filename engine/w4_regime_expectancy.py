"""
SUPER_MAX Wave 4 — Conditional Expectancy by Regime Bucket
Read-only. Never mutates any DB. Called by router stub + Bridge display.
"""
from __future__ import annotations
import sqlite3
import logging
from typing import Optional

logger = logging.getLogger(__name__)

SIGNALS_DB   = "signal-center/signals.db"
MIN_N        = 50       # minimum trades per bucket before reporting
DSR_GATE     = 0.95
PBO_GATE     = 0.30


def get_bucket_expectancy(
    setup_tag: Optional[str] = None,
    regime_bucket: Optional[str] = None,
    horizon_days: int = 5,
) -> list[dict]:
    """
    Query per-(setup_tag × regime_bucket) expectancy from scored_predictions.
    Returns list of dicts sorted by expectancy desc.
    Only returns buckets with n >= MIN_N.
    Never mutates anything.
    """
    conn = sqlite3.connect(SIGNALS_DB)
    conn.row_factory = sqlite3.Row

    where_clauses = [
        "sp.horizon_days = ?", "sp.scoreable = 1",
        # W4 gate fix: exclude the no-regime COALESCE default — route only on REAL regime buckets
        "sp.w4_gamma_sign != 'UNKNOWN'", "sp.w4_vix_state != 'unknown'", "sp.w4_tod != 'unknown'",
    ]
    params = [horizon_days]

    if setup_tag:
        where_clauses.append("sp.setup_tag = ?")
        params.append(setup_tag)
    if regime_bucket:
        where_clauses.append("sp.w4_regime_bucket = ?")
        params.append(regime_bucket)

    where = " AND ".join(where_clauses)

    rows = conn.execute(f"""
        SELECT
            sp.setup_tag,
            sp.w4_regime_bucket,
            COUNT(*)                          AS n,
            ROUND(AVG(sp.r_multiple), 4)      AS avg_r,
            ROUND(
                SUM(CASE WHEN sp.r_multiple > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*),
                4
            )                                 AS win_rate,
            ROUND(
                AVG(sp.r_multiple) * (
                    SUM(CASE WHEN sp.r_multiple > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*)
                ) - (1 - SUM(CASE WHEN sp.r_multiple > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*)),
                4
            )                                 AS expectancy,
            MIN(sp.scored_at)                 AS first_seen,
            MAX(sp.scored_at)                 AS last_seen
        FROM scored_predictions sp
        WHERE {where}
        GROUP BY sp.setup_tag, sp.w4_regime_bucket
        HAVING COUNT(*) >= {MIN_N}
        ORDER BY expectancy DESC
    """, params).fetchall()

    conn.close()
    return [dict(r) for r in rows]


def get_gate_status() -> dict:
    """
    Report accrual diversity and gate clearance.
    Returns summary dict for NTFY / Bridge display.
    Never mutates anything.
    """
    conn = sqlite3.connect(SIGNALS_DB)

    total = conn.execute(
        "SELECT COUNT(*) FROM scored_predictions WHERE scoreable=1 AND horizon_days=5"
    ).fetchone()[0]

    buckets = conn.execute("""
        SELECT w4_regime_bucket, COUNT(*) as n,
               ROUND(AVG(r_multiple),4) as avg_r
        FROM scored_predictions
        WHERE scoreable=1 AND horizon_days=5
          AND w4_regime_bucket IS NOT NULL
          AND w4_gamma_sign != 'UNKNOWN' AND w4_vix_state != 'unknown' AND w4_tod != 'unknown'
        GROUP BY w4_regime_bucket
        ORDER BY n DESC
    """).fetchall()

    cleared = [b for b in buckets if b[1] >= MIN_N]

    conn.close()
    return {
        "total_scored":    total,
        "buckets_seen":    len(buckets),
        "buckets_cleared": len(cleared),
        "gate_open":       len(cleared) > 0,
        "cleared_detail":  [{"bucket": b[0], "n": b[1], "avg_r": b[2]} for b in cleared],
        "all_buckets":     [{"bucket": b[0], "n": b[1], "avg_r": b[2]} for b in buckets],
        "min_n_required":  MIN_N,
    }
