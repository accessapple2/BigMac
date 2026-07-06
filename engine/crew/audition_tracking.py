"""HM-INCUMBENT-AUDITION-TRACKER-2026-07-05 + HM-SWEEP-SIGNALS-TABLE-BLIND-SPOT fix.

Split into its own module (not weekly_tuning_crew.py) deliberately: this file
is brand new and imported by nothing yet, so it can be built and fully tested
without any risk to the live-running trader process.

Two separate problems, both from the same Admiral audit session:

1. `_run_auditions()` only scores `halt_mode != 'active'` bench candidates --
   it structurally never touches the two currently-active ACTIVE-AUDITIONING
   seats (options-sosnoff, qwen3-8b-flash), since they already hold seats.
   track_incumbent_auditions() fixes that.

2. Both `_run_auditions()` and the standalone `fleet_realism_sweep*.py`
   measure activity by counting `signals` rows only (HM-SWEEP-SIGNALS-TABLE-
   BLIND-SPOT) -- agents that route through options_trades or a non-standard
   pipeline show clean_signals_in_db=0 and get silently mislabeled
   "insufficient_data". score_bench_candidate_from_real_trades() fixes that.

*** HM-EDGE-PROVENANCE RULING (2026-07-05) -- SUPERSEDES THE ORIGINAL FLOOR/
DEADLINE DESIGN BELOW. READ THIS BEFORE TOUCHING EITHER INCUMBENT ENTRY. ***

The original design counted ANY clean trade (internal-simulation included)
toward each incumbent's 20-trade audition. A same-day venue-provenance audit
(HM-EDGE-PROVENANCE) found:
  - options-sosnoff's 84 CSPs: 100% internal-simulation. `engine.options_exec`'s
    own docstring: "NO broker API is called." Her "fill price" is a VIX-scaled
    FORMULA (engine.wheel_strategy.py), not a fetched quote. Zero of her rows
    have ever had a `broker_order_id`.
  - qwen3-8b-flash's 16 clean trades: also 100% `execution_type='simulated'`,
    zero `alpaca_order_id`. Same disease, different agent -- the routing
    mechanism exists generally (163 other agents' equity trades are real
    Alpaca fills), she just isn't being routed through it.

Admiral ruling: counting formula-priced or otherwise-unrouted trades measures
a heuristic, not a trader. BOTH incumbent auditions are SUSPENDED as
currently defined. Redefined: an audition trade only counts if it carries
real broker evidence (`broker_order_id`/`alpaca_order_id` present). The
displayed count stays 0/20 for both -- but its meaning changes from "quiet"
to "not yet routable." Each agent's clock restarts once broker routing for
their structure is confirmed live (HM-ROUTE-TO-BROKER, addendum item 8,
promoted to top of Phase 2) -- not started as of this ruling, so no new
floor/deadline is set yet. `INCUMBENT_AUDITIONS`' original floor/deadline
values are kept below for the historical record (what the pre-ruling design
used) but are NOT read by track_incumbent_auditions() while suspended.

This is fleet-wide, not incumbent-specific: "a trade that produces no broker
evidence doesn't count toward anything -- auditions, gates, leaderboards"
(HM-EDGE-PROVENANCE ruling item 4). score_bench_candidate_from_real_trades()
below is updated to match -- it now requires broker evidence too, not just
"any clean trade in trades/options_trades".
"""
from __future__ import annotations

INCUMBENT_AUDITIONS = {
    "options-sosnoff": {
        "source": "options_trades",
        "suspended": True,
        # Historical record only -- NOT read while suspended. Was the
        # verified CSP cap-gate-live timestamp (first live boot after commit
        # 75b63f1) / a 6-week deadline from that floor.
        "pre_ruling_floor": "2026-07-04 09:34:26",
        "pre_ruling_deadline": "2026-08-15",
        "suspension_reason": (
            "HM-EDGE-PROVENANCE (2026-07-05): all 84 historical CSPs are "
            "internal-simulation, formula-priced (VIX-scaled estimate, not a "
            "fetched quote), zero broker_order_id ever. No code path exists "
            "yet that routes her CSP structure to a real broker -- "
            "engine.alpaca_options.py (the only module that calls a real "
            "Alpaca options API) doesn't serve options-sosnoff. Audition "
            "redefined: 20 BROKER-EXECUTED CSPs. Clock restarts once "
            "HM-ROUTE-TO-BROKER lands for her structure."
        ),
    },
    "qwen3-8b-flash": {
        "source": "trades",
        "suspended": True,
        "pre_ruling_floor": "2026-05-14",
        "pre_ruling_deadline": "2026-08-16",
        "suspension_reason": (
            "HM-EDGE-PROVENANCE (2026-07-05): verified all 16 clean-window "
            "trades are execution_type='simulated', zero alpaca_order_id. "
            "Same suspension logic as options-sosnoff, different root cause "
            "-- the equity routing mechanism already works for 163 other "
            "fleet trades (ollie-auto/neo-matrix/guardian-of-forever), she "
            "simply isn't being routed through it. Audition redefined: 20 "
            "BROKER-EXECUTED trades. Clock restarts once her own trades "
            "start carrying a real alpaca_order_id."
        ),
    },
}

TARGET_GUARDED_TRADES = 20


def clean_options_trade_count(conn, player_id: str, floor: str,
                               require_broker_execution: bool = False) -> dict:
    """Closed CSP trade rollup from options_trades since `floor`.

    require_broker_execution=True (HM-EDGE-PROVENANCE 2026-07-05) restricts
    to rows with a real broker_order_id -- internal-simulation rows (the
    overwhelming majority as of this ruling) are excluded. Default False
    preserves the pre-ruling "any clean trade" behavior for callers that
    explicitly want the unfiltered view (e.g. reporting raw activity, not
    scoring an audition/gate).

    No known_contaminated-equivalent column exists on options_trades
    (checked during HM-ROSTER-RATIONALIZE, 2026-07-05) -- book_tag='fleet'
    + status='closed' is the full cleanliness filter available beyond that.
    """
    broker_clause = "AND broker_order_id IS NOT NULL " if require_broker_execution else ""
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS n, COALESCE(SUM(pnl), 0) AS total_pnl,
               SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS wins
        FROM options_trades
        WHERE agent_id = ? AND book_tag = 'fleet' AND status = 'closed'
          AND entry_date >= ? {broker_clause}
        """,
        (player_id, floor),
    ).fetchone()
    n = row["n"] or 0
    return {
        "trade_count": n,
        "total_pnl": round(row["total_pnl"] or 0.0, 2),
        "win_rate": round(100.0 * (row["wins"] or 0) / n, 1) if n else None,
    }


def broker_executed_trade_count(conn, player_id: str, floor: str) -> dict:
    """HM-EDGE-PROVENANCE 2026-07-05: real broker-confirmed trade rollup from
    `trades` since `floor`. Requires BOTH execution_type IN ('alpaca_paper',
    'alpaca') AND alpaca_order_id IS NOT NULL -- not just any clean trade.
    engine.trades_filter.fleet_realized_pnl() does not filter on venue at
    all, so it can't answer "was this actually broker-executed"; this does.
    """
    row = conn.execute(
        """
        SELECT COUNT(*) AS n, COALESCE(SUM(realized_pnl), 0) AS total_pnl,
               SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) AS wins
        FROM trades
        WHERE player_id = ? AND action IN ('SELL', 'COVER') AND realized_pnl IS NOT NULL
          AND executed_at >= ?
          AND execution_type IN ('alpaca_paper', 'alpaca') AND alpaca_order_id IS NOT NULL
        """,
        (player_id, floor),
    ).fetchone()
    n = row["n"] or 0
    return {
        "trade_count": n,
        "total_pnl": round(row["total_pnl"] or 0.0, 2),
        "win_rate": round(100.0 * (row["wins"] or 0) / n, 1) if n else None,
    }


def wheel_scan_diagnosis(conn, floor: str) -> dict:
    """HM-CSP-WHEEL-SCAN-LOG-2026-07-05: distinguishes a structural zero
    (cap gate blocking every attempt, or no scan ever reaching evaluation)
    from a performance zero (setups evaluated, none opened). Reads whatever
    csp_wheel_scan_log has recorded since `floor` -- reports "no data
    captured yet" honestly if the table doesn't exist this boot or is
    empty, rather than fabricating a verdict.
    """
    try:
        rows = conn.execute(
            """SELECT outcome, COUNT(*) AS n FROM csp_wheel_scan_log
               WHERE scanned_at >= ? GROUP BY outcome""",
            (floor,),
        ).fetchall()
    except Exception:
        return {"captured": False, "note": "csp_wheel_scan_log table not present this boot"}

    counts = {r["outcome"]: r["n"] for r in rows}
    total_scans = sum(counts.values())
    if total_scans == 0:
        return {
            "captured": True,
            "total_scans": 0,
            "note": "no wheel scans recorded since floor -- cannot yet distinguish "
                    "structural from performance zero",
        }

    evaluated = counts.get("scan_completed", 0)
    cap_blocked = counts.get("cap_blocked", 0)
    mechanical_skips = counts.get("vix_skip", 0) + counts.get("max_positions_reached", 0)
    if evaluated == 0 and cap_blocked > 0:
        diagnosis = "structural_zero: cap gate blocked every attempt"
    elif evaluated == 0 and cap_blocked == 0:
        diagnosis = "structural_zero: no scan reached evaluation (VIX/max-positions/market-hours)"
    else:
        diagnosis = "setups were evaluated -- any remaining zero is a performance/opportunity read, not mechanical"

    return {
        "captured": True,
        "total_scans": total_scans,
        "scan_completed": evaluated,
        "cap_blocked": cap_blocked,
        "mechanical_skips": mechanical_skips,
        "diagnosis": diagnosis,
    }


def track_incumbent_auditions(conn) -> list[dict]:
    """Progress for the two currently-active ACTIVE-AUDITIONING seats.
    Neither is scored by _run_auditions() in weekly_tuning_crew.py -- that
    function only scores halt_mode != 'active' bench candidates; these two
    already hold seats.

    BOTH are SUSPENDED per HM-EDGE-PROVENANCE (2026-07-05) -- see module
    docstring. clean_guarded_trades now only counts BROKER-EXECUTED trades
    (require_broker_execution=True throughout), which is 0 for both as of
    this ruling. No floor/deadline/days_remaining in the output while
    suspended -- the clock hasn't restarted (HM-ROUTE-TO-BROKER not built
    yet), so a countdown would be fiction.
    """
    results = []
    for pid, cfg in INCUMBENT_AUDITIONS.items():
        if cfg["source"] == "options_trades":
            stats = clean_options_trade_count(
                conn, pid, cfg["pre_ruling_floor"], require_broker_execution=True
            )
        else:
            stats = broker_executed_trade_count(conn, pid, cfg["pre_ruling_floor"])

        entry = {
            "player_id": pid,
            "suspended": cfg["suspended"],
            "suspension_reason": cfg["suspension_reason"],
            "clean_guarded_trades": stats["trade_count"],
            "target": TARGET_GUARDED_TRADES,
            "total_pnl": stats["total_pnl"],
            "win_rate": stats["win_rate"],
            "status": "suspended_pending_broker_routing",
        }
        if cfg["source"] == "options_trades":
            entry["structural_diagnosis"] = wheel_scan_diagnosis(conn, cfg["pre_ruling_floor"])
        results.append(entry)
    return results


def score_bench_candidate_from_real_trades(conn, player_id: str, cutoff: str) -> dict | None:
    """HM-SWEEP-SIGNALS-TABLE-BLIND-SPOT fix, for _run_auditions() to call
    when a bench candidate has zero `signals` rows.

    HM-EDGE-PROVENANCE (2026-07-05) update: now requires broker-execution
    evidence (require_broker_execution=True), not just any clean trade in
    trades/options_trades -- "a trade that produces no broker evidence
    doesn't count toward anything, auditions, gates, leaderboards" applies
    here too, not only to the two incumbents. This will report fewer/no
    real trades for candidates whose activity turns out to be internal-sim
    (that IS the correct, honest answer post-ruling, not a regression).

    Returns None if there's genuinely no broker-executed activity either
    (the caller should fall back to plain "insufficient_data" in that case).
    """
    from config import AUDITION_CRITERIA

    real = broker_executed_trade_count(conn, player_id, cutoff)
    real_opts = clean_options_trade_count(conn, player_id, cutoff, require_broker_execution=True)
    real_count = (real["trade_count"] or 0) + real_opts["trade_count"]
    if not real_count:
        return None

    real_pnl = round((real["total_pnl"] or 0.0) + real_opts["total_pnl"], 2)
    detail = {
        "measured_via": "broker_executed_trades_and_or_options_trades",
        "real_trades_count": real["trade_count"],
        "real_options_trades_count": real_opts["trade_count"],
        "real_total_pnl": real_pnl,
        "note": "no signals-table rows -- scored from broker-executed trade history "
                "instead (HM-EDGE-PROVENANCE: internal-sim trades excluded even if "
                "present; spam_rate_pct/friction_to_pnl not applicable, omitted "
                "rather than faked)",
    }
    if real_count < AUDITION_CRITERIA["min_guarded_trades"]:
        verdict = "insufficient_data"
    else:
        verdict = "pass" if real_pnl > 0 else "fail"
    return {"verdict": verdict, "detail": detail}
