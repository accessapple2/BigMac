"""engine/phase13_sizing.py — HM-XO-PLAN-2026-09 Phase 1.3: position sizing inputs.

Rebuilt 2026-09-11 after Admiral review rejected the first version: that
version treated "alpha below threshold" and "alpha missing entirely" as
neutral (1.0, unchanged), which meant the least-evidenced trades could size
as large as the best-evidenced ones -- fail-OPEN on the exact dimension the
spec said must be fail-closed. Fixed here: missing data and low alpha both
size SMALL, never neutral, never large.

Two INDEPENDENT sizing dimensions, kept in separate functions with separate
config flags (per Admiral review) rather than one combined function:

1. compute_alpha_sizing_multiplier(symbol, as_of=None) -- gated by
   config.PHASE_1_3_ALPHA_SIZING_ENABLED. Wired into engine.paper_trader.
   execute_signal()'s BUY path.

2. compute_calibration_sizing_multiplier(regime, stated_confidence) -- gated
   by config.PHASE_1_3_CALIBRATION_SIZING_ENABLED. Built here, NOT wired into
   any call site yet -- see "Calibration dimension: an honest design note"
   below for why, and see engine.calibration_map for has_calibration_evidence()
   itself.

Neither function has any branch that can return more than 1.0. There is no
"boost" tier in this design -- the ladder only ever holds size at the
existing default or reduces it.

## Alpha ladder (fixed, monotonic, fail-closed)

  alpha >= 0.6            -> 1.0  (full)
  alpha in [0.3, 0.6)     -> 0.5  (base)
  alpha < 0.3             -> 0.25 (low -- the gate upstream should already
                                    have blocked this; if it didn't, size it
                                    small, don't reward it)
  no alpha data at all    -> 0.25 (low -- same as unknown/no calibration
                                    evidence: missing evidence never earns
                                    full size)
  internal error          -> 0.25 (low -- an error means we don't actually
                                    know the alpha; "don't know" gets the
                                    same treatment as "don't know", not the
                                    treatment of "fine, unchanged")

## Calibration dimension: an honest design note

has_calibration_evidence() requires >= calibration_map.MIN_BUCKET_N (8) real,
settled, trade_fire-linked observations in the exact (regime, 0.1-wide
confidence bucket) being checked. Live count, checked 2026-09-11: 3 clean
trade_fire-linked settled rows fleet-wide, total, across every regime and
every player. McCoy specifically has fired only 10 trade_fire events since
2026-07-01 (2.3 months) -- and those 10 scatter across whatever regimes and
confidence buckets applied on each of those 10 days, not all landing in one
bucket. At that scatter rate, no single bucket reaches n=8 on any plausible
horizon -- this is not "wait a few more weeks," it is a throughput the
current design cannot cross. **Corrected 2026-09-11 (an earlier version of
this note was wrong): the bottleneck is NOT an unpopulated trade_id.**
Verified directly: all 198 fleet-wide trade_fire events have a populated
trade_id, and every one resolves to a real trades row (0 dangling). The
real constraint is settlement lag -- 195 of those 198 are matched to a
trade that hasn't closed yet (no realized P&L), only 3 have settled. This
isn't a logging gap fixable by backfilling anything; a position's P&L
doesn't exist until it closes, and closing faster isn't a code change.
Filed: docs/XO_BACKLOG.md, "calibration_map.py's trade_fire->trade_id join
is too narrow to ever be useful" -- owner Scotty, target date unslotted,
no decision made yet between its two proposed directions (relax
calibration_map to a direct player-scoped trades query, or find another
way to raise the settled-trade count). Filing this correction alongside it
rather than leaving the wrong "unpopulated trade_id" framing standing.

**What this design does instead of waiting:** nothing, today -- the
calibration dimension is built and testable in isolation but deliberately
NOT wired into any live call site (config.PHASE_1_3_CALIBRATION_SIZING_ENABLED
exists and is read nowhere). Wiring it in, and deciding how it would combine
with the alpha ladder above (multiply the two multipliers? take the more
conservative of the two? gate one on the other?), is an open design question
this commit does not answer -- shipping a combination scheme for a dimension
that structurally cannot accumulate evidence would be building on sand. The
honest options, for a human decision, are: (a) leave this dimension
permanently unwired until settled-trade coverage improves upstream, since
fixing throughput here without fixing that changes nothing; (b) relax
calibration_map's own fleet-wide, trade_fire-gated query to something that
can see more of the real, closed McCoy trade history directly (the backlog
item's own suggestion); or (c) lower MIN_BUCKET_N as a deliberate, documented
tradeoff -- not something this file decides on its own.
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
LOW_TIER_MULTIPLIER = 0.25  # alpha < 0.3, missing data, or an internal error -- never neutral, never full


def _get_composite_alpha(symbol: str, as_of: str | None = None) -> float | None:
    """composite_alpha.composite_score for symbol.

    composite_alpha carries real daily history (UNIQUE(as_of_date, symbol),
    64 distinct as_of_date values going back to 2026-04-09 as of this
    build -- confirmed live, NOT a single-row-per-symbol table as the first
    version of this file assumed). Two modes:

    - as_of=None (live use, at decision time): the most recent snapshot,
      ordered by as_of_date (the snapshot's own date), not created_at (when
      the row happened to be written -- a backfill could write an old
      as_of_date late and created_at would misorder it).
    - as_of='YYYY-MM-DD' (replay use, scripts/phase13_sizing_dry_run.py):
      the most recent snapshot ON OR BEFORE that date -- what a decision
      made ON that date would actually have seen, not today's value.

    Returns None if no qualifying row exists at all (never fabricates 0.0
    for "no data" -- see module docstring, this is treated as LOW_TIER, not
    neutral, by the caller)."""
    try:
        conn = sqlite3.connect(str(ALPHA_DB_PATH), timeout=5)
        if as_of is None:
            row = conn.execute(
                "SELECT composite_score FROM composite_alpha WHERE symbol=? "
                "ORDER BY as_of_date DESC LIMIT 1",
                (symbol,),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT composite_score FROM composite_alpha WHERE symbol=? "
                "AND as_of_date <= ? ORDER BY as_of_date DESC LIMIT 1",
                (symbol, as_of),
            ).fetchone()
        conn.close()
        return float(row[0]) if row and row[0] is not None else None
    except Exception:
        return None


def compute_alpha_sizing_multiplier(symbol: str, as_of: str | None = None) -> tuple[float, str]:
    """Returns (sizing_multiplier, reason). Never raises -- an internal
    failure is LOW_TIER (0.25), not FULL_TIER -- "don't know" gets the same
    treatment as "don't know", never the treatment of "unchanged/fine".

    as_of: None for live use (decision-time value); a 'YYYY-MM-DD' date
    string for historical replay (scripts/phase13_sizing_dry_run.py) --
    see _get_composite_alpha()'s docstring."""
    try:
        alpha = _get_composite_alpha(symbol, as_of=as_of)
        _when = f" as of {as_of}" if as_of else ""
        if alpha is None:
            return LOW_TIER_MULTIPLIER, (
                f"no composite_alpha data for {symbol}{_when} -- missing evidence "
                f"never earns full size, sized low"
            )
        if alpha >= ALPHA_FULL_TIER_MIN:
            return FULL_TIER_MULTIPLIER, (
                f"composite_alpha={alpha:.3f}{_when} >= {ALPHA_FULL_TIER_MIN} -- full tier"
            )
        if alpha >= ALPHA_BASE_TIER_MIN:
            return BASE_TIER_MULTIPLIER, (
                f"composite_alpha={alpha:.3f}{_when} in [{ALPHA_BASE_TIER_MIN}, "
                f"{ALPHA_FULL_TIER_MIN}) -- base tier"
            )
        return LOW_TIER_MULTIPLIER, (
            f"composite_alpha={alpha:.3f}{_when} < {ALPHA_BASE_TIER_MIN} -- the gate "
            f"upstream should already have blocked this; sized low, not rewarded"
        )
    except Exception as e:
        return LOW_TIER_MULTIPLIER, (
            f"phase13_sizing error ({type(e).__name__}: {e}) -- treated as unknown, sized low"
        )


def compute_calibration_sizing_multiplier(regime: str, stated_confidence: float) -> tuple[float, str]:
    """Independent sizing dimension from the alpha ladder above -- see module
    docstring, "Calibration dimension: an honest design note", for why this
    is built but NOT wired into any live call site as of this commit.
    config.PHASE_1_3_CALIBRATION_SIZING_ENABLED exists and is read nowhere.

    Never raises. Same LOW_TIER-for-unknown posture as the alpha function."""
    try:
        if has_calibration_evidence(regime, stated_confidence):
            return FULL_TIER_MULTIPLIER, (
                f"calibration evidence present for regime={regime} "
                f"stated_confidence={stated_confidence:.2f} -- full tier"
            )
        return LOW_TIER_MULTIPLIER, (
            f"no calibration evidence for regime={regime} "
            f"stated_confidence={stated_confidence:.2f} -- fail-closed, sized low"
        )
    except Exception as e:
        return LOW_TIER_MULTIPLIER, (
            f"phase13_sizing error ({type(e).__name__}: {e}) -- treated as unknown, sized low"
        )
