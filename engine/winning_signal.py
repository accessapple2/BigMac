"""Stage 0 — Canonical winning-signal definition (ONE place, no copies).

RULE: A signal is "winning" if:
  • Source is UHURA (self-certified at ≥4-of-7 aligned votes, the 86% filter), OR
  • ≥ MIN_BUS_SOURCES independent sources in signals_v2 agree on same ticker/direction
  AND grade is A, B, or ungraded (None).

Everything in the execution pipeline imports from here.
Do NOT duplicate this logic elsewhere.
"""
from __future__ import annotations

# UHURA's internal vote threshold (enforced by uhura.py before it emits to signals_v2)
UHURA_MIN_ALIGNED: int = 4

# Minimum distinct sources in signals_v2 for a non-UHURA signal to qualify
MIN_BUS_SOURCES: int = 2

# UHURA self-certifies — trust its signals without a cross-source count requirement
SELF_CERTIFYING_SOURCES: frozenset[str] = frozenset({"uhura"})

# Valid trade grades — None means ungraded (passes through)
VALID_GRADES: frozenset[str] = frozenset({"A", "B"})


def is_winning(
    source: str,
    source_count: int,
    grade: str | None = None,
) -> bool:
    """Return True iff the signal meets the execution-pipeline winning bar.

    source:       signals_v2.source value (e.g. 'uhura', 'deep_scan', 'fleet')
    source_count: number of distinct sources agreeing on this (symbol, direction) pair
    grade:        trade grade from signals_v2.metadata or buy() provenance field
    """
    grade_ok = grade is None or grade in VALID_GRADES
    if not grade_ok:
        return False
    if source in SELF_CERTIFYING_SOURCES:
        return True  # UHURA already ran its own 4-of-7 filter
    return source_count >= MIN_BUS_SOURCES


def describe() -> str:
    """Human-readable rule summary for logging and docs."""
    return (
        f"UHURA (self-certified at ≥{UHURA_MIN_ALIGNED}-of-7) "
        f"OR ≥{MIN_BUS_SOURCES} bus sources, "
        f"AND grade ∈ ({', '.join(sorted(VALID_GRADES))}, None)"
    )
