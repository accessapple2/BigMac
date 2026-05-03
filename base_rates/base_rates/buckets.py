"""Bucket assignment for base rate features. Pure functions, no I/O."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# Default bucket boundaries. Overrideable via the `buckets` kwarg in query().
DEFAULT_BUCKETS: dict[str, list[float]] = {
    # move_intensity: today's pct_change (as decimal, e.g. 0.05 = +5%)
    # v0.2: collapsed extreme tails (was split at +/-10%) to keep N usable for big moves
    # Buckets: <-5, -5/-3, -3/-1, -1/+1, +1/+3, +3/+5, >+5
    "move_intensity": [-0.05, -0.03, -0.01, 0.01, 0.03, 0.05],
    # rsi_zone: <30, 30-50, 50-70, >70
    "rsi_zone": [30, 50, 70],
    # vix_level: <15, 15-20, 20-25, 25-30, >30
    "vix_level": [15, 20, 25, 30],
    # vix_move: <-3%, -3/-1, -1/+1, +1/+3, >+3%  (v0.2: was +/-5%, tightened tails)
    "vix_move": [-0.03, -0.01, 0.01, 0.03],
}


def _bucket_index(value: float, edges: list[float]) -> int:
    """Return the bucket index for a value given sorted edges. NaN-safe."""
    if value is None:
        return -1
    try:
        v = float(value)
    except (TypeError, ValueError):
        return -1
    if v != v:  # NaN check
        return -1
    for i, edge in enumerate(edges):
        if v < edge:
            return i
    return len(edges)


def assign_buckets(
    pct_change: float,
    rsi14: float,
    rsi_slope: float,
    vix_close: float,
    vix_pct_change: float,
    spy_above_200: int,
    overrides: dict[str, list[float]] | None = None,
) -> dict[str, int]:
    """Assign all 6 buckets for one observation. Returns ints (-1 if bad input).

    The result is the canonical 'bucket vector' used as the match key.
    """
    edges = dict(DEFAULT_BUCKETS)
    if overrides:
        edges.update(overrides)

    return {
        "move_intensity": _bucket_index(pct_change, edges["move_intensity"]),
        "rsi_zone": _bucket_index(rsi14, edges["rsi_zone"]),
        "rsi_slope": 1 if (rsi_slope is not None and rsi_slope > 0) else 0,
        "vix_level": _bucket_index(vix_close, edges["vix_level"]),
        "vix_move": _bucket_index(vix_pct_change, edges["vix_move"]),
        "market_trend": 1 if spy_above_200 else 0,
    }


def bucket_vector_key(buckets: dict[str, int]) -> tuple:
    """Stable tuple key for a bucket vector. Used for SQL match and grouping."""
    return (
        buckets["move_intensity"],
        buckets["rsi_zone"],
        buckets["rsi_slope"],
        buckets["vix_level"],
        buckets["vix_move"],
        buckets["market_trend"],
    )


def describe_buckets(buckets: dict[str, int], overrides: dict[str, list[float]] | None = None) -> dict[str, str]:
    """Human-readable labels for a bucket vector. For CLI output."""
    edges = dict(DEFAULT_BUCKETS)
    if overrides:
        edges.update(overrides)

    def _label(name: str, idx: int, unit: str = "") -> str:
        if idx < 0:
            return "n/a"
        e = edges[name]
        if idx == 0:
            return f"<{e[0]}{unit}"
        if idx == len(e):
            return f">={e[-1]}{unit}"
        return f"{e[idx-1]}{unit} to {e[idx]}{unit}"

    return {
        "move_intensity": _label("move_intensity", buckets["move_intensity"], ""),
        "rsi_zone": _label("rsi_zone", buckets["rsi_zone"], ""),
        "rsi_slope": "rising" if buckets["rsi_slope"] else "falling/flat",
        "vix_level": _label("vix_level", buckets["vix_level"], ""),
        "vix_move": _label("vix_move", buckets["vix_move"], ""),
        "market_trend": "SPY>200d" if buckets["market_trend"] else "SPY<=200d",
    }
