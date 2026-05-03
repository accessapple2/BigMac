"""base_rates: conditional base-rate research utility for OllieTrades.

Research/interrogation only. Not wired into the trade loop.
"""
from .query import base_rate, BaseRateResult
from .buckets import assign_buckets, describe_buckets, DEFAULT_BUCKETS
from .features import rsi_wilder, forward_return, forward_max_drawdown, compute_features
from .migrate import migrate

__version__ = "0.2.0"
__all__ = [
    "base_rate",
    "BaseRateResult",
    "assign_buckets",
    "describe_buckets",
    "DEFAULT_BUCKETS",
    "rsi_wilder",
    "forward_return",
    "forward_max_drawdown",
    "compute_features",
    "migrate",
]
