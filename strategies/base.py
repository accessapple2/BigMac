"""
Strategy base class. All OllieTrades strategies inherit from this.
Contract: each strategy must implement evaluate(), which is called with
market context and returns zero or more StrategySignal objects.
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime, timezone


@dataclass
class StrategySignal:
    """One actionable signal from a strategy."""
    strategy_id: str           # e.g. "bull_spread_v1"
    ticker: str
    action: str                # "open", "close", "adjust"
    asset_type: str            # "stock", "option", "spread"
    direction: str             # "bull", "bear", "neutral"

    # Sizing and risk
    max_risk_usd: float        # Hard dollar risk cap
    confidence: float = 0.5    # 0-1, for logging/analysis

    # Strategy-specific payload (strikes, DTE, legs, etc.)
    payload: dict = field(default_factory=dict)

    # Exit-rule tag for A/B testing: "textbook" | "scaleout" | "single"
    exit_tag: str = "single"

    # Metadata
    generated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    reasoning: str = ""


@dataclass
class MarketContext:
    """What a strategy sees when asked to evaluate."""
    as_of: datetime
    regime: str                # "BULL", "BEAR", "CAUTIOUS", "CRISIS"
    vix: float
    spy_price: float
    # Tickers the strategy is allowed to consider
    universe: list[str] = field(default_factory=list)
    # Any upstream signals (Scotty flags, fleet consensus, etc.)
    upstream_signals: dict = field(default_factory=dict)


class Strategy(ABC):
    """Base class every strategy inherits."""

    # Subclasses override these:
    strategy_id: str = "UNSET"
    display_name: str = "UNSET"
    enabled_default: bool = False
    description: str = ""

    def __init__(self, enabled: bool | None = None):
        self.enabled = self.enabled_default if enabled is None else enabled

    @abstractmethod
    def evaluate(self, ctx: MarketContext) -> list[StrategySignal]:
        """Inspect market context, return zero or more signals."""
        ...

    def is_enabled(self) -> bool:
        return self.enabled
