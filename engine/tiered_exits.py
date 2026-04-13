"""
Model F — Spread-Specific Tiered Exits
========================================
Based on 180-day backtest: +15pp improvement over all-or-nothing exits.

Strategy:
  Tier 1: Exit 50% at 50% of max profit (theta decay capture)
  Tier 2: Exit 30% at 75% of max profit
  Tier 3: Exit 20% at 90% of max profit OR 21 DTE (whichever comes first)
  Stop:   Exit 100% if current loss >= 2× credit received

Applies to: iron_condor, bear_call_spread, bull_put_spread, bear_put_spread,
            bull_call_spread, covered_call, csp

Does NOT apply to: long_equity, swing_trade, rsi_bounce, long_call, long_put
  (those use the existing _ollie_check_tiered_tp() equity logic)

Usage:
  from engine.tiered_exits import check_spread_exits
  actions = check_spread_exits(open_positions)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants

SPREAD_STRATEGIES = frozenset({
    "iron_condor",
    "bear_call_spread",
    "bull_put_spread",
    "bear_put_spread",
    "bull_call_spread",
    "covered_call",
    "csp",
})

MODEL_F_THRESHOLDS = {
    "tier_1": {"profit_pct": 50,  "qty_fraction": 0.50},  # 50% of pos at 50% max profit
    "tier_2": {"profit_pct": 75,  "qty_fraction": 0.30},  # 30% of pos at 75% max profit
    "tier_3": {"profit_pct": 90,  "qty_fraction": 1.00,   # remaining (20%) at 90% or 21 DTE
               "dte_trigger": 21},
    "stop":   {"loss_multiplier": 2.0},                    # 100% exit at 2× credit loss
}


class ExitTier(Enum):
    TIER_1 = "tier_1"
    TIER_2 = "tier_2"
    TIER_3 = "tier_3"
    STOP   = "stop"


# ---------------------------------------------------------------------------
# Dataclass

@dataclass
class SpreadPosition:
    """Tracks a spread position through Model F tiered exits."""

    symbol: str
    strategy: str
    player_id: str
    entry_credit: float        # Premium received at entry (positive number)
    total_qty: int             # Original quantity
    remaining_qty: int         # Quantity still open
    tier_1_exited: bool = False
    tier_2_exited: bool = False
    trade_id: Optional[int] = None
    entry_date: str = ""

    def profit_captured_pct(self, current_value: float) -> float:
        """
        What % of max profit has been captured?
        For credit spreads: profit = credit received - current value.
        current_value should be the current cost-to-close the position.
        Returns 0-100+.
        """
        if self.entry_credit <= 0:
            return 0.0
        profit = self.entry_credit - current_value
        return (profit / self.entry_credit) * 100.0

    def current_loss(self, current_value: float) -> float:
        """Net loss if current_value > entry_credit (spread widened)."""
        return max(0.0, current_value - self.entry_credit)


# ---------------------------------------------------------------------------
# Core logic

def calculate_exit_action(
    position: SpreadPosition,
    current_value: float,
    dte: Optional[int] = None,
) -> Optional[dict]:
    """
    Evaluate whether to exit any portion of this spread position.

    Args:
        position:      SpreadPosition dataclass with state
        current_value: Current cost-to-close the position (mark price)
        dte:           Days to expiration (None if unknown)

    Returns:
        dict with {action, qty, tier, reason} or None if no action needed.
    """
    if position.strategy not in SPREAD_STRATEGIES:
        return None
    if position.remaining_qty <= 0:
        return None

    profit_pct = position.profit_captured_pct(current_value)
    loss = position.current_loss(current_value)

    # ── STOP: 2× credit received ──────────────────────────────────────────
    stop_threshold = position.entry_credit * MODEL_F_THRESHOLDS["stop"]["loss_multiplier"]
    if loss >= stop_threshold:
        return {
            "action": "exit",
            "qty": position.remaining_qty,
            "tier": ExitTier.STOP,
            "reason": (
                f"Model F stop: loss ${loss:.2f} >= 2× credit ${position.entry_credit:.2f}"
            ),
        }

    # ── TIER 1: 50% at 50% profit ─────────────────────────────────────────
    if not position.tier_1_exited and profit_pct >= MODEL_F_THRESHOLDS["tier_1"]["profit_pct"]:
        exit_qty = max(1, int(position.total_qty * MODEL_F_THRESHOLDS["tier_1"]["qty_fraction"]))
        exit_qty = min(exit_qty, position.remaining_qty)
        return {
            "action": "exit",
            "qty": exit_qty,
            "tier": ExitTier.TIER_1,
            "reason": f"Model F Tier 1: {profit_pct:.0f}% profit captured, exiting 50%",
        }

    # ── TIER 2: 30% at 75% profit ─────────────────────────────────────────
    if (
        position.tier_1_exited
        and not position.tier_2_exited
        and profit_pct >= MODEL_F_THRESHOLDS["tier_2"]["profit_pct"]
    ):
        exit_qty = max(1, int(position.total_qty * MODEL_F_THRESHOLDS["tier_2"]["qty_fraction"]))
        exit_qty = min(exit_qty, position.remaining_qty)
        return {
            "action": "exit",
            "qty": exit_qty,
            "tier": ExitTier.TIER_2,
            "reason": f"Model F Tier 2: {profit_pct:.0f}% profit captured, exiting 30%",
        }

    # ── TIER 3: remaining 20% at 90% OR 21 DTE ───────────────────────────
    if position.tier_1_exited and position.tier_2_exited:
        t3 = MODEL_F_THRESHOLDS["tier_3"]
        trigger_profit = profit_pct >= t3["profit_pct"]
        trigger_dte    = dte is not None and dte <= t3["dte_trigger"]
        if trigger_profit or trigger_dte:
            reason = (
                f"Model F Tier 3: {profit_pct:.0f}% profit captured"
                if trigger_profit
                else f"Model F Tier 3: {dte} DTE ≤ 21, time exit"
            )
            return {
                "action": "exit",
                "qty": position.remaining_qty,
                "tier": ExitTier.TIER_3,
                "reason": reason,
            }

    return None  # No action needed


# ---------------------------------------------------------------------------
# Batch helper

def check_spread_exits(open_positions: list[dict]) -> list[dict]:
    """
    Check a list of open position dicts for Model F exit triggers.

    Expected dict keys:
        symbol, strategy, player_id,
        entry_price (credit received),
        qty (remaining), original_qty,
        current_value (mark price, cost-to-close),
        tier_1_exited (bool, default False),
        tier_2_exited (bool, default False),
        dte (int, optional),
        trade_id (int, optional)

    Returns list of exit action dicts.
    """
    exit_orders: list[dict] = []

    for pos in open_positions:
        strat = pos.get("strategy", "")
        if strat not in SPREAD_STRATEGIES:
            continue

        sp = SpreadPosition(
            symbol        = pos.get("symbol", "?"),
            strategy      = strat,
            player_id     = pos.get("player_id", ""),
            entry_credit  = float(pos.get("entry_price") or 0),
            total_qty     = int(pos.get("original_qty") or pos.get("qty") or 1),
            remaining_qty = int(pos.get("qty") or 1),
            tier_1_exited = bool(pos.get("tier_1_exited", False)),
            tier_2_exited = bool(pos.get("tier_2_exited", False)),
            trade_id      = pos.get("trade_id"),
            entry_date    = pos.get("entry_date", ""),
        )

        current_value = float(pos.get("current_value") or sp.entry_credit)
        dte = pos.get("dte")

        action = calculate_exit_action(sp, current_value, dte)
        if action:
            _log_tiered_exit(sp, action)
            exit_orders.append({
                "symbol":    sp.symbol,
                "player_id": sp.player_id,
                "trade_id":  sp.trade_id,
                "qty":       action["qty"],
                "tier":      action["tier"].value,
                "reason":    action["reason"],
            })

    return exit_orders


def _log_tiered_exit(position: SpreadPosition, action: dict) -> None:
    emoji = {"tier_1": "📊", "tier_2": "📈", "tier_3": "🏁", "stop": "🛑"}.get(
        action["tier"].value, "📌"
    )
    logger.info(
        f"{emoji} TIERED EXIT [{action['tier'].value.upper()}] "
        f"{position.symbol} ({position.strategy}) | "
        f"qty={action['qty']} | {action['reason']}"
    )
