"""
Exit Manager for OllieTrades strategies.

Scans open options_trades rows owned by registry strategies, evaluates
exit rules per exit_tag, and (when execution is enabled) closes positions
via alpaca_options.

Exit rules by tag:
  bullspread-textbook:
    - Close ALL contracts at 50% of max profit
    - Close ALL contracts at 50% of max loss
    - Hard close ALL at 1 DTE

  bullspread-scaleout:
    - Close 50% of contracts at 50% of max profit
    - Close 25% more of contracts at 75% of max profit
    - Runner remaining contracts held to 1 DTE
    - Full-position stop-loss at 50% of max loss

IMPORTANT: exit_manager never places orders directly in Task 7a. It
evaluates, builds CloseIntent objects, and either logs them (gate CLOSED)
or passes them to the executor (gate OPEN — not yet).
"""
from __future__ import annotations
import json
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

from .executor import _EXECUTION_ENABLED, close_position


DB_PATH = Path(__file__).parent.parent / "data" / "trader.db"


@dataclass
class OpenPosition:
    """Snapshot of an open row from options_trades."""
    id: int
    agent_id: str
    symbol: str
    structure: str
    contracts: int
    contracts_closed_so_far: int
    expiration: str
    legs_json: str
    entry_credit_debit: float
    entry_date: str
    strategy_id: Optional[str]
    exit_tag: Optional[str]

    @property
    def contracts_remaining(self) -> int:
        return max(0, self.contracts - self.contracts_closed_so_far)

    @property
    def days_to_expiration(self) -> int:
        exp = date.fromisoformat(self.expiration)
        return (exp - date.today()).days

    @property
    def is_credit(self) -> bool:
        return self.entry_credit_debit > 0

    @property
    def is_debit(self) -> bool:
        return self.entry_credit_debit < 0


@dataclass
class MarkToMarket:
    """What the position is worth right now."""
    position_id: int
    current_value: float        # Positive = we could close for this
    unrealized_pnl: float       # + = winning, - = losing
    pct_of_max_profit: float    # 0.0 - 1.0
    pct_of_max_loss: float      # 0.0 - 1.0
    source: str                  # "mock" | "polygon" | "unavailable"


@dataclass
class CloseIntent:
    """A decision that a position should be closed (fully or partially)."""
    position_id: int
    contracts_to_close: int
    reason: str  # "TP_50", "TP_75", "SL_50", "DTE_1", "RUNNER_CLOSE"
    exit_rule_tag: str  # "bullspread-textbook" | "bullspread-scaleout"
    mark_to_market: MarkToMarket


def fetch_open_strategy_positions(strategy_id: str = "bull_spread_v1") -> list[OpenPosition]:
    """Get all open options_trades rows for a strategy."""
    conn = sqlite3.connect(str(DB_PATH))
    try:
        # Try schema-flexible COALESCE first; falls back if column names differ
        try:
            cur = conn.execute("""
                SELECT id, agent_id, symbol, structure,
                       COALESCE(contracts, 1) AS contracts,
                       COALESCE(contracts_closed_so_far, 0) AS contracts_closed_so_far,
                       expiration, legs_json,
                       entry_credit_debit, entry_date,
                       strategy_id, exit_tag
                FROM options_trades
                WHERE strategy_id = ?
                  AND exec_status = 'open'
            """, (strategy_id,))
        except sqlite3.OperationalError as e:
            print(f"[exit_manager] schema error: {e}")
            cur = conn.execute("""
                SELECT id, agent_id, symbol, structure,
                       COALESCE(contracts, 1),
                       COALESCE(contracts_closed_so_far, 0),
                       expiration, legs_json,
                       entry_credit_debit, entry_date,
                       strategy_id, exit_tag
                FROM options_trades
                WHERE strategy_id = ?
                  AND exec_status = 'open'
            """, (strategy_id,))
        rows = cur.fetchall()
    finally:
        conn.close()

    positions = []
    for r in rows:
        positions.append(OpenPosition(
            id=r[0], agent_id=r[1], symbol=r[2],
            structure=r[3], contracts=r[4], contracts_closed_so_far=r[5],
            expiration=r[6], legs_json=r[7],
            entry_credit_debit=r[8], entry_date=r[9],
            strategy_id=r[10], exit_tag=r[11],
        ))
    return positions


def mark_to_market(pos: OpenPosition) -> MarkToMarket:
    """
    Estimate current value of position.
    In Task 7a: mock-only estimation based on synthetic time decay.
    In Task 7b+: real-time chain lookup.
    """
    from .mock_data import is_mock_mode

    # Reconstruct max_profit / max_loss from entry legs
    legs = json.loads(pos.legs_json)
    long_leg = next((l for l in legs if l["action"] == "buy"), None)
    short_leg = next((l for l in legs if l["action"] == "sell"), None)
    if not (long_leg and short_leg):
        return MarkToMarket(
            position_id=pos.id, current_value=0.0, unrealized_pnl=0.0,
            pct_of_max_profit=0.0, pct_of_max_loss=0.0,
            source="unavailable",
        )
    width = abs(short_leg["strike"] - long_leg["strike"])

    if pos.is_debit:
        entry_cost_per = abs(pos.entry_credit_debit)
        max_profit_per = width - entry_cost_per
        max_loss_per = entry_cost_per
    else:
        entry_credit_per = pos.entry_credit_debit
        max_profit_per = entry_credit_per
        max_loss_per = width - entry_credit_per

    if is_mock_mode():
        # Synthetic progression: hash (pos.id + today) -> stable per-position per-day value
        import hashlib
        seed = hashlib.md5(f"{pos.id}-{date.today()}".encode()).hexdigest()
        pct = (int(seed[:8], 16) % 1000) / 1000.0  # 0.0 - 1.0
        if pct >= 0.5:
            win_pct = (pct - 0.5) * 2.0
            unrealized_per = win_pct * max_profit_per
        else:
            lose_pct = (0.5 - pct) * 2.0
            unrealized_per = -lose_pct * max_loss_per

        unrealized = unrealized_per * pos.contracts * 100.0
        pct_of_mp = max(0.0, unrealized_per / max_profit_per) if max_profit_per > 0 else 0.0
        pct_of_ml = max(0.0, -unrealized_per / max_loss_per) if max_loss_per > 0 else 0.0

        return MarkToMarket(
            position_id=pos.id,
            current_value=unrealized,
            unrealized_pnl=unrealized,
            pct_of_max_profit=min(1.0, pct_of_mp),
            pct_of_max_loss=min(1.0, pct_of_ml),
            source="mock",
        )

    # TODO(Task 7b): real-time polygon chain lookup
    return MarkToMarket(
        position_id=pos.id, current_value=0.0, unrealized_pnl=0.0,
        pct_of_max_profit=0.0, pct_of_max_loss=0.0,
        source="unavailable",
    )


def evaluate_exits(positions: list[OpenPosition]) -> list[CloseIntent]:
    """Apply exit rules to each open position, return CloseIntent list."""
    intents: list[CloseIntent] = []
    for pos in positions:
        mtm = mark_to_market(pos)
        dte = pos.days_to_expiration

        if pos.exit_tag == "bullspread-textbook":
            if mtm.pct_of_max_profit >= 0.50:
                intents.append(CloseIntent(
                    position_id=pos.id, contracts_to_close=pos.contracts,
                    reason="TP_50", exit_rule_tag=pos.exit_tag, mark_to_market=mtm,
                ))
                continue
            if mtm.pct_of_max_loss >= 0.50:
                intents.append(CloseIntent(
                    position_id=pos.id, contracts_to_close=pos.contracts,
                    reason="SL_50", exit_rule_tag=pos.exit_tag, mark_to_market=mtm,
                ))
                continue
            if dte <= 1:
                intents.append(CloseIntent(
                    position_id=pos.id, contracts_to_close=pos.contracts,
                    reason="DTE_1", exit_rule_tag=pos.exit_tag, mark_to_market=mtm,
                ))
                continue

        elif pos.exit_tag == "bullspread-scaleout":
            # Full stop-loss takes priority regardless of partial state
            if mtm.pct_of_max_loss >= 0.50:
                intents.append(CloseIntent(
                    position_id=pos.id,
                    contracts_to_close=pos.contracts_remaining,
                    reason="SL_50_FULL", exit_rule_tag=pos.exit_tag, mark_to_market=mtm,
                ))
                continue

            # Scaleout ladder — infer current stage from contracts_closed_so_far
            # Stage 0 (0 closed):   fire TP_50 → close 50% (2 of 4)
            # Stage 1 (2 closed):   fire TP_75 → close 25% more (1 of 4)
            # Stage 2 (3 closed):   runner — hold until 1 DTE
            closed = pos.contracts_closed_so_far
            total = pos.contracts  # 4 for scaleout

            if closed == 0 and mtm.pct_of_max_profit >= 0.50:
                # First scale: close 50%
                to_close = max(1, total // 2)
                intents.append(CloseIntent(
                    position_id=pos.id, contracts_to_close=to_close,
                    reason="TP_50_SCALEOUT",
                    exit_rule_tag=pos.exit_tag, mark_to_market=mtm,
                ))
                continue

            if closed == total // 2 and mtm.pct_of_max_profit >= 0.75:
                # Second scale: close next 25%
                to_close = max(1, total // 4)
                intents.append(CloseIntent(
                    position_id=pos.id, contracts_to_close=to_close,
                    reason="TP_75_SCALEOUT",
                    exit_rule_tag=pos.exit_tag, mark_to_market=mtm,
                ))
                continue

            # Runner: remaining contracts held to 1 DTE
            if pos.contracts_remaining > 0 and dte <= 1:
                intents.append(CloseIntent(
                    position_id=pos.id,
                    contracts_to_close=pos.contracts_remaining,
                    reason="DTE_1_RUNNER",
                    exit_rule_tag=pos.exit_tag, mark_to_market=mtm,
                ))
                continue

    return intents


def process_intents(intents: list[CloseIntent]) -> dict:
    """
    Task 7b: calls close_position() for every intent.
    Gate is still False — close_position() logs + increments DB, no alpaca calls.
    Returns summary dict for the smoke test.
    """
    summary = {
        "total": len(intents),
        "executed": 0,
        "logged": 0,
        "errors": 0,
        "by_reason": {},
    }
    for intent in intents:
        summary["by_reason"][intent.reason] = \
            summary["by_reason"].get(intent.reason, 0) + 1

        result = close_position(intent)

        print(f"[exit_manager] {intent.exit_rule_tag} pos#{intent.position_id} "
              f"{intent.reason} close={intent.contracts_to_close} "
              f"pnl=${intent.mark_to_market.unrealized_pnl:.2f} "
              f"-> {result.status} [gate_open={_EXECUTION_ENABLED}]")

        if result.status == "executed":
            summary["executed"] += 1
        elif result.status == "logged":
            summary["logged"] += 1
        else:
            summary["errors"] += 1

    return summary


def run_cycle(strategy_id: str = "bull_spread_v1") -> dict:
    """One full exit-evaluation cycle. Returns summary."""
    positions = fetch_open_strategy_positions(strategy_id)
    print(f"[exit_manager] {len(positions)} open positions for {strategy_id}")
    intents = evaluate_exits(positions)
    print(f"[exit_manager] {len(intents)} close intents generated")
    return process_intents(intents)
