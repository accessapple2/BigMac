"""
Execution adapter: routes StrategySignal -> alpaca_options paper orders.

HARD SAFETY GATE: _EXECUTION_ENABLED is a module-level constant hardcoded
to False. Task 7b ships close_position() stub — gate stays False.
NOT an env var. NOT runtime config. Must be edited in source to enable.
"""
from __future__ import annotations
import json
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

from .base import StrategySignal


# ═══════════════════════════════════════════════════════════════════════
# HARD SAFETY GATE — DO NOT CHANGE WITHOUT TASK 7 SIGN-OFF
# ═══════════════════════════════════════════════════════════════════════
_EXECUTION_ENABLED: bool = False
# ═══════════════════════════════════════════════════════════════════════


DB_PATH = Path(__file__).parent.parent / "data" / "trader.db"


@dataclass
class ExecutionResult:
    signal_id: Optional[int]
    ticker: str
    exit_tag: str
    status: str  # "emit_only" | "executed" | "rejected" | "error"
    broker_order_id: Optional[str] = None
    options_trade_id: Optional[int] = None
    reason: Optional[str] = None
    executed_at: Optional[datetime] = None


def execute_signal(signal: StrategySignal, signal_id: Optional[int] = None) -> ExecutionResult:
    """
    Execute a StrategySignal. In Task 6, always returns 'emit_only' due to
    the hard gate. Task 7 will flip _EXECUTION_ENABLED.
    """
    if not _EXECUTION_ENABLED:
        return ExecutionResult(
            signal_id=signal_id,
            ticker=signal.ticker,
            exit_tag=signal.exit_tag,
            status="emit_only",
            reason="_EXECUTION_ENABLED is False — gated until Task 7 complete",
        )
    return _execute_live(signal, signal_id)


def _execute_live(signal: StrategySignal, signal_id: Optional[int]) -> ExecutionResult:
    """Live path. Only reachable when _EXECUTION_ENABLED is True."""
    try:
        from engine.alpaca_options import submit_vertical_spread
    except ImportError as e:
        return ExecutionResult(
            signal_id=signal_id, ticker=signal.ticker, exit_tag=signal.exit_tag,
            status="error",
            reason=f"alpaca_options import failed: {e}"
        )

    payload = signal.payload
    structure = payload.get("structure")
    if structure not in ("bull_call_spread", "bull_put_spread"):
        return ExecutionResult(
            signal_id=signal_id, ticker=signal.ticker, exit_tag=signal.exit_tag,
            status="rejected",
            reason=f"Unknown structure: {structure}"
        )

    contracts = payload.get("contracts", 1)
    long_leg = payload["long_leg"]
    short_leg = payload["short_leg"]

    # Real alpaca signature: submit_vertical_spread(
    #   player_id, buy_symbol, sell_symbol, qty, strategy
    # )
    player_id = f"strategy:{signal.strategy_id}"
    buy_symbol = _occ_symbol(signal.ticker, long_leg)
    sell_symbol = _occ_symbol(signal.ticker, short_leg)

    try:
        result = submit_vertical_spread(
            player_id=player_id,
            buy_symbol=buy_symbol,
            sell_symbol=sell_symbol,
            qty=contracts,
            strategy=structure,
        )
    except Exception as e:
        return ExecutionResult(
            signal_id=signal_id, ticker=signal.ticker, exit_tag=signal.exit_tag,
            status="error",
            reason=f"alpaca submit raised: {type(e).__name__}: {e}"
        )

    # submit_vertical_spread returns dict: {"order_id": ..., ...} or {"skipped": True, ...}
    if isinstance(result, dict):
        if result.get("skipped"):
            return ExecutionResult(
                signal_id=signal_id, ticker=signal.ticker, exit_tag=signal.exit_tag,
                status="rejected",
                reason=f"alpaca skipped: {result}"
            )
        broker_order_id = result.get("order_id") or result.get("id")
    else:
        broker_order_id = getattr(result, "id", None)
        broker_order_id = str(broker_order_id) if broker_order_id else None

    trade_id = _record_options_trade(signal, broker_order_id, signal_id)

    return ExecutionResult(
        signal_id=signal_id, ticker=signal.ticker, exit_tag=signal.exit_tag,
        status="executed",
        broker_order_id=broker_order_id,
        options_trade_id=trade_id,
        executed_at=datetime.now(timezone.utc),
    )


def _occ_symbol(underlying: str, leg: dict) -> str:
    """Build OCC option symbol: e.g. SPY250425C00700000"""
    exp = date.fromisoformat(leg["expiration"])
    yy = exp.strftime("%y")
    mm = exp.strftime("%m")
    dd = exp.strftime("%d")
    cp = "C" if leg["option_type"] == "call" else "P"
    strike_int = int(round(leg["strike"] * 1000))
    return f"{underlying}{yy}{mm}{dd}{cp}{strike_int:08d}"


@dataclass
class CloseResult:
    position_id: int
    contracts_closed: int
    reason: str
    status: str  # "logged" | "executed" | "error"
    broker_order_id: Optional[str] = None
    closed_at: Optional[datetime] = None


def close_position(intent) -> CloseResult:
    """
    Close (partially or fully) an open options_trades position.

    Task 7b: gate is still False — logs the intent and increments
    contracts_closed_so_far in the DB. Does NOT call alpaca.

    When _EXECUTION_ENABLED flips True (Admiral sign-off), the live
    path will fire alpaca close orders and then update the DB.
    """
    if not _EXECUTION_ENABLED:
        # Log-only: record partial close in DB so scaleout ladder advances
        _increment_closed(intent.position_id, intent.contracts_to_close, intent.reason)
        return CloseResult(
            position_id=intent.position_id,
            contracts_closed=intent.contracts_to_close,
            reason=intent.reason,
            status="logged",
            closed_at=datetime.now(timezone.utc),
        )
    return _close_live(intent)


def _close_live(intent) -> CloseResult:
    """
    Actual close execution. Only reachable when _EXECUTION_ENABLED is True.

    Closes each leg of the spread individually via alpaca_options:
      - Entry "buy"  leg → sell to close  (close_options_position, side="sell")
      - Entry "sell" leg → buy to close   (submit_single_option,   side="buy")
    Then increments contracts_closed_so_far in DB.
    """
    try:
        from engine.alpaca_options import close_options_position, submit_single_option
    except ImportError as e:
        return CloseResult(
            position_id=intent.position_id, contracts_closed=0,
            reason=intent.reason, status="error",
            closed_at=datetime.now(timezone.utc),
        )

    # Load position details from options_trades
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cur = conn.execute(
            "SELECT symbol, legs_json, strategy_id FROM options_trades WHERE id = ?",
            (intent.position_id,),
        )
        row = cur.fetchone()
        conn.close()
    except Exception as e:
        return CloseResult(
            position_id=intent.position_id, contracts_closed=0,
            reason=intent.reason, status="error",
            closed_at=datetime.now(timezone.utc),
        )

    if not row:
        return CloseResult(
            position_id=intent.position_id, contracts_closed=0,
            reason=f"position {intent.position_id} not found",
            status="error", closed_at=datetime.now(timezone.utc),
        )

    symbol, legs_json_str, strategy_id = row
    player_id = f"strategy:{strategy_id}"

    try:
        legs = json.loads(legs_json_str)
    except Exception as e:
        return CloseResult(
            position_id=intent.position_id, contracts_closed=0,
            reason=f"legs_json parse failed: {e}",
            status="error", closed_at=datetime.now(timezone.utc),
        )

    # Close each leg with the correct buy/sell direction
    close_results = []
    for leg in legs:
        occ = _occ_symbol(symbol, leg)
        entry_action = leg.get("action", "buy")
        try:
            if entry_action == "buy":
                # Long leg: sell to close
                r = close_options_position(
                    player_id=player_id,
                    contract_symbol=occ,
                    qty=intent.contracts_to_close,
                )
            else:
                # Short leg: buy to close
                r = submit_single_option(
                    player_id=player_id,
                    contract_symbol=occ,
                    qty=intent.contracts_to_close,
                    side="buy",
                )
            close_results.append({"leg": occ, "result": r})
        except Exception as e:
            close_results.append({"leg": occ, "error": f"{type(e).__name__}: {e}"})

    errors = [r for r in close_results if "error" in r]
    if errors:
        return CloseResult(
            position_id=intent.position_id, contracts_closed=0,
            reason=f"leg close failed: {errors}",
            status="error", closed_at=datetime.now(timezone.utc),
        )

    _increment_closed(intent.position_id, intent.contracts_to_close, intent.reason)

    broker_ref = str([r.get("result", {}).get("order_id") for r in close_results])
    return CloseResult(
        position_id=intent.position_id,
        contracts_closed=intent.contracts_to_close,
        reason=intent.reason,
        status="executed",
        broker_order_id=broker_ref,
        closed_at=datetime.now(timezone.utc),
    )


def _increment_closed(position_id: int, count: int, reason: str,
                      total: int = 0) -> None:
    """Increment contracts_closed_so_far and update exec_status if fully closed.

    exit_date and exit_reason are only written when the position transitions
    to 'closed' (contracts_closed_so_far reaches contracts). Partial closes
    leave both fields untouched.

    The `total` parameter is accepted for call-site clarity but is unused —
    the SQL compares against the `contracts` column directly, so a single
    UPDATE handles both partial and full closes atomically.
    """
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cur = conn.execute(
            """
            UPDATE options_trades
               SET contracts_closed_so_far =
                       MIN(contracts, contracts_closed_so_far + ?),
                   exec_status =
                       CASE WHEN contracts_closed_so_far + ? >= contracts
                            THEN 'closed' ELSE 'open' END,
                   exit_date =
                       CASE WHEN contracts_closed_so_far + ? >= contracts
                            THEN CURRENT_TIMESTAMP ELSE exit_date END,
                   exit_reason =
                       CASE WHEN contracts_closed_so_far + ? >= contracts
                            THEN COALESCE(exit_reason || ' | ', '') || ?
                            ELSE exit_reason END
             WHERE id = ?
            """,
            (count, count, count, count, reason, position_id),
        )
        conn.commit()
        if cur.rowcount == 0:
            print(f"[executor] _increment_closed: position {position_id} not found")
            return
        row = conn.execute(
            "SELECT contracts_closed_so_far, contracts, exec_status "
            "FROM options_trades WHERE id = ?", (position_id,)
        ).fetchone()
        if row:
            new_closed, total_ct, new_status = row
            print(f"[executor] position #{position_id}: closed {count} contracts "
                  f"({new_closed}/{total_ct}) reason={reason} status={new_status}")
    except Exception as e:
        print(f"[executor] _increment_closed failed: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _record_options_trade(signal: StrategySignal, order_id: Optional[str],
                          signal_id: Optional[int]) -> Optional[int]:
    """Record open spread into options_trades. Matches real legacy schema."""
    import json as _json
    try:
        conn = sqlite3.connect(str(DB_PATH))
        payload = signal.payload
        structure = payload.get("structure", "unknown")

        # Build legs_json — schema stores both legs here
        legs_json = _json.dumps([
            {
                "action": payload["long_leg"]["action"],
                "option_type": payload["long_leg"]["option_type"],
                "strike": payload["long_leg"]["strike"],
                "expiration": payload["long_leg"]["expiration"],
                "premium": payload["long_leg"]["premium"],
            },
            {
                "action": payload["short_leg"]["action"],
                "option_type": payload["short_leg"]["option_type"],
                "strike": payload["short_leg"]["strike"],
                "expiration": payload["short_leg"]["expiration"],
                "premium": payload["short_leg"]["premium"],
            },
        ])

        # entry_credit_debit: positive = net credit received, negative = net debit paid
        net_debit = payload.get("net_debit", 0) or 0
        net_credit = payload.get("net_credit", 0) or 0
        entry_credit_debit = net_credit - net_debit

        # agent_id prefix lets us filter strategy vs legacy agent trades
        agent_id = f"strategy:{signal.strategy_id}"

        # Canonical expiration: long leg's expiration date
        expiration = payload["long_leg"]["expiration"]

        cur = conn.execute("""
            INSERT INTO options_trades
                (agent_id, symbol, structure, expiration, legs_json,
                 entry_credit_debit, entry_date,
                 strategy_id, exit_tag, broker_order_id, signal_id, exec_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open')
        """, (
            agent_id, signal.ticker, structure, expiration, legs_json,
            entry_credit_debit, datetime.now(timezone.utc).isoformat(),
            signal.strategy_id, signal.exit_tag, order_id, signal_id,
        ))
        conn.commit()
        return cur.lastrowid
    except Exception as e:
        print(f"[executor] record failed: {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass
