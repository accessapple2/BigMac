"""HM-WHEEL-ASSIGNMENT-LEDGER 2026-05-18 — canonical writer for wheel CSP
ITM-at-expiry assignments. G20=C decoupling preserved: no ai_players.cash
mutation. Virtual buying power tracked in paper_assignment_liability
(side table per audit C-ii sub-disposition).

Race-tolerant by design: close_options_trade fires first (atomic
status='open' guard). If close returns None (already-closed by a
concurrent caller), assign_csp returns the noop sentinel WITHOUT writing
a ledger row, preserving single-row-per-assignment invariant.

Companion audits:
- reports/hm_wheel_assignment_ledger_audit.md
- reports/hm_wheel_assignment_ledger_blast_radius.md
"""
from __future__ import annotations

import json
import sqlite3
from datetime import date as _date
from typing import Optional

from engine.options_exec import close_options_trade

DB_PATH = "data/trader.db"
VALID_SIDES = ("long_shares", "short_shares")


def assign_csp(
    trade_id: int,
    spot_at_expiry: float,
    assignment_date: Optional[str] = None,
) -> dict:
    """Mark a CSP trade as ITM-assigned.

    Returns a dict with keys: status ('assigned' | 'noop' | 'partial_failure'),
    reason (str, only when status != 'assigned'), assignment_id (int | None),
    pnl (float | None), shares (int), and on success: cost_basis, capital,
    intrinsic.

    Idempotent: re-calling with the same trade_id after a successful
    assignment returns status='noop' reason='trade_not_open'.
    """
    if assignment_date is None:
        assignment_date = _date.today().strftime("%Y-%m-%d")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT id, agent_id, symbol, structure, legs_json, book_tag, "
            "       entry_credit_debit, status, expiration "
            "FROM options_trades WHERE id=?",
            (trade_id,),
        ).fetchone()
    finally:
        conn.close()

    if not row:
        return {
            "status": "noop", "reason": "trade_id_not_found",
            "assignment_id": None, "pnl": None, "shares": 0,
        }
    if row["status"] != "open":
        return {
            "status": "noop", "reason": "trade_not_open",
            "assignment_id": None, "pnl": None, "shares": 0,
        }
    if row["structure"] != "csp":
        return {
            "status": "noop", "reason": "structure_not_csp",
            "assignment_id": None, "pnl": None, "shares": 0,
        }

    try:
        legs = json.loads(row["legs_json"])
    except (json.JSONDecodeError, TypeError):
        return {
            "status": "noop", "reason": "bad_legs_json",
            "assignment_id": None, "pnl": None, "shares": 0,
        }

    short_puts = [l for l in legs if l.get("side") == "short" and l.get("type") == "put"]
    if not short_puts:
        return {
            "status": "noop", "reason": "no_short_put_leg",
            "assignment_id": None, "pnl": None, "shares": 0,
        }
    leg = short_puts[0]
    strike = float(leg.get("strike", 0) or 0)
    qty = int(leg.get("qty", 0) or 0)
    if strike <= 0 or qty <= 0:
        return {
            "status": "noop", "reason": "bad_strike_or_qty",
            "assignment_id": None, "pnl": None, "shares": 0,
        }

    intrinsic = max(0.0, round(strike - spot_at_expiry, 2))
    if intrinsic <= 0:
        return {
            "status": "noop", "reason": "not_itm_at_assignment",
            "assignment_id": None, "pnl": None, "shares": 0,
            "strike": strike, "spot": spot_at_expiry,
        }

    # CLOSE-FIRST race-safety: close_options_trade has an atomic
    # WHERE id=? AND status='open' guard. If a concurrent caller already
    # closed this row, we receive None and bail without writing a ledger
    # row — single-row-per-assignment invariant preserved.
    exit_legs = [{
        "side": "short", "type": "put",
        "strike": strike, "qty": qty,
        "exit_price": intrinsic,
    }]
    pnl = close_options_trade(trade_id, exit_legs, exit_reason="expired_itm_assigned")
    if pnl is None:
        return {
            "status": "noop", "reason": "close_options_trade_returned_none",
            "assignment_id": None, "pnl": None, "shares": 0,
        }

    qty_shares = qty * 100
    cash_secured_capital = round(strike * qty_shares, 2)
    note = (
        f"assigned_at_spot={spot_at_expiry:.2f}, intrinsic={intrinsic:.2f}, "
        f"entry_credit={row['entry_credit_debit']:.2f}, "
        f"realized_pnl={pnl:.2f}"
    )

    conn = sqlite3.connect(DB_PATH)
    try:
        c = conn.execute(
            "INSERT INTO paper_assignment_liability "
            "(book_tag, source_trade_id, agent_id, symbol, side, qty_shares, "
            " strike_price, cash_secured_capital, assignment_date, status, notes) "
            "VALUES (?, ?, ?, ?, 'long_shares', ?, ?, ?, ?, 'open', ?)",
            (
                row["book_tag"], trade_id, row["agent_id"], row["symbol"],
                qty_shares, strike, cash_secured_capital, assignment_date, note,
            ),
        )
        assignment_id = c.lastrowid
        conn.commit()
    except Exception:
        conn.rollback()
        # close_options_trade has already committed. The close stands —
        # better an unrecorded assignment than fighting an already-mutated
        # row. Caller's fallback NTFY will surface this to Captain.
        return {
            "status": "partial_failure", "reason": "ledger_insert_failed",
            "assignment_id": None, "pnl": pnl, "shares": qty_shares,
        }
    finally:
        conn.close()

    return {
        "status": "assigned",
        "assignment_id": assignment_id,
        "pnl": pnl,
        "shares": qty_shares,
        "cost_basis": strike,
        "capital": cash_secured_capital,
        "intrinsic": intrinsic,
    }


def get_open_assignments(
    agent_id: Optional[str] = None,
    symbol: Optional[str] = None,
) -> list[dict]:
    """Return all currently-open assignments. Used by get_wheel_status
    enrichment in engine/wheel_strategy.py. Cheap via idx_pal_status."""
    sql = "SELECT * FROM paper_assignment_liability WHERE status='open'"
    args: list = []
    if agent_id:
        sql += " AND agent_id=?"
        args.append(agent_id)
    if symbol:
        sql += " AND symbol=?"
        args.append(symbol)
    sql += " ORDER BY assignment_date DESC"

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(sql, args).fetchall()]
    finally:
        conn.close()


def close_assignment(
    assignment_id: int,
    close_trade_id: Optional[int],
    close_reason: str,
    closed_date: Optional[str] = None,
) -> bool:
    """Transition an assignment row from open → closed. Used by the
    future HM-WHEEL-COVERED-CALL-CYCLE epic. Atomic status='open' guard.
    Returns True on success, False if the row was not open or missing."""
    if closed_date is None:
        closed_date = _date.today().strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_PATH)
    try:
        c = conn.execute(
            "UPDATE paper_assignment_liability "
            "SET status='closed', closed_date=?, close_trade_id=?, close_reason=? "
            "WHERE id=? AND status='open'",
            (closed_date, close_trade_id, close_reason, assignment_id),
        )
        conn.commit()
        return c.rowcount > 0
    except Exception:
        conn.rollback()
        return False
    finally:
        conn.close()
