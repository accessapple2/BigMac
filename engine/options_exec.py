"""
Paper execution helpers for options trades.

Production and ghost agents will call these in later sprints.
For now the helpers exist — no agent is wired to call them yet.

ALL writes are to data/trader.db only. NO broker API is called.
NO real money is touched under any circumstances.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional

DB_PATH = "data/trader.db"

VALID_STRUCTURES = frozenset({
    "iron_condor", "bull_put_spread", "bear_call_spread",
    "bull_call_spread", "bear_put_spread",
    "long_call", "long_put", "covered_call", "csp",
})


def open_options_trade(
    book_tag: str,
    agent_id: str,
    structure: str,
    symbol: str,
    expiration: str,
    legs: List[Dict],
    regime: Optional[str] = None,
    vix: Optional[float] = None,
    notes: Optional[str] = None,
) -> Optional[int]:
    """
    Record a new paper options trade entry.
    Returns trade id on success, None on failure.
    NEVER touches real broker API.

    legs format:
      [{"side": "short"|"long", "type": "put"|"call",
        "strike": float, "qty": int, "entry_price": float}, ...]
    """
    assert book_tag in ("fleet", "ghost"), f"bad book_tag: {book_tag}"
    assert structure in VALID_STRUCTURES, f"unknown structure: {structure}"

    # Net credit (+) or debit (-) at entry
    net = 0.0
    for leg in legs:
        mult = 1.0 if leg["side"] == "short" else -1.0
        net += mult * float(leg["entry_price"]) * int(leg["qty"]) * 100

    # Max profit / max loss by structure
    max_p: Optional[float] = None
    max_l: Optional[float] = None
    if structure == "iron_condor":
        max_p = net
        puts  = sorted([l for l in legs if l["type"] == "put"],  key=lambda x: x["strike"])
        calls = sorted([l for l in legs if l["type"] == "call"], key=lambda x: x["strike"])
        put_w  = abs(puts[1]["strike"]  - puts[0]["strike"])  if len(puts)  >= 2 else 0
        call_w = abs(calls[1]["strike"] - calls[0]["strike"]) if len(calls) >= 2 else 0
        max_l = -(max(put_w, call_w) * 100 - net)
    elif structure in ("bull_put_spread", "bear_call_spread"):
        max_p = net
        width = abs(legs[0]["strike"] - legs[1]["strike"]) if len(legs) >= 2 else 0
        max_l = -(width * 100 - net)
    elif structure in ("bull_call_spread", "bear_put_spread"):
        max_l = net  # debit paid (negative)
        width = abs(legs[0]["strike"] - legs[1]["strike"]) if len(legs) >= 2 else 0
        max_p = width * 100 + net  # width minus debit
    elif structure in ("long_call", "long_put"):
        max_l = net       # debit (negative)
        max_p = None      # theoretically unlimited
    elif structure in ("covered_call", "csp"):
        max_p = net
        max_l = None      # underlying-dependent, left to caller

    # DTE at entry
    dte: Optional[int] = None
    try:
        exp_dt = datetime.fromisoformat(expiration)
        dte = (exp_dt.date() - datetime.now().date()).days
    except Exception:
        pass

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute(
            """
            INSERT INTO options_trades
              (book_tag, agent_id, structure, symbol, entry_date,
               expiration, dte_at_entry, legs_json, entry_credit_debit,
               max_profit, max_loss, regime_at_entry, vix_at_entry, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                book_tag, agent_id, structure, symbol,
                datetime.utcnow().isoformat(timespec="seconds"),
                expiration, dte, json.dumps(legs),
                net, max_p, max_l, regime, vix, notes,
            ),
        )
        trade_id = c.lastrowid
        # Credit trades increase cash; debit trades decrease cash
        c.execute(
            "UPDATE options_books SET current_cash = current_cash + ?, total_trades = total_trades + 1 WHERE book_tag = ?",
            (net, book_tag),
        )
        conn.commit()
        return trade_id
    except Exception:
        conn.rollback()
        return None
    finally:
        conn.close()


def close_options_trade(
    trade_id: int,
    exit_legs: List[Dict],
    exit_reason: str = "manual",
) -> Optional[float]:
    """
    Close a paper options trade and compute final P&L.
    Returns realized P&L on success, None on failure.

    exit_legs format: same as legs but with 'exit_price' instead of 'entry_price'.
    Convention: close_cost = sum of prices paid to buy back all legs.
    P&L = entry_credit_debit - close_cost
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        c.execute("SELECT * FROM options_trades WHERE id = ? AND status = 'open'", (trade_id,))
        row = c.fetchone()
        if not row:
            return None

        close_cost = sum(
            float(leg["exit_price"]) * int(leg["qty"]) * 100
            for leg in exit_legs
        )
        pnl = row["entry_credit_debit"] - close_cost

        max_l = row["max_loss"]
        pnl_pct = (pnl / abs(max_l) * 100) if max_l else None

        c.execute(
            """
            UPDATE options_trades
            SET status = 'closed',
                exit_date = ?,
                exit_credit_debit = ?,
                pnl = ?,
                pnl_pct = ?,
                exit_reason = ?
            WHERE id = ?
            """,
            (
                datetime.utcnow().isoformat(timespec="seconds"),
                -close_cost,
                pnl,
                pnl_pct,
                exit_reason,
                trade_id,
            ),
        )
        c.execute(
            """
            UPDATE options_books
            SET current_cash = current_cash - ?,
                wins   = wins   + CASE WHEN ? > 0 THEN 1 ELSE 0 END,
                losses = losses + CASE WHEN ? <= 0 THEN 1 ELSE 0 END
            WHERE book_tag = ?
            """,
            (close_cost, pnl, pnl, row["book_tag"]),
        )
        conn.commit()
        return pnl
    except Exception:
        conn.rollback()
        return None
    finally:
        conn.close()
