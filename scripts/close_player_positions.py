#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.market_data import get_stock_price
from engine.paper_trader import DB, _conn, _current_season, record_portfolio_snapshot


def _fetch_open_positions(player_id: str) -> list[sqlite3.Row]:
    conn = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT id, symbol, qty, avg_price, asset_type, option_type, strike_price, expiry_date, opened_at
        FROM positions
        WHERE player_id=?
        ORDER BY opened_at, id
        """,
        (player_id,),
    ).fetchall()
    conn.close()
    return rows


def _resolve_exit_price(symbol: str, avg_price: float) -> tuple[float, str]:
    quote = get_stock_price(symbol) or {}
    price = float(quote.get("price") or 0)
    if price > 0:
        return round(price, 2), quote.get("source", "market_data")
    return round(float(avg_price), 2), "avg_price_fallback"


def close_all_positions(player_id: str) -> dict:
    open_rows = _fetch_open_positions(player_id)
    if not open_rows:
        portfolio = _conn().execute(
            "SELECT cash FROM ai_players WHERE id=?",
            (player_id,),
        ).fetchone()
        cash = round(float(portfolio[0]), 2) if portfolio else 0.0
        return {
            "player_id": player_id,
            "positions_closed": 0,
            "total_realized_pnl": 0.0,
            "final_cash": cash,
            "final_equity": cash,
            "prices": {},
            "closed": [],
        }

    season = _current_season()
    prices: dict[str, dict] = {}
    closed: list[dict] = []
    total_realized_pnl = 0.0
    conn = _conn()

    for row in open_rows:
        symbol = row["symbol"]
        qty = float(row["qty"])
        avg_price = float(row["avg_price"])
        asset_type = row["asset_type"] or "stock"
        option_type = row["option_type"]

        exit_price, price_source = _resolve_exit_price(symbol, avg_price)
        prices[symbol] = {"price": exit_price, "source": price_source}

        cash_row = conn.execute(
            "SELECT cash FROM ai_players WHERE id=?",
            (player_id,),
        ).fetchone()
        current_cash = float(cash_row[0]) if cash_row else 0.0

        if qty < 0:
            trade_action = "COVER"
            trade_qty = abs(qty)
            margin = round(trade_qty * avg_price, 2)
            realized_pnl = round(trade_qty * (avg_price - exit_price), 2)
            new_cash = round(current_cash + margin + realized_pnl, 2)
        else:
            trade_action = "SELL"
            trade_qty = qty
            proceeds = round(trade_qty * exit_price, 2)
            realized_pnl = round(proceeds - (trade_qty * avg_price), 2)
            new_cash = round(current_cash + proceeds, 2)

        conn.execute("UPDATE ai_players SET cash=? WHERE id=?", (new_cash, player_id))
        conn.execute("DELETE FROM positions WHERE id=?", (row["id"],))
        conn.execute(
            """
            INSERT INTO trades(
                player_id, symbol, action, qty, price, asset_type, option_type,
                reasoning, confidence, entry_price, exit_price, realized_pnl, season
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                player_id,
                symbol,
                trade_action,
                trade_qty,
                exit_price,
                asset_type,
                option_type,
                "target: legacy cleanup exit",
                0.0,
                avg_price,
                exit_price,
                realized_pnl,
                season,
            ),
        )

        total_realized_pnl += realized_pnl
        closed.append(
            {
                "position_id": row["id"],
                "symbol": symbol,
                "action": trade_action,
                "qty": trade_qty,
                "entry_price": avg_price,
                "exit_price": exit_price,
                "realized_pnl": realized_pnl,
                "price_source": price_source,
            }
        )

    conn.commit()
    conn.close()

    record_portfolio_snapshot(player_id, prices)

    verify = _conn()
    final_cash_row = verify.execute(
        "SELECT cash FROM ai_players WHERE id=?",
        (player_id,),
    ).fetchone()
    open_count_row = verify.execute(
        "SELECT COUNT(*) FROM positions WHERE player_id=?",
        (player_id,),
    ).fetchone()
    verify.close()

    final_cash = round(float(final_cash_row[0]), 2) if final_cash_row else 0.0
    return {
        "player_id": player_id,
        "positions_closed": len(closed),
        "total_realized_pnl": round(total_realized_pnl, 2),
        "final_cash": final_cash,
        "final_equity": final_cash,
        "open_positions_remaining": int(open_count_row[0]) if open_count_row else 0,
        "prices": prices,
        "closed": closed,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Close all open positions for a player.")
    parser.add_argument("player_id")
    args = parser.parse_args()

    result = close_all_positions(args.player_id)
    print(f"PLAYER {result['player_id']}")
    print(f"POSITIONS_CLOSED {result['positions_closed']}")
    print(f"TOTAL_REALIZED_PNL {result['total_realized_pnl']:.2f}")
    print(f"FINAL_CASH {result['final_cash']:.2f}")
    print(f"FINAL_EQUITY {result['final_equity']:.2f}")
    print(f"OPEN_POSITIONS_REMAINING {result.get('open_positions_remaining', 0)}")
    for row in result["closed"]:
        print(
            "CLOSED "
            f"id={row['position_id']} symbol={row['symbol']} action={row['action']} "
            f"qty={row['qty']:.4f} entry={row['entry_price']:.2f} exit={row['exit_price']:.2f} "
            f"pnl={row['realized_pnl']:.2f} source={row['price_source']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
