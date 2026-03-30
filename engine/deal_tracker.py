"""Deal Tracking — group multiple buys of the same stock by same model into a single deal.

A "deal" is all BUY/SELL trades of the same symbol by the same player while a position is open.
"""
from __future__ import annotations
import sqlite3
from datetime import datetime
from rich.console import Console

console = Console()
DB = "data/trader.db"


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def get_active_deals() -> list:
    """Get all active deals — open positions grouped by player + symbol."""
    conn = _conn()

    # Get all open positions
    positions = conn.execute(
        "SELECT player_id, symbol, qty, avg_price, asset_type, option_type "
        "FROM positions"
    ).fetchall()

    deals = []
    for pos in positions:
        player_id = pos["player_id"]
        symbol = pos["symbol"]
        asset_type = pos["asset_type"] or "stock"
        option_type = pos["option_type"]

        # Get all trades for this player+symbol combo
        if asset_type == "stock":
            trades = conn.execute(
                "SELECT action, qty, price, reasoning, confidence, executed_at "
                "FROM trades WHERE player_id=? AND symbol=? AND asset_type='stock' "
                "ORDER BY executed_at ASC",
                (player_id, symbol)
            ).fetchall()
        else:
            trades = conn.execute(
                "SELECT action, qty, price, reasoning, confidence, executed_at "
                "FROM trades WHERE player_id=? AND symbol=? AND option_type=? "
                "ORDER BY executed_at ASC",
                (player_id, symbol, option_type)
            ).fetchall()

        if not trades:
            continue

        # Find the last "position open" sequence — from last net-zero point
        trade_list = [dict(t) for t in trades]
        open_trades = _find_current_deal_trades(trade_list)

        if not open_trades:
            continue

        # Calculate deal metrics
        buys = [t for t in open_trades if t["action"] == "BUY"]
        sells = [t for t in open_trades if t["action"] == "SELL"]

        total_bought_qty = sum(t["qty"] for t in buys)
        total_bought_cost = sum(t["qty"] * t["price"] for t in buys)
        total_sold_qty = sum(t["qty"] for t in sells)
        total_sold_proceeds = sum(t["qty"] * t["price"] for t in sells)

        dca_entry = total_bought_cost / total_bought_qty if total_bought_qty > 0 else 0
        remaining_qty = pos["qty"]

        # Get player display name
        player_row = conn.execute(
            "SELECT display_name FROM ai_players WHERE id=?", (player_id,)
        ).fetchone()
        display_name = player_row["display_name"] if player_row else player_id

        deal = {
            "player_id": player_id,
            "display_name": display_name,
            "symbol": symbol,
            "asset_type": asset_type,
            "option_type": option_type,
            "dca_entry": round(dca_entry, 4),
            "current_qty": remaining_qty,
            "total_buys": len(buys),
            "total_sells": len(sells),
            "total_bought_qty": round(total_bought_qty, 4),
            "total_sold_qty": round(total_sold_qty, 4),
            "realized_pnl": round(total_sold_proceeds - (total_sold_qty * dca_entry), 2) if total_sold_qty > 0 else 0,
            "cost_basis": round(remaining_qty * dca_entry, 2),
            "opened_at": open_trades[0]["executed_at"],
            "last_action_at": open_trades[-1]["executed_at"],
            "trades": open_trades,
        }
        deals.append(deal)

    conn.close()
    return deals


def get_deals_with_pnl(prices: dict) -> list:
    """Get active deals enriched with live P&L."""
    deals = get_active_deals()
    for deal in deals:
        symbol = deal["symbol"]
        price_data = prices.get(symbol, {})
        current_price = price_data.get("price", deal["dca_entry"])

        market_value = deal["current_qty"] * current_price
        unrealized_pnl = market_value - deal["cost_basis"]
        unrealized_pct = ((current_price - deal["dca_entry"]) / deal["dca_entry"] * 100) if deal["dca_entry"] > 0 else 0
        total_pnl = deal["realized_pnl"] + unrealized_pnl

        deal["current_price"] = round(current_price, 2)
        deal["market_value"] = round(market_value, 2)
        deal["unrealized_pnl"] = round(unrealized_pnl, 2)
        deal["unrealized_pct"] = round(unrealized_pct, 2)
        deal["total_pnl"] = round(total_pnl, 2)

    return deals


def get_closed_deals(limit: int = 50) -> list:
    """Get recently closed deals — positions that were fully sold."""
    conn = _conn()

    # Get symbols that have sells but no current position for each player
    all_trades = conn.execute(
        "SELECT player_id, symbol, action, qty, price, reasoning, confidence, executed_at, asset_type "
        "FROM trades ORDER BY executed_at ASC"
    ).fetchall()
    conn.close()

    # Group by player+symbol
    groups: dict[tuple, list] = {}
    for t in all_trades:
        key = (t["player_id"], t["symbol"], t["asset_type"] or "stock")
        if key not in groups:
            groups[key] = []
        groups[key].append(dict(t))

    closed_deals = []
    for (player_id, symbol, asset_type), trades in groups.items():
        # Find completed deal sequences
        deals = _extract_closed_deals(trades)
        for d in deals:
            d["player_id"] = player_id
            d["symbol"] = symbol
            d["asset_type"] = asset_type
            closed_deals.append(d)

    closed_deals.sort(key=lambda x: x.get("closed_at", ""), reverse=True)
    return closed_deals[:limit]


def _find_current_deal_trades(trades: list) -> list:
    """Find the trades belonging to the current open deal."""
    # Walk forward, tracking net position
    # The current deal starts after the last time position went to zero
    net_qty = 0
    last_zero_idx = -1

    for i, t in enumerate(trades):
        if t["action"] == "BUY":
            net_qty += t["qty"]
        elif t["action"] == "SELL":
            net_qty -= t["qty"]
        if abs(net_qty) < 0.0001:
            last_zero_idx = i

    # Current deal is everything after last zero point
    return trades[last_zero_idx + 1:]


def _extract_closed_deals(trades: list) -> list:
    """Extract completed (closed) deals from trade history."""
    deals = []
    deal_start = 0
    net_qty = 0

    for i, t in enumerate(trades):
        if t["action"] == "BUY":
            net_qty += t["qty"]
        elif t["action"] == "SELL":
            net_qty -= t["qty"]

        if abs(net_qty) < 0.0001 and i > deal_start:
            # Deal closed
            deal_trades = trades[deal_start:i + 1]
            buys = [x for x in deal_trades if x["action"] == "BUY"]
            sells = [x for x in deal_trades if x["action"] == "SELL"]

            if buys and sells:
                total_cost = sum(x["qty"] * x["price"] for x in buys)
                total_proceeds = sum(x["qty"] * x["price"] for x in sells)
                total_qty = sum(x["qty"] for x in buys)
                dca_entry = total_cost / total_qty if total_qty > 0 else 0
                avg_exit = total_proceeds / total_qty if total_qty > 0 else 0

                deals.append({
                    "dca_entry": round(dca_entry, 4),
                    "avg_exit": round(avg_exit, 4),
                    "total_qty": round(total_qty, 4),
                    "pnl": round(total_proceeds - total_cost, 2),
                    "pnl_pct": round((avg_exit - dca_entry) / dca_entry * 100, 2) if dca_entry > 0 else 0,
                    "num_buys": len(buys),
                    "num_sells": len(sells),
                    "opened_at": deal_trades[0]["executed_at"],
                    "closed_at": deal_trades[-1]["executed_at"],
                })

            deal_start = i + 1
            net_qty = 0

    return deals
