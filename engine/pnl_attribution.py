"""P&L Attribution — break down daily P&L by model, sector, trade type, entry time."""
from __future__ import annotations
import sqlite3
from datetime import datetime, timedelta
from collections import defaultdict
from rich.console import Console

console = Console()
DB = "data/trader.db"


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def get_pnl_attribution(days: int = 7) -> dict:
    """Break down P&L by model, sector, trade type (stock/call/put), and entry time.

    Returns {by_model, by_sector, by_trade_type, by_hour, daily_breakdown}.
    """
    from engine.sector_tracker import SECTOR_MAP

    conn = _conn()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    # Get all sells with matched buy prices
    sells = conn.execute("""
        SELECT t.player_id, p.display_name, t.symbol, t.qty, t.price as sell_price,
               t.asset_type, t.option_type, t.executed_at,
               (SELECT t2.price FROM trades t2
                WHERE t2.player_id = t.player_id AND t2.symbol = t.symbol
                AND t2.action LIKE 'BUY%' AND t2.executed_at < t.executed_at
                ORDER BY t2.executed_at DESC LIMIT 1) as buy_price,
               (SELECT t2.executed_at FROM trades t2
                WHERE t2.player_id = t.player_id AND t2.symbol = t.symbol
                AND t2.action LIKE 'BUY%' AND t2.executed_at < t.executed_at
                ORDER BY t2.executed_at DESC LIMIT 1) as buy_time
        FROM trades t JOIN ai_players p ON t.player_id = p.id
        WHERE t.action = 'SELL' AND t.executed_at >= ?
        ORDER BY t.executed_at ASC
    """, (cutoff,)).fetchall()
    conn.close()

    by_model = defaultdict(lambda: {"pnl": 0, "trades": 0, "name": ""})
    by_sector = defaultdict(lambda: {"pnl": 0, "trades": 0})
    by_type = defaultdict(lambda: {"pnl": 0, "trades": 0})
    by_hour = defaultdict(lambda: {"pnl": 0, "trades": 0})
    daily = defaultdict(lambda: defaultdict(float))  # date -> model -> pnl

    for s in sells:
        if not s["buy_price"]:
            continue

        pnl = (s["sell_price"] - s["buy_price"]) * s["qty"]
        pid = s["player_id"]
        sym = s["symbol"]
        sector = SECTOR_MAP.get(sym, "Other")

        # Trade type
        asset = s["asset_type"] or "stock"
        if asset == "option":
            trade_type = f"{(s['option_type'] or 'call').upper()}"
        else:
            trade_type = "STOCK"

        # Entry hour
        entry_hour = 12
        if s["buy_time"]:
            try:
                entry_hour = datetime.fromisoformat(s["buy_time"].replace("Z", "")).hour
            except Exception:
                pass

        # Date
        try:
            sell_date = s["executed_at"][:10]
        except Exception:
            sell_date = datetime.now().strftime("%Y-%m-%d")

        by_model[pid]["pnl"] += pnl
        by_model[pid]["trades"] += 1
        by_model[pid]["name"] = s["display_name"]

        by_sector[sector]["pnl"] += pnl
        by_sector[sector]["trades"] += 1

        by_type[trade_type]["pnl"] += pnl
        by_type[trade_type]["trades"] += 1

        by_hour[entry_hour]["pnl"] += pnl
        by_hour[entry_hour]["trades"] += 1

        daily[sell_date][pid] += pnl

    # Round everything
    for k in by_model:
        by_model[k]["pnl"] = round(by_model[k]["pnl"], 2)
    for k in by_sector:
        by_sector[k]["pnl"] = round(by_sector[k]["pnl"], 2)
    for k in by_type:
        by_type[k]["pnl"] = round(by_type[k]["pnl"], 2)
    for k in by_hour:
        by_hour[k]["pnl"] = round(by_hour[k]["pnl"], 2)

    # Daily breakdown: list of {date, models: {pid: pnl}}
    daily_list = []
    for date in sorted(daily.keys()):
        daily_list.append({
            "date": date,
            "models": {pid: round(pnl, 2) for pid, pnl in daily[date].items()},
            "total": round(sum(daily[date].values()), 2),
        })

    return {
        "days": days,
        "by_model": dict(by_model),
        "by_sector": dict(by_sector),
        "by_trade_type": dict(by_type),
        "by_hour": {str(h): v for h, v in sorted(by_hour.items())},
        "daily_breakdown": daily_list,
    }
