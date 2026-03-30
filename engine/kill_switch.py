"""Kill Switch — emergency close ALL positions across ALL models. Flash crash protection."""
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


def kill_all_positions(prices: dict) -> dict:
    """Close EVERY position across ALL models. Returns summary.

    This is the nuclear option — flash crash protection.
    """
    from engine.paper_trader import sell
    from engine.dayblade import sell_position as db_sell, get_portfolio as db_portfolio, DAYBLADE_PLAYER
    from engine.telegram_alerts import send_alert

    conn = _conn()
    # Get all positions across all players
    positions = conn.execute("""
        SELECT pos.player_id, p.display_name, pos.symbol, pos.qty, pos.avg_price,
               pos.asset_type, pos.option_type
        FROM positions pos JOIN ai_players p ON pos.player_id = p.id
    """).fetchall()
    conn.close()

    closed = []
    errors = []
    total_proceeds = 0
    total_pnl = 0

    for pos in positions:
        pid = pos["player_id"]
        sym = pos["symbol"]
        asset_type = pos["asset_type"] or "stock"
        opt_type = pos["option_type"]
        current_price = prices.get(sym, {}).get("price", pos["avg_price"])

        try:
            if pid == DAYBLADE_PLAYER:
                result = db_sell(sym, current_price, opt_type or "call",
                                reasoning="KILL SWITCH — emergency liquidation")
            else:
                result = sell(pid, sym, current_price,
                             asset_type=asset_type,
                             reasoning="KILL SWITCH — emergency liquidation",
                             option_type=opt_type)

            if result:
                pnl = result.get("pnl", 0)
                proceeds = pos["qty"] * current_price
                total_proceeds += proceeds
                total_pnl += pnl
                closed.append({
                    "player_id": pid,
                    "display_name": pos["display_name"],
                    "symbol": sym,
                    "qty": pos["qty"],
                    "price": current_price,
                    "pnl": round(pnl, 2),
                })
        except Exception as e:
            errors.append({"player_id": pid, "symbol": sym, "error": str(e)})

    summary = {
        "positions_closed": len(closed),
        "errors": len(errors),
        "total_proceeds": round(total_proceeds, 2),
        "total_pnl": round(total_pnl, 2),
        "closed": closed,
        "error_details": errors,
        "executed_at": datetime.now().isoformat(),
    }

    console.log(f"[bold red]KILL SWITCH ACTIVATED: {len(closed)} positions closed, P&L: ${total_pnl:+,.2f}")

    # Telegram alert
    try:
        send_alert(
            f"🚨🚨🚨 <b>KILL SWITCH ACTIVATED</b> 🚨🚨🚨\n"
            f"Closed {len(closed)} positions across all models\n"
            f"Total P&L: ${total_pnl:+,.2f}\n"
            f"All portfolios now CASH ONLY"
        )
    except Exception:
        pass

    # Log to DB
    try:
        import json
        conn2 = _conn()
        conn2.execute(
            "INSERT INTO kill_switch_log (positions_closed, total_pnl, details) VALUES (?, ?, ?)",
            (len(closed), total_pnl, json.dumps(summary))
        )
        conn2.commit()
        conn2.close()
    except Exception:
        pass

    return summary


def get_kill_switch_history() -> list:
    """Get history of kill switch activations."""
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT positions_closed, total_pnl, activated_at FROM kill_switch_log ORDER BY activated_at DESC LIMIT 10"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        conn.close()
        return []
