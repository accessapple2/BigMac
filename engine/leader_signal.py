"""Leader Signal — copy-the-leader intelligence + weekly elimination.

Tracks the #1 ranked model's recent BUY trades and injects them into other
models' prompts as informational signals. Also handles Friday elimination
of underperforming models.
"""
from __future__ import annotations
import sqlite3
from datetime import datetime
from rich.console import Console
from shared.matrix_bridge import is_independent_player

console = Console()
DB = "data/trader.db"


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def _get_standings() -> list[dict]:
    """Get ranked standings for all active arena models (non-human, non-dayblade)."""
    conn = _conn()
    players = conn.execute(
        "SELECT id, display_name, cash FROM ai_players "
        "WHERE is_active=1 AND id NOT IN ('dayblade-0dte','steve-webull') "
        "AND (is_paused IS NULL OR is_paused=0)"
    ).fetchall()

    if not players:
        conn.close()
        return []

    from engine.paper_trader import get_portfolio_with_pnl
    from engine.market_data import get_stock_price

    # Build price map
    prices = {}
    all_syms = set()
    for p in players:
        for row in conn.execute("SELECT DISTINCT symbol FROM positions WHERE player_id=?", (p["id"],)):
            all_syms.add(row["symbol"])
    conn.close()

    for sym in all_syms:
        try:
            d = get_stock_price(sym)
            if "error" not in d:
                prices[sym] = d
        except Exception:
            pass

    standings = []
    starting = 7000.0
    for p in players:
        try:
            pnl = get_portfolio_with_pnl(p["id"], prices)
            tv = pnl["total_value"]
        except Exception:
            tv = p["cash"]
        ret = round((tv - starting) / starting * 100, 1) if starting > 0 else 0
        standings.append({
            "id": p["id"],
            "name": p["display_name"],
            "value": round(tv, 2),
            "return_pct": ret,
        })

    standings.sort(key=lambda x: x["value"], reverse=True)
    return standings


def _get_leader_recent_buys() -> list[dict]:
    """Get BUY trades from the #1 ranked model in the last 24 hours."""
    standings = _get_standings()
    if not standings:
        return []

    leader = standings[0]
    conn = _conn()
    buys = conn.execute(
        "SELECT symbol, action, confidence, reasoning, price, executed_at "
        "FROM trades WHERE player_id=? AND action IN ('BUY','BUY_CALL','BUY_PUT') "
        "AND executed_at >= datetime('now', '-24 hours') "
        "ORDER BY executed_at DESC LIMIT 5",
        (leader["id"],)
    ).fetchall()
    conn.close()

    results = []
    for b in buys:
        results.append({
            "symbol": b["symbol"],
            "action": b["action"],
            "confidence": b["confidence"],
            "reasoning": (b["reasoning"] or "")[:150],
            "price": b["price"],
            "leader_name": leader["name"],
            "leader_return": leader["return_pct"],
        })
    return results


def build_leader_signal_prompt_section(current_player_id: str) -> str:
    """Build prompt section showing the leader's recent trades.

    Only injected for non-leader models. The leader doesn't see its own signal.
    """
    standings = _get_standings()
    if len(standings) < 2:
        return ""

    leader = standings[0]

    # Don't inject into the leader's own prompt
    if leader["id"] == current_player_id:
        return ""

    conn = _conn()
    buys = conn.execute(
        "SELECT symbol, action, confidence, reasoning, price, executed_at "
        "FROM trades WHERE player_id=? AND action IN ('BUY','BUY_CALL','BUY_PUT') "
        "AND executed_at >= datetime('now', '-24 hours') "
        "ORDER BY executed_at DESC LIMIT 3",
        (leader["id"],)
    ).fetchall()
    conn.close()

    if not buys:
        return ""

    lines = ["=== LEADER INTELLIGENCE ==="]
    for b in buys:
        conf_pct = round((b["confidence"] or 0) * 100)
        reason_short = (b["reasoning"] or "no thesis")[:120]
        action_label = b["action"].replace("_", " ")
        lines.append(
            f"ALERT: The leading model ({leader['name']}, {leader['return_pct']:+.1f}%) "
            f"just executed {action_label} on {b['symbol']} at ${b['price']:.2f} "
            f"with {conf_pct}% confidence. Thesis: {reason_short}"
        )

    lines.append(
        "Consider whether these trades align with your strategy. "
        "The leader's moves are informational — do NOT blindly copy. "
        "Think about WHY the leader is buying and whether your own analysis agrees."
    )
    return "\n".join(lines)


def run_weekly_elimination() -> list[dict]:
    """Friday elimination: pause models below -15% return.

    Returns list of eliminated models.
    """
    standings = _get_standings()
    if not standings:
        return []

    eliminated = []
    conn = _conn()

    for s in standings:
        if is_independent_player(s["id"]):
            continue
        if s["return_pct"] <= -15.0:
            # Pause the model
            conn.execute(
                "UPDATE ai_players SET is_paused=1 WHERE id=?", (s["id"],)
            )
            eliminated.append(s)
            console.log(
                f"[bold red]ELIMINATED: {s['name']} ({s['id']}) removed from arena — "
                f"${s['value']:,.0f} ({s['return_pct']:+.1f}%) below -15% threshold"
            )

            # Log elimination to journal
            try:
                conn.execute(
                    "INSERT INTO ai_journal (player_id, entry) VALUES (?, ?)",
                    (s["id"],
                     f"[ELIMINATED] Removed from arena for poor performance. "
                     f"Final account: ${s['value']:,.0f} ({s['return_pct']:+.1f}%). "
                     f"Failed to maintain minimum -15% threshold. Season over."),
                )
            except Exception:
                pass

    if eliminated:
        conn.commit()
        # Telegram alert
        try:
            from engine.telegram_alerts import send_alert
            names = ", ".join(f"{e['name']} ({e['return_pct']:+.1f}%)" for e in eliminated)
            send_alert(
                f"WEEKLY ELIMINATION\n"
                f"{len(eliminated)} model(s) eliminated: {names}\n"
                f"Below -15% return threshold. Paused from arena."
            )
        except Exception:
            pass

    conn.close()
    return eliminated
