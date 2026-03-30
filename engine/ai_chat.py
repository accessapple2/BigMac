"""AI chat system - AIs discuss their trades and debate market moves."""
from __future__ import annotations
import sqlite3
import random
from rich.console import Console
from engine.providers.base import AIProvider
from shared.matrix_bridge import annotate_player_payload

console = Console()
DB = "data/trader.db"


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    return c


def generate_chat_message(provider: AIProvider, player_id: str,
                          recent_action: str = None, prices: dict = None,
                          other_messages: list[dict] = None) -> str | None:
    """Have an AI generate a chat message about their trading activity."""
    # Build context
    conn = _conn()
    conn.row_factory = sqlite3.Row

    # Get player's latest trade
    latest_trade = conn.execute(
        "SELECT symbol, action, qty, price, reasoning FROM trades "
        "WHERE player_id=? ORDER BY executed_at DESC LIMIT 1",
        (player_id,)
    ).fetchone()

    # Get player's portfolio
    cash = conn.execute(
        "SELECT cash FROM ai_players WHERE id=?", (player_id,)
    ).fetchone()

    positions = conn.execute(
        "SELECT symbol, qty, avg_price FROM positions WHERE player_id=?",
        (player_id,)
    ).fetchall()

    # Get recent chat messages from other AIs
    recent_chats = conn.execute(
        "SELECT c.message, p.display_name FROM ai_chat c "
        "JOIN ai_players p ON c.player_id = p.id "
        "WHERE c.player_id != ? "
        "ORDER BY c.created_at DESC LIMIT 3",
        (player_id,)
    ).fetchall()
    conn.close()

    # Build chat prompt
    positions_str = ", ".join(f"{p['symbol']}({p['qty']})" for p in positions) or "None"
    cash_val = cash["cash"] if cash else 0

    other_chat_str = ""
    if recent_chats:
        other_chat_str = "\n\nRecent messages from other AI traders:\n" + "\n".join(
            f"- {c['display_name']}: \"{c['message']}\""
            for c in recent_chats
        )

    trade_str = ""
    if latest_trade:
        trade_str = f"\nYour last trade: {latest_trade['action']} {latest_trade['qty']} {latest_trade['symbol']} @ ${latest_trade['price']:.2f}"

    market_str = ""
    if prices:
        market_str = "\nMarket snapshot: " + ", ".join(
            f"{s} ${d['price']:.2f} ({d['change_pct']:+.1f}%)"
            for s, d in list(prices.items())[:5]
        )

    prompt = f"""You are {provider.display_name}, an AI trader in a competitive trading arena.
Your portfolio: ${cash_val:,.2f} cash, positions: {positions_str}{trade_str}{market_str}{other_chat_str}

Write a short chat message (1-2 sentences max) about your current market view or recent trade.
Be opinionated, competitive, and show personality. You can agree/disagree with other AIs.
Keep it concise and punchy like a trading floor chat. No emojis. No hashtags.
Just the message text, nothing else."""

    try:
        provider.limiter.wait()
        response = provider.call_model(prompt)

        # Track chat cost
        try:
            from engine.cost_tracker import log_cost
            log_cost(player_id, "chat", prompt, response)
        except Exception:
            pass

        message = response.strip().strip('"').strip("'")
        # Limit message length
        if len(message) > 280:
            message = message[:277] + "..."
        return message
    except Exception as e:
        console.log(f"[red]Chat generation error for {player_id}: {e}")
        return None


def save_chat_message(player_id: str, message: str, context: str = None,
                      reply_to: int = None):
    """Save a chat message to the DB."""
    conn = _conn()
    conn.execute(
        "INSERT INTO ai_chat (player_id, message, context, reply_to) VALUES (?,?,?,?)",
        (player_id, message, context, reply_to)
    )
    conn.commit()
    conn.close()


def get_recent_chat(limit: int = 50) -> list[dict]:
    """Get recent chat messages."""
    conn = _conn()
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT c.id, c.player_id, p.display_name, p.provider, c.message, "
        "c.context, c.reply_to, c.created_at "
        "FROM ai_chat c JOIN ai_players p ON c.player_id = p.id "
        "ORDER BY c.created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [annotate_player_payload(dict(r)) for r in rows]


def get_player_chat(player_id: str, limit: int = 20) -> list[dict]:
    """Get chat messages for a specific player."""
    conn = _conn()
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT c.id, c.player_id, p.display_name, p.provider, c.message, "
        "c.context, c.reply_to, c.created_at "
        "FROM ai_chat c JOIN ai_players p ON c.player_id = p.id "
        "WHERE c.player_id = ? ORDER BY c.created_at DESC LIMIT ?",
        (player_id, limit)
    ).fetchall()
    conn.close()
    return [annotate_player_payload(dict(r)) for r in rows]
