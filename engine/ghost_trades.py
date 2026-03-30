"""Ghost Trades — track HOLD decisions with >60% confidence as phantom trades."""
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


def log_ghost_trade(player_id: str, symbol: str, confidence: float,
                    reasoning: str, price: float):
    """Log a ghost trade — a HOLD decision that had >60% confidence."""
    if confidence < 0.60:
        return
    conn = _conn()
    conn.execute(
        "INSERT INTO ghost_trades (player_id, symbol, confidence, reasoning, entry_price) "
        "VALUES (?, ?, ?, ?, ?)",
        (player_id, symbol, confidence, reasoning, price)
    )
    conn.commit()
    conn.close()


def update_ghost_outcomes(prices: dict):
    """Update ghost trades with current prices to see what we missed.

    Called periodically to track how ghost trades would have performed.
    """
    conn = _conn()
    # Get all open ghost trades (no outcome yet)
    ghosts = conn.execute(
        "SELECT id, symbol, entry_price FROM ghost_trades WHERE outcome_price IS NULL"
    ).fetchall()

    for g in ghosts:
        sym = g["symbol"]
        if sym in prices:
            current = prices[sym].get("price", 0)
            if current > 0:
                pnl_pct = ((current / g["entry_price"]) - 1) * 100
                conn.execute(
                    "UPDATE ghost_trades SET outcome_price=?, outcome_pnl_pct=?, updated_at=CURRENT_TIMESTAMP "
                    "WHERE id=?",
                    (current, round(pnl_pct, 2), g["id"])
                )

    conn.commit()
    conn.close()


def get_ghost_trades(player_id: str = None, limit: int = 50) -> list:
    """Get ghost trades, optionally filtered by player."""
    conn = _conn()
    if player_id:
        rows = conn.execute(
            "SELECT g.*, p.display_name FROM ghost_trades g "
            "JOIN ai_players p ON g.player_id = p.id "
            "WHERE g.player_id=? ORDER BY g.created_at DESC LIMIT ?",
            (player_id, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT g.*, p.display_name FROM ghost_trades g "
            "JOIN ai_players p ON g.player_id = p.id "
            "ORDER BY g.created_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_ghost_stats() -> dict:
    """Get aggregate ghost trade statistics — missed opportunities."""
    conn = _conn()

    # Total ghosts with outcomes
    stats = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN outcome_pnl_pct > 0 THEN 1 ELSE 0 END) as would_have_won,
            SUM(CASE WHEN outcome_pnl_pct <= 0 THEN 1 ELSE 0 END) as would_have_lost,
            AVG(outcome_pnl_pct) as avg_pnl_pct,
            MAX(outcome_pnl_pct) as best_ghost,
            MIN(outcome_pnl_pct) as worst_ghost
        FROM ghost_trades WHERE outcome_price IS NOT NULL
    """).fetchone()

    # Top missed opportunities
    top_missed = conn.execute("""
        SELECT g.player_id, p.display_name, g.symbol, g.confidence,
               g.entry_price, g.outcome_price, g.outcome_pnl_pct, g.created_at
        FROM ghost_trades g JOIN ai_players p ON g.player_id = p.id
        WHERE g.outcome_pnl_pct > 0
        ORDER BY g.outcome_pnl_pct DESC LIMIT 5
    """).fetchall()

    conn.close()

    return {
        "total_ghosts": stats["total"] if stats else 0,
        "would_have_won": stats["would_have_won"] if stats else 0,
        "would_have_lost": stats["would_have_lost"] if stats else 0,
        "avg_pnl_pct": round(stats["avg_pnl_pct"], 2) if stats and stats["avg_pnl_pct"] else 0,
        "best_ghost_pct": round(stats["best_ghost"], 2) if stats and stats["best_ghost"] else 0,
        "worst_ghost_pct": round(stats["worst_ghost"], 2) if stats and stats["worst_ghost"] else 0,
        "top_missed": [dict(r) for r in top_missed],
    }
