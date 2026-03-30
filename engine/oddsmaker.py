"""OddsMaker Score -- backtest signal setups against recent history for win probability."""
from __future__ import annotations
import sqlite3
from datetime import datetime, timedelta
from rich.console import Console

import config

console = Console()
DB = "data/trader.db"


def calculate_odds(symbol: str, signal: str, lookback_days: int = 30) -> dict:
    """Backtest a signal type for a symbol against recent trade history.
    Returns win probability based on past trades with similar setups.
    """
    try:
        conn = sqlite3.connect(DB, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        cutoff = (datetime.now() - timedelta(days=lookback_days)).isoformat()

        # Find all closed trades for this symbol with this signal direction
        is_buy = "BUY" in signal.upper()
        action = "BUY" if is_buy else "SELL"

        # Get trades that match this symbol and direction
        rows = conn.execute(
            """
            SELECT t.realized_pnl, t.symbol, t.action, t.executed_at
            FROM trades t
            WHERE t.symbol = ? AND t.action = ? AND t.executed_at > ? AND t.realized_pnl IS NOT NULL AND t.realized_pnl != 0
            ORDER BY t.executed_at DESC
        """,
            (symbol.upper(), action, cutoff),
        ).fetchall()

        if not rows:
            # Broaden: look at all trades for this symbol
            rows = conn.execute(
                """
                SELECT t.realized_pnl, t.symbol, t.action, t.executed_at
                FROM trades t
                WHERE t.symbol = ? AND t.executed_at > ? AND t.realized_pnl IS NOT NULL AND t.realized_pnl != 0
                ORDER BY t.executed_at DESC
            """,
                (symbol.upper(), cutoff),
            ).fetchall()

        if not rows:
            # Even broader: look at all trades for similar signals
            rows = conn.execute(
                """
                SELECT t.realized_pnl, t.symbol, t.action, t.executed_at
                FROM trades t
                WHERE t.action = ? AND t.executed_at > ? AND t.realized_pnl IS NOT NULL AND t.realized_pnl != 0
                ORDER BY t.executed_at DESC LIMIT 50
            """,
                (action, cutoff),
            ).fetchall()

        conn.close()
    except Exception:
        return {
            "symbol": symbol,
            "signal": signal,
            "odds": None,
            "sample_size": 0,
            "error": "DB error",
        }

    if not rows:
        return {
            "symbol": symbol,
            "signal": signal,
            "odds": None,
            "sample_size": 0,
            "note": "No historical data",
        }

    wins = sum(1 for r in rows if r["realized_pnl"] > 0)
    total = len(rows)
    odds = round(wins / total * 100, 1) if total > 0 else 0
    avg_pnl = round(sum(r["realized_pnl"] for r in rows) / total, 2)

    return {
        "symbol": symbol,
        "signal": signal,
        "odds": odds,
        "wins": wins,
        "losses": total - wins,
        "sample_size": total,
        "avg_pnl": avg_pnl,
        "lookback_days": lookback_days,
        "summary": f"This setup has won {wins}/{total} times in the last {lookback_days} days = {odds}% odds",
    }


def get_signals_with_odds(limit: int = 20) -> list:
    """Get recent signals with OddsMaker probability attached."""
    try:
        conn = sqlite3.connect(DB, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT s.player_id, s.symbol, s.signal, s.confidence, s.reasoning, s.created_at,
                   p.display_name
            FROM signals s
            JOIN ai_players p ON p.id = s.player_id
            ORDER BY s.created_at DESC LIMIT ?
        """,
            (limit,),
        ).fetchall()
        conn.close()
    except Exception:
        return []

    results = []
    seen = set()
    for r in rows:
        sig = dict(r)
        key = f"{sig['symbol']}:{sig['signal']}"
        if key not in seen:
            odds = calculate_odds(sig["symbol"], sig["signal"])
            sig["odds"] = odds
            seen.add(key)
        results.append(sig)
    return results
