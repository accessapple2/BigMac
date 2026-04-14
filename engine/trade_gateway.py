"""Trade Gateway — Policy Engine for OllieTrades Season 6.
Intercepts every trade before it hits Alpaca and enforces rules."""

import sqlite3
from datetime import datetime, date
from rich.console import Console

console = Console()

DB_PATH = "data/trader.db"

# Per-agent daily trade limits
AGENT_LIMITS = {
    "ollie-auto": 15,
}
DEFAULT_LIMIT = 10

# Max dollar value per single position
MAX_POSITION_VALUE = 5000.0


def check_trade(agent_id: str, symbol: str, action: str, qty: float, price: float) -> dict:
    """
    Returns {'allowed': True} or {'allowed': False, 'reason': '...'}
    """
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        # 1. Kill switch check
        row = conn.execute(
            "SELECT value FROM settings WHERE key = ?",
            (f"kill_switch_{agent_id}",)
        ).fetchone()
        if row and row[0] == "1":
            reason = f"Kill switch active for {agent_id}"
            _log_block(conn, agent_id, symbol, action, qty, price, reason)
            return {"allowed": False, "reason": reason}

        # 2. Daily trade count check
        today = date.today().isoformat()
        count_row = conn.execute(
            """SELECT COUNT(*) FROM trades
               WHERE player_id = ?
               AND date(executed_at) = ?""",
            (agent_id, today)
        ).fetchone()
        count = count_row[0] if count_row else 0
        limit = AGENT_LIMITS.get(agent_id, DEFAULT_LIMIT)
        if count >= limit:
            reason = f"{agent_id} daily limit reached ({count}/{limit})"
            _log_block(conn, agent_id, symbol, action, qty, price, reason)
            return {"allowed": False, "reason": reason}

        # 3. Position size check (buys only)
        if action.upper() == "BUY":
            value = qty * price
            if value > MAX_POSITION_VALUE:
                reason = f"Position size ${value:.2f} exceeds max ${MAX_POSITION_VALUE:.2f}"
                _log_block(conn, agent_id, symbol, action, qty, price, reason)
                return {"allowed": False, "reason": reason}

        conn.commit()
        return {"allowed": True}

    except Exception as e:
        console.log(f"[yellow]Gateway error (allowing trade): {e}")
        return {"allowed": True}
    finally:
        conn.close()


def _log_block(conn, agent_id, symbol, action, qty, price, reason):
    """Log blocked trades to kill_switch_log table."""
    try:
        conn.execute(
            """INSERT INTO kill_switch_log
               (agent_id, symbol, action, qty, price, reason, blocked_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (agent_id, symbol, action, qty, price, reason, datetime.utcnow().isoformat())
        )
        console.log(f"[red]🚫 GATEWAY BLOCKED [{agent_id}] {action} {qty} {symbol} — {reason}")
    except Exception as e:
        console.log(f"[yellow]Could not log blocked trade: {e}")
