"""
sub_portfolio.py — Ring-fenced budget isolation for TradeMinds strategy groups.

Prevents any one strategy from consuming the entire paper account.

Default ceilings:
  Bridge Vote Picks  $30,000
  DayBlade Options   $10,000
  User Agents         $5,000
  Scanner Picks      $20,000
"""
import logging
import os
import sqlite3
from typing import Optional

logger = logging.getLogger("sub_portfolio")

DB_PATH = os.environ.get(
    "TRADEMINDS_DB",
    os.path.expanduser("~/autonomous-trader/data/trader.db"),
)

# Maps strategy name → player_id substrings (case-insensitive prefix/contains match)
_STRATEGY_PLAYERS: dict[str, list[str]] = {
    "Bridge Vote Picks": [
        "claude-sonnet", "gemini-2.5-pro", "gpt-4o", "gpt-o3", "grok-3",
        "captain-sisko", "seven-of-nine", "captain-janeway", "lt-tuvok",
        "ensign-hoshi", "bridge",
    ],
    "DayBlade Options": ["dayblade", "sulu", "dte-", "options"],
    "User Agents":      ["user-agent"],
    "Scanner Picks":    ["chekov", "scotty", "scanner", "warp10", "momentum", "gap-", "volume-"],
}

_DEFAULTS: dict[str, float] = {
    "Bridge Vote Picks": 30_000.0,
    "DayBlade Options":  10_000.0,
    "User Agents":        5_000.0,
    "Scanner Picks":     20_000.0,
}


def _conn():
    return sqlite3.connect(DB_PATH, timeout=30)


def _init_table():
    with _conn() as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS sub_portfolios (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                name           TEXT UNIQUE NOT NULL,
                budget_ceiling REAL NOT NULL DEFAULT 10000.0,
                strategy_type  TEXT,
                created_at     TEXT DEFAULT (datetime('now')),
                updated_at     TEXT DEFAULT (datetime('now'))
            )
        """)
        db.commit()
        for name, ceiling in _DEFAULTS.items():
            db.execute(
                "INSERT OR IGNORE INTO sub_portfolios (name, budget_ceiling, strategy_type) VALUES (?, ?, ?)",
                (name, ceiling, name),
            )
        db.commit()


def _positions_for_strategy(strategy_name: str) -> list[dict]:
    """Return positions belonging to a strategy by matching player_id patterns."""
    patterns = _STRATEGY_PLAYERS.get(strategy_name, [])
    db = _conn()
    try:
        rows = db.execute(
            "SELECT player_id, symbol, qty, avg_price FROM positions WHERE qty > 0"
        ).fetchall()
    finally:
        db.close()

    result = []
    for player_id, symbol, qty, avg_price in rows:
        pid = (player_id or "").lower()
        for pat in patterns:
            if pat.lower() in pid:
                result.append({
                    "player_id": player_id,
                    "symbol": symbol,
                    "qty": float(qty or 0),
                    "avg_price": float(avg_price or 0),
                })
                break
    return result


def get_current_exposure(strategy_name: str) -> float:
    """Return total cost-basis exposure (qty × avg_price) for a strategy group."""
    positions = _positions_for_strategy(strategy_name)
    return sum(p["qty"] * p["avg_price"] for p in positions)


def check_budget(strategy_name: str, trade_value: float) -> tuple[bool, str]:
    """
    Check if a trade would push the strategy over its budget ceiling.
    Returns (allowed: bool, reason: str).
    """
    _init_table()
    db = _conn()
    row = db.execute(
        "SELECT budget_ceiling FROM sub_portfolios WHERE name = ?",
        (strategy_name,),
    ).fetchone()
    db.close()

    if not row:
        return True, "No budget configured — trade allowed"

    ceiling  = float(row[0])
    current  = get_current_exposure(strategy_name)
    projected = current + trade_value

    if projected > ceiling:
        return False, (
            f"Budget exceeded [{strategy_name}]: "
            f"${current:,.0f} + ${trade_value:,.0f} = ${projected:,.0f} > ${ceiling:,.0f} ceiling"
        )
    return True, f"OK [{strategy_name}] ${current:,.0f} / ${ceiling:,.0f}"


def set_budget(name: str, ceiling: float) -> dict:
    """Create or update the budget ceiling for a sub-portfolio."""
    _init_table()
    with _conn() as db:
        db.execute(
            """INSERT INTO sub_portfolios (name, budget_ceiling, strategy_type, updated_at)
               VALUES (?, ?, ?, datetime('now'))
               ON CONFLICT(name) DO UPDATE SET
                   budget_ceiling = excluded.budget_ceiling,
                   updated_at = datetime('now')""",
            (name, ceiling, name),
        )
        db.commit()
    return {"ok": True, "name": name, "budget_ceiling": ceiling}


def list_sub_portfolios() -> list:
    """Return all sub-portfolios with live exposure and availability."""
    _init_table()
    db = _conn()
    rows = db.execute(
        "SELECT id, name, budget_ceiling, strategy_type, updated_at FROM sub_portfolios ORDER BY id"
    ).fetchall()
    db.close()

    result = []
    for r in rows:
        name    = r[1]
        ceiling = float(r[2])
        current = get_current_exposure(name)
        result.append({
            "id":              r[0],
            "name":            name,
            "budget_ceiling":  ceiling,
            "current_value":   round(current, 2),
            "available":       round(max(0.0, ceiling - current), 2),
            "utilization_pct": round(current / ceiling * 100, 1) if ceiling > 0 else 0.0,
            "strategy_type":   r[3],
            "updated_at":      r[4],
        })
    return result
