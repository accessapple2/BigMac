"""Captain's Decision Tracker — logs when Kirk follows or ignores crew advice.

Tracks EXECUTED/IGNORED/MODIFIED decisions and measures P&L outcomes.
"""
from __future__ import annotations
import sqlite3
from datetime import datetime
from rich.console import Console

console = Console()
DB = "data/trader.db"


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def ensure_tables():
    """Create captain_decisions table."""
    conn = _conn()
    conn.execute("""CREATE TABLE IF NOT EXISTS captain_decisions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT DEFAULT (datetime('now')),
        ticker TEXT NOT NULL,
        crew_member TEXT NOT NULL,
        crew_action TEXT NOT NULL,
        crew_conviction REAL DEFAULT 0,
        captain_action TEXT NOT NULL,
        captain_notes TEXT,
        entry_price REAL,
        current_price REAL,
        outcome_pnl REAL,
        outcome_pct REAL,
        resolved_at TEXT,
        season INTEGER
    )""")
    conn.commit()
    conn.close()


def log_decision(ticker: str, crew_member: str, crew_action: str,
                 captain_action: str, conviction: float = 0,
                 notes: str = "", entry_price: float = 0) -> dict:
    """Log a Captain's decision on a crew recommendation."""
    ensure_tables()
    from engine.paper_trader import _current_season
    conn = _conn()
    conn.execute(
        "INSERT INTO captain_decisions "
        "(ticker, crew_member, crew_action, captain_action, "
        "crew_conviction, captain_notes, entry_price, season) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (ticker.upper(), crew_member, crew_action.upper(), captain_action.upper(),
         conviction, notes, entry_price, _current_season())
    )
    conn.commit()
    entry_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.close()
    console.log(f"[magenta]Captain decision: {captain_action} {crew_member}'s {crew_action} on {ticker}")
    return {"ok": True, "id": entry_id}


def get_decisions(crew: str = None, limit: int = 50) -> list:
    """Get captain's decisions, optionally filtered by crew member."""
    ensure_tables()
    conn = _conn()
    if crew:
        rows = conn.execute(
            "SELECT * FROM captain_decisions WHERE crew_member=? ORDER BY created_at DESC LIMIT ?",
            (crew, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM captain_decisions ORDER BY created_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_scorecard() -> dict:
    """How well did Kirk do following vs ignoring crew advice?"""
    ensure_tables()
    conn = _conn()
    decisions = conn.execute(
        "SELECT crew_member, captain_action, outcome_pnl "
        "FROM captain_decisions WHERE outcome_pnl IS NOT NULL"
    ).fetchall()
    conn.close()

    scorecard = {}
    for d in decisions:
        crew = d["crew_member"]
        action = d["captain_action"]
        pnl = d["outcome_pnl"] or 0

        if crew not in scorecard:
            scorecard[crew] = {
                "followed": {"count": 0, "total_pnl": 0, "wins": 0},
                "ignored": {"count": 0, "total_pnl": 0, "wins": 0},
            }

        bucket = "followed" if action == "EXECUTED" else "ignored"
        scorecard[crew][bucket]["count"] += 1
        scorecard[crew][bucket]["total_pnl"] = round(
            scorecard[crew][bucket]["total_pnl"] + pnl, 2
        )
        if pnl > 0:
            scorecard[crew][bucket]["wins"] += 1

    # Calculate win rates
    for crew in scorecard:
        for bucket in ("followed", "ignored"):
            b = scorecard[crew][bucket]
            b["win_rate"] = round(b["wins"] / b["count"] * 100, 1) if b["count"] > 0 else 0

    return scorecard


def resolve_decision(decision_id: int, outcome_pnl: float, outcome_pct: float = 0) -> dict:
    """Resolve a decision with its P&L outcome."""
    ensure_tables()
    conn = _conn()
    conn.execute(
        "UPDATE captain_decisions SET outcome_pnl=?, outcome_pct=?, resolved_at=datetime('now') WHERE id=?",
        (outcome_pnl, outcome_pct, decision_id)
    )
    conn.commit()
    conn.close()
    return {"ok": True}
