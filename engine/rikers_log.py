"""Riker's Log — Captain's decision journal with officer recommendations.

Tracks:
- Captain's manual entries (trade reasoning, market thoughts)
- Spock's CTO briefings (auto-synced)
- Data's SuperGrok recommendations (pasted by Captain)
- Trade outcomes linked back to the original thesis

entry_type: 'manual' | 'spock_briefing' | 'data_recommendation' | 'trade_outcome'
source: 'captain' | 'spock' | 'data' | 'system'
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


def add_entry(entry_type: str, source: str, content: str,
              title: str = None, ticker: str = None, action: str = None,
              conviction: float = None, tags: str = None) -> dict:
    """Add a new entry to Riker's Log."""
    conn = _conn()
    conn.execute(
        "INSERT INTO rikers_log (entry_type, source, title, content, ticker, action, conviction, tags) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (entry_type, source, title, content, ticker, action, conviction, tags)
    )
    conn.commit()
    entry_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.close()
    console.log(f"[magenta]Riker's Log: {entry_type} from {source} — {(title or content)[:60]}")
    return {"id": entry_id, "ok": True}


def get_entries(limit: int = 50, entry_type: str = None, source: str = None) -> list:
    """Get log entries, optionally filtered."""
    conn = _conn()
    query = "SELECT * FROM rikers_log"
    params = []
    conditions = []
    if entry_type:
        conditions.append("entry_type = ?")
        params.append(entry_type)
    if source:
        conditions.append("source = ?")
        params.append(source)
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_outcome(entry_id: int, outcome: str, outcome_pnl: float = None) -> dict:
    """Update an entry with its trade outcome."""
    conn = _conn()
    conn.execute(
        "UPDATE rikers_log SET outcome = ?, outcome_pnl = ? WHERE id = ?",
        (outcome, outcome_pnl, entry_id)
    )
    conn.commit()
    conn.close()
    return {"ok": True, "id": entry_id}


def sync_spock_briefings():
    """Auto-sync today's Spock CTO briefings into Riker's Log.

    Only syncs briefings not already in the log (checks by created_at timestamp).
    """
    try:
        from engine.cto_advisor import get_todays_briefings
        briefings = get_todays_briefings()
        if not briefings:
            return 0

        conn = _conn()
        synced = 0
        for b in briefings:
            ts = b.get("created_at", "")
            # Check if already synced
            existing = conn.execute(
                "SELECT id FROM rikers_log WHERE source='spock' AND entry_type='spock_briefing' "
                "AND created_at = ?", (ts,)
            ).fetchone()
            if existing:
                continue

            briefing_text = b.get("briefing", "")
            regime = b.get("regime", "")
            flow = b.get("flow_lean", "")
            btype = b.get("briefing_type", "")
            title = f"Spock's {btype.replace('_', ' ').title()} Briefing"

            conn.execute(
                "INSERT INTO rikers_log (entry_type, source, title, content, tags, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("spock_briefing", "spock", title, briefing_text,
                 f"regime:{regime},flow:{flow}", ts)
            )
            synced += 1

        conn.commit()
        conn.close()
        if synced:
            console.log(f"[magenta]Riker's Log: synced {synced} Spock briefing(s)")
        return synced
    except Exception as e:
        console.log(f"[red]Riker's Log sync error: {e}")
        return 0


def get_stats() -> dict:
    """Get log statistics."""
    conn = _conn()
    total = conn.execute("SELECT COUNT(*) FROM rikers_log").fetchone()[0]
    by_source = {}
    for row in conn.execute("SELECT source, COUNT(*) as cnt FROM rikers_log GROUP BY source").fetchall():
        by_source[row["source"]] = row["cnt"]
    by_type = {}
    for row in conn.execute("SELECT entry_type, COUNT(*) as cnt FROM rikers_log GROUP BY entry_type").fetchall():
        by_type[row["entry_type"]] = row["cnt"]
    # Win/loss on entries with outcomes
    outcomes = conn.execute(
        "SELECT COUNT(*) as total, "
        "SUM(CASE WHEN outcome_pnl > 0 THEN 1 ELSE 0 END) as wins, "
        "SUM(CASE WHEN outcome_pnl <= 0 THEN 1 ELSE 0 END) as losses, "
        "AVG(outcome_pnl) as avg_pnl "
        "FROM rikers_log WHERE outcome_pnl IS NOT NULL"
    ).fetchone()
    conn.close()
    return {
        "total_entries": total,
        "by_source": by_source,
        "by_type": by_type,
        "outcomes": {
            "total": outcomes["total"] or 0,
            "wins": outcomes["wins"] or 0,
            "losses": outcomes["losses"] or 0,
            "avg_pnl": round(outcomes["avg_pnl"] or 0, 2),
        }
    }
