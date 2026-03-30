"""Season Manager — handles season rotation, history, and resets.

Seasons rotate automatically every Sunday at 11:59 PM MST.
All historical data is preserved forever — never delete trades/signals.
Steve's Webull portfolio (is_human=1) is never reset.
"""
from __future__ import annotations
import sqlite3
from datetime import datetime
from rich.console import Console
from shared.matrix_bridge import NEO_PLAYER_ID

console = Console()
DB = "data/trader.db"

DEFAULT_CASH = 7000.0
DAYBLADE_CASH = 3500.0


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def ensure_tables():
    """Create season_history table if it doesn't exist."""
    conn = _conn()
    conn.execute("""CREATE TABLE IF NOT EXISTS season_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        season INTEGER NOT NULL,
        player_id TEXT NOT NULL,
        display_name TEXT,
        final_value REAL,
        total_return_pct REAL,
        total_trades INTEGER DEFAULT 0,
        win_rate REAL DEFAULT 0,
        ended_at TEXT
    )""")
    conn.commit()
    conn.close()


def get_current_season() -> int:
    """Get current season number from settings."""
    conn = _conn()
    row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
    conn.close()
    return int(row[0]) if row else 1


def save_season_summary(season: int):
    """Save final leaderboard standings for a completed season."""
    ensure_tables()
    conn = _conn()

    # Check if already saved
    existing = conn.execute(
        "SELECT 1 FROM season_history WHERE season=?", (season,)
    ).fetchone()
    if existing:
        conn.close()
        return

    players = conn.execute(
        "SELECT id, display_name, cash FROM ai_players WHERE is_active=1"
    ).fetchall()

    for p in players:
        pid = p["id"]
        cash = p["cash"]

        # Calculate total value from positions
        positions = conn.execute(
            "SELECT symbol, qty, avg_price FROM positions WHERE player_id=?",
            (pid,)
        ).fetchall()
        positions_value = sum(r["qty"] * r["avg_price"] for r in positions)
        total_value = cash + positions_value

        # Get trade stats for this season
        stats = conn.execute(
            "SELECT COUNT(*) as total, "
            "SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as wins "
            "FROM trades WHERE player_id=? AND season=?",
            (pid, season)
        ).fetchone()
        total_trades = stats["total"] or 0
        wins = stats["wins"] or 0
        win_rate = round(wins / total_trades * 100, 1) if total_trades > 0 else 0

        # Determine starting cash for return calculation
        if pid == "dayblade-0dte":
            starting = DAYBLADE_CASH
        elif pid == "steve-webull":
            starting = 7021.81
        else:
            starting = DEFAULT_CASH
        return_pct = round((total_value - starting) / starting * 100, 2) if starting > 0 else 0

        conn.execute(
            "INSERT INTO season_history "
            "(season, player_id, display_name, final_value, total_return_pct, "
            "total_trades, win_rate, ended_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (season, pid, p["display_name"], round(total_value, 2),
             return_pct, total_trades, win_rate, datetime.now().isoformat())
        )

    conn.commit()
    conn.close()
    console.log(f"[bold green]Season {season} summary saved ({len(players)} players)")


def rotate_season() -> int:
    """Rotate to a new season. Returns the new season number."""
    ensure_tables()
    current = get_current_season()
    new_season = current + 1

    # Save summary of ending season
    save_season_summary(current)

    conn = _conn()

    # Update season number
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES ('current_season', ?)",
        (str(new_season),)
    )
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
        (f"season_{new_season}_start", datetime.now().isoformat())
    )

    # Reset AI player cash — NOT human players, NOT Steve
    conn.execute(
        "UPDATE ai_players SET cash=?, season=? WHERE id != 'steve-webull' AND id != 'dayblade-0dte' AND id != ?",
        (DEFAULT_CASH, new_season, NEO_PLAYER_ID)
    )
    conn.execute(
        "UPDATE ai_players SET cash=?, season=? WHERE id='dayblade-0dte'",
        (DAYBLADE_CASH, new_season)
    )
    # Steve keeps his portfolio but gets season tag updated
    conn.execute(
        "UPDATE ai_players SET season=? WHERE id='steve-webull'",
        (new_season,)
    )

    # Unhalt all AI players for the new season
    conn.execute(
        "UPDATE ai_players SET is_halted=0, halt_reason=NULL WHERE id != 'steve-webull' AND id != ?",
        (NEO_PLAYER_ID,)
    )

    # Close all AI positions (not Steve's)
    conn.execute(
        "DELETE FROM positions WHERE player_id != 'steve-webull' AND player_id != ?",
        (NEO_PLAYER_ID,)
    )

    conn.commit()
    conn.close()

    # Post to War Room
    try:
        from engine.war_room import save_hot_take
        save_hot_take(
            "steve-webull", "SEASON",
            f"⭐ ADMIRAL PICARD: Season {new_season} has begun. "
            f"Final standings for Season {current} are locked. "
            f"All crew reset to starting positions. "
            f"Captain Kirk's portfolio carries forward as the human benchmark. "
            f"Engage."
        )
    except Exception as e:
        console.log(f"[red]Season rotation War Room post failed: {e}")

    console.log(f"[bold green]SEASON ROTATION: Season {current} → Season {new_season}")
    return new_season


def get_season_history() -> list:
    """Get all season summaries with winners."""
    ensure_tables()
    conn = _conn()
    current = get_current_season()

    seasons = []
    # Get all unique seasons from history
    season_nums = conn.execute(
        "SELECT DISTINCT season FROM season_history ORDER BY season DESC"
    ).fetchall()

    for row in season_nums:
        s = row["season"]
        # Get all players for this season, ordered by return
        players = conn.execute(
            "SELECT * FROM season_history WHERE season=? ORDER BY total_return_pct DESC",
            (s,)
        ).fetchall()
        players_list = [dict(p) for p in players]
        winner = players_list[0] if players_list else None
        seasons.append({
            "season": s,
            "winner": winner,
            "players": players_list,
            "ended_at": winner["ended_at"] if winner else None,
        })

    # Add current season as "LIVE"
    seasons.insert(0, {
        "season": current,
        "winner": None,
        "players": [],
        "ended_at": None,
        "live": True,
    })

    conn.close()
    return seasons


def start_season(season_num: int):
    """Directly start a specific season number (for manual season launches)."""
    current = get_current_season()
    if season_num <= current:
        return {"error": f"Season {season_num} is not greater than current season {current}"}

    # Save current season summary
    save_season_summary(current)

    conn = _conn()

    # Set new season
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES ('current_season', ?)",
        (str(season_num),)
    )
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
        (f"season_{season_num}_start", datetime.now().isoformat())
    )

    # Reset AI player cash
    conn.execute(
        "UPDATE ai_players SET cash=?, season=? WHERE id != 'steve-webull' AND id != 'dayblade-0dte' AND id != ?",
        (DEFAULT_CASH, season_num, NEO_PLAYER_ID)
    )
    conn.execute(
        "UPDATE ai_players SET cash=?, season=? WHERE id='dayblade-0dte'",
        (DAYBLADE_CASH, season_num)
    )
    conn.execute("UPDATE ai_players SET season=? WHERE id='steve-webull'", (season_num,))
    conn.execute(
        "UPDATE ai_players SET is_halted=0, halt_reason=NULL WHERE id != 'steve-webull' AND id != ?",
        (NEO_PLAYER_ID,),
    )
    conn.execute("DELETE FROM positions WHERE player_id != 'steve-webull' AND player_id != ?", (NEO_PLAYER_ID,))

    conn.commit()
    conn.close()

    # Post announcement
    try:
        from engine.war_room import save_hot_take
        save_hot_take(
            "steve-webull", "SEASON",
            f"⭐ ADMIRAL PICARD: Season {season_num} has begun. "
            f"All crew reset to starting positions. "
            f"Captain Kirk's portfolio carries forward as the human benchmark. "
            f"Make it so."
        )
    except Exception:
        pass

    console.log(f"[bold green]Season {season_num} started manually")
    return {"ok": True, "season": season_num}
