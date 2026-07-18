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
from engine.trades_filter import CLEAN_TRADES_WHERE

console = Console()
DB = "data/trader.db"

DEFAULT_CASH = 7000.0
DAYBLADE_CASH = 3500.0

# HM-SEASON-ROTATION-BLANKET-REACTIVATE (2026-07-18): the halt_mode reset
# below must never touch an agent with an explicit halt_reason on file
# (retired, HM-item halts, roster-cap exclusions, bakeoff/audit-trail-only
# clones, exit_only, etc.) — only agents with halt_reason IS NULL are
# eligible, i.e. agents that were never deliberately halted with a reason.
# Verified 2026-07-18 against live data: 100% clean partition (every
# currently-active agent has halt_reason IS NULL; every currently-halted
# agent has halt_reason set) — this predicate currently matches exactly
# the already-active set, making the reset a no-op under normal operation.
# Belt+suspenders: if the dry-run count of eligible rows ever exceeds the
# current active count by more than this margin, rotation aborts before
# any write — a season rotation should never multiply the active fleet.
ROTATION_REACTIVATION_MARGIN = 10


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
            "FROM trades WHERE player_id=? AND season=? "
            # HM-TRACKING-AGGREGATOR: query had NO realized_pnl gate (counted every row
            # incl BUY). Added realized_pnl IS NOT NULL (BUY-row exclusion) separately
            # from the clean-trades boundary.
            "AND realized_pnl IS NOT NULL "
            f"AND {CLEAN_TRADES_WHERE}",
            (pid, season)
        ).fetchone()
        total_trades = stats["total"] or 0
        wins = stats["wins"] or 0
        win_rate = round(wins / total_trades * 100, 1) if total_trades > 0 else 0

        # Determine starting cash for return calculation
        if pid == "dayblade-0dte":
            starting = DAYBLADE_CASH
        elif pid == "webull":
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


# Rows eligible for the season-reset unhalt — never touches a row with an
# explicit halt_reason on file. See ROTATION_REACTIVATION_MARGIN docstring.
_UNHALT_ELIGIBLE_WHERE = (
    "id NOT IN ('webull','alpaca-mirror') AND id != ? AND halt_reason IS NULL"
)


def _dry_run_unhalt_scope(conn) -> dict:
    """Read-only: count agents currently active vs. agents the season-reset
    UPDATE would touch under _UNHALT_ELIGIBLE_WHERE. No writes.

    Returns {"active_before": int, "would_affect": int, "safe": bool}.
    "safe" is False if would_affect exceeds active_before by more than
    ROTATION_REACTIVATION_MARGIN — the caller must abort before writing.
    """
    active_before = conn.execute(
        "SELECT COUNT(*) FROM ai_players "
        "WHERE id NOT IN ('webull','alpaca-mirror') AND id != ? AND halt_mode='active'",
        (NEO_PLAYER_ID,)
    ).fetchone()[0]
    would_affect = conn.execute(
        f"SELECT COUNT(*) FROM ai_players WHERE {_UNHALT_ELIGIBLE_WHERE}",
        (NEO_PLAYER_ID,)
    ).fetchone()[0]
    return {
        "active_before": active_before,
        "would_affect": would_affect,
        "safe": would_affect <= active_before + ROTATION_REACTIVATION_MARGIN,
    }


def _alert_rotation_aborted(scope: dict, season: int) -> None:
    try:
        from engine.alert_channels import send_alert, AlertLevel
        send_alert(
            message=(
                f"Season rotation ABORTED before Season {season}: the halt_mode "
                f"reset would have touched {scope['would_affect']} agents against "
                f"{scope['active_before']} currently active "
                f"(margin={ROTATION_REACTIVATION_MARGIN}). No DB writes were made. "
                f"A season rotation should never multiply the active fleet — "
                f"investigate engine/season_manager.py before the next attempt."
            ),
            level=AlertLevel.RED_ALERT,
            alert_type="hm-season-rotation-aborted",
            rate_limit_secs=3600,
        )
    except Exception as e:
        console.log(f"[red]Season rotation abort-NTFY failed: {e}")


def rotate_season() -> int | None:
    """Rotate to a new season. Returns the new season number, or None if
    the rotation was aborted by the reactivation-scope safety check (no
    writes made in that case — safe to retry once investigated)."""
    ensure_tables()
    current = get_current_season()
    new_season = current + 1

    # HM-SEASON-ROTATION-BLANKET-REACTIVATE: dry-run the unhalt scope BEFORE
    # any write (including save_season_summary) so an abort leaves the DB
    # completely untouched — no rollback needed, trivially safe to retry.
    check_conn = _conn()
    scope = _dry_run_unhalt_scope(check_conn)
    check_conn.close()
    if not scope["safe"]:
        console.log(
            f"[bold red]SEASON ROTATION ABORTED — reactivation scope check failed: "
            f"would_affect={scope['would_affect']} active_before={scope['active_before']} "
            f"margin={ROTATION_REACTIVATION_MARGIN}[/bold red]"
        )
        _alert_rotation_aborted(scope, new_season)
        return None

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

    # Reset AI player cash — NOT human players, NOT Steve, NOT broker mirror
    # HM-I-β-Item3 (2026-05-05): exclude alpaca-mirror — its cash is sync'd
    # from Alpaca every 5 min by alpaca_portfolio_sync, not season-managed.
    conn.execute(
        "UPDATE ai_players SET cash=?, season=? WHERE id NOT IN ('webull','alpaca-mirror','dayblade-0dte') AND id != ?",
        (DEFAULT_CASH, new_season, NEO_PLAYER_ID)
    )
    conn.execute(
        "UPDATE ai_players SET cash=?, season=? WHERE id='dayblade-0dte'",
        (DAYBLADE_CASH, new_season)
    )
    # Steve and the broker mirror keep their portfolios but get season tag updated
    conn.execute(
        "UPDATE ai_players SET season=? WHERE id IN ('webull','alpaca-mirror')",
        (new_season,)
    )

    # Unhalt AI players for the new season — HM-SEASON-ROTATION-BLANKET-
    # REACTIVATE (2026-07-18): scoped to halt_reason IS NULL only. Any agent
    # with an explicit halt_reason (retired, HM-item halts, roster-cap
    # exclusions, bakeoff clones, exit_only, etc.) is never touched here.
    # HM-B-pre: migrated is_halted=0 → halt_mode='active' (drop halt_reason + halted_at on season reset)
    cur = conn.execute(
        f"UPDATE ai_players SET halt_mode='active', halt_reason=NULL, halted_at=NULL WHERE {_UNHALT_ELIGIBLE_WHERE}",
        (NEO_PLAYER_ID,)
    )
    console.log(f"[cyan]Season rotation unhalt: {cur.rowcount} rows touched (scope check: {scope})")

    # Close all AI positions (not Steve's, not broker mirror's — alpaca_sync rebuilds it)
    conn.execute(
        "DELETE FROM positions WHERE player_id NOT IN ('webull','alpaca-mirror') AND player_id != ?",
        (NEO_PLAYER_ID,)
    )

    conn.commit()
    conn.close()

    # Post to War Room
    try:
        from engine.war_room import save_hot_take
        save_hot_take(
            "webull", "SEASON",
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

    # HM-SEASON-ROTATION-BLANKET-REACTIVATE: same dry-run safety check as
    # rotate_season() — abort before any write if the unhalt scope looks
    # like it would multiply the active fleet instead of a no-op refresh.
    check_conn = _conn()
    scope = _dry_run_unhalt_scope(check_conn)
    check_conn.close()
    if not scope["safe"]:
        console.log(
            f"[bold red]SEASON START ABORTED — reactivation scope check failed: "
            f"would_affect={scope['would_affect']} active_before={scope['active_before']} "
            f"margin={ROTATION_REACTIVATION_MARGIN}[/bold red]"
        )
        _alert_rotation_aborted(scope, season_num)
        return {"error": "aborted by reactivation-scope safety check", "scope": scope}

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
    # HM-I-β-Item3 (2026-05-05): exclude alpaca-mirror (broker-sync target).
    conn.execute(
        "UPDATE ai_players SET cash=?, season=? WHERE id NOT IN ('webull','alpaca-mirror','dayblade-0dte') AND id != ?",
        (DEFAULT_CASH, season_num, NEO_PLAYER_ID)
    )
    conn.execute(
        "UPDATE ai_players SET cash=?, season=? WHERE id='dayblade-0dte'",
        (DAYBLADE_CASH, season_num)
    )
    conn.execute("UPDATE ai_players SET season=? WHERE id IN ('webull','alpaca-mirror')", (season_num,))
    # Unhalt AI players — HM-SEASON-ROTATION-BLANKET-REACTIVATE (2026-07-18):
    # scoped to halt_reason IS NULL only, same as rotate_season(). Never
    # touches an agent with an explicit halt_reason on file.
    # HM-B-pre: migrated is_halted=0 → halt_mode='active' (drop halt_reason + halted_at on season reset)
    cur = conn.execute(
        f"UPDATE ai_players SET halt_mode='active', halt_reason=NULL, halted_at=NULL WHERE {_UNHALT_ELIGIBLE_WHERE}",
        (NEO_PLAYER_ID,),
    )
    console.log(f"[cyan]Season start unhalt: {cur.rowcount} rows touched (scope check: {scope})")
    conn.execute("DELETE FROM positions WHERE player_id NOT IN ('webull','alpaca-mirror') AND player_id != ?", (NEO_PLAYER_ID,))

    conn.commit()
    conn.close()

    # Post announcement
    try:
        from engine.war_room import save_hot_take
        save_hot_take(
            "webull", "SEASON",
            f"⭐ ADMIRAL PICARD: Season {season_num} has begun. "
            f"All crew reset to starting positions. "
            f"Captain Kirk's portfolio carries forward as the human benchmark. "
            f"Make it so."
        )
    except Exception:
        pass

    console.log(f"[bold green]Season {season_num} started manually")
    return {"ok": True, "season": season_num}
