import argparse
import os
import sqlite3
from datetime import datetime


DB_PATH = os.environ.get("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))


def _db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def ensure_schema(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS player_funding_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id TEXT NOT NULL REFERENCES ai_players(id),
            event_type TEXT NOT NULL DEFAULT 'recapitalization',
            amount REAL NOT NULL,
            previous_cash REAL,
            new_cash REAL,
            previous_total_equity REAL,
            new_total_equity REAL,
            reason TEXT DEFAULT '',
            season INTEGER DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now'))
        )
        """
    )


def current_positions_value(conn, player_id: str) -> float:
    row = conn.execute(
        "SELECT COALESCE(SUM(qty * avg_price), 0) AS positions_value FROM positions WHERE player_id=?",
        (player_id,),
    ).fetchone()
    return float(row["positions_value"] or 0.0)


def recapitalize(player_id: str, amount: float, reason: str, resume_trading: bool) -> dict:
    conn = _db()
    try:
        ensure_schema(conn)
        player = conn.execute(
            "SELECT id, display_name, cash, COALESCE(is_paused, 0) AS is_paused, season FROM ai_players WHERE id=?",
            (player_id,),
        ).fetchone()
        if not player:
            raise SystemExit(f"Player not found: {player_id}")

        positions_value = current_positions_value(conn, player_id)
        previous_cash = float(player["cash"] or 0.0)
        new_cash = round(previous_cash + amount, 2)
        previous_total_equity = round(previous_cash + positions_value, 2)
        new_total_equity = round(new_cash + positions_value, 2)
        season = int(player["season"] or 1)
        now = datetime.now().isoformat(timespec="seconds")

        conn.execute(
            """
            INSERT INTO player_funding_events
            (player_id, event_type, amount, previous_cash, new_cash,
             previous_total_equity, new_total_equity, reason, season, created_at)
            VALUES (?, 'recapitalization', ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                player_id,
                amount,
                previous_cash,
                new_cash,
                previous_total_equity,
                new_total_equity,
                reason,
                season,
                now,
            ),
        )

        conn.execute(
            "UPDATE ai_players SET cash=?, is_paused=? WHERE id=?",
            (new_cash, 0 if resume_trading else player["is_paused"], player_id),
        )

        conn.execute(
            """
            INSERT INTO portfolio_history
            (player_id, total_value, cash, positions_value, recorded_at, season)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                player_id,
                new_total_equity,
                new_cash,
                positions_value,
                now,
                season,
            ),
        )

        conn.commit()
        return {
            "player_id": player_id,
            "display_name": player["display_name"],
            "amount": amount,
            "previous_cash": previous_cash,
            "new_cash": new_cash,
            "previous_total_equity": previous_total_equity,
            "new_total_equity": new_total_equity,
            "positions_value": positions_value,
            "season": season,
            "resumed": bool(resume_trading),
            "recorded_at": now,
        }
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Record a recapitalization event for an AI player.")
    parser.add_argument("player_id")
    parser.add_argument("amount", type=float)
    parser.add_argument("--reason", default="Managed recapitalization after drawdown; preserve lifetime history.")
    parser.add_argument("--resume-trading", action="store_true")
    args = parser.parse_args()

    result = recapitalize(args.player_id, args.amount, args.reason, args.resume_trading)
    for key, value in result.items():
        print(f"{key}={value}")


if __name__ == "__main__":
    main()
