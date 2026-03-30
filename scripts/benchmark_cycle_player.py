#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sqlite3
from datetime import datetime


DB_PATH = os.environ.get("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))
DEFAULT_STARTING_CASH = 7000.0
SPECIAL_STARTING_CASH = {
    "dayblade-0dte": 3500.0,
    "steve-webull": 7021.81,
    "super-agent": 100000.0,
}


def _db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def ensure_schema(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS player_benchmark_cycles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id TEXT NOT NULL REFERENCES ai_players(id),
            label TEXT NOT NULL,
            benchmark_cycle_start TEXT NOT NULL,
            benchmark_start_cash REAL NOT NULL,
            benchmark_start_equity REAL NOT NULL,
            season INTEGER DEFAULT 1,
            notes TEXT DEFAULT '',
            is_active INTEGER DEFAULT 1
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_player_benchmark_cycles_player
        ON player_benchmark_cycles(player_id, benchmark_cycle_start DESC)
        """
    )


def current_equity(conn, player_id: str) -> tuple[float, float, float, int]:
    player = conn.execute(
        "SELECT cash FROM ai_players WHERE id=?",
        (player_id,),
    ).fetchone()
    cash = float(player["cash"] or 0.0) if player else 0.0
    row = conn.execute(
        "SELECT COALESCE(SUM(qty * avg_price), 0) AS positions_value, COUNT(*) AS open_positions "
        "FROM positions WHERE player_id=?",
        (player_id,),
    ).fetchone()
    positions_value = float(row["positions_value"] or 0.0)
    open_positions = int(row["open_positions"] or 0)
    return cash, positions_value, round(cash + positions_value, 2), open_positions


def total_funded_capital(conn, player_id: str) -> float:
    base = SPECIAL_STARTING_CASH.get(player_id, DEFAULT_STARTING_CASH)
    row = conn.execute(
        "SELECT COALESCE(SUM(amount), 0) AS total FROM player_funding_events WHERE player_id=?",
        (player_id,),
    ).fetchone()
    return round(base + float(row["total"] or 0.0), 2)


def active_cycle(conn, player_id: str):
    return conn.execute(
        """
        SELECT *
        FROM player_benchmark_cycles
        WHERE player_id=? AND COALESCE(is_active, 1)=1
        ORDER BY benchmark_cycle_start DESC, id DESC
        LIMIT 1
        """,
        (player_id,),
    ).fetchone()


def start_cycle(player_id: str, label: str | None = None, notes: str = "") -> dict:
    conn = _db()
    try:
        ensure_schema(conn)
        player = conn.execute(
            "SELECT id, display_name, season FROM ai_players WHERE id=?",
            (player_id,),
        ).fetchone()
        if not player:
            raise SystemExit(f"Player not found: {player_id}")

        cash, positions_value, equity, open_positions = current_equity(conn, player_id)
        now = datetime.now().isoformat(timespec="seconds")
        cycle_label = label or f"benchmark-cycle-{now[:10]}"
        season = int(player["season"] or 1)

        conn.execute(
            "UPDATE player_benchmark_cycles SET is_active=0 WHERE player_id=? AND COALESCE(is_active,1)=1",
            (player_id,),
        )
        conn.execute(
            """
            INSERT INTO player_benchmark_cycles
            (player_id, label, benchmark_cycle_start, benchmark_start_cash,
             benchmark_start_equity, season, notes, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (player_id, cycle_label, now, cash, equity, season, notes),
        )
        conn.commit()
        return {
            "player_id": player_id,
            "display_name": player["display_name"],
            "label": cycle_label,
            "benchmark_cycle_start": now,
            "benchmark_start_cash": round(cash, 2),
            "benchmark_start_equity": round(equity, 2),
            "benchmark_start_positions_value": round(positions_value, 2),
            "open_positions": open_positions,
            "season": season,
            "benchmark_clean": open_positions == 0,
        }
    finally:
        conn.close()


def benchmark_report(player_id: str) -> dict:
    conn = _db()
    try:
        ensure_schema(conn)
        player = conn.execute(
            "SELECT id, display_name, season FROM ai_players WHERE id=?",
            (player_id,),
        ).fetchone()
        if not player:
            raise SystemExit(f"Player not found: {player_id}")

        cash, positions_value, equity, open_positions = current_equity(conn, player_id)
        funded = total_funded_capital(conn, player_id)
        lifetime_pnl = round(equity - funded, 2)
        lifetime_return_pct = round((lifetime_pnl / funded) * 100, 2) if funded > 0 else 0.0

        cycle = active_cycle(conn, player_id)
        cycle_data = None
        if cycle:
            start_equity = float(cycle["benchmark_start_equity"] or 0.0)
            cycle_pnl = round(equity - start_equity, 2)
            cycle_return_pct = round((cycle_pnl / start_equity) * 100, 2) if start_equity > 0 else 0.0
            cycle_data = {
                "label": cycle["label"],
                "benchmark_cycle_start": cycle["benchmark_cycle_start"],
                "benchmark_start_cash": round(float(cycle["benchmark_start_cash"] or 0.0), 2),
                "benchmark_start_equity": round(start_equity, 2),
                "benchmark_pnl": cycle_pnl,
                "benchmark_return_pct": cycle_return_pct,
                "season": int(cycle["season"] or 1),
            }

        return {
            "player_id": player_id,
            "display_name": player["display_name"],
            "current_cash": round(cash, 2),
            "current_positions_value": round(positions_value, 2),
            "current_equity": round(equity, 2),
            "open_positions": open_positions,
            "benchmark_clean": open_positions == 0,
            "lifetime_funded_capital": funded,
            "lifetime_pnl": lifetime_pnl,
            "lifetime_return_pct": lifetime_return_pct,
            "cycle": cycle_data,
        }
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Start or report a player's benchmark cycle.")
    sub = parser.add_subparsers(dest="command", required=True)

    start_p = sub.add_parser("start", help="Start a new benchmark cycle.")
    start_p.add_argument("player_id")
    start_p.add_argument("--label", default=None)
    start_p.add_argument("--notes", default="Benchmark-clean restart after legacy position cleanup.")

    report_p = sub.add_parser("report", help="Show current benchmark-cycle report.")
    report_p.add_argument("player_id")

    args = parser.parse_args()
    if args.command == "start":
        result = start_cycle(args.player_id, label=args.label, notes=args.notes)
        for key, value in result.items():
            print(f"{key}={value}")
        return

    report = benchmark_report(args.player_id)
    print(f"player_id={report['player_id']}")
    print(f"display_name={report['display_name']}")
    print(f"benchmark_clean={report['benchmark_clean']}")
    print(f"open_positions={report['open_positions']}")
    print(f"current_cash={report['current_cash']}")
    print(f"current_equity={report['current_equity']}")
    print(f"lifetime_funded_capital={report['lifetime_funded_capital']}")
    print(f"lifetime_pnl={report['lifetime_pnl']}")
    print(f"lifetime_return_pct={report['lifetime_return_pct']}")
    if report["cycle"]:
        cycle = report["cycle"]
        for key, value in cycle.items():
            print(f"{key}={value}")


if __name__ == "__main__":
    main()
