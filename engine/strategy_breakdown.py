#!/usr/bin/env python3
"""
Per-Strategy Deep Dive
Breaks down agent (player) performance by timeframe/strategy.
Adapted to actual trades schema: player_id, timeframe, qty, entry_price, exit_price, realized_pnl.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "trader.db"


def get_strategy_breakdown(agent_id: str | None = None, days: int = 30) -> list[dict]:
    """
    Group trades by (player_id, timeframe) and compute performance metrics.
    Uses 'timeframe' as the strategy proxy (SWING / SCALP / DAY / etc.).
    """
    conn        = sqlite3.connect(DB_PATH)
    date_filter = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    query = """
        SELECT
            COALESCE(timeframe, 'SWING')           AS strategy,
            player_id                               AS agent,
            COUNT(*)                                AS total_trades,
            SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN realized_pnl < 0 THEN 1 ELSE 0 END) AS losses,
            AVG(CASE WHEN realized_pnl > 0 THEN realized_pnl END) AS avg_win,
            AVG(CASE WHEN realized_pnl < 0 THEN realized_pnl END) AS avg_loss,
            SUM(realized_pnl)                       AS total_pnl,
            AVG(confidence)                         AS avg_confidence
        FROM trades
        WHERE executed_at >= ?
          AND realized_pnl IS NOT NULL
    """
    params: list = [date_filter]

    if agent_id:
        query += " AND player_id = ?"
        params.append(agent_id)

    query += " GROUP BY timeframe, player_id ORDER BY total_pnl DESC"

    rows = conn.execute(query, params).fetchall()
    conn.close()

    results = []
    for row in rows:
        strategy, agent, total, wins, losses, avg_win, avg_loss, total_pnl, avg_conf = row

        wins   = wins   or 0
        losses = losses or 0
        win_rate = (wins / total * 100) if total > 0 else 0

        gross_wins   = wins   * (avg_win  or 0)
        gross_losses = abs(losses * (avg_loss or 0))
        profit_factor = (gross_wins / gross_losses) if gross_losses > 0 else 0.0
        ev            = (total_pnl / total) if total and total > 0 else 0.0

        results.append({
            "strategy":       strategy or "SWING",
            "agent":          agent,
            "total_trades":   total,
            "wins":           wins,
            "losses":         losses,
            "win_rate":       round(win_rate, 1),
            "avg_win":        round(avg_win  or 0, 2),
            "avg_loss":       round(avg_loss or 0, 2),
            "total_pnl":      round(total_pnl or 0, 2),
            "profit_factor":  round(profit_factor, 2),
            "expected_value": round(ev, 2),
            "avg_confidence": round(avg_conf or 0, 1),
            "grade":          _grade_strategy(win_rate, profit_factor, total),
        })

    return results


def _grade_strategy(win_rate: float, profit_factor: float, trades: int) -> str:
    if trades < 5:
        return "N/A"

    score = 0
    if win_rate >= 70:       score += 3
    elif win_rate >= 55:     score += 2
    elif win_rate >= 45:     score += 1

    if profit_factor >= 2.0: score += 3
    elif profit_factor >= 1.5: score += 2
    elif profit_factor >= 1.0: score += 1

    if trades >= 20:         score += 1

    return "A" if score >= 6 else ("B" if score >= 5 else ("C" if score >= 4 else ("D" if score >= 2 else "F")))


def get_top_strategies(n: int = 10, days: int = 30, min_trades: int = 5) -> list[dict]:
    data = get_strategy_breakdown(days=days)
    data = [s for s in data if s["total_trades"] >= min_trades]
    data.sort(key=lambda x: (x["profit_factor"], x["win_rate"]), reverse=True)
    return data[:n]


def get_worst_strategies(n: int = 5, days: int = 30, min_trades: int = 3) -> list[dict]:
    data = get_strategy_breakdown(days=days)
    data = [s for s in data if s["total_trades"] >= min_trades]
    data.sort(key=lambda x: x["total_pnl"])
    return data[:n]


if __name__ == "__main__":
    print("\n📊 STRATEGY BREAKDOWN (Last 30 Days)")
    print("=" * 70)

    top = get_top_strategies(10)
    print("\nTOP STRATEGIES:")
    for s in top:
        print(f"  [{s['grade']}] {s['strategy']:8} | {s['agent']:20} | {s['total_trades']:3} trades | "
              f"WR {s['win_rate']:5.1f}% | PF {s['profit_factor']:.2f} | P&L ${s['total_pnl']:+8.2f}")

    worst = get_worst_strategies(5)
    print("\nUNDERPERFORMING STRATEGIES:")
    for s in worst:
        print(f"  [{s['grade']}] {s['strategy']:8} | {s['agent']:20} | {s['total_trades']:3} trades | "
              f"WR {s['win_rate']:5.1f}% | PF {s['profit_factor']:.2f} | P&L ${s['total_pnl']:+8.2f}")
