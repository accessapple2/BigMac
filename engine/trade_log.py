#!/usr/bin/env python3
"""
Trade-by-Trade Log
Complete trade history with entry/exit details.
Adapted to actual trades schema: player_id, qty, entry_price, exit_price,
realized_pnl, executed_at, timeframe, confidence, reasoning.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "trader.db"


def get_trade_log(
    agent_id: str | None = None,
    days: int = 30,
    limit: int = 100,
) -> list[dict]:
    """Return detailed trade log, newest first."""
    conn        = sqlite3.connect(DB_PATH)
    date_filter = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    query = """
        SELECT
            t.id,
            t.symbol,
            t.player_id,
            t.action,
            t.entry_price,
            t.exit_price,
            t.qty,
            t.realized_pnl,
            COALESCE(t.timeframe, 'SWING'),
            t.confidence,
            t.reasoning,
            t.executed_at,
            t.asset_type
        FROM trades t
        WHERE t.executed_at >= ?
    """
    params: list = [date_filter]

    if agent_id:
        query += " AND t.player_id = ?"
        params.append(agent_id)

    query += f" ORDER BY t.executed_at DESC LIMIT {int(limit)}"

    rows = conn.execute(query, params).fetchall()
    conn.close()

    trades = []
    for row in rows:
        (trade_id, symbol, player_id, action, entry, exit_p,
         qty, pnl, strategy, conf, reason, executed, asset_type) = row

        # Calculate return %
        return_pct = 0.0
        if entry and exit_p and entry > 0:
            return_pct = ((exit_p - entry) / entry) * 100
            if action in ("SELL", "SHORT"):
                return_pct = -return_pct

        outcome = "OPEN"
        if pnl is not None:
            outcome = "WIN" if pnl > 0 else ("LOSS" if pnl < 0 else "FLAT")

        trades.append({
            "id":          trade_id,
            "symbol":      symbol,
            "agent":       player_id,
            "action":      action,
            "entry_price": round(entry,  2) if entry  else None,
            "exit_price":  round(exit_p, 2) if exit_p else None,
            "quantity":    qty,
            "pnl":         round(pnl, 2) if pnl is not None else 0,
            "return_pct":  round(return_pct, 2),
            "strategy":    strategy,
            "confidence":  conf,
            "reasoning":   (reason[:200] if reason else None),
            "executed_at": executed,
            "asset_type":  asset_type,
            "outcome":     outcome,
        })

    return trades


def get_trade_summary(days: int = 30) -> dict:
    """Return aggregate trade statistics."""
    conn        = sqlite3.connect(DB_PATH)
    date_filter = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    row = conn.execute("""
        SELECT
            COUNT(*)                                                    AS total,
            SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END)         AS wins,
            SUM(CASE WHEN realized_pnl < 0 THEN 1 ELSE 0 END)         AS losses,
            SUM(CASE WHEN exit_price IS NULL THEN 1 ELSE 0 END)        AS open_trades,
            SUM(realized_pnl)                                           AS total_pnl,
            AVG(realized_pnl)                                           AS avg_pnl,
            AVG(confidence)                                             AS avg_confidence,
            MIN(realized_pnl)                                           AS worst_trade,
            MAX(realized_pnl)                                           AS best_trade
        FROM trades
        WHERE executed_at >= ?
    """, (date_filter,)).fetchone()
    conn.close()

    total, wins, losses, open_t, total_pnl, avg_pnl, avg_conf, worst, best = row
    wins   = wins   or 0
    losses = losses or 0
    win_rate = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0.0

    return {
        "total_trades":   total   or 0,
        "wins":           wins,
        "losses":         losses,
        "open_trades":    open_t  or 0,
        "win_rate":       round(win_rate, 1),
        "total_pnl":      round(total_pnl  or 0, 2),
        "avg_pnl":        round(avg_pnl    or 0, 2),
        "avg_confidence": round(avg_conf   or 0, 1),
        "worst_trade":    round(worst or 0, 2),
        "best_trade":     round(best  or 0, 2),
    }


if __name__ == "__main__":
    print("\nTRADE LOG (Last 30 Days)")
    print("=" * 80)

    summary = get_trade_summary(30)
    print(f"\nSUMMARY:")
    print(f"  Total: {summary['total_trades']} | Wins: {summary['wins']} | Losses: {summary['losses']} | Open: {summary['open_trades']}")
    print(f"  Win Rate: {summary['win_rate']}% | Total P&L: ${summary['total_pnl']:.2f}")
    print(f"  Best: ${summary['best_trade']:.2f} | Worst: ${summary['worst_trade']:.2f}")

    trades = get_trade_log(days=7, limit=10)
    print(f"\nRECENT TRADES (Last 7 Days):")
    for t in trades:
        status = "W" if t["outcome"] == "WIN" else ("L" if t["outcome"] == "LOSS" else "O")
        print(f"  [{status}] {t['symbol']:6} | {t['action']:4} | "
              f"${t['entry_price'] or 0:.2f} -> ${t['exit_price'] or 0:.2f} | "
              f"{t['return_pct']:+.1f}% | {t['strategy']} | {t['agent']}")
