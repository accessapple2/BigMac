#!/usr/bin/env python3
"""
Regime-Aware Analysis
Splits trade performance by market regime, joining trades with regime_history on date.
regime_history schema: date TEXT, regime TEXT, spy_close, ma_8, ma_21, vix (via health).
"""
from __future__ import annotations

import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "trader.db"

REGIMES = ["BULL", "CAUTIOUS", "BEAR", "CRISIS", "NEUTRAL"]


def get_trades_by_regime(agent_id: str | None = None, days: int = 90) -> dict[str, list[dict]]:
    """
    Join trades with regime_history on date to produce per-regime trade stats.
    Groups by (regime, player_id).
    """
    conn        = sqlite3.connect(DB_PATH)
    date_filter = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    query = """
        SELECT
            COALESCE(rh.regime, 'UNKNOWN')                              AS regime,
            t.player_id,
            COUNT(*)                                                     AS trades,
            SUM(CASE WHEN t.realized_pnl > 0 THEN 1 ELSE 0 END)        AS wins,
            SUM(CASE WHEN t.realized_pnl < 0 THEN 1 ELSE 0 END)        AS losses,
            SUM(t.realized_pnl)                                          AS total_pnl,
            AVG(t.realized_pnl)                                          AS avg_pnl
        FROM trades t
        LEFT JOIN regime_history rh
               ON date(t.executed_at) = rh.date
        WHERE t.executed_at >= ?
          AND t.realized_pnl IS NOT NULL
    """
    params: list = [date_filter]

    if agent_id:
        query += " AND t.player_id = ?"
        params.append(agent_id)

    query += " GROUP BY rh.regime, t.player_id"

    rows = conn.execute(query, params).fetchall()
    conn.close()

    results: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        regime, player_id, trades, wins, losses, total_pnl, avg_pnl = row
        wins   = wins   or 0
        losses = losses or 0
        win_rate = (wins / trades * 100) if trades > 0 else 0.0

        results[regime].append({
            "agent":     player_id,
            "trades":    trades,
            "wins":      wins,
            "losses":    losses,
            "win_rate":  round(win_rate, 1),
            "total_pnl": round(total_pnl or 0, 2),
            "avg_pnl":   round(avg_pnl   or 0, 2),
        })

    return dict(results)


def get_regime_summary(days: int = 90) -> dict[str, dict]:
    """Aggregate across agents per regime."""
    by_regime = get_trades_by_regime(days=days)

    summary: dict[str, dict] = {}
    for regime in REGIMES:
        data = by_regime.get(regime, [])
        total  = sum(d["trades"] for d in data)
        wins   = sum(d["wins"]   for d in data)
        losses = sum(d["losses"] for d in data)
        pnl    = sum(d["total_pnl"] for d in data)
        wr     = (wins / total * 100) if total > 0 else 0.0

        summary[regime] = {
            "total_trades": total,
            "wins":         wins,
            "losses":       losses,
            "win_rate":     round(wr, 1),
            "total_pnl":    round(pnl, 2),
            "agent_count":  len(data),
        }

    # Also include any regimes not in the canonical list (e.g. UNKNOWN)
    for regime, data in by_regime.items():
        if regime not in summary:
            total  = sum(d["trades"] for d in data)
            wins   = sum(d["wins"]   for d in data)
            losses = sum(d["losses"] for d in data)
            pnl    = sum(d["total_pnl"] for d in data)
            wr     = (wins / total * 100) if total > 0 else 0.0
            summary[regime] = {
                "total_trades": total,
                "wins":         wins,
                "losses":       losses,
                "win_rate":     round(wr, 1),
                "total_pnl":    round(pnl, 2),
                "agent_count":  len(data),
            }

    return summary


def get_best_agents_by_regime(days: int = 90) -> dict[str, dict]:
    """Find the highest-P&L agent per regime."""
    by_regime = get_trades_by_regime(days=days)
    best: dict[str, dict] = {}

    for regime, agents in by_regime.items():
        sorted_agents = sorted(agents, key=lambda x: x["total_pnl"], reverse=True)
        if sorted_agents:
            best[regime] = {
                "best_agent": sorted_agents[0]["agent"],
                "pnl":        sorted_agents[0]["total_pnl"],
                "win_rate":   sorted_agents[0]["win_rate"],
                "trades":     sorted_agents[0]["trades"],
            }

    return best


def get_regime_recommendations() -> list[dict]:
    summary     = get_regime_summary(90)
    best_agents = get_best_agents_by_regime(90)
    recs: list[dict] = []

    for regime, data in summary.items():
        if data["total_trades"] < 5:
            continue

        if data["win_rate"] < 40:
            recs.append({
                "type":    "WARNING",
                "regime":  regime,
                "message": (f"Low win rate ({data['win_rate']}%) in {regime} — "
                            f"consider reducing exposure."),
            })

        if regime in best_agents and best_agents[regime]["pnl"] > 100:
            b = best_agents[regime]
            recs.append({
                "type":    "INSIGHT",
                "regime":  regime,
                "message": (f"{b['best_agent']} excels in {regime} (+${b['pnl']:.0f}, "
                            f"WR {b['win_rate']}%). Consider increasing allocation."),
            })

    return recs


if __name__ == "__main__":
    print("\nREGIME ANALYSIS (Last 90 Days)")
    print("=" * 70)

    summary = get_regime_summary(90)
    print("\nPERFORMANCE BY REGIME:")
    for regime, data in summary.items():
        if data["total_trades"] > 0:
            sign = "+" if data["total_pnl"] >= 0 else ""
            print(f"  {regime:10} | {data['total_trades']:3} trades | "
                  f"WR {data['win_rate']:5.1f}% | P&L ${sign}{data['total_pnl']:.2f}")

    best = get_best_agents_by_regime(90)
    print("\nBEST AGENT BY REGIME:")
    for regime, data in best.items():
        print(f"  {regime:10} -> {data['best_agent']:20} (+${data['pnl']:.2f}, WR {data['win_rate']}%)")

    recs = get_regime_recommendations()
    if recs:
        print("\nRECOMMENDATIONS:")
        for rec in recs:
            icon = "WARNING" if rec["type"] == "WARNING" else "INSIGHT"
            print(f"  [{icon}] {rec['message']}")
