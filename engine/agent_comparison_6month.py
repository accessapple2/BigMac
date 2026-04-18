#!/usr/bin/env python3
"""
Per-Agent 6-Month Comparison
Shows how each S6 agent performed individually over 180 days.
Run: venv/bin/python3 engine/agent_comparison_6month.py
"""
import sqlite3
import statistics
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

_root = Path(__file__).parent.parent
DB_PATH = _root / "data" / "trader.db"
BACKTEST_DAYS   = 180
INITIAL_CAPITAL = 100_000


def _conn():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def _agent_metrics(player_id: str, days: int) -> Dict:
    conn  = _conn()
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    rows = conn.execute(
        """SELECT realized_pnl, executed_at, symbol
           FROM trades
           WHERE player_id = ?
             AND executed_at >= ?
             AND exit_price IS NOT NULL
             AND realized_pnl IS NOT NULL
           ORDER BY executed_at""",
        (player_id, since),
    ).fetchall()
    conn.close()

    if not rows:
        return {
            "agent": player_id, "trades": 0, "wins": 0, "losses": 0,
            "win_rate": 0.0, "total_pnl": 0.0, "avg_pnl": 0.0,
            "sharpe": 0.0, "max_dd": 0.0, "return_pct": 0.0,
        }

    pnls     = [float(r["realized_pnl"]) for r in rows]
    wins     = sum(1 for p in pnls if p > 0)
    losses   = sum(1 for p in pnls if p < 0)
    total    = sum(pnls)
    win_rate = wins / len(pnls) * 100

    # Sharpe
    if len(pnls) > 1:
        rets   = [p / INITIAL_CAPITAL for p in pnls]
        mu     = statistics.mean(rets)
        std    = statistics.stdev(rets)
        sharpe = (mu / std) * (252 ** 0.5) if std > 0 else 0.0
    else:
        sharpe = 0.0

    # Max drawdown
    cum, peak, max_dd = 0.0, 0.0, 0.0
    for p in pnls:
        cum += p
        peak = max(peak, cum)
        dd = (peak - cum) / INITIAL_CAPITAL * 100 if peak > 0 else 0.0
        max_dd = max(max_dd, dd)

    return {
        "agent":      player_id,
        "trades":     len(pnls),
        "wins":       wins,
        "losses":     losses,
        "win_rate":   round(win_rate, 1),
        "total_pnl":  round(total, 2),
        "return_pct": round(total / INITIAL_CAPITAL * 100, 2),
        "avg_pnl":    round(total / len(pnls), 2),
        "sharpe":     round(sharpe, 3),
        "max_dd":     round(max_dd, 2),
    }


def _all_active_agents(days: int) -> List[str]:
    """Return all player_ids that have trades in the period."""
    conn  = _conn()
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows  = conn.execute(
        "SELECT DISTINCT player_id FROM trades WHERE executed_at >= ?", (since,)
    ).fetchall()
    conn.close()
    return [r["player_id"] for r in rows]


def run_agent_comparison():
    print("\n" + "=" * 80)
    print("👥 PER-AGENT 6-MONTH COMPARISON")
    print("=" * 80)
    print(f"Period : {BACKTEST_DAYS} days | Capital : ${INITIAL_CAPITAL:,}")
    print("=" * 80)

    agents  = _all_active_agents(BACKTEST_DAYS)
    results = [_agent_metrics(a, BACKTEST_DAYS) for a in agents]
    results.sort(key=lambda x: x["total_pnl"], reverse=True)

    # Table
    print(f"\n{'Agent':<20} | {'Trades':>7} | {'Wins':>5} | {'Loss':>5} | "
          f"{'WR':>6} | {'Return':>8} | {'P&L':>12} | {'Sharpe':>7} | {'MaxDD':>7}")
    print("-" * 95)

    for r in results:
        if r["trades"] == 0:
            continue
        pnl_str = f"${r['total_pnl']:>+,.2f}"
        ret_str = f"{r['return_pct']:>+.2f}%"
        print(
            f"{r['agent']:<20} | {r['trades']:>7,} | {r['wins']:>5} | {r['losses']:>5} | "
            f"{r['win_rate']:>5.1f}% | {ret_str:>8} | {pnl_str:>12} | "
            f"{r['sharpe']:>7.3f} | {r['max_dd']:>6.2f}%"
        )

    inactive = [r for r in results if r["trades"] == 0]

    print("=" * 95)

    # Summary
    total_pnl_all  = sum(r["total_pnl"] for r in results)
    total_trade_all = sum(r["trades"] for r in results)
    active = [r for r in results if r["trades"] > 0]

    print(f"\n📊 FLEET SUMMARY ({BACKTEST_DAYS}d):")
    print(f"   Active agents : {len(active)}")
    print(f"   Total trades  : {total_trade_all:,}")
    print(f"   Total P&L     : ${total_pnl_all:+,.2f}")
    print(f"   Fleet return  : {total_pnl_all / INITIAL_CAPITAL * 100:+.2f}%")

    if active:
        top = active[0]
        print(f"\n🏆 TOP PERFORMER : {top['agent']} — ${top['total_pnl']:+,.2f} | WR: {top['win_rate']:.1f}%")

        bottom = [r for r in active if r["total_pnl"] < 0]
        if bottom:
            print(f"\n⚠️  UNDERPERFORMERS:")
            for r in bottom:
                print(f"   {r['agent']:<20} ${r['total_pnl']:>+,.2f} | WR: {r['win_rate']:.1f}% | {r['trades']} trades")

    if inactive:
        print(f"\n💤 INACTIVE (0 trades in {BACKTEST_DAYS}d):")
        for r in inactive:
            print(f"   {r['agent']}")

    return results


if __name__ == "__main__":
    run_agent_comparison()
