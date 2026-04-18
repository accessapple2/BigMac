#!/usr/bin/env python3
"""
MEGA 6-MONTH BACKTEST — Full S6 Fleet Analysis
Reads 180 days of actual live trade data and computes fleet-wide metrics.
Compares against SPY over the same period.
Run: venv/bin/python3 engine/mega_backtest_6month.py
"""
import sqlite3
import json
import statistics
import requests
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any

_root = Path(__file__).parent.parent
DB_PATH  = _root / "data" / "trader.db"
NTFY_URL = "https://ntfy.sh/ollietrades-admin"

BACKTEST_DAYS    = 180
INITIAL_CAPITAL  = 100_000

# Agent groups for comparison breakdown
_S6_AGENTS = [
    "ollie-auto", "navigator", "ollama-qwen3", "ollama-plutus",
    "ollama-coder", "neo-matrix", "capitol-trades", "dayblade-sulu",
    "grok-4", "holly-scanner",
]

_S6_NEW_WIRING = {"holly-scanner", "neo-matrix"}   # re-activated / new in S6.2


def _conn():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def _get_spy_return(days: int) -> float:
    """SPY return over the period from regime_history.spy_close."""
    conn = _conn()
    try:
        cur = conn.execute(
            "SELECT spy_close FROM regime_history ORDER BY date DESC LIMIT 1"
        )
        row = cur.fetchone()
        current_spy = float(row["spy_close"]) if row else 500.0

        cur = conn.execute(
            "SELECT spy_close FROM regime_history "
            "WHERE date <= date('now', ? ) ORDER BY date DESC LIMIT 1",
            (f"-{days} days",),
        )
        row = cur.fetchone()
        start_spy = float(row["spy_close"]) if row else current_spy
        return round(((current_spy - start_spy) / start_spy) * 100, 2) if start_spy else 0.0
    except Exception as e:
        print(f"  ⚠️  SPY return error: {e}")
        return 0.0
    finally:
        conn.close()


def _get_regime_distribution(days: int) -> Dict[str, int]:
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT regime, COUNT(*) as cnt FROM regime_history "
            "WHERE date >= date('now', ?) GROUP BY regime",
            (f"-{days} days",),
        ).fetchall()
        return {r["regime"]: r["cnt"] for r in rows}
    except:
        return {}
    finally:
        conn.close()


def _load_trades(days: int) -> List[sqlite3.Row]:
    conn = _conn()
    date_filter = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = conn.execute(
        """SELECT player_id, symbol, entry_price, exit_price, qty,
                  realized_pnl, executed_at, timeframe
           FROM trades
           WHERE executed_at >= ?
             AND exit_price IS NOT NULL
             AND realized_pnl IS NOT NULL
           ORDER BY executed_at""",
        (date_filter,),
    ).fetchall()
    conn.close()
    return rows


def _metrics(trades: List, label: str, description: str) -> Dict:
    """Compute performance metrics from a list of trade rows."""
    if not trades:
        return {
            "config": label, "description": description,
            "total_trades": 0, "wins": 0, "losses": 0,
            "return_pct": 0.0, "sharpe": 0.0, "win_rate": 0.0,
            "max_dd": 0.0, "profit_factor": 0.0,
            "avg_trade": 0.0, "total_pnl": 0.0,
        }

    pnls      = [float(t["realized_pnl"]) for t in trades]
    wins      = sum(1 for p in pnls if p > 0)
    losses    = sum(1 for p in pnls if p < 0)
    total_pnl = sum(pnls)
    gross_win = sum(p for p in pnls if p > 0)
    gross_los = abs(sum(p for p in pnls if p < 0))

    win_rate     = wins / len(pnls) * 100
    return_pct   = total_pnl / INITIAL_CAPITAL * 100
    profit_factor = gross_win / gross_los if gross_los > 0 else 0.0

    # Sharpe (annualised, daily returns assumed)
    daily_rets = [p / INITIAL_CAPITAL for p in pnls]
    if len(daily_rets) > 1:
        mu  = statistics.mean(daily_rets)
        std = statistics.stdev(daily_rets)
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
        "config":        label,
        "description":   description,
        "total_trades":  len(trades),
        "wins":          wins,
        "losses":        losses,
        "return_pct":    round(return_pct, 2),
        "sharpe":        round(sharpe, 3),
        "win_rate":      round(win_rate, 1),
        "max_dd":        round(max_dd, 2),
        "profit_factor": round(profit_factor, 2),
        "avg_trade":     round(total_pnl / len(pnls), 2),
        "total_pnl":     round(total_pnl, 2),
    }


def _save_results(results: List[Dict], run_id: str, spy_return: float):
    """INSERT results into backtest_history using actual column schema."""
    conn = _conn()
    saved = 0
    for r in results:
        try:
            conn.execute(
                """INSERT INTO backtest_history
                   (player_id, player_name, run_date, period_days,
                    return_pct, total_pnl, win_count, loss_count, win_rate,
                    total_trades, spy_return_pct, sharpe, max_dd, vs_spy,
                    run_id, notes, config_snapshot)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    f"FLEET_{r['config']}",
                    r["description"],
                    datetime.now().strftime("%Y-%m-%d"),
                    BACKTEST_DAYS,
                    r["return_pct"],
                    r["total_pnl"],
                    r["wins"],
                    r["losses"],
                    r["win_rate"],
                    r["total_trades"],
                    spy_return,
                    r["sharpe"],
                    r["max_dd"],
                    round(r["return_pct"] - spy_return, 2),
                    run_id,
                    f"Mega 6-month backtest {run_id}",
                    json.dumps({"config": r["config"], "days": BACKTEST_DAYS}),
                ),
            )
            saved += 1
        except Exception as e:
            print(f"  ⚠️  Save error for {r['config']}: {e}")
    conn.commit()
    conn.close()
    print(f"  ✅ Saved {saved} results to backtest_history")


def _print_table(results: List[Dict], spy_return: float):
    print("\n" + "=" * 95)
    print("📊 6-MONTH BACKTEST — FLEET PERFORMANCE COMPARISON")
    print("=" * 95)
    print(f"Period: {BACKTEST_DAYS} days | Capital: ${INITIAL_CAPITAL:,} | SPY: {spy_return:+.2f}%")
    print("=" * 95)
    hdr = f"{'Config':<18} | {'Return':>8} | {'Alpha':>7} | {'Sharpe':>7} | {'WR':>6} | {'MaxDD':>7} | {'PF':>5} | {'Trades':>7} | Description"
    print(hdr)
    print("-" * 95)
    for r in sorted(results, key=lambda x: x["return_pct"], reverse=True):
        alpha = r["return_pct"] - spy_return
        print(
            f"{r['config']:<18} | {r['return_pct']:>+7.2f}% | {alpha:>+6.2f}% | "
            f"{r['sharpe']:>7.3f} | {r['win_rate']:>5.1f}% | {r['max_dd']:>6.2f}% | "
            f"{r['profit_factor']:>5.2f} | {r['total_trades']:>7,} | {r['description']}"
        )
    print("=" * 95)


def run_mega_backtest():
    start = datetime.now()
    run_id = f"mega_6mo_{start.strftime('%Y%m%d_%H%M')}"

    print("\n" + "=" * 70)
    print("🚀 MEGA 6-MONTH BACKTEST — FULL S6 FLEET")
    print("=" * 70)
    print(f"Run ID  : {run_id}")
    print(f"Period  : {BACKTEST_DAYS} days")
    print(f"Capital : ${INITIAL_CAPITAL:,}")
    print("=" * 70)

    spy_return = _get_spy_return(BACKTEST_DAYS)
    print(f"\n📈 SPY Return (benchmark): {spy_return:+.2f}%")

    regime_dist = _get_regime_distribution(BACKTEST_DAYS)
    if regime_dist:
        print(f"📊 Regime Distribution   : {dict(regime_dist)}")

    all_trades = _load_trades(BACKTEST_DAYS)
    print(f"\n📋 Total closed trades in period: {len(all_trades):,}")

    # Build comparison slices
    # FULL_S6          — all trades
    # LEGACY_AGENTS    — trades from original fleet (excl. new S6.2 agents)
    # NEW_WIRING       — trades from re-activated / new S6.2 agents only
    # HIGH_CONFIDENCE  — trades with confidence >= 75
    # OLLIE_APPROVED   — trades with [Ollie✓] in reasoning (saved in reason_str)

    full      = all_trades
    legacy    = [t for t in all_trades if t["player_id"] not in _S6_NEW_WIRING]
    new_wire  = [t for t in all_trades if t["player_id"] in _S6_NEW_WIRING]

    # Try to get confidence from signals table for high-conf slice
    conn = _conn()
    date_filter = (datetime.now() - timedelta(days=BACKTEST_DAYS)).strftime("%Y-%m-%d")
    high_conf_syms = {
        r["symbol"]
        for r in conn.execute(
            "SELECT DISTINCT symbol FROM signals WHERE confidence >= 75 AND created_at >= ?",
            (date_filter,),
        ).fetchall()
    }
    conn.close()
    high_conf = [t for t in all_trades if t["symbol"] in high_conf_syms]

    configs = [
        (full,       "FULL_S6",        "All S6 fleet trades (180d)"),
        (legacy,     "LEGACY_FLEET",   "Original fleet only (excl. neo, holly)"),
        (new_wire,   "S6_NEW_AGENTS",  "Neo-matrix + Holly-scanner (S6.2 re-activations)"),
        (high_conf,  "HIGH_CONF_75+",  "Only trades with signal confidence >= 75"),
    ]

    print(f"\n🔬 Computing {len(configs)} configurations...")
    results = []
    for trades, label, desc in configs:
        r = _metrics(trades, label, desc)
        results.append(r)
        print(f"  {label:<18}: {r['return_pct']:>+7.2f}%  ({r['total_trades']:,} trades)")

    _print_table(results, spy_return)

    # Highlights
    full_r = results[0]
    print(f"\n💡 INSIGHTS:")
    print(f"   • Fleet total P&L       : ${full_r['total_pnl']:+,.2f}")
    print(f"   • Alpha vs SPY          : {full_r['return_pct'] - spy_return:+.2f}%")
    print(f"   • Win rate              : {full_r['win_rate']:.1f}%")
    print(f"   • Sharpe ratio          : {full_r['sharpe']:.3f}")
    print(f"   • Max drawdown          : {full_r['max_dd']:.2f}%")

    _save_results(results, run_id, spy_return)

    elapsed = (datetime.now() - start).total_seconds() / 60
    print(f"\n⏱️  Duration: {elapsed:.1f} minutes")

    # ntfy notification
    try:
        msg = (
            f"Mega 6-month backtest complete\n"
            f"Run ID: {run_id}\n"
            f"SPY: {spy_return:+.2f}%\n"
            f"Fleet: {full_r['return_pct']:+.2f}% ({full_r['total_trades']} trades)\n"
            f"Alpha: {full_r['return_pct'] - spy_return:+.2f}%\n"
            f"Sharpe: {full_r['sharpe']:.3f} | WR: {full_r['win_rate']:.1f}%"
        )
        requests.post(NTFY_URL, data=msg.encode(),
                      headers={"Title": "Mega Backtest Complete", "Priority": "high"},
                      timeout=10)
    except Exception:
        pass

    print("\n" + "=" * 70)
    print("🎉 MEGA BACKTEST COMPLETE")
    print("=" * 70)
    return results


if __name__ == "__main__":
    run_mega_backtest()
