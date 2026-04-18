#!/usr/bin/env python3
"""
Agent Coaching Report — XO Diagnostic
Diagnoses WHY agents are underperforming and recommends fixes.
Run: venv/bin/python3 engine/agent_coaching_report.py
"""
import sqlite3
import statistics
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

_root = Path(__file__).parent.parent
DB_PATH = _root / "data" / "trader.db"

ANALYSIS_DAYS   = 180
INITIAL_CAPITAL = 100_000

# From backtest results
UNDERPERFORMERS = ["dayblade-0dte", "ollama-local"]
MID_PERFORMERS  = ["navigator", "grok-4", "dayblade-sulu"]


def _conn():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def _get_trades(player_id: str, days: int) -> List[sqlite3.Row]:
    conn  = _conn()
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows  = conn.execute(
        """SELECT player_id, symbol, entry_price, exit_price,
                  qty, realized_pnl, executed_at, timeframe, action
           FROM trades
           WHERE player_id = ?
             AND executed_at >= ?
             AND exit_price IS NOT NULL
             AND realized_pnl IS NOT NULL
           ORDER BY executed_at DESC""",
        (player_id, since),
    ).fetchall()
    conn.close()
    return rows


def _get_signals(player_id: str, days: int) -> List[sqlite3.Row]:
    conn  = _conn()
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows  = conn.execute(
        """SELECT symbol, confidence, timeframe, execution_status,
                  rejection_reason, created_at
           FROM signals
           WHERE player_id = ?
             AND created_at >= ?
           ORDER BY created_at DESC""",
        (player_id, since),
    ).fetchall()
    conn.close()
    return rows


def _get_regime(date_str: str) -> str:
    conn = _conn()
    date_only = (date_str or "")[:10]
    row = conn.execute(
        "SELECT regime FROM regime_history WHERE date <= ? ORDER BY date DESC LIMIT 1",
        (date_only,),
    ).fetchone()
    conn.close()
    return row["regime"] if row else "UNKNOWN"


def _analyze(player_id: str, days: int = 180) -> Optional[Dict]:
    print(f"\n{'=' * 70}")
    print(f"🔬 COACHING REPORT: {player_id.upper()}")
    print(f"{'=' * 70}")

    trades  = _get_trades(player_id, days)
    signals = _get_signals(player_id, days)

    if not trades:
        print(f"   No closed trades found.")
        return None

    pnls   = [float(t["realized_pnl"]) for t in trades]
    wins   = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    total  = sum(pnls)

    win_rate  = len(wins) / len(pnls) * 100
    avg_win   = statistics.mean(wins)   if wins   else 0.0
    avg_loss  = statistics.mean(losses) if losses else 0.0
    rr_ratio  = abs(avg_win / avg_loss) if avg_loss else 0.0

    gross_win = sum(wins)
    gross_los = abs(sum(losses))
    pf        = gross_win / gross_los if gross_los > 0 else 0.0

    print(f"\n📊 BASIC STATS:")
    print(f"   Trades : {len(pnls)} | Wins: {len(wins)} | Losses: {len(losses)}")
    print(f"   Win Rate  : {win_rate:.1f}%")
    print(f"   Total P&L : ${total:+,.2f}")
    print(f"   Avg Win   : ${avg_win:+,.2f}  |  Avg Loss: ${avg_loss:+,.2f}")
    print(f"   R:R Ratio : {rr_ratio:.2f}  |  Profit Factor: {pf:.2f}")

    if rr_ratio < 1.0:
        print(f"   ⚠️  R:R < 1 — avg win smaller than avg loss")

    # Signal analysis
    print(f"\n📡 SIGNALS ({len(signals)} total):")
    if signals:
        confs = [float(s["confidence"]) for s in signals if s["confidence"]]
        if confs:
            print(f"   Avg confidence : {statistics.mean(confs):.1f}%")
            print(f"   Range          : {min(confs):.0f}% – {max(confs):.0f}%")

        # Execution status breakdown
        statuses = defaultdict(int)
        for s in signals:
            statuses[s["execution_status"] or "unknown"] += 1
        for st, cnt in sorted(statuses.items(), key=lambda x: -x[1]):
            print(f"   {st:<20}: {cnt}")

        conv = len(trades) / len(signals) * 100
        print(f"   Signal→Trade conversion: {conv:.1f}%")
        if conv > 40:
            print(f"   ⚠️  Converting too many signals — threshold may be too low")

    # Symbol breakdown
    sym_data: Dict[str, Dict] = defaultdict(lambda: {"pnl": 0.0, "trades": 0, "wins": 0})
    for t, p in zip(trades, pnls):
        d = sym_data[t["symbol"]]
        d["pnl"] += p
        d["trades"] += 1
        if p > 0:
            d["wins"] += 1

    sorted_syms = sorted(sym_data.items(), key=lambda x: x[1]["pnl"])

    print(f"\n📈 SYMBOL BREAKDOWN:")
    print(f"   🔴 WORST:")
    for sym, d in sorted_syms[:5]:
        wr = d["wins"] / d["trades"] * 100 if d["trades"] else 0
        print(f"      {sym:<7} ${d['pnl']:>+9,.2f}  {d['trades']} trades  {wr:.0f}% WR")

    print(f"   🟢 BEST:")
    for sym, d in sorted_syms[-3:]:
        wr = d["wins"] / d["trades"] * 100 if d["trades"] else 0
        print(f"      {sym:<7} ${d['pnl']:>+9,.2f}  {d['trades']} trades  {wr:.0f}% WR")

    # Timeframe breakdown (using timeframe column as strategy proxy)
    tf_data: Dict[str, Dict] = defaultdict(lambda: {"pnl": 0.0, "trades": 0, "wins": 0})
    for t, p in zip(trades, pnls):
        tf = t["timeframe"] or "unknown"
        tf_data[tf]["pnl"] += p
        tf_data[tf]["trades"] += 1
        if p > 0:
            tf_data[tf]["wins"] += 1

    print(f"\n⏱️  TIMEFRAME / STRATEGY:")
    for tf, d in sorted(tf_data.items(), key=lambda x: x[1]["pnl"]):
        wr  = d["wins"] / d["trades"] * 100 if d["trades"] else 0
        tag = "🔴" if d["pnl"] < 0 else "🟢"
        print(f"   {tag} {tf:<14} ${d['pnl']:>+9,.2f}  {d['trades']} trades  {wr:.0f}% WR")

    # Regime breakdown
    regime_data: Dict[str, Dict] = defaultdict(lambda: {"pnl": 0.0, "trades": 0, "wins": 0})
    for t, p in zip(trades, pnls):
        regime = _get_regime(t["executed_at"] or "")
        regime_data[regime]["pnl"] += p
        regime_data[regime]["trades"] += 1
        if p > 0:
            regime_data[regime]["wins"] += 1

    print(f"\n🌡️  REGIME BREAKDOWN:")
    for reg, d in sorted(regime_data.items(), key=lambda x: x[1]["pnl"]):
        wr  = d["wins"] / d["trades"] * 100 if d["trades"] else 0
        tag = "🔴" if d["pnl"] < 0 else "🟢"
        print(f"   {tag} {reg:<18} ${d['pnl']:>+9,.2f}  {d['trades']} trades  {wr:.0f}% WR")

    # Trade frequency
    trades_per_day = len(trades) / days
    print(f"\n📅 FREQUENCY: {trades_per_day:.2f} trades/day ({len(trades)} in {days}d)")
    if trades_per_day > 2.0:
        print(f"   ⚠️  Over-trading — {trades_per_day:.1f}/day")

    # ── Recommendations ────────────────────────────────────────────────────────
    recs = []

    if win_rate < 30:
        recs.append({
            "priority": "HIGH",
            "issue":    f"Win rate {win_rate:.1f}% is too low",
            "fix":      "Raise Ollie threshold or add confidence floor",
            "code":     f'AGENT_THRESHOLDS["{player_id}"] = 2.5  # raise from default',
        })

    if rr_ratio < 1.0 and avg_loss < 0:
        recs.append({
            "priority": "HIGH",
            "issue":    f"R:R {rr_ratio:.2f} — wins smaller than losses",
            "fix":      "Tighten stop-loss (2%) and/or hold winners longer",
            "code":     f'# In crew_scanner.py:\n_AGENT_STOP_PCT["{player_id}"] = 0.02',
        })

    if trades_per_day > 1.5:
        recs.append({
            "priority": "MEDIUM",
            "issue":    f"Over-trading ({trades_per_day:.1f}/day)",
            "fix":      "Lower max daily trades or add cooldown",
            "code":     f'_MAX_DAILY_TRADES_PER_AGENT = 2  # override for {player_id}',
        })

    if sorted_syms and sorted_syms[0][1]["pnl"] < -500:
        worst_sym = sorted_syms[0][0]
        recs.append({
            "priority": "MEDIUM",
            "issue":    f"{worst_sym} contributing ${sorted_syms[0][1]['pnl']:+,.0f}",
            "fix":      f"Block {worst_sym} for this agent",
            "code":     f'# Add to mandate in crew_specialization.py:\n"blocked_symbols": ["{worst_sym}"]',
        })

    for reg, d in regime_data.items():
        if d["pnl"] < -1000 and d["trades"] >= 5:
            recs.append({
                "priority": "MEDIUM",
                "issue":    f"Loses ${abs(d['pnl']):,.0f} in {reg} regime",
                "fix":      f"Gate agent during {reg}",
                "code":     f'# In should_agent_trade():\nif regime == "{reg}" and player_id == "{player_id}": return False',
            })

    print(f"\n{'─' * 70}")
    print(f"📋 RECOMMENDATIONS for {player_id}:")
    if recs:
        for i, r in enumerate(recs, 1):
            print(f"\n  {i}. [{r['priority']}] {r['issue']}")
            print(f"     FIX : {r['fix']}")
            print(f"     CODE: {r['code']}")
    else:
        print("     No blocking issues — may need more trade volume for signal.")

    return {
        "agent":           player_id,
        "trades":          len(trades),
        "win_rate":        round(win_rate, 1),
        "total_pnl":       round(total, 2),
        "rr_ratio":        round(rr_ratio, 2),
        "profit_factor":   round(pf, 2),
        "trades_per_day":  round(trades_per_day, 2),
        "recommendations": recs,
        "worst_symbol":    sorted_syms[0][0] if sorted_syms else None,
        "worst_regime":    min(regime_data.items(), key=lambda x: x[1]["pnl"])[0] if regime_data else None,
    }


def run_coaching_report():
    print("\n" + "=" * 70)
    print("🎖️  XO COACHING REPORT — AGENT PERFORMANCE ANALYSIS")
    print("=" * 70)
    print(f"Period: {ANALYSIS_DAYS} days")
    print(f"Focus : {', '.join(UNDERPERFORMERS)}")
    print("=" * 70)

    all_results = []

    print("\n🔴 UNDERPERFORMING AGENTS")
    print("─" * 70)
    for agent in UNDERPERFORMERS:
        r = _analyze(agent, ANALYSIS_DAYS)
        if r:
            all_results.append(r)

    print("\n\n🟡 MID-TIER AGENTS (benchmark comparison)")
    print("─" * 70)
    for agent in MID_PERFORMERS:
        r = _analyze(agent, ANALYSIS_DAYS)
        if r:
            all_results.append(r)

    # Executive summary
    print("\n\n" + "=" * 70)
    print("📊 EXECUTIVE SUMMARY")
    print("=" * 70)

    total_recs = sum(len(r["recommendations"]) for r in all_results)
    high_recs  = [
        (r["agent"], rec)
        for r in all_results
        for rec in r["recommendations"]
        if rec["priority"] == "HIGH"
    ]

    print(f"\n  Agents analysed  : {len(all_results)}")
    print(f"  Total recs       : {total_recs}")
    print(f"  HIGH priority    : {len(high_recs)}")

    if high_recs:
        print(f"\n{'=' * 70}")
        print("🎯 HIGH PRIORITY FIXES (do these first)")
        print("=" * 70)
        for agent, rec in high_recs:
            print(f"\n  • {agent}: {rec['issue']}")
            print(f"    → {rec['fix']}")

    # Quick comparison table
    print(f"\n{'─' * 70}")
    print(f"{'Agent':<20} | {'Trades':>7} | {'WR':>6} | {'R:R':>5} | {'P&L':>12} | {'Rec':>4}")
    print("─" * 70)
    for r in all_results:
        n_recs = len(r["recommendations"])
        flag = "🔴" if n_recs >= 2 else ("🟡" if n_recs == 1 else "🟢")
        print(
            f"  {r['agent']:<18} | {r['trades']:>7} | {r['win_rate']:>5.1f}% | "
            f"{r['rr_ratio']:>5.2f} | ${r['total_pnl']:>+10,.2f} | {flag} {n_recs}"
        )

    print("\n")
    return all_results


if __name__ == "__main__":
    run_coaching_report()
