"""Trade Memory Injection — formats historical trade performance into prompt blocks.

Each block is injected into crew prompts before market data so every model
can calibrate its conviction against its own track record.

Functions:
  get_memory_block_for_player(player_id)  — individual track record
  get_memory_block_for_chekov()           — strategy performance rankings
  get_memory_block_for_debate(symbol)     — crew history on a specific symbol

All functions return "" on failure — memory injection must never break the prompt flow.
"""
from __future__ import annotations
import os
import sqlite3

from rich.console import Console

console = Console()
_EMPTY_MSG = "No trade history yet — building track record."
DB = os.environ.get(
    "TRADEMINDS_DB",
    os.path.expanduser("~/autonomous-trader/data/trader.db"),
)


# ---------------------------------------------------------------------------
# Player track record
# ---------------------------------------------------------------------------

def get_memory_block_for_player(player_id: str) -> str:
    """Return formatted 30-day track record block for injection into a player's prompt.

    Placed BEFORE market data but AFTER system prompt so the model sees its
    own history before evaluating today's setup.
    """
    try:
        from engine.trade_outcomes import get_player_stats
        stats = get_player_stats(player_id, lookback_days=30)

        if stats.get("empty") or stats.get("error") or not stats.get("total_trades"):
            return f"\n=== YOUR TRADING TRACK RECORD ===\n{_EMPTY_MSG}\n"

        win_rate = stats["win_rate"]
        wins = stats["wins"]
        losses = stats["losses"]
        total = stats["total_trades"]
        avg_pnl = stats["avg_pnl"]
        best = stats["best_trade"]
        worst = stats["worst_trade"]
        avg_hold = stats["avg_hold_hours"]
        regime_stats = stats.get("regime_stats", {})
        last_10 = stats.get("last_10_trades", [])

        # Last 5 trades string
        last_5_parts = []
        for t in last_10[:5]:
            sign = "+" if (t["pnl_dollars"] or 0) >= 0 else ""
            mark = "✓" if t["outcome"] == "win" else "✗"
            last_5_parts.append(f"{t['symbol']} {sign}${t['pnl_dollars']:.0f} {mark}")
        last_5 = ", ".join(last_5_parts) if last_5_parts else "N/A"

        # Best trade description
        b_hold = f", held {best['hold_hours']:.1f}h" if best.get("hold_hours") else ""
        b_regime = f", {best['regime']} regime" if best.get("regime") else ""
        best_desc = f"{best['symbol']} +${best['pnl']:.0f}{b_hold}{b_regime}"

        # Worst trade description
        w_hold = f", held {worst['hold_hours']:.1f}h" if worst.get("hold_hours") else ""
        w_regime = f", {worst['regime']} regime" if worst.get("regime") else ""
        worst_desc = f"{worst['symbol']} -${abs(worst['pnl']):.0f}{w_hold}{w_regime}"

        # Regime breakdown
        regime_parts = []
        for regime, rs in sorted(regime_stats.items(), key=lambda x: -x[1]["wins"]):
            wr = round(rs["wins"] / rs["total"] * 100) if rs["total"] > 0 else 0
            regime_parts.append(f"{regime} {wr:.0f}% win rate")
        regime_line = " | ".join(regime_parts) if regime_parts else ""

        # Lesson — derived from regime stats with enough data
        lesson_parts = []
        for regime, rs in regime_stats.items():
            if rs["total"] < 3:
                continue
            wr = round(rs["wins"] / rs["total"] * 100)
            if wr >= 70:
                lesson_parts.append(
                    f"You perform best in {regime} regime (+{wr:.0f}% win rate)."
                )
            elif wr <= 33:
                lesson_parts.append(
                    f"You lose most in {regime} regime ({wr:.0f}% win rate). "
                    f"Consider reducing position size or sitting out."
                )
        if not lesson_parts:
            preferred = stats.get("preferred_regime") or "N/A"
            lesson_parts.append(f"Best results in {preferred} regime.")
        lesson = " ".join(lesson_parts)

        lines = [
            f"\n=== YOUR TRADING TRACK RECORD (last 30 days) ===",
            f"Win Rate: {win_rate:.0f}% ({wins}W / {losses}L) over {total} trades",
            f"Avg P&L: {'+' if avg_pnl >= 0 else ''}${avg_pnl:.2f} per trade",
            f"Best:  {best_desc}",
            f"Worst: {worst_desc}",
            f"Avg hold: {avg_hold:.1f} hours",
            f"Last 5 trades: {last_5}",
        ]
        if regime_line:
            lines.append(f"Pattern: {regime_line}")
        lines.append(f"Lesson: {lesson}")
        lines.append("=== USE THIS TO CALIBRATE YOUR CONVICTION ===")
        return "\n".join(lines) + "\n"

    except Exception as e:
        console.log(f"[dim]trade_memory: player block error ({e})")
        return ""


# ---------------------------------------------------------------------------
# Chekov strategy performance
# ---------------------------------------------------------------------------

def get_memory_block_for_chekov() -> str:
    """Return strategy performance block for Chekov's convergence scanner.

    Injected before each scan decision so the scanner knows which strategies
    are hot and which are cold over the last 60 days.
    """
    try:
        from engine.trade_outcomes import get_strategy_stats
        stats = get_strategy_stats(lookback_days=60)

        if not stats:
            return f"\n=== STRATEGY PERFORMANCE (last 60 days) ===\n{_EMPTY_MSG}\n"

        top = sorted(
            [(n, s) for n, s in stats.items() if s["win_rate"] >= 70 and s["trades"] >= 5],
            key=lambda x: -x[1]["win_rate"],
        )
        under = sorted(
            [(n, s) for n, s in stats.items() if s["win_rate"] < 40 and s["trades"] >= 5],
            key=lambda x: x[1]["win_rate"],
        )
        new_strats = [(n, s) for n, s in stats.items() if s["trades"] < 5]

        lines = ["\n=== STRATEGY PERFORMANCE (last 60 days) ==="]

        if top:
            lines.append("Top performers (weight 1.5x in convergence):")
            for name, s in top[:6]:
                lines.append(
                    f"  {name}: {s['win_rate']:.0f}% win rate, "
                    f"{s['trades']} trades, avg ${s['avg_pnl']:+.2f}"
                )
        if under:
            lines.append("Underperformers (weight 0.5x — need 4+ agreement):")
            for name, s in under[:6]:
                lines.append(
                    f"  {name}: {s['win_rate']:.0f}% win rate, "
                    f"{s['trades']} trades, avg ${s['avg_pnl']:+.2f}"
                )
        if new_strats:
            names_str = ", ".join(n for n, _ in new_strats[:8])
            lines.append(f"New strategies (neutral, <5 trades): {names_str}")

        if top:
            top_names = [n for n, _ in top[:3]]
            lines.append(
                f"Recommendation: Weight {', '.join(top_names)} higher in convergence scoring."
            )
        if under:
            under_names = [n for n, _ in under[:2]]
            lines.append(
                f"Caution: {', '.join(under_names)} — require 4+ strategy agreement when primary signal."
            )

        lines.append("=== END STRATEGY PERFORMANCE ===")
        return "\n".join(lines) + "\n"

    except Exception as e:
        console.log(f"[dim]trade_memory: chekov block error ({e})")
        return ""


# ---------------------------------------------------------------------------
# Symbol history for debate
# ---------------------------------------------------------------------------

def get_memory_block_for_debate(symbol: str) -> str:
    """Return the crew's collective trade history on a specific symbol.

    Injected into debate agent prompts before market data so Riker, Worf,
    and Picard know what happened the last time the fleet traded this ticker.
    """
    try:
        conn = sqlite3.connect(DB, check_same_thread=False, timeout=10)
        conn.row_factory = sqlite3.Row

        rows = conn.execute("""
            SELECT player_id, symbol, pnl_dollars, outcome,
                   hold_duration_hours, regime_at_entry, gex_regime_at_entry,
                   exit_time
            FROM trade_outcomes
            WHERE symbol = ?
            ORDER BY created_at DESC
            LIMIT 20
        """, (symbol,)).fetchall()
        conn.close()

        if not rows:
            return (
                f"\n=== CREW TRACK RECORD ON {symbol} ===\n"
                f"No prior trades on {symbol} — this is uncharted territory.\n"
            )

        total = len(rows)
        wins = sum(1 for r in rows if r["outcome"] == "win")
        win_rate = round(wins / total * 100) if total > 0 else 0
        holds = [r["hold_duration_hours"] for r in rows if r["hold_duration_hours"]]
        avg_hold = round(sum(holds) / len(holds), 1) if holds else 0.0

        # Last trade summary
        last = rows[0]
        last_pnl = last["pnl_dollars"] or 0
        last_outcome = "won" if last["outcome"] == "win" else "lost"
        last_regime = last["regime_at_entry"] or "unknown regime"
        last_player = last["player_id"]
        last_sign = "+" if last_pnl >= 0 else ""

        # GEX context (if we have any historical data)
        gex_pos = sum(1 for r in rows if r["gex_regime_at_entry"] == "POSITIVE")
        gex_neg = sum(1 for r in rows if r["gex_regime_at_entry"] == "NEGATIVE")

        lines = [
            f"\n=== CREW TRACK RECORD ON {symbol} ===",
            f"Fleet has traded {symbol} {total} time(s). "
            f"Win rate: {win_rate:.0f}%. Avg hold: {avg_hold:.1f} hours.",
            f"Last trade: {last_player} {last_outcome} "
            f"{last_sign}${abs(last_pnl):.0f}. Regime was {last_regime}.",
        ]

        if gex_pos + gex_neg >= 3:
            better = "positive" if gex_pos > gex_neg else "negative"
            lines.append(
                f"GEX history: positive gamma in {gex_pos} trades, "
                f"negative in {gex_neg} — crew performs better when gamma is {better}."
            )

        lines.append("=== END CREW TRACK RECORD ===")
        return "\n".join(lines) + "\n"

    except Exception as e:
        console.log(f"[dim]trade_memory: debate block error ({e})")
        return ""
