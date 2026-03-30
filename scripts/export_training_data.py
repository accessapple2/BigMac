#!/usr/bin/env python3
"""Export TradeMinds training data for fine-tuning LoRA models.

Exports three JSONL files from trader.db (read-only):
  1. trades.jsonl — closed trades with entry/exit, thesis, market context
  2. convergence_signals.jsonl — strategy signals with 24h/48h outcomes
  3. war_room_outcomes.jsonl — debate consensus vs actual price moves

Usage:
    python3 scripts/export_training_data.py            # full export + stats
    python3 scripts/export_training_data.py --auto     # silent, append new since last run
    python3 scripts/export_training_data.py --stats    # stats only, no export
"""
from __future__ import annotations
import os
import sys
import json
import sqlite3
import argparse
import re
from datetime import datetime, timedelta
from pathlib import Path
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB = "data/trader.db"
OUT_DIR = Path("training_data")
STATE_FILE = OUT_DIR / ".last_export.json"

# Model display names
MODEL_NAMES = {
    "grok-4": "Lt. Cmdr. Spock", "ollama-local": "Lt. Cmdr. Geordi",
    "gemini-2.5-flash": "Lt. Cmdr. Worf", "ollama-qwen3": "Lt. Cmdr. Scotty",
    "ollama-plutus": "Dr. McCoy", "energy-arnold": "Cmdr. Trip Tucker",
    "options-sosnoff": "Counselor Troi", "dalio-metals": "Cmdr. Dalio",
    "dayblade-sulu": "Lt. Sulu", "navigator": "Ensign Chekov",
    "ollama-llama": "Lt. Cmdr. Uhura", "steve-webull": "Captain Kirk",
    "q-entity": "Q", "dayblade-0dte": "DayBlade Options",
}


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA busy_timeout=30000")
    return c


def _parse_price_tag(reasoning: str, tag: str) -> float | None:
    """Extract [STOP: $xx.xx] or [TARGET: $xx.xx] from reasoning text."""
    if not reasoning:
        return None
    m = re.search(rf"\[{tag}: \$([0-9]+\.?[0-9]*)\]", reasoning)
    return float(m.group(1)) if m else None


def _parse_convergence_count(sources: str) -> int | None:
    """Extract convergence count from sources like 'convergence-4'."""
    if not sources:
        return None
    m = re.search(r"convergence-(\d+)", sources)
    return int(m.group(1)) if m else None


def _detect_regime(reasoning: str) -> str:
    """Detect market regime from trade reasoning."""
    if not reasoning:
        return "unknown"
    r = reasoning.lower()
    if "bear" in r or "bearish" in r or "risk-off" in r:
        return "bear"
    if "bull" in r or "bullish" in r or "breakout" in r or "rally" in r:
        return "bull"
    return "neutral"


def _detect_strategies(reasoning: str, sources: str) -> list:
    """Extract strategy names from reasoning/sources."""
    strategies = []
    if not reasoning and not sources:
        return strategies
    text = (reasoning or "") + " " + (sources or "")
    known = [
        "breakout_volume", "rsi_oversold_bounce", "macd_crossover", "bollinger_bounce",
        "ema_ribbon", "golden_cross", "volume_dry_up", "pullback_sma20",
        "trend_resumption", "relative_strength_high", "gap_and_go", "mean_reversion",
        "momentum", "can_slim", "druckenmiller", "simons", "convergence",
    ]
    for s in known:
        if s in text.lower():
            strategies.append(s)
    return strategies


def _time_of_day(ts: str) -> str:
    """Classify time of day from timestamp."""
    try:
        dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        h = dt.hour
        if h < 10:
            return "pre_market"
        elif h < 12:
            return "morning"
        elif h < 14:
            return "midday"
        elif h < 16:
            return "afternoon"
        else:
            return "after_hours"
    except Exception:
        return "unknown"


def _day_of_week(ts: str) -> str:
    try:
        dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        return ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][dt.weekday()]
    except Exception:
        return "unknown"


# ─── EXPORT 1: CLOSED TRADES ───

def export_trades(conn, since: str = None, auto: bool = False) -> int:
    """Export closed trades as JSONL."""
    query = """
        SELECT s.player_id, s.symbol, s.action, s.qty, s.price as sell_price,
               s.reasoning as sell_reasoning, s.confidence as sell_confidence,
               s.executed_at as exit_date, s.realized_pnl, s.entry_price,
               s.exit_price, s.season, s.sources as sell_sources,
               b.reasoning as buy_reasoning, b.confidence as buy_confidence,
               b.executed_at as entry_date, b.sources as buy_sources,
               p.display_name, p.provider, p.model_id
        FROM trades s
        LEFT JOIN trades b ON b.player_id = s.player_id
            AND b.symbol = s.symbol AND b.action = 'BUY'
            AND b.executed_at = (
                SELECT MAX(b2.executed_at) FROM trades b2
                WHERE b2.player_id = s.player_id AND b2.symbol = s.symbol
                AND b2.action = 'BUY' AND b2.executed_at < s.executed_at
            )
        LEFT JOIN ai_players p ON p.id = s.player_id
        WHERE s.action = 'SELL' AND s.realized_pnl IS NOT NULL
    """
    params = []
    if since:
        query += " AND s.executed_at > ?"
        params.append(since)
    query += " ORDER BY s.executed_at"

    rows = conn.execute(query, params).fetchall()

    mode = "a" if auto and since else "w"
    path = OUT_DIR / "trades.jsonl"
    count = 0

    with open(path, mode) as f:
        for r in rows:
            entry_price = r["entry_price"] or r["sell_price"]
            exit_price = r["exit_price"] or r["sell_price"]
            pnl = r["realized_pnl"] or 0
            pnl_pct = (pnl / (entry_price * (r["qty"] or 1)) * 100) if entry_price and r["qty"] else 0

            # Hold duration
            hold_hours = None
            if r["entry_date"] and r["exit_date"]:
                try:
                    entry_dt = datetime.strptime(r["entry_date"], "%Y-%m-%d %H:%M:%S")
                    exit_dt = datetime.strptime(r["exit_date"], "%Y-%m-%d %H:%M:%S")
                    hold_hours = round((exit_dt - entry_dt).total_seconds() / 3600, 1)
                except Exception:
                    pass

            buy_reasoning = r["buy_reasoning"] or ""
            stop_price = _parse_price_tag(buy_reasoning, "STOP") or _parse_price_tag(buy_reasoning, "AUTO-STOP")
            target_price = _parse_price_tag(buy_reasoning, "TARGET")

            record = {
                "ticker": r["symbol"],
                "entry_price": round(entry_price, 4) if entry_price else None,
                "exit_price": round(exit_price, 4) if exit_price else None,
                "entry_date": r["entry_date"],
                "exit_date": r["exit_date"],
                "pnl_dollars": round(pnl, 2),
                "pnl_percent": round(pnl_pct, 2),
                "win": pnl > 0,
                "model_name": MODEL_NAMES.get(r["player_id"], r["display_name"] or r["player_id"]),
                "player_id": r["player_id"],
                "personality": f"{r['provider']}/{r['model_id']}" if r["provider"] else "",
                "thesis_text": buy_reasoning[:500] if buy_reasoning else "",
                "strategies_used": _detect_strategies(buy_reasoning, r["buy_sources"]),
                "convergence_count": _parse_convergence_count(r["buy_sources"]),
                "market_regime": _detect_regime(buy_reasoning),
                "time_of_day": _time_of_day(r["entry_date"] or ""),
                "day_of_week": _day_of_week(r["entry_date"] or ""),
                "hold_duration_hours": hold_hours,
                "stop_loss_price": stop_price,
                "take_profit_price": target_price,
                "hit_stop": exit_price <= stop_price if stop_price and exit_price else False,
                "hit_target": exit_price >= target_price if target_price and exit_price else False,
                "season": r["season"],
            }
            f.write(json.dumps(record) + "\n")
            count += 1

    return count


# ─── EXPORT 2: CONVERGENCE SIGNALS ───

def export_convergence(conn, since: str = None, auto: bool = False) -> int:
    """Export convergence signals with price outcomes."""
    query = """
        SELECT scan_date, ticker,
               COUNT(DISTINCT strategy_name) as convergence_count,
               GROUP_CONCAT(DISTINCT strategy_name) as strategies,
               AVG(confidence) as avg_confidence,
               MIN(entry_price) as entry_price,
               MIN(stop_price) as stop_price,
               MAX(target_price) as target_price
        FROM strategy_signals
        WHERE signal_type = 'BUY'
    """
    params = []
    if since:
        query += " AND scan_date > ?"
        params.append(since[:10])  # date only
    query += " GROUP BY scan_date, ticker HAVING convergence_count >= 3 ORDER BY scan_date, convergence_count DESC"

    rows = conn.execute(query, params).fetchall()

    mode = "a" if auto and since else "w"
    path = OUT_DIR / "convergence_signals.jsonl"
    count = 0

    with open(path, mode) as f:
        for r in rows:
            entry = r["entry_price"] or 0
            target = r["target_price"] or 0
            stop = r["stop_price"] or 0
            risk = entry - stop if entry and stop else 0
            reward = target - entry if target and entry else 0
            rr = round(reward / risk, 2) if risk > 0 else 0

            # Look up actual prices 24h and 48h later using trade prices or price cache
            actual_24h = None
            actual_48h = None
            try:
                scan_dt = datetime.strptime(r["scan_date"], "%Y-%m-%d")
                day1 = (scan_dt + timedelta(days=1)).strftime("%Y-%m-%d")
                day2 = (scan_dt + timedelta(days=2)).strftime("%Y-%m-%d")

                # Check if we have historical prices
                hp = conn.execute(
                    "SELECT date, close FROM historical_prices WHERE symbol=? AND date IN (?,?) ORDER BY date",
                    (r["ticker"], day1, day2)
                ).fetchall()
                for h in hp:
                    if h["date"] == day1:
                        actual_24h = h["close"]
                    elif h["date"] == day2:
                        actual_48h = h["close"]
            except Exception:
                pass

            record = {
                "ticker": r["ticker"],
                "signal_date": r["scan_date"],
                "strategies_list": r["strategies"].split(",") if r["strategies"] else [],
                "convergence_count": r["convergence_count"],
                "avg_confidence": round(r["avg_confidence"], 2) if r["avg_confidence"] else 0,
                "entry_price_suggested": round(entry, 2) if entry else None,
                "target_price": round(target, 2) if target else None,
                "stop_price": round(stop, 2) if stop else None,
                "rr_ratio": rr,
                "actual_price_24h_later": round(actual_24h, 2) if actual_24h else None,
                "actual_price_48h_later": round(actual_48h, 2) if actual_48h else None,
                "would_have_hit_target": actual_48h >= target if actual_48h and target else None,
                "would_have_hit_stop": actual_24h <= stop if actual_24h and stop else None,
            }
            f.write(json.dumps(record) + "\n")
            count += 1

    return count


# ─── EXPORT 3: WAR ROOM OUTCOMES ───

def export_war_room(conn, since: str = None, auto: bool = False) -> int:
    """Export War Room debates with price outcomes."""
    # Get debates grouped by symbol + 30-minute window
    query = """
        SELECT symbol,
               MIN(created_at) as debate_start,
               GROUP_CONCAT(DISTINCT player_id) as participants,
               GROUP_CONCAT(take, '|||') as takes,
               COUNT(*) as message_count
        FROM war_room
        WHERE symbol != 'SCAN' AND symbol != 'MARKET'
    """
    params = []
    if since:
        query += " AND created_at > ?"
        params.append(since)
    query += """
        GROUP BY symbol, strftime('%Y-%m-%d %H', created_at)
        HAVING message_count >= 2
        ORDER BY debate_start
    """

    rows = conn.execute(query, params).fetchall()

    mode = "a" if auto and since else "w"
    path = OUT_DIR / "war_room_outcomes.jsonl"
    count = 0

    with open(path, mode) as f:
        for r in rows:
            symbol = r["symbol"]
            takes_text = r["takes"] or ""
            participants = r["participants"].split(",") if r["participants"] else []

            # Count bull/bear sentiment from takes
            bull_count = 0
            bear_count = 0
            for take in takes_text.split("|||"):
                t = take.lower()
                if any(w in t for w in ["bull", "buy", "long", "upside", "breakout"]):
                    bull_count += 1
                elif any(w in t for w in ["bear", "sell", "short", "downside", "risk"]):
                    bear_count += 1

            if bull_count > bear_count:
                consensus = "BULLISH"
            elif bear_count > bull_count:
                consensus = "BEARISH"
            else:
                consensus = "NEUTRAL"

            # Price at debate time and 24h/48h later
            price_at_debate = None
            price_24h = None
            price_48h = None

            try:
                debate_dt = datetime.strptime(r["debate_start"], "%Y-%m-%d %H:%M:%S")
                debate_date = debate_dt.strftime("%Y-%m-%d")
                day1 = (debate_dt + timedelta(days=1)).strftime("%Y-%m-%d")
                day2 = (debate_dt + timedelta(days=2)).strftime("%Y-%m-%d")

                hp = conn.execute(
                    "SELECT date, close FROM historical_prices WHERE symbol=? AND date IN (?,?,?) ORDER BY date",
                    (symbol, debate_date, day1, day2)
                ).fetchall()
                for h in hp:
                    if h["date"] == debate_date:
                        price_at_debate = h["close"]
                    elif h["date"] == day1:
                        price_24h = h["close"]
                    elif h["date"] == day2:
                        price_48h = h["close"]
            except Exception:
                pass

            # Was consensus correct?
            consensus_correct = None
            if price_at_debate and price_24h:
                price_moved_up = price_24h > price_at_debate
                if consensus == "BULLISH":
                    consensus_correct = price_moved_up
                elif consensus == "BEARISH":
                    consensus_correct = not price_moved_up

            record = {
                "ticker": symbol,
                "debate_date": r["debate_start"],
                "participants": [MODEL_NAMES.get(p, p) for p in participants],
                "participant_ids": participants,
                "message_count": r["message_count"],
                "bull_count": bull_count,
                "bear_count": bear_count,
                "consensus": consensus,
                "price_at_debate": round(price_at_debate, 2) if price_at_debate else None,
                "price_24h_later": round(price_24h, 2) if price_24h else None,
                "price_48h_later": round(price_48h, 2) if price_48h else None,
                "consensus_was_correct": consensus_correct,
            }
            f.write(json.dumps(record) + "\n")
            count += 1

    return count


# ─── STATS ───

def print_stats():
    """Print summary statistics from exported data."""
    print("\n" + "=" * 60)
    print("TRADEMINDS TRAINING DATA — SUMMARY STATS")
    print("=" * 60)

    # Trades
    trades_path = OUT_DIR / "trades.jsonl"
    if trades_path.exists():
        trades = [json.loads(line) for line in open(trades_path)]
        wins = [t for t in trades if t["win"]]
        losses = [t for t in trades if not t["win"]]

        print(f"\n--- TRADES ({len(trades)} total) ---")
        print(f"  Win rate:     {len(wins) / len(trades) * 100:.1f}%")
        print(f"  Total P&L:    ${sum(t['pnl_dollars'] for t in trades):,.2f}")
        print(f"  Avg win:      ${sum(t['pnl_dollars'] for t in wins) / len(wins):,.2f}" if wins else "  Avg win:      —")
        print(f"  Avg loss:     ${sum(t['pnl_dollars'] for t in losses) / len(losses):,.2f}" if losses else "  Avg loss:     —")

        # Best model by win rate (min 5 trades)
        model_stats = defaultdict(lambda: {"wins": 0, "total": 0, "pnl": 0})
        for t in trades:
            ms = model_stats[t["model_name"]]
            ms["total"] += 1
            ms["pnl"] += t["pnl_dollars"]
            if t["win"]:
                ms["wins"] += 1

        print(f"\n  --- Model Performance (min 5 trades) ---")
        ranked = sorted(
            [(name, s) for name, s in model_stats.items() if s["total"] >= 5],
            key=lambda x: x[1]["wins"] / x[1]["total"],
            reverse=True,
        )
        for name, s in ranked:
            wr = s["wins"] / s["total"] * 100
            print(f"  {name:25s}  {s['total']:3d} trades  {wr:5.1f}% WR  ${s['pnl']:>8,.2f} P&L")

        # Best strategy
        strat_wins = Counter()
        strat_total = Counter()
        for t in trades:
            for s in t.get("strategies_used", []):
                strat_total[s] += 1
                if t["win"]:
                    strat_wins[s] += 1

        if strat_total:
            print(f"\n  --- Strategy Performance ---")
            for strat, total in strat_total.most_common(10):
                wr = strat_wins[strat] / total * 100
                print(f"  {strat:30s}  {total:3d} trades  {wr:5.1f}% WR")

    # Convergence
    conv_path = OUT_DIR / "convergence_signals.jsonl"
    if conv_path.exists():
        signals = [json.loads(line) for line in open(conv_path)]
        with_outcome = [s for s in signals if s.get("would_have_hit_target") is not None]
        print(f"\n--- CONVERGENCE SIGNALS ({len(signals)} total) ---")
        if with_outcome:
            hit_target = sum(1 for s in with_outcome if s["would_have_hit_target"])
            print(f"  With outcomes: {len(with_outcome)}")
            print(f"  Hit target:    {hit_target}/{len(with_outcome)} ({hit_target / len(with_outcome) * 100:.1f}%)")

        by_count = defaultdict(list)
        for s in signals:
            by_count[s["convergence_count"]].append(s)
        for cnt in sorted(by_count):
            sigs = by_count[cnt]
            w = [s for s in sigs if s.get("would_have_hit_target")]
            hit = sum(1 for s in w if s["would_have_hit_target"]) if w else 0
            print(f"  {cnt}-strategy:   {len(sigs)} signals" + (f", {hit}/{len(w)} hit target" if w else ""))

    # War Room
    wr_path = OUT_DIR / "war_room_outcomes.jsonl"
    if wr_path.exists():
        debates = [json.loads(line) for line in open(wr_path)]
        with_outcome = [d for d in debates if d.get("consensus_was_correct") is not None]
        print(f"\n--- WAR ROOM DEBATES ({len(debates)} total) ---")
        if with_outcome:
            correct = sum(1 for d in with_outcome if d["consensus_was_correct"])
            print(f"  With outcomes:    {len(with_outcome)}")
            print(f"  Consensus correct: {correct}/{len(with_outcome)} ({correct / len(with_outcome) * 100:.1f}%)")

    print("\n" + "=" * 60)


# ─── MAIN ───

def main():
    parser = argparse.ArgumentParser(description="Export TradeMinds training data")
    parser.add_argument("--auto", action="store_true", help="Silent mode, append new data since last export")
    parser.add_argument("--stats", action="store_true", help="Show stats only, don't export")
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if args.stats:
        print_stats()
        return

    # Load last export timestamp for incremental mode
    since = None
    if args.auto and STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text())
            since = state.get("last_export")
        except Exception:
            pass

    conn = _conn()

    if not args.auto:
        print("TradeMinds Training Data Export")
        print("-" * 40)

    # Export all three datasets
    n_trades = export_trades(conn, since, args.auto)
    n_conv = export_convergence(conn, since, args.auto)
    n_wr = export_war_room(conn, since, args.auto)

    conn.close()

    # Save state for incremental mode
    STATE_FILE.write_text(json.dumps({
        "last_export": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "trades": n_trades,
        "convergence": n_conv,
        "war_room": n_wr,
    }))

    if not args.auto:
        print(f"\nExported:")
        print(f"  trades.jsonl:              {n_trades} closed trades")
        print(f"  convergence_signals.jsonl: {n_conv} signals")
        print(f"  war_room_outcomes.jsonl:   {n_wr} debates")
        print(f"\nFiles saved to: {OUT_DIR.resolve()}")
        print_stats()
    else:
        # Silent mode — just log count
        if n_trades + n_conv + n_wr > 0:
            print(f"[training_data] Exported {n_trades} trades, {n_conv} signals, {n_wr} debates (since {since or 'beginning'})")


if __name__ == "__main__":
    main()
