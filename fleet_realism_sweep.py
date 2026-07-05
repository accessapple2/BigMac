#!/usr/bin/env python3
"""HM-FLEET-REALISM-SWEEP 2026-07-03 — honest backtest across the whole roster.

Runs the REALISM (honest) backtest for every agent with >= MIN_SIGNALS BUY-side
signals, both guarded and raw, and ranks the fleet. Window auto-sizes per agent
to cover their full signal history (so no history is amputated).

Doctrine compliance:
  - KEEP ALL DATA: results are appended to a timestamped JSON after EVERY agent
    (crash-safe, incremental). Output files are never overwritten.
  - LET RUNS COMPLETE: designed for nohup; progress logged per agent so a tail
    of the log shows where it is. No early-exit on individual agent errors.

Run from ~/autonomous-trader (after any in-flight realism checks finish):
    nohup venv/bin/python -u fleet_realism_sweep.py > logs/fleet_sweep.log 2>&1 &
"""
import json
import sqlite3
import sys
import traceback
from datetime import datetime

import engine.backtester as bt
from engine.backtester import backtest_player

MIN_SIGNALS = 100
STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
OUT = f"reports/fleet_realism_sweep_{STAMP}.json"

# Honest mode, always.
# HM-BACKTEST-REALISM-FIX 2026-07-04: ENFORCE_DISPATCH_REALISM split into
# ENFORCE_STALENESS + ENFORCE_REENTRY — see engine/backtester.py. Sweep still
# defaults both on; per-agent staleness correctness depends on whether that
# agent's dispatch path is actually async (see backtester.py notes).
bt.ENFORCE_STALENESS = True
bt.ENFORCE_REENTRY = True
bt.ENFORCE_COST_MODEL = True


def eligible_agents():
    c = sqlite3.connect("data/trader.db")
    rows = c.execute(
        """
        SELECT player_id, COUNT(*) AS n, MIN(created_at) AS oldest
          FROM signals
         WHERE signal IN ('BUY', 'BUY_CALL', 'BUY_PUT')
         GROUP BY player_id
        HAVING n >= ?
         ORDER BY n DESC
        """,
        (MIN_SIGNALS,),
    ).fetchall()
    c.close()
    return rows


def window_days(oldest: str) -> int:
    """Cover the agent's full history (+ buffer), capped at 365."""
    try:
        first = datetime.fromisoformat(str(oldest).replace("Z", "").replace("T", " ").split(".")[0])
        return min(365, (datetime.now() - first).days + 7)
    except Exception:
        return 180


def slim(result: dict) -> dict:
    s = result.get("stats", {})
    return {
        "signals_tested": result.get("signals_tested"),
        "trades": s.get("total_trades"),
        "win_rate": s.get("win_rate"),
        "total_pnl": s.get("total_pnl"),
        "return_pct": s.get("total_return_pct"),
        "expired_pre_dispatch": s.get("expired_pre_dispatch"),
        "reentry_blocked": s.get("reentry_blocked"),
        "friction_paid": round(s.get("friction_paid") or 0.0, 2),
    }


def save(results: list) -> None:
    """Incremental, crash-safe: rewrite the timestamped file after each agent."""
    with open(OUT, "w") as f:
        json.dump({"generated": datetime.now().isoformat(), "honest": True,
                   "min_signals": MIN_SIGNALS, "agents": results}, f, indent=2)


def main() -> int:
    roster = eligible_agents()
    print(f"[SWEEP] {len(roster)} agents with >= {MIN_SIGNALS} signals -> {OUT}")
    results = []
    for i, (pid, n, oldest) in enumerate(roster, 1):
        days = window_days(oldest)
        print(f"[SWEEP] {i}/{len(roster)} {pid} (n={n}, window={days}d) "
              f"start {datetime.now().strftime('%H:%M:%S')}", flush=True)
        entry = {"player_id": pid, "signals_in_db": n, "oldest": str(oldest),
                 "window_days": days}
        try:
            bt._vix_cache = {}
            guarded = backtest_player(pid, days=days, apply_guardrails=True)
            raw = backtest_player(pid, days=days, apply_guardrails=False)
            entry["guarded"] = slim(guarded)
            entry["raw"] = slim(raw)
            # Health metrics for the reopening decision:
            rb = (entry["raw"].get("reentry_blocked") or 0)
            st = (entry["raw"].get("signals_tested") or 0)
            entry["spam_rate_pct"] = round(rb / st * 100.0, 1) if st else None
            g = entry["guarded"]
            entry["friction_to_pnl"] = (
                round(g["friction_paid"] / abs(g["total_pnl"]), 3)
                if g.get("total_pnl") else None)
        except Exception as exc:
            entry["error"] = f"{type(exc).__name__}: {exc}"
            traceback.print_exc()
        results.append(entry)
        save(results)  # crash-safe checkpoint — keep all data

    # Final ranked table by guarded honest return.
    ok = [r for r in results if "guarded" in r and r["guarded"].get("return_pct") is not None]
    ok.sort(key=lambda r: r["guarded"]["return_pct"], reverse=True)
    print(f"\n{'agent':<28}{'ret%':>8}{'trades':>8}{'WR%':>7}{'spam%':>8}{'friction':>10}")
    for r in ok:
        g = r["guarded"]
        print(f"{r['player_id']:<28}{g['return_pct']:>8.2f}{g['trades']:>8}"
              f"{g['win_rate']:>7.1f}{(r.get('spam_rate_pct') or 0):>8.1f}"
              f"{g['friction_paid']:>10.2f}")
    errs = [r["player_id"] for r in results if "error" in r]
    if errs:
        print(f"[SWEEP] errors: {errs}")
    print(f"[SWEEP] done -> {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
