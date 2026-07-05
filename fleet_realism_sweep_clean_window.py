#!/usr/bin/env python3
"""HM-FLEET-REBASELINE-2026-07-04 clean-window variant of fleet_realism_sweep.py.

Restricts every agent's backtest to signals created >= GATE 0 cutoff
(2026-05-14 -- see docs/XO_BACKLOG.md GATE 0), instead of each agent's full
lifetime history. Same slim()/methodology as fleet_realism_sweep.py, but uses
backtest_player(start_date=...) instead of days=... so the window is anchored
to the clean-data boundary rather than "now minus N days".

Roster is fixed to the 22 player_ids in reports/fleet_realism_sweep_20260704_073227.json
so the two reports are directly comparable agent-for-agent.

Doctrine compliance: incremental crash-safe save after every agent; never
overwrites the original sweep; keeps all data (including n=0 clean-window
agents, reported explicitly rather than dropped).

Run:
    nohup venv/bin/python -u fleet_realism_sweep_clean_window.py > logs/fleet_sweep_clean.log 2>&1 &
"""
import json
import sqlite3
import sys
import traceback
from datetime import datetime

import engine.backtester as bt
from engine.backtester import backtest_player

CLEAN_CUTOFF = "2026-05-14"
ORIG_REPORT = "reports/fleet_realism_sweep_20260704_073227.json"
STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
OUT = f"reports/fleet_realism_sweep_clean_{STAMP}.json"

bt.ENFORCE_STALENESS = True
bt.ENFORCE_REENTRY = True
bt.ENFORCE_COST_MODEL = True


def roster_from_original():
    with open(ORIG_REPORT) as f:
        orig = json.load(f)
    return [a["player_id"] for a in orig["agents"]]


def clean_signal_count(player_id: str):
    c = sqlite3.connect("data/trader.db")
    row = c.execute(
        """
        SELECT COUNT(*), MIN(created_at)
          FROM signals
         WHERE player_id = ? AND signal IN ('BUY', 'BUY_CALL', 'BUY_PUT')
           AND created_at >= ?
        """,
        (player_id, CLEAN_CUTOFF),
    ).fetchone()
    c.close()
    return row[0], row[1]


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
    with open(OUT, "w") as f:
        json.dump({
            "generated": datetime.now().isoformat(),
            "honest": True,
            "clean_cutoff": CLEAN_CUTOFF,
            "compares_to": ORIG_REPORT,
            "agents": results,
        }, f, indent=2)


def main() -> int:
    roster = roster_from_original()
    print(f"[CLEAN-SWEEP] {len(roster)} agents (fixed roster from {ORIG_REPORT}) -> {OUT}")
    results = []
    for i, pid in enumerate(roster, 1):
        n, oldest = clean_signal_count(pid)
        print(f"[CLEAN-SWEEP] {i}/{len(roster)} {pid} (clean_signals_in_db={n}) "
              f"start {datetime.now().strftime('%H:%M:%S')}", flush=True)
        entry = {"player_id": pid, "clean_signals_in_db": n, "clean_oldest": str(oldest) if oldest else None}
        if n == 0:
            entry["note"] = "no signals since clean cutoff -- cannot backtest"
            results.append(entry)
            save(results)
            continue
        try:
            bt._vix_cache = {}
            guarded = backtest_player(pid, start_date=CLEAN_CUTOFF, apply_guardrails=True)
            raw = backtest_player(pid, start_date=CLEAN_CUTOFF, apply_guardrails=False)
            entry["guarded"] = slim(guarded)
            entry["raw"] = slim(raw)
            rb = (entry["raw"].get("reentry_blocked") or 0)
            st = (entry["raw"].get("signals_tested") or 0)
            entry["spam_rate_pct"] = round(rb / st * 100.0, 1) if st else None
            g = entry["guarded"]
            entry["friction_to_pnl"] = (
                round(g["friction_paid"] / abs(g["total_pnl"]), 3)
                if g.get("total_pnl") else None)
            if (entry["guarded"].get("signals_tested") or 0) < 100:
                entry["thin_sample_warning"] = (
                    f"only {entry['guarded'].get('signals_tested')} clean signals tested "
                    f"(< 100 threshold used by the original sweep) -- low confidence"
                )
        except Exception as exc:
            entry["error"] = f"{type(exc).__name__}: {exc}"
            traceback.print_exc()
        results.append(entry)
        save(results)

    ok = [r for r in results if "guarded" in r and r["guarded"].get("return_pct") is not None]
    ok.sort(key=lambda r: r["guarded"]["return_pct"], reverse=True)
    print(f"\n{'agent':<28}{'clean_n':>9}{'ret%':>8}{'trades':>8}{'WR%':>7}")
    for r in ok:
        g = r["guarded"]
        print(f"{r['player_id']:<28}{r['clean_signals_in_db']:>9}{g['return_pct']:>8.2f}"
              f"{g['trades']:>8}{(g['win_rate'] or 0):>7.1f}")
    zero = [r["player_id"] for r in results if r.get("clean_signals_in_db") == 0]
    if zero:
        print(f"[CLEAN-SWEEP] zero clean-window signals (cannot assess): {zero}")
    errs = [r["player_id"] for r in results if "error" in r]
    if errs:
        print(f"[CLEAN-SWEEP] errors: {errs}")
    print(f"[CLEAN-SWEEP] done -> {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
