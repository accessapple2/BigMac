#!/usr/bin/env python3
"""HM-BACKTEST-REALISM — before/after comparison runner.

Runs backtest_compare twice per player: once with realism flags OFF
(legacy/biased numbers) and once ON (honest numbers). Prints a compact
side-by-side. Run from ~/autonomous-trader:

    python3 run_realism_check.py                 # default: ollama-plutus, 90d
    python3 run_realism_check.py navigator 180   # any player id, any days
"""
import json
import sys

import engine.backtester as bt
from engine.backtester import backtest_player


def run(player: str, days: int, realism: bool) -> dict:
    bt.ENFORCE_DISPATCH_REALISM = realism
    bt.ENFORCE_COST_MODEL = realism
    bt._vix_cache = {}
    out = {}
    for guarded in (False, True):
        r = backtest_player(player, days=days, apply_guardrails=guarded)
        s = r.get("stats", {})
        out["guarded" if guarded else "raw"] = {
            "signals_tested": r.get("signals_tested"),
            "trades": s.get("total_trades"),
            "win_rate": s.get("win_rate"),
            "total_pnl": s.get("total_pnl"),
            "return_pct": s.get("total_return_pct"),
            "expired_pre_dispatch": s.get("expired_pre_dispatch"),
            "reentry_blocked": s.get("reentry_blocked"),
            "friction_paid": round(s.get("friction_paid", 0.0), 2),
        }
    return out


def main() -> int:
    player = sys.argv[1] if len(sys.argv) > 1 else "ollama-plutus"
    days = int(sys.argv[2]) if len(sys.argv) > 2 else 90
    print(f"=== {player} / {days}d ===")
    legacy = run(player, days, realism=False)
    honest = run(player, days, realism=True)
    print(json.dumps({"LEGACY (biased)": legacy, "REALISM (honest)": honest}, indent=2))
    for mode in ("raw", "guarded"):
        lp, hp = legacy[mode]["return_pct"], honest[mode]["return_pct"]
        if lp is not None and hp is not None:
            print(f"{mode}: legacy {lp:+.2f}% -> honest {hp:+.2f}%  (delta {hp - lp:+.2f} pts)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
