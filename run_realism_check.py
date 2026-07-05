#!/usr/bin/env python3
"""HM-BACKTEST-REALISM — before/after comparison runner (read-only).

HM-BACKTEST-REALISM-FIX 2026-07-04: ENFORCE_DISPATCH_REALISM split into
ENFORCE_STALENESS + ENFORCE_REENTRY in engine/backtester.py — they model
unrelated things and don't share a validity condition (see that file's
header comment). Adds a third SYNC-HONEST config (staleness off, reentry +
cost on) for agents empirically confirmed to dispatch save_signal() ->
buy() synchronously, where the staleness poll-race model doesn't apply.
"""
import json, sys
import engine.backtester as bt
from engine.backtester import backtest_player

def run(player, days, staleness, reentry, cost):
    bt.ENFORCE_STALENESS = staleness
    bt.ENFORCE_REENTRY = reentry
    bt.ENFORCE_COST_MODEL = cost
    bt._vix_cache = {}
    out = {}
    for guarded in (False, True):
        r = backtest_player(player, days=days, apply_guardrails=guarded)
        s = r.get("stats", {})
        out["guarded" if guarded else "raw"] = {
            "signals_tested": r.get("signals_tested"),
            "trades": s.get("total_trades"), "win_rate": s.get("win_rate"),
            "total_pnl": s.get("total_pnl"), "return_pct": s.get("total_return_pct"),
            "expired_pre_dispatch": s.get("expired_pre_dispatch"),
            "reentry_blocked": s.get("reentry_blocked"),
            "friction_paid": round(s.get("friction_paid", 0.0), 2)}
    return out

player = sys.argv[1] if len(sys.argv) > 1 else "ollama-plutus"
days = int(sys.argv[2]) if len(sys.argv) > 2 else 90
legacy = run(player, days, False, False, False)
honest = run(player, days, True, True, True)
sync_honest = run(player, days, False, True, True)
print(json.dumps({
    "LEGACY (biased)": legacy,
    "REALISM (honest, all-on)": honest,
    "SYNC-HONEST (staleness off — sync-dispatch agents)": sync_honest,
}, indent=2))
for m in ("raw", "guarded"):
    lp = legacy[m]["return_pct"]
    hp = honest[m]["return_pct"]
    sp = sync_honest[m]["return_pct"]
    if lp is not None and hp is not None and sp is not None:
        print(f"{m}: legacy {lp:+.2f}% -> honest(all-on) {hp:+.2f}% "
              f"-> sync-honest {sp:+.2f}% (legacy->sync-honest delta {sp-lp:+.2f} pts)")
