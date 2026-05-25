"""HM-FLEET-TRAIL-CONVICTION-SCALE Phase D — targeted impact analysis.

Why not the same A/B harness as HM-RISK-MANAGER-CONVICTION-STOP-WIRE
Phase 5b/c?
  The Lane A backtest harness (scripts/hm_conviction_stop_backtest_compare.py)
  exercises engine.backtester._simulate_guarded, which models a gain-tiered
  trail via _v3_trailing_stop_pct(gain) — NOT the flat 3% fleet trail in
  engine/risk_manager.py::check_stop_loss_take_profit that Phase B touched.
  The two trail models are structurally different (gain-scaling vs flat-
  with-allow-list-and-flag); the backtester cannot directly simulate the
  production-trail change.

What this script DOES:
  Walks the current live positions table for each AI_SIGNAL_PLAYERS
  player, looks up each position's conviction, and reports the trail-
  width delta (scaled tier minus flat 3% baseline) per position. This is
  a DIRECTIONAL signal — how many positions are in the wider tiers + how
  much wider would their trails be under flag-on — NOT a P&L simulation.

What this script DOES NOT do:
  - Replay historical trades through the new trail
  - Compute fleet P&L impact
  - Exercise the 4 acceptance gates (G1/G2/G3/G4)

Decision implication: G1-G4 acceptance gates cannot fire on this output.
Admiral's call on whether to ship Phase B behind the flag with shadow-
validate-live (mirror Lane A approval pattern) OR build out a fleet-
trail-specific backtest harness in a separate sprint.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from engine.risk_manager import RiskManager  # noqa: E402
from engine.stops import get_trail_pct  # noqa: E402


DB = "data/trader.db"
FLAT_TRAIL = 0.03


def main() -> int:
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    allow = set(RiskManager.AI_SIGNAL_PLAYERS)

    rows = conn.execute(
        "SELECT id, player_id, symbol, qty, avg_price, conviction, "
        " conviction_source, opened_at "
        "FROM positions "
        "WHERE qty != 0 AND player_id IN ({}) "
        "ORDER BY player_id, symbol".format(",".join("?" * len(allow))),
        list(allow),
    ).fetchall()
    conn.close()

    tier_counts = {0.03: 0, 0.04: 0, 0.05: 0}
    null_conv = 0
    per_player: dict = {}
    deltas: list = []

    for r in rows:
        pid = r["player_id"]
        conv = r["conviction"]
        if conv is None:
            null_conv += 1
            trail = 0.03
        else:
            trail = get_trail_pct(conv)
        tier_counts[trail] = tier_counts.get(trail, 0) + 1
        per_player.setdefault(pid, {"positions": 0, "tier_sum_pp": 0.0})
        per_player[pid]["positions"] += 1
        per_player[pid]["tier_sum_pp"] += (trail - FLAT_TRAIL) * 100.0
        if trail != FLAT_TRAIL:
            deltas.append({
                "player_id": pid,
                "symbol": r["symbol"],
                "conviction": conv,
                "scaled_trail": trail,
                "flat_trail": FLAT_TRAIL,
                "delta_pp": (trail - FLAT_TRAIL) * 100.0,
            })

    print("=" * 75)
    print(f"AI_SIGNAL_PLAYERS open positions: {len(rows)}")
    print(f"  NULL conviction (would inherit flat 3%): {null_conv}")
    print(f"  Trail-width tier distribution (flag-on, allow-list, conv-known):")
    for tier in (0.03, 0.04, 0.05):
        print(f"    {tier:.0%} trail: {tier_counts.get(tier, 0):>3} positions")

    print()
    print("Per-player summary (flag-on minus flat):")
    print(f"  {'player_id':<22} {'positions':>9} {'total Δ pp':>12}")
    for pid in sorted(per_player.keys()):
        p = per_player[pid]
        print(f"  {pid:<22} {p['positions']:>9}  {p['tier_sum_pp']:>+11.2f}")

    print()
    print("Positions where flag-on diverges from flag-off:")
    if not deltas:
        print("  (none — all positions would inherit flat 3% trail regardless)")
    else:
        for d in deltas[:20]:
            print(
                f"  {d['player_id']:<22} {d['symbol']:<6} "
                f"conv={d['conviction']:.2f} "
                f"trail {d['flat_trail']:.0%} -> {d['scaled_trail']:.0%}  "
                f"(+{d['delta_pp']:.0f}pp)"
            )

    print()
    print("INTERPRETATION:")
    if tier_counts.get(0.05, 0) + tier_counts.get(0.04, 0) == 0:
        print("  Zero positions would experience a wider trail under flag-on.")
        print("  Phase B is structurally INERT against the current portfolio.")
        print("  Shipping behind the flag has zero immediate behavior impact.")
    else:
        wide = tier_counts.get(0.05, 0) + tier_counts.get(0.04, 0)
        print(f"  {wide} position(s) would see a wider trail under flag-on.")
        print(f"  Live impact depends on whether those positions reach the trail-trigger band.")
        print(f"  Admiral can ship behind the flag and observe shadow signal post-merge.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
