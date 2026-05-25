"""HM-OPTIONS-CONVICTION-STOP-WIRE Phase D — targeted impact analysis.

Same structural backtester limitation as HM-FLEET-TRAIL-CONVICTION-SCALE
Phase D (banked as HM-FLEET-TRAIL-BACKTEST-HARNESS): engine.backtester
does not simulate the options stop path. The Lane A backtest harness
exercises equity-only logic; options-stop A/B requires a dedicated
harness extension.

This script reports the impact-shape against current live options
positions. NOT a P&L A/B — directional input only.

DOCTRINE DEVIATION NOTE: unlike the entry-stop + fleet-trail layers,
the options tier table inverts the floor invariant — low-conviction
positions get TIGHTER stops than the current 50% baseline. This is
intentional (Admiral-locked, theta-decay rationale). See Phase E's
Rule #5 amendment in docs/DOCTRINE.md.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from engine.risk_manager import RiskManager  # noqa: E402
from engine.stops import get_options_stop_pct  # noqa: E402

DB = "data/trader.db"
FLAT_BASELINE = 0.50  # config.OPTIONS_STOP_LOSS_PCT default


def main() -> int:
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    allow = set(RiskManager.AI_SIGNAL_PLAYERS)

    rows = conn.execute(
        "SELECT id, player_id, symbol, option_type, strike_price, "
        "       expiry_date, conviction, conviction_source, opened_at "
        "  FROM positions "
        " WHERE asset_type = 'option' "
        " ORDER BY player_id, symbol",
    ).fetchall()
    conn.close()

    if not rows:
        print("No options positions in the fleet currently.")
        print()
        print("Future-impact projection:")
        print("  If any AI_SIGNAL_PLAYERS player opens an options position with")
        print(f"  conviction >= 0.90  -> 0.50 stop (preserves current 50% baseline)")
        print(f"  conviction >= 0.80  -> 0.40 stop")
        print(f"  conviction <  0.80  -> 0.30 stop (TIGHTER than 0.50 baseline)")
        print()
        print("INTERPRETATION:")
        print("  Phase B is structurally INERT against the current portfolio")
        print("  (no options positions exist). Flag flip would have zero")
        print("  immediate effect; first impact is on next options BUY.")
        return 0

    in_allow_list = 0
    out_allow_list = 0
    null_conv = 0
    tier_counts = {0.30: 0, 0.40: 0, 0.50: 0}
    deltas: list = []

    for r in rows:
        pid = r["player_id"]
        if pid not in allow:
            out_allow_list += 1
            continue
        in_allow_list += 1
        conv = r["conviction"]
        if conv is None:
            null_conv += 1
            scaled = FLAT_BASELINE
        else:
            scaled = get_options_stop_pct(conv)
        tier_counts[scaled] = tier_counts.get(scaled, 0) + 1
        if scaled != FLAT_BASELINE:
            deltas.append({
                "player_id":  pid,
                "symbol":     r["symbol"],
                "option_type": r["option_type"],
                "strike":     r["strike_price"],
                "expiry":     r["expiry_date"],
                "conviction": conv,
                "scaled":     scaled,
                "flat":       FLAT_BASELINE,
                "delta_pp":   (scaled - FLAT_BASELINE) * 100.0,
            })

    print("=" * 75)
    print(f"Total options positions:          {len(rows)}")
    print(f"  In AI_SIGNAL_PLAYERS allow:     {in_allow_list}")
    print(f"  Outside allow (would inherit flat): {out_allow_list}")
    print(f"  NULL conviction (allow + flat fallback): {null_conv}")
    print()
    print("Scaled-stop tier distribution (flag-on path):")
    for tier in (0.50, 0.40, 0.30):
        print(f"  {int(tier*100)}% stop: {tier_counts.get(tier, 0):>3} positions")

    print()
    print("Positions where flag-on diverges from flag-off:")
    if not deltas:
        print("  (none — all positions would inherit flat 50% stop regardless)")
    else:
        for d in deltas:
            print(
                f"  {d['player_id']:<22} {d['symbol']:<5} "
                f"{d['option_type']:<4} K=${d['strike']:.2f} exp={d['expiry']} "
                f"conv={d['conviction']:.2f} "
                f"stop {int(d['flat']*100)}% -> {int(d['scaled']*100)}%  "
                f"({d['delta_pp']:+.0f}pp)"
            )

    print()
    print("INTERPRETATION:")
    if len(deltas) == 0:
        print("  Phase B is structurally INERT against the current portfolio.")
        print("  Flag flip would have zero immediate effect.")
    else:
        tightening = sum(1 for d in deltas if d["scaled"] < d["flat"])
        widening = sum(1 for d in deltas if d["scaled"] > d["flat"])
        print(f"  {len(deltas)} position(s) would diverge under flag-on.")
        print(f"  {tightening} would TIGHTEN (low-conv tier 30%, doctrine deviation).")
        print(f"  {widening} would widen (n/a — top tier preserves 50% baseline).")
        print()
        print("  Live impact depends on whether divergent positions reach the")
        print("  premium-drop band that triggers the stop. Tighter stops kick out")
        print("  faster — protective on theta decay, premature on recovery.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
