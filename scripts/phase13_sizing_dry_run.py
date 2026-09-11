#!/usr/bin/env python3
"""HM-XO-PLAN-2026-09 Phase 1.3 — dry-run demonstration, read-only.

Rebuilt 2026-09-11 after Admiral review of the first version. Two sections:

1. Live sample: compute_alpha_sizing_multiplier() against today's live
   composite_alpha for a sample of symbols, showing all four ladder outcomes
   (full/base/low/no-data) on real current data.
2. Real replay: McCoy's actual last 10 trade_fire decisions (decision_audit),
   with the sizing multiplier each WOULD have gotten, using composite_alpha's
   own as_of_date history to look up the value AS OF that decision's date
   (not today's value) -- this is what the Admiral asked for after finding
   composite_alpha already carries real daily history (64 distinct as_of_date
   values back to 2026-04-09) that the first version of this script ignored.

Does NOT touch config.PHASE_1_3_ALPHA_SIZING_ENABLED, does NOT call buy() or
execute_signal(), does NOT write anything to any table.

Run: .venv/bin/python3 scripts/phase13_sizing_dry_run.py
"""
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from engine.phase13_sizing import compute_alpha_sizing_multiplier, compute_calibration_sizing_multiplier

ALPHA_DB = Path(__file__).resolve().parent.parent / "data" / "alpha_signals.db"
TRADER_DB = Path(__file__).resolve().parent.parent / "data" / "trader.db"


def section_live_sample() -> None:
    print("=" * 110)
    print("SECTION 1 — live sample: compute_alpha_sizing_multiplier(symbol) against TODAY's")
    print("composite_alpha (as_of=None -> most recent as_of_date). Every ladder branch shown.")
    print("=" * 110)
    conn = sqlite3.connect(str(ALPHA_DB), timeout=5)
    rows = conn.execute(
        "SELECT DISTINCT symbol FROM composite_alpha ORDER BY symbol LIMIT 15"
    ).fetchall()
    conn.close()
    symbols = [r[0] for r in rows]
    # Add a symbol guaranteed to have zero composite_alpha rows, to show the
    # "no data" branch explicitly rather than only by accident.
    symbols.append("ZZZNODATA")

    print(f"{'symbol':<12}{'multiplier':>12}  reason")
    print("-" * 110)
    for symbol in symbols:
        mult, reason = compute_alpha_sizing_multiplier(symbol)
        print(f"{symbol:<12}{mult:>12.2f}  {reason}")


def section_real_replay() -> None:
    print("\n" + "=" * 110)
    print("SECTION 2 — real replay: McCoy's last 10 real trade_fire decisions, sized with the")
    print("composite_alpha value AS OF each decision's own date (as_of=<decision date>), not")
    print("today's value. 'actual_mult' is what really happened (Phase 1.3 has never been on,")
    print("so it is always 1.0).")
    print("=" * 110)
    conn = sqlite3.connect(str(TRADER_DB), timeout=5)
    rows = conn.execute("""
        SELECT symbol, confidence, regime, created_at
        FROM decision_audit
        WHERE player_id='ollama-plutus' AND event_type='trade_fire'
        ORDER BY created_at DESC LIMIT 10
    """).fetchall()
    conn.close()

    print(f"{'symbol':<8}{'conf':<7}{'regime':<15}{'decided_at':<21}{'would-be':<10}{'actual':<8}reason")
    print("-" * 150)
    for symbol, confidence, regime, created_at in rows:
        as_of_date = created_at[:10]  # 'YYYY-MM-DD HH:MM:SS' -> 'YYYY-MM-DD'
        mult, reason = compute_alpha_sizing_multiplier(symbol, as_of=as_of_date)
        print(f"{symbol:<8}{confidence:<7}{regime:<15}{created_at:<21}{mult:<10.2f}{1.0:<8.2f}{reason}")


def section_calibration_dimension() -> None:
    print("\n" + "=" * 110)
    print("SECTION 3 — calibration dimension (compute_calibration_sizing_multiplier), SEPARATE")
    print("from the alpha ladder above and NOT wired into any live call site. Shown here only")
    print("to demonstrate it runs correctly in isolation, using the real, current, live")
    print("regime/confidence buckets -- see engine/phase13_sizing.py for why this is unwired.")
    print("=" * 110)
    from engine.regime_router import get_current_regime
    regime = get_current_regime()
    print(f"Live current regime: {regime}\n")
    for conf in (0.65, 0.80, 0.95):
        mult, reason = compute_calibration_sizing_multiplier(regime, conf)
        print(f"  regime={regime} stated_confidence={conf:.2f} -> multiplier={mult}  {reason}")

    print("\nLive per-bucket observation counts vs. MIN_BUCKET_N (why every live bucket above")
    print("is 'no evidence' today):")
    from engine.calibration_map import load_calibration_data, fit_binned_calibration, MIN_BUCKET_N
    data = load_calibration_data()
    print(f"  Total clean trade_fire-linked settled rows, fleet-wide: {len(data)}  (MIN_BUCKET_N={MIN_BUCKET_N})")
    m = fit_binned_calibration(data)
    any_nonzero = False
    for reg, buckets in m.items():
        for b in buckets:
            if b["n"] > 0:
                any_nonzero = True
                status = "HAS EVIDENCE" if b["hit_rate"] is not None else f"fail-closed (needs {MIN_BUCKET_N - b['n']} more)"
                print(f"    {reg:15s} conf[{b['lo']:.1f},{b['hi']:.1f})  n={b['n']:3d}  {status}")
    if not any_nonzero:
        print("    (no bucket has any observations at all)")


if __name__ == "__main__":
    section_live_sample()
    section_real_replay()
    section_calibration_dimension()
