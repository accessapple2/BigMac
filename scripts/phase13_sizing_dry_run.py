#!/usr/bin/env python3
"""HM-XO-PLAN-2026-09 Phase 1.3 — dry-run demonstration, read-only.

Calls engine.phase13_sizing.compute_alpha_sizing_multiplier() directly against
LIVE data (real composite_alpha scores, real current regime) for a sample of
symbols and confidence levels. Does NOT touch config.PHASE_1_3_ALPHA_SIZING_
ENABLED, does NOT call buy() or execute_signal(), does NOT write anything to
any table. Purpose: let the Admiral see real tier decisions on real data
before ever flipping the flag, per the directive ("I want to read the diff and
the first dry-run output before it sizes anything real").

Run: .venv/bin/python3 scripts/phase13_sizing_dry_run.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from engine.phase13_sizing import compute_alpha_sizing_multiplier
from engine.regime_router import get_current_regime
from engine.calibration_map import has_calibration_evidence
import sqlite3

ALPHA_DB = Path(__file__).resolve().parent.parent / "data" / "alpha_signals.db"


def main() -> None:
    regime = get_current_regime()
    print(f"Live current regime: {regime}\n")

    conn = sqlite3.connect(str(ALPHA_DB), timeout=5)
    rows = conn.execute(
        "SELECT symbol, composite_score FROM composite_alpha "
        "ORDER BY created_at DESC, symbol LIMIT 20"
    ).fetchall()
    conn.close()

    if not rows:
        print("No composite_alpha rows found -- nothing to demonstrate.")
        return

    # Sample confidence values spanning the calibration bins (BIN_EDGES 0.5..1.0).
    sample_confidences = [0.65, 0.80, 0.95]

    print(f"{'symbol':<8} {'alpha':>8} {'conf':>6} {'multiplier':>10}  reason")
    print("-" * 100)
    for symbol, alpha in rows:
        for conf in sample_confidences:
            mult, reason = compute_alpha_sizing_multiplier(symbol, regime, conf)
            print(f"{symbol:<8} {alpha:>8.3f} {conf:>6.2f} {mult:>10.2f}  {reason}")
        print()

    print("=" * 100)
    print("Calibration evidence by bucket, this regime (what has_calibration_evidence()\n"
          "is actually checking against for every 'fail-closed' line above):")
    for conf in sample_confidences:
        has_ev = has_calibration_evidence(regime, conf)
        print(f"  regime={regime} stated_confidence={conf:.2f} -> "
              f"has_calibration_evidence={has_ev}")

    print("\n" + "=" * 100)
    print("HONEST NOTE: as of this run, EVERY (regime, confidence-bucket) in the live\n"
          "DB has fewer than MIN_BUCKET_N=8 real observations (the whole calibration_map\n"
          "pool is ~3 fleet-wide trade_fire-linked rows right now -- see docs/XO_PLAN_\n"
          "2026-09.md's 'Coordination question' blocker note). This means the fail-closed\n"
          "rule is effectively ALWAYS active today: no live symbol/regime/confidence\n"
          "combination can currently earn the 1.0 full tier via the calibration-evidence\n"
          "path, only 1.0 (unaffected, alpha<0.3) or 0.5 (base). The full-tier-with-\n"
          "evidence branch is exercised below with a SYNTHETIC example (not live data)\n"
          "so both code paths are visible, not just the one live data happens to hit today.")

    print("\nSynthetic example (fabricated inputs, NOT from any live table):")
    from unittest.mock import patch
    with patch("engine.phase13_sizing._get_composite_alpha", return_value=0.75), \
         patch("engine.phase13_sizing.has_calibration_evidence", return_value=True):
        mult, reason = compute_alpha_sizing_multiplier("SYNTH_HIGH_ALPHA_WITH_EVIDENCE", "BULL_CROSS", 0.90)
        print(f"  alpha=0.75 (fabricated), evidence=True (fabricated)  -> "
              f"multiplier={mult}  reason={reason}")
    with patch("engine.phase13_sizing._get_composite_alpha", return_value=0.75), \
         patch("engine.phase13_sizing.has_calibration_evidence", return_value=False):
        mult, reason = compute_alpha_sizing_multiplier("SYNTH_HIGH_ALPHA_NO_EVIDENCE", "BULL_CROSS", 0.90)
        print(f"  alpha=0.75 (fabricated), evidence=False (fabricated) -> "
              f"multiplier={mult}  reason={reason}")


if __name__ == "__main__":
    main()
