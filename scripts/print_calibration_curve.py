#!/usr/bin/env python3
"""HM-XO-PLAN-2026-09 Phase 2, item B.2 -- print the confidence calibration curve.

Read-only. Prints stated-confidence-bucket -> realized-hit-rate per
regime, sample size per bucket, and the gap between a stated 0.80 and
the realized rate in each regime. Does not wire anything into sizing.

Usage:
    venv/bin/python3 scripts/print_calibration_curve.py
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from engine.calibration_map import (  # noqa: E402
    MIN_BUCKET_N,
    fit_binned_calibration,
    get_calibrated_confidence,
    load_calibration_data,
)


def main() -> int:
    data = load_calibration_data()
    print(f"Total clean trade_fire observations: {len(data)}\n")
    if not data:
        print("No data -- nothing to calibrate.")
        return 1

    cal = fit_binned_calibration(data)
    for regime in sorted(cal.keys()):
        buckets = cal[regime]
        total_n = sum(b["n"] for b in buckets)
        print(f"=== {regime} (n={total_n}) ===")
        for b in buckets:
            if b["n"] == 0:
                continue
            rate_str = f"{b['hit_rate']*100:.1f}%" if b["hit_rate"] is not None else f"INSUFFICIENT (n<{MIN_BUCKET_N})"
            print(f"  stated [{b['lo']:.1f}-{b['hi']:.1f}) n={b['n']:>3}  "
                  f"avg_stated={b['avg_stated']:.3f}  realized_hit_rate={rate_str}")
        print()

    print("=== Gap at stated confidence 0.80, per regime ===")
    for regime in sorted(cal.keys()):
        calibrated = get_calibrated_confidence(regime, 0.80)
        note = "" if calibrated != 0.80 else "  (bucket insufficient or missing -- passthrough, not a real calibration)"
        gap = calibrated - 0.80
        print(f"  {regime}: stated=0.80 -> calibrated={calibrated:.3f}  (gap={gap:+.3f}){note}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
