"""Unit tests for strategies/validation.py (HM-VALIDATION-RIGOR).

Golden test: reproduces the HM-BACKTEST-123 the_continuation OOS DSR = 0.8695.
Frozen inputs were recovered deterministically from the data/backtest.db cache
(block2_holly_ab OOS_Apr15-May29 winner), so this test needs no data and is
reproducible forever.

Run:  .venv-backtest/bin/python3 tests/test_validation.py   (or: pytest)
"""
import math
import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import strategies.validation as V

# ── HM-BACKTEST-123 OOS golden inputs (recovered from cache, frozen) ──────────
OOS = dict(wsr=0.31346740436910037, T=30,
           skew=0.5631099751025335, kurt=2.5405335755178355,
           sr0=0.11972639552354382)
OOS_DSR = 0.8695


def test_reproduce_hm123_oos_dsr():
    dsr = V.deflated_sharpe(OOS["wsr"], OOS["T"], skew=OOS["skew"],
                            kurt=OOS["kurt"], sr0=OOS["sr0"])
    assert round(dsr, 4) == OOS_DSR, f"expected {OOS_DSR}, got {round(dsr,4)}"


def test_oos_fails_095_gate():
    # the_continuation OOS DSR 0.87 < 0.95 -> reads FAIL until it clears (per spec)
    dsr = V.deflated_sharpe(OOS["wsr"], OOS["T"], skew=OOS["skew"],
                            kurt=OOS["kurt"], sr0=OOS["sr0"])
    v = V.graduation_verdict(round(dsr, 4), pbo=0.1)
    assert v["verdict"] == "HOLD" and not v["dsr_passes"]


def test_sr_equals_sr0_gives_half():
    # z=0 when sr_hat==sr0 -> DSR=0.5 exactly
    assert abs(V.deflated_sharpe(0.2, 100, skew=0.0, kurt=3.0, sr0=0.2) - 0.5) < 1e-9


def test_expected_max_sharpe_two_trials():
    sr0, Vv, N = V.expected_max_sharpe([0.31346740436910037, -0.013], n_trials=2)
    # reproduces block2 OOS-ish sr0 from V; N honored
    assert N == 2 and sr0 > 0


def test_more_trials_lowers_dsr():
    # The literature's core point: more variants tested -> higher SR0 -> lower DSR.
    tl = V.TrialLog("sweep")
    tl.add_many([0.30])
    base_sr0, _, _ = tl.sr0(n_trials=2)
    many_sr0, _, _ = V.expected_max_sharpe([0.30, 0.25, 0.22, 0.20, 0.18, 0.15, 0.10], n_trials=50)
    d_few = V.deflated_sharpe(0.30, 100, sr0=base_sr0)
    d_many = V.deflated_sharpe(0.30, 100, sr0=many_sr0)
    assert d_many < d_few


def test_pbo_noise_is_high():
    # Pure noise strategies -> IS-best doesn't generalize -> PBO near 0.5.
    rng = np.random.RandomState(42)
    M = rng.randn(800, 20)
    r = V.cscv_pbo(M, n_blocks=10)
    assert 0.30 <= r["pbo"] <= 0.70, r


def test_pbo_dominant_strategy_is_low():
    # One strategy with consistent edge every block -> IS-best generalizes -> PBO low.
    rng = np.random.RandomState(7)
    M = rng.randn(800, 20) * 0.01
    M[:, 0] += 0.02   # column 0 has a real, persistent positive mean
    r = V.cscv_pbo(M, n_blocks=10)
    assert r["pbo"] <= 0.20, r


def test_pbo_needs_two_strategies():
    r = V.cscv_pbo(np.random.randn(100, 1), n_blocks=4)
    assert r["pbo"] is None


def test_deflate_ranking_sorts_and_verdicts():
    trials = [
        {"name": "strong", "sharpe": 0.30, "T": 200, "skew": 0.1, "kurt": 3.0},
        {"name": "weak", "sharpe": 0.05, "T": 200, "skew": 0.0, "kurt": 3.0},
    ]
    rep = V.deflate_ranking(trials, n_trials=2)
    assert rep["ranking"][0]["name"] == "strong"
    assert rep["ranking"][0]["dsr"] >= rep["ranking"][1]["dsr"]
    assert "verdict" in rep["winner"]


def _run_all():
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    passed = 0
    for fn in fns:
        try:
            fn()
            print(f"  PASS  {fn.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"  FAIL  {fn.__name__}: {e}")
        except Exception as e:
            print(f"  ERROR {fn.__name__}: {type(e).__name__}: {e}")
    print(f"\n{passed}/{len(fns)} passed")
    return passed == len(fns)


if __name__ == "__main__":
    sys.exit(0 if _run_all() else 1)
