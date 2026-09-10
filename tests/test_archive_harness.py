"""Tests for scripts/archive_harness.py (HM-VALIDATION-RIGOR rebuild).

Cross-validates the stdlib-only reimplementation against
strategies/validation.py's scipy-based golden test (same frozen HM-BACKTEST-123
OOS inputs), then covers the three contract points that motivated the
rebuild: provenance ceiling, hard refusal without trials_tested, and
options-ratio filtering.
"""
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import scripts.archive_harness as H

# ── Frozen golden inputs, identical to tests/test_validation.py ─────────────
OOS = dict(wsr=0.31346740436910037, T=30,
           skew=0.5631099751025335, kurt=2.5405335755178355,
           sr0=0.11972639552354382)
OOS_DSR = 0.8695


def test_reproduce_hm123_oos_dsr():
    """Same frozen inputs as strategies/validation.py's golden test — proves
    the stdlib NormalDist-based CDF matches scipy.stats.norm.cdf here."""
    dsr = H.deflated_sharpe(OOS["wsr"], OOS["T"], skew=OOS["skew"],
                            kurt=OOS["kurt"], sr0=OOS["sr0"])
    assert round(dsr, 4) == OOS_DSR


def test_sr_equals_sr0_gives_half():
    assert abs(H.deflated_sharpe(0.2, 100, skew=0.0, kurt=3.0, sr0=0.2) - 0.5) < 1e-9


def test_deflated_sharpe_none_on_tiny_T():
    assert H.deflated_sharpe(0.5, 1) is None
    assert H.deflated_sharpe(0.5, None) is None


def test_skew_kurtosis_cross_validated_against_scipy_backed_module():
    """strategies/validation.py wraps scipy directly — use it as ground truth
    for the stdlib moment reimplementation on an independent synthetic series."""
    strategies_validation = pytest.importorskip("strategies.validation")
    returns = [0.02, -0.01, 0.03, -0.04, 0.01, 0.05, -0.02, 0.015, -0.005, 0.07, -0.03, 0.02]
    ref = strategies_validation.trade_metrics(returns)
    assert abs(H.skewness(returns) - ref["skew"]) < 1e-9
    assert abs(H.excess_kurtosis_plus3(returns) - ref["kurtosis"]) < 1e-9


def test_skew_kurtosis_fallback_on_small_n():
    assert H.skewness([0.01, 0.02]) == 0.0          # n < 3
    assert H.excess_kurtosis_plus3([0.01, 0.02, 0.03]) == 3.0  # n < 4


# ── Contract point 1: hard refusal without / with insufficient trials_tested ─
def test_expected_max_sharpe_requires_trials_tested_arg():
    with pytest.raises(TypeError):
        H.expected_max_sharpe([0.1, 0.2, 0.3])  # trials_tested omitted


def test_expected_max_sharpe_refuses_undercount():
    with pytest.raises(ValueError, match="undercounting"):
        H.expected_max_sharpe([0.1, 0.2, 0.3], trials_tested=2)  # 3 sharpes, claims 2 trials


def test_rank_strategies_requires_trials_tested_kwarg():
    conn = sqlite3.connect(":memory:")
    with pytest.raises(TypeError):
        H.rank_strategies(conn, 1)  # trials_tested omitted entirely


# ── Contract point 2: provenance ceiling ─────────────────────────────────────
def _make_test_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE trades (
            player_id TEXT, symbol TEXT, action TEXT, asset_type TEXT,
            entry_price REAL, price REAL, realized_pnl REAL, season INTEGER
        )
    """)
    return conn


def _insert(conn, player_id, asset_type, entry_price, price, season=1, pnl=1.0):
    conn.execute(
        "INSERT INTO trades (player_id, symbol, action, asset_type, entry_price, price, "
        "realized_pnl, season) VALUES (?, 'XYZ', 'SELL', ?, ?, ?, ?, ?)",
        (player_id, asset_type, entry_price, price, pnl, season),
    )


def test_ceiling_insufficient_n():
    conn = _make_test_db()
    for entry, exitp in [(100, 101), (100, 99)]:  # only 2 trades, below min_n=5
        _insert(conn, "tiny-strategy", "stock", entry, exitp)
    conn.commit()
    report = H.rank_strategies(conn, 1, trials_tested=1, min_n=5)
    row = next(r for r in report["results"] if r.player_id == "tiny-strategy")
    assert row.ceiling == "INSUFFICIENT_N"
    assert row.dsr is None and row.psr is None


def test_ceiling_options_excluded_and_ratio_filter():
    conn = _make_test_db()
    # 5 plausible stock trades (keeps n >= min_n)
    for entry, exitp in [(100, 105), (100, 95), (100, 110), (100, 90), (100, 102)]:
        _insert(conn, "mixed-strategy", "stock", entry, exitp)
    # 1 plausible option close (premium-scale, ratio well under 15x)
    _insert(conn, "mixed-strategy", "option", 10.0, 12.0)
    # 1 corrupted option close — price is the underlying's, not the premium (ratio >> 15x)
    _insert(conn, "mixed-strategy", "option", 10.0, 250.0)
    conn.commit()

    report = H.rank_strategies(conn, 1, trials_tested=1, min_n=5)
    row = next(r for r in report["results"] if r.player_id == "mixed-strategy")
    assert row.ceiling == "OPTIONS_EXCLUDED"
    assert row.n_options_excluded == 1
    assert row.n == 6  # 5 stock + 1 kept option, corrupted one excluded
    assert row.dsr is not None  # still computable — not withheld


def test_ceiling_ok_when_clean():
    conn = _make_test_db()
    for entry, exitp in [(100, 105), (100, 95), (100, 110), (100, 90), (100, 102)]:
        _insert(conn, "clean-strategy", "stock", entry, exitp)
    conn.commit()
    report = H.rank_strategies(conn, 1, trials_tested=1, min_n=5)
    row = next(r for r in report["results"] if r.player_id == "clean-strategy")
    assert row.ceiling == "OK"
    assert row.n_options_excluded == 0


# ── Contract point 3: no naive compounding in the reported numbers ──────────
def test_no_total_return_field_exists():
    """StrategyResult intentionally has no cumulative/compounded total-return
    field — see design note #4. Guards against it silently coming back."""
    field_names = {f for f in H.StrategyResult.__dataclass_fields__}
    assert "total_return_pct" not in field_names
    assert {"mean_return_pct", "median_return_pct"} <= field_names


# ── End-to-end sanity ────────────────────────────────────────────────────────
def test_rank_strategies_end_to_end_sorts_by_dsr():
    conn = _make_test_db()
    # strategy A: consistently positive, larger n
    for i in range(10):
        _insert(conn, "A", "stock", 100, 103 + i * 0.1)
    # strategy B: consistently negative
    for i in range(10):
        _insert(conn, "B", "stock", 100, 97 - i * 0.1)
    conn.commit()
    report = H.rank_strategies(conn, 1, trials_tested=2, min_n=5)
    assert report["results"][0].player_id == "A"
    assert report["results"][0].dsr >= report["results"][-1].dsr
