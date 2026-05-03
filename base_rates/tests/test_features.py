"""Tests for features module using synthetic data."""
from __future__ import annotations

import numpy as np
import pandas as pd

from base_rates.features import (
    rsi_wilder,
    pct_change_simple,
    forward_return,
    forward_max_drawdown,
    compute_features,
)


def _synth_ohlcv(n: int = 300, seed: int = 42) -> pd.DataFrame:
    """Synthetic OHLCV with mild trend + noise."""
    rng = np.random.default_rng(seed)
    drift = 0.0003
    vol = 0.012
    rets = rng.normal(drift, vol, n)
    close = 100 * np.cumprod(1 + rets)
    high = close * (1 + np.abs(rng.normal(0, 0.005, n)))
    low = close * (1 - np.abs(rng.normal(0, 0.005, n)))
    open_ = close * (1 + rng.normal(0, 0.003, n))
    vol_ = rng.integers(1_000_000, 5_000_000, n)
    idx = pd.date_range("2020-01-01", periods=n, freq="B")
    return pd.DataFrame(
        {"open": open_, "high": high, "low": low, "close": close, "volume": vol_},
        index=idx,
    )


def test_rsi_returns_series_with_nans_at_start():
    df = _synth_ohlcv(100)
    rsi = rsi_wilder(df["close"], period=14)
    assert isinstance(rsi, pd.Series)
    # First 13 should be NaN
    assert rsi.iloc[:13].isna().all()
    # After warmup, in [0, 100]
    valid = rsi.iloc[14:].dropna()
    assert (valid >= 0).all() and (valid <= 100).all()


def test_rsi_known_pattern_only_gains():
    """If price only goes up, RSI should approach 100."""
    close = pd.Series([100 + i for i in range(50)])
    rsi = rsi_wilder(close, period=14)
    last = rsi.iloc[-1]
    assert last == 100.0 or last > 99.0


def test_pct_change():
    s = pd.Series([100, 110, 99])
    pc = pct_change_simple(s)
    assert pc.iloc[0] != pc.iloc[0]  # NaN
    assert abs(pc.iloc[1] - 0.10) < 1e-9
    assert abs(pc.iloc[2] - (-0.10)) < 1e-9


def test_forward_return_5d():
    close = pd.Series([100, 101, 102, 103, 104, 105, 110])
    fr = forward_return(close, days=5)
    # At idx 0: close[5]/close[0] - 1 = 105/100 - 1 = 0.05
    assert abs(fr.iloc[0] - 0.05) < 1e-9
    # At idx 1: 110/101 - 1
    assert abs(fr.iloc[1] - (110/101 - 1)) < 1e-9
    # Last 5 NaN
    assert fr.iloc[-5:].isna().all()


def test_forward_max_drawdown_basic():
    """Constructed: today close = 100, next 5 lows are [99, 98, 95, 97, 99].
    Max drawdown = 95/100 - 1 = -0.05.
    """
    close = pd.Series([100.0] + [100.0] * 5 + [100.0])
    low = pd.Series([100.0, 99.0, 98.0, 95.0, 97.0, 99.0, 100.0])
    md = forward_max_drawdown(low, close, days=5)
    # At idx 0: min of low[1..5] = 95 → 95/100 - 1 = -0.05
    assert abs(md.iloc[0] - (-0.05)) < 1e-9


def test_compute_features_full_pipeline():
    """End-to-end: feed synthetic OHLCV for symbol + VIX + SPY, get a feature frame."""
    n = 350
    sym = _synth_ohlcv(n, seed=1)
    vix = _synth_ohlcv(n, seed=2)
    spy = _synth_ohlcv(n, seed=3)
    feats = compute_features(sym, vix, spy, forward_days=5)

    expected_cols = {
        "close", "pct_change", "rsi14", "rsi_slope",
        "fwd_return", "fwd_maxdd",
        "vix_close", "vix_pct_change", "spy_above_200",
    }
    assert expected_cols.issubset(feats.columns)
    # After 200d SMA warmup + 14d RSI warmup, most rows should have full data
    valid_count = feats.dropna(subset=["pct_change", "rsi14", "rsi_slope",
                                       "vix_close", "vix_pct_change",
                                       "spy_above_200"]).shape[0]
    # Should have plenty of valid rows (n - 200 SMA warmup roughly)
    assert valid_count > 100
