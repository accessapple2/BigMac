"""Feature computation: RSI(14), forward 5d return, forward 5d max drawdown.

All functions take pandas Series/DataFrame and return new columns. No I/O.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def rsi_wilder(close: pd.Series, period: int = 14) -> pd.Series:
    """RSI(14) using Wilder's smoothing (the standard).

    Matches TradingView/most platforms. Returns NaN for the first `period` rows.
    """
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    # Wilder's smoothing == EWM with alpha = 1/period
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    # Where avg_loss is 0, RSI is 100; where both 0 (flat), set NaN
    rsi = rsi.where(avg_loss != 0, 100.0)
    rsi = rsi.where(~((avg_gain == 0) & (avg_loss == 0)), np.nan)
    return rsi


def pct_change_simple(close: pd.Series) -> pd.Series:
    """Day-over-day pct change as decimal."""
    return close.pct_change(fill_method=None)


def forward_return(close: pd.Series, days: int = 5) -> pd.Series:
    """Forward N-day return as decimal: close[t+N]/close[t] - 1.

    The last N rows will be NaN (no future data yet).
    """
    return close.shift(-days) / close - 1


def forward_max_drawdown(low: pd.Series, close: pd.Series, days: int = 5) -> pd.Series:
    """Worst intraday drawdown over the next N trading days.

    Computed as min(low[t+1..t+N]) / close[t] - 1. Returns negative numbers
    (or 0 if price never went below today's close intraday over the window).
    Last N rows NaN.
    """
    # Rolling min of next N lows (exclusive of today): shift -1 first
    fwd_low = low.shift(-1).rolling(window=days, min_periods=days).min()
    # Align: at index t, fwd_low has min of low[t+1..t+N] when shifted properly.
    # rolling().min() at row t covers rows [t-N+1..t] of the shifted series,
    # which corresponds to original rows [t-N+2..t+1]. We need [t+1..t+N], so
    # shift the result by -(N-1):
    fwd_low = fwd_low.shift(-(days - 1))
    return fwd_low / close - 1


def compute_features(
    df: pd.DataFrame,
    vix_df: pd.DataFrame,
    spy_df: pd.DataFrame,
    forward_days: int = 5,
    rsi_period: int = 14,
) -> pd.DataFrame:
    """Build the full feature row for one symbol.

    df:     symbol OHLCV indexed by date (DatetimeIndex). Columns: open, high, low, close, volume.
    vix_df: ^VIX OHLCV indexed by date. We use 'close' and its pct_change.
    spy_df: SPY OHLCV indexed by date. We compute SPY > 200d SMA.

    Returns a DataFrame with columns ready for insert into base_rate_features.
    """
    out = pd.DataFrame(index=df.index.copy())
    out["close"] = df["close"]
    out["pct_change"] = pct_change_simple(df["close"])
    out["rsi14"] = rsi_wilder(df["close"], period=rsi_period)
    out["rsi_slope"] = out["rsi14"].diff()
    out["fwd_return"] = forward_return(df["close"], days=forward_days)
    out["fwd_maxdd"] = forward_max_drawdown(df["low"], df["close"], days=forward_days)

    # Join VIX
    vix_close = vix_df["close"].reindex(out.index, method=None)
    vix_pct = vix_close.pct_change(fill_method=None)
    out["vix_close"] = vix_close
    out["vix_pct_change"] = vix_pct

    # SPY trend: close > SMA(200)
    spy_close = spy_df["close"].reindex(out.index, method=None)
    spy_sma200 = spy_close.rolling(window=200, min_periods=200).mean()
    out["spy_above_200"] = (spy_close > spy_sma200).astype("Int64")

    return out
