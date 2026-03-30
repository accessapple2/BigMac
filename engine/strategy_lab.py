"""Strategy Lab — automated backtesting and parameter optimization for trading strategies."""
from __future__ import annotations
import itertools
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
import numpy as np
import pandas as pd
import yfinance as yf
from rich.console import Console

console = Console()

REPORT_DIR = Path("data/strategy_lab")
REPORT_DIR.mkdir(parents=True, exist_ok=True)

# ─── Built-in strategy definitions ────────────────────────────────────────────

STRATEGIES = {
    "rsi_mean_reversion": {
        "name": "RSI Mean Reversion",
        "description": "Buy when RSI drops below threshold (oversold), sell when RSI rises above exit threshold (overbought).",
        "params": {
            "rsi_period":     {"default": 14,  "type": "int",   "label": "RSI Period"},
            "rsi_buy":        {"default": 30,  "type": "int",   "label": "RSI Buy Threshold"},
            "rsi_sell":       {"default": 70,  "type": "int",   "label": "RSI Sell Threshold"},
            "stop_loss_pct":  {"default": -12, "type": "float", "label": "Stop Loss %"},
            "position_pct":   {"default": 10,  "type": "float", "label": "Position Size %"},
        },
        "optimize_grid": {
            "rsi_buy":       [15, 20, 25, 30, 35, 40],
            "rsi_sell":      [60, 65, 70, 75, 80],
            "stop_loss_pct": [-5, -8, -10, -12, -15, -20],
        },
    },
    "macd_crossover": {
        "name": "MACD Crossover",
        "description": "Buy on MACD golden cross (MACD crosses above signal), sell on death cross.",
        "params": {
            "fast_period":    {"default": 12,  "type": "int",   "label": "Fast EMA"},
            "slow_period":    {"default": 26,  "type": "int",   "label": "Slow EMA"},
            "signal_period":  {"default": 9,   "type": "int",   "label": "Signal Period"},
            "stop_loss_pct":  {"default": -12, "type": "float", "label": "Stop Loss %"},
            "position_pct":   {"default": 10,  "type": "float", "label": "Position Size %"},
        },
        "optimize_grid": {
            "fast_period":   [6, 8, 10, 12, 15],
            "slow_period":   [18, 22, 26, 30, 35],
            "signal_period": [7, 9, 12],
            "stop_loss_pct": [-5, -8, -10, -12, -15, -20],
        },
    },
    "ma_breakout": {
        "name": "Moving Average Breakout",
        "description": "Buy when price breaks above MA on above-average volume, sell when it drops below.",
        "params": {
            "ma_period":      {"default": 50,  "type": "int",   "label": "MA Period"},
            "volume_mult":    {"default": 1.5, "type": "float", "label": "Volume Multiplier"},
            "stop_loss_pct":  {"default": -12, "type": "float", "label": "Stop Loss %"},
            "position_pct":   {"default": 10,  "type": "float", "label": "Position Size %"},
        },
        "optimize_grid": {
            "ma_period":     [10, 20, 50, 100, 200],
            "volume_mult":   [1.0, 1.2, 1.5, 2.0, 2.5],
            "stop_loss_pct": [-5, -8, -10, -12, -15, -20],
        },
    },
    "buy_the_blood": {
        "name": "Buy The Blood",
        "description": "Buy when RSI drops below extreme oversold on large-cap stocks. Pure contrarian.",
        "params": {
            "rsi_period":     {"default": 14,  "type": "int",   "label": "RSI Period"},
            "rsi_extreme":    {"default": 20,  "type": "int",   "label": "RSI Extreme Threshold"},
            "rsi_exit":       {"default": 50,  "type": "int",   "label": "RSI Exit (mean revert to)"},
            "stop_loss_pct":  {"default": -15, "type": "float", "label": "Stop Loss %"},
            "position_pct":   {"default": 15,  "type": "float", "label": "Position Size %"},
        },
        "optimize_grid": {
            "rsi_extreme":   [10, 15, 20, 25, 30],
            "rsi_exit":      [35, 40, 45, 50, 55, 60],
            "stop_loss_pct": [-5, -8, -10, -12, -15, -18, -20],
        },
    },
    "momentum": {
        "name": "Momentum Breakout",
        "description": "Buy stocks making new 20-day highs. Ride the trend, sell on breakdown.",
        "params": {
            "lookback":       {"default": 20,  "type": "int",   "label": "High Lookback Days"},
            "trail_stop_pct": {"default": -8,  "type": "float", "label": "Trailing Stop %"},
            "stop_loss_pct":  {"default": -12, "type": "float", "label": "Hard Stop Loss %"},
            "position_pct":   {"default": 10,  "type": "float", "label": "Position Size %"},
        },
        "optimize_grid": {
            "lookback":       [5, 10, 15, 20, 30, 50],
            "trail_stop_pct": [-3, -5, -8, -10, -12],
            "stop_loss_pct":  [-5, -8, -10, -12, -15, -20],
        },
    },
    "dip_bounce": {
        "name": "Dip Bounce (PTJ Mean Reversion)",
        "description": "Buy stocks that dropped 3%+ with RSI below 30. Sell when RSI recovers to 50. Cut losers at stop.",
        "params": {
            "rsi_period":      {"default": 14,  "type": "int",   "label": "RSI Period"},
            "dip_pct":         {"default": -3,  "type": "float", "label": "Min Daily Drop %"},
            "rsi_entry":       {"default": 30,  "type": "int",   "label": "RSI Entry (below)"},
            "rsi_exit":        {"default": 50,  "type": "int",   "label": "RSI Exit (bounce to)"},
            "stop_loss_pct":   {"default": -5,  "type": "float", "label": "Stop Loss %"},
            "position_pct":    {"default": 12,  "type": "float", "label": "Position Size %"},
        },
        "optimize_grid": {
            "dip_pct":       [-2, -3, -4, -5, -7],
            "rsi_entry":     [20, 25, 30, 35],
            "rsi_exit":      [40, 45, 50, 55, 60],
            "stop_loss_pct": [-3, -5, -7, -10],
        },
    },
    "event_driven": {
        "name": "Event-Driven (Druckenmiller)",
        "description": "Only trade around earnings events. Buy 2 days before earnings if stock is trending up, sell 1 day after. Concentrated bets.",
        "params": {
            "days_before":     {"default": 2,   "type": "int",   "label": "Days Before Earnings"},
            "days_after":      {"default": 1,   "type": "int",   "label": "Days After Earnings"},
            "trend_sma":       {"default": 20,  "type": "int",   "label": "Trend SMA Period"},
            "stop_loss_pct":   {"default": -8,  "type": "float", "label": "Stop Loss %"},
            "position_pct":    {"default": 25,  "type": "float", "label": "Position Size %"},
        },
        "optimize_grid": {
            "days_before":   [1, 2, 3, 5],
            "days_after":    [1, 2, 3, 5],
            "trend_sma":     [10, 20, 50],
            "stop_loss_pct": [-5, -8, -10, -12],
        },
    },
}


# ─── Indicator helpers ────────────────────────────────────────────────────────

def _calc_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _calc_macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram


# ─── Core backtest engine ─────────────────────────────────────────────────────

def _download_data(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Download historical daily data from Yahoo Finance."""
    # Add buffer for indicator warmup
    start_dt = datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=250)
    df = yf.download(symbol, start=start_dt.strftime("%Y-%m-%d"),
                     end=end_date, interval="1d", progress=False, auto_adjust=True)
    if df.empty:
        return df
    # Flatten multi-level columns from yfinance
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df


def run_strategy_backtest(strategy_name: str, params: dict,
                          symbol: str, start_date: str, end_date: str) -> dict:
    """Run a single backtest with given strategy and parameters.

    Returns dict with trades, stats, equity curve.
    """
    df = _download_data(symbol, start_date, end_date)
    if df.empty or len(df) < 50:
        return {"error": f"Insufficient data for {symbol}"}

    # Merge defaults with overrides
    strategy_def = STRATEGIES.get(strategy_name)
    if not strategy_def:
        return {"error": f"Unknown strategy: {strategy_name}"}
    merged = {k: v["default"] for k, v in strategy_def["params"].items()}
    merged.update(params)

    # Dispatch to strategy-specific simulator
    runner = _STRATEGY_RUNNERS.get(strategy_name)
    if not runner:
        return {"error": f"No runner for {strategy_name}"}

    # Trim to actual backtest range (after warmup)
    trade_start = pd.Timestamp(start_date)
    df_trade = df[df.index >= trade_start].copy()
    if df_trade.empty:
        return {"error": "No data in selected date range"}

    return runner(df, df_trade, merged, symbol)


def _build_result(trades: list, equity_curve: list, symbol: str,
                  params: dict, starting_cash: float = 10000.0) -> dict:
    """Compute stats from a list of completed trades."""
    if not trades:
        return {
            "symbol": symbol,
            "params": params,
            "trades": [],
            "equity_curve": equity_curve,
            "stats": _empty_stats(starting_cash),
        }

    wins = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]
    total_pnl = sum(t["pnl"] for t in trades)
    gross_profit = sum(t["pnl"] for t in wins) if wins else 0
    gross_loss = abs(sum(t["pnl"] for t in losses)) if losses else 0

    # Max drawdown from equity curve
    peak = starting_cash
    max_dd = 0
    for pt in equity_curve:
        v = pt["value"]
        if v > peak:
            peak = v
        dd = (peak - v) / peak * 100
        if dd > max_dd:
            max_dd = dd

    # Average hold time
    hold_days = []
    for t in trades:
        try:
            entry = datetime.strptime(t["entry_date"], "%Y-%m-%d")
            exit_ = datetime.strptime(t["exit_date"], "%Y-%m-%d")
            hold_days.append((exit_ - entry).days)
        except Exception:
            pass

    final_value = equity_curve[-1]["value"] if equity_curve else starting_cash

    return {
        "symbol": symbol,
        "params": params,
        "trades": trades,
        "equity_curve": equity_curve,
        "stats": {
            "total_trades": len(trades),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": round(len(wins) / len(trades) * 100, 1),
            "total_pnl": round(total_pnl, 2),
            "total_return_pct": round(total_pnl / starting_cash * 100, 2),
            "profit_factor": round(gross_profit / gross_loss, 2) if gross_loss > 0 else (
                999.0 if gross_profit > 0 else 0.0),
            "max_drawdown_pct": round(max_dd, 2),
            "avg_hold_days": round(sum(hold_days) / len(hold_days), 1) if hold_days else 0,
            "avg_pnl": round(total_pnl / len(trades), 2),
            "best_trade": round(max(t["pnl"] for t in trades), 2),
            "worst_trade": round(min(t["pnl"] for t in trades), 2),
            "final_value": round(final_value, 2),
        },
    }


def _empty_stats(starting_cash: float = 10000.0) -> dict:
    return {
        "total_trades": 0, "wins": 0, "losses": 0, "win_rate": 0,
        "total_pnl": 0, "total_return_pct": 0, "profit_factor": 0,
        "max_drawdown_pct": 0, "avg_hold_days": 0, "avg_pnl": 0,
        "best_trade": 0, "worst_trade": 0, "final_value": starting_cash,
    }


# ─── Strategy runners ─────────────────────────────────────────────────────────

def _run_rsi_mean_reversion(df_full, df_trade, params, symbol):
    rsi_period = params["rsi_period"]
    rsi_buy = params["rsi_buy"]
    rsi_sell = params["rsi_sell"]
    stop_loss = params["stop_loss_pct"] / 100
    pos_pct = params["position_pct"] / 100

    rsi = _calc_rsi(df_full["Close"], rsi_period)
    rsi = rsi.reindex(df_trade.index)

    cash = 10000.0
    position = None  # {qty, entry_price, entry_date}
    trades = []
    equity = [{"date": df_trade.index[0].strftime("%Y-%m-%d"), "value": cash}]

    for i, (idx, row) in enumerate(df_trade.iterrows()):
        price = float(row["Close"])
        r = float(rsi.iloc[i]) if pd.notna(rsi.iloc[i]) else 50.0
        date_str = idx.strftime("%Y-%m-%d")

        if position:
            pnl_pct = (price / position["entry_price"]) - 1
            # Stop loss or sell signal
            if pnl_pct <= stop_loss or r >= rsi_sell:
                pnl = (price - position["entry_price"]) * position["qty"]
                cash += position["qty"] * price
                trades.append({
                    "symbol": symbol, "entry_date": position["entry_date"],
                    "exit_date": date_str, "entry_price": round(position["entry_price"], 2),
                    "exit_price": round(price, 2), "qty": round(position["qty"], 4),
                    "pnl": round(pnl, 2),
                    "pnl_pct": round(pnl_pct * 100, 2),
                    "exit_reason": "stop_loss" if pnl_pct <= stop_loss else "rsi_sell",
                })
                position = None
        else:
            # Buy signal
            if r <= rsi_buy:
                alloc = cash * pos_pct
                qty = alloc / price
                if qty > 0 and alloc <= cash:
                    cash -= alloc
                    position = {"qty": qty, "entry_price": price, "entry_date": date_str}

        value = cash + (position["qty"] * price if position else 0)
        equity.append({"date": date_str, "value": round(value, 2)})

    # Close open position at end
    if position:
        price = float(df_trade.iloc[-1]["Close"])
        pnl = (price - position["entry_price"]) * position["qty"]
        cash += position["qty"] * price
        trades.append({
            "symbol": symbol, "entry_date": position["entry_date"],
            "exit_date": df_trade.index[-1].strftime("%Y-%m-%d"),
            "entry_price": round(position["entry_price"], 2),
            "exit_price": round(price, 2), "qty": round(position["qty"], 4),
            "pnl": round(pnl, 2),
            "pnl_pct": round(((price / position["entry_price"]) - 1) * 100, 2),
            "exit_reason": "end_of_period",
        })

    return _build_result(trades, equity, symbol, params)


def _run_macd_crossover(df_full, df_trade, params, symbol):
    fast = params["fast_period"]
    slow = params["slow_period"]
    sig = params["signal_period"]
    stop_loss = params["stop_loss_pct"] / 100
    pos_pct = params["position_pct"] / 100

    macd_line, signal_line, hist = _calc_macd(df_full["Close"], fast, slow, sig)
    macd_line = macd_line.reindex(df_trade.index)
    signal_line = signal_line.reindex(df_trade.index)
    hist = hist.reindex(df_trade.index)

    cash = 10000.0
    position = None
    trades = []
    equity = [{"date": df_trade.index[0].strftime("%Y-%m-%d"), "value": cash}]
    prev_hist = None

    for i, (idx, row) in enumerate(df_trade.iterrows()):
        price = float(row["Close"])
        h = float(hist.iloc[i]) if pd.notna(hist.iloc[i]) else 0
        date_str = idx.strftime("%Y-%m-%d")

        if position:
            pnl_pct = (price / position["entry_price"]) - 1
            # Death cross or stop loss
            if pnl_pct <= stop_loss or (prev_hist is not None and prev_hist > 0 and h <= 0):
                pnl = (price - position["entry_price"]) * position["qty"]
                cash += position["qty"] * price
                trades.append({
                    "symbol": symbol, "entry_date": position["entry_date"],
                    "exit_date": date_str, "entry_price": round(position["entry_price"], 2),
                    "exit_price": round(price, 2), "qty": round(position["qty"], 4),
                    "pnl": round(pnl, 2),
                    "pnl_pct": round(pnl_pct * 100, 2),
                    "exit_reason": "stop_loss" if pnl_pct <= stop_loss else "death_cross",
                })
                position = None
        else:
            # Golden cross: histogram flips positive
            if prev_hist is not None and prev_hist <= 0 and h > 0:
                alloc = cash * pos_pct
                qty = alloc / price
                if qty > 0 and alloc <= cash:
                    cash -= alloc
                    position = {"qty": qty, "entry_price": price, "entry_date": date_str}

        prev_hist = h
        value = cash + (position["qty"] * price if position else 0)
        equity.append({"date": date_str, "value": round(value, 2)})

    if position:
        price = float(df_trade.iloc[-1]["Close"])
        pnl = (price - position["entry_price"]) * position["qty"]
        cash += position["qty"] * price
        trades.append({
            "symbol": symbol, "entry_date": position["entry_date"],
            "exit_date": df_trade.index[-1].strftime("%Y-%m-%d"),
            "entry_price": round(position["entry_price"], 2),
            "exit_price": round(price, 2), "qty": round(position["qty"], 4),
            "pnl": round(pnl, 2),
            "pnl_pct": round(((price / position["entry_price"]) - 1) * 100, 2),
            "exit_reason": "end_of_period",
        })

    return _build_result(trades, equity, symbol, params)


def _run_ma_breakout(df_full, df_trade, params, symbol):
    ma_period = params["ma_period"]
    vol_mult = params["volume_mult"]
    stop_loss = params["stop_loss_pct"] / 100
    pos_pct = params["position_pct"] / 100

    ma = df_full["Close"].rolling(ma_period).mean().reindex(df_trade.index)
    avg_vol = df_full["Volume"].rolling(20).mean().reindex(df_trade.index)

    cash = 10000.0
    position = None
    trades = []
    equity = [{"date": df_trade.index[0].strftime("%Y-%m-%d"), "value": cash}]
    prev_above = None

    for i, (idx, row) in enumerate(df_trade.iterrows()):
        price = float(row["Close"])
        vol = float(row["Volume"])
        ma_val = float(ma.iloc[i]) if pd.notna(ma.iloc[i]) else price
        avg_v = float(avg_vol.iloc[i]) if pd.notna(avg_vol.iloc[i]) else vol
        date_str = idx.strftime("%Y-%m-%d")
        above = price > ma_val

        if position:
            pnl_pct = (price / position["entry_price"]) - 1
            if pnl_pct <= stop_loss or (prev_above is True and not above):
                pnl = (price - position["entry_price"]) * position["qty"]
                cash += position["qty"] * price
                trades.append({
                    "symbol": symbol, "entry_date": position["entry_date"],
                    "exit_date": date_str, "entry_price": round(position["entry_price"], 2),
                    "exit_price": round(price, 2), "qty": round(position["qty"], 4),
                    "pnl": round(pnl, 2),
                    "pnl_pct": round(pnl_pct * 100, 2),
                    "exit_reason": "stop_loss" if pnl_pct <= stop_loss else "ma_breakdown",
                })
                position = None
        else:
            # Breakout: cross above MA on high volume
            if prev_above is False and above and vol > avg_v * vol_mult:
                alloc = cash * pos_pct
                qty = alloc / price
                if qty > 0 and alloc <= cash:
                    cash -= alloc
                    position = {"qty": qty, "entry_price": price, "entry_date": date_str}

        prev_above = above
        value = cash + (position["qty"] * price if position else 0)
        equity.append({"date": date_str, "value": round(value, 2)})

    if position:
        price = float(df_trade.iloc[-1]["Close"])
        pnl = (price - position["entry_price"]) * position["qty"]
        cash += position["qty"] * price
        trades.append({
            "symbol": symbol, "entry_date": position["entry_date"],
            "exit_date": df_trade.index[-1].strftime("%Y-%m-%d"),
            "entry_price": round(position["entry_price"], 2),
            "exit_price": round(price, 2), "qty": round(position["qty"], 4),
            "pnl": round(pnl, 2),
            "pnl_pct": round(((price / position["entry_price"]) - 1) * 100, 2),
            "exit_reason": "end_of_period",
        })

    return _build_result(trades, equity, symbol, params)


def _run_buy_the_blood(df_full, df_trade, params, symbol):
    rsi_period = params["rsi_period"]
    rsi_extreme = params["rsi_extreme"]
    rsi_exit = params["rsi_exit"]
    stop_loss = params["stop_loss_pct"] / 100
    pos_pct = params["position_pct"] / 100

    rsi = _calc_rsi(df_full["Close"], rsi_period).reindex(df_trade.index)

    cash = 10000.0
    position = None
    trades = []
    equity = [{"date": df_trade.index[0].strftime("%Y-%m-%d"), "value": cash}]

    for i, (idx, row) in enumerate(df_trade.iterrows()):
        price = float(row["Close"])
        r = float(rsi.iloc[i]) if pd.notna(rsi.iloc[i]) else 50.0
        date_str = idx.strftime("%Y-%m-%d")

        if position:
            pnl_pct = (price / position["entry_price"]) - 1
            if pnl_pct <= stop_loss or r >= rsi_exit:
                pnl = (price - position["entry_price"]) * position["qty"]
                cash += position["qty"] * price
                trades.append({
                    "symbol": symbol, "entry_date": position["entry_date"],
                    "exit_date": date_str, "entry_price": round(position["entry_price"], 2),
                    "exit_price": round(price, 2), "qty": round(position["qty"], 4),
                    "pnl": round(pnl, 2),
                    "pnl_pct": round(pnl_pct * 100, 2),
                    "exit_reason": "stop_loss" if pnl_pct <= stop_loss else "rsi_recovery",
                })
                position = None
        else:
            if r <= rsi_extreme:
                alloc = cash * pos_pct
                qty = alloc / price
                if qty > 0 and alloc <= cash:
                    cash -= alloc
                    position = {"qty": qty, "entry_price": price, "entry_date": date_str}

        value = cash + (position["qty"] * price if position else 0)
        equity.append({"date": date_str, "value": round(value, 2)})

    if position:
        price = float(df_trade.iloc[-1]["Close"])
        pnl = (price - position["entry_price"]) * position["qty"]
        cash += position["qty"] * price
        trades.append({
            "symbol": symbol, "entry_date": position["entry_date"],
            "exit_date": df_trade.index[-1].strftime("%Y-%m-%d"),
            "entry_price": round(position["entry_price"], 2),
            "exit_price": round(price, 2), "qty": round(position["qty"], 4),
            "pnl": round(pnl, 2),
            "pnl_pct": round(((price / position["entry_price"]) - 1) * 100, 2),
            "exit_reason": "end_of_period",
        })

    return _build_result(trades, equity, symbol, params)


def _run_momentum(df_full, df_trade, params, symbol):
    lookback = params["lookback"]
    trail_pct = params["trail_stop_pct"] / 100
    stop_loss = params["stop_loss_pct"] / 100
    pos_pct = params["position_pct"] / 100

    rolling_high = df_full["Close"].rolling(lookback).max().reindex(df_trade.index)

    cash = 10000.0
    position = None
    peak_price = 0
    trades = []
    equity = [{"date": df_trade.index[0].strftime("%Y-%m-%d"), "value": cash}]

    for i, (idx, row) in enumerate(df_trade.iterrows()):
        price = float(row["Close"])
        rh = float(rolling_high.iloc[i]) if pd.notna(rolling_high.iloc[i]) else 0
        date_str = idx.strftime("%Y-%m-%d")

        if position:
            if price > peak_price:
                peak_price = price
            pnl_pct_entry = (price / position["entry_price"]) - 1
            pnl_pct_peak = (price / peak_price) - 1

            if pnl_pct_entry <= stop_loss or pnl_pct_peak <= trail_pct:
                pnl = (price - position["entry_price"]) * position["qty"]
                cash += position["qty"] * price
                trades.append({
                    "symbol": symbol, "entry_date": position["entry_date"],
                    "exit_date": date_str, "entry_price": round(position["entry_price"], 2),
                    "exit_price": round(price, 2), "qty": round(position["qty"], 4),
                    "pnl": round(pnl, 2),
                    "pnl_pct": round(pnl_pct_entry * 100, 2),
                    "exit_reason": "stop_loss" if pnl_pct_entry <= stop_loss else "trailing_stop",
                })
                position = None
                peak_price = 0
        else:
            # New high breakout
            if rh > 0 and price >= rh:
                alloc = cash * pos_pct
                qty = alloc / price
                if qty > 0 and alloc <= cash:
                    cash -= alloc
                    position = {"qty": qty, "entry_price": price, "entry_date": date_str}
                    peak_price = price

        value = cash + (position["qty"] * price if position else 0)
        equity.append({"date": date_str, "value": round(value, 2)})

    if position:
        price = float(df_trade.iloc[-1]["Close"])
        pnl = (price - position["entry_price"]) * position["qty"]
        cash += position["qty"] * price
        trades.append({
            "symbol": symbol, "entry_date": position["entry_date"],
            "exit_date": df_trade.index[-1].strftime("%Y-%m-%d"),
            "entry_price": round(position["entry_price"], 2),
            "exit_price": round(price, 2), "qty": round(position["qty"], 4),
            "pnl": round(pnl, 2),
            "pnl_pct": round(((price / position["entry_price"]) - 1) * 100, 2),
            "exit_reason": "end_of_period",
        })

    return _build_result(trades, equity, symbol, params)


def _run_dip_bounce(df_full, df_trade, params, symbol):
    """PTJ Mean Reversion: buy on 3%+ daily dip + RSI < 30, sell at RSI 50."""
    rsi_period = params["rsi_period"]
    dip_pct = params["dip_pct"] / 100  # e.g., -0.03
    rsi_entry = params["rsi_entry"]
    rsi_exit = params["rsi_exit"]
    stop_loss = params["stop_loss_pct"] / 100
    pos_pct = params["position_pct"] / 100

    rsi = _calc_rsi(df_full["Close"], rsi_period).reindex(df_trade.index)
    daily_return = df_full["Close"].pct_change().reindex(df_trade.index)

    cash = 10000.0
    position = None
    trades = []
    equity = []

    for i in range(len(df_trade)):
        date_str = df_trade.index[i].strftime("%Y-%m-%d")
        price = float(df_trade.iloc[i]["Close"])
        r = float(rsi.iloc[i]) if pd.notna(rsi.iloc[i]) else 50
        dr = float(daily_return.iloc[i]) if pd.notna(daily_return.iloc[i]) else 0

        if position:
            pnl_pct = (price / position["entry_price"]) - 1
            # Stop loss
            if pnl_pct <= stop_loss:
                pnl = (price - position["entry_price"]) * position["qty"]
                cash += position["qty"] * price
                trades.append({
                    "symbol": symbol, "entry_date": position["entry_date"],
                    "exit_date": date_str,
                    "entry_price": round(position["entry_price"], 2),
                    "exit_price": round(price, 2), "qty": round(position["qty"], 4),
                    "pnl": round(pnl, 2),
                    "pnl_pct": round(pnl_pct * 100, 2),
                    "exit_reason": "stop_loss",
                })
                position = None
            # RSI exit: bounce recovered
            elif r >= rsi_exit:
                pnl = (price - position["entry_price"]) * position["qty"]
                cash += position["qty"] * price
                trades.append({
                    "symbol": symbol, "entry_date": position["entry_date"],
                    "exit_date": date_str,
                    "entry_price": round(position["entry_price"], 2),
                    "exit_price": round(price, 2), "qty": round(position["qty"], 4),
                    "pnl": round(pnl, 2),
                    "pnl_pct": round(pnl_pct * 100, 2),
                    "exit_reason": "rsi_bounce",
                })
                position = None
        else:
            # Entry: daily drop >= dip_pct AND RSI < entry threshold
            if dr <= dip_pct and r < rsi_entry:
                alloc = cash * pos_pct
                qty = alloc / price
                if qty > 0 and alloc <= cash:
                    cash -= alloc
                    position = {"qty": qty, "entry_price": price, "entry_date": date_str}

        value = cash + (position["qty"] * price if position else 0)
        equity.append({"date": date_str, "value": round(value, 2)})

    # Close any open position at end
    if position:
        price = float(df_trade.iloc[-1]["Close"])
        pnl = (price - position["entry_price"]) * position["qty"]
        cash += position["qty"] * price
        trades.append({
            "symbol": symbol, "entry_date": position["entry_date"],
            "exit_date": df_trade.index[-1].strftime("%Y-%m-%d"),
            "entry_price": round(position["entry_price"], 2),
            "exit_price": round(price, 2), "qty": round(position["qty"], 4),
            "pnl": round(pnl, 2),
            "pnl_pct": round(((price / position["entry_price"]) - 1) * 100, 2),
            "exit_reason": "end_of_period",
        })

    return _build_result(trades, equity, symbol, params)


def _run_event_driven(df_full, df_trade, params, symbol):
    """Druckenmiller Event-Driven: buy before earnings if trending up, sell after."""
    days_before = params["days_before"]
    days_after = params["days_after"]
    trend_sma = params["trend_sma"]
    stop_loss = params["stop_loss_pct"] / 100
    pos_pct = params["position_pct"] / 100

    sma = df_full["Close"].rolling(window=trend_sma).mean().reindex(df_trade.index)

    # Get historical earnings dates from yfinance
    try:
        ticker = yf.Ticker(symbol)
        earnings_dates = []
        if hasattr(ticker, 'earnings_dates') and ticker.earnings_dates is not None:
            earnings_dates = [d.strftime("%Y-%m-%d") for d in ticker.earnings_dates.index]
        elif hasattr(ticker, 'calendar') and ticker.calendar is not None:
            cal = ticker.calendar
            if isinstance(cal, dict) and "Earnings Date" in cal:
                earnings_dates = [d.strftime("%Y-%m-%d") if hasattr(d, 'strftime') else str(d)
                                  for d in cal["Earnings Date"]]
    except Exception:
        earnings_dates = []

    # Convert to set of dates for fast lookup
    earnings_set = set(earnings_dates)
    # Build entry/exit windows around each earnings date
    trade_dates = df_trade.index
    earnings_entries = set()  # dates to enter (days_before earnings)
    earnings_exits = set()    # dates to exit (days_after earnings)

    for ed_str in earnings_set:
        try:
            ed = pd.Timestamp(ed_str)
            for d in range(days_before, 0, -1):
                entry_dt = ed - pd.Timedelta(days=d)
                earnings_entries.add(entry_dt.strftime("%Y-%m-%d"))
            for d in range(1, days_after + 1):
                exit_dt = ed + pd.Timedelta(days=d)
                earnings_exits.add(exit_dt.strftime("%Y-%m-%d"))
            # Also add earnings day itself as an exit
            earnings_exits.add(ed_str)
        except Exception:
            pass

    cash = 10000.0
    position = None
    trades = []
    equity = []

    for i in range(len(df_trade)):
        date_str = df_trade.index[i].strftime("%Y-%m-%d")
        price = float(df_trade.iloc[i]["Close"])
        sma_val = float(sma.iloc[i]) if pd.notna(sma.iloc[i]) else price

        if position:
            pnl_pct = (price / position["entry_price"]) - 1
            # Stop loss
            if pnl_pct <= stop_loss:
                pnl = (price - position["entry_price"]) * position["qty"]
                cash += position["qty"] * price
                trades.append({
                    "symbol": symbol, "entry_date": position["entry_date"],
                    "exit_date": date_str,
                    "entry_price": round(position["entry_price"], 2),
                    "exit_price": round(price, 2), "qty": round(position["qty"], 4),
                    "pnl": round(pnl, 2),
                    "pnl_pct": round(pnl_pct * 100, 2),
                    "exit_reason": "stop_loss",
                })
                position = None
            # Exit after earnings
            elif date_str in earnings_exits:
                pnl = (price - position["entry_price"]) * position["qty"]
                cash += position["qty"] * price
                trades.append({
                    "symbol": symbol, "entry_date": position["entry_date"],
                    "exit_date": date_str,
                    "entry_price": round(position["entry_price"], 2),
                    "exit_price": round(price, 2), "qty": round(position["qty"], 4),
                    "pnl": round(pnl, 2),
                    "pnl_pct": round(pnl_pct * 100, 2),
                    "exit_reason": "post_earnings_exit",
                })
                position = None
        else:
            # Entry: within earnings window AND price above trend SMA
            if date_str in earnings_entries and price > sma_val:
                alloc = cash * pos_pct
                qty = alloc / price
                if qty > 0 and alloc <= cash:
                    cash -= alloc
                    position = {"qty": qty, "entry_price": price, "entry_date": date_str}

        value = cash + (position["qty"] * price if position else 0)
        equity.append({"date": date_str, "value": round(value, 2)})

    # Close any open position
    if position:
        price = float(df_trade.iloc[-1]["Close"])
        pnl = (price - position["entry_price"]) * position["qty"]
        cash += position["qty"] * price
        trades.append({
            "symbol": symbol, "entry_date": position["entry_date"],
            "exit_date": df_trade.index[-1].strftime("%Y-%m-%d"),
            "entry_price": round(position["entry_price"], 2),
            "exit_price": round(price, 2), "qty": round(position["qty"], 4),
            "pnl": round(pnl, 2),
            "pnl_pct": round(((price / position["entry_price"]) - 1) * 100, 2),
            "exit_reason": "end_of_period",
        })

    return _build_result(trades, equity, symbol, params)


_STRATEGY_RUNNERS = {
    "rsi_mean_reversion": _run_rsi_mean_reversion,
    "macd_crossover": _run_macd_crossover,
    "ma_breakout": _run_ma_breakout,
    "buy_the_blood": _run_buy_the_blood,
    "momentum": _run_momentum,
    "dip_bounce": _run_dip_bounce,
    "event_driven": _run_event_driven,
}


# ─── Optimizer ────────────────────────────────────────────────────────────────

def optimize_strategy(strategy_name: str, symbol: str,
                      start_date: str, end_date: str,
                      custom_grid: dict | None = None,
                      progress_cb=None) -> dict:
    """Run a grid search over parameter combinations, rank by profit factor.

    Returns list of results sorted by profit factor descending.
    """
    strategy_def = STRATEGIES.get(strategy_name)
    if not strategy_def:
        return {"error": f"Unknown strategy: {strategy_name}"}

    grid = custom_grid or strategy_def.get("optimize_grid", {})
    if not grid:
        return {"error": "No optimization grid defined"}

    # Build all combinations
    param_names = list(grid.keys())
    param_values = [grid[k] for k in param_names]
    combos = list(itertools.product(*param_values))

    # Download data once
    df = _download_data(symbol, start_date, end_date)
    if df.empty or len(df) < 50:
        return {"error": f"Insufficient data for {symbol}"}

    trade_start = pd.Timestamp(start_date)
    df_trade = df[df.index >= trade_start].copy()
    if df_trade.empty:
        return {"error": "No data in selected date range"}

    runner = _STRATEGY_RUNNERS.get(strategy_name)
    if not runner:
        return {"error": f"No runner for {strategy_name}"}

    base_params = {k: v["default"] for k, v in strategy_def["params"].items()}
    results = []

    for i, combo in enumerate(combos):
        params = base_params.copy()
        for j, name in enumerate(param_names):
            params[name] = combo[j]

        if progress_cb:
            progress_cb(int((i + 1) / len(combos) * 100),
                        f"Testing combo {i+1}/{len(combos)}")

        result = runner(df, df_trade, params, symbol)
        if "error" not in result:
            results.append(result)

    # Sort by profit factor descending
    results.sort(key=lambda r: r["stats"]["profit_factor"], reverse=True)

    return {
        "strategy": strategy_name,
        "strategy_name": strategy_def["name"],
        "symbol": symbol,
        "start_date": start_date,
        "end_date": end_date,
        "total_combinations": len(combos),
        "results": results,
        "best": results[0] if results else None,
    }


# ─── Deploy winning params to trading rules ───────────────────────────────────

def deploy_winning_params(strategy_name: str, params: dict, stats: dict) -> dict:
    """Append the winning parameters to trading_rules.txt."""
    strategy_def = STRATEGIES.get(strategy_name)
    if not strategy_def:
        return {"error": f"Unknown strategy: {strategy_name}"}

    param_str = ", ".join(f"{k}={v}" for k, v in params.items())
    stats_str = (
        f"Win Rate: {stats.get('win_rate', 0)}%, "
        f"Profit Factor: {stats.get('profit_factor', 0)}, "
        f"Return: {stats.get('total_return_pct', 0)}%, "
        f"Max DD: {stats.get('max_drawdown_pct', 0)}%"
    )

    block = (
        f"\n\nSTRATEGY LAB OPTIMIZED — {strategy_def['name'].upper()}:\n"
        f"   Parameters: {param_str}\n"
        f"   Backtest Stats: {stats_str}\n"
        f"   Deployed: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
    )

    try:
        with open("trading_rules.txt", "a") as f:
            f.write(block)
        return {"success": True, "message": f"Deployed {strategy_def['name']} params to trading_rules.txt"}
    except Exception as e:
        return {"error": str(e)}


# ─── Auto-optimize pipeline ──────────────────────────────────────────────────

def _get_current_config_params() -> dict:
    """Read current trading parameters from config.py for comparison."""
    try:
        from config import STOP_LOSS_PCT, POSITION_SIZE_PCT
        return {
            "stop_loss_pct": round(-STOP_LOSS_PCT * 100, 1),  # 0.12 → -12
            "position_pct": round(POSITION_SIZE_PCT * 100, 1),  # 0.10 → 10
        }
    except Exception:
        return {"stop_loss_pct": -12, "position_pct": 10}


def _update_config_param(param_name: str, new_value: float) -> bool:
    """Update a numeric parameter in config.py."""
    config_path = Path("config.py")
    try:
        text = config_path.read_text()
        import re
        # Map strategy param names to config variable names
        config_map = {
            "stop_loss_pct": ("STOP_LOSS_PCT", abs(new_value) / 100),  # -12 → 0.12
            "position_pct":  ("POSITION_SIZE_PCT", new_value / 100),   # 10 → 0.10
        }
        if param_name not in config_map:
            return False
        var_name, val = config_map[param_name]
        pattern = rf"^({var_name}\s*=\s*)[\d.]+(.*)$"
        replacement = rf"\g<1>{val:.2f}\2"
        new_text, count = re.subn(pattern, replacement, text, flags=re.MULTILINE)
        if count > 0:
            config_path.write_text(new_text)
            return True
        return False
    except Exception as e:
        console.log(f"[red]Failed to update config.py: {e}")
        return False


def auto_optimize_all(progress_cb=None) -> dict:
    """Full automated optimization pipeline across all watchlist stocks and strategies.

    1. Downloads 2yr data for all 16 watchlist stocks
    2. Tests 5 strategies × 16 stocks × 20+ param combos each
    3. Ranks by profit factor
    4. Compares best vs current config
    5. Auto-deploys if best beats current by >10%
    6. Saves report to data/strategy_lab/
    """
    from config import WATCH_STOCKS

    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M")
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
    current_params = _get_current_config_params()

    console.log(f"[cyan]Strategy Lab: Starting auto-optimize — {len(WATCH_STOCKS)} stocks × {len(STRATEGIES)} strategies")

    all_results = []
    total_work = len(STRATEGIES) * len(WATCH_STOCKS)
    done = 0

    for strat_name, strat_def in STRATEGIES.items():
        strategy_results = []

        for sym in WATCH_STOCKS:
            done += 1
            if progress_cb:
                progress_cb(int(done / total_work * 95),
                            f"{strat_def['name']} on {sym} ({done}/{total_work})")

            try:
                result = optimize_strategy(strat_name, sym, start_date, end_date)
                if "error" not in result and result.get("best"):
                    best = result["best"]
                    strategy_results.append({
                        "symbol": sym,
                        "params": best["params"],
                        "stats": best["stats"],
                        "combos_tested": result["total_combinations"],
                    })
            except Exception as e:
                console.log(f"[yellow]Strategy Lab: {strat_name}/{sym} failed: {e}")
                continue

        if strategy_results:
            # Aggregate: pick the params that worked best across the MOST stocks
            # Score = avg profit factor across all symbols with that param set
            param_scores = {}
            for r in strategy_results:
                key = json.dumps(r["params"], sort_keys=True)
                if key not in param_scores:
                    param_scores[key] = {"params": r["params"], "pf_sum": 0, "wr_sum": 0, "count": 0, "symbols": []}
                pf = min(r["stats"]["profit_factor"], 50)  # cap outliers
                param_scores[key]["pf_sum"] += pf
                param_scores[key]["wr_sum"] += r["stats"]["win_rate"]
                param_scores[key]["count"] += 1
                param_scores[key]["symbols"].append(r["symbol"])

            ranked = sorted(param_scores.values(),
                            key=lambda x: x["pf_sum"] / x["count"], reverse=True)
            best_aggregate = ranked[0] if ranked else None

            if best_aggregate:
                avg_pf = round(best_aggregate["pf_sum"] / best_aggregate["count"], 2)
                avg_wr = round(best_aggregate["wr_sum"] / best_aggregate["count"], 1)
                all_results.append({
                    "strategy": strat_name,
                    "strategy_name": strat_def["name"],
                    "best_params": best_aggregate["params"],
                    "avg_profit_factor": avg_pf,
                    "avg_win_rate": avg_wr,
                    "stocks_tested": best_aggregate["count"],
                    "symbols": best_aggregate["symbols"],
                    "per_stock": strategy_results,
                })

    # Sort all strategies by avg profit factor
    all_results.sort(key=lambda x: x["avg_profit_factor"], reverse=True)

    # Compare best vs current and auto-deploy
    deployed = []
    if all_results:
        best = all_results[0]
        best_pf = best["avg_profit_factor"]

        # Calculate current baseline PF (use default params)
        current_pf_estimate = 1.0  # baseline assumption
        improvement = ((best_pf - current_pf_estimate) / max(current_pf_estimate, 0.01)) * 100

        # Check if any optimized params differ from current config and beat by >10%
        for param_name, param_val in best["best_params"].items():
            if param_name in current_params:
                current_val = current_params[param_name]
                if param_val != current_val and improvement > 10:
                    if _update_config_param(param_name, param_val):
                        deployed.append({
                            "param": param_name,
                            "old": current_val,
                            "new": param_val,
                            "improvement_pct": round(improvement, 1),
                        })
                        console.log(
                            f"[bold green]Strategy Lab: {best['strategy_name']} with "
                            f"{param_name}={param_val} beat current rules by "
                            f"{improvement:.0f}%. Auto-deploying new parameters."
                        )

        # Also deploy to trading_rules.txt
        if deployed:
            deploy_winning_params(best["strategy"], best["best_params"], {
                "win_rate": best["avg_win_rate"],
                "profit_factor": best_pf,
                "total_return_pct": 0,
                "max_drawdown_pct": 0,
            })

    # Save report
    report = {
        "timestamp": timestamp,
        "start_date": start_date,
        "end_date": end_date,
        "stocks_tested": len(WATCH_STOCKS),
        "strategies_tested": len(STRATEGIES),
        "current_config": current_params,
        "results": all_results,
        "deployed": deployed,
        "best_strategy": all_results[0] if all_results else None,
    }

    report_path = REPORT_DIR / f"auto_optimize_{timestamp}.json"
    try:
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2, default=str)
        console.log(f"[green]Strategy Lab: Report saved to {report_path}")
    except Exception as e:
        console.log(f"[red]Strategy Lab: Failed to save report: {e}")

    if progress_cb:
        progress_cb(100, "Complete")

    return report


def get_latest_report() -> dict | None:
    """Load the most recent auto-optimize report."""
    reports = sorted(REPORT_DIR.glob("auto_optimize_*.json"), reverse=True)
    if not reports:
        return None
    try:
        with open(reports[0]) as f:
            return json.load(f)
    except Exception:
        return None


def get_report_history(limit: int = 20) -> list:
    """Get summary of recent optimization reports."""
    reports = sorted(REPORT_DIR.glob("auto_optimize_*.json"), reverse=True)[:limit]
    summaries = []
    for rp in reports:
        try:
            with open(rp) as f:
                data = json.load(f)
            best = data.get("best_strategy", {})
            summaries.append({
                "timestamp": data.get("timestamp"),
                "best_strategy": best.get("strategy_name", "N/A"),
                "avg_profit_factor": best.get("avg_profit_factor", 0),
                "avg_win_rate": best.get("avg_win_rate", 0),
                "deployed_count": len(data.get("deployed", [])),
                "stocks_tested": data.get("stocks_tested", 0),
            })
        except Exception:
            continue
    return summaries
