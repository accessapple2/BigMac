#!/usr/bin/env python3
"""
s6_180d_backtest.py — Season 6 All-Agent 180-Day Comparative Backtest

Tests all 9 active S6 agents using VectorBT Holodeck simulation engine.
Each agent runs its characteristic strategy profile against historical prices.

Agents: ollie-auto, navigator, chekov, ollama-llama, ollama-plutus,
        ollama-qwen3, ollama-coder, neo-matrix, capitol-trades

Metrics: Total Return (%, $), Sharpe, Sortino, Calmar, Max Drawdown,
         Win Rate, Alpha vs SPY

Saves to: backtest_history table in trader.db
          data/s6_180d_backtest.json (detailed results)

Usage:
    venv/bin/python3 scripts/s6_180d_backtest.py
"""
from __future__ import annotations

import json
import math
import os
import sqlite3
import sys
import threading
import time
from collections import defaultdict
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import numpy as np
    import pandas as pd
    import yfinance as yf
except ImportError as e:
    print(f"Missing dependency: {e}")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────
DAYS             = 180
STARTING_CAPITAL = 100_000.0
SLIPPAGE         = 0.001    # 0.1% per side
MAX_POSITIONS    = 5
BENCHMARK_SYM    = "SPY"
AGENT_TIMEOUT_S  = 180
OUT_PATH         = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                "data", "s6_180d_backtest.json")
DB_PATH          = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                "data", "trader.db")

# Trading universes
UNIVERSE = ["SPY", "QQQ", "AAPL", "MSFT", "NVDA", "AMD",
            "TSLA", "AMZN", "META", "GOOGL", "NFLX", "CRM"]

TECH_UNIVERSE = ["AAPL", "MSFT", "NVDA", "AMD", "TSLA",
                 "GOOGL", "META", "CRM", "ADBE", "INTC", "AVGO", "QCOM"]

CONGRESS_UNIVERSE = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL",
                     "JPM", "BAC", "GS", "LMT", "NOC", "RTX",
                     "UNH", "CVX", "XOM", "TSLA", "META"]

US_HOLIDAYS = {
    date(2026, 1, 1), date(2026, 1, 19), date(2026, 2, 16),
    date(2026, 4, 3), date(2026, 5, 25), date(2026, 7, 3),
    date(2026, 9, 7), date(2026, 11, 26), date(2026, 12, 25),
    date(2025, 1, 1), date(2025, 1, 20), date(2025, 2, 17),
    date(2025, 4, 18), date(2025, 5, 26), date(2025, 7, 4),
    date(2025, 9, 1), date(2025, 11, 27), date(2025, 12, 25),
}

# ── S6 Agent profiles ─────────────────────────────────────────────────────────
# Each agent's strategy characteristics derived from their engine implementations.
# qual_gate = min quality checks out of 5 needed to enter
AGENTS_S6 = [
    {
        "id": "navigator", "name": "Navigator",
        "rsi_buy": 38, "rsi_sell": 62, "min_conv": 0.50,
        "max_pos_pct": 0.15, "stop_pct": 0.08, "tp_pct": 0.15,
        "qual_gate": 3,
        "regime_block": ("CRISIS", "BEAR_STRONG"),
        "universe": UNIVERSE,
        "description": "EMA pullback + Bull Momentum Breakout, wide regime gate",
    },
    {
        "id": "chekov", "name": "Chekov",
        "rsi_buy": 36, "rsi_sell": 64, "min_conv": 0.55,
        "max_pos_pct": 0.15, "stop_pct": 0.08, "tp_pct": 0.15,
        "qual_gate": 3,
        "regime_block": ("CRISIS", "BEAR_STRONG", "BEAR"),
        "universe": UNIVERSE,
        "description": "Same chekov_rules as Navigator, slightly selective",
    },
    {
        "id": "ollama-llama", "name": "Llama",
        "rsi_buy": 35, "rsi_sell": 65, "min_conv": 0.60,
        "max_pos_pct": 0.18, "stop_pct": 0.08, "tp_pct": 0.15,
        "qual_gate": 3,
        "regime_block": ("CRISIS", "BEAR_STRONG"),
        "universe": UNIVERSE,
        "description": "General RSI strategy, moderate conviction filter",
    },
    {
        "id": "ollama-plutus", "name": "Plutus",
        "rsi_buy": 28, "rsi_sell": 72, "min_conv": 0.65,
        "max_pos_pct": 0.12, "stop_pct": 0.07, "tp_pct": 0.18,
        "qual_gate": 4,
        "regime_block": ("CRISIS", "BEAR_STRONG", "BEAR"),
        "universe": UNIVERSE,
        "description": "Conservative deep-value oversold entries, 4/5 quality gate",
    },
    {
        "id": "ollama-qwen3", "name": "Qwen3",
        "rsi_buy": 32, "rsi_sell": 65, "min_conv": 0.55,
        "max_pos_pct": 0.20, "stop_pct": 0.10, "tp_pct": 0.20,
        "qual_gate": 2,
        "regime_block": ("CRISIS",),
        "universe": UNIVERSE,
        "description": "Aggressive growth, lowered Ollie threshold 1.5, wider stops",
    },
    {
        "id": "ollama-coder", "name": "Coder",
        "rsi_buy": 38, "rsi_sell": 62, "min_conv": 0.60,
        "max_pos_pct": 0.15, "stop_pct": 0.08, "tp_pct": 0.15,
        "qual_gate": 3,
        "regime_block": ("CRISIS", "BEAR_STRONG"),
        "universe": TECH_UNIVERSE,
        "description": "Tech-focused specialist universe (12 stocks)",
    },
    {
        "id": "neo-matrix", "name": "Neo Matrix",
        "rsi_buy": 42, "rsi_sell": 60, "min_conv": 0.60,
        "max_pos_pct": 0.18, "stop_pct": 0.09, "tp_pct": 0.12,
        "qual_gate": 3,
        "regime_block": ("CRISIS", "BEAR_STRONG"),
        "universe": UNIVERSE,
        "description": "Momentum strategy, tighter RSI range 42-60",
    },
    {
        "id": "capitol-trades", "name": "Capitol Trades",
        "rsi_buy": 35, "rsi_sell": 65, "min_conv": 0.55,
        "max_pos_pct": 0.15, "stop_pct": 0.08, "tp_pct": 0.15,
        "qual_gate": 2,
        "regime_block": ("CRISIS",),
        "universe": CONGRESS_UNIVERSE,
        "description": "Congress-tracked stocks, dual-source intel, lower gate",
    },
    {
        "id": "ollie-auto", "name": "Ollie (Gate)",
        "rsi_buy": 40, "rsi_sell": 60, "min_conv": 0.70,
        "max_pos_pct": 0.10, "stop_pct": 0.06, "tp_pct": 0.12,
        "qual_gate": 4,
        "regime_block": ("CRISIS", "BEAR_STRONG", "BEAR"),
        "universe": UNIVERSE,
        "description": "Quality gate simulation: highest conviction threshold, tightest sizing",
    },
]


# ── Date helpers ──────────────────────────────────────────────────────────────

def trading_days(start: date, end: date) -> list[date]:
    days, d = [], start
    while d <= end:
        if d.weekday() < 5 and d not in US_HOLIDAYS:
            days.append(d)
        d += timedelta(days=1)
    return days


def last_n_trading_days(n: int) -> tuple[date, date]:
    end = date.today()
    if end.weekday() >= 5:
        end -= timedelta(days=end.weekday() - 4)
    count, d = 0, end
    while count < n:
        if d.weekday() < 5 and d not in US_HOLIDAYS:
            count += 1
            if count == n:
                break
        d -= timedelta(days=1)
    return d, end


# ── Technical indicators ──────────────────────────────────────────────────────

def calc_rsi(closes: np.ndarray, period: int = 14) -> float:
    s = pd.Series(closes)
    diff = s.diff(1)
    gain = diff.clip(lower=0).rolling(period).mean()
    loss = (-diff.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, 1e-9)
    rsi_series = 100 - 100 / (1 + rs)
    val = rsi_series.dropna()
    return float(val.iloc[-1]) if len(val) > 0 else 50.0


def calc_sma(closes: np.ndarray, period: int) -> float:
    if len(closes) < period:
        return float(np.mean(closes))
    return float(np.mean(closes[-period:]))


def calc_vol_ratio(volumes: np.ndarray, window: int = 20) -> float:
    if len(volumes) < 2:
        return 1.0
    avg = float(np.mean(volumes[-window-1:-1])) if len(volumes) > window else float(np.mean(volumes[:-1]))
    return round(float(volumes[-1]) / max(avg, 1), 2)


def detect_regime(spy_closes: np.ndarray) -> str:
    if len(spy_closes) < 21:
        return "UNKNOWN"
    p, ma8, ma21 = spy_closes[-1], np.mean(spy_closes[-8:]), np.mean(spy_closes[-21:])
    ma50 = np.mean(spy_closes[-50:]) if len(spy_closes) >= 50 else ma21
    if p > ma8 > ma21:
        return "BULL_STRONG"
    elif p > ma21:
        return "BULL"
    elif p < ma8 < ma21 and p < ma50 * 0.95:
        return "CRISIS"
    elif p < ma8 < ma21:
        return "BEAR_STRONG"
    elif p < ma21:
        return "BEAR"
    return "NEUTRAL"


def score_and_signals(df_prior: pd.DataFrame) -> tuple[float, float, list[str]]:
    if len(df_prior) < 20:
        return 0.0, 0.0, []
    closes   = df_prior["Close"].values
    volumes  = df_prior["Volume"].values
    rsi_val  = calc_rsi(closes)
    sma_20   = calc_sma(closes, 20)
    sma_50   = calc_sma(closes, 50)
    vol_ratio = calc_vol_ratio(volumes)
    price    = float(closes[-1])
    mom_5d   = (closes[-1] - closes[-6]) / closes[-6] * 100 if len(closes) >= 6 else 0.0

    signals, score = [], 30
    if rsi_val < 30:      score += 15; signals.append("RSI_OVERSOLD")
    elif rsi_val < 40:    score += 10; signals.append("RSI_LOW")
    elif 40 <= rsi_val <= 60: score += 15; signals.append("RSI_NEUTRAL")
    elif rsi_val > 70:    score += 5;  signals.append("RSI_OVERBOUGHT")
    elif rsi_val > 60:    score += 5;  signals.append("RSI_HIGH")

    if vol_ratio >= 2.0:  score += 20; signals.append("VOLUME_SURGE")
    elif vol_ratio >= 1.5: score += 12; signals.append("VOLUME_ELEVATED")
    elif vol_ratio >= 1.2: score += 5;  signals.append("VOLUME_MILD")

    if mom_5d >= 3:       score += 15; signals.append("BULL_MOMENTUM")
    elif mom_5d >= 1:     score += 8;  signals.append("MILD_MOMENTUM")
    elif mom_5d <= -3:    score += 5;  signals.append("BEAR_MOMENTUM")

    if price > sma_20:    score += 5;  signals.append("ABOVE_SMA20")
    if price > sma_50:    score += 5;  signals.append("ABOVE_SMA50")

    score = min(100, max(0, score))
    return round(score / 100.0, 3), float(score), signals


# ── Portfolio simulator ───────────────────────────────────────────────────────

class Portfolio:
    def __init__(self, capital: float = STARTING_CAPITAL,
                 stop_pct: float = 0.08, tp_pct: float = 0.15):
        self.cash     = capital
        self.capital  = capital
        self.stop_pct = stop_pct
        self.tp_pct   = tp_pct
        self.positions: dict[str, dict] = {}
        self.trades:    list[dict]       = []
        self.daily_values: dict[str, float] = {}

    def portfolio_value(self, prices: dict[str, float]) -> float:
        pos_val = sum(
            self.positions[s]["qty"] * prices.get(s, self.positions[s]["entry"])
            for s in self.positions
        )
        return round(self.cash + pos_val, 2)

    def open_count(self) -> int:
        return len(self.positions)

    def try_buy(self, sym: str, price: float, date_str: str,
                alloc_pct: float, conviction: float, signals: list[str]) -> bool:
        if self.open_count() >= MAX_POSITIONS:
            return False
        if sym in self.positions:
            return False
        buy_price  = round(price * (1 + SLIPPAGE), 4)
        alloc_cash = self.cash * alloc_pct
        alloc_cash = min(alloc_cash, self.cash * 0.95)
        if alloc_cash < 100:
            return False
        qty  = alloc_cash / buy_price
        cost = qty * buy_price
        self.cash -= cost
        self.positions[sym] = {
            "qty":       qty,
            "entry":     buy_price,
            "sl":        round(buy_price * (1 - self.stop_pct), 4),
            "tp":        round(buy_price * (1 + self.tp_pct), 4),
            "date":      date_str,
            "conviction": conviction,
            "signals":   signals,
        }
        self.trades.append({
            "date": date_str, "symbol": sym, "action": "BUY",
            "price": buy_price, "qty": round(qty, 4),
            "cost": round(cost, 2), "conviction": conviction,
            "signals": signals, "pnl": None,
        })
        return True

    def check_exits(self, sym: str, day_open: float, day_high: float,
                    day_low: float, day_close: float, date_str: str) -> None:
        if sym not in self.positions:
            return
        pos   = self.positions[sym]
        entry = pos["entry"]
        sl    = pos["sl"]
        tp    = pos["tp"]
        qty   = pos["qty"]
        exit_price, reason = None, None

        if day_open <= sl:
            exit_price, reason = day_open, "STOP_GAP"
        elif day_low <= sl:
            exit_price, reason = sl, "STOP_LOSS"
        elif day_high >= tp:
            exit_price, reason = tp, "TAKE_PROFIT"

        if exit_price is not None:
            sell_price = round(exit_price * (1 - SLIPPAGE), 4)
            proceeds   = qty * sell_price
            pnl        = round(proceeds - qty * entry, 2)
            pnl_pct    = round((sell_price - entry) / entry * 100, 2)
            self.cash += proceeds
            self.trades.append({
                "date": date_str, "symbol": sym, "action": "SELL",
                "price": sell_price, "qty": round(qty, 4),
                "proceeds": round(proceeds, 2),
                "pnl": pnl, "pnl_pct": pnl_pct, "reason": reason,
            })
            del self.positions[sym]

    def force_exit(self, sym: str, price: float, date_str: str, reason: str = "SIGNAL") -> None:
        if sym not in self.positions:
            return
        pos        = self.positions[sym]
        sell_price = round(price * (1 - SLIPPAGE), 4)
        qty        = pos["qty"]
        proceeds   = qty * sell_price
        pnl        = round(proceeds - qty * pos["entry"], 2)
        pnl_pct    = round((sell_price - pos["entry"]) / pos["entry"] * 100, 2)
        self.cash += proceeds
        self.trades.append({
            "date": date_str, "symbol": sym, "action": "SELL",
            "price": sell_price, "qty": round(qty, 4),
            "proceeds": round(proceeds, 2),
            "pnl": pnl, "pnl_pct": pnl_pct, "reason": reason,
        })
        del self.positions[sym]

    def liquidate_all(self, prices: dict[str, float], date_str: str) -> None:
        for sym in list(self.positions.keys()):
            p = prices.get(sym, self.positions[sym]["entry"])
            self.force_exit(sym, p, date_str, "EOD_LIQUIDATE")


# ── Data download ─────────────────────────────────────────────────────────────

def download_data(symbols: list[str], start: date, end: date) -> dict[str, pd.DataFrame]:
    warmup_start = start - timedelta(days=100)
    all_syms = list(set(symbols + [BENCHMARK_SYM]))
    print(f"  Downloading {len(all_syms)} symbols: {warmup_start} → {end} ...")

    for attempt in range(3):
        try:
            raw = yf.download(
                all_syms,
                start=warmup_start.strftime("%Y-%m-%d"),
                end=(end + timedelta(days=1)).strftime("%Y-%m-%d"),
                auto_adjust=True, progress=False, group_by="ticker",
            )
            break
        except Exception as e:
            print(f"  Download attempt {attempt+1} failed: {e}")
            if attempt < 2:
                time.sleep(3)
            else:
                print("FATAL: Could not download data."); sys.exit(1)

    data: dict[str, pd.DataFrame] = {}
    for sym in all_syms:
        try:
            if len(all_syms) == 1:
                df = raw.dropna(subset=["Close"])
            else:
                try:
                    df = raw[sym].dropna(subset=["Close"])
                except Exception:
                    df = raw.xs(sym, level=1, axis=1).dropna(subset=["Close"])
            data[sym] = df
        except Exception:
            try:
                df = yf.download(sym, start=warmup_start.strftime("%Y-%m-%d"),
                                 end=(end + timedelta(days=1)).strftime("%Y-%m-%d"),
                                 auto_adjust=True, progress=False)
                data[sym] = df.dropna(subset=["Close"])
            except Exception as ex:
                print(f"  WARNING: {sym} failed: {ex}")
                data[sym] = pd.DataFrame()
        time.sleep(0.15)

    ok = sum(1 for d in data.values() if len(d) > 20)
    print(f"  OK: {ok}/{len(all_syms)} symbols with data")
    return data


# ── SPY benchmark ─────────────────────────────────────────────────────────────

def spy_benchmark(data: dict[str, pd.DataFrame], test_days: list[date]) -> float:
    df = data.get(BENCHMARK_SYM, pd.DataFrame())
    if len(df) == 0 or len(test_days) < 2:
        return 0.0
    start_rows = df[df.index.date <= test_days[0]]
    end_rows   = df[df.index.date <= test_days[-1]]
    if len(start_rows) == 0 or len(end_rows) == 0:
        return 0.0
    p0 = float(start_rows["Close"].iloc[-1])
    p1 = float(end_rows["Close"].iloc[-1])
    return round((p1 - p0) / p0 * 100, 2)


# ── Stats computation (with Sortino + Calmar) ─────────────────────────────────

def compute_stats(port: Portfolio, data: dict[str, pd.DataFrame],
                  spy_return_pct: float) -> dict:
    sell_trades  = [t for t in port.trades if t["action"] == "SELL" and t["pnl"] is not None]
    total_trades = len(sell_trades)
    winners      = [t for t in sell_trades if t["pnl"] > 0]
    losers       = [t for t in sell_trades if t["pnl"] <= 0]
    win_rate     = round(len(winners) / max(total_trades, 1) * 100, 1)

    # Final portfolio value (open positions at last known price)
    last_prices = {}
    for sym in list(port.positions.keys()):
        df = data.get(sym, pd.DataFrame())
        if len(df) > 0:
            last_prices[sym] = float(df["Close"].iloc[-1])
    final_value      = port.portfolio_value(last_prices)
    total_return_pct = round((final_value - port.capital) / port.capital * 100, 2)
    total_pnl        = round(final_value - port.capital, 2)

    # Daily returns from daily_values
    daily_vals_list = list(port.daily_values.values())

    # Sharpe ratio
    if len(daily_vals_list) > 1:
        returns  = np.array([daily_vals_list[i]/daily_vals_list[i-1]-1
                             for i in range(1, len(daily_vals_list))])
        mean_r   = float(np.mean(returns))
        std_r    = float(np.std(returns))
        sharpe   = round(mean_r / std_r * math.sqrt(252), 2) if std_r > 0 else 0.0

        # Sortino ratio (downside deviation only)
        down_returns = returns[returns < 0]
        down_std     = float(np.std(down_returns)) if len(down_returns) > 1 else (std_r or 1e-9)
        sortino      = round(mean_r / down_std * math.sqrt(252), 2) if down_std > 0 else 0.0
    else:
        sharpe  = 0.0
        sortino = 0.0

    # Max drawdown from daily_values
    vals = daily_vals_list or [port.capital]
    peak, mdd = vals[0], 0.0
    for v in vals:
        if v > peak:
            peak = v
        dd = (peak - v) / peak * 100
        if dd > mdd:
            mdd = dd
    mdd = round(mdd, 2)

    # Calmar ratio: annualized return / max drawdown
    annualized_return = round(total_return_pct * (365.0 / DAYS), 2)
    calmar = round(annualized_return / mdd, 2) if mdd > 0 else 0.0

    # Alpha vs SPY
    alpha = round(total_return_pct - spy_return_pct, 2)

    # Trade-level stats
    gross_wins  = sum(t["pnl"] for t in winners) if winners else 0
    gross_losses = abs(sum(t["pnl"] for t in losers)) if losers else 0
    profit_factor = round(gross_wins / gross_losses, 2) if gross_losses > 0 else (float("inf") if gross_wins > 0 else 0.0)
    avg_win  = round(gross_wins  / len(winners), 2) if winners else 0.0
    avg_loss = round(gross_losses / len(losers), 2) if losers else 0.0

    best_trade  = max(sell_trades, key=lambda t: t["pnl"], default=None)
    worst_trade = min(sell_trades, key=lambda t: t["pnl"], default=None)

    return {
        "final_value":       round(final_value, 2),
        "total_return_pct":  total_return_pct,
        "total_return_usd":  total_pnl,
        "annualized_return": annualized_return,
        "sharpe":            sharpe,
        "sortino":           sortino,
        "calmar":            calmar,
        "max_drawdown_pct":  mdd,
        "win_rate":          win_rate,
        "total_trades":      total_trades,
        "winners":           len(winners),
        "losers":            len(losers),
        "avg_win_usd":       avg_win,
        "avg_loss_usd":      avg_loss,
        "profit_factor":     profit_factor,
        "spy_return_pct":    spy_return_pct,
        "alpha_pct":         alpha,
        "best_trade":  {"symbol": best_trade["symbol"],  "pnl": best_trade["pnl"],  "date": best_trade["date"]}  if best_trade  else None,
        "worst_trade": {"symbol": worst_trade["symbol"], "pnl": worst_trade["pnl"], "date": worst_trade["date"]} if worst_trade else None,
        "trades":      sell_trades,
        "daily_values": port.daily_values,
    }


# ── Single agent simulation ───────────────────────────────────────────────────

def run_agent(cfg: dict, data: dict[str, pd.DataFrame],
              test_days: list[date], spy_return_pct: float) -> dict:
    agent_id  = cfg["id"]
    universe  = cfg["universe"]
    min_conv  = cfg["min_conv"]
    rsi_buy   = cfg["rsi_buy"]
    rsi_sell  = cfg["rsi_sell"]
    max_pos   = cfg["max_pos_pct"]
    qual_gate = cfg["qual_gate"]
    regime_block = tuple(cfg["regime_block"])

    port   = Portfolio(capital=STARTING_CAPITAL,
                       stop_pct=cfg["stop_pct"], tp_pct=cfg["tp_pct"])
    spy_df = data.get(BENCHMARK_SYM, pd.DataFrame())

    for i, trade_date in enumerate(test_days):
        ds = trade_date.strftime("%Y-%m-%d")
        spy_sub = spy_df[spy_df.index.date <= trade_date]
        regime  = detect_regime(spy_sub["Close"].values) if len(spy_sub) >= 21 else "UNKNOWN"

        # Collect today's prices for portfolio mark
        prices_today = {}
        for sym in universe:
            df = data.get(sym, pd.DataFrame())
            today = df[df.index.date == trade_date]
            if len(today) > 0:
                prices_today[sym] = float(today["Close"].iloc[0])

        # Check exits on held positions
        for sym in list(port.positions.keys()):
            df = data.get(sym, pd.DataFrame())
            today = df[df.index.date == trade_date]
            if len(today) == 0:
                continue
            port.check_exits(
                sym,
                float(today["Open"].iloc[0]),
                float(today["High"].iloc[0]),
                float(today["Low"].iloc[0]),
                float(today["Close"].iloc[0]),
                ds,
            )

        # RSI sell signal on held positions not covered by stop/tp
        if regime not in regime_block:
            for sym in list(port.positions.keys()):
                df = data.get(sym, pd.DataFrame())
                prior = df[df.index.date < trade_date]
                today = df[df.index.date == trade_date]
                if len(prior) < 20 or len(today) == 0:
                    continue
                rsi_now = calc_rsi(prior["Close"].values)
                if rsi_now > rsi_sell:
                    port.force_exit(sym, float(today["Open"].iloc[0]), ds, "RSI_SELL")

        # Evaluate new entries
        if regime not in regime_block:
            for sym in universe:
                if sym == BENCHMARK_SYM or sym in port.positions:
                    continue
                if port.open_count() >= MAX_POSITIONS:
                    break
                df    = data.get(sym, pd.DataFrame())
                prior = df[df.index.date < trade_date]
                today = df[df.index.date == trade_date]
                if len(prior) < 55 or len(today) == 0:
                    continue

                conviction, score, signals = score_and_signals(prior)
                if conviction < min_conv:
                    continue

                closes    = prior["Close"].values
                rsi_val   = calc_rsi(closes)
                sma_20    = calc_sma(closes, 20)
                sma_50    = calc_sma(closes, 50)
                vol_ratio = calc_vol_ratio(prior["Volume"].values)
                price     = float(closes[-1])
                mom_5d    = (closes[-1] - closes[-6]) / closes[-6] * 100 if len(closes) >= 6 else 0.0

                qual_checks = [
                    rsi_val < rsi_buy,
                    vol_ratio >= 1.2,
                    mom_5d > 0,
                    price > sma_20,
                    price > sma_50,
                ]
                if sum(qual_checks) < qual_gate:
                    continue

                day_open = float(today["Open"].iloc[0])
                port.try_buy(sym, day_open, ds, max_pos, conviction, signals)

        # Mark portfolio value
        port.daily_values[ds] = port.portfolio_value(prices_today)

    # Liquidate remaining open positions at end
    final_prices = {}
    for sym in universe:
        df = data.get(sym, pd.DataFrame())
        if len(df) > 0:
            final_prices[sym] = float(df["Close"].iloc[-1])
    if port.positions:
        port.liquidate_all(final_prices, test_days[-1].strftime("%Y-%m-%d"))

    return compute_stats(port, data, spy_return_pct)


# ── DB persistence ────────────────────────────────────────────────────────────

def _ensure_backtest_table(conn: sqlite3.Connection) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS backtest_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id TEXT NOT NULL,
        player_name TEXT,
        run_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        period_days INTEGER DEFAULT 30,
        start_date TEXT,
        end_date TEXT,
        starting_value REAL DEFAULT 7000,
        final_value REAL,
        return_pct REAL,
        total_pnl REAL,
        win_count INTEGER DEFAULT 0,
        loss_count INTEGER DEFAULT 0,
        win_rate REAL DEFAULT 0,
        total_trades INTEGER DEFAULT 0,
        best_trade_pnl REAL,
        worst_trade_pnl REAL,
        best_trade_symbol TEXT,
        worst_trade_symbol TEXT,
        spy_return_pct REAL,
        rallies_top_return_pct REAL,
        rallies_top_name TEXT,
        notes TEXT,
        config_snapshot TEXT,
        guardrails_applied INTEGER DEFAULT 0,
        signals_tested INTEGER DEFAULT 0,
        signals_skipped INTEGER DEFAULT 0,
        skip_summary TEXT DEFAULT NULL
    )""")
    conn.commit()


def save_to_db(agent_cfg: dict, stats: dict, start_dt: date, end_dt: date) -> None:
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        _ensure_backtest_table(conn)

        # Store extended metrics in notes as JSON
        extended = {
            "sharpe":            stats.get("sharpe"),
            "sortino":           stats.get("sortino"),
            "calmar":            stats.get("calmar"),
            "alpha_pct":         stats.get("alpha_pct"),
            "annualized_return": stats.get("annualized_return"),
            "profit_factor":     stats.get("profit_factor"),
            "avg_win_usd":       stats.get("avg_win_usd"),
            "avg_loss_usd":      stats.get("avg_loss_usd"),
            "description":       agent_cfg.get("description"),
            "universe":          agent_cfg.get("universe"),
            "rsi_buy":           agent_cfg.get("rsi_buy"),
            "rsi_sell":          agent_cfg.get("rsi_sell"),
            "min_conv":          agent_cfg.get("min_conv"),
            "backtest_type":     "s6_180d_comparative",
            "run_version":       "s6_180d_backtest_v1",
        }

        bt = stats.get("best_trade")  or {}
        wt = stats.get("worst_trade") or {}

        conn.execute("""
            INSERT INTO backtest_history
              (player_id, player_name, period_days, start_date, end_date,
               starting_value, final_value, return_pct, total_pnl,
               win_count, loss_count, win_rate, total_trades,
               best_trade_pnl, worst_trade_pnl, best_trade_symbol, worst_trade_symbol,
               spy_return_pct, notes, config_snapshot, guardrails_applied)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            agent_cfg["id"],
            agent_cfg["name"],
            DAYS,
            start_dt.strftime("%Y-%m-%d"),
            end_dt.strftime("%Y-%m-%d"),
            STARTING_CAPITAL,
            stats.get("final_value"),
            stats.get("total_return_pct"),
            stats.get("total_return_usd"),
            stats.get("winners"),
            stats.get("losers"),
            stats.get("win_rate"),
            stats.get("total_trades"),
            bt.get("pnl"),
            wt.get("pnl"),
            bt.get("symbol"),
            wt.get("symbol"),
            stats.get("spy_return_pct"),
            json.dumps(extended),
            json.dumps({"rsi_buy": agent_cfg.get("rsi_buy"),
                        "rsi_sell": agent_cfg.get("rsi_sell"),
                        "min_conv": agent_cfg.get("min_conv"),
                        "stop_pct": agent_cfg.get("stop_pct"),
                        "tp_pct":   agent_cfg.get("tp_pct"),
                        "qual_gate": agent_cfg.get("qual_gate"),
                        "universe_size": len(agent_cfg.get("universe", []))}),
            0,  # guardrails_applied
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"  [DB ERROR] {agent_cfg['id']}: {e}")


# ── Print helpers ─────────────────────────────────────────────────────────────

def print_banner() -> None:
    print()
    print("=" * 80)
    print("  S6 ALL-AGENT 180-DAY COMPARATIVE BACKTEST")
    print(f"  Capital: ${STARTING_CAPITAL:,.0f}  |  Period: {DAYS} days  |  Benchmark: SPY")
    print("=" * 80)


def print_summary_table(results: dict[str, dict], spy_return: float) -> None:
    print()
    print("=" * 100)
    print(f"  COMPARISON TABLE — 180-Day Season 6 Backtest  |  SPY: {spy_return:+.2f}%")
    print("=" * 100)
    hdr = (f"  {'Agent':<18} {'Return%':>8} {'Return$':>10} {'Sharpe':>7} "
           f"{'Sortino':>8} {'Calmar':>7} {'MaxDD%':>7} {'WinRate':>8} "
           f"{'Alpha':>7} {'Trades':>7}")
    print(hdr)
    print("-" * 100)

    # Sort by total return descending
    sorted_agents = sorted(
        [(aid, s) for aid, s in results.items() if isinstance(s, dict)],
        key=lambda x: x[1].get("total_return_pct", -999),
        reverse=True
    )
    for rank, (aid, s) in enumerate(sorted_agents, 1):
        ret   = s.get("total_return_pct", 0)
        rusd  = s.get("total_return_usd", 0)
        sharpe = s.get("sharpe", 0)
        sortino = s.get("sortino", 0)
        calmar = s.get("calmar", 0)
        mdd   = s.get("max_drawdown_pct", 0)
        wr    = s.get("win_rate", 0)
        alpha = s.get("alpha_pct", 0)
        trades = s.get("total_trades", 0)
        alpha_str = f"{alpha:+.2f}%"
        ret_str = f"{ret:+.2f}%"
        medal = "🥇" if rank == 1 else ("🥈" if rank == 2 else ("🥉" if rank == 3 else f"{rank}. "))
        print(f"  {medal} {aid:<16} {ret_str:>8} {rusd:>+10,.0f} {sharpe:>7.2f} "
              f"{sortino:>8.2f} {calmar:>7.2f} {mdd:>7.2f} {wr:>7.1f}% "
              f"{alpha_str:>8} {trades:>7}")

    for aid, s in results.items():
        if isinstance(s, str):
            print(f"  ✗  {aid:<16} {s}")
    print("-" * 100)
    print()


def print_individual(agent_id: str, agent_name: str, stats: dict) -> None:
    bt = stats.get("best_trade")  or {}
    wt = stats.get("worst_trade") or {}
    print(f"\n  ── {agent_name} ({agent_id}) ──────────────────────────────")
    print(f"  Total Return:      {stats['total_return_pct']:+.2f}%  (${stats['total_return_usd']:+,.2f})")
    print(f"  Annualized:        {stats['annualized_return']:+.2f}%")
    print(f"  Final Value:       ${stats['final_value']:,.2f}")
    print(f"  Sharpe Ratio:      {stats['sharpe']:.3f}")
    print(f"  Sortino Ratio:     {stats['sortino']:.3f}")
    print(f"  Calmar Ratio:      {stats['calmar']:.3f}")
    print(f"  Max Drawdown:      -{stats['max_drawdown_pct']:.2f}%")
    print(f"  Win Rate:          {stats['win_rate']:.1f}%  ({stats['winners']}W / {stats['losers']}L)")
    print(f"  Profit Factor:     {stats['profit_factor']}")
    print(f"  Alpha vs SPY:      {stats['alpha_pct']:+.2f}%")
    print(f"  Total Trades:      {stats['total_trades']}")
    print(f"  Avg Win:           ${stats['avg_win_usd']:,.2f}")
    print(f"  Avg Loss:          -${stats['avg_loss_usd']:,.2f}")
    if bt:
        print(f"  Best Trade:        {bt['symbol']} +${bt['pnl']:,.2f} ({bt['date']})")
    if wt:
        print(f"  Worst Trade:       {wt['symbol']} -${abs(wt['pnl']):,.2f} ({wt['date']})")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print_banner()

    # Date range
    start_dt, end_dt = last_n_trading_days(DAYS)
    test_days = trading_days(start_dt, end_dt)
    print(f"\n  Period:     {start_dt} → {end_dt}  ({len(test_days)} trading days)")
    print(f"  Agents:     {len(AGENTS_S6)}")

    # Collect all unique symbols
    all_symbols = list(set(
        sym for cfg in AGENTS_S6 for sym in cfg["universe"]
    ) | {BENCHMARK_SYM})
    print(f"  Universe:   {len(all_symbols)} unique symbols across all agents")

    # Download historical data (one batch for all agents)
    print()
    all_data = download_data(all_symbols, start_dt, end_dt)

    # SPY benchmark
    spy_return = spy_benchmark(all_data, test_days)
    print(f"\n  SPY 180-day return: {spy_return:+.2f}%")
    print(f"  SPY 180-day P/L on $100k: ${spy_return/100*STARTING_CAPITAL:+,.2f}")

    # Run each agent
    print()
    all_results: dict[str, dict] = {}

    for cfg in AGENTS_S6:
        agent_id   = cfg["id"]
        agent_name = cfg["name"]
        print(f"  ▸ {agent_name:<20} ({agent_id}) ...", end=" ", flush=True)

        result_holder: list = []
        err_holder:    list = []

        def _run(c=cfg, data=all_data, days=test_days, spy=spy_return,
                 rh=result_holder, eh=err_holder):
            try:
                rh.append(run_agent(c, data, days, spy))
            except Exception as ex:
                import traceback
                eh.append(f"{ex}\n{traceback.format_exc()}")

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        t.join(timeout=AGENT_TIMEOUT_S)

        if t.is_alive():
            print("TIMEOUT")
            all_results[agent_id] = "TIMEOUT"
        elif err_holder:
            print(f"ERROR: {err_holder[0][:80]}")
            all_results[agent_id] = f"ERROR: {err_holder[0][:80]}"
        else:
            stats = result_holder[0]
            all_results[agent_id] = stats
            print(f"return={stats['total_return_pct']:+.2f}%  "
                  f"sharpe={stats['sharpe']:.2f}  "
                  f"wr={stats['win_rate']:.1f}%  "
                  f"trades={stats['total_trades']}")

            # Save to trader.db
            save_to_db(cfg, stats, start_dt, end_dt)

    # Print summary table
    print_summary_table(all_results, spy_return)

    # Print individual breakdowns
    print("  INDIVIDUAL AGENT BREAKDOWNS")
    print("  " + "─" * 60)
    for cfg in AGENTS_S6:
        s = all_results.get(cfg["id"])
        if isinstance(s, dict):
            print_individual(cfg["id"], cfg["name"], s)
        else:
            print(f"\n  ── {cfg['name']} ({cfg['id']}) — {s}")

    # Save JSON output
    output = {
        "run_date":       datetime.now().isoformat(),
        "period_days":    DAYS,
        "start_date":     start_dt.strftime("%Y-%m-%d"),
        "end_date":       end_dt.strftime("%Y-%m-%d"),
        "trading_days":   len(test_days),
        "starting_capital": STARTING_CAPITAL,
        "spy_return_pct": spy_return,
        "agents": {
            cfg["id"]: {
                "config": {k: v for k, v in cfg.items() if k != "universe"},
                "stats":  {k: v for k, v in (all_results.get(cfg["id"]) or {}).items()
                           if k not in ("trades", "daily_values")}
                          if isinstance(all_results.get(cfg["id"]), dict) else all_results.get(cfg["id"]),
            }
            for cfg in AGENTS_S6
        },
    }
    with open(OUT_PATH, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"\n  Saved → {OUT_PATH}")
    print(f"  Saved → {DB_PATH} (backtest_history table)")
    print()
    print("=" * 80)
    print()


if __name__ == "__main__":
    main()
