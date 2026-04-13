#!/usr/bin/env python3
"""
ollie_backtest_compare_v5.py — Three-Way Agent Backtest Comparison

Tests:
  A  BASELINE    — Pure RSI(14)/SMA rules, no AI, single run
  B  V2_REPLAY   — AI agents, conviction ≥ 0.6, quality gate 3-of-5 signals
  C  CURRENT_S6  — Exact S6 guardrails per agent (conviction-scaled sizing)

Consistent variables across all tests:
  Universe    : SPY QQQ AAPL MSFT NVDA AMD TSLA AMZN META GOOGL
  Capital     : $10,000 per agent
  Slippage    : 0.1% per side (equity)
  Max position: 20% of portfolio
  Stop loss   : -8%
  Take profit : +15%
  Benchmark   : SPY buy-and-hold same period

NEVER touches trader.db or arena.db.
Saves to: data/backtest_v5_results.json  +  data/backtest_v5_detail.json

Usage:
  venv/bin/python3 scripts/ollie_backtest_compare_v5.py --days 5
  venv/bin/python3 scripts/ollie_backtest_compare_v5.py --days 60
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
import threading
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

# ── Output paths (NEVER trader.db / arena.db) ────────────────────────────────
_ROOT   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(_ROOT, "data")
RESULTS_PATH = os.path.join(OUT_DIR, "backtest_v5_results.json")
DETAIL_PATH  = os.path.join(OUT_DIR, "backtest_v5_detail.json")

# ── Consistent simulation parameters ─────────────────────────────────────────
UNIVERSE         = ["SPY", "QQQ", "AAPL", "MSFT", "NVDA", "AMD",
                    "TSLA", "AMZN", "META", "GOOGL"]
BENCHMARK_SYM    = "SPY"
STARTING_CAPITAL = 10_000.0
SLIPPAGE         = 0.001   # 0.1% per side
MAX_POSITION_PCT = 0.20    # 20% of portfolio per position
STOP_LOSS_PCT    = 0.08    # -8%
TAKE_PROFIT_PCT  = 0.15    # +15%
MAX_POSITIONS    = 5
AGENT_TIMEOUT_S  = 120

AGENTS = [
    "navigator",
    "ollama-qwen3",
    "ollama-plutus",
    "ollama-coder",
    "neo-matrix",
    "ollama-llama",
]

US_HOLIDAYS = {
    date(2026, 1, 1), date(2026, 1, 19), date(2026, 2, 16),
    date(2026, 4, 3), date(2026, 5, 25), date(2026, 7, 3),
    date(2026, 9, 7), date(2026, 11, 26), date(2026, 12, 25),
    date(2025, 1, 1), date(2025, 1, 20), date(2025, 2, 17),
    date(2025, 4, 18), date(2025, 5, 26), date(2025, 7, 4),
    date(2025, 9, 1), date(2025, 11, 27), date(2025, 12, 25),
}

# ── Agent-specific configs ────────────────────────────────────────────────────
# TEST B: V2_Replay — all use conviction 0.6, slightly different RSI thresholds
AGENT_B_CONFIG = {
    "navigator":     {"rsi_buy": 40, "rsi_sell": 60, "min_conv": 0.55},
    "ollama-qwen3":  {"rsi_buy": 35, "rsi_sell": 65, "min_conv": 0.60},
    "ollama-plutus": {"rsi_buy": 30, "rsi_sell": 70, "min_conv": 0.65},
    "ollama-coder":  {"rsi_buy": 38, "rsi_sell": 62, "min_conv": 0.60},
    "neo-matrix":    {"rsi_buy": 40, "rsi_sell": 60, "min_conv": 0.55},
    "ollama-llama":  {"rsi_buy": 35, "rsi_sell": 65, "min_conv": 0.60},
}

# TEST C: CURRENT_S6 — from risk_manager.py MODEL_GUARDRAILS + ai_brain.py sizing
# min_conviction, max_pos_pct, stop_pct (override), score_rsi_buy, score_rsi_sell
AGENT_S6_CONFIG = {
    "navigator":     {"min_conv": 0.50, "max_pos_pct": 0.20, "rsi_buy": 40, "rsi_sell": 60},
    "ollama-qwen3":  {"min_conv": 0.60, "max_pos_pct": 0.20, "rsi_buy": 35, "rsi_sell": 65},
    "ollama-plutus": {"min_conv": 0.60, "max_pos_pct": 0.20, "rsi_buy": 30, "rsi_sell": 70},
    "ollama-coder":  {"min_conv": 0.60, "max_pos_pct": 0.20, "rsi_buy": 38, "rsi_sell": 62},
    "neo-matrix":    {"min_conv": 0.60, "max_pos_pct": 0.20, "rsi_buy": 40, "rsi_sell": 60},
    "ollama-llama":  {"min_conv": 0.60, "max_pos_pct": 0.20, "rsi_buy": 35, "rsi_sell": 65},
}

# ── Holiday / trading-day helpers ─────────────────────────────────────────────

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


# ── Technical helpers ─────────────────────────────────────────────────────────

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
    """Return (conviction 0-1, score 0-100, signal_names)."""
    if len(df_prior) < 20:
        return 0.0, 0.0, []
    closes  = df_prior["Close"].values
    volumes = df_prior["Volume"].values

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
    """Tracks cash, open positions, and completed trades for one agent run."""

    def __init__(self, capital: float = STARTING_CAPITAL):
        self.cash     = capital
        self.capital  = capital
        self.positions: dict[str, dict] = {}   # sym → {qty, entry, sl, tp, date}
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
        alloc_cash = min(alloc_cash, self.cash * 0.95)   # keep 5% buffer
        if alloc_cash < 50:
            return False
        qty     = alloc_cash / buy_price
        cost    = qty * buy_price
        self.cash -= cost
        self.positions[sym] = {
            "qty":       qty,
            "entry":     buy_price,
            "sl":        round(buy_price * (1 - STOP_LOSS_PCT), 4),
            "tp":        round(buy_price * (1 + TAKE_PROFIT_PCT), 4),
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
        pos = self.positions[sym]
        entry = pos["entry"]
        sl    = pos["sl"]
        tp    = pos["tp"]
        qty   = pos["qty"]

        exit_price, reason = None, None

        # Gap-down through SL at open
        if day_open <= sl:
            exit_price, reason = day_open, "STOP_GAP"
        # Intraday SL
        elif day_low <= sl:
            exit_price, reason = sl, "STOP_LOSS"
        # Intraday TP
        elif day_high >= tp:
            exit_price, reason = tp, "TAKE_PROFIT"
        # EOD — keep holding (exits handled by explicit sell signal or next SL/TP)

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
    print(f"  Downloading OHLCV: {warmup_start} → {end} for {len(symbols)} symbols...")
    all_syms = list(set(symbols + [BENCHMARK_SYM]))

    # Batch download
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
                print("  FATAL: Could not download data."); sys.exit(1)

    data: dict[str, pd.DataFrame] = {}
    for sym in all_syms:
        try:
            if len(all_syms) == 1:
                df = raw.dropna(subset=["Close"])
            elif sym in raw.columns.get_level_values(0):
                df = raw[sym].dropna(subset=["Close"])
            else:
                df = raw.xs(sym, level=1, axis=1).dropna(subset=["Close"])
            data[sym] = df
        except Exception:
            time.sleep(2)
            try:
                df = yf.download(sym, start=warmup_start.strftime("%Y-%m-%d"),
                                 end=(end + timedelta(days=1)).strftime("%Y-%m-%d"),
                                 auto_adjust=True, progress=False)
                data[sym] = df.dropna(subset=["Close"])
            except Exception as ex:
                print(f"  WARNING: {sym} failed: {ex}")
                data[sym] = pd.DataFrame()
        time.sleep(0.5)   # be gentle

    print(f"  Downloaded {sum(1 for d in data.values() if len(d)>20)}/{len(all_syms)} symbols OK")
    return data


# ── Stats computation ─────────────────────────────────────────────────────────

def compute_stats(port: Portfolio, test_days: list[date],
                  data: dict[str, pd.DataFrame], spy_return_pct: float) -> dict:
    sell_trades = [t for t in port.trades if t["action"] == "SELL" and t["pnl"] is not None]
    total_trades = len(sell_trades)
    winners = [t for t in sell_trades if t["pnl"] > 0]
    losers  = [t for t in sell_trades if t["pnl"] <= 0]
    win_rate = round(len(winners) / max(total_trades, 1) * 100, 1)
    total_pnl = sum(t["pnl"] for t in sell_trades)

    # Final portfolio value (liquidate open positions at last known price)
    last_prices = {}
    for sym in list(port.positions.keys()):
        df = data.get(sym, pd.DataFrame())
        if len(df) > 0:
            last_prices[sym] = float(df["Close"].iloc[-1])
    final_value = port.portfolio_value(last_prices)
    total_return_pct = round((final_value - port.capital) / port.capital * 100, 2)

    # Daily portfolio values — approximate with trade-day marks
    daily_vals_list = list(port.daily_values.values()) if port.daily_values else []
    if len(daily_vals_list) > 1:
        returns = [daily_vals_list[i]/daily_vals_list[i-1]-1
                   for i in range(1, len(daily_vals_list))]
        mean_r  = float(np.mean(returns))
        std_r   = float(np.std(returns))
        sharpe  = round(mean_r / std_r * math.sqrt(252), 2) if std_r > 0 else 0.0
    else:
        sharpe = 0.0

    # Max drawdown from daily_values
    vals = list(port.daily_values.values()) if port.daily_values else [port.capital]
    peak, mdd = vals[0], 0.0
    for v in vals:
        if v > peak:
            peak = v
        dd = (peak - v) / peak * 100
        if dd > mdd:
            mdd = dd
    mdd = round(mdd, 2)

    avg_trade_return = round(sum(t.get("pnl_pct", 0) for t in sell_trades) / max(total_trades, 1), 2)
    best_trade  = max(sell_trades, key=lambda t: t["pnl"], default=None)
    worst_trade = min(sell_trades, key=lambda t: t["pnl"], default=None)

    # Monthly breakdown
    monthly: dict[str, dict] = defaultdict(lambda: {"pnl": 0.0, "trades": 0, "wins": 0})
    for t in sell_trades:
        m = t["date"][:7]
        monthly[m]["pnl"]    += t["pnl"]
        monthly[m]["trades"] += 1
        if t["pnl"] > 0:
            monthly[m]["wins"] += 1

    return {
        "final_value":      round(final_value, 2),
        "total_return_pct": total_return_pct,
        "sharpe":           sharpe,
        "win_rate":         win_rate,
        "max_drawdown_pct": mdd,
        "total_trades":     total_trades,
        "winners":          len(winners),
        "losers":           len(losers),
        "avg_trade_return": avg_trade_return,
        "best_trade":       {
            "symbol": best_trade["symbol"], "pnl": best_trade["pnl"],
            "pnl_pct": best_trade.get("pnl_pct"), "date": best_trade["date"]
        } if best_trade else None,
        "worst_trade": {
            "symbol": worst_trade["symbol"], "pnl": worst_trade["pnl"],
            "pnl_pct": worst_trade.get("pnl_pct"), "date": worst_trade["date"]
        } if worst_trade else None,
        "spy_return_pct":   spy_return_pct,
        "alpha_pct":        round(total_return_pct - spy_return_pct, 2),
        "monthly":          {m: dict(v) for m, v in sorted(monthly.items())},
        "trades":           sell_trades,
    }


# ── SPY benchmark ─────────────────────────────────────────────────────────────

def spy_benchmark(data: dict[str, pd.DataFrame], test_days: list[date]) -> float:
    df = data.get(BENCHMARK_SYM, pd.DataFrame())
    if len(df) == 0 or len(test_days) < 2:
        return 0.0
    start_mask = df.index.date <= test_days[0]
    end_mask   = df.index.date <= test_days[-1]
    start_rows = df[start_mask]
    end_rows   = df[end_mask]
    if len(start_rows) == 0 or len(end_rows) == 0:
        return 0.0
    p0 = float(start_rows["Close"].iloc[-1])
    p1 = float(end_rows["Close"].iloc[-1])
    return round((p1 - p0) / p0 * 100, 2)


# ── TEST A — BASELINE (pure RSI/SMA, no AI) ───────────────────────────────────

def run_test_a(data: dict[str, pd.DataFrame], test_days: list[date]) -> dict:
    print("\n[TEST A] BASELINE — RSI(14) + 50-SMA, no AI")
    port = Portfolio()
    spy_df = data.get(BENCHMARK_SYM, pd.DataFrame())

    for i, trade_date in enumerate(test_days):
        ds = trade_date.strftime("%Y-%m-%d")
        spy_sub = spy_df[spy_df.index.date <= trade_date]
        regime  = detect_regime(spy_sub["Close"].values) if len(spy_sub) >= 21 else "UNKNOWN"

        # Check existing positions first
        prices_today = {}
        for sym in UNIVERSE:
            df = data.get(sym, pd.DataFrame())
            today = df[df.index.date == trade_date]
            if len(today) > 0:
                prices_today[sym] = float(today["Close"].iloc[0])

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

        # Evaluate new entries (skip CRISIS/BEAR_STRONG)
        if regime not in ("CRISIS", "BEAR_STRONG"):
            for sym in UNIVERSE:
                if sym == BENCHMARK_SYM or sym in port.positions:
                    continue
                df = data.get(sym, pd.DataFrame())
                prior = df[df.index.date < trade_date]
                today = df[df.index.date == trade_date]
                if len(prior) < 55 or len(today) == 0:
                    continue

                closes   = prior["Close"].values
                rsi_val  = calc_rsi(closes)
                sma_50   = calc_sma(closes, 50)
                price    = float(closes[-1])
                day_open = float(today["Open"].iloc[0])

                # Rule: RSI < 35 → BUY, only if price > 50 SMA
                if rsi_val < 35 and price > sma_50:
                    port.try_buy(sym, day_open, ds, MAX_POSITION_PCT, 0.60, ["RSI_OVERSOLD", "ABOVE_SMA50"])

                # Rule: RSI > 65 → SELL any open position
                elif rsi_val > 65 and sym in port.positions:
                    port.force_exit(sym, day_open, ds, "RSI_OVERBOUGHT")

        # Record daily portfolio value
        port.daily_values[ds] = port.portfolio_value(prices_today)

        if (i + 1) % 10 == 0 or i == len(test_days) - 1:
            print(f"  [{i+1:>3}/{len(test_days)}] {ds}  regime={regime:<12}  "
                  f"positions={port.open_count()}  "
                  f"value=${port.portfolio_value(prices_today):,.0f}")

    spy_ret = spy_benchmark(data, test_days)
    stats   = compute_stats(port, test_days, data, spy_ret)
    print(f"  → Return: {stats['total_return_pct']:+.2f}%  "
          f"Sharpe: {stats['sharpe']:.2f}  "
          f"WR: {stats['win_rate']:.1f}%  "
          f"MaxDD: {stats['max_drawdown_pct']:.1f}%  "
          f"Trades: {stats['total_trades']}")
    return stats


# ── TEST B — V2_REPLAY (AI agents, conviction 0.6, quality gate 3/5) ─────────

def run_test_b_agent(agent_id: str, data: dict[str, pd.DataFrame],
                     test_days: list[date]) -> dict | str:
    """Run V2_Replay for one agent. Returns stats dict or 'TIMEOUT'."""
    cfg     = AGENT_B_CONFIG[agent_id]
    min_conv = cfg["min_conv"]
    rsi_buy  = cfg["rsi_buy"]
    rsi_sell = cfg["rsi_sell"]

    port   = Portfolio()
    spy_df = data.get(BENCHMARK_SYM, pd.DataFrame())

    for i, trade_date in enumerate(test_days):
        ds = trade_date.strftime("%Y-%m-%d")
        spy_sub = spy_df[spy_df.index.date <= trade_date]
        regime  = detect_regime(spy_sub["Close"].values) if len(spy_sub) >= 21 else "UNKNOWN"

        prices_today = {}
        for sym in UNIVERSE:
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

        # Evaluate entries
        if regime not in ("CRISIS", "BEAR_STRONG"):
            for sym in UNIVERSE:
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

                closes   = prior["Close"].values
                rsi_val  = calc_rsi(closes)
                sma_20   = calc_sma(closes, 20)
                sma_50   = calc_sma(closes, 50)
                vol_ratio = calc_vol_ratio(prior["Volume"].values)
                price    = float(closes[-1])
                mom_5d   = (closes[-1] - closes[-6]) / closes[-6] * 100 if len(closes) >= 6 else 0.0

                # 5 quality-gate checks (need 3+ for BUY)
                qual_checks = [
                    rsi_val < rsi_buy,          # 1. RSI oversold
                    vol_ratio >= 1.2,           # 2. Elevated volume
                    mom_5d > 0,                 # 3. Positive momentum
                    price > sma_20,             # 4. Above 20 SMA
                    price > sma_50,             # 5. Above 50 SMA
                ]
                qual_score = sum(qual_checks)
                if qual_score < 3:
                    continue

                # RSI sell signal on held positions
                if rsi_val > rsi_sell and sym in port.positions:
                    day_open = float(today["Open"].iloc[0])
                    port.force_exit(sym, day_open, ds, "RSI_SIGNAL")
                    continue

                day_open = float(today["Open"].iloc[0])
                port.try_buy(sym, day_open, ds, MAX_POSITION_PCT, conviction, signals)

        port.daily_values[ds] = port.portfolio_value(prices_today)

    spy_ret = spy_benchmark(data, test_days)
    return compute_stats(port, test_days, data, spy_ret)


def run_test_b(data: dict[str, pd.DataFrame], test_days: list[date]) -> dict[str, dict | str]:
    print("\n[TEST B] V2_REPLAY — AI agents, conviction ≥ 0.6, quality gate 3/5")
    results = {}
    for agent_id in AGENTS:
        print(f"  Running agent: {agent_id} ...", end=" ", flush=True)
        result_holder: list = []
        err_holder:    list = []

        def _run(rid=agent_id, rh=result_holder, eh=err_holder):
            try:
                rh.append(run_test_b_agent(rid, data, test_days))
            except Exception as ex:
                eh.append(str(ex))

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        t.join(timeout=AGENT_TIMEOUT_S)

        if t.is_alive():
            print("TIMEOUT")
            results[agent_id] = "TIMEOUT"
        elif err_holder:
            print(f"ERROR: {err_holder[0]}")
            results[agent_id] = f"ERROR: {err_holder[0]}"
        else:
            stats = result_holder[0]
            results[agent_id] = stats
            print(f"return={stats['total_return_pct']:+.2f}%  "
                  f"sharpe={stats['sharpe']:.2f}  "
                  f"wr={stats['win_rate']:.1f}%  "
                  f"trades={stats['total_trades']}")
    return results


# ── TEST C — CURRENT_S6 (S6 guardrails, conviction-scaled sizing) ─────────────

def s6_conviction_position_pct(conviction: float, max_pos_pct: float) -> float:
    """Conviction-weighted sizing from ai_brain.py (lines 1161-1165)."""
    if conviction >= 0.80:
        return max_pos_pct
    elif conviction >= 0.50:
        return max_pos_pct * 0.50
    else:
        return max_pos_pct * 0.25


def run_test_c_agent(agent_id: str, data: dict[str, pd.DataFrame],
                     test_days: list[date]) -> dict | str:
    """Run CURRENT_S6 for one agent."""
    cfg      = AGENT_S6_CONFIG[agent_id]
    min_conv = cfg["min_conv"]
    rsi_buy  = cfg["rsi_buy"]
    rsi_sell = cfg["rsi_sell"]
    max_pos  = cfg["max_pos_pct"]

    port   = Portfolio()
    spy_df = data.get(BENCHMARK_SYM, pd.DataFrame())

    for i, trade_date in enumerate(test_days):
        ds = trade_date.strftime("%Y-%m-%d")
        spy_sub = spy_df[spy_df.index.date <= trade_date]
        regime  = detect_regime(spy_sub["Close"].values) if len(spy_sub) >= 21 else "UNKNOWN"

        prices_today = {}
        for sym in UNIVERSE:
            df = data.get(sym, pd.DataFrame())
            today = df[df.index.date == trade_date]
            if len(today) > 0:
                prices_today[sym] = float(today["Close"].iloc[0])

        # S6: hard skip on CRISIS / BEAR_STRONG
        skip = regime in ("CRISIS", "BEAR_STRONG")

        # Check exits (SL/TP always honored regardless of regime)
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

        # Entries (skip in CRISIS/BEAR_STRONG)
        if not skip:
            for sym in UNIVERSE:
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

                closes   = prior["Close"].values
                rsi_val  = calc_rsi(closes)
                sma_50   = calc_sma(closes, 50)
                price    = float(closes[-1])

                # S6 gate: price must be above 50 SMA in non-BULL_STRONG regimes
                if regime not in ("BULL_STRONG", "BULL") and price < sma_50:
                    continue

                # RSI sell on existing position
                if rsi_val > rsi_sell and sym in port.positions:
                    day_open = float(today["Open"].iloc[0])
                    port.force_exit(sym, day_open, ds, "RSI_SIGNAL_S6")
                    continue

                # BUY: RSI below threshold + conviction gate
                if rsi_val < rsi_buy:
                    alloc = s6_conviction_position_pct(conviction, max_pos)
                    day_open = float(today["Open"].iloc[0])
                    port.try_buy(sym, day_open, ds, alloc, conviction, signals)
                elif conviction >= 0.80 and rsi_val < 55:
                    # High-conviction override: buy at half-size even without deep RSI dip
                    alloc = s6_conviction_position_pct(conviction, max_pos) * 0.5
                    day_open = float(today["Open"].iloc[0])
                    port.try_buy(sym, day_open, ds, alloc, conviction, signals)

        port.daily_values[ds] = port.portfolio_value(prices_today)

    spy_ret = spy_benchmark(data, test_days)
    return compute_stats(port, test_days, data, spy_ret)


def run_test_c(data: dict[str, pd.DataFrame], test_days: list[date]) -> dict[str, dict | str]:
    print("\n[TEST C] CURRENT_S6 — Exact S6 guardrails per agent")
    results = {}
    for agent_id in AGENTS:
        print(f"  Running agent: {agent_id} ...", end=" ", flush=True)
        result_holder: list = []
        err_holder:    list = []

        def _run(rid=agent_id, rh=result_holder, eh=err_holder):
            try:
                rh.append(run_test_c_agent(rid, data, test_days))
            except Exception as ex:
                eh.append(str(ex))

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        t.join(timeout=AGENT_TIMEOUT_S)

        if t.is_alive():
            print("TIMEOUT")
            results[agent_id] = "TIMEOUT"
        elif err_holder:
            print(f"ERROR: {err_holder[0]}")
            results[agent_id] = f"ERROR: {err_holder[0]}"
        else:
            stats = result_holder[0]
            results[agent_id] = stats
            print(f"return={stats['total_return_pct']:+.2f}%  "
                  f"sharpe={stats['sharpe']:.2f}  "
                  f"wr={stats['win_rate']:.1f}%  "
                  f"trades={stats['total_trades']}")
    return results


# ── Comparison table ──────────────────────────────────────────────────────────

def _row(label: str, val, best, fmt: str) -> str:
    s = fmt.format(val)
    star = " ◀" if val == best else "  "
    return f"  {label:<24} {s:>10}{star}"


def print_comparison_table(test_a: dict, test_b: dict[str, dict | str],
                            test_c: dict[str, dict | str], spy_ret: float,
                            test_days: list[date]) -> None:
    print("\n" + "=" * 78)
    print("BACKTEST COMPARISON — v5")
    print(f"Period: {test_days[0]} → {test_days[-1]}  ({len(test_days)} trading days)")
    print(f"SPY buy-and-hold: {spy_ret:+.2f}%")
    print("=" * 78)

    # ── Per-test aggregate rows ──────────────────────────────────────────────
    def avg_stat(results: dict, key: str) -> float:
        vals = [v[key] for v in results.values()
                if isinstance(v, dict) and key in v]
        return round(sum(vals) / len(vals), 2) if vals else 0.0

    rows = [
        ("Test",          "A BASELINE",       "B V2_REPLAY (avg)", "C CURRENT_S6 (avg)"),
        ("─" * 24,        "─" * 12,           "─" * 19,            "─" * 19),
        ("Return %",      f"{test_a['total_return_pct']:+.2f}%",
                          f"{avg_stat(test_b, 'total_return_pct'):+.2f}%",
                          f"{avg_stat(test_c, 'total_return_pct'):+.2f}%"),
        ("Sharpe",        f"{test_a['sharpe']:.2f}",
                          f"{avg_stat(test_b, 'sharpe'):.2f}",
                          f"{avg_stat(test_c, 'sharpe'):.2f}"),
        ("Win Rate",      f"{test_a['win_rate']:.1f}%",
                          f"{avg_stat(test_b, 'win_rate'):.1f}%",
                          f"{avg_stat(test_c, 'win_rate'):.1f}%"),
        ("Max Drawdown",  f"{test_a['max_drawdown_pct']:.1f}%",
                          f"{avg_stat(test_b, 'max_drawdown_pct'):.1f}%",
                          f"{avg_stat(test_c, 'max_drawdown_pct'):.1f}%"),
        ("Total Trades",  str(test_a['total_trades']),
                          f"{avg_stat(test_b, 'total_trades'):.0f} avg",
                          f"{avg_stat(test_c, 'total_trades'):.0f} avg"),
        ("Avg Trade Ret", f"{test_a['avg_trade_return']:.2f}%",
                          f"{avg_stat(test_b, 'avg_trade_return'):.2f}%",
                          f"{avg_stat(test_c, 'avg_trade_return'):.2f}%"),
        ("vs SPY (alpha)", f"{test_a['alpha_pct']:+.2f}%",
                           f"{avg_stat(test_b, 'alpha_pct'):+.2f}%",
                           f"{avg_stat(test_c, 'alpha_pct'):+.2f}%"),
    ]
    col_w = [24, 16, 21, 21]
    for row in rows:
        print("  " + "  ".join(f"{str(v):<{col_w[i]}}" for i, v in enumerate(row)))

    # ── Per-agent detail ─────────────────────────────────────────────────────
    print("\n" + "─" * 78)
    print("  PER-AGENT DETAIL")
    print("  " + f"{'Agent':<18} {'Test':<4} {'Return':>8} {'Sharpe':>7} {'WR':>7} {'MaxDD':>7} {'Trades':>7} {'Alpha':>8}")
    print("  " + "─" * 70)

    for agent_id in AGENTS:
        for test_label, results in [("B", test_b), ("C", test_c)]:
            v = results.get(agent_id)
            if isinstance(v, dict):
                print("  " + f"{agent_id:<18} {test_label:<4} "
                      f"{v['total_return_pct']:>+7.2f}% "
                      f"{v['sharpe']:>7.2f} "
                      f"{v['win_rate']:>6.1f}% "
                      f"{v['max_drawdown_pct']:>6.1f}% "
                      f"{v['total_trades']:>7} "
                      f"{v['alpha_pct']:>+7.2f}%")
            else:
                print("  " + f"{agent_id:<18} {test_label:<4} {'TIMEOUT/ERROR':>40}")

    # ── Monthly breakdown (Test A) ───────────────────────────────────────────
    if test_a.get("monthly"):
        print("\n" + "─" * 78)
        print("  MONTHLY BREAKDOWN — Test A Baseline")
        print("  " + f"{'Month':<10} {'PnL':>10} {'Trades':>8} {'WR':>8}")
        print("  " + "─" * 40)
        for m, v in sorted(test_a["monthly"].items()):
            wr = round(v["wins"] / max(v["trades"], 1) * 100, 1)
            print("  " + f"{m:<10} ${v['pnl']:>+9.2f} {v['trades']:>8} {wr:>7.1f}%")

    # ── Best / worst trades ──────────────────────────────────────────────────
    print("\n" + "─" * 78)
    print("  TEST A — BEST / WORST TRADES")
    if test_a.get("best_trade"):
        b, w = test_a["best_trade"], test_a["worst_trade"]
        print(f"  Best:  {b['symbol']} on {b['date']}  ${b['pnl']:+.2f}  ({b.get('pnl_pct', 0):+.1f}%)")
    if test_a.get("worst_trade") and test_a["worst_trade"]:
        w = test_a["worst_trade"]
        print(f"  Worst: {w['symbol']} on {w['date']}  ${w['pnl']:+.2f}  ({w.get('pnl_pct', 0):+.1f}%)")
    print("=" * 78)


# ── Save results ──────────────────────────────────────────────────────────────

def save_results(test_a: dict, test_b: dict, test_c: dict,
                 test_days: list[date], spy_ret: float) -> None:
    ts = datetime.now().isoformat()

    # Strip trades list from summary (goes to detail file)
    def strip_trades(d):
        if isinstance(d, dict):
            return {k: v for k, v in d.items() if k != "trades"}
        return d

    summary = {
        "generated_at":  ts,
        "period_start":  str(test_days[0]),
        "period_end":    str(test_days[-1]),
        "trading_days":  len(test_days),
        "spy_return_pct": spy_ret,
        "test_a_baseline":  strip_trades(test_a),
        "test_b_v2_replay": {k: strip_trades(v) for k, v in test_b.items()},
        "test_c_current_s6": {k: strip_trades(v) for k, v in test_c.items()},
    }

    detail = {
        "generated_at": ts,
        "period_start": str(test_days[0]),
        "period_end":   str(test_days[-1]),
        "test_a_baseline_trades":     test_a.get("trades", []),
        "test_b_v2_replay_trades":    {k: (v.get("trades", []) if isinstance(v, dict) else v)
                                       for k, v in test_b.items()},
        "test_c_current_s6_trades":   {k: (v.get("trades", []) if isinstance(v, dict) else v)
                                       for k, v in test_c.items()},
    }

    os.makedirs(OUT_DIR, exist_ok=True)
    with open(RESULTS_PATH, "w") as f:
        json.dump(summary, f, indent=2)
    with open(DETAIL_PATH, "w") as f:
        json.dump(detail, f, indent=2)

    print(f"\n  Results saved → {RESULTS_PATH}")
    print(f"  Detail saved  → {DETAIL_PATH}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Ollie Backtest Compare v5")
    parser.add_argument("--days", type=int, default=60,
                        help="Number of trading days to backtest (default: 60)")
    args = parser.parse_args()
    n_days = max(args.days, 5)

    print("=" * 78)
    print("OLLIE BACKTEST COMPARE v5")
    print(f"Tests: A=BASELINE  B=V2_REPLAY  C=CURRENT_S6")
    print(f"Days : {n_days}   Universe: {', '.join(UNIVERSE)}")
    print(f"Capital: ${STARTING_CAPITAL:,.0f}/agent   SL: {STOP_LOSS_PCT*100:.0f}%   "
          f"TP: {TAKE_PROFIT_PCT*100:.0f}%   Slip: {SLIPPAGE*100:.1f}%")
    print("=" * 78)

    test_start, test_end = last_n_trading_days(n_days)
    test_days = trading_days(test_start, test_end)
    print(f"\nPeriod: {test_start} → {test_end}  ({len(test_days)} trading days)")

    # Download once, share across all tests
    all_syms = list(set(UNIVERSE + [BENCHMARK_SYM]))
    data = download_data(all_syms, test_start, test_end)

    spy_ret = spy_benchmark(data, test_days)
    print(f"SPY benchmark return: {spy_ret:+.2f}%\n")

    # Run tests sequentially
    t0 = time.time()
    test_a = run_test_a(data, test_days)
    print(f"  Test A done in {time.time()-t0:.1f}s")

    t1 = time.time()
    test_b = run_test_b(data, test_days)
    print(f"  Test B done in {time.time()-t1:.1f}s")

    t2 = time.time()
    test_c = run_test_c(data, test_days)
    print(f"  Test C done in {time.time()-t2:.1f}s")

    print_comparison_table(test_a, test_b, test_c, spy_ret, test_days)
    save_results(test_a, test_b, test_c, test_days, spy_ret)

    print(f"\nTotal runtime: {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
