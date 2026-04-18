#!/usr/bin/env python3
"""
s6_sim_180d_backtest.py — Season 6 "New Rules" Simulation Backtest

Replays the same 180-day window with today's new rules active:

  Rule 1: Tractor Beam → chekov   (qual_gate 3→2, convergence vote fires)
  Rule 2: Tractor Beam → navigator (+2 score boost, qual_gate 3→2)
  Rule 3: qwen3 Ollie threshold 2.0→1.5  (min_conv 0.55→0.41)
  Rule 4: BULL Momentum Breakout active  (20d high + 1.5x vol + RSI 50-70 + ADX>25)
  Rule 5: Capitol-trades expanded scoring (chair 1.5x, multi 2x, options 1.3x …)

Baseline numbers loaded from data/s6_180d_backtest.json (previous run).
Simulation numbers computed fresh, saved to backtest_history with tag="s6_simulation_180d".

Output:
  data/s6_sim_180d_backtest.json
  data/trader.db  → backtest_history (9 rows, notes includes delta vs baseline)

Usage:
    venv/bin/python3 scripts/s6_sim_180d_backtest.py
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

# ── Paths & constants ─────────────────────────────────────────────────────────
ROOT             = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASELINE_PATH    = os.path.join(ROOT, "data", "s6_180d_backtest.json")
OUT_PATH         = os.path.join(ROOT, "data", "s6_sim_180d_backtest.json")
DB_PATH          = os.path.join(ROOT, "data", "trader.db")

DAYS             = 180
STARTING_CAPITAL = 100_000.0
SLIPPAGE         = 0.001
MAX_POSITIONS    = 5
BENCHMARK_SYM    = "SPY"
AGENT_TIMEOUT_S  = 180

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

# ── Simulation agent configs (NEW RULES) ─────────────────────────────────────
#
# Each agent lists the *changed* parameters vs the baseline.
# add_bmb=True  → BULL Momentum Breakout is an additional entry path.
# tractor_beam  → qual_gate lowered and convergence shortcut enabled.
# qwen3_boost   → min_conv lowered to reflect Ollie threshold 1.5.
# capitol_expanded → lower gate + multi-signal bonus for congress trades.

AGENTS_SIM = [
    {
        "id": "navigator", "name": "Navigator",
        # Rule 2: Tractor Beam +2 score boost → qual_gate 3→2, min_conv 0.50→0.45
        "rsi_buy": 38, "rsi_sell": 62,
        "min_conv": 0.45,   # was 0.50
        "max_pos_pct": 0.15, "stop_pct": 0.08, "tp_pct": 0.15,
        "qual_gate": 2,     # was 3 — tractor beam convergence vote
        "regime_block": ("CRISIS", "BEAR_STRONG"),
        "universe": UNIVERSE,
        "add_bmb": True,    # Rule 4: BULL momentum breakout active
        "tractor_beam": True,
        "rule_tags": ["tractor_beam_navigator", "bull_momentum_breakout"],
    },
    {
        "id": "chekov", "name": "Chekov",
        # Rule 1: Tractor Beam → chekov (2 strategies + tractor-beam = execute)
        "rsi_buy": 36, "rsi_sell": 64,
        "min_conv": 0.48,   # was 0.55
        "max_pos_pct": 0.15, "stop_pct": 0.08, "tp_pct": 0.15,
        "qual_gate": 2,     # was 3 — convergence vote with tractor beam
        "regime_block": ("CRISIS", "BEAR_STRONG", "BEAR"),
        "universe": UNIVERSE,
        "add_bmb": True,
        "tractor_beam": True,
        "rule_tags": ["tractor_beam_chekov", "bull_momentum_breakout"],
    },
    {
        "id": "ollama-llama", "name": "Llama",
        # No specific rule change; gains BMB entry path only
        "rsi_buy": 35, "rsi_sell": 65,
        "min_conv": 0.60, "max_pos_pct": 0.18, "stop_pct": 0.08, "tp_pct": 0.15,
        "qual_gate": 3,
        "regime_block": ("CRISIS", "BEAR_STRONG"),
        "universe": UNIVERSE,
        "add_bmb": True,
        "rule_tags": ["bull_momentum_breakout"],
    },
    {
        "id": "ollama-plutus", "name": "Plutus",
        # No specific rule change; gains BMB entry path only
        "rsi_buy": 28, "rsi_sell": 72,
        "min_conv": 0.65, "max_pos_pct": 0.12, "stop_pct": 0.07, "tp_pct": 0.18,
        "qual_gate": 4,
        "regime_block": ("CRISIS", "BEAR_STRONG", "BEAR"),
        "universe": UNIVERSE,
        "add_bmb": True,
        "rule_tags": ["bull_momentum_breakout"],
    },
    {
        "id": "ollama-qwen3", "name": "Qwen3",
        # Rule 3: Ollie threshold 2.0→1.5 = 25% reduction → min_conv 0.55→0.41
        "rsi_buy": 32, "rsi_sell": 65,
        "min_conv": 0.41,   # was 0.55 — Ollie threshold 1.5 unlocks more trades
        "max_pos_pct": 0.20, "stop_pct": 0.10, "tp_pct": 0.20,
        "qual_gate": 2,     # was 2, stays same (already aggressive) — but threshold lowered
        "regime_block": ("CRISIS",),
        "universe": UNIVERSE,
        "add_bmb": True,
        "rule_tags": ["qwen3_ollie_threshold_1.5", "bull_momentum_breakout"],
    },
    {
        "id": "ollama-coder", "name": "Coder",
        # No specific rule change; gains BMB entry path only
        "rsi_buy": 38, "rsi_sell": 62,
        "min_conv": 0.60, "max_pos_pct": 0.15, "stop_pct": 0.08, "tp_pct": 0.15,
        "qual_gate": 3,
        "regime_block": ("CRISIS", "BEAR_STRONG"),
        "universe": TECH_UNIVERSE,
        "add_bmb": True,
        "rule_tags": ["bull_momentum_breakout"],
    },
    {
        "id": "neo-matrix", "name": "Neo Matrix",
        # No specific rule change; gains BMB entry path only
        "rsi_buy": 42, "rsi_sell": 60,
        "min_conv": 0.60, "max_pos_pct": 0.18, "stop_pct": 0.09, "tp_pct": 0.12,
        "qual_gate": 3,
        "regime_block": ("CRISIS", "BEAR_STRONG"),
        "universe": UNIVERSE,
        "add_bmb": True,
        "rule_tags": ["bull_momentum_breakout"],
    },
    {
        "id": "capitol-trades", "name": "Capitol Trades",
        # Rule 5: Expanded scoring (freshness +12, chair 1.5x, multi 2x, options 1.3x, sector +8)
        # → min_conv 0.55→0.42, qual_gate 2→1, bonus position for high-score signals
        "rsi_buy": 35, "rsi_sell": 65,
        "min_conv": 0.42,   # was 0.55 — expanded scoring boosts more signals past threshold
        "max_pos_pct": 0.18, "stop_pct": 0.08, "tp_pct": 0.15,  # was 0.15
        "qual_gate": 1,     # was 2 — single strong congress signal sufficient with multipliers
        "regime_block": ("CRISIS",),
        "universe": CONGRESS_UNIVERSE,
        "add_bmb": True,
        "capitol_expanded": True,
        "rule_tags": ["capitol_expanded_scoring", "bull_momentum_breakout"],
    },
    {
        "id": "ollie-auto", "name": "Ollie (Gate)",
        # No specific rule change; gains BMB entry path only (Ollie gate is regime-aware)
        "rsi_buy": 40, "rsi_sell": 60,
        "min_conv": 0.70, "max_pos_pct": 0.10, "stop_pct": 0.06, "tp_pct": 0.12,
        "qual_gate": 4,
        "regime_block": ("CRISIS", "BEAR_STRONG", "BEAR"),
        "universe": UNIVERSE,
        "add_bmb": True,
        "rule_tags": ["bull_momentum_breakout"],
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
    rs   = gain / loss.replace(0, 1e-9)
    rsi  = 100 - 100 / (1 + rs)
    val  = rsi.dropna()
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


def calc_adx(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> float:
    """Wilder's ADX (simplified single-pass). Returns 0 if insufficient data."""
    n = period * 2 + 2
    if len(closes) < n:
        return 0.0
    h, l, c = highs[-n:], lows[-n:], closes[-n:]
    tr   = np.maximum(h[1:] - l[1:],
           np.maximum(np.abs(h[1:] - c[:-1]), np.abs(l[1:] - c[:-1])))
    pdm  = np.where((h[1:] - h[:-1]) > (l[:-1] - l[1:]),
                    np.maximum(h[1:] - h[:-1], 0.0), 0.0)
    mdm  = np.where((l[:-1] - l[1:]) > (h[1:] - h[:-1]),
                    np.maximum(l[:-1] - l[1:], 0.0), 0.0)
    # Wilder sum for last `period` bars
    atr  = float(np.mean(tr[-period:]))
    apdi = float(np.mean(pdm[-period:]))
    amdi = float(np.mean(mdm[-period:]))
    pdi  = 100.0 * apdi / max(atr, 1e-9)
    mdi  = 100.0 * amdi / max(atr, 1e-9)
    di_sum = pdi + mdi
    dx   = 100.0 * abs(pdi - mdi) / max(di_sum, 1e-9)
    return round(dx, 1)


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
    closes    = df_prior["Close"].values
    volumes   = df_prior["Volume"].values
    rsi_val   = calc_rsi(closes)
    sma_20    = calc_sma(closes, 20)
    sma_50    = calc_sma(closes, 50)
    vol_ratio = calc_vol_ratio(volumes)
    price     = float(closes[-1])
    mom_5d    = (closes[-1] - closes[-6]) / closes[-6] * 100 if len(closes) >= 6 else 0.0

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


def check_bull_momentum_breakout(df_prior: pd.DataFrame) -> bool:
    """
    Rule 4: BULL Momentum Breakout
    Entry: price > 20d high, vol > 1.5x avg, RSI 50-70, ADX > 25
    """
    if len(df_prior) < 22:
        return False
    closes  = df_prior["Close"].values
    highs   = df_prior["High"].values if "High" in df_prior.columns else closes
    lows    = df_prior["Low"].values  if "Low"  in df_prior.columns else closes
    volumes = df_prior["Volume"].values

    price   = float(closes[-1])
    high_20 = float(np.max(highs[-21:-1]))  # 20-bar high excluding today
    rsi     = calc_rsi(closes)
    vol_r   = calc_vol_ratio(volumes)
    adx     = calc_adx(highs, lows, closes)

    return (price > high_20 and vol_r > 1.5 and 50.0 <= rsi <= 70.0 and adx > 25.0)


# ── Portfolio ─────────────────────────────────────────────────────────────────

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
        return round(self.cash + sum(
            self.positions[s]["qty"] * prices.get(s, self.positions[s]["entry"])
            for s in self.positions
        ), 2)

    def open_count(self) -> int:
        return len(self.positions)

    def try_buy(self, sym: str, price: float, date_str: str,
                alloc_pct: float, conviction: float, signals: list[str],
                stop_override: float = None, tp_override: float = None) -> bool:
        if self.open_count() >= MAX_POSITIONS or sym in self.positions:
            return False
        buy_price  = round(price * (1 + SLIPPAGE), 4)
        alloc_cash = min(self.cash * alloc_pct, self.cash * 0.95)
        if alloc_cash < 100:
            return False
        qty  = alloc_cash / buy_price
        cost = qty * buy_price
        self.cash -= cost
        sl_pct = stop_override if stop_override is not None else self.stop_pct
        tp_pct = tp_override  if tp_override  is not None else self.tp_pct
        self.positions[sym] = {
            "qty": qty, "entry": buy_price,
            "sl":  round(buy_price * (1 - sl_pct), 4),
            "tp":  round(buy_price * (1 + tp_pct), 4),
            "date": date_str, "conviction": conviction, "signals": signals,
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
        exit_price, reason = None, None
        if day_open <= pos["sl"]:
            exit_price, reason = day_open, "STOP_GAP"
        elif day_low <= pos["sl"]:
            exit_price, reason = pos["sl"], "STOP_LOSS"
        elif day_high >= pos["tp"]:
            exit_price, reason = pos["tp"], "TAKE_PROFIT"
        if exit_price is not None:
            sell_price = round(exit_price * (1 - SLIPPAGE), 4)
            qty = pos["qty"]
            proceeds = qty * sell_price
            pnl = round(proceeds - qty * pos["entry"], 2)
            pnl_pct = round((sell_price - pos["entry"]) / pos["entry"] * 100, 2)
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
        pos = self.positions[sym]
        sell_price = round(price * (1 - SLIPPAGE), 4)
        qty = pos["qty"]
        proceeds = qty * sell_price
        pnl = round(proceeds - qty * pos["entry"], 2)
        pnl_pct = round((sell_price - pos["entry"]) / pos["entry"] * 100, 2)
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
            self.force_exit(sym, prices.get(sym, self.positions[sym]["entry"]), date_str, "EOD_LIQUIDATE")


# ── Data download ─────────────────────────────────────────────────────────────

def download_data(symbols: list[str], start: date, end: date) -> dict[str, pd.DataFrame]:
    warmup  = start - timedelta(days=100)
    all_sym = list(set(symbols + [BENCHMARK_SYM]))
    print(f"  Downloading {len(all_sym)} symbols: {warmup} → {end} ...")
    for attempt in range(3):
        try:
            raw = yf.download(
                all_sym,
                start=warmup.strftime("%Y-%m-%d"),
                end=(end + timedelta(days=1)).strftime("%Y-%m-%d"),
                auto_adjust=True, progress=False, group_by="ticker",
            )
            break
        except Exception as e:
            if attempt < 2:
                time.sleep(3)
            else:
                print(f"FATAL: {e}"); sys.exit(1)

    data: dict[str, pd.DataFrame] = {}
    for sym in all_sym:
        try:
            df = raw[sym].dropna(subset=["Close"]) if len(all_sym) > 1 else raw.dropna(subset=["Close"])
            data[sym] = df
        except Exception:
            try:
                df = yf.download(sym, start=warmup.strftime("%Y-%m-%d"),
                                 end=(end + timedelta(days=1)).strftime("%Y-%m-%d"),
                                 auto_adjust=True, progress=False)
                data[sym] = df.dropna(subset=["Close"])
            except Exception:
                data[sym] = pd.DataFrame()
        time.sleep(0.15)

    ok = sum(1 for d in data.values() if len(d) > 20)
    print(f"  OK: {ok}/{len(all_sym)} symbols")
    return data


def spy_benchmark(data: dict[str, pd.DataFrame], test_days: list[date]) -> float:
    df = data.get(BENCHMARK_SYM, pd.DataFrame())
    if len(df) == 0 or len(test_days) < 2:
        return 0.0
    p0 = float(df[df.index.date <= test_days[0]]["Close"].iloc[-1])
    p1 = float(df[df.index.date <= test_days[-1]]["Close"].iloc[-1])
    return round((p1 - p0) / p0 * 100, 2)


# ── Stats ─────────────────────────────────────────────────────────────────────

def compute_stats(port: Portfolio, data: dict[str, pd.DataFrame],
                  spy_return_pct: float) -> dict:
    sells = [t for t in port.trades if t["action"] == "SELL" and t["pnl"] is not None]
    wins  = [t for t in sells if t["pnl"] > 0]
    loses = [t for t in sells if t["pnl"] <= 0]
    n     = len(sells)

    last_prices = {
        sym: float(data[sym]["Close"].iloc[-1])
        for sym in port.positions if sym in data and len(data[sym]) > 0
    }
    final_value      = port.portfolio_value(last_prices)
    total_return_pct = round((final_value - port.capital) / port.capital * 100, 2)
    total_pnl        = round(final_value - port.capital, 2)
    win_rate         = round(len(wins) / max(n, 1) * 100, 1)

    dv = list(port.daily_values.values())
    if len(dv) > 1:
        rets    = np.array([dv[i]/dv[i-1]-1 for i in range(1, len(dv))])
        mean_r  = float(np.mean(rets))
        std_r   = float(np.std(rets))
        sharpe  = round(mean_r / std_r  * math.sqrt(252), 2) if std_r > 0 else 0.0
        down_r  = rets[rets < 0]
        down_s  = float(np.std(down_r)) if len(down_r) > 1 else (std_r or 1e-9)
        sortino = round(mean_r / down_s * math.sqrt(252), 2) if down_s > 0 else 0.0
    else:
        sharpe = sortino = 0.0

    peak, mdd = (dv[0] if dv else port.capital), 0.0
    for v in dv:
        peak = max(peak, v)
        dd = (peak - v) / peak * 100
        mdd = max(mdd, dd)
    mdd = round(mdd, 2)

    annualized = round(total_return_pct * 365.0 / DAYS, 2)
    calmar     = round(annualized / mdd, 2) if mdd > 0 else 0.0
    alpha      = round(total_return_pct - spy_return_pct, 2)

    gw = sum(t["pnl"] for t in wins)
    gl = abs(sum(t["pnl"] for t in loses))
    pf = round(gw / gl, 2) if gl > 0 else (float("inf") if gw > 0 else 0.0)
    aw = round(gw / len(wins),  2) if wins  else 0.0
    al = round(gl / len(loses), 2) if loses else 0.0

    bt = max(sells, key=lambda t: t["pnl"],  default=None)
    wt = min(sells, key=lambda t: t["pnl"],  default=None)

    return {
        "final_value": round(final_value, 2),
        "total_return_pct":  total_return_pct,
        "total_return_usd":  total_pnl,
        "annualized_return": annualized,
        "sharpe":   sharpe, "sortino": sortino, "calmar": calmar,
        "max_drawdown_pct": mdd,
        "win_rate":    win_rate,
        "total_trades": n,
        "winners":  len(wins),  "losers": len(loses),
        "avg_win_usd": aw, "avg_loss_usd": al,
        "profit_factor": pf,
        "spy_return_pct": spy_return_pct,
        "alpha_pct": alpha,
        "best_trade":  {"symbol": bt["symbol"], "pnl": bt["pnl"], "date": bt["date"]} if bt else None,
        "worst_trade": {"symbol": wt["symbol"], "pnl": wt["pnl"], "date": wt["date"]} if wt else None,
        "daily_values": port.daily_values,
        "trades": sells,
    }


# ── Agent simulation (new rules) ─────────────────────────────────────────────

def run_agent_sim(cfg: dict, data: dict[str, pd.DataFrame],
                  test_days: list[date], spy_return_pct: float) -> dict:
    universe      = cfg["universe"]
    min_conv      = cfg["min_conv"]
    rsi_buy       = cfg["rsi_buy"]
    rsi_sell      = cfg["rsi_sell"]
    max_pos       = cfg["max_pos_pct"]
    qual_gate     = cfg["qual_gate"]
    regime_block  = tuple(cfg["regime_block"])
    add_bmb       = cfg.get("add_bmb", False)
    capitol_exp   = cfg.get("capitol_expanded", False)
    tractor_beam  = cfg.get("tractor_beam", False)

    port   = Portfolio(capital=STARTING_CAPITAL,
                       stop_pct=cfg["stop_pct"], tp_pct=cfg["tp_pct"])
    spy_df = data.get(BENCHMARK_SYM, pd.DataFrame())
    bmb_trades = 0   # count BMB-specific entries

    for i, trade_date in enumerate(test_days):
        ds = trade_date.strftime("%Y-%m-%d")
        spy_sub = spy_df[spy_df.index.date <= trade_date]
        regime  = detect_regime(spy_sub["Close"].values) if len(spy_sub) >= 21 else "UNKNOWN"

        prices_today = {}
        for sym in universe:
            df = data.get(sym, pd.DataFrame())
            t  = df[df.index.date == trade_date]
            if len(t) > 0:
                prices_today[sym] = float(t["Close"].iloc[0])

        # Exit checks on held positions
        for sym in list(port.positions.keys()):
            df = data.get(sym, pd.DataFrame())
            t  = df[df.index.date == trade_date]
            if len(t) == 0:
                continue
            port.check_exits(sym,
                float(t["Open"].iloc[0]),  float(t["High"].iloc[0]),
                float(t["Low"].iloc[0]),   float(t["Close"].iloc[0]), ds)

        # RSI-based sell on held positions
        if regime not in regime_block:
            for sym in list(port.positions.keys()):
                df = data.get(sym, pd.DataFrame())
                prior = df[df.index.date < trade_date]
                t     = df[df.index.date == trade_date]
                if len(prior) < 20 or len(t) == 0:
                    continue
                if calc_rsi(prior["Close"].values) > rsi_sell:
                    port.force_exit(sym, float(t["Open"].iloc[0]), ds, "RSI_SELL")

        # New entries
        if regime not in regime_block:
            for sym in universe:
                if sym == BENCHMARK_SYM or sym in port.positions:
                    continue
                if port.open_count() >= MAX_POSITIONS:
                    break

                df    = data.get(sym, pd.DataFrame())
                prior = df[df.index.date < trade_date]
                t     = df[df.index.date == trade_date]
                if len(prior) < 55 or len(t) == 0:
                    continue

                closes    = prior["Close"].values
                volumes   = prior["Volume"].values
                rsi_val   = calc_rsi(closes)
                sma_20    = calc_sma(closes, 20)
                sma_50    = calc_sma(closes, 50)
                vol_ratio = calc_vol_ratio(volumes)
                price     = float(closes[-1])
                mom_5d    = (closes[-1] - closes[-6]) / closes[-6] * 100 if len(closes) >= 6 else 0.0
                day_open  = float(t["Open"].iloc[0])

                conviction, score, signals = score_and_signals(prior)

                # ── Path A: Standard RSI/quality-gate entry ────────────────
                if conviction >= min_conv:
                    qual_checks = [
                        rsi_val < rsi_buy,
                        vol_ratio >= 1.2,
                        mom_5d > 0,
                        price > sma_20,
                        price > sma_50,
                    ]
                    # Capitol expanded: bonus if 2+ consecutive prior sessions bullish
                    if capitol_exp and len(closes) >= 3:
                        consec_up = closes[-1] > closes[-2] > closes[-3]
                        if consec_up:
                            qual_checks.append(True)  # extra congress-intel signal

                    # Tractor beam: if RSI oversold + volume surge both fire → treat as convergence
                    if tractor_beam and rsi_val < rsi_buy and vol_ratio >= 1.5:
                        # Two strategies + tractor beam = execute regardless of qual_gate
                        port.try_buy(sym, day_open, ds, max_pos, conviction,
                                     signals + ["TRACTOR_BEAM"])
                        continue

                    if sum(qual_checks) >= qual_gate:
                        port.try_buy(sym, day_open, ds, max_pos, conviction, signals)

                # ── Path B: BULL Momentum Breakout ─────────────────────────
                # Rule 4: fires independently — regime must be BULL family only
                elif (add_bmb
                      and sym not in port.positions
                      and regime in ("BULL_STRONG", "BULL", "NEUTRAL")
                      and len(prior) >= 55):
                    if check_bull_momentum_breakout(prior):
                        # BMB uses tighter risk: 2% stop, 6% TP (3:1 R/R)
                        alloc = max_pos * 0.8 if conviction < 0.5 else max_pos
                        fired = port.try_buy(sym, day_open, ds, alloc, conviction,
                                             signals + ["BULL_MOMENTUM_BREAKOUT"],
                                             stop_override=0.02, tp_override=0.06)
                        if fired:
                            bmb_trades += 1

            # ── Second pass: BMB for agents that passed qual_gate check (add BMB on top) ──
            if add_bmb:
                for sym in universe:
                    if sym == BENCHMARK_SYM or sym in port.positions:
                        continue
                    if port.open_count() >= MAX_POSITIONS:
                        break
                    if regime not in ("BULL_STRONG", "BULL", "NEUTRAL"):
                        break

                    df    = data.get(sym, pd.DataFrame())
                    prior = df[df.index.date < trade_date]
                    t_row = df[df.index.date == trade_date]
                    if len(prior) < 55 or len(t_row) == 0:
                        continue

                    if check_bull_momentum_breakout(prior):
                        conviction, _, signals = score_and_signals(prior)
                        day_open = float(t_row["Open"].iloc[0])
                        fired = port.try_buy(sym, day_open, ds, max_pos * 0.8, conviction,
                                             signals + ["BULL_MOMENTUM_BREAKOUT"],
                                             stop_override=0.02, tp_override=0.06)
                        if fired:
                            bmb_trades += 1

        port.daily_values[ds] = port.portfolio_value(prices_today)

    # Final liquidation
    final_p = {sym: float(data[sym]["Close"].iloc[-1])
               for sym in universe if sym in data and len(data[sym]) > 0}
    if port.positions:
        port.liquidate_all(final_p, test_days[-1].strftime("%Y-%m-%d"))

    stats = compute_stats(port, data, spy_return_pct)
    stats["bmb_trades"] = bmb_trades
    return stats


# ── DB persistence ────────────────────────────────────────────────────────────

def save_to_db(agent_cfg: dict, stats: dict, baseline_stats: dict,
               start_dt: date, end_dt: date) -> None:
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""CREATE TABLE IF NOT EXISTS backtest_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id TEXT NOT NULL, player_name TEXT,
            run_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            period_days INTEGER DEFAULT 30, start_date TEXT, end_date TEXT,
            starting_value REAL DEFAULT 7000, final_value REAL,
            return_pct REAL, total_pnl REAL,
            win_count INTEGER DEFAULT 0, loss_count INTEGER DEFAULT 0,
            win_rate REAL DEFAULT 0, total_trades INTEGER DEFAULT 0,
            best_trade_pnl REAL, worst_trade_pnl REAL,
            best_trade_symbol TEXT, worst_trade_symbol TEXT,
            spy_return_pct REAL, rallies_top_return_pct REAL,
            rallies_top_name TEXT, notes TEXT, config_snapshot TEXT,
            guardrails_applied INTEGER DEFAULT 0,
            signals_tested INTEGER DEFAULT 0, signals_skipped INTEGER DEFAULT 0,
            skip_summary TEXT DEFAULT NULL
        )""")
        conn.commit()

        delta = {
            "return_delta":    round(stats["total_return_pct"] - baseline_stats.get("total_return_pct", 0), 2),
            "sharpe_delta":    round(stats["sharpe"]           - baseline_stats.get("sharpe", 0), 3),
            "sortino_delta":   round(stats["sortino"]          - baseline_stats.get("sortino", 0), 3),
            "win_rate_delta":  round(stats["win_rate"]         - baseline_stats.get("win_rate", 0), 1),
            "mdd_delta":       round(stats["max_drawdown_pct"] - baseline_stats.get("max_drawdown_pct", 0), 2),
            "trade_delta":     stats["total_trades"]           - baseline_stats.get("total_trades", 0),
            "bmb_trades":      stats.get("bmb_trades", 0),
            "baseline_return": baseline_stats.get("total_return_pct", 0),
            "baseline_sharpe": baseline_stats.get("sharpe", 0),
            "baseline_trades": baseline_stats.get("total_trades", 0),
        }
        notes = {
            "tag":             "s6_simulation_180d",
            "backtest_type":   "s6_simulation_new_rules",
            "run_version":     "s6_sim_180d_backtest_v1",
            "rules_applied":   agent_cfg.get("rule_tags", []),
            "sharpe":          stats["sharpe"],
            "sortino":         stats["sortino"],
            "calmar":          stats["calmar"],
            "alpha_pct":       stats["alpha_pct"],
            "annualized_return": stats["annualized_return"],
            "profit_factor":   stats["profit_factor"],
            "avg_win_usd":     stats["avg_win_usd"],
            "avg_loss_usd":    stats["avg_loss_usd"],
            "delta_vs_baseline": delta,
        }
        bt = stats.get("best_trade")  or {}
        wt = stats.get("worst_trade") or {}

        conn.execute("""INSERT INTO backtest_history
            (player_id, player_name, period_days, start_date, end_date,
             starting_value, final_value, return_pct, total_pnl,
             win_count, loss_count, win_rate, total_trades,
             best_trade_pnl, worst_trade_pnl, best_trade_symbol, worst_trade_symbol,
             spy_return_pct, notes, config_snapshot, guardrails_applied)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                agent_cfg["id"], agent_cfg["name"],
                DAYS, start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d"),
                STARTING_CAPITAL, stats["final_value"],
                stats["total_return_pct"], stats["total_return_usd"],
                stats["winners"], stats["losers"], stats["win_rate"], stats["total_trades"],
                bt.get("pnl"), wt.get("pnl"), bt.get("symbol"), wt.get("symbol"),
                stats["spy_return_pct"],
                json.dumps(notes),
                json.dumps({"add_bmb": agent_cfg.get("add_bmb"), "qual_gate": agent_cfg.get("qual_gate"),
                             "min_conv": agent_cfg.get("min_conv"), "rule_tags": agent_cfg.get("rule_tags", [])}),
                1,  # guardrails_applied = True (new rules active)
            ))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"  [DB ERROR] {agent_cfg['id']}: {e}")


# ── Print helpers ─────────────────────────────────────────────────────────────

def _sign(v: float) -> str:
    return f"+{v:.2f}" if v >= 0 else f"{v:.2f}"


def print_delta_table(sim_results: dict, baseline: dict, spy_return: float) -> None:
    print()
    print("=" * 110)
    print(f"  NEW RULES vs BASELINE — 180-Day Season 6 Simulation  |  SPY: +{spy_return:.2f}%")
    print("=" * 110)
    hdr = (f"  {'Agent':<18}  {'BasRet%':>8} {'SimRet%':>8} {'Δ Ret':>7}  "
           f"{'BasSharpe':>9} {'SimSharpe':>9} {'Δ Sharpe':>8}  "
           f"{'BasWR%':>7} {'SimWR%':>7} {'Δ WR':>6}  "
           f"{'BasMDD%':>7} {'SimMDD%':>7} {'Δ MDD':>6}  "
           f"{'ΔTrades':>8} {'BMB':>5}")
    print(hdr)
    print("-" * 110)

    for cfg in AGENTS_SIM:
        aid  = cfg["id"]
        sim  = sim_results.get(aid)
        base = baseline.get(aid, {})
        if not isinstance(sim, dict):
            print(f"  {aid:<18}  {'ERROR / TIMEOUT':>50}")
            continue

        b_ret    = base.get("total_return_pct", 0)
        s_ret    = sim["total_return_pct"]
        d_ret    = round(s_ret - b_ret, 2)
        b_sh     = base.get("sharpe", 0)
        s_sh     = sim["sharpe"]
        d_sh     = round(s_sh - b_sh, 3)
        b_wr     = base.get("win_rate", 0)
        s_wr     = sim["win_rate"]
        d_wr     = round(s_wr - b_wr, 1)
        b_mdd    = base.get("max_drawdown_pct", 0)
        s_mdd    = sim["max_drawdown_pct"]
        d_mdd    = round(s_mdd - b_mdd, 2)    # negative = improved (lower drawdown)
        d_trades = sim["total_trades"] - base.get("total_trades", 0)
        bmb      = sim.get("bmb_trades", 0)

        # Colour markers (text-based for terminal)
        ret_arrow = "▲" if d_ret > 0 else ("▼" if d_ret < 0 else "─")
        sh_arrow  = "▲" if d_sh  > 0 else ("▼" if d_sh  < 0 else "─")

        print(f"  {aid:<18}  "
              f"{b_ret:>+8.2f} {s_ret:>+8.2f} {ret_arrow}{_sign(d_ret):>6}  "
              f"{b_sh:>9.2f} {s_sh:>9.2f} {sh_arrow}{d_sh:>+7.3f}  "
              f"{b_wr:>7.1f} {s_wr:>7.1f} {d_wr:>+6.1f}  "
              f"{b_mdd:>7.2f} {s_mdd:>7.2f} {d_mdd:>+6.2f}  "
              f"{d_trades:>+8} {bmb:>5}")

    print("-" * 110)
    print()
    print("  Legend: Δ = Sim minus Baseline  |  BMB = trades fired via Bull Momentum Breakout")
    print("          Δ MDD: negative = BETTER (lower drawdown)  |  ▲ = improvement  ▼ = regression")
    print()


def print_rule_impact(sim_results: dict, baseline: dict) -> None:
    print("  RULE-BY-RULE IMPACT SUMMARY")
    print("  " + "─" * 70)

    rule_groups = {
        "Rule 1+2 — Tractor Beam (navigator, chekov)":
            ["navigator", "chekov"],
        "Rule 3  — qwen3 Ollie threshold 2.0→1.5":
            ["ollama-qwen3"],
        "Rule 4  — BULL Momentum Breakout (all agents)":
            [c["id"] for c in AGENTS_SIM],
        "Rule 5  — Capitol-Trades Expanded Scoring":
            ["capitol-trades"],
    }

    for rule_label, agents in rule_groups.items():
        print(f"\n  {rule_label}")
        total_ret_delta = 0.0
        total_trade_delta = 0
        total_bmb = 0
        for aid in agents:
            sim  = sim_results.get(aid)
            base = baseline.get(aid, {})
            if not isinstance(sim, dict):
                continue
            d_ret    = round(sim["total_return_pct"] - base.get("total_return_pct", 0), 2)
            d_trades = sim["total_trades"] - base.get("total_trades", 0)
            bmb      = sim.get("bmb_trades", 0)
            total_ret_delta    += d_ret
            total_trade_delta  += d_trades
            total_bmb          += bmb
            d_ret_usd = round(d_ret / 100 * STARTING_CAPITAL, 0)
            print(f"    {aid:<20}  ΔRet={d_ret:>+6.2f}%  ({d_ret_usd:>+7,.0f})  "
                  f"ΔTrades={d_trades:>+5}  BMB={bmb}")
        if len(agents) > 1:
            print(f"    {'TOTAL':20}  ΔRet={total_ret_delta:>+6.2f}%  ΔTrades={total_trade_delta:>+5}  BMB={total_bmb}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    # ── Load baseline ──────────────────────────────────────────────────────────
    if not os.path.exists(BASELINE_PATH):
        print(f"ERROR: Baseline not found at {BASELINE_PATH}")
        print("Run s6_180d_backtest.py first.")
        sys.exit(1)

    with open(BASELINE_PATH) as f:
        baseline_json = json.load(f)

    baseline_stats = {
        aid: data["stats"]
        for aid, data in baseline_json["agents"].items()
        if isinstance(data.get("stats"), dict)
    }
    spy_return = baseline_json["spy_return_pct"]

    # ── Date range (same as baseline) ─────────────────────────────────────────
    start_dt = datetime.strptime(baseline_json["start_date"], "%Y-%m-%d").date()
    end_dt   = datetime.strptime(baseline_json["end_date"],   "%Y-%m-%d").date()
    test_days = trading_days(start_dt, end_dt)

    print()
    print("=" * 80)
    print("  S6 SIMULATION BACKTEST — NEW RULES vs BASELINE")
    print(f"  Period: {start_dt} → {end_dt}  ({len(test_days)} trading days)")
    print(f"  Capital: ${STARTING_CAPITAL:,.0f}  |  SPY: {spy_return:+.2f}%")
    print("  Rules: Tractor Beam (nav/chekov), qwen3@1.5, Bull Momentum, Capitol+ Scoring")
    print("=" * 80)

    # ── Download data ──────────────────────────────────────────────────────────
    all_syms = list(set(sym for cfg in AGENTS_SIM for sym in cfg["universe"]) | {BENCHMARK_SYM})
    print()
    all_data = download_data(all_syms, start_dt, end_dt)

    # ── Run each agent under new rules ─────────────────────────────────────────
    print()
    sim_results: dict[str, dict] = {}

    for cfg in AGENTS_SIM:
        aid  = cfg["id"]
        name = cfg["name"]
        tags = ", ".join(cfg.get("rule_tags", []))
        print(f"  ▸ {name:<20} [{tags}]", end=" ... ", flush=True)

        result_holder: list = []
        err_holder:    list = []

        def _run(c=cfg, d=all_data, days=test_days, spy=spy_return, rh=result_holder, eh=err_holder):
            try:
                rh.append(run_agent_sim(c, d, days, spy))
            except Exception as ex:
                import traceback
                eh.append(str(ex) + "\n" + traceback.format_exc())

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        t.join(timeout=AGENT_TIMEOUT_S)

        if t.is_alive():
            print("TIMEOUT")
            sim_results[aid] = "TIMEOUT"
        elif err_holder:
            print(f"ERROR: {err_holder[0][:80]}")
            sim_results[aid] = f"ERROR: {err_holder[0][:80]}"
        else:
            s = result_holder[0]
            sim_results[aid] = s
            base = baseline_stats.get(aid, {})
            d_ret = round(s["total_return_pct"] - base.get("total_return_pct", 0), 2)
            print(f"sim={s['total_return_pct']:+.2f}% (base={base.get('total_return_pct',0):+.2f}%)  "
                  f"Δ={d_ret:+.2f}%  trades={s['total_trades']} (Δ{s['total_trades']-base.get('total_trades',0):+d})  "
                  f"bmb={s.get('bmb_trades',0)}")
            save_to_db(cfg, s, base, start_dt, end_dt)

    # ── Print comparison tables ────────────────────────────────────────────────
    print_delta_table(sim_results, baseline_stats, spy_return)
    print_rule_impact(sim_results, baseline_stats)

    # ── Individual agent detail ────────────────────────────────────────────────
    print()
    print()
    print("  INDIVIDUAL AGENT DETAIL (Sim vs Baseline)")
    print("  " + "─" * 70)
    for cfg in AGENTS_SIM:
        aid  = cfg["id"]
        sim  = sim_results.get(aid)
        base = baseline_stats.get(aid, {})
        if not isinstance(sim, dict):
            print(f"\n  ── {cfg['name']} ({aid}) — {sim}")
            continue
        bt = sim.get("best_trade")  or {}
        wt = sim.get("worst_trade") or {}
        print(f"\n  ── {cfg['name']} ({aid}) ──")
        print(f"     Return:         Baseline={base.get('total_return_pct',0):+.2f}%  "
              f"→ Sim={sim['total_return_pct']:+.2f}%  "
              f"Δ={sim['total_return_pct']-base.get('total_return_pct',0):+.2f}%  "
              f"(${sim['total_return_usd']:+,.0f})")
        print(f"     Sharpe:         {base.get('sharpe',0):.3f} → {sim['sharpe']:.3f}  "
              f"Δ={sim['sharpe']-base.get('sharpe',0):+.3f}")
        print(f"     Sortino:        {base.get('sortino',0):.3f} → {sim['sortino']:.3f}")
        print(f"     Calmar:         {base.get('calmar',0):.3f} → {sim['calmar']:.3f}")
        print(f"     Max Drawdown:   {base.get('max_drawdown_pct',0):.2f}% → {sim['max_drawdown_pct']:.2f}%  "
              f"Δ={sim['max_drawdown_pct']-base.get('max_drawdown_pct',0):+.2f}%")
        print(f"     Win Rate:       {base.get('win_rate',0):.1f}% → {sim['win_rate']:.1f}%  "
              f"Δ={sim['win_rate']-base.get('win_rate',0):+.1f}%")
        print(f"     Trades:         {base.get('total_trades',0)} → {sim['total_trades']}  "
              f"Δ={sim['total_trades']-base.get('total_trades',0):+d}  "
              f"(BMB: {sim.get('bmb_trades',0)})")
        print(f"     Profit Factor:  {sim['profit_factor']}  Alpha: {sim['alpha_pct']:+.2f}%")
        if bt:
            print(f"     Best Trade:     {bt['symbol']} +${bt['pnl']:,.2f} ({bt['date']})")
        if wt:
            print(f"     Worst Trade:    {wt['symbol']} -${abs(wt['pnl']):,.2f} ({wt['date']})")

    # ── Save JSON ──────────────────────────────────────────────────────────────
    output = {
        "run_date":        datetime.now().isoformat(),
        "simulation_type": "s6_simulation_180d",
        "period_days":     DAYS,
        "start_date":      start_dt.strftime("%Y-%m-%d"),
        "end_date":        end_dt.strftime("%Y-%m-%d"),
        "starting_capital": STARTING_CAPITAL,
        "spy_return_pct":  spy_return,
        "rules_active": [
            "tractor_beam_navigator_chekov",
            "qwen3_ollie_threshold_1.5",
            "bull_momentum_breakout_all",
            "capitol_trades_expanded_scoring",
        ],
        "agents": {
            cfg["id"]: {
                "config":   {k: v for k, v in cfg.items() if k not in ("universe",)},
                "sim_stats": {k: v for k, v in (sim_results.get(cfg["id"]) or {}).items()
                              if k not in ("trades", "daily_values")}
                              if isinstance(sim_results.get(cfg["id"]), dict) else sim_results.get(cfg["id"]),
                "baseline_stats": {k: baseline_stats.get(cfg["id"], {}).get(k)
                                   for k in ("total_return_pct","sharpe","sortino","calmar",
                                             "max_drawdown_pct","win_rate","total_trades")},
                "delta": {
                    "return_pct":      round((sim_results.get(cfg["id"]) or {}).get("total_return_pct", 0)
                                            - baseline_stats.get(cfg["id"], {}).get("total_return_pct", 0), 2)
                                       if isinstance(sim_results.get(cfg["id"]), dict) else None,
                    "bmb_trades":      (sim_results.get(cfg["id"]) or {}).get("bmb_trades", 0)
                                       if isinstance(sim_results.get(cfg["id"]), dict) else None,
                    "trade_delta":     ((sim_results.get(cfg["id"]) or {}).get("total_trades", 0)
                                       - baseline_stats.get(cfg["id"], {}).get("total_trades", 0))
                                       if isinstance(sim_results.get(cfg["id"]), dict) else None,
                },
            }
            for cfg in AGENTS_SIM
        },
    }
    with open(OUT_PATH, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print()
    print(f"\n  Saved → {OUT_PATH}")
    print(f"  Saved → {DB_PATH} (backtest_history, tag=s6_simulation_180d)")
    print()
    print("=" * 80)
    print()


if __name__ == "__main__":
    main()
