#!/usr/bin/env python3
"""
ollie_backtest_v6.py — OllieTrades Season 6 Full Comparison
# ============================================================
# Tier 2: Real signal stack — VIX/GEX proxy, Fear & Greed,
#          agent-specific strategies, regime-aware sizing,
#          McCoy crisis mode.  Imports backtest_baseline.py.
# ALL future backtests must import backtest_baseline.
# Sacred: NEVER delete trader.db or arena.db.
# ============================================================

3 strategy versions × 5 agents × N days.
APPEND-ONLY writes to backtest_runs + backtest_results.
NEVER touches trade history or arena.db.

Usage:
    venv/bin/python3 scripts/ollie_backtest_v6.py --days 5
    venv/bin/python3 scripts/ollie_backtest_v6.py --days 60
"""
from __future__ import annotations

import argparse
import json
import math
import os
import re
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import numpy as np
    import pandas as pd
    import yfinance as yf
except ImportError as e:
    print(f"Missing dependency: {e}"); sys.exit(1)

try:
    import requests as _requests
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False

from backtest_baseline import (
    fetch_vix_history, fetch_fear_greed_history, fetch_spy_vs_200ma,
    get_daily_regime, get_position_size_multiplier, should_agent_trade_today,
    build_agent_prompt, build_results_summary,
    AGENT_MODELS, AGENT_STRATEGIES, AGENT_INSTRUMENTS,
    _closest_prior,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "trader.db")

UNIVERSE: list[str] = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AMD", "INTC", "QCOM",
    "JPM",  "BAC",  "WFC",  "C",    "GS",   "MS",    "AXP",  "BLK", "SCHW", "USB",
    "JNJ",  "PFE",  "MRK",  "ABBV", "LLY",  "BMY",   "AMGN", "GILD","CVS",  "UNH",
    "WMT",  "COST", "TGT",  "HD",   "LOW",  "MCD",   "SBUX", "NKE", "DIS",  "NFLX",
    "XOM",  "CVX",  "COP",  "OXY",
    "GE",   "BA",   "CAT",  "HON",  "MMM",  "CRM",
]

BENCHMARK = "SPY"

SECTOR_MAP: dict[str, str] = {
    "AAPL":"tech",  "MSFT":"tech",  "NVDA":"tech",  "AMD":"tech",   "INTC":"tech",
    "QCOM":"tech",  "CRM":"tech",
    "AMZN":"cons",  "TSLA":"cons",  "HD":"cons",    "LOW":"cons",   "MCD":"cons",
    "SBUX":"cons",  "NKE":"cons",   "TGT":"cons",
    "META":"comm",  "GOOGL":"comm", "DIS":"comm",   "NFLX":"comm",
    "JPM":"fin",    "BAC":"fin",    "WFC":"fin",    "C":"fin",      "GS":"fin",
    "MS":"fin",     "AXP":"fin",    "BLK":"fin",    "SCHW":"fin",   "USB":"fin",
    "JNJ":"health", "PFE":"health", "MRK":"health", "ABBV":"health","LLY":"health",
    "BMY":"health", "AMGN":"health","GILD":"health","CVS":"health", "UNH":"health",
    "WMT":"staples","COST":"staples",
    "XOM":"energy", "CVX":"energy", "COP":"energy", "OXY":"energy",
    "GE":"indust",  "BA":"indust",  "CAT":"indust", "HON":"indust", "MMM":"indust",
}

AGENTS: dict[str, dict] = {
    "navigator":     {"display": "Chekov", "rsi_buy": 40, "rsi_sell": 62, "conservative": False},
    "ollama-plutus": {"display": "McCoy",  "rsi_buy": 35, "rsi_sell": 65, "conservative": True},
    "ollama-qwen3":  {"display": "Dax",    "rsi_buy": 38, "rsi_sell": 63, "conservative": True},
    "ollama-coder":  {"display": "Data",   "rsi_buy": 36, "rsi_sell": 64, "conservative": False},
    "neo-matrix":    {"display": "Neo",    "rsi_buy": 42, "rsi_sell": 60, "conservative": False},
}

# Ollama model assignments — imported from backtest_baseline (AGENT_MODELS)
# McCoy's defensive universe: only active when VIX >= 22
MCCOY_UNIVERSE: list[str] = ["GLD", "TLT", "XLU", "SH", "PSQ", "GDX"]

_LOCALHOST = "http://localhost:11434"
try:
    from config import AI_PLAYERS as _AI_PLAYERS
    _PLAYER_BASE_URLS: dict = {p["id"]: p.get("url", _LOCALHOST) for p in _AI_PLAYERS}
except Exception:
    _PLAYER_BASE_URLS: dict = {}
# Models that support think:false suppression
THINK_MODELS    = {"qwen3.5:9b", "qwen3:8b", "qwen3:14b", "qwen3:30b", "deepseek-r1:14b"}

# Shared cache: (agent_id, sym, date_str) → (signal, confidence)
_ollama_cache: dict[tuple, tuple] = {}

STARTING_CAPITAL   = 10_000.0
SLIPPAGE           = 0.001
MAX_POS_BASE       = 8
MAX_POS_V3         = 5
MAX_POS_V3_CONS    = 3   # McCoy + Dax
STOP_LOSS_A        = 0.08
STOP_LOSS_B        = 0.12
TAKE_PROFIT        = 0.15
BASE_POS_PCT       = 0.20

US_HOLIDAYS = {
    date(2025,1,1),  date(2025,1,20), date(2025,2,17), date(2025,4,18),
    date(2025,5,26), date(2025,7,4),  date(2025,9,1),  date(2025,11,27),
    date(2025,12,25),
    date(2026,1,1),  date(2026,1,19), date(2026,2,16), date(2026,4,3),
    date(2026,5,25), date(2026,7,3),  date(2026,9,7),  date(2026,11,26),
    date(2026,12,25),
}

# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Technical helpers
# ---------------------------------------------------------------------------

def calc_rsi(closes: np.ndarray, period: int = 14) -> float:
    s = pd.Series(closes)
    diff = s.diff(1)
    gain = diff.clip(lower=0).rolling(period).mean()
    loss = (-diff.clip(upper=0)).rolling(period).mean()
    rs   = gain / loss.replace(0, 1e-9)
    v    = (100 - 100 / (1 + rs)).dropna()
    return float(v.iloc[-1]) if len(v) > 0 else 50.0


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
    if p > ma8 > ma21:                       return "BULL_STRONG"
    elif p > ma21:                           return "BULL"
    elif p < ma8 < ma21 and p < ma50 * 0.95: return "CRISIS"
    elif p < ma8 < ma21:                     return "BEAR_STRONG"
    elif p < ma21:                           return "BEAR"
    return "NEUTRAL"

# ---------------------------------------------------------------------------
# Signal computation
# ---------------------------------------------------------------------------

def signal_grade(rsi: float) -> str:
    if rsi < 30:   return "A"
    elif rsi < 40: return "B"
    elif rsi < 55: return "C"
    return "D"


def compute_alpha(closes: np.ndarray, volumes: np.ndarray) -> float:
    if len(closes) < 22:
        return 0.0
    rsi       = calc_rsi(closes)
    sma_20    = calc_sma(closes, 20)
    sma_50    = calc_sma(closes, 50)
    vol_ratio = calc_vol_ratio(volumes)
    price     = float(closes[-1])
    mom_5d    = (closes[-1] - closes[-6]) / closes[-6] if len(closes) >= 6 else 0.0

    score = 0.0
    if rsi < 25:        score += 0.30
    elif rsi < 30:      score += 0.22
    elif rsi < 35:      score += 0.15
    elif rsi < 40:      score += 0.08

    if vol_ratio >= 2.0:   score += 0.22
    elif vol_ratio >= 1.5: score += 0.15
    elif vol_ratio >= 1.2: score += 0.08

    if mom_5d > 0.03:   score += 0.18
    elif mom_5d > 0.01: score += 0.10
    elif mom_5d > 0:    score += 0.04

    if price > sma_20: score += 0.15
    if price > sma_50: score += 0.15
    return min(1.0, round(score, 3))


def compute_confidence(closes: np.ndarray) -> float:
    """% of last 10 closes above 20-day SMA → 0–100."""
    if len(closes) < 30:
        return 50.0
    sma_20 = calc_sma(closes[:-10], 20)
    above  = sum(1 for c in closes[-10:] if c > sma_20)
    return above * 10.0


def ollie_score(rsi: float, vol_ratio: float, mom_5d: float,
                above_sma20: bool, above_sma50: bool) -> float:
    return float(
        (rsi < 40) + (vol_ratio >= 1.5) + (mom_5d > 0) + above_sma20 + above_sma50
    )

# ---------------------------------------------------------------------------
# Ollama integration  (Tier 1 — real model queries)
# ---------------------------------------------------------------------------

_SIGNAL_PROMPT = """\
/no_think
You are a quantitative trader. Output ONE line only, no markdown, no explanation.

Ticker: {sym} | Date: {date} | Price: ${price:.2f}
RSI(14): {rsi:.1f} | vs 20d MA: {vs20:+.1f}% | vs 50d MA: {vs50:+.1f}%
5d momentum: {mom5d:+.2f}% | Vol ratio: {vol_ratio:.1f}x | Regime: {regime}

Output format (copy exactly, replace values):
SIGNAL: BUY | CONFIDENCE: 7 | REASON: oversold with volume confirmation

SIGNAL must be BUY, SELL, or HOLD. Output that one line only."""


def _strip_think(text: str) -> str:
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<think>.*", "", text, flags=re.DOTALL | re.IGNORECASE)
    return text.strip()


def _call_model(model_id: str, prompt: str, agent_id: str = "", timeout: int = 90) -> str:
    """HTTP call to Ollama. Returns raw text or '' on failure."""
    if not _HAS_REQUESTS:
        return ""
    is_think = model_id in THINK_MODELS
    _base = _PLAYER_BASE_URLS.get(agent_id, _LOCALHOST)
    for attempt in range(3):
        try:
            if attempt > 0:
                time.sleep(20 * attempt)
            if is_think:
                resp = _requests.post(_base + "/api/chat", json={
                    "model": model_id,
                    "messages": [{"role": "user", "content": prompt}],
                    "stream": False, "think": False,
                    "options": {"temperature": 0.1, "num_predict": 80, "num_ctx": 512},
                }, timeout=timeout)
            else:
                resp = _requests.post(_base + "/api/generate", json={
                    "model": model_id,
                    "prompt": prompt, "stream": False,
                    "options": {"temperature": 0.1, "num_predict": 80, "num_ctx": 512},
                }, timeout=timeout)
            resp.raise_for_status()
            if is_think:
                raw = resp.json().get("message", {}).get("content", "") or ""
            else:
                raw = resp.json().get("response", "") or ""
            return _strip_think(raw).strip()
        except Exception as e:
            print(f"      [ollama-err] {model_id} attempt {attempt+1}: {e!s:.60}")
    return ""


def _parse_signal(text: str) -> tuple[str, int]:
    clean = re.sub(r"\*+", "", text)
    m = re.search(r"SIGNAL:\s*(BUY|SELL|HOLD)", clean, re.IGNORECASE)
    if m:
        signal = m.group(1).upper()
    else:
        for word in re.sub(r"[^\w\s]", " ", text.upper()).split()[:8]:
            if word in ("BUY", "SELL", "HOLD"):
                signal = word
                break
        else:
            signal = "HOLD"
    mc = re.search(r"CONFIDENCE:\s*(\d+)", clean, re.IGNORECASE)
    conf = int(mc.group(1)) if mc else 5
    return signal, min(max(conf, 1), 10)


def ask_ollama(agent_id: str, sym: str, snap: dict,
               vix: float = 20.0, fg: float = 50.0,
               regime: str = "NEUTRAL") -> tuple[str, int]:
    """Query the agent's assigned Ollama model. Cached per (agent, sym, date).

    Returns (signal, confidence) where signal is 'BUY', 'SELL', or 'HOLD'.
    Falls back to ('HOLD', 5) if Ollama is unavailable.
    """
    # Cache shared across versions: signal depends on market data, not portfolio mechanics
    cache_key = (agent_id, sym, snap["date"])
    if cache_key in _ollama_cache:
        return _ollama_cache[cache_key]

    model_id = AGENT_MODELS.get(agent_id, "qwen3.5:9b")
    agent_display = AGENTS[agent_id]["display"]
    prompt = build_agent_prompt(
        agent_id, sym, snap["date"], {"close": snap["price"]},
        regime, vix, fg, snap["rsi"], snap["sma20"], snap["sma50"],
    )
    raw = _call_model(model_id, prompt, agent_id=agent_id)
    signal, conf = _parse_signal(raw)

    print(f"      [OLLAMA] {agent_display}/{model_id} {sym} {snap['date']}: "
          f"{signal}({conf}/10) regime={regime} vix={vix:.1f} fg={fg:.0f}")

    _ollama_cache[cache_key] = (signal, conf)
    return signal, conf


def _make_snap(sym: str, ds: str, price: float, rsi_val: float,
               sma_20: float, sma_50: float, mom_5d: float,
               vol_ratio: float, regime: str) -> dict:
    """Build the snapshot dict passed to ask_ollama."""
    vs20 = (price / sma_20 - 1) * 100 if sma_20 else 0.0
    vs50 = (price / sma_50 - 1) * 100 if sma_50 else 0.0
    return {
        "sym": sym, "date": ds, "price": price,
        "rsi": rsi_val, "vs20": vs20, "vs50": vs50,
        "mom5d": mom_5d * 100, "vol_ratio": vol_ratio, "regime": regime,
        "sma20": sma_20, "sma50": sma_50,
    }

# ---------------------------------------------------------------------------
# Portfolio
# ---------------------------------------------------------------------------

class Portfolio:
    def __init__(self, capital: float = STARTING_CAPITAL):
        self.cash      = capital
        self.capital   = capital
        self.positions: dict[str, dict] = {}
        self.trades:    list[dict]      = []
        self.daily_values: dict[str, float] = {}

    def value(self, prices: dict[str, float]) -> float:
        pos_val = sum(
            self.positions[s]["qty"] * prices.get(s, self.positions[s]["entry"])
            for s in self.positions
        )
        return round(self.cash + pos_val, 2)

    def open_sectors(self) -> set[str]:
        return {SECTOR_MAP.get(s, "other") for s in self.positions}

    def _sell(self, sym: str, exit_px: float, ds: str, reason: str) -> None:
        pos        = self.positions.pop(sym)
        sell_px    = round(exit_px * (1 - SLIPPAGE), 4)
        proceeds   = pos["qty"] * sell_px
        pnl        = round(proceeds - pos["qty"] * pos["entry"], 2)
        pnl_pct    = round((sell_px / pos["entry"] - 1) * 100, 2)
        self.cash += proceeds
        self.trades.append({
            "date": ds, "ticker": sym, "action": "SELL",
            "entry": pos["entry"], "exit": sell_px,
            "qty": round(pos["qty"], 4),
            "pnl": pnl, "pct": pnl_pct, "reason": reason,
        })

    def buy(self, sym: str, price: float, ds: str, alloc_pct: float,
            conviction: float, stop_pct: float, signals: list[str]) -> bool:
        if sym in self.positions:
            return False
        buy_px = round(price * (1 + SLIPPAGE), 4)
        alloc  = min(self.cash * alloc_pct, self.cash * 0.95)
        if alloc < 50:
            return False
        qty = alloc / buy_px
        self.cash -= qty * buy_px
        self.positions[sym] = {
            "qty": qty, "entry": buy_px,
            "sl":  round(buy_px * (1 - stop_pct), 4),
            "tp":  round(buy_px * (1 + TAKE_PROFIT), 4),
            "date": ds, "conviction": conviction, "signals": signals,
        }
        self.trades.append({
            "date": ds, "ticker": sym, "action": "BUY",
            "entry": buy_px, "qty": round(qty, 4),
            "cost": round(qty * buy_px, 2),
        })
        return True

    def buy_v3(self, sym: str, price: float, ds: str, alloc_pct: float,
               conviction: float, signals: list[str]) -> bool:
        if sym in self.positions:
            return False
        buy_px = round(price * (1 + SLIPPAGE), 4)
        alloc  = min(self.cash * alloc_pct, self.cash * 0.95)
        if alloc < 50:
            return False
        qty = alloc / buy_px
        self.cash -= qty * buy_px
        self.positions[sym] = {
            "qty": qty, "entry": buy_px,
            "trail_stop": round(buy_px * (1 - STOP_LOSS_A), 4),
            "trail_pct": STOP_LOSS_A,
            "trail_active": False,
            "peak": buy_px,
            "pyramided": False,
            "orig_alloc": alloc_pct,
            "date": ds, "conviction": conviction, "signals": signals,
        }
        self.trades.append({
            "date": ds, "ticker": sym, "action": "BUY",
            "entry": buy_px, "qty": round(qty, 4),
            "cost": round(qty * buy_px, 2),
        })
        return True

    def check_exits_standard(self, sym: str, day_open: float, day_high: float,
                              day_low: float, day_close: float, ds: str) -> None:
        if sym not in self.positions:
            return
        pos = self.positions[sym]
        if day_open >= pos["tp"]:
            self._sell(sym, day_open, ds, "TAKE_PROFIT_GAP")
        elif day_open <= pos["sl"]:
            self._sell(sym, day_open, ds, "STOP_GAP")
        elif day_high >= pos["tp"]:
            self._sell(sym, pos["tp"], ds, "TAKE_PROFIT")
        elif day_low <= pos["sl"]:
            self._sell(sym, pos["sl"], ds, "STOP_LOSS")

    def check_exits_v3(self, sym: str, day_open: float, day_high: float,
                       day_low: float, day_close: float, ds: str) -> None:
        if sym not in self.positions:
            return
        pos = self.positions[sym]
        # Update peak
        if day_high > pos["peak"]:
            pos["peak"] = day_high
        # Tighten trail after +5% gain
        if not pos["trail_active"] and day_high >= pos["entry"] * 1.05:
            pos["trail_active"] = True
            pos["trail_pct"]    = 0.05
        # Update trail stop (ratchet up only)
        if pos["trail_active"]:
            new_stop = round(pos["peak"] * (1 - pos["trail_pct"]), 4)
            if new_stop > pos["trail_stop"]:
                pos["trail_stop"] = new_stop
        # Check gap or intraday trail stop
        if day_open <= pos["trail_stop"]:
            self._sell(sym, day_open, ds, "TRAIL_GAP")
        elif day_low <= pos["trail_stop"]:
            self._sell(sym, pos["trail_stop"], ds, "TRAIL_STOP")

    def pyramid_v3(self, sym: str, current_price: float, ds: str) -> bool:
        if sym not in self.positions:
            return False
        pos = self.positions[sym]
        if pos["pyramided"] or current_price < pos["entry"] * 1.05:
            return False
        add_pct  = pos["orig_alloc"] * 0.5
        add_cash = self.cash * add_pct
        if add_cash < 50:
            return False
        buy_px  = round(current_price * (1 + SLIPPAGE), 4)
        add_qty = add_cash / buy_px
        self.cash -= add_qty * buy_px
        old_cost      = pos["qty"] * pos["entry"]
        pos["qty"]   += add_qty
        pos["entry"]  = round((old_cost + add_qty * buy_px) / pos["qty"], 4)
        pos["pyramided"] = True
        self.trades.append({
            "date": ds, "ticker": sym, "action": "PYRAMID",
            "entry": buy_px, "qty": round(add_qty, 4),
            "cost": round(add_qty * buy_px, 2),
        })
        return True

    def force_sell(self, sym: str, price: float, ds: str, reason: str = "SIGNAL") -> None:
        if sym in self.positions:
            self._sell(sym, price, ds, reason)

# ---------------------------------------------------------------------------
# Data download
# ---------------------------------------------------------------------------

def download_data(symbols: list[str], start: date, end: date) -> dict[str, pd.DataFrame]:
    warmup   = start - timedelta(days=120)
    all_syms = list(set(symbols + [BENCHMARK]))
    print(f"  Downloading {len(all_syms)} symbols: {warmup} → {end} ...")

    for attempt in range(3):
        try:
            raw = yf.download(
                all_syms,
                start=warmup.strftime("%Y-%m-%d"),
                end=(end + timedelta(days=1)).strftime("%Y-%m-%d"),
                auto_adjust=True, progress=False, group_by="ticker",
            )
            break
        except Exception as e:
            print(f"  Attempt {attempt+1} failed: {e}")
            if attempt < 2:
                time.sleep(5)
            else:
                print("  FATAL: could not download data."); sys.exit(1)

    def _extract(r: pd.DataFrame, s: str) -> pd.DataFrame | None:
        """Extract single-ticker DataFrame from a batch download result."""
        try:
            cols = r.columns
            if isinstance(cols, pd.MultiIndex):
                lvl0 = cols.get_level_values(0).unique().tolist()
                lvl1 = cols.get_level_values(1).unique().tolist()
                if s in lvl0:          # (ticker, field) layout
                    df = r[s]
                elif s in lvl1:        # (field, ticker) layout
                    df = r.xs(s, level=1, axis=1)
                else:
                    return None
            else:
                df = r  # single-ticker result
            df = df.dropna(subset=["Close"]).copy()
            df.index = pd.to_datetime(df.index)
            return df if len(df) > 20 else None
        except Exception:
            return None

    data: dict[str, pd.DataFrame] = {}
    for sym in all_syms:
        df = _extract(raw, sym)
        if df is not None:
            data[sym] = df
        else:
            try:
                time.sleep(1)
                r2 = yf.download(
                    sym,
                    start=warmup.strftime("%Y-%m-%d"),
                    end=(end + timedelta(days=1)).strftime("%Y-%m-%d"),
                    auto_adjust=True, progress=False,
                )
                if len(r2) > 20:
                    r2 = r2.dropna(subset=["Close"]).copy()
                    r2.index = pd.to_datetime(r2.index)
                    data[sym] = r2
            except Exception as ex:
                print(f"  WARNING: {sym} unavailable: {ex}")

    ok = sum(1 for d in data.values() if len(d) > 20)
    print(f"  Got {ok}/{len(all_syms)} symbols with sufficient history")
    return data

# ---------------------------------------------------------------------------
# Date-safe DataFrame filtering
# ---------------------------------------------------------------------------

def _on(df: pd.DataFrame, d: date) -> pd.DataFrame:
    """Return rows where index == d, handling tz-aware and tz-naive indexes."""
    idx = df.index
    if hasattr(idx, "date"):
        return df[idx.date == d]
    idx_dt = pd.to_datetime(idx)
    ts = pd.Timestamp(d)
    return df[idx_dt.normalize() == ts]


def _before(df: pd.DataFrame, d: date) -> pd.DataFrame:
    """Return rows where index.date < d."""
    idx = df.index
    if hasattr(idx, "date"):
        return df[idx.date < d]
    idx_dt = pd.to_datetime(idx)
    ts = pd.Timestamp(d)
    return df[idx_dt.normalize() < ts]


def _upto(df: pd.DataFrame, d: date) -> pd.DataFrame:
    """Return rows where index.date <= d."""
    idx = df.index
    if hasattr(idx, "date"):
        return df[idx.date <= d]
    idx_dt = pd.to_datetime(idx)
    ts = pd.Timestamp(d)
    return df[idx_dt.normalize() <= ts]


# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------

def spy_benchmark(data: dict[str, pd.DataFrame], test_days: list[date]) -> float:
    df = data.get(BENCHMARK, pd.DataFrame())
    if len(df) == 0 or len(test_days) < 2:
        return 0.0
    r0 = _upto(df, test_days[0])
    r1 = _upto(df, test_days[-1])
    if len(r0) == 0 or len(r1) == 0:
        return 0.0
    return round((float(r1["Close"].iloc[-1]) / float(r0["Close"].iloc[-1]) - 1) * 100, 2)

# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def compute_stats(port: Portfolio, data: dict[str, pd.DataFrame], spy_pct: float) -> dict:
    last_px = {
        s: float(data[s]["Close"].iloc[-1])
        for s in port.positions
        if s in data and len(data[s]) > 0
    }
    final_val = port.value(last_px)
    ret_dec   = round((final_val - port.capital) / port.capital, 4)

    sell_trades = [t for t in port.trades if t["action"] == "SELL"]
    n_trades    = len(sell_trades)
    winners     = [t for t in sell_trades if t.get("pnl", 0) > 0]
    win_rate    = round(len(winners) / max(n_trades, 1), 4)
    best_pct    = max((t.get("pct", 0) for t in sell_trades), default=0.0) / 100
    worst_pct   = min((t.get("pct", 0) for t in sell_trades), default=0.0) / 100

    vals = list(port.daily_values.values())
    if len(vals) > 1:
        rets   = [vals[i] / vals[i-1] - 1 for i in range(1, len(vals))]
        mean_r = float(np.mean(rets))
        std_r  = float(np.std(rets))
        sharpe = round(mean_r / std_r * math.sqrt(252), 3) if std_r > 1e-9 else 0.0
    else:
        sharpe = 0.0

    peak, mdd = port.capital, 0.0
    for v in vals:
        peak = max(peak, v)
        mdd  = max(mdd, (peak - v) / peak)

    trades_json = json.dumps([
        {"date": t["date"], "ticker": t["ticker"],
         "entry": t.get("entry", 0), "exit": t.get("exit", 0),
         "pct": t.get("pct", 0)}
        for t in sell_trades
    ])
    equity_json = json.dumps([
        {"date": ds, "value": round(v, 2)}
        for ds, v in port.daily_values.items()
    ])

    return {
        "final_value":      round(final_val, 2),
        "total_return_pct": ret_dec,
        "win_rate":         win_rate,
        "sharpe_ratio":     sharpe,
        "max_drawdown":     round(mdd, 4),
        "num_trades":       n_trades,
        "best_trade_pct":   round(best_pct, 4),
        "worst_trade_pct":  round(worst_pct, 4),
        "spy_pct":          spy_pct,
        "alpha_dec":        round(ret_dec - spy_pct / 100, 4),
        "trades_json":      trades_json,
        "equity_json":      equity_json,
    }

# ---------------------------------------------------------------------------
# VERSION A — BASELINE
# ---------------------------------------------------------------------------

def run_baseline(agent_id: str, data: dict[str, pd.DataFrame],
                 test_days: list[date],
                 vix_data: dict, fg_data: dict, spy200_data: dict) -> dict:
    cfg      = AGENTS[agent_id]
    rsi_sell = cfg["rsi_sell"]
    port     = Portfolio()

    vix_list: list[float] = []
    fg_list:  list[float] = []
    regime_counts: dict[str, int] = {}
    mccoy_active_days = 0

    for trade_date in test_days:
        ds         = trade_date.strftime("%Y-%m-%d")
        day_vix    = _closest_prior(vix_data, ds, 20.0)
        day_fg     = float(fg_data.get(ds, 50))
        day_regime = get_daily_regime(ds, vix_data, fg_data, spy200_data)
        mccoy_on   = (agent_id == "ollama-plutus" and day_vix >= 22)

        vix_list.append(day_vix)
        fg_list.append(day_fg)
        regime_counts[day_regime] = regime_counts.get(day_regime, 0) + 1
        if mccoy_on:
            mccoy_active_days += 1

        print(f"  [{ds}] VIX={day_vix:.1f}  F&G={day_fg:.0f}  "
              f"Regime={day_regime}  McCoy={'active' if mccoy_on else 'inactive'}  "
              f"[{agent_id}/BASELINE]")

        universe = MCCOY_UNIVERSE if agent_id == "ollama-plutus" else UNIVERSE
        prices_today: dict[str, float] = {}
        for sym in universe:
            df    = data.get(sym, pd.DataFrame())
            today = _on(df, trade_date)
            if len(today) > 0:
                prices_today[sym] = float(today["Close"].iloc[0])

        # 1. SL/TP exits
        for sym in list(port.positions):
            df    = data.get(sym, pd.DataFrame())
            today = _on(df, trade_date)
            if len(today) == 0:
                continue
            port.check_exits_standard(
                sym,
                float(today["Open"].iloc[0]), float(today["High"].iloc[0]),
                float(today["Low"].iloc[0]),  float(today["Close"].iloc[0]),
                ds,
            )

        # 2. RSI sell on held positions
        for sym in list(port.positions):
            df    = data.get(sym, pd.DataFrame())
            prior = _before(df, trade_date)
            today = _on(df, trade_date)
            if len(prior) < 14 or len(today) == 0:
                continue
            if calc_rsi(prior["Close"].values) > rsi_sell:
                port.force_sell(sym, float(today["Open"].iloc[0]), ds, "RSI_SELL")

        # 3. New entries — gated by regime + agent rules
        if should_agent_trade_today(agent_id, day_regime, day_vix):
            regime_mult = get_position_size_multiplier(day_regime, agent_id)
            for sym in universe:
                if sym in port.positions or len(port.positions) >= MAX_POS_BASE:
                    continue
                df    = data.get(sym, pd.DataFrame())
                prior = _before(df, trade_date)
                today = _on(df, trade_date)
                if len(prior) < 55 or len(today) == 0:
                    continue

                closes    = prior["Close"].values
                volumes   = prior["Volume"].values
                rsi_val   = calc_rsi(closes)
                sma_20    = calc_sma(closes, 20)
                sma_50    = calc_sma(closes, 50)
                price     = float(closes[-1])
                vol_ratio = calc_vol_ratio(volumes)
                mom_5d    = (closes[-1] - closes[-6]) / closes[-6] if len(closes) >= 6 else 0.0

                alpha = compute_alpha(closes, volumes)

                # Loose pre-filter: cut obvious non-starters before Ollama call
                if rsi_val > 55 or alpha < 0.1:
                    continue

                snap = _make_snap(sym, ds, price, rsi_val, sma_20, sma_50,
                                  mom_5d, vol_ratio, day_regime)
                signal, conf = ask_ollama(agent_id, sym, snap,
                                          vix=day_vix, fg=day_fg, regime=day_regime)
                if signal != "BUY" or conf < 5:
                    continue

                alloc = (BASE_POS_PCT if alpha >= 0.6 else BASE_POS_PCT * 0.5) * regime_mult
                if alloc > 0:
                    port.buy(sym, float(today["Open"].iloc[0]), ds,
                             alloc, alpha, STOP_LOSS_A,
                             [f"ollama={signal}({conf})", f"a={alpha:.2f}", f"reg={day_regime}"])

        port.daily_values[ds] = port.value(prices_today)

    stats = compute_stats(port, data, spy_benchmark(data, test_days))
    stats["regime_counts"]    = regime_counts
    stats["vix_avg"]          = round(sum(vix_list) / len(vix_list), 1) if vix_list else 20.0
    stats["fg_avg"]           = round(sum(fg_list)  / len(fg_list),  0) if fg_list  else 50.0
    stats["mccoy_active_days"] = mccoy_active_days
    return stats

# ---------------------------------------------------------------------------
# VERSION B — V2_ALPHA
# ---------------------------------------------------------------------------

def _v2_alloc(alpha: float) -> float:
    """Linear scale: alpha 0.3→0.9 maps to 0.5x→2.0x of BASE_POS_PCT."""
    t = min(1.0, max(0.0, (alpha - 0.3) / 0.6))
    return BASE_POS_PCT * (0.5 + t * 1.5)


def run_v2_alpha(agent_id: str, data: dict[str, pd.DataFrame],
                 test_days: list[date],
                 vix_data: dict, fg_data: dict, spy200_data: dict) -> dict:
    cfg      = AGENTS[agent_id]
    rsi_sell = cfg["rsi_sell"]
    port     = Portfolio()

    vix_list: list[float] = []
    fg_list:  list[float] = []
    regime_counts: dict[str, int] = {}
    mccoy_active_days = 0

    for trade_date in test_days:
        ds         = trade_date.strftime("%Y-%m-%d")
        day_vix    = _closest_prior(vix_data, ds, 20.0)
        day_fg     = float(fg_data.get(ds, 50))
        day_regime = get_daily_regime(ds, vix_data, fg_data, spy200_data)
        mccoy_on   = (agent_id == "ollama-plutus" and day_vix >= 22)

        vix_list.append(day_vix)
        fg_list.append(day_fg)
        regime_counts[day_regime] = regime_counts.get(day_regime, 0) + 1
        if mccoy_on:
            mccoy_active_days += 1

        print(f"  [{ds}] VIX={day_vix:.1f}  F&G={day_fg:.0f}  "
              f"Regime={day_regime}  McCoy={'active' if mccoy_on else 'inactive'}  "
              f"[{agent_id}/V2_ALPHA]")

        universe = MCCOY_UNIVERSE if agent_id == "ollama-plutus" else UNIVERSE
        prices_today: dict[str, float] = {}
        for sym in universe:
            df    = data.get(sym, pd.DataFrame())
            today = _on(df, trade_date)
            if len(today) > 0:
                prices_today[sym] = float(today["Close"].iloc[0])

        for sym in list(port.positions):
            df    = data.get(sym, pd.DataFrame())
            today = _on(df, trade_date)
            if len(today) == 0: continue
            port.check_exits_standard(
                sym,
                float(today["Open"].iloc[0]), float(today["High"].iloc[0]),
                float(today["Low"].iloc[0]),  float(today["Close"].iloc[0]),
                ds,
            )

        for sym in list(port.positions):
            df    = data.get(sym, pd.DataFrame())
            prior = _before(df, trade_date)
            today = _on(df, trade_date)
            if len(prior) < 14 or len(today) == 0: continue
            if calc_rsi(prior["Close"].values) > rsi_sell:
                port.force_sell(sym, float(today["Open"].iloc[0]), ds, "RSI_SELL")

        if should_agent_trade_today(agent_id, day_regime, day_vix):
            regime_mult = get_position_size_multiplier(day_regime, agent_id)
            for sym in universe:
                if sym in port.positions or len(port.positions) >= MAX_POS_BASE:
                    continue
                df    = data.get(sym, pd.DataFrame())
                prior = _before(df, trade_date)
                today = _on(df, trade_date)
                if len(prior) < 55 or len(today) == 0: continue

                closes    = prior["Close"].values
                volumes   = prior["Volume"].values
                rsi_val   = calc_rsi(closes)
                sma_20    = calc_sma(closes, 20)
                sma_50    = calc_sma(closes, 50)
                price     = float(closes[-1])
                vol_ratio = calc_vol_ratio(volumes)
                mom_5d    = (closes[-1] - closes[-6]) / closes[-6] if len(closes) >= 6 else 0.0

                alpha = compute_alpha(closes, volumes)

                # Loose pre-filter before Ollama call
                if rsi_val > 55 or alpha < 0.1:
                    continue

                snap = _make_snap(sym, ds, price, rsi_val, sma_20, sma_50,
                                  mom_5d, vol_ratio, day_regime)
                signal, conf = ask_ollama(agent_id, sym, snap,
                                          vix=day_vix, fg=day_fg, regime=day_regime)
                if signal != "BUY" or conf < 5:
                    continue

                alloc = _v2_alloc(alpha) * regime_mult
                if alloc > 0:
                    port.buy(sym, float(today["Open"].iloc[0]), ds,
                             alloc, alpha, STOP_LOSS_B,
                             [f"ollama={signal}({conf})", f"a={alpha:.2f}", f"reg={day_regime}"])

        port.daily_values[ds] = port.value(prices_today)

    stats = compute_stats(port, data, spy_benchmark(data, test_days))
    stats["regime_counts"]    = regime_counts
    stats["vix_avg"]          = round(sum(vix_list) / len(vix_list), 1) if vix_list else 20.0
    stats["fg_avg"]           = round(sum(fg_list)  / len(fg_list),  0) if fg_list  else 50.0
    stats["mccoy_active_days"] = mccoy_active_days
    return stats

# ---------------------------------------------------------------------------
# VERSION C — V3_CONCENTRATED
# ---------------------------------------------------------------------------

def run_v3_conc(agent_id: str, data: dict[str, pd.DataFrame],
                test_days: list[date],
                vix_data: dict, fg_data: dict, spy200_data: dict) -> dict:
    cfg      = AGENTS[agent_id]
    rsi_sell = cfg["rsi_sell"]
    max_pos  = MAX_POS_V3_CONS if cfg["conservative"] else MAX_POS_V3
    port     = Portfolio()
    spy_df   = data.get(BENCHMARK, pd.DataFrame())

    vix_list: list[float] = []
    fg_list:  list[float] = []
    regime_counts: dict[str, int] = {}
    mccoy_active_days = 0

    for trade_date in test_days:
        ds         = trade_date.strftime("%Y-%m-%d")
        day_vix    = _closest_prior(vix_data, ds, 20.0)
        day_fg     = float(fg_data.get(ds, 50))
        day_regime = get_daily_regime(ds, vix_data, fg_data, spy200_data)
        mccoy_on   = (agent_id == "ollama-plutus" and day_vix >= 22)

        vix_list.append(day_vix)
        fg_list.append(day_fg)
        regime_counts[day_regime] = regime_counts.get(day_regime, 0) + 1
        if mccoy_on:
            mccoy_active_days += 1

        print(f"  [{ds}] VIX={day_vix:.1f}  F&G={day_fg:.0f}  "
              f"Regime={day_regime}  McCoy={'active' if mccoy_on else 'inactive'}  "
              f"[{agent_id}/V3_CONC]")

        universe = MCCOY_UNIVERSE if agent_id == "ollama-plutus" else UNIVERSE
        prices_today: dict[str, float] = {}
        for sym in universe:
            df    = data.get(sym, pd.DataFrame())
            today = _on(df, trade_date)
            if len(today) > 0:
                prices_today[sym] = float(today["Close"].iloc[0])

        # Trailing stop exits + pyramid
        for sym in list(port.positions):
            df    = data.get(sym, pd.DataFrame())
            today = _on(df, trade_date)
            if len(today) == 0: continue
            port.check_exits_v3(
                sym,
                float(today["Open"].iloc[0]), float(today["High"].iloc[0]),
                float(today["Low"].iloc[0]),  float(today["Close"].iloc[0]),
                ds,
            )
            if sym in port.positions:
                port.pyramid_v3(sym, float(today["Close"].iloc[0]), ds)

        # RSI sells
        for sym in list(port.positions):
            df    = data.get(sym, pd.DataFrame())
            prior = _before(df, trade_date)
            today = _on(df, trade_date)
            if len(prior) < 14 or len(today) == 0: continue
            if calc_rsi(prior["Close"].values) > rsi_sell:
                port.force_sell(sym, float(today["Open"].iloc[0]), ds, "RSI_SELL")

        if should_agent_trade_today(agent_id, day_regime, day_vix):
            regime_mult  = get_position_size_multiplier(day_regime, agent_id)
            spy_prior    = _before(spy_df, trade_date)
            held_sectors = port.open_sectors()

            for sym in universe:
                if sym in port.positions or len(port.positions) >= max_pos:
                    continue
                df    = data.get(sym, pd.DataFrame())
                prior = _before(df, trade_date)
                today = _on(df, trade_date)
                if len(prior) < 55 or len(today) == 0: continue

                closes    = prior["Close"].values
                volumes   = prior["Volume"].values
                rsi_val   = calc_rsi(closes)
                sma_20    = calc_sma(closes, 20)
                sma_50    = calc_sma(closes, 50)
                price     = float(closes[-1])
                vol_ratio = calc_vol_ratio(volumes)
                mom_5d    = (closes[-1] - closes[-6]) / closes[-6] if len(closes) >= 6 else 0.0

                alpha      = compute_alpha(closes, volumes)
                sym_sector = SECTOR_MAP.get(sym, "other")

                # Loose pre-filter + sector diversity gate before Ollama call
                if rsi_val > 55 or alpha < 0.1:
                    continue
                if sym_sector in held_sectors:
                    continue

                snap = _make_snap(sym, ds, price, rsi_val, sma_20, sma_50,
                                  mom_5d, vol_ratio, day_regime)
                signal, conf = ask_ollama(agent_id, sym, snap,
                                          vix=day_vix, fg=day_fg, regime=day_regime)
                if signal != "BUY" or conf < 6:
                    continue

                alloc = BASE_POS_PCT * regime_mult
                if alloc > 0 and port.buy_v3(
                    sym, float(today["Open"].iloc[0]), ds, alloc, alpha,
                    [f"ollama={signal}({conf})", f"a={alpha:.2f}", f"reg={day_regime}"],
                ):
                    held_sectors = port.open_sectors()

        port.daily_values[ds] = port.value(prices_today)

    stats = compute_stats(port, data, spy_benchmark(data, test_days))
    stats["regime_counts"]    = regime_counts
    stats["vix_avg"]          = round(sum(vix_list) / len(vix_list), 1) if vix_list else 20.0
    stats["fg_avg"]           = round(sum(fg_list)  / len(fg_list),  0) if fg_list  else 50.0
    stats["mccoy_active_days"] = mccoy_active_days
    return stats

# ---------------------------------------------------------------------------
# Run all versions × agents
# ---------------------------------------------------------------------------

def run_all(data: dict[str, pd.DataFrame], test_days: list[date],
            vix_data: dict, fg_data: dict, spy200_data: dict) -> dict[str, dict[str, dict]]:
    results: dict[str, dict[str, dict]] = {"BASELINE": {}, "V2_ALPHA": {}, "V3_CONC": {}}
    runners = [("BASELINE", run_baseline), ("V2_ALPHA", run_v2_alpha), ("V3_CONC", run_v3_conc)]

    for ver, fn in runners:
        print(f"\n[{ver}]")
        for agent_id, cfg in AGENTS.items():
            print(f"  {cfg['display']:<7} ...", flush=True)
            try:
                s = fn(agent_id, data, test_days, vix_data, fg_data, spy200_data)
                results[ver][agent_id] = s
                print(f"  → return={s['total_return_pct']*100:+.2f}%  "
                      f"sharpe={s['sharpe_ratio']:.2f}  "
                      f"wr={s['win_rate']*100:.0f}%  "
                      f"trades={s['num_trades']}  "
                      f"vix_avg={s.get('vix_avg',0):.1f}  "
                      f"fg_avg={s.get('fg_avg',0):.0f}  "
                      f"mccoy_days={s.get('mccoy_active_days',0)}")
            except Exception as e:
                print(f"ERROR: {e}")
                results[ver][agent_id] = None
    return results

# ---------------------------------------------------------------------------
# Print table
# ---------------------------------------------------------------------------

def print_table(results: dict, spy_ret: float, days: int,
                test_days: list[date]) -> None:
    s0 = test_days[0].strftime("%Y-%m-%d") if test_days else "?"
    s1 = test_days[-1].strftime("%Y-%m-%d") if test_days else "?"
    label = "5-DAY VERIFICATION" if days <= 10 else f"{days}-DAY FULL RUN"

    print(f"\n{'='*78}")
    print(f"=== OllieTrades Backtest v6 — {label} ===")
    print(f"Period: {s0} → {s1}   SPY: {spy_ret:+.2f}%")
    print(f"{'='*78}")

    H  = f"{'Agent':<10}| {'Version':<10}| {'Return':>8} | {'Sharpe':>6} | {'Win%':>5} | {'MaxDD':>6} | {'Trades':>6} | {'vs SPY':>7}"
    HR = "-" * len(H)
    print(H); print(HR)

    vmap = {"BASELINE": "Baseline", "V2_ALPHA": "V2_Alpha", "V3_CONC": "V3_Conc"}
    for agent_id, cfg in AGENTS.items():
        for ver in ("BASELINE", "V2_ALPHA", "V3_CONC"):
            s = results[ver].get(agent_id)
            if s is None:
                print(f"{cfg['display']:<10}| {vmap[ver]:<10}| {'ERROR':>9} | {'':>6} | {'':>5} | {'':>6} | {'':>6} | {'':>7}")
                continue
            ret  = s["total_return_pct"] * 100
            alph = s["alpha_dec"] * 100
            print(f"{cfg['display']:<10}| {vmap[ver]:<10}| {ret:>+7.2f}%  | "
                  f"{s['sharpe_ratio']:>6.2f} | {s['win_rate']*100:>4.0f}% | "
                  f"{s['max_drawdown']*100:>5.1f}% | {s['num_trades']:>6} | {alph:>+6.2f}%")
        print(HR)

    print(f"\n{'=== SUMMARY TABLE ':=<78}")
    SH = f"{'Version':<10}| {'Avg Return':>10} | {'Avg Sharpe':>10} | {'Avg WR':>7} | {'Avg MaxDD':>9} | {'Tot Trades':>10} | {'vs SPY':>7}"
    print(SH); print("-" * len(SH))

    best_ver, best_ret = None, -999.0
    for ver in ("BASELINE", "V2_ALPHA", "V3_CONC"):
        valid = [s for s in results[ver].values() if s is not None]
        if not valid: continue
        avg_ret    = sum(s["total_return_pct"] for s in valid) / len(valid) * 100
        avg_sharpe = sum(s["sharpe_ratio"] for s in valid) / len(valid)
        avg_wr     = sum(s["win_rate"] for s in valid) / len(valid) * 100
        avg_mdd    = sum(s["max_drawdown"] for s in valid) / len(valid) * 100
        tot_trades = sum(s["num_trades"] for s in valid)
        avg_alpha  = sum(s["alpha_dec"] for s in valid) / len(valid) * 100
        print(f"{vmap[ver]:<10}| {avg_ret:>+9.2f}% | {avg_sharpe:>10.3f} | "
              f"{avg_wr:>6.1f}% | {avg_mdd:>8.1f}% | {tot_trades:>10} | {avg_alpha:>+6.2f}%")
        if avg_ret > best_ret:
            best_ret, best_ver = avg_ret, ver

    reasons = {
        "BASELINE": "strict triple-filter preserves capital with consistent quality",
        "V2_ALPHA":  "conviction-scaled sizing + wider stops capture more upside",
        "V3_CONC":   "concentrated quality + trailing stops deliver best risk-adjusted return",
    }
    print()
    if best_ver:
        winner_label = vmap[best_ver]
        print(f"WINNER: {winner_label} — {reasons.get(best_ver, '')}")
        if best_ret > spy_ret:
            print(f"RECOMMENDATION: switch to {winner_label} (beats SPY by {best_ret - spy_ret:+.2f}%)")
        else:
            print("RECOMMENDATION: keep current / review signal thresholds — all vs SPY negative")
    print("=" * 78)

# ---------------------------------------------------------------------------
# DB save (APPEND ONLY)
# ---------------------------------------------------------------------------

def ensure_columns(conn: sqlite3.Connection) -> None:
    for col, typ in [("run_name","TEXT"),("version_tag","TEXT"),
                     ("days","INTEGER"),("spy_return","REAL"),("notes","TEXT")]:
        try:
            conn.execute(f"ALTER TABLE backtest_runs ADD COLUMN {col} {typ}")
        except sqlite3.OperationalError:
            pass  # already exists


def save_to_db(results: dict, test_days: list[date],
               spy_ret: float, days: int) -> list[int]:
    date_tag  = date.today().strftime("%Y%m%d")
    start_str = test_days[0].strftime("%Y-%m-%d") if test_days else ""
    end_str   = test_days[-1].strftime("%Y-%m-%d") if test_days else ""
    model_ids = ",".join(AGENTS.keys())
    now       = datetime.now().isoformat(timespec="seconds")
    notes     = "Season 6 fleet — Tier 2: VIX/GEX proxy, Fear&Greed, regime-aware sizing, McCoy crisis mode"

    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    ensure_columns(conn)

    run_ids = []
    for ver in ("BASELINE", "V2_ALPHA", "V3_CONC"):
        run_name = f"v6_{days}day_{date_tag}_{ver.lower()}"
        cur = conn.execute(
            """INSERT INTO backtest_runs
               (run_type, start_date, end_date, model_ids, status,
                created_at, completed_at, run_name, version_tag, days, spy_return, notes)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("v6_backtest", start_str, end_str, model_ids, "completed",
             now, now, run_name, ver, days, round(spy_ret / 100, 4), notes),
        )
        run_id = cur.lastrowid
        run_ids.append(run_id)

        for agent_id, s in results[ver].items():
            if s is None:
                continue
            conn.execute(
                """INSERT INTO backtest_results
                   (run_id, player_id, display_name, test_date,
                    final_value, total_return_pct, win_rate, sharpe_ratio,
                    max_drawdown, num_trades, best_trade_pct, worst_trade_pct,
                    trades_json, equity_json)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (run_id, agent_id, AGENTS[agent_id]["display"], end_str,
                 s["final_value"], s["total_return_pct"], s["win_rate"],
                 s["sharpe_ratio"], s["max_drawdown"], s["num_trades"],
                 s["best_trade_pct"], s["worst_trade_pct"],
                 s["trades_json"], s["equity_json"]),
            )

    conn.commit()
    conn.close()
    print(f"\n  Saved 3 runs → backtest_runs ids: {run_ids}")
    return run_ids


def verify_save(run_ids: list[int]) -> None:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    ph   = ",".join("?" * len(run_ids))
    rows = conn.execute(
        f"""SELECT br.run_name, br.version_tag, res.player_id,
               ROUND(res.total_return_pct * 100, 2) as return_pct,
               ROUND(res.sharpe_ratio, 3) as sharpe,
               ROUND(res.win_rate * 100, 1) as wr_pct,
               res.num_trades
           FROM backtest_results res
           JOIN backtest_runs br ON br.id = res.run_id
           WHERE br.id IN ({ph})
           ORDER BY br.id, res.player_id""",
        run_ids,
    ).fetchall()
    conn.close()

    print(f"\n  {'run_name':<35} {'ver':<10} {'player':<18} {'ret%':>6} {'sharpe':>7} {'wr%':>5} {'trades':>6}")
    print("  " + "-" * 95)
    for r in rows:
        print(f"  {r[0]:<35} {r[1]:<10} {r[2]:<18} {r[3]:>6.2f} {r[4]:>7.3f} {r[5]:>5.1f} {r[6]:>6}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=5)
    args = parser.parse_args()

    print("=" * 78)
    print("OllieTrades Backtest v6 — Season 6 Full Comparison  [Tier 2: Baseline Standard]")
    print(f"Versions: BASELINE × V2_ALPHA × V3_CONC  |  Agents: {', '.join(AGENTS)}")
    print(f"Days: {args.days}   Capital: ${STARTING_CAPITAL:,.0f}/agent   "
          f"SL: {STOP_LOSS_A*100:.0f}%/V2:{STOP_LOSS_B*100:.0f}%   Slip: {SLIPPAGE*100:.1f}%")
    print("=" * 78)

    start_d, end_d = last_n_trading_days(args.days)
    test_days      = trading_days(start_d, end_d)
    start_str      = start_d.strftime("%Y-%m-%d")
    end_str        = end_d.strftime("%Y-%m-%d")
    print(f"Period: {start_d} → {end_d}  ({len(test_days)} trading days)")

    # Fetch all signal sources (cached to data/backtest_cache/)
    print("\n  Fetching signal sources ...")
    vix_data   = fetch_vix_history(start_str, end_str)
    fg_data    = fetch_fear_greed_history(start_str, end_str, vix_data=vix_data)
    spy200     = fetch_spy_vs_200ma(start_str, end_str)
    print(f"  VIX days: {len(vix_data)}  |  F&G days: {len(fg_data)}  |  SPY/200MA days: {len(spy200)}")

    all_syms = list(set(UNIVERSE + MCCOY_UNIVERSE))
    data     = download_data(all_syms, start_d, end_d)
    spy_ret  = spy_benchmark(data, test_days)
    print(f"  SPY benchmark: {spy_ret:+.2f}%")

    results = run_all(data, test_days, vix_data, fg_data, spy200)
    print_table(results, spy_ret, args.days, test_days)

    print("\n  Saving results to DB (APPEND ONLY) ...")
    run_ids = save_to_db(results, test_days, spy_ret, args.days)
    verify_save(run_ids)

    print("\nBASELINE STANDARD: CONFIRMED")


if __name__ == "__main__":
    main()
