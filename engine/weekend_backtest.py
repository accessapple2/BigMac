"""
Weekend Bake-Off Backtester — USS TradeMinds
=============================================
Replays N trading days using historical OHLCV data, running 5 fleet agents
against each day's reconstructed market context.  Compares two Ollama models
across the identical data set.

Usage (quick 3-day test):
    from engine.weekend_backtest import run_backtest
    run_backtest(days=3, model="qwen3.5:9b")

Full bake-off:
    run_backtest(days=30, model="qwen3.5:9b")
    run_backtest(days=30, model="0xroyce/plutus")
"""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from typing import Any, Optional

import requests

logger = logging.getLogger("weekend_backtest")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [bakeoff] %(levelname)s: %(message)s",
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BACKTEST_DB   = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "backtest.db"))
_LOCK_FILE    = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "backtest.lock"))
OLLAMA_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434").rstrip("/")

STARTING_CASH = 10_000.0
PROFIT_TARGET = 0.05   # +5% (default; overridden per-agent below)
STOP_LOSS     = -0.03  # -3% (default)
MAX_HOLD_DAYS = 5
POSITION_PCT  = 0.20   # 20% of cash per position

# Per-agent exit thresholds (item 10: wider stops to avoid shake-outs)
_AGENT_STOPS: dict[str, dict] = {
    "neo-matrix":       {"stop": -0.05, "target": 0.08, "max_days": 5},
    "grok-4":           {"stop": -0.04, "target": 0.05, "max_days": 5},
    "ollama-qwen3":     {"stop": -0.04, "target": 0.06, "max_days": 7},
    "ollama-plutus":    {"stop": -0.03, "target": 0.04, "max_days": 5},
    "ollama-glm4":      {"stop": -0.03, "target": 0.05, "max_days": 5},
    "data-tng":         {"stop": -0.04, "target": 0.06, "max_days": 6},
    # Silent Four
    "dayblade-sulu":    {"stop": -0.03, "target": 0.05, "max_days": 3},   # gap fades fast
    "gemini-2.5-flash": {"stop": -0.04, "target": 0.06, "max_days": 5},
    "ollama-llama":     {"stop": -0.04, "target": 0.07, "max_days": 4},   # catalyst plays
    "gemini-2.5-pro":   {"stop": -0.04, "target": 0.06, "max_days": 5},
}

# Scaled exit tiers for backtest: (profit_threshold, fraction_to_sell, label)
_BT_SCALED_TIERS: dict[str, list[tuple[float, float, str]]] = {
    "neo-matrix":    [(0.08, 0.15, "T3"), (0.05, 0.25, "T2"), (0.03, 0.50, "T1")],
    "grok-4":        [(0.05, 0.25, "T2"), (0.03, 0.50, "T1")],
    "ollama-qwen3":  [(0.06, 0.25, "T2"), (0.04, 0.50, "T1")],
    "ollama-plutus": [(0.04, 0.50, "T1")],
    "ollama-glm4":   [(0.05, 0.25, "T2"), (0.03, 0.50, "T1")],
    "data-tng":      [(0.06, 0.25, "T2"), (0.03, 0.50, "T1")],
}

# 50-symbol universe: watchlist + liquid large-caps
BAKEOFF_UNIVERSE = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "BRK-B", "JPM", "V",
    "UNH", "XOM", "LLY", "JNJ", "MA", "AVGO", "HD", "CVX", "MRK", "ABBV",
    "PEP", "KO", "WMT", "BAC", "PFE", "TMO", "COST", "DIS", "NFLX", "CSCO",
    "VZ", "INTC", "AMD", "QCOM", "TXN", "IBM", "GE", "CAT", "HON", "UPS",
    "SPY", "QQQ", "IWM", "XLK", "XLE", "XLF", "XLV", "XLI", "GLD", "TLT",
]

# Neo's preferred trading universe — mirrors crew_scanner.NEO_PREFERRED
NEO_PREFERRED: list[str] = [
    "NVDA", "AMD", "TSLA", "META", "AAPL", "AMZN",
    "NFLX", "GOOGL", "MSFT", "AVGO", "MU", "COIN",
    "PLTR", "SOFI", "INTC", "CRM",
]

# Backtest trailing stop tracker for Neo runner positions
# {f"{agent_id}|{symbol}": high_watermark} — updated per-day
_bt_trail_highs: dict[str, float] = {}

# 9 bake-off agents (original fleet + Silent Four)
BAKEOFF_AGENTS = [
    {"id": "neo-matrix",      "name": "Neo",        "strategy": "momentum breakouts",      "hint": "Trade the leaders — NVDA, AMD, TSLA, META, AAPL, AMZN. Momentum + volume."},
    {"id": "grok-4",          "name": "Spock",       "strategy": "mean reversion RSI",      "hint": "RSI oversold (<35). Session must NOT be trending."},
    {"id": "ollama-glm4",     "name": "Q",           "strategy": "any edge, no constraints","hint": "Find the best asymmetric risk/reward. Surprise me."},
    {"id": "ollama-qwen3",    "name": "Dax",         "strategy": "defensive value XLU/XLP", "hint": "Defensive/value: XLU, XLP, XLV when risk-off."},
    {"id": "ollama-plutus",   "name": "Dr. McCoy",   "strategy": "crisis fading VIX > 22",  "hint": "Only if VIX > 22. Oversold bounces, panic fades."},
    {"id": "data-tng",        "name": "Data",        "strategy": "quantitative scoring",    "hint": "Score: RSI<35+3, vol>2x+2, SMA20+1, MACD cross+2, sector+1, green+1. Buy>=6, half>=4."},
    # Silent Four — added to backtest
    {"id": "dayblade-sulu",   "name": "Sulu",        "strategy": "gap and go momentum",     "hint": "Stocks gapping up >2% at open. Trend-following. Session must be bullish."},
    {"id": "gemini-2.5-flash","name": "Worf",        "strategy": "bearish inverse ETFs",    "hint": "VIX > 20 only. Buy SH, SQQQ, or UVXY. Skip confirmed bull sessions."},
    {"id": "ollama-llama",    "name": "Uhura",       "strategy": "earnings catalyst plays", "hint": "Big movers: >4% single-day on >2x volume. Ride the catalyst."},
    {"id": "gemini-2.5-pro",  "name": "Seven",       "strategy": "pure quant data",         "hint": "Pick the symbol with highest signal_strength. Pure data, no bias."},
]

# Agent split: rules-based (instant) vs Ollama (model under test)
BACKTEST_RULES_AGENTS  = [a for a in BAKEOFF_AGENTS if a["id"] in (
    "grok-4", "ollama-qwen3", "ollama-plutus", "data-tng",
    "gemini-2.5-flash", "ollama-llama",   # active rules agents
)]
BACKTEST_OLLAMA_AGENTS = [a for a in BAKEOFF_AGENTS if a["id"] in ("neo-matrix", "ollama-glm4")]

# Non-ticker words to exclude from response parsing (mirrors crew_scanner)
_NON_TICKER_WORDS = {
    "TRADE", "BUY", "SELL", "SHORT", "HOLD", "PASS", "HIGH", "LOW",
    "RSI", "VIX", "ETF", "ATM", "OTM", "ITM", "DTE", "PCR",
    "THE", "AND", "FOR", "WITH", "FROM", "INTO", "THAT",
    "THIS", "WILL", "HAVE", "MORE", "THAN", "ALSO", "WHEN", "THEN",
    "YOUR", "THEY", "BEEN", "WOULD", "COULD", "SHOULD", "BOTH",
    "MARKET", "STOCK", "PRICE", "ABOVE", "BELOW", "BASED", "GIVEN",
    "SESSION", "SIGNAL", "TREND", "SECTOR", "SETUP", "ENTRY",
    "CONFIDENCE", "REASON", "DECISION",
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class BakeoffTrade:
    agent_id: str
    agent_name: str
    symbol: str
    action: str         # BUY
    entry_date: str
    entry_price: float
    qty: float
    confidence: int
    reason: str
    exit_date: Optional[str] = None
    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None   # PROFIT_TARGET / STOP_LOSS / MAX_HOLD / EOD
    pnl: float = 0.0
    pnl_pct: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class AgentState:
    agent_id: str
    agent_name: str
    cash: float = STARTING_CASH
    positions: list = field(default_factory=list)  # list of BakeoffTrade (open)
    closed_trades: list = field(default_factory=list)

    @property
    def equity(self) -> float:
        return self.cash + sum(
            t.qty * t.entry_price for t in self.positions
        )

    @property
    def win_rate(self) -> float:
        wins = [t for t in self.closed_trades if t.pnl > 0]
        return len(wins) / len(self.closed_trades) * 100 if self.closed_trades else 0.0

    @property
    def total_return_pct(self) -> float:
        return (self.equity - STARTING_CASH) / STARTING_CASH * 100

    @property
    def profit_factor(self) -> float:
        gross_win  = sum(t.pnl for t in self.closed_trades if t.pnl > 0)
        gross_loss = abs(sum(t.pnl for t in self.closed_trades if t.pnl < 0))
        return gross_win / gross_loss if gross_loss > 0 else (gross_win if gross_win > 0 else 1.0)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _warmup_model(model: str) -> None:
    """Send a tiny query to load the model into RAM before the replay loop."""
    logger.info(f"Warming up {model}...")
    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={
                "model":   model,
                "prompt":  "say OK",
                "stream":  False,
                "think":   False,
                "options": {"num_predict": 5},
            },
            timeout=180,
        )
        if r.ok:
            logger.info(f"{model} warm — ready to scan")
        else:
            logger.warning(f"{model} warmup failed: {r.status_code}")
    except Exception as e:
        logger.warning(f"{model} warmup error: {e}")


def _conn() -> sqlite3.Connection:
    """Write connection to backtest.db (never touches trader.db)."""
    c = sqlite3.connect(BACKTEST_DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def ensure_bakeoff_tables() -> None:
    """Create bakeoff_runs and bakeoff_trades tables if they don't exist."""
    c = _conn()
    try:
        c.executescript("""
            CREATE TABLE IF NOT EXISTS bakeoff_runs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                model       TEXT NOT NULL,
                days        INTEGER NOT NULL,
                start_date  TEXT,
                end_date    TEXT,
                status      TEXT DEFAULT 'pending',
                progress    INTEGER DEFAULT 0,
                message     TEXT,
                results_json TEXT,
                started_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                finished_at TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS bakeoff_trades (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id      INTEGER NOT NULL REFERENCES bakeoff_runs(id),
                model       TEXT NOT NULL,
                agent_id    TEXT NOT NULL,
                agent_name  TEXT NOT NULL,
                symbol      TEXT NOT NULL,
                action      TEXT NOT NULL,
                entry_date  TEXT NOT NULL,
                entry_price REAL,
                qty         REAL,
                confidence  INTEGER,
                reason      TEXT,
                exit_date   TEXT,
                exit_price  REAL,
                exit_reason TEXT,
                pnl         REAL DEFAULT 0,
                pnl_pct     REAL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS backtest_market_data (
                symbol      TEXT NOT NULL,
                trade_date  TEXT NOT NULL,
                open        REAL,
                high        REAL,
                low         REAL,
                close       REAL,
                volume      REAL,
                cached_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, trade_date)
            );
        """)
        c.commit()
    finally:
        c.close()


# ---------------------------------------------------------------------------
# Historical market data
# ---------------------------------------------------------------------------

def _fetch_ohlcv(symbols: list[str], days: int) -> dict[str, dict[str, dict]]:
    """
    Download OHLCV for symbols going back `days` trading days.
    Returns: {symbol: {date_str: {open,high,low,close,volume}}}
    Caches to backtest_market_data table.
    """
    import yfinance as yf
    import pandas as pd

    end = datetime.now()
    start = end - timedelta(days=days + 10)  # buffer for weekends/holidays

    logger.info(f"Downloading OHLCV for {len(symbols)} symbols ({days}d)...")
    result: dict[str, dict[str, dict]] = {}

    # Batch download
    tickers = yf.download(
        " ".join(symbols),
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        auto_adjust=True,
        progress=False,
        threads=True,
    )

    c = _conn()
    try:
        # yfinance returns multi-level columns for multi-ticker download
        if isinstance(tickers.columns, pd.MultiIndex):
            for sym in symbols:
                try:
                    df = tickers.xs(sym, level=1, axis=1).dropna()
                    if df.empty:
                        continue
                    sym_data: dict[str, dict] = {}
                    for idx, row in df.iterrows():
                        d = idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx)[:10]
                        sym_data[d] = {
                            "open":   float(row.get("Open",  row.get("open",  0))),
                            "high":   float(row.get("High",  row.get("high",  0))),
                            "low":    float(row.get("Low",   row.get("low",   0))),
                            "close":  float(row.get("Close", row.get("close", 0))),
                            "volume": float(row.get("Volume",row.get("volume",0))),
                        }
                        c.execute(
                            "INSERT OR REPLACE INTO backtest_market_data "
                            "(symbol, trade_date, open, high, low, close, volume) VALUES (?,?,?,?,?,?,?)",
                            (sym, d, sym_data[d]["open"], sym_data[d]["high"],
                             sym_data[d]["low"], sym_data[d]["close"], sym_data[d]["volume"]),
                        )
                    result[sym] = sym_data
                except Exception as e:
                    logger.warning(f"OHLCV parse error for {sym}: {e}")
        else:
            # Single ticker fallback
            sym = symbols[0]
            df = tickers.dropna()
            sym_data = {}
            for idx, row in df.iterrows():
                d = idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx)[:10]
                sym_data[d] = {
                    "open":   float(row.get("Open",  0)),
                    "high":   float(row.get("High",  0)),
                    "low":    float(row.get("Low",   0)),
                    "close":  float(row.get("Close", 0)),
                    "volume": float(row.get("Volume",0)),
                }
            result[sym] = sym_data

        c.commit()
    finally:
        c.close()

    logger.info(f"Downloaded {sum(len(v) for v in result.values())} rows across {len(result)} symbols")
    return result


def _get_trading_days(days: int) -> list[str]:
    """Return last N trading days as date strings (most recent last)."""
    import yfinance as yf
    spy = yf.download("SPY", period=f"{days + 15}d", auto_adjust=True, progress=False)
    if spy.empty:
        # fallback: business days
        end = datetime.now().date()
        dates = []
        d = end - timedelta(days=1)
        while len(dates) < days:
            if d.weekday() < 5:  # Mon-Fri
                dates.insert(0, d.strftime("%Y-%m-%d"))
            d -= timedelta(days=1)
        return dates
    all_days = [idx.strftime("%Y-%m-%d") for idx in spy.index]
    return all_days[-days:]


# ---------------------------------------------------------------------------
# Per-day technical indicators for rules agents
# ---------------------------------------------------------------------------

def _compute_scan_picks(ohlcv: dict[str, dict[str, dict]], date_str: str) -> list[dict]:
    """
    From ohlcv {symbol: {date: {open,high,low,close,volume}}}, compute
    RSI-14, SMA-20, volume ratio, and 5d ROC for each symbol up to date_str.
    Returns list sorted by |roc_5d| descending (most movement first).
    """
    picks = []
    for symbol, sym_data in ohlcv.items():
        dates = sorted(d for d in sym_data if d <= date_str)
        if len(dates) < 14:
            continue

        closes  = [float(sym_data[d]["close"])  for d in dates]
        volumes = [float(sym_data[d]["volume"]) for d in dates]

        # RSI-14
        deltas   = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
        gains    = [d if d > 0 else 0 for d in deltas[-14:]]
        losses   = [-d if d < 0 else 0 for d in deltas[-14:]]
        avg_gain = sum(gains) / 14
        avg_loss = sum(losses) / 14
        rs       = avg_gain / avg_loss if avg_loss > 0 else 100
        rsi      = 100 - (100 / (1 + rs))

        # SMA-20
        tail20 = closes[-20:]
        sma20  = sum(tail20) / len(tail20)

        # Volume ratio vs 20d avg
        vol_tail  = volumes[-20:]
        vol_avg   = sum(vol_tail) / len(vol_tail)
        vol_ratio = volumes[-1] / vol_avg if vol_avg > 0 else 1.0

        # 5d ROC
        roc_5d = ((closes[-1] - closes[-6]) / closes[-6] * 100) if len(closes) >= 6 else 0.0

        # Today's change
        change_today = ((closes[-1] - closes[-2]) / closes[-2] * 100) if len(closes) >= 2 else 0.0

        # MACD cross up (12, 26, 9) — needs >=35 bars
        macd_cross_up = False
        if len(closes) >= 35:
            def _ema_series(vals: list[float], period: int) -> list[float]:
                k, out = 2 / (period + 1), [vals[0]]
                for v in vals[1:]:
                    out.append(v * k + out[-1] * (1 - k))
                return out
            ema12 = _ema_series(closes, 12)
            ema26 = _ema_series(closes, 26)
            macd  = [ema12[i] - ema26[i] for i in range(len(closes))]
            sig   = _ema_series(macd, 9)
            # Cross up: yesterday macd < signal, today macd >= signal
            if len(macd) >= 2 and macd[-1] >= sig[-1] and macd[-2] < sig[-2]:
                macd_cross_up = True

        # Composite score (item 3): volume-weighted + RSI bonus + today's move + trend
        rsi_bonus  = 1.0 if rsi < 40 else 0.0
        above_sma  = 0.5 if closes[-1] > sma20 > 0 else 0.0
        score = vol_ratio * 2.0 + rsi_bonus + abs(change_today) * 0.5 + above_sma

        picks.append({
            "symbol":          symbol,
            "close":           closes[-1],
            "rsi_14":          round(rsi, 1),
            "sma_20":          round(sma20, 2),
            "volume_ratio":    round(vol_ratio, 2),
            "roc_5d":          round(roc_5d, 2),
            "change_today":    round(change_today, 2),
            "signal_strength": round(score, 2),
            "macd_cross_up":   macd_cross_up,
        })

    picks.sort(key=lambda x: x["signal_strength"], reverse=True)
    return picks


# ---------------------------------------------------------------------------
# Per-day market context reconstruction
# ---------------------------------------------------------------------------

def _compute_spy_vol_ratio(spy_data: dict[str, dict], date_str: str) -> float:
    """Return today's SPY volume divided by its 20-day average."""
    dates = sorted(d for d in spy_data if d <= date_str)
    if len(dates) < 2:
        return 1.0
    vols = [float(spy_data[d].get("volume", 0)) for d in dates[-21:]]
    if len(vols) < 2 or vols[-1] == 0:
        return 1.0
    avg = sum(vols[:-1]) / len(vols[:-1])
    return round(vols[-1] / avg, 2) if avg > 0 else 1.0


def _reconstruct_context(date_str: str, ohlcv: dict[str, dict[str, dict]]) -> dict[str, Any]:
    """
    Reconstruct what the market context would have looked like on `date_str`.
    Uses only data available up to and including that date.
    """
    spy_data = ohlcv.get("SPY", {})
    vix_data = ohlcv.get("^VIX", {}) or ohlcv.get("VIX", {})

    # SPY day return
    spy_today = spy_data.get(date_str, {})
    spy_close = float(spy_today.get("close", 0))

    # VIX from data or SPY vol proxy
    vix_today = vix_data.get(date_str, {})
    vix = float(vix_today.get("close", 0))
    if vix == 0:
        # proxy: intraday range of SPY as VIX surrogate
        if spy_today.get("high") and spy_today.get("low") and spy_today.get("close"):
            rng = (float(spy_today["high"]) - float(spy_today["low"])) / float(spy_today["close"])
            vix = round(rng * 250, 1)  # annualize daily range
        vix = max(vix, 12)

    # SPY 5-day momentum (ROC)
    sorted_spy_dates = sorted(spy_data.keys())
    spy_before = [d for d in sorted_spy_dates if d <= date_str]
    spy_5d_roc = 0.0
    if len(spy_before) >= 6:
        c5 = float(spy_data[spy_before[-1]].get("close", 0))
        c0 = float(spy_data[spy_before[-6]].get("close", 1))
        spy_5d_roc = (c5 - c0) / c0 * 100 if c0 > 0 else 0.0

    # Session type based on SPY return
    spy_prev = spy_before[-2] if len(spy_before) >= 2 else None
    spy_prev_close = float(spy_data.get(spy_prev, {}).get("close", spy_close)) if spy_prev else spy_close
    day_ret = (spy_close - spy_prev_close) / spy_prev_close * 100 if spy_prev_close else 0
    if abs(day_ret) < 0.3:
        session_type = "CHOPPY"
    elif day_ret >= 0.5:
        session_type = "TRENDING_UP"
    elif day_ret <= -0.5:
        session_type = "TRENDING_DOWN"
    else:
        session_type = "MIXED"

    # Breadth: count of symbols above their 20-day SMA
    above_20ma = 0
    total_checked = 0
    for sym in list(ohlcv.keys())[:20]:  # check first 20 for speed
        sym_dates = sorted(d for d in ohlcv[sym] if d <= date_str)
        if len(sym_dates) >= 20:
            closes = [float(ohlcv[sym][d]["close"]) for d in sym_dates[-20:]]
            sma20 = sum(closes) / 20
            if closes[-1] > sma20:
                above_20ma += 1
            total_checked += 1
    breadth = above_20ma / total_checked if total_checked > 0 else 0.5

    # Fear & Greed estimate from VIX + momentum
    fg_raw = 50 + spy_5d_roc * 3 - (vix - 20) * 1.5
    fg_score = max(0, min(100, int(fg_raw)))

    # Top picks: rank symbols by 5d momentum
    picks = []
    for sym in BAKEOFF_UNIVERSE:
        sym_dates = sorted(d for d in ohlcv.get(sym, {}) if d <= date_str)
        if len(sym_dates) >= 6:
            c_now  = float(ohlcv[sym][sym_dates[-1]].get("close", 0))
            c_5ago = float(ohlcv[sym][sym_dates[-6]].get("close", 1))
            if c_5ago > 0:
                roc5 = (c_now - c_5ago) / c_5ago * 100
                picks.append({"symbol": sym, "signal_strength": round(roc5 / 10, 2)})
    picks.sort(key=lambda x: x["signal_strength"], reverse=True)

    # Volume spikes: symbols with vol > 2x average and |change| > 1%
    volume_spikes = []
    for sym in BAKEOFF_UNIVERSE:
        sym_dates = sorted(d for d in ohlcv.get(sym, {}) if d <= date_str)
        if len(sym_dates) < 21:
            continue
        sym_vols   = [float(ohlcv[sym][d].get("volume", 0)) for d in sym_dates[-21:]]
        avg_vol    = sum(sym_vols[:-1]) / max(len(sym_vols) - 1, 1)
        vol_ratio  = sym_vols[-1] / avg_vol if avg_vol > 0 else 1.0
        sym_closes = [float(ohlcv[sym][d].get("close", 0)) for d in sym_dates[-2:]]
        change_pct = ((sym_closes[-1] - sym_closes[-2]) / sym_closes[-2] * 100
                      if len(sym_closes) == 2 and sym_closes[-2] > 0 else 0.0)
        if vol_ratio >= 2.0 and abs(change_pct) >= 1.0:
            volume_spikes.append({
                "symbol":       sym,
                "volume_ratio": round(vol_ratio, 2),
                "change_pct":   round(change_pct, 2),
            })
    volume_spikes.sort(key=lambda x: x["volume_ratio"], reverse=True)

    # Overnight gaps for Sulu: today's open vs yesterday's close
    gaps = []
    for sym in BAKEOFF_UNIVERSE:
        sym_dates = sorted(d for d in ohlcv.get(sym, {}) if d <= date_str)
        if len(sym_dates) < 2:
            continue
        prev_close = float(ohlcv[sym][sym_dates[-2]].get("close", 0))
        today_open = float(ohlcv[sym][sym_dates[-1]].get("open", 0))
        if prev_close > 0 and today_open > 0:
            gap_pct = (today_open / prev_close - 1) * 100
            if abs(gap_pct) > 1.5:
                gaps.append({
                    "symbol":    sym,
                    "gap_pct":   round(gap_pct, 1),
                    "direction": "UP" if gap_pct > 0 else "DOWN",
                })
    gaps.sort(key=lambda x: abs(x["gap_pct"]), reverse=True)

    return {
        "session_type":   session_type,
        "vix":            vix,
        "momentum_score": round(spy_5d_roc, 1),
        "fg_score":       fg_score,
        "breadth_score":  round(breadth * 11, 1),  # out of 11 like live system
        "spy_day_return": round(day_ret, 2),
        "spy_price":      spy_close,
        "deep_scan_top":  picks[:5],
        "sector_leaders": [p["symbol"] for p in picks[:3]],
        "sector_laggards":[p["symbol"] for p in picks[-3:]],
        "troi_signal":    "GO" if vix < 25 and day_ret > -1 else "CAUTION",
        "pc_ratio":       1.0,
        "spy_volume_ratio": _compute_spy_vol_ratio(spy_data, date_str),
        "volume_spikes":  volume_spikes[:10],
        "gaps":           gaps[:10],
    }


# ---------------------------------------------------------------------------
# Ollama prompt + response
# ---------------------------------------------------------------------------

def _query_ollama(model: str, system_prompt: str, user_prompt: str,
                  timeout: int = 120, agent_id: str = "") -> str:
    try:
        resp = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={
                "model":   model,
                "system":  system_prompt,
                "prompt":  user_prompt,
                "stream":  False,
                "think":   False,
                "options": {"num_predict": 120 if agent_id == "neo-matrix" else 80, "temperature": 0.3},
            },
            timeout=timeout,
        )
        resp.raise_for_status()
        return resp.json().get("response", "").strip()
    except requests.Timeout:
        logger.warning(f"Ollama timeout ({model})")
        return ""
    except Exception as e:
        logger.warning(f"Ollama error ({model}): {e}")
        return ""


def _parse_decision(response: str) -> dict[str, Any]:
    if not response:
        return {"action": "PASS", "symbol": None, "confidence": 0, "reason": "No response"}

    up = response.upper()
    if any(w in up for w in ("PASS", "HOLD", "NO TRADE", "STAND DOWN")):
        return {"action": "PASS", "symbol": None, "confidence": 0, "reason": response[:200]}

    if not (("TRADE" in up) or any(w in up for w in ("BUY", "SELL", "SHORT"))):
        return {"action": "PASS", "symbol": None, "confidence": 0, "reason": response[:200]}

    action = "BUY"
    if "SHORT" in up or ("SELL" in up and "BUY" not in up):
        action = "SELL"

    raw_syms = re.findall(r'\b([A-Z]{2,5})\b', response)
    tickers = [s for s in raw_syms if s not in _NON_TICKER_WORDS]
    symbol = tickers[0] if tickers else "SPY"

    confidence = 50
    m = re.search(r'(\d{1,3})\s*%?\s*(?:confidence|conf)', response, re.IGNORECASE)
    if not m:
        m = re.search(r'confidence[:\s]+(\d{1,3})', response, re.IGNORECASE)
    if m:
        v = int(m.group(1))
        if 0 < v <= 100:
            confidence = v

    return {"action": action, "symbol": symbol, "confidence": confidence, "reason": response[:200]}


def _build_prompt(agent: dict, ctx: dict, date_str: str,
                  scan_picks: list[dict] | None = None) -> tuple[str, str]:
    # Use rich scan_picks for Neo (has volume_ratio, roc_5d, close); fall back to ctx
    ds_top = scan_picks if scan_picks is not None else ctx.get("deep_scan_top", [])

    if agent["id"] == "neo-matrix":
        # ── NEO: rebuilt aggressive prompt (mirrors live crew_scanner) ─────────
        # Neo sees ONLY preferred symbols — sort volume spikes first, then ROC
        neo_ds = [p for p in ds_top if p.get("symbol") in NEO_PREFERRED]
        neo_ds.sort(key=lambda p: (-(p.get("volume_ratio", 1)), -(p.get("roc_5d", 0))))
        ds_str = ", ".join(
            f"{r['symbol']}({r.get('signal_strength',0):.2f})" for r in neo_ds[:5]
        ) or "none (no preferred symbols in scan today)"

        # Knives to avoid — from full scan_picks to catch any preferred names
        knives = [
            p["symbol"] for p in ds_top
            if p.get("roc_5d", 0) < -5.0
            and float(p.get("close", 0)) < float(p.get("sma_20", 99999))
            and p.get("symbol") in NEO_PREFERRED
        ][:4]

        # Volume spike leaders: NEO_PREFERRED with vol > 1.5x, sorted highest first
        vol_leaders = sorted(
            [p for p in ds_top if p.get("symbol") in NEO_PREFERRED and p.get("volume_ratio", 1) >= 1.5],
            key=lambda x: x.get("volume_ratio", 1), reverse=True
        )[:3]
        vol_str = ", ".join(
            f"{p['symbol']} {p.get('change_today',0):+.1f}% ({p.get('volume_ratio',1):.1f}x)"
            for p in vol_leaders
        ) or "none"

        system_prompt = (
            f"You are Neo — The One. Backtest replay: {date_str}. "
            "You are AGGRESSIVE but SMART.\n"
            "Trade the LEADERS with VOLUME and MOMENTUM. "
            "NEVER buy falling knives (down >5% in 5d, below SMA20).\n"
            "On high volume days you MUST find a trade.\n"
            "Format: TRADE BUY [SYMBOL] [CONFIDENCE 0-100] [ONE SENTENCE THESIS]\n"
            "Or: PASS [ONE SENTENCE WHY]"
        )
        user_prompt = (
            f"Date: {date_str} | Session: {ctx['session_type']}\n"
            f"VIX: {float(ctx['vix']):.1f} | F&G: {ctx['fg_score']} | "
            f"Momentum: {float(ctx['momentum_score']):.0f} | Breadth: {ctx['breadth_score']}/11\n"
            f"SPY: {float(ctx.get('spy_day_return',0)):+.1f}%\n\n"
            f"VOLUME LEADERS: {vol_str}\n"
            f"Top picks: {ds_str}\n"
            f"AVOID (falling knives): {', '.join(knives) if knives else 'none'}\n"
            f"Your mandate: Trade the leaders. Find momentum.\n"
            f"Decision?"
        )
    else:
        # ── All other agents — standard prompt ────────────────────────────────
        ds_str = ", ".join(
            f"{r['symbol']}({r.get('signal_strength', 0):.2f})" for r in ds_top[:5]
        ) or "none"
        hint = agent.get("hint", "")
        system_prompt = (
            f"You are {agent['name']}. Backtest replay: {date_str}. Decide: TRADE or PASS.\n"
            f"Format: TRADE BUY/SELL [SYMBOL] [CONFIDENCE 0-100] [REASON]\n"
            f"Or: PASS [REASON]\n"
            f"One line only. No explanation."
        )
        user_prompt = (
            f"Session: {ctx['session_type']}\n"
            f"VIX: {float(ctx['vix']):.1f}\n"
            f"Momentum: {float(ctx['momentum_score']):.0f}\n"
            f"F&G: {ctx['fg_score']}\n"
            f"Breadth: {ctx['breadth_score']}/11\n"
            f"Top picks: {ds_str}\n"
            f"Your mandate: {agent['strategy']}. {hint}\n"
            f"Decision?"
        )

    return system_prompt, user_prompt


# ---------------------------------------------------------------------------
# Position management
# ---------------------------------------------------------------------------

def _apply_exits(
    state: AgentState,
    date_str: str,
    ohlcv: dict[str, dict[str, dict]],
    all_dates: list[str],
) -> None:
    """Check open positions against exit rules; close if triggered."""
    still_open = []
    for trade in state.positions:
        sym_data = ohlcv.get(trade.symbol, {})
        today = sym_data.get(date_str, {})
        close_price = float(today.get("close", trade.entry_price))

        pnl_pct = (close_price - trade.entry_price) / trade.entry_price

        # Days held
        try:
            entry_idx = all_dates.index(trade.entry_date) if trade.entry_date in all_dates else 0
            today_idx = all_dates.index(date_str) if date_str in all_dates else entry_idx
            days_held = today_idx - entry_idx
        except Exception:
            days_held = 0

        # Per-agent thresholds (item 10: wider stops)
        thresholds = _AGENT_STOPS.get(
            state.agent_id,
            {"stop": STOP_LOSS, "target": PROFIT_TARGET, "max_days": MAX_HOLD_DAYS}
        )
        stop_pct  = thresholds["stop"]
        max_days  = thresholds["max_days"]

        # Hard stop check
        exit_reason = None
        if pnl_pct <= stop_pct:
            exit_reason = "STOP_LOSS"
        elif days_held >= max_days:
            exit_reason = "MAX_HOLD"
        else:
            # Scaled exits (item 11): partial sells at profit tiers
            tiers = _BT_SCALED_TIERS.get(state.agent_id, [])
            tier_key = f"{trade.symbol}_tiers_hit"
            tiers_hit: set = getattr(trade, "_tiers_hit", set())
            for threshold, fraction, label in sorted(tiers, key=lambda x: -x[0]):
                if pnl_pct >= threshold and label not in tiers_hit:
                    sell_qty  = trade.qty * fraction
                    sell_val  = close_price * sell_qty
                    trade.qty -= sell_qty
                    state.cash += sell_val
                    tiers_hit.add(label)
                    trade._tiers_hit = tiers_hit  # type: ignore[attr-defined]
                    logger.info(
                        f"[{state.agent_id}] SCALE {label}: sold {fraction*100:.0f}% "
                        f"of {trade.symbol} at +{pnl_pct*100:.1f}% (+${sell_val:.2f})"
                    )
                    break
            # Neo trailing stop: activates after T1 fires
            if state.agent_id == "neo-matrix" and "T1" in tiers_hit:
                trail_key = f"neo-matrix|{trade.symbol}"
                day_high = float(today.get("high", close_price))
                prev_high = _bt_trail_highs.get(trail_key, 0.0)
                _bt_trail_highs[trail_key] = max(prev_high, day_high)
                trail_floor = max(trade.entry_price, _bt_trail_highs[trail_key] * 0.95)
                if close_price <= trail_floor and _bt_trail_highs[trail_key] > trade.entry_price:
                    exit_reason = "TRAILING_STOP"
                    logger.info(
                        f"[neo-matrix] TRAILING_STOP {trade.symbol}: "
                        f"close={close_price:.2f} floor={trail_floor:.2f} "
                        f"high_wm={_bt_trail_highs[trail_key]:.2f}"
                    )

            # Full exit: last tier (T1) was hit AND qty small enough, or highest target hit
            if not exit_reason:
                if tiers and len(tiers_hit) >= len(tiers):
                    exit_reason = "SCALED_EXIT_FULL"
                elif not tiers and pnl_pct >= thresholds.get("target", PROFIT_TARGET):
                    exit_reason = "PROFIT_TARGET"

        if exit_reason:
            trade.exit_date   = date_str
            trade.exit_price  = close_price
            trade.exit_reason = exit_reason
            trade.pnl         = (close_price - trade.entry_price) * trade.qty
            trade.pnl_pct     = pnl_pct * 100
            state.cash += close_price * trade.qty
            state.closed_trades.append(trade)
            # Clean up Neo trailing stop tracker
            _bt_trail_highs.pop(f"neo-matrix|{trade.symbol}", None)
            logger.info(
                f"[{state.agent_id}] CLOSED {trade.symbol} {exit_reason} "
                f"pnl={trade.pnl:+.2f} ({pnl_pct*100:+.1f}%)"
            )
        else:
            still_open.append(trade)

    state.positions = still_open


def _apply_entry(
    state: AgentState,
    decision: dict,
    date_str: str,
    ohlcv: dict[str, dict[str, dict]],
    agent: dict,
) -> Optional[BakeoffTrade]:
    """Open a new position if valid BUY decision and enough cash."""
    if decision["action"] != "BUY":
        return None
    symbol = decision["symbol"]
    if not symbol:
        return None
    # Must be in our universe and have data for the day
    sym_data = ohlcv.get(symbol, {})
    today = sym_data.get(date_str, {})
    entry_price = float(today.get("open", today.get("close", 0)))
    if entry_price <= 0:
        logger.debug(f"[{state.agent_id}] No price data for {symbol} on {date_str}, skipping")
        return None

    # Don't double-up on same symbol
    if any(p.symbol == symbol for p in state.positions):
        return None

    # Max 3 positions at a time
    if len(state.positions) >= 3:
        return None

    # Falling knife filter (item 4/15): skip if steep downtrend + below SMA20
    sym_dates_all = sorted(d for d in sym_data if d <= date_str)
    if len(sym_dates_all) >= 21:
        recent_closes = [float(sym_data[d]["close"]) for d in sym_dates_all[-21:]]
        roc_5d_chk    = (recent_closes[-1] - recent_closes[-6]) / recent_closes[-6] * 100
        sma20_chk     = sum(recent_closes[-20:]) / 20
        if roc_5d_chk < -5.0 and recent_closes[-1] < sma20_chk:
            logger.debug(
                f"[{state.agent_id}] FALLING KNIFE skip: {symbol} "
                f"5d {roc_5d_chk:.1f}%, below SMA20"
            )
            return None

    # Neo: confidence-based position sizing
    # Data: half size at conf=55 (score 4-5), full size at conf=82 (score >= 6)
    if state.agent_id == "neo-matrix":
        conf = decision.get("confidence", 70)
        if conf >= 90:   _size_pct = 0.10
        elif conf >= 80: _size_pct = 0.07
        elif conf >= 70: _size_pct = 0.05
        else:            _size_pct = 0.03
        position_value = state.cash * _size_pct
    elif state.agent_id == "data-tng":
        conf = decision.get("confidence", 82)
        position_value = state.cash * (POSITION_PCT / 2 if conf < 70 else POSITION_PCT)
    else:
        position_value = state.cash * POSITION_PCT
    if position_value < 100:  # not enough cash
        return None
    qty = position_value / entry_price

    trade = BakeoffTrade(
        agent_id    = state.agent_id,
        agent_name  = state.agent_name,
        symbol      = symbol,
        action      = "BUY",
        entry_date  = date_str,
        entry_price = entry_price,
        qty         = qty,
        confidence  = decision["confidence"],
        reason      = decision["reason"],
    )
    state.cash -= position_value
    state.positions.append(trade)
    logger.info(
        f"[{state.agent_id}] BUY {symbol} @ {entry_price:.2f} "
        f"qty={qty:.2f} conf={decision['confidence']}"
    )
    return trade


def _force_close_all(
    state: AgentState,
    date_str: str,
    ohlcv: dict[str, dict[str, dict]],
) -> None:
    """Close all positions at EOD price (end of backtest window)."""
    for trade in state.positions:
        sym_data = ohlcv.get(trade.symbol, {})
        today = sym_data.get(date_str, {})
        close_price = float(today.get("close", trade.entry_price))
        trade.exit_date   = date_str
        trade.exit_price  = close_price
        trade.exit_reason = "EOD_CLOSE"
        trade.pnl         = (close_price - trade.entry_price) * trade.qty
        trade.pnl_pct     = (close_price - trade.entry_price) / trade.entry_price * 100
        state.cash += close_price * trade.qty
        state.closed_trades.append(trade)
    state.positions = []


# ---------------------------------------------------------------------------
# Main backtest runner
# ---------------------------------------------------------------------------

def run_backtest(
    days: int = 30,
    model: str = "qwen3.5:9b",
    run_id: Optional[int] = None,
    progress_cb = None,
) -> dict:
    """
    Run a full bake-off backtest.

    Args:
        days:        Number of trading days to replay
        model:       Ollama model name (e.g. "qwen3.5:9b", "0xroyce/plutus")
        run_id:      bakeoff_runs row id (created by caller or auto-created here)
        progress_cb: Optional callable(pct: int, msg: str) for live progress

    Returns:
        results dict with per-agent summary and all_trades list
    """
    if os.path.exists(_LOCK_FILE):
        logger.error("Another backtest is already running (backtest.lock exists) — aborting")
        return {"error": "backtest already running"}

    ensure_bakeoff_tables()

    try:
        with open(_LOCK_FILE, "w") as _lf:
            _lf.write(str(os.getpid()))
    except Exception as _le:
        logger.warning(f"Could not write lock file: {_le}")

    try:
        return _run_backtest_inner(
            days=days, model=model, run_id=run_id, progress_cb=progress_cb
        )
    finally:
        try:
            os.remove(_LOCK_FILE)
        except FileNotFoundError:
            pass


def _run_backtest_inner(
    days: int = 30,
    model: str = "qwen3.5:9b",
    run_id: Optional[int] = None,
    progress_cb = None,
) -> dict:
    """Inner backtest logic — called only when lock is held."""

    def _progress(pct: int, msg: str):
        logger.info(f"[{pct}%] {msg}")
        if progress_cb:
            progress_cb(pct, msg)
        if run_id:
            try:
                c = _conn()
                c.execute(
                    "UPDATE bakeoff_runs SET progress=?, message=? WHERE id=?",
                    (pct, msg, run_id),
                )
                c.commit()
                c.close()
            except Exception:
                pass

    _progress(2, f"Fetching {days}d OHLCV for {len(BAKEOFF_UNIVERSE)} symbols...")

    # Download data
    all_symbols = BAKEOFF_UNIVERSE + ["^VIX"]
    ohlcv = _fetch_ohlcv(all_symbols, days + 10)

    # Get ordered trading days
    trading_days = _get_trading_days(days)
    if not trading_days:
        return {"error": "Could not determine trading days"}

    _progress(10, f"Got {len(trading_days)} trading days. Warming up {model}...")
    _warmup_model(model)
    _progress(12, "Model warm. Starting replay...")

    # Initialize agent states
    states = {
        a["id"]: AgentState(agent_id=a["id"], agent_name=a["name"])
        for a in BAKEOFF_AGENTS
    }

    all_trades: list[BakeoffTrade] = []

    # Import rules functions once (lazy to avoid heavy crew_scanner deps at module level)
    from engine.crew_scanner import spock_rules, dax_rules, mccoy_rules, data_rules

    # Silent-Four rules defined locally (not in crew_scanner)
    def sulu_rules(ctx: dict, scan_picks: list) -> dict:
        """Sulu — Gap & Go: overnight gaps > 2% up, trend-following."""
        if "BEAR" in ctx.get("session_type", "") or "DOWN" in ctx.get("session_type", ""):
            return {"action": "PASS", "reason": "Sulu stands down in bear/down sessions"}
        for gap in ctx.get("gaps", []):
            if gap["direction"] == "UP" and gap["gap_pct"] >= 2.0:
                return {
                    "action": "BUY", "symbol": gap["symbol"], "confidence": 78,
                    "reason": f"Gap up {gap['gap_pct']:+.1f}% — momentum continuation play",
                }
        # Fallback: volume momentum spike on an up day
        for spike in ctx.get("volume_spikes", []):
            if spike["change_pct"] > 2.0 and spike["volume_ratio"] >= 2.0:
                return {
                    "action": "BUY", "symbol": spike["symbol"], "confidence": 72,
                    "reason": (
                        f"Volume surge {spike['volume_ratio']:.1f}x, "
                        f"+{spike['change_pct']:.1f}% — momentum entry"
                    ),
                }
        return {"action": "PASS", "reason": "No qualifying gaps or volume momentum today"}

    def worf_rules(ctx: dict, scan_picks: list) -> dict:
        """Worf — Bear Specialist: inverse ETFs when VIX > 20, not in bull sessions."""
        vix     = float(ctx.get("vix", 20))
        session = ctx.get("session_type", "")
        logger.info(f"[WORF CHECK] VIX={vix:.1f}, session={session}, momentum={ctx.get('momentum_score',0):.1f}")
        if vix < 20:
            return {"action": "PASS", "reason": f"VIX {vix:.1f} too low, Worf holds fire"}
        if "BULL" in session or session == "TRENDING_UP":
            return {"action": "PASS", "reason": f"Session {session} — Worf stands down in confirmed bulls"}
        _WORF_INVERSE = ["SH", "SQQQ", "TLT", "GLD"]
        # Prefer the one with most momentum (least negative roc_5d)
        candidates = [p for p in scan_picks if p["symbol"] in _WORF_INVERSE]
        if candidates:
            best = max(candidates, key=lambda p: p.get("roc_5d", 0))
            conf = 85 if vix > 25 else 75
            return {
                "action": "BUY", "symbol": best["symbol"], "confidence": conf,
                "reason": f"VIX {vix:.1f} elevated, session {session} — {best['symbol']} inverse play",
            }
        # Fallback
        conf = 80 if vix > 25 else 70
        symbol = "SQQQ" if ctx.get("momentum_score", 0) < -20 else "SH"
        return {
            "action": "BUY", "symbol": symbol, "confidence": conf,
            "reason": f"VIX {vix:.1f}, session {session} — bearish hedge",
        }

    def uhura_rules(ctx: dict, scan_picks: list) -> dict:
        """Uhura — Earnings Catalyst: proxy via >4% move + >2x volume."""
        for pick in scan_picks:
            change = abs(float(pick.get("change_today", 0)))
            vol_r  = float(pick.get("volume_ratio", 1))
            if change > 4.0 and vol_r > 2.0:
                direction = "BUY" if float(pick.get("change_today", 0)) > 0 else "PASS"
                if direction == "BUY":
                    return {
                        "action": "BUY", "symbol": pick["symbol"], "confidence": 80,
                        "reason": (
                            f"Earnings-proxy catalyst: {pick['symbol']} "
                            f"+{pick['change_today']:.1f}% on {vol_r:.1f}x volume"
                        ),
                    }
        return {"action": "PASS", "reason": "No earnings-proxy catalysts today (need >4% + >2x vol)"}

    # Day loop
    for day_idx, date_str in enumerate(trading_days):
        pct = 10 + int(80 * day_idx / len(trading_days))
        _progress(pct, f"Replaying {date_str} ({day_idx+1}/{len(trading_days)})...")

        ctx        = _reconstruct_context(date_str, ohlcv)
        scan_picks = _compute_scan_picks(ohlcv, date_str)

        # ── Rules agents (instant) ────────────────────────────────────────────
        for agent in BACKTEST_RULES_AGENTS:
            state = states[agent["id"]]
            _apply_exits(state, date_str, ohlcv, trading_days)
            if state.cash > 200:
                if agent["id"] == "grok-4":
                    decision = spock_rules(ctx, scan_picks)
                elif agent["id"] == "ollama-qwen3":
                    decision = dax_rules(ctx, scan_picks)
                elif agent["id"] == "ollama-plutus":
                    decision = mccoy_rules(ctx, scan_picks)
                elif agent["id"] == "data-tng":
                    decision = data_rules(ctx, scan_picks)
                elif agent["id"] == "dayblade-sulu":
                    decision = sulu_rules(ctx, scan_picks)
                elif agent["id"] == "gemini-2.5-flash":
                    decision = worf_rules(ctx, scan_picks)
                elif agent["id"] == "ollama-llama":
                    decision = uhura_rules(ctx, scan_picks)
                else:
                    decision = {"action": "PASS", "reason": "Unknown rules agent"}

                if decision.get("action") == "BUY":
                    trade = _apply_entry(state, decision, date_str, ohlcv, agent)
                    if trade:
                        all_trades.append(trade)

        # ── Ollama agents (model under test) ──────────────────────────────────
        spy_vol_ratio = float(ctx.get("spy_volume_ratio", 1.0))
        for agent in BACKTEST_OLLAMA_AGENTS:
            aid   = agent["id"]
            state = states[aid]
            _apply_exits(state, date_str, ohlcv, trading_days)
            if state.cash > 200:
                sys_p, usr_p = _build_prompt(agent, ctx, date_str, scan_picks=scan_picks)
                response     = _query_ollama(model, sys_p, usr_p, agent_id=aid)
                decision     = _parse_decision(response)

                # Neo: second-query on high-vol days if first answer is PASS
                if aid == "neo-matrix" and decision["action"] == "PASS" and spy_vol_ratio >= 1.5:
                    neo_vol_picks = sorted(
                        [p for p in scan_picks if p.get("symbol") in NEO_PREFERRED
                         and p.get("volume_ratio", 1) >= 1.5],
                        key=lambda x: -x["volume_ratio"]
                    )[:4]
                    if neo_vol_picks:
                        pick_lines = "\n".join(
                            f"{p['symbol']} ${p['close']:.2f} "
                            f"({p.get('change_today',0):+.1f}%) vol {p['volume_ratio']:.1f}x"
                            for p in neo_vol_picks
                        )
                        retry_prompt = (
                            f"HIGH VOLUME DAY. Pick the best setup:\n"
                            f"{pick_lines}\n"
                            f"TRADE BUY [SYMBOL] [CONFIDENCE] [REASON] or PASS [WHY]"
                        )
                        logger.info(f"[neo-matrix] vol day PASS — second look: "
                                    f"{[p['symbol'] for p in neo_vol_picks]}")
                        r2 = _query_ollama(model, sys_p, retry_prompt, agent_id=aid)
                        d2 = _parse_decision(r2)
                        if d2["action"] != "PASS":
                            decision = d2
                            decision["reason"] = f"[2nd look vol day] {decision.get('reason','')}"

                if decision["action"] == "BUY":
                    trade = _apply_entry(state, decision, date_str, ohlcv, agent)
                    if trade:
                        all_trades.append(trade)

    # Force-close all remaining positions at last day's close
    last_day = trading_days[-1]
    for state in states.values():
        _force_close_all(state, last_day, ohlcv)
        all_trades.extend(state.closed_trades)

    # Build results
    _progress(95, "Tallying results...")
    agent_results = []
    for agent in BAKEOFF_AGENTS:
        state = states[agent["id"]]
        final_equity = state.cash  # all closed now
        r = {
            "agent_id":        agent["id"],
            "agent_name":      agent["name"],
            "final_equity":    round(final_equity, 2),
            "total_return_pct": round((final_equity - STARTING_CASH) / STARTING_CASH * 100, 2),
            "num_trades":      len(state.closed_trades),
            "win_rate":        round(state.win_rate, 1),
            "profit_factor":   round(state.profit_factor, 2),
            "total_pnl":       round(sum(t.pnl for t in state.closed_trades), 2),
        }
        agent_results.append(r)
    agent_results.sort(key=lambda x: x["total_return_pct"], reverse=True)

    results = {
        "model":          model,
        "days":           days,
        "start_date":     trading_days[0],
        "end_date":       trading_days[-1],
        "agents":         agent_results,
        "all_trades":     [t.to_dict() for t in all_trades],
        "total_trades":   len(all_trades),
    }

    # Persist trades to DB
    if run_id:
        try:
            c = _conn()
            for t in all_trades:
                c.execute(
                    """INSERT INTO bakeoff_trades
                    (run_id, model, agent_id, agent_name, symbol, action,
                     entry_date, entry_price, qty, confidence, reason,
                     exit_date, exit_price, exit_reason, pnl, pnl_pct)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (run_id, model, t.agent_id, t.agent_name, t.symbol, t.action,
                     t.entry_date, t.entry_price, t.qty, t.confidence, t.reason,
                     t.exit_date, t.exit_price, t.exit_reason, t.pnl, t.pnl_pct),
                )
            c.execute(
                "UPDATE bakeoff_runs SET status='complete', progress=100, "
                "message='Complete', results_json=?, finished_at=CURRENT_TIMESTAMP WHERE id=?",
                (json.dumps(results), run_id),
            )
            c.commit()
            c.close()
        except Exception as e:
            logger.error(f"DB persist error: {e}")

    _progress(100, f"Done. {len(all_trades)} trades across {len(BAKEOFF_AGENTS)} agents.")
    return results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    days_arg  = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    model_arg = sys.argv[2] if len(sys.argv) > 2 else "qwen3.5:9b"
    print(f"\n=== Bake-Off: {model_arg} | {days_arg} days ===\n")
    res = run_backtest(days=days_arg, model=model_arg)
    print(f"\nStart: {res['start_date']}  End: {res['end_date']}")
    print(f"Total trades: {res['total_trades']}\n")
    print(f"{'Agent':<20} {'Return':>8} {'Trades':>7} {'WinRate':>8} {'PF':>6}")
    print("-" * 55)
    for a in res["agents"]:
        sign = "+" if a["total_return_pct"] >= 0 else ""
        print(
            f"{a['agent_name']:<20} {sign}{a['total_return_pct']:>7.2f}%"
            f" {a['num_trades']:>7} {a['win_rate']:>7.1f}%  {a['profit_factor']:>5.2f}"
        )
