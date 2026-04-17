"""Master Backtest Engine — USS TradeMinds comprehensive 90-day backtest.

9 strategy tiers × 24 symbols. Reuses B-S/signal helpers from arsenal_backtest.py
and VectorBT runners from holly_nightly_backtest.py.

Run:  from engine.master_backtest import run_master_backtest; run_master_backtest()
"""
from __future__ import annotations

import logging
import math
import os
import sqlite3
import time
import warnings
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Paths ────────────────────────────────────────────────────────────────────
_DIR        = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_BACKTEST_DB = os.path.join(_DIR, "data", "backtest.db")
_TRADER_DB   = os.path.join(_DIR, "data", "trader.db")

# ── Universe ──────────────────────────────────────────────────────────────────
MASTER_UNIVERSE = [
    "NVDA","AMD","MU","AVGO","META","GOOGL","AAPL","AMZN","MSFT","TSLA",
    "TQQQ","SPY","QQQ","PLTR","MRVL","NFLX","SOFI","COIN","BAC","MARA",
    "XLE","INTC","STAA","SMR",   # Webull additions
]
INVERSE_ETFS = ["SQQQ", "SPXS"]
METALS_ETFS  = ["GLD", "SLV", "CPER"]

# ── Transaction costs ─────────────────────────────────────────────────────────
SLIPPAGE       = 0.001          # 0.1% per equity trade (unchanged)
COMMISSION_STK = 0.00
OPT_COST       = 0.65           # per contract ($0.65 hard cost)
FEES           = SLIPPAGE
STARTING_CASH  = 100_000.0
BACKTEST_DAYS  = 180            # extended from 90 → 180 days

# Realistic options slippage (bid/ask spread cost)
OPT_SLIPPAGE_LEGS = {1: 0.03, 2: 0.06, 4: 0.12}  # legs → round-trip % of premium
OPT_SLIP_PER_LEG  = 0.03   # 3% of premium per leg

# Execution delay penalty (45-second fill latency → ~0.15% adverse move)
EXEC_DELAY     = 0.0015         # 0.15% on top of slippage for entries only

# Arsenal allocation baseline
ALLOC_LONG_EQ   = 0.50
ALLOC_SHORT_EQ  = 0.10
ALLOC_BEAR_CS   = 0.20
ALLOC_IC        = 0.15
ALLOC_CASH      = 0.05

# Options defaults
OPT_DTE_DEFAULT = 30
RISK_FREE       = 0.04


# ═══════════════════════════════════════════════════════════════════════════
# DB helpers
# ═══════════════════════════════════════════════════════════════════════════

def _conn():
    c = sqlite3.connect(_BACKTEST_DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def _init_tables():
    conn = _conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS backtest_master_results (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date         TEXT NOT NULL,
            tier             INTEGER NOT NULL,
            tier_name        TEXT NOT NULL,
            strategy         TEXT NOT NULL,
            ticker           TEXT NOT NULL,
            total_return     REAL, win_rate REAL, sharpe REAL,
            realistic_sharpe REAL, needs_validation INTEGER DEFAULT 0,
            max_drawdown     REAL, avg_hold_hours REAL, num_trades INTEGER,
            profit_factor    REAL, calmar REAL,
            best_trade_pct   REAL, worst_trade_pct REAL,
            spy_return       REAL, vs_spy REAL,
            max_consec_wins INTEGER, max_consec_losses INTEGER,
            regime         TEXT DEFAULT 'ALL',
            created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS backtest_symbol_params (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date     TEXT NOT NULL,
            ticker       TEXT NOT NULL,
            best_strategy TEXT NOT NULL,
            best_sharpe  REAL,
            rsi_period   INTEGER, rsi_entry REAL, rsi_exit REAL,
            macd_fast    INTEGER, macd_slow INTEGER, macd_signal INTEGER,
            bb_period    INTEGER, bb_std REAL,
            sma_fast     INTEGER, sma_slow INTEGER,
            best_tod     TEXT,
            best_options_strategy TEXT,
            optimal_dte  INTEGER, optimal_delta REAL,
            iv_pct_sweet_spot REAL,
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(run_date, ticker)
        );
        CREATE TABLE IF NOT EXISTS backtest_equity_curve (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date  TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            equity    REAL NOT NULL,
            daily_pnl REAL,
            regime    TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS backtest_options_results (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date        TEXT NOT NULL,
            strategy        TEXT NOT NULL,
            ticker          TEXT NOT NULL,
            entry_date      TEXT, exit_date TEXT,
            option_type     TEXT,
            strike          REAL, premium REAL,
            dte_entry       INTEGER, dte_exit INTEGER,
            pnl             REAL, pnl_pct REAL, win INTEGER,
            iv_rank_entry   REAL, iv_rank_exit REAL,
            theta_captured  REAL,
            exit_type       TEXT,
            regime          TEXT,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS backtest_spread_results (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date    TEXT NOT NULL,
            strategy    TEXT NOT NULL,
            ticker      TEXT NOT NULL,
            entry_date  TEXT, exit_date TEXT,
            spread_type TEXT,
            credit      REAL, max_profit REAL, max_loss REAL,
            pnl         REAL, pnl_pct REAL, win INTEGER,
            dte_entry   INTEGER, days_held INTEGER,
            pop_est     REAL,
            exit_type   TEXT, regime TEXT,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS backtest_0dte_results (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date    TEXT NOT NULL,
            strategy    TEXT NOT NULL,
            ticker      TEXT NOT NULL,
            trade_date  TEXT,
            direction   TEXT,
            entry_price REAL, exit_price REAL,
            pnl         REAL, pnl_pct REAL, win INTEGER,
            gex_env     TEXT, vix_at_entry REAL,
            session     TEXT,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS backtest_greeks_summary (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date    TEXT NOT NULL,
            strategy    TEXT NOT NULL,
            ticker      TEXT NOT NULL,
            avg_delta   REAL, avg_theta_per_day REAL, avg_gamma REAL, avg_vega REAL,
            theta_total REAL, avg_iv_entry REAL, avg_iv_exit REAL,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS backtest_monthly_breakdown (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date    TEXT NOT NULL,
            strategy    TEXT NOT NULL,
            ticker      TEXT NOT NULL,
            month       TEXT NOT NULL,
            pnl         REAL, return_pct REAL, num_trades INTEGER, win_rate REAL,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS options_strategy_heatmap (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date    TEXT NOT NULL,
            ticker      TEXT NOT NULL,
            strategy    TEXT NOT NULL,
            regime      TEXT NOT NULL,
            avg_return  REAL, win_rate REAL, num_trades INTEGER, sharpe REAL,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    # Migrate existing tables that may be missing the new columns
    for col, col_def in [("realistic_sharpe", "REAL"),
                         ("needs_validation",  "INTEGER DEFAULT 0")]:
        try:
            conn.execute(f"ALTER TABLE backtest_master_results ADD COLUMN {col} {col_def}")
            conn.commit()
        except Exception:
            pass  # column already exists
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# Math helpers (mirror arsenal_backtest to avoid import side-effects)
# ═══════════════════════════════════════════════════════════════════════════

def _norm_cdf(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def _bs_price(S, K, T, r, sigma, opt="call") -> float:
    if T <= 0:
        return max(S - K, 0) if opt == "call" else max(K - S, 0)
    sigma = max(sigma, 0.01)
    d1 = (math.log(S / K) + (r + sigma**2 / 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if opt == "call":
        return S * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)
    return K * math.exp(-r * T) * _norm_cdf(-d2) - S * _norm_cdf(-d1)


def _bs_delta(S, K, T, r, sigma, opt="call") -> float:
    if T <= 0:
        return 1.0 if (opt == "call" and S > K) else 0.0
    sigma = max(sigma, 0.01)
    d1 = (math.log(S / K) + (r + sigma**2 / 2) * T) / (sigma * math.sqrt(T))
    return _norm_cdf(d1) if opt == "call" else _norm_cdf(d1) - 1


def _bs_theta(S, K, T, r, sigma, opt="call") -> float:
    """Daily theta decay (per share, positive = credit)."""
    if T <= 0 or sigma <= 0:
        return 0.0
    d1 = (math.log(S / K) + (r + sigma**2 / 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    pdf_d1 = math.exp(-d1**2 / 2) / math.sqrt(2 * math.pi)
    theta = (-S * pdf_d1 * sigma / (2 * math.sqrt(T)) - r * K * math.exp(-r * T) * _norm_cdf(d2)) / 365
    return abs(theta)


def _hist_vol(closes, period=20) -> float:
    arr = np.asarray(closes, dtype=float)
    if len(arr) < period + 1:
        return 0.30
    rets = np.diff(np.log(arr[-(period + 1):]))
    return float(np.std(rets) * math.sqrt(252) * 1.3)


def _iv_rank(closes, period=20) -> float:
    """Estimate IV rank (0-100) from rolling historical vol."""
    arr = np.asarray(closes, dtype=float)
    if len(arr) < 60:
        return 50.0
    hv_now = float(np.std(np.diff(np.log(arr[-21:]))) * math.sqrt(252))
    hvs = []
    for i in range(max(0, len(arr) - 252), len(arr) - 20):
        hvs.append(float(np.std(np.diff(np.log(arr[i:i + 21]))) * math.sqrt(252)))
    if not hvs or max(hvs) == min(hvs):
        return 50.0
    return float((hv_now - min(hvs)) / (max(hvs) - min(hvs)) * 100)


def _ema(data, span):
    alpha = 2 / (span + 1)
    r = [float(data[0])]
    for v in data[1:]:
        r.append(alpha * float(v) + (1 - alpha) * r[-1])
    return np.array(r)


def _rsi(c, period=14) -> float:
    if len(c) < period + 1:
        return 50.0
    d = np.diff(np.asarray(c, dtype=float))
    g = np.where(d > 0, d, 0); l = np.where(d < 0, -d, 0)
    ag = np.mean(g[-period:]); al = np.mean(l[-period:])
    return 100 - (100 / (1 + ag / al)) if al > 0 else 100.0


def _atr(h, l, c, period=14) -> float:
    if len(h) < period + 1:
        return float(np.mean(np.abs(np.diff(np.asarray(c[-period:], dtype=float))))) or float(c[-1]) * 0.02
    tr = [max(float(h[i]) - float(l[i]),
              abs(float(h[i]) - float(c[i - 1])),
              abs(float(l[i]) - float(c[i - 1]))) for i in range(-period, 0)]
    return sum(tr) / len(tr)


def _classify_regime(vix: float) -> str:
    if vix <= 18:   return "BULL"
    if vix <= 25:   return "CAUTIOUS"
    if vix <= 35:   return "BEAR"
    return "CRISIS"


# ── trade metrics helpers ────────────────────────────────────────────────────

def _trade_metrics(trades: list[dict]) -> dict:
    """Compute standard metrics from a list of trade dicts with 'pnl_pct' and 'hold_days'."""
    if not trades:
        return dict(total_return=0, win_rate=0, sharpe=0, max_drawdown=0,
                    avg_hold_hours=0, num_trades=0, profit_factor=0, calmar=0,
                    best_trade_pct=0, worst_trade_pct=0,
                    max_consec_wins=0, max_consec_losses=0)

    pcts   = [t["pnl_pct"] for t in trades]
    wins   = [p for p in pcts if p > 0]
    losses = [p for p in pcts if p <= 0]

    total_return   = float(((1 + np.array(pcts) / 100).prod() - 1) * 100)
    win_rate       = len(wins) / len(pcts) * 100
    profit_factor  = (sum(wins) / (-sum(losses))) if losses and sum(losses) != 0 else float("inf")
    avg_ret        = np.mean(pcts)
    # Floor std at 1.0% to avoid astronomical Sharpe from homogeneous option outcomes
    std_ret        = max(float(np.std(pcts)), 1.0)
    avg_hold       = max(1, float(np.mean([t.get("hold_days", 1) for t in trades])))
    sharpe         = float(avg_ret / std_ret * math.sqrt(252 / avg_hold))

    # Max drawdown on cumulative curve
    cum = np.cumprod(1 + np.array(pcts) / 100)
    roll_max = np.maximum.accumulate(cum)
    dd_series = (cum - roll_max) / (roll_max + 1e-9) * 100
    max_dd = float(np.min(dd_series))

    calmar = float(total_return / (-max_dd + 1e-9)) if max_dd < 0 else total_return

    # Consecutive wins/losses
    consec_w = consec_l = cur_w = cur_l = 0
    for p in pcts:
        if p > 0:
            cur_w += 1; cur_l = 0
            consec_w = max(consec_w, cur_w)
        else:
            cur_l += 1; cur_w = 0
            consec_l = max(consec_l, cur_l)

    avg_hold_h = float(np.mean([t.get("hold_days", 1) * 24 for t in trades]))

    needs_validation = 1 if abs(sharpe) > 5.0 else 0
    realistic_sharpe = round(max(-5.0, min(5.0, sharpe)), 3)

    return dict(
        total_return=round(total_return, 2),
        win_rate=round(win_rate, 1),
        sharpe=round(sharpe, 3),
        realistic_sharpe=realistic_sharpe,
        needs_validation=needs_validation,
        max_drawdown=round(max_dd, 2),
        avg_hold_hours=round(avg_hold_h, 1),
        num_trades=len(trades),
        profit_factor=round(min(profit_factor, 999), 3),
        calmar=round(calmar, 3),
        best_trade_pct=round(max(pcts), 2),
        worst_trade_pct=round(min(pcts), 2),
        max_consec_wins=consec_w,
        max_consec_losses=consec_l,
    )


def _monthly_breakdown(trades: list[dict]) -> dict[str, dict]:
    """Group trades by month, return {YYYY-MM: metrics}."""
    from collections import defaultdict
    by_month: dict[str, list] = defaultdict(list)
    for t in trades:
        date_str = t.get("entry_date", t.get("date", ""))[:7]  # YYYY-MM
        if date_str:
            by_month[date_str].append(t)
    result = {}
    for mo, ts in sorted(by_month.items()):
        m = _trade_metrics(ts)
        result[mo] = {
            "month": mo, "pnl": round(sum(t["pnl_pct"] for t in ts), 2),
            "return_pct": m["total_return"], "num_trades": len(ts),
            "win_rate": m["win_rate"],
        }
    return result


# ═══════════════════════════════════════════════════════════════════════════
# Data download & cache
# ═══════════════════════════════════════════════════════════════════════════

def _download_universe(days: int = 120) -> dict[str, pd.DataFrame]:
    """Bulk-download all tickers once, return dict of OHLCV DataFrames."""
    import yfinance as yf

    all_tickers = list(set(MASTER_UNIVERSE + INVERSE_ETFS + METALS_ETFS + ["^VIX", "SPY"]))
    start = (datetime.now() - timedelta(days=days + 30)).strftime("%Y-%m-%d")

    logger.info("Downloading %d tickers from %s", len(all_tickers), start)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        raw = yf.download(all_tickers, start=start, auto_adjust=True,
                          group_by="ticker", threads=True, progress=False)

    td: dict[str, pd.DataFrame] = {}
    for sym in all_tickers:
        try:
            df = raw[sym].dropna() if sym in raw.columns.get_level_values(0) else pd.DataFrame()
            if df.empty:
                # single ticker fallback with jitter to avoid rate limits
                time.sleep(0.8)
                df = yf.download(sym, start=start, auto_adjust=True, progress=False).dropna()
            if not df.empty and len(df) >= 20:
                df.index = pd.to_datetime(df.index).normalize()
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = [c[0] for c in df.columns]
                td[sym] = df
        except Exception as e:
            logger.warning("Download failed %s: %s", sym, e)
        time.sleep(0.05)

    logger.info("Downloaded %d/%d tickers", len(td), len(all_tickers))
    return td


def _get_trading_days(td: dict, days: int = 90) -> list:
    # Prefer SPY; fall back to any large-cap ticker
    for anchor in ("SPY", "QQQ", "NVDA", "AAPL", "MSFT"):
        df = td.get(anchor)
        if df is not None and len(df) > 20:
            cutoff = datetime.now() - timedelta(days=days)
            return [d for d in sorted(df.index) if d.to_pydatetime().replace(tzinfo=None) >= cutoff]
    return []


# ═══════════════════════════════════════════════════════════════════════════
# TIER 1 — Core Technical (VectorBT)
# ═══════════════════════════════════════════════════════════════════════════

_T1_PARAMS = {
    "rsi_bounce":     {"window": 14, "entry": 30, "exit": 70},
    "macd_cross":     {"fast": 12, "slow": 26, "signal": 9},
    "bollinger":      {"window": 20, "std": 2.0},
    "sma_cross":      {"fast": 20, "slow": 50},
    "sma_cross_200":  {"fast": 50, "slow": 200},
}


def _run_tier1_vbt(closes: pd.Series, ticker: str) -> list[dict]:
    """Run VectorBT strategies on a price series. Returns list of result dicts."""
    from engine.holly_nightly_backtest import _run_rsi, _run_macd, _run_bollinger, _run_sma_cross, _s, _stat, _avg_hold_days
    results = []

    def _wrap(fn, name, params):
        try:
            r = fn(closes, params)
            if r:
                r.update({"strategy": name, "ticker": ticker})
                results.append(r)
        except Exception as e:
            logger.debug("VBT %s/%s: %s", name, ticker, e)

    _wrap(_run_rsi,       "rsi_bounce",    _T1_PARAMS["rsi_bounce"])
    _wrap(_run_macd,      "macd_cross",    _T1_PARAMS["macd_cross"])
    _wrap(_run_bollinger, "bollinger",     _T1_PARAMS["bollinger"])
    _wrap(_run_sma_cross, "sma_cross",     _T1_PARAMS["sma_cross"])
    _wrap(_run_sma_cross, "sma_cross_200", _T1_PARAMS["sma_cross_200"])

    # RSI Divergence (VBT custom)
    try:
        import vectorbt as vbt
        rsi_div_entries = _rsi_divergence_signals(closes)
        rsi_val   = vbt.RSI.run(closes, window=14)
        exits_sig = rsi_val.rsi_crossed_below(70)
        pf = vbt.Portfolio.from_signals(closes, rsi_div_entries, exits_sig,
                                        freq="1D", fees=FEES, init_cash=10_000)
        st = pf.stats()
        from engine.holly_nightly_backtest import _s as _hs, _stat as _hstat
        results.append({
            "strategy": "rsi_divergence", "ticker": ticker,
            "total_return": _hstat(st, "Total Return [%]"),
            "win_rate":     _hstat(st, "Win Rate [%]"),
            "sharpe":       _hstat(st, "Sharpe Ratio", 3),
            "max_drawdown": _hstat(st, "Max Drawdown [%]"),
            "profit_factor":_hstat(st, "Profit Factor"),
            "num_trades":   int(_hs(st.get("Total Trades", 0))),
            "final_value":  round(_hs(pf.final_value()), 2),
            "avg_hold":     _avg_hold_days(pf),
        })
    except Exception as e:
        logger.debug("rsi_divergence %s: %s", ticker, e)

    # EMA Ribbon (8/13/21/34/55 all aligned)
    try:
        import vectorbt as vbt
        ema_entries, ema_exits = _ema_ribbon_signals(closes)
        pf = vbt.Portfolio.from_signals(closes, ema_entries, ema_exits,
                                        freq="1D", fees=FEES, init_cash=10_000)
        st = pf.stats()
        from engine.holly_nightly_backtest import _s as _hs, _stat as _hstat
        results.append({
            "strategy": "ema_ribbon", "ticker": ticker,
            "total_return": _hstat(st, "Total Return [%]"),
            "win_rate":     _hstat(st, "Win Rate [%]"),
            "sharpe":       _hstat(st, "Sharpe Ratio", 3),
            "max_drawdown": _hstat(st, "Max Drawdown [%]"),
            "profit_factor":_hstat(st, "Profit Factor"),
            "num_trades":   int(_hs(st.get("Total Trades", 0))),
            "final_value":  round(_hs(pf.final_value()), 2),
            "avg_hold":     _avg_hold_days(pf),
        })
    except Exception as e:
        logger.debug("ema_ribbon %s: %s", ticker, e)

    # Pullback SMA20
    try:
        import vectorbt as vbt
        pb_entries, pb_exits = _pullback_sma20_signals(closes)
        pf = vbt.Portfolio.from_signals(closes, pb_entries, pb_exits,
                                        freq="1D", fees=FEES, init_cash=10_000)
        st = pf.stats()
        from engine.holly_nightly_backtest import _s as _hs, _stat as _hstat
        results.append({
            "strategy": "pullback_sma20", "ticker": ticker,
            "total_return": _hstat(st, "Total Return [%]"),
            "win_rate":     _hstat(st, "Win Rate [%]"),
            "sharpe":       _hstat(st, "Sharpe Ratio", 3),
            "max_drawdown": _hstat(st, "Max Drawdown [%]"),
            "profit_factor":_hstat(st, "Profit Factor"),
            "num_trades":   int(_hs(st.get("Total Trades", 0))),
            "final_value":  round(_hs(pf.final_value()), 2),
            "avg_hold":     _avg_hold_days(pf),
        })
    except Exception as e:
        logger.debug("pullback_sma20 %s: %s", ticker, e)

    # Breakout Volume (needs volume — skip if not available, handled in event loop)
    return results


def _rsi_divergence_signals(closes: pd.Series) -> pd.Series:
    """Bullish RSI divergence: price lower low but RSI higher low."""
    entries = pd.Series(False, index=closes.index)
    c = closes.values
    for i in range(25, len(c)):
        window_c = c[i - 25:i]
        window_r = [_rsi(c[max(0, i - 25 + j - 14):i - 25 + j + 1]) for j in range(25)]
        recent_low_c = np.min(window_c[-5:])
        prior_low_c  = np.min(window_c[-15:-5])
        recent_rsi   = min(window_r[-5:])
        prior_rsi    = min(window_r[-15:-5])
        if recent_low_c < prior_low_c and recent_rsi > prior_rsi and recent_rsi < 40:
            entries.iloc[i] = True
    return entries


def _ema_ribbon_signals(closes: pd.Series) -> tuple[pd.Series, pd.Series]:
    """EMA ribbon: all 5 EMAs aligned bullishly → buy, bearishly → sell."""
    c = closes.values
    spans = [8, 13, 21, 34, 55]
    emas  = [_ema(c, s) for s in spans]
    entries = pd.Series(False, index=closes.index)
    exits   = pd.Series(False, index=closes.index)
    for i in range(55, len(c)):
        vals = [float(e[i]) for e in emas]
        if all(vals[j] > vals[j + 1] for j in range(len(vals) - 1)):
            entries.iloc[i] = True
        elif all(vals[j] < vals[j + 1] for j in range(len(vals) - 1)):
            exits.iloc[i] = True
    return entries, exits


def _pullback_sma20_signals(closes: pd.Series) -> tuple[pd.Series, pd.Series]:
    c = closes.values
    entries = pd.Series(False, index=closes.index)
    exits   = pd.Series(False, index=closes.index)
    for i in range(55, len(c)):
        sma20 = float(np.mean(c[i - 20:i]))
        sma50 = float(np.mean(c[i - 50:i]))
        price = float(c[i])
        prev  = float(c[i - 1])
        # Uptrend + price touching SMA20 from below
        if price > sma50 and prev <= sma20 and price > sma20:
            entries.iloc[i] = True
        # Exit when price 5% above SMA20
        if price > sma20 * 1.05:
            exits.iloc[i] = True
    return entries, exits


# ═══════════════════════════════════════════════════════════════════════════
# TIER 2 — Intraday Momentum (event-driven)
# ═══════════════════════════════════════════════════════════════════════════

def _tier2_signals(c, h, l, v, avg_v, spy_c=None) -> dict[str, bool]:
    """Return dict of tier2 strategy signals for current bar."""
    sigs = {}
    if len(c) < 22:
        return sigs

    px = float(c[-1]); prev = float(c[-2])
    # Gap & Go — proxy on daily: prev close to open gap using high
    gap_pct = (float(h[-1]) - prev) / prev * 100
    sigs["gap_and_go"]          = gap_pct >= 4.0 and float(v[-1]) > avg_v * 2

    # Momentum Breakout
    hi21 = float(np.max(h[-22:-1]))
    sigs["momentum_breakout"]   = px > hi21 and float(v[-1]) > avg_v * 1.5

    # Reversal Bounce — oversold snap-back
    rsi_now  = _rsi(c)
    rsi_prev = _rsi(c[:-1])
    sigs["reversal_bounce"]     = rsi_prev < 30 and rsi_now > 30 and px > prev

    # Volatility Breakout — ATR expansion
    atr_now  = _atr(h, l, c, 5)
    atr_norm = _atr(h, l, c, 20)
    sigs["volatility_breakout"] = atr_now > atr_norm * 1.5 and px > prev

    # VWAP Reclaim — typical price vs 5-day VWAP proxy
    tp = [(float(h[-i]) + float(l[-i]) + float(c[-i])) / 3 for i in range(5, 0, -1)]
    vwap = float(np.mean(tp))
    sigs["vwap_reclaim"]        = float(c[-2]) < vwap and px > vwap

    return sigs


# ═══════════════════════════════════════════════════════════════════════════
# TIER 3 — Holly-Style (event-driven)
# ═══════════════════════════════════════════════════════════════════════════

def _tier3_signals(c, h, l, v, avg_v) -> dict[str, bool]:
    sigs = {}
    if len(c) < 10:
        return sigs

    px   = float(c[-1]); prev = float(c[-2])
    body = abs(px - prev)
    lo   = float(l[-1]); hi  = float(h[-1])
    lower_wick = min(px, prev) - lo
    upper_wick = hi - max(px, prev)

    # Hammer Candle
    sigs["hammer_candle"]   = lower_wick >= 2 * body and px > prev and body > 0

    # Bull Trap (false breakdown: price dipped below prior low then closed above)
    lo5 = float(np.min(l[-6:-1]))
    sigs["bull_bear_trap"]  = lo < lo5 and px > lo5 and float(v[-1]) > avg_v * 1.3

    # Falling Knife — large drop + RSI oversold + today bouncing
    drop_pct = (float(c[-2]) - px) / float(c[-2]) * 100 if float(c[-2]) > 0 else 0
    sigs["falling_knife"]   = drop_pct >= 5.0 and _rsi(c) < 35 and px > float(l[-1]) * 1.005

    # AVWAP Bounce — price near 20-day VWAP then recovering
    tp20 = [(float(h[-i]) + float(l[-i]) + float(c[-i])) / 3 for i in range(20, 0, -1)]
    vwap20 = float(np.mean(tp20))
    sigs["avwap_bounce"]    = float(c[-2]) < vwap20 and px > vwap20 and float(v[-1]) > avg_v * 1.2

    # 5-Day Bounce
    down5 = all(float(c[-i]) < float(c[-i - 1]) for i in range(1, 6) if len(c) > i + 1)
    sigs["five_day_bounce"] = down5 and _rsi(c) < 35 and (px - float(l[-1])) / (float(h[-1]) - float(l[-1]) + 1e-9) > 0.5

    # Alpha Predator — 3-day price+vol acceleration
    if len(c) >= 5 and len(v) >= 5:
        p3 = (float(c[-1]) - float(c[-4])) / float(c[-4]) * 100 if float(c[-4]) > 0 else 0
        v3 = float(np.mean(v[-3:])) / (float(np.mean(v[-6:-3])) + 1e-9)
        sigs["alpha_predator"] = p3 > 5 and v3 > 2 and px < 20
    else:
        sigs["alpha_predator"] = False

    return sigs


# ═══════════════════════════════════════════════════════════════════════════
# TIER 4 — Agent-Specific (simulated proxies)
# ═══════════════════════════════════════════════════════════════════════════

def _tier4_signals(c, h, l, v, avg_v, vix: float, ticker: str, spy_c=None) -> dict[str, bool]:
    sigs = {}
    if len(c) < 22:
        return sigs

    px = float(c[-1]); prev = float(c[-2])

    # Ollie Super Trader — high-conviction breakout (5+ long signals proxy)
    t2 = _tier2_signals(c, h, l, v, avg_v, spy_c)
    t3 = _tier3_signals(c, h, l, v, avg_v)
    n_bull = sum(1 for v_ in list(t2.values()) + list(t3.values()) if v_)
    sigs["ollie_super"]    = n_bull >= 3

    # Neo — RSI oversold + volume spike (LLM proxy: oversold + vol)
    sigs["neo_plutus"]     = _rsi(c) < 35 and float(v[-1]) > avg_v * 2.5 and px > prev

    # Super Agent — small-cap, CRITICAL volume (100x+)
    sigs["super_agent"]    = float(v[-1]) > avg_v * 100 and float(c[-1]) < 20

    # Congress Copycat — 3+ convergence signals + strong price action proxy
    convergence = sum([
        px > float(np.mean(c[-50:])),      # above SMA50
        float(v[-1]) > avg_v * 1.5,        # elevated volume
        _rsi(c) > 50,                       # momentum
    ])
    sigs["congress_copycat"] = convergence >= 3 and vix < 25

    # Dalio Metals — triggered externally (handled separately for GLD/SLV/CPER)
    sigs["dalio_metals"]   = False  # set by _run_dalio_metals()

    # DayBlade 0DTE — SPY/QQQ only, proxy
    sigs["dayblade_0dte"]  = ticker in ("SPY", "QQQ", "TQQQ") and abs(px - prev) / prev > 0.005

    return sigs


def _run_dalio_metals(td: dict, days: list) -> list[dict]:
    """Dalio Metals: buy GLD/SLV/CPER when RSI<40 and price > SMA50."""
    trades = []
    for sym in ["GLD", "SLV", "CPER"]:
        df = td.get(sym)
        if df is None or len(df) < 60:
            continue
        positions = []
        for day in days:
            m = df.index <= day
            if m.sum() < 55:
                continue
            sub = df.loc[m]
            c = sub["Close"].values
            rsi_val = _rsi(c)
            sma50   = float(np.mean(c[-50:]))
            px      = float(c[-1])
            prev    = float(c[-2]) if len(c) > 1 else px

            # Manage open
            still = []
            for pos in positions:
                gain = (px - pos["entry"]) / pos["entry"]
                if gain >= 0.05 or gain <= -0.03 or pos.get("days_held", 0) >= 30:
                    pnl_pct = gain * 100 - SLIPPAGE * 200
                    trades.append({"strategy": "dalio_metals", "ticker": sym,
                                   "entry_date": pos["entry_date"],
                                   "exit_date": day.strftime("%Y-%m-%d"),
                                   "pnl_pct": round(pnl_pct, 2),
                                   "hold_days": pos.get("days_held", 1),
                                   "pnl": round(pnl_pct, 2)})
                else:
                    pos["days_held"] = pos.get("days_held", 0) + 1
                    still.append(pos)
            positions = still

            # New entry
            if rsi_val < 40 and px > sma50 and not positions:
                positions.append({"entry": px * (1 + SLIPPAGE + EXEC_DELAY),
                                  "entry_date": day.strftime("%Y-%m-%d"),
                                  "days_held": 0})

        # Close remaining
        for pos in positions:
            px_last = float(df["Close"].iloc[-1])
            pnl_pct = (px_last - pos["entry"]) / pos["entry"] * 100 - SLIPPAGE * 200
            trades.append({"strategy": "dalio_metals", "ticker": sym,
                           "entry_date": pos["entry_date"],
                           "exit_date": days[-1].strftime("%Y-%m-%d") if days else "EOP",
                           "pnl_pct": round(pnl_pct, 2), "hold_days": pos.get("days_held", 1),
                           "pnl": round(pnl_pct, 2)})
    return trades


# ═══════════════════════════════════════════════════════════════════════════
# Options simulation helpers (adapted from arsenal_backtest.py)
# ═══════════════════════════════════════════════════════════════════════════

def _opt_slip(premium: float, legs: int = 1) -> float:
    """Options round-trip slippage: 3% of premium per leg + $0.65/contract per leg."""
    return premium * OPT_SLIP_PER_LEG * legs + legs * OPT_COST / 100


def _sim_long_call(future_df, entry_px, iv, dte) -> dict | None:
    strike  = round(entry_px, 0)
    T       = dte / 365
    fair    = _bs_price(entry_px, strike, T, RISK_FREE, iv, "call")
    if fair <= 0:
        return None
    # Pay ask (fair + half-spread) + exec delay penalty + commission
    premium = fair * (1 + OPT_SLIP_PER_LEG / 2) + OPT_COST / 100
    delta   = _bs_delta(entry_px, strike, T, RISK_FREE, iv, "call")
    theta   = _bs_theta(entry_px, strike, T, RISK_FREE, iv, "call")
    for i in range(min(dte - 5, len(future_df))):
        px  = float(future_df["Close"].iloc[i])
        rem = max(1, dte - i - 1) / 365
        cur = _bs_price(px, strike, rem, RISK_FREE, iv * 0.95, "call")
        # Sell at bid (fair - half-spread) at exit
        exit_val = cur * (1 - OPT_SLIP_PER_LEG / 2) - OPT_COST / 100
        pnl_pct  = (exit_val - premium) / premium * 100
        if pnl_pct >= 100:
            return {"pnl": exit_val - premium, "pnl_pct": 100, "exit_type": "TARGET",
                    "days": i + 1, "delta": delta, "theta": theta,
                    "iv_entry": iv, "iv_exit": iv * 0.95, "premium": premium}
        if pnl_pct <= -50:
            return {"pnl": exit_val - premium, "pnl_pct": -50, "exit_type": "STOP",
                    "days": i + 1, "delta": delta, "theta": theta,
                    "iv_entry": iv, "iv_exit": iv * 0.95, "premium": premium}
    final_px  = float(future_df["Close"].iloc[-1]) if len(future_df) > 0 else entry_px
    final_v   = _bs_price(final_px, strike, 21 / 365, RISK_FREE, iv * 0.90, "call")
    exit_val  = final_v * (1 - OPT_SLIP_PER_LEG / 2) - OPT_COST / 100
    return {"pnl": exit_val - premium, "pnl_pct": (exit_val - premium) / premium * 100,
            "exit_type": "21DTE", "days": dte - 21, "delta": delta, "theta": theta,
            "iv_entry": iv, "iv_exit": iv * 0.90, "premium": premium}


def _sim_long_put(future_df, entry_px, iv, dte) -> dict | None:
    strike  = round(entry_px, 0)
    T       = dte / 365
    fair    = _bs_price(entry_px, strike, T, RISK_FREE, iv, "put")
    if fair <= 0:
        return None
    premium = fair * (1 + OPT_SLIP_PER_LEG / 2) + OPT_COST / 100
    delta   = _bs_delta(entry_px, strike, T, RISK_FREE, iv, "put")
    theta   = _bs_theta(entry_px, strike, T, RISK_FREE, iv, "put")
    for i in range(min(dte - 5, len(future_df))):
        px  = float(future_df["Close"].iloc[i])
        rem = max(1, dte - i - 1) / 365
        cur = _bs_price(px, strike, rem, RISK_FREE, iv * 0.95, "put")
        exit_val = cur * (1 - OPT_SLIP_PER_LEG / 2) - OPT_COST / 100
        pnl_pct  = (exit_val - premium) / premium * 100
        if pnl_pct >= 100:
            return {"pnl": exit_val - premium, "pnl_pct": 100, "exit_type": "TARGET",
                    "days": i + 1, "delta": delta, "theta": theta,
                    "iv_entry": iv, "iv_exit": iv * 0.95, "premium": premium}
        if pnl_pct <= -50:
            return {"pnl": exit_val - premium, "pnl_pct": -50, "exit_type": "STOP",
                    "days": i + 1, "delta": delta, "theta": theta,
                    "iv_entry": iv, "iv_exit": iv * 0.95, "premium": premium}
    final_px = float(future_df["Close"].iloc[-1]) if len(future_df) > 0 else entry_px
    final_v  = _bs_price(final_px, strike, 21 / 365, RISK_FREE, iv * 0.90, "put")
    exit_val = final_v * (1 - OPT_SLIP_PER_LEG / 2) - OPT_COST / 100
    return {"pnl": exit_val - premium, "pnl_pct": (exit_val - premium) / premium * 100,
            "exit_type": "21DTE", "days": dte - 21, "delta": delta, "theta": theta,
            "iv_entry": iv, "iv_exit": iv * 0.90, "premium": premium}


def _sim_csp(future_df, entry_px, iv, dte) -> dict | None:
    """Cash-Secured Put: sell 30-delta OTM put. Realistic: receive bid not mid."""
    otm_pct = iv * math.sqrt(dte / 365) * 0.5
    strike  = round(entry_px * (1 - otm_pct), 0)
    T       = dte / 365
    fair    = _bs_price(entry_px, strike, T, RISK_FREE, iv, "put")
    # Short options: sell at bid (fair - half-spread) minus commission
    credit  = fair * (1 - OPT_SLIP_PER_LEG / 2) - OPT_COST / 100
    if credit <= 0:
        return None
    delta   = abs(_bs_delta(entry_px, strike, T, RISK_FREE, iv, "put"))
    theta   = _bs_theta(entry_px, strike, T, RISK_FREE, iv, "put")
    pop     = 1 - delta  # rough probability of profit
    for i in range(min(dte, len(future_df))):
        px  = float(future_df["Close"].iloc[i])
        rem = max(1, dte - i - 1) / 365
        cur = _bs_price(px, strike, rem, RISK_FREE, iv * 0.85, "put")
        pnl = credit - cur
        if pnl >= credit * 0.5:
            return {"pnl": pnl, "pnl_pct": pnl / (strike * 0.10) * 100,
                    "credit": credit, "exit_type": "PROFIT_50", "days": i + 1,
                    "delta": delta, "theta": theta, "iv_entry": iv, "iv_exit": iv * 0.85,
                    "assignment": 0, "pop": pop, "premium": credit}
        if px < strike * 0.97:  # deep ITM → assign risk
            return {"pnl": -abs(strike - px) + credit,
                    "pnl_pct": (-abs(strike - px) + credit) / (strike * 0.10) * 100,
                    "credit": credit, "exit_type": "ASSIGNED", "days": i + 1,
                    "delta": delta, "theta": theta, "iv_entry": iv, "iv_exit": iv,
                    "assignment": 1, "pop": pop, "premium": credit}
    # Expiry
    final_px = float(future_df["Close"].iloc[-1]) if len(future_df) > 0 else entry_px
    win = final_px >= strike
    return {"pnl": credit if win else -(strike - final_px) + credit,
            "pnl_pct": credit / (strike * 0.10) * 100 if win else (-(strike - final_px) + credit) / (strike * 0.10) * 100,
            "credit": credit, "exit_type": "EXPIRED_WIN" if win else "EXPIRED_LOSS",
            "days": dte, "delta": delta, "theta": theta,
            "iv_entry": iv, "iv_exit": iv * 0.80, "assignment": 0 if win else 1,
            "pop": pop, "premium": credit}


def _sim_covered_call(future_df, entry_px, iv, dte) -> dict | None:
    """Covered Call: sell 30-delta OTM call. Receive bid minus commission."""
    otm_pct = iv * math.sqrt(dte / 365) * 0.5
    strike  = round(entry_px * (1 + otm_pct), 0)
    T       = dte / 365
    fair    = _bs_price(entry_px, strike, T, RISK_FREE, iv, "call")
    credit  = fair * (1 - OPT_SLIP_PER_LEG / 2) - OPT_COST / 100
    if credit <= 0:
        return None
    delta = _bs_delta(entry_px, strike, T, RISK_FREE, iv, "call")
    theta = _bs_theta(entry_px, strike, T, RISK_FREE, iv, "call")
    pop   = 1 - delta
    for i in range(min(dte, len(future_df))):
        px  = float(future_df["Close"].iloc[i])
        rem = max(1, dte - i - 1) / 365
        cur = _bs_price(px, strike, rem, RISK_FREE, iv * 0.85, "call")
        option_pnl = credit - cur
        if option_pnl >= credit * 0.5:
            # Close option at 50% profit; close stock position at current price.
            # Total P&L = option gain + stock gain (position notional = entry_px).
            stock_pnl = px - entry_px
            total_pnl = option_pnl + stock_pnl
            return {"pnl": total_pnl, "pnl_pct": total_pnl / entry_px * 100,
                    "credit": credit, "exit_type": "PROFIT_50", "days": i + 1,
                    "delta": delta, "theta": theta, "iv_entry": iv, "iv_exit": iv * 0.85,
                    "assignment": 0, "pop": pop, "premium": credit}
    final_px = float(future_df["Close"].iloc[-1]) if len(future_df) > 0 else entry_px
    win = final_px <= strike
    if win:
        # Option expires worthless; sell stock at final_px.
        total_pnl = credit + (final_px - entry_px)
    else:
        # Stock called away at strike; receive strike price for shares.
        # Upside is capped at strike, not final_px — covered call, not naked.
        total_pnl = credit + (strike - entry_px)
    return {"pnl": total_pnl, "pnl_pct": total_pnl / entry_px * 100,
            "credit": credit, "exit_type": "EXPIRED_WIN" if win else "CALLED_AWAY",
            "days": dte, "delta": delta, "theta": theta,
            "iv_entry": iv, "iv_exit": iv * 0.80,
            "assignment": 1 if not win else 0, "pop": pop, "premium": credit}


def _sim_bull_call_spread(future_df, entry_px, iv, dte) -> dict | None:
    """Bull Call Spread: buy ATM call, sell OTM call (5%). 2-leg: 6% round-trip slippage."""
    K_long  = round(entry_px, 0)
    K_short = round(entry_px * 1.05, 0)
    T = dte / 365
    # Buy long leg at ask, sell short leg at bid → net debit inflated by 2-leg slippage
    long_ask  = _bs_price(entry_px, K_long,  T, RISK_FREE, iv, "call") * (1 + OPT_SLIP_PER_LEG / 2)
    short_bid = _bs_price(entry_px, K_short, T, RISK_FREE, iv, "call") * (1 - OPT_SLIP_PER_LEG / 2)
    debit = long_ask - short_bid + 2 * OPT_COST / 100
    if debit <= 0:
        return None
    max_profit = (K_short - K_long) - debit
    for i in range(min(dte, len(future_df))):
        px  = float(future_df["Close"].iloc[i])
        rem = max(1, dte - i - 1) / 365
        # Exit: sell long at bid, buy short at ask
        long_bid2  = _bs_price(px, K_long,  rem, RISK_FREE, iv * 0.95, "call") * (1 - OPT_SLIP_PER_LEG / 2)
        short_ask2 = _bs_price(px, K_short, rem, RISK_FREE, iv * 0.95, "call") * (1 + OPT_SLIP_PER_LEG / 2)
        cur = long_bid2 - short_ask2 - 2 * OPT_COST / 100
        pnl = cur - debit
        if pnl >= max_profit * 0.75:
            return {"pnl": pnl, "pnl_pct": pnl / debit * 100, "credit": -debit,
                    "max_profit": max_profit, "max_loss": debit,
                    "pop": 0.45, "exit_type": "PROFIT_75", "days": i + 1}
        if pnl <= -debit * 0.5:
            return {"pnl": -debit * 0.5, "pnl_pct": -50, "credit": -debit,
                    "max_profit": max_profit, "max_loss": debit,
                    "pop": 0.45, "exit_type": "STOP_50", "days": i + 1}
    final_px = float(future_df["Close"].iloc[-1]) if len(future_df) > 0 else entry_px
    itm = max(0, final_px - K_long) - max(0, final_px - K_short)
    return {"pnl": itm - debit, "pnl_pct": (itm - debit) / debit * 100,
            "credit": -debit, "max_profit": max_profit, "max_loss": debit,
            "pop": 0.45, "exit_type": "EXPIRED", "days": dte}


def _sim_bear_put_spread(future_df, entry_px, iv, dte) -> dict | None:
    """Bear Put Spread: buy ATM put, sell OTM put (5% below). 2-leg: 6% slippage."""
    K_long  = round(entry_px, 0)
    K_short = round(entry_px * 0.95, 0)
    T = dte / 365
    long_ask  = _bs_price(entry_px, K_long,  T, RISK_FREE, iv, "put") * (1 + OPT_SLIP_PER_LEG / 2)
    short_bid = _bs_price(entry_px, K_short, T, RISK_FREE, iv, "put") * (1 - OPT_SLIP_PER_LEG / 2)
    debit = long_ask - short_bid + 2 * OPT_COST / 100
    if debit <= 0:
        return None
    max_profit = (K_long - K_short) - debit
    for i in range(min(dte, len(future_df))):
        px  = float(future_df["Close"].iloc[i])
        rem = max(1, dte - i - 1) / 365
        long_bid2  = _bs_price(px, K_long,  rem, RISK_FREE, iv * 0.95, "put") * (1 - OPT_SLIP_PER_LEG / 2)
        short_ask2 = _bs_price(px, K_short, rem, RISK_FREE, iv * 0.95, "put") * (1 + OPT_SLIP_PER_LEG / 2)
        cur = long_bid2 - short_ask2 - 2 * OPT_COST / 100
        pnl = cur - debit
        if pnl >= max_profit * 0.75:
            return {"pnl": pnl, "pnl_pct": pnl / debit * 100, "credit": -debit,
                    "max_profit": max_profit, "max_loss": debit,
                    "pop": 0.45, "exit_type": "PROFIT_75", "days": i + 1}
        if pnl <= -debit * 0.5:
            return {"pnl": -debit * 0.5, "pnl_pct": -50, "credit": -debit,
                    "max_profit": max_profit, "max_loss": debit,
                    "pop": 0.45, "exit_type": "STOP_50", "days": i + 1}
    final_px = float(future_df["Close"].iloc[-1]) if len(future_df) > 0 else entry_px
    itm = max(0, K_long - final_px) - max(0, K_short - final_px)
    return {"pnl": itm - debit, "pnl_pct": (itm - debit) / debit * 100,
            "credit": -debit, "max_profit": max_profit, "max_loss": debit,
            "pop": 0.45, "exit_type": "EXPIRED", "days": dte}


def _sim_bull_put_spread(future_df, entry_px, iv, dte) -> dict | None:
    """Bull Put Spread (credit): sell 3% OTM put, buy 8% OTM put. 2-leg: 6% slippage."""
    ps = round(entry_px * 0.97); pb = round(entry_px * 0.92)
    T  = dte / 365
    # Sell short leg at bid, buy long leg at ask
    short_bid = _bs_price(entry_px, ps, T, RISK_FREE, iv, "put") * (1 - OPT_SLIP_PER_LEG / 2)
    long_ask  = _bs_price(entry_px, pb, T, RISK_FREE, iv, "put") * (1 + OPT_SLIP_PER_LEG / 2)
    credit = short_bid - long_ask - 2 * OPT_COST / 100
    if credit <= 0:
        return None
    width = ps - pb; max_loss = width * 0.05 - credit
    if max_loss <= 0: max_loss = credit
    for i in range(min(dte, len(future_df))):
        px  = float(future_df["Close"].iloc[i])
        rem = max(1, dte - i - 1) / 365
        cur_s = _bs_price(px, ps, rem, RISK_FREE, iv * 0.90, "put") * (1 + OPT_SLIP_PER_LEG / 2)
        cur_l = _bs_price(px, pb, rem, RISK_FREE, iv * 0.90, "put") * (1 - OPT_SLIP_PER_LEG / 2)
        pnl   = credit - (cur_s - cur_l)
        if pnl >= credit * 0.5:
            return {"pnl": pnl, "pnl_pct": pnl / (width * 0.05) * 100, "credit": credit,
                    "max_profit": credit, "max_loss": max_loss,
                    "pop": 0.70, "exit_type": "PROFIT_50", "days": i + 1}
        if pnl <= -max_loss:
            return {"pnl": -max_loss, "pnl_pct": -max_loss / (width * 0.05) * 100, "credit": credit,
                    "max_profit": credit, "max_loss": max_loss,
                    "pop": 0.70, "exit_type": "MAX_LOSS", "days": i + 1}
    final_px = float(future_df["Close"].iloc[-1]) if len(future_df) > 0 else entry_px
    win = final_px > ps
    return {"pnl": credit if win else -max_loss * 0.5,
            "pnl_pct": (credit if win else -max_loss * 0.5) / (width * 0.05) * 100,
            "credit": credit, "max_profit": credit, "max_loss": max_loss,
            "pop": 0.70, "exit_type": "EXPIRED_WIN" if win else "EXPIRED_LOSS", "days": dte}


def _sim_bear_call_spread(future_df, entry_px, iv, dte) -> dict | None:
    """Bear Call Spread (credit): sell 3% OTM call, buy 8% OTM call. 2-leg: 6% slippage."""
    cs = round(entry_px * 1.03); cb = round(entry_px * 1.08)
    T  = dte / 365
    short_bid = _bs_price(entry_px, cs, T, RISK_FREE, iv, "call") * (1 - OPT_SLIP_PER_LEG / 2)
    long_ask  = _bs_price(entry_px, cb, T, RISK_FREE, iv, "call") * (1 + OPT_SLIP_PER_LEG / 2)
    credit = short_bid - long_ask - 2 * OPT_COST / 100
    if credit <= 0:
        return None
    width = cb - cs; max_loss = width * 0.05 - credit
    if max_loss <= 0: max_loss = credit
    for i in range(min(dte, len(future_df))):
        px  = float(future_df["Close"].iloc[i])
        rem = max(1, dte - i - 1) / 365
        cur_s = _bs_price(px, cs, rem, RISK_FREE, iv * 0.90, "call") * (1 + OPT_SLIP_PER_LEG / 2)
        cur_l = _bs_price(px, cb, rem, RISK_FREE, iv * 0.90, "call") * (1 - OPT_SLIP_PER_LEG / 2)
        pnl   = credit - (cur_s - cur_l)
        if pnl >= credit * 0.5:
            return {"pnl": pnl, "pnl_pct": pnl / (width * 0.05) * 100, "credit": credit,
                    "max_profit": credit, "max_loss": max_loss,
                    "pop": 0.70, "exit_type": "PROFIT_50", "days": i + 1}
        if pnl <= -max_loss:
            return {"pnl": -max_loss, "pnl_pct": -max_loss / (width * 0.05) * 100, "credit": credit,
                    "max_profit": credit, "max_loss": max_loss,
                    "pop": 0.70, "exit_type": "MAX_LOSS", "days": i + 1}
    final_px = float(future_df["Close"].iloc[-1]) if len(future_df) > 0 else entry_px
    win = final_px < cs
    return {"pnl": credit if win else -max_loss * 0.5,
            "pnl_pct": (credit if win else -max_loss * 0.5) / (width * 0.05) * 100,
            "credit": credit, "max_profit": credit, "max_loss": max_loss,
            "pop": 0.70, "exit_type": "EXPIRED_WIN" if win else "EXPIRED_LOSS", "days": dte}


def _sim_ic(future_df, entry_px, iv, dte) -> dict | None:
    """Standard Iron Condor: ±5% short strikes, ±10% wings. 4-leg: 12% slippage."""
    ps = round(entry_px * 0.95); pb = round(entry_px * 0.90)
    cs = round(entry_px * 1.05); cb = round(entry_px * 1.10)
    T  = dte / 365
    # Sell inner legs at bid, buy outer legs at ask
    put_short_bid  = _bs_price(entry_px, ps, T, RISK_FREE, iv, "put")  * (1 - OPT_SLIP_PER_LEG / 2)
    put_long_ask   = _bs_price(entry_px, pb, T, RISK_FREE, iv, "put")  * (1 + OPT_SLIP_PER_LEG / 2)
    call_short_bid = _bs_price(entry_px, cs, T, RISK_FREE, iv, "call") * (1 - OPT_SLIP_PER_LEG / 2)
    call_long_ask  = _bs_price(entry_px, cb, T, RISK_FREE, iv, "call") * (1 + OPT_SLIP_PER_LEG / 2)
    credit = (put_short_bid - put_long_ask + call_short_bid - call_long_ask) - 4 * OPT_COST / 100
    if credit <= 0:
        return None
    max_loss = (cs - ps) * 0.08 + credit  # width × buffer
    for i in range(min(dte, len(future_df))):
        px  = float(future_df["Close"].iloc[i])
        rem = max(1, dte - i - 1) / 365
        cur = ((_bs_price(px, ps, rem, RISK_FREE, iv, "put")  * (1 + OPT_SLIP_PER_LEG / 2)
                - _bs_price(px, pb, rem, RISK_FREE, iv, "put")  * (1 - OPT_SLIP_PER_LEG / 2))
               + (_bs_price(px, cs, rem, RISK_FREE, iv, "call") * (1 + OPT_SLIP_PER_LEG / 2)
                  - _bs_price(px, cb, rem, RISK_FREE, iv, "call") * (1 - OPT_SLIP_PER_LEG / 2)))
        pnl = credit - cur
        if pnl >= credit * 0.5:
            return {"pnl": pnl, "credit": credit, "max_profit": credit,
                    "max_loss": max_loss, "pop": 0.68, "exit_type": "PROFIT_50", "days": i + 1}
        if pnl <= -max_loss:
            return {"pnl": -max_loss, "credit": credit, "max_profit": credit,
                    "max_loss": max_loss, "pop": 0.68, "exit_type": "MAX_LOSS", "days": i + 1}
    final_px = float(future_df["Close"].iloc[-1]) if len(future_df) > 0 else entry_px
    win = ps < final_px < cs
    return {"pnl": credit if win else -max_loss * 0.5, "credit": credit,
            "max_profit": credit, "max_loss": max_loss, "pop": 0.68,
            "exit_type": "EXPIRED_WIN" if win else "EXPIRED_LOSS", "days": dte}


def _sim_broken_wing_ic(future_df, entry_px, iv, dte) -> dict | None:
    """Broken Wing IC: wider put side (directional lean bullish)."""
    ps = round(entry_px * 0.95); pb = round(entry_px * 0.88)  # wider put wing
    cs = round(entry_px * 1.05); cb = round(entry_px * 1.10)
    T  = dte / 365
    credit = (_bs_price(entry_px, ps, T, RISK_FREE, iv, "put")
              - _bs_price(entry_px, pb, T, RISK_FREE, iv, "put")
              + _bs_price(entry_px, cs, T, RISK_FREE, iv, "call")
              - _bs_price(entry_px, cb, T, RISK_FREE, iv, "call"))
    if credit <= 0:
        return None
    max_loss = (cs - ps) * 0.05
    for i in range(min(dte, len(future_df))):
        px  = float(future_df["Close"].iloc[i])
        rem = max(1, dte - i - 1) / 365
        cur = (_bs_price(px, ps, rem, RISK_FREE, iv, "put") - _bs_price(px, pb, rem, RISK_FREE, iv, "put")
               + _bs_price(px, cs, rem, RISK_FREE, iv, "call") - _bs_price(px, cb, rem, RISK_FREE, iv, "call"))
        pnl = credit - cur
        if pnl >= credit * 0.50:
            return {"pnl": pnl, "credit": credit, "max_profit": credit,
                    "max_loss": max_loss, "pop": 0.68, "exit_type": "PROFIT_50", "days": i + 1}
        if pnl <= -max_loss:
            return {"pnl": -max_loss, "credit": credit, "max_profit": credit,
                    "max_loss": max_loss, "pop": 0.68, "exit_type": "MAX_LOSS", "days": i + 1}
    final_px = float(future_df["Close"].iloc[-1]) if len(future_df) > 0 else entry_px
    win = ps < final_px < cs
    return {"pnl": credit if win else -max_loss * 0.5, "credit": credit,
            "max_profit": credit, "max_loss": max_loss, "pop": 0.68,
            "exit_type": "EXPIRED_WIN" if win else "EXPIRED_LOSS", "days": dte}


def _sim_0dte(future_row, entry_px, iv, direction) -> dict | None:
    """0DTE simulation: use intraday high/low as proxy for outcome."""
    if future_row is None:
        return None
    day_high  = float(future_row.get("High", entry_px * 1.01))
    day_low   = float(future_row.get("Low",  entry_px * 0.99))
    day_close = float(future_row.get("Close", entry_px))

    # ATM 0DTE premium ≈ 0.5 × IV × price × sqrt(1/252)
    premium = max(0.01, 0.5 * iv * entry_px * math.sqrt(1 / 252)) + OPT_COST / 100

    if direction == "call":
        intrinsic = max(0, day_high - entry_px) * 0.7  # intraday capture
        pnl       = intrinsic - premium
    else:
        intrinsic = max(0, entry_px - day_low) * 0.7
        pnl       = intrinsic - premium

    return {"pnl": round(pnl, 4), "pnl_pct": round(pnl / premium * 100, 1),
            "premium": round(premium, 4), "days": 1,
            "exit_type": "0DTE_EXPIRY",
            "iv_entry": iv, "iv_exit": iv * 0.5}


# ═══════════════════════════════════════════════════════════════════════════
# TIER 9 — Short Strategies
# ═══════════════════════════════════════════════════════════════════════════

def _tier9_short_signals(c, h, l, v, avg_v) -> dict[str, bool]:
    sigs = {}
    if len(c) < 55:
        return sigs
    # Death cross + downtrend
    sma50  = float(np.mean(c[-50:]))
    sma200 = float(np.mean(c[-200:])) if len(c) >= 200 else sma50
    ema8   = float(_ema(c, 8)[-1])
    death  = sma50 < sma200 and float(np.mean(c[-51:-1])) >= float(np.mean(c[-200:-1]))
    sigs["short_equity"]  = death and _rsi(c) > 60 and float(v[-1]) > avg_v * 1.5
    sigs["inverse_etf"]   = sma50 < sma200 * 0.98  # regime shift
    # Short put debit spread: OTM bearish signal
    sigs["short_put_spread"] = _rsi(c) < 30 and float(c[-1]) < sma50
    return sigs


# ═══════════════════════════════════════════════════════════════════════════
# Main event-driven loop (Tiers 2–4, 9)
# ═══════════════════════════════════════════════════════════════════════════

def _run_event_loop(td: dict, days: list, vix_map: dict) -> dict[str, list]:
    """Runs a daily scan loop for Tiers 2-4 and Tier 9. Returns dict of strategy → trades."""
    from collections import defaultdict
    strategy_trades: dict[str, list] = defaultdict(list)
    open_positions: dict[str, list]  = defaultdict(list)  # strategy → [{ticker,entry,...}]

    MAX_OPEN = 5
    HOLD_MAX = 20   # max hold days
    TARGET   = 0.07  # 7% target
    STOP     = 0.035 # 3.5% stop

    spy_closes = td.get("SPY", pd.DataFrame()).get("Close", pd.Series())

    for day in days:
        day_str = day.strftime("%Y-%m-%d")
        vix_val = vix_map.get(day, 18.0)
        regime  = _classify_regime(vix_val)

        # Get SPY closes up to this day
        spy_c = spy_closes[spy_closes.index <= day].values if len(spy_closes) > 0 else None

        # ── Manage open positions ──
        for strat in list(open_positions.keys()):
            still = []
            for pos in open_positions[strat]:
                sym = pos["ticker"]
                df  = td.get(sym)
                if df is None:
                    still.append(pos)
                    continue
                m   = df.index <= day
                if m.sum() == 0:
                    still.append(pos)
                    continue
                px  = float(df.loc[m, "Close"].iloc[-1])
                hi  = float(df.loc[m, "High"].iloc[-1])
                pos["days_held"] = pos.get("days_held", 0) + 1
                gain = (px - pos["entry"]) / pos["entry"]

                exit_reason = None
                if px >= pos["target"]:
                    exit_reason = "TARGET"
                elif px <= pos["stop"]:
                    exit_reason = "STOP"
                elif pos["days_held"] >= HOLD_MAX:
                    exit_reason = "MAX_HOLD"

                if exit_reason:
                    exit_px  = px * (1 - SLIPPAGE)
                    pnl_pct  = (exit_px - pos["entry"]) / pos["entry"] * 100
                    strategy_trades[strat].append({
                        "strategy":   strat, "ticker": sym,
                        "entry_date": pos["entry_date"], "exit_date": day_str,
                        "pnl_pct":    round(pnl_pct, 2),
                        "pnl":        round(pnl_pct, 2),
                        "hold_days":  pos["days_held"],
                        "exit_type":  exit_reason,
                        "regime":     pos.get("regime", regime),
                    })
                else:
                    still.append(pos)
            open_positions[strat] = still

        # ── Scan for new signals ──
        for sym, df in td.items():
            if sym in ("^VIX", "SPY") and sym not in ("SPY",):
                continue
            m = df.index <= day
            if m.sum() < 55:
                continue
            sub   = df.loc[m]
            c     = sub["Close"].values
            h     = sub["High"].values
            l     = sub["Low"].values
            v     = sub["Volume"].values if "Volume" in sub.columns else np.ones(len(c))
            avg_v = float(np.mean(v[-20:])) if len(v) >= 20 else 1.0
            px    = float(c[-1])
            atr   = _atr(h, l, c)

            t2_sigs = _tier2_signals(c, h, l, v, avg_v, spy_c)
            t3_sigs = _tier3_signals(c, h, l, v, avg_v)
            t4_sigs = _tier4_signals(c, h, l, v, avg_v, vix_val, sym, spy_c)
            t9_sigs = _tier9_short_signals(c, h, l, v, avg_v)

            all_sigs = {**t2_sigs, **t3_sigs, **t4_sigs, **t9_sigs}

            for strat, fired in all_sigs.items():
                if not fired:
                    continue
                held = [p["ticker"] for p in open_positions[strat]]
                if sym in held or len(open_positions[strat]) >= MAX_OPEN:
                    continue

                # Slippage (0.1%) + execution delay penalty (0.15%) on buys
                fill_cost = SLIPPAGE + EXEC_DELAY
                entry_px  = px * (1 + fill_cost)
                is_short  = strat in ("short_equity", "inverse_etf", "short_put_spread")
                target    = entry_px * (1 - TARGET) if is_short else entry_px * (1 + TARGET)
                stop      = entry_px * (1 + STOP)   if is_short else entry_px * (1 - STOP)

                # Inverse ETF: trade SQQQ/SPXS instead of shorting the underlying
                if strat == "inverse_etf":
                    inv_sym = "SQQQ" if sym in ("QQQ", "TQQQ", "NVDA", "AMD", "META") else "SPXS"
                    if inv_sym in td:
                        sym = inv_sym

                open_positions[strat].append({
                    "ticker":     sym, "entry": entry_px,
                    "entry_date": day_str, "target": target, "stop": stop,
                    "days_held":  0, "regime": regime,
                })

    # Close all remaining at end
    last_day = days[-1].strftime("%Y-%m-%d") if days else "EOP"
    for strat, positions in open_positions.items():
        for pos in positions:
            df = td.get(pos["ticker"])
            if df is not None and len(df) > 0:
                px = float(df["Close"].iloc[-1]) * (1 - SLIPPAGE)
                pnl_pct = (px - pos["entry"]) / pos["entry"] * 100
                strategy_trades[strat].append({
                    "strategy":   strat, "ticker": pos["ticker"],
                    "entry_date": pos["entry_date"], "exit_date": last_day,
                    "pnl_pct":    round(pnl_pct, 2), "pnl": round(pnl_pct, 2),
                    "hold_days":  pos.get("days_held", 1), "exit_type": "EOP",
                    "regime":     pos.get("regime", "UNKNOWN"),
                })

    return dict(strategy_trades)


def _exit_date_str(future_df, days: int, fallback: str) -> str:
    """Return the actual exit date from the future price slice + days held."""
    if future_df is None or len(future_df) == 0:
        return fallback
    idx = min(max(days - 1, 0), len(future_df) - 1)
    return future_df.index[idx].strftime("%Y-%m-%d")


# ═══════════════════════════════════════════════════════════════════════════
# Options loop (Tiers 5–8)
# ═══════════════════════════════════════════════════════════════════════════

def _run_options_loop(td: dict, days: list, vix_map: dict) -> tuple[list, list, list]:
    """Returns (options_trades, spread_trades, dte0_trades)."""
    options_trades = []; spread_trades = []; dte0_trades = []

    SCAN_FREQ = 5   # fire options entries every 5 trading days to avoid over-trading
    day_counter = 0

    for day in days:
        day_counter += 1
        if day_counter % SCAN_FREQ != 0:
            continue

        day_str = day.strftime("%Y-%m-%d")
        vix_val = vix_map.get(day, 18.0)
        regime  = _classify_regime(vix_val)

        for sym in MASTER_UNIVERSE:
            df = td.get(sym)
            if df is None or len(df) < 30:
                continue
            m     = df.index <= day
            sub   = df.loc[m]
            c     = sub["Close"].values
            h     = sub["High"].values
            l     = sub["Low"].values
            v     = sub["Volume"].values if "Volume" in sub.columns else np.ones(len(c))
            avg_v = float(np.mean(v[-20:])) if len(v) >= 20 else 1.0
            px    = float(c[-1])
            iv    = _hist_vol(c)
            ivr   = _iv_rank(c)

            future = df.loc[df.index > day]
            if len(future) < 5:
                continue

            t2   = _tier2_signals(c, h, l, v, avg_v)
            t3   = _tier3_signals(c, h, l, v, avg_v)
            bull = sum(1 for v_ in list(t2.values()) + list(t3.values()) if v_)
            bear = sum(1 for v_ in _tier9_short_signals(c, h, l, v, avg_v).values() if v_)

            # ── Tier 5: Single-leg options ──
            # Long Call (bullish, low IV preferred)
            if bull >= 2 and ivr < 60:
                r = _sim_long_call(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    options_trades.append({**r, "strategy": "long_call", "ticker": sym,
                                           "entry_date": day_str, "option_type": "call",
                                           "exit_date": _exit_date_str(future, r["days"], day_str),
                                           "strike": round(px, 0), "dte_entry": OPT_DTE_DEFAULT,
                                           "iv_rank_entry": round(ivr, 1), "regime": regime,
                                           "win": 1 if r["pnl"] > 0 else 0})

            # Long Put (bearish, VIX > 25)
            if bear >= 2 and vix_val > 25:
                r = _sim_long_put(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    options_trades.append({**r, "strategy": "long_put", "ticker": sym,
                                           "entry_date": day_str, "option_type": "put",
                                           "exit_date": _exit_date_str(future, r["days"], day_str),
                                           "strike": round(px, 0), "dte_entry": OPT_DTE_DEFAULT,
                                           "iv_rank_entry": round(ivr, 1), "regime": regime,
                                           "win": 1 if r["pnl"] > 0 else 0})

            # Cash-Secured Put (high IV, bullish/neutral)
            if ivr > 50 and bull >= 1 and regime in ("BULL", "CAUTIOUS"):
                r = _sim_csp(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    options_trades.append({**r, "strategy": "csp", "ticker": sym,
                                           "entry_date": day_str, "option_type": "put",
                                           "exit_date": _exit_date_str(future, r["days"], day_str),
                                           "strike": round(px * 0.97, 0), "dte_entry": OPT_DTE_DEFAULT,
                                           "iv_rank_entry": round(ivr, 1), "regime": regime,
                                           "win": 1 if r["pnl"] > 0 else 0})

            # Covered Call (on positions we'd hold)
            if bull >= 2 and ivr > 40:
                r = _sim_covered_call(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    options_trades.append({**r, "strategy": "covered_call", "ticker": sym,
                                           "entry_date": day_str, "option_type": "call",
                                           "exit_date": _exit_date_str(future, r["days"], day_str),
                                           "strike": round(px * 1.03, 0), "dte_entry": OPT_DTE_DEFAULT,
                                           "iv_rank_entry": round(ivr, 1), "regime": regime,
                                           "win": 1 if r["pnl"] > 0 else 0})

            # ── Tier 6: Vertical spreads ──
            if bull >= 2:
                r = _sim_bull_call_spread(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    spread_trades.append({**r, "strategy": "bull_call_spread", "ticker": sym,
                                          "entry_date": day_str, "spread_type": "BULL_CALL",
                                          "exit_date": _exit_date_str(future, r["days"], day_str),
                                          "dte_entry": OPT_DTE_DEFAULT, "regime": regime,
                                          "win": 1 if r["pnl"] > 0 else 0,
                                          "days_held": r.get("days", OPT_DTE_DEFAULT)})
                r = _sim_bull_put_spread(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    spread_trades.append({**r, "strategy": "bull_put_spread", "ticker": sym,
                                          "entry_date": day_str, "spread_type": "BULL_PUT",
                                          "exit_date": _exit_date_str(future, r["days"], day_str),
                                          "dte_entry": OPT_DTE_DEFAULT, "regime": regime,
                                          "win": 1 if r.get("pnl", 0) > 0 else 0,
                                          "days_held": r.get("days", OPT_DTE_DEFAULT)})

            if bear >= 2:
                r = _sim_bear_put_spread(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    spread_trades.append({**r, "strategy": "bear_put_spread", "ticker": sym,
                                          "entry_date": day_str, "spread_type": "BEAR_PUT",
                                          "exit_date": _exit_date_str(future, r["days"], day_str),
                                          "dte_entry": OPT_DTE_DEFAULT, "regime": regime,
                                          "win": 1 if r["pnl"] > 0 else 0,
                                          "days_held": r.get("days", OPT_DTE_DEFAULT)})
                if vix_val > 20:
                    r = _sim_bear_call_spread(future, px, iv, OPT_DTE_DEFAULT)
                    if r:
                        spread_trades.append({**r, "strategy": "bear_call_spread", "ticker": sym,
                                              "entry_date": day_str, "spread_type": "BEAR_CALL",
                                              "exit_date": _exit_date_str(future, r["days"], day_str),
                                              "dte_entry": OPT_DTE_DEFAULT, "regime": regime,
                                              "win": 1 if r.get("pnl", 0) > 0 else 0,
                                              "days_held": r.get("days", OPT_DTE_DEFAULT)})

            # ── Tier 7: Iron Condors (VIX > 20, range-bound) ──
            if vix_val > 20:
                sma20 = float(np.mean(c[-20:]))
                if abs(px - sma20) / px < 0.02:  # range-bound
                    r = _sim_ic(future, px, iv, OPT_DTE_DEFAULT)
                    if r:
                        spread_trades.append({**r, "strategy": "iron_condor", "ticker": sym,
                                              "entry_date": day_str, "spread_type": "IC",
                                              "exit_date": _exit_date_str(future, r["days"], day_str),
                                              "dte_entry": OPT_DTE_DEFAULT, "regime": regime,
                                              "win": 1 if r.get("pnl", 0) > 0 else 0,
                                              "days_held": r.get("days", OPT_DTE_DEFAULT)})
                    r = _sim_broken_wing_ic(future, px, iv, OPT_DTE_DEFAULT)
                    if r:
                        spread_trades.append({**r, "strategy": "broken_wing_ic", "ticker": sym,
                                              "entry_date": day_str, "spread_type": "BWI",
                                              "exit_date": _exit_date_str(future, r["days"], day_str),
                                              "dte_entry": OPT_DTE_DEFAULT, "regime": regime,
                                              "win": 1 if r.get("pnl", 0) > 0 else 0,
                                              "days_held": r.get("days", OPT_DTE_DEFAULT)})

        # ── Tier 8: 0DTE (SPY/QQQ/TQQQ) ──
        for sym0 in ("SPY", "QQQ", "TQQQ"):
            df = td.get(sym0)
            if df is None or len(df) < 30:
                continue
            m   = df.index <= day
            sub = df.loc[m]
            c   = sub["Close"].values
            h   = sub["High"].values
            l   = sub["Low"].values
            px  = float(c[-1])
            iv  = _hist_vol(c, 10)  # short-term vol for 0DTE

            # Get next day data (0DTE outcome)
            future_day = df.loc[df.index > day]
            if len(future_day) < 1:
                continue
            next_row = {"High":  float(future_day["High"].iloc[0]),
                        "Low":   float(future_day["Low"].iloc[0]),
                        "Close": float(future_day["Close"].iloc[0])}

            # SPY 0DTE calls: positive GEX proxy (price > 5-day SMA)
            sma5 = float(np.mean(c[-5:]))
            if px > sma5 and sym0 == "SPY":
                r = _sim_0dte(next_row, px, iv, "call")
                if r:
                    dte0_trades.append({**r, "strategy": "spy_0dte_call", "ticker": sym0,
                                        "trade_date": day_str, "direction": "call",
                                        "gex_env": "POSITIVE", "vix_at_entry": vix_val,
                                        "win": 1 if r["pnl"] > 0 else 0,
                                        "pnl_pct": r.get("pnl_pct", 0)})

            # SPY 0DTE puts: negative GEX proxy (price < 5-day SMA)
            if px < sma5 and sym0 == "SPY":
                r = _sim_0dte(next_row, px, iv, "put")
                if r:
                    dte0_trades.append({**r, "strategy": "spy_0dte_put", "ticker": sym0,
                                        "trade_date": day_str, "direction": "put",
                                        "gex_env": "NEGATIVE", "vix_at_entry": vix_val,
                                        "win": 1 if r["pnl"] > 0 else 0,
                                        "pnl_pct": r.get("pnl_pct", 0)})

            # TQQQ 0DTE straddle on VIX spike days (VIX > 20)
            if vix_val > 20 and sym0 == "TQQQ":
                r_c = _sim_0dte(next_row, px, iv, "call")
                r_p = _sim_0dte(next_row, px, iv, "put")
                if r_c and r_p:
                    combined_pnl     = r_c["pnl"] + r_p["pnl"]
                    combined_premium = r_c["premium"] + r_p["premium"]
                    dte0_trades.append({"strategy": "tqqq_0dte_straddle", "ticker": sym0,
                                        "trade_date": day_str, "direction": "straddle",
                                        "pnl": combined_pnl,
                                        "pnl_pct": combined_pnl / combined_premium * 100,
                                        "gex_env": "SPIKE", "vix_at_entry": vix_val,
                                        "win": 1 if combined_pnl > 0 else 0,
                                        "premium": combined_premium})

            # QQQ 0DTE scalps: VWAP reclaim
            if sym0 == "QQQ" and len(c) >= 5:
                tp5  = [(float(df["High"].iloc[-(5-i)]) + float(df["Low"].iloc[-(5-i)]) + float(df["Close"].iloc[-(5-i)])) / 3
                        for i in range(5)]
                vwap = float(np.mean(tp5))
                direction = "call" if px > vwap else "put"
                r = _sim_0dte(next_row, px, iv, direction)
                if r:
                    dte0_trades.append({**r, "strategy": "qqq_0dte_scalp", "ticker": sym0,
                                        "trade_date": day_str, "direction": direction,
                                        "gex_env": "VWAP", "vix_at_entry": vix_val,
                                        "win": 1 if r["pnl"] > 0 else 0,
                                        "pnl_pct": r.get("pnl_pct", 0)})

    return options_trades, spread_trades, dte0_trades


# ═══════════════════════════════════════════════════════════════════════════
# Save results to DB
# ═══════════════════════════════════════════════════════════════════════════

def _save_master_results(run_date: str, tier: int, tier_name: str,
                          strategy: str, ticker: str, metrics: dict,
                          spy_return: float, regime: str = "ALL"):
    conn = _conn()
    vs_spy = round(metrics["total_return"] - spy_return, 2)
    conn.execute("""
        INSERT INTO backtest_master_results
          (run_date,tier,tier_name,strategy,ticker,total_return,win_rate,sharpe,
           realistic_sharpe,needs_validation,
           max_drawdown,avg_hold_hours,num_trades,profit_factor,calmar,
           best_trade_pct,worst_trade_pct,spy_return,vs_spy,
           max_consec_wins,max_consec_losses,regime)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (run_date, tier, tier_name, strategy, ticker,
          metrics["total_return"], metrics["win_rate"], metrics["sharpe"],
          metrics.get("realistic_sharpe", min(abs(metrics["sharpe"]), 5.0)),
          metrics.get("needs_validation", 0),
          metrics["max_drawdown"], metrics["avg_hold_hours"], metrics["num_trades"],
          metrics["profit_factor"], metrics["calmar"],
          metrics["best_trade_pct"], metrics["worst_trade_pct"],
          spy_return, vs_spy,
          metrics["max_consec_wins"], metrics["max_consec_losses"], regime))
    conn.commit()
    conn.close()


def _save_options_trade(run_date: str, t: dict):
    conn = _conn()
    conn.execute("""
        INSERT INTO backtest_options_results
          (run_date,strategy,ticker,entry_date,option_type,strike,premium,
           dte_entry,pnl,pnl_pct,win,iv_rank_entry,theta_captured,exit_type,regime)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (run_date, t.get("strategy"), t.get("ticker"), t.get("entry_date"),
          t.get("option_type"), t.get("strike"), t.get("premium", t.get("credit")),
          t.get("dte_entry", 30), t.get("pnl", 0), t.get("pnl_pct", 0), t.get("win", 0),
          t.get("iv_rank_entry"), t.get("theta"),
          t.get("exit_type"), t.get("regime", "ALL")))
    conn.commit()
    conn.close()


def _save_spread_trade(run_date: str, t: dict):
    conn = _conn()
    conn.execute("""
        INSERT INTO backtest_spread_results
          (run_date,strategy,ticker,entry_date,spread_type,credit,max_profit,max_loss,
           pnl,pnl_pct,win,dte_entry,days_held,pop_est,exit_type,regime)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (run_date, t.get("strategy"), t.get("ticker"), t.get("entry_date"),
          t.get("spread_type"), t.get("credit"), t.get("max_profit"), t.get("max_loss"),
          t.get("pnl", 0), t.get("pnl_pct", 0), t.get("win", 0),
          t.get("dte_entry", 30), t.get("days_held", 30),
          t.get("pop", t.get("pop_est")), t.get("exit_type"), t.get("regime", "ALL")))
    conn.commit()
    conn.close()


def _save_0dte_trade(run_date: str, t: dict):
    conn = _conn()
    conn.execute("""
        INSERT INTO backtest_0dte_results
          (run_date,strategy,ticker,trade_date,direction,entry_price,
           pnl,pnl_pct,win,gex_env,vix_at_entry)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (run_date, t.get("strategy"), t.get("ticker"), t.get("trade_date"),
          t.get("direction"), t.get("premium"),
          t.get("pnl", 0), t.get("pnl_pct", 0), t.get("win", 0),
          t.get("gex_env"), t.get("vix_at_entry")))
    conn.commit()
    conn.close()


def _save_monthly(run_date: str, strategy: str, ticker: str, monthly: dict):
    conn = _conn()
    for mo, m in monthly.items():
        conn.execute("""
            INSERT INTO backtest_monthly_breakdown
              (run_date,strategy,ticker,month,pnl,return_pct,num_trades,win_rate)
            VALUES (?,?,?,?,?,?,?,?)
        """, (run_date, strategy, ticker, mo,
              m["pnl"], m["return_pct"], m["num_trades"], m["win_rate"]))
    conn.commit()
    conn.close()


def _save_greeks(run_date: str, strategy: str, ticker: str, trades: list):
    deltas = [t.get("delta", 0) for t in trades if t.get("delta") is not None]
    thetas = [t.get("theta", 0) for t in trades if t.get("theta") is not None]
    iv_e   = [t.get("iv_entry", 0) for t in trades if t.get("iv_entry") is not None]
    iv_x   = [t.get("iv_exit", 0)  for t in trades if t.get("iv_exit") is not None]
    if not deltas:
        return
    conn = _conn()
    conn.execute("""
        INSERT INTO backtest_greeks_summary
          (run_date,strategy,ticker,avg_delta,avg_theta_per_day,avg_iv_entry,avg_iv_exit,theta_total)
        VALUES (?,?,?,?,?,?,?,?)
    """, (run_date, strategy, ticker,
          round(float(np.mean(deltas)), 4), round(float(np.mean(thetas)), 4),
          round(float(np.mean(iv_e)), 4), round(float(np.mean(iv_x)), 4),
          round(float(np.sum(thetas)), 4)))
    conn.commit()
    conn.close()


def _save_heatmap(run_date: str, ticker: str, strategy: str, regime: str, trades: list):
    if not trades:
        return
    pcts = [t["pnl_pct"] for t in trades]
    wins = [p for p in pcts if p > 0]
    conn = _conn()
    conn.execute("""
        INSERT OR REPLACE INTO options_strategy_heatmap
          (run_date,ticker,strategy,regime,avg_return,win_rate,num_trades,sharpe)
        VALUES (?,?,?,?,?,?,?,?)
    """, (run_date, ticker, strategy, regime,
          round(float(np.mean(pcts)), 3),
          round(len(wins) / len(pcts) * 100, 1),
          len(trades),
          round(float(np.mean(pcts)) / (float(np.std(pcts)) + 1e-9) * math.sqrt(252), 3)))
    conn.commit()
    conn.close()


def _save_equity_curve(run_date: str, curve: list[dict]):
    conn = _conn()
    conn.execute("DELETE FROM backtest_equity_curve WHERE run_date = ?", (run_date,))
    for row in curve:
        conn.execute("""
            INSERT INTO backtest_equity_curve (run_date, trade_date, equity, daily_pnl, regime)
            VALUES (?,?,?,?,?)
        """, (run_date, row["date"], row["equity"], row.get("daily_pnl", 0), row.get("regime", "UNKNOWN")))
    conn.commit()
    conn.close()


def _save_symbol_params(run_date: str, ticker: str, best_strat: str, best_sharpe: float,
                         best_tod: str, best_opt: str):
    conn = _conn()
    conn.execute("""
        INSERT OR REPLACE INTO backtest_symbol_params
          (run_date,ticker,best_strategy,best_sharpe,
           rsi_period,rsi_entry,rsi_exit,macd_fast,macd_slow,macd_signal,
           bb_period,bb_std,sma_fast,sma_slow,best_tod,best_options_strategy,
           optimal_dte,optimal_delta,iv_pct_sweet_spot)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (run_date, ticker, best_strat, round(best_sharpe, 3),
          14, 30, 70,  12, 26, 9,  20, 2.0, 20, 50,
          best_tod, best_opt, 30, 0.30, 50.0))
    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# Summary report generator
# ═══════════════════════════════════════════════════════════════════════════

def _generate_summary(run_date: str, spy_return: float) -> dict:
    conn = _conn()

    # Top 10 strategies by Sharpe (all tickers aggregated)
    top10_q = conn.execute("""
        SELECT strategy, tier_name,
               AVG(sharpe) as avg_sharpe, AVG(total_return) as avg_return,
               AVG(win_rate) as avg_wr, SUM(num_trades) as total_trades,
               AVG(max_drawdown) as avg_dd, AVG(calmar) as avg_calmar
        FROM backtest_master_results
        WHERE run_date = ? AND num_trades > 0
        GROUP BY strategy
        ORDER BY avg_sharpe DESC
        LIMIT 10
    """, (run_date,)).fetchall()
    top10 = [dict(r) for r in top10_q]

    # Best strategy per symbol
    sym_best_q = conn.execute("""
        SELECT ticker, strategy, sharpe, total_return, win_rate
        FROM backtest_master_results
        WHERE run_date = ? AND num_trades > 0
        GROUP BY ticker
        HAVING MAX(sharpe)
        ORDER BY ticker
    """, (run_date,)).fetchall()
    sym_best = [dict(r) for r in sym_best_q]

    # Regime breakdown
    regime_q = conn.execute("""
        SELECT regime, strategy, AVG(total_return) as avg_ret, AVG(win_rate) as avg_wr
        FROM backtest_master_results
        WHERE run_date = ? AND num_trades > 0 AND regime != 'ALL'
        GROUP BY regime, strategy
        ORDER BY regime, avg_ret DESC
    """, (run_date,)).fetchall()
    regime_data = [dict(r) for r in regime_q]

    # Options summary
    opts_q = conn.execute("""
        SELECT strategy, COUNT(*) as n, AVG(pnl_pct) as avg_pnl,
               AVG(CASE WHEN win=1 THEN 1.0 ELSE 0.0 END)*100 as win_rate
        FROM backtest_options_results
        WHERE run_date = ?
        GROUP BY strategy ORDER BY avg_pnl DESC
    """, (run_date,)).fetchall()
    opts_summary = [dict(r) for r in opts_q]

    # Spread summary
    spread_q = conn.execute("""
        SELECT strategy, COUNT(*) as n, AVG(pnl_pct) as avg_pnl,
               AVG(CASE WHEN win=1 THEN 1.0 ELSE 0.0 END)*100 as win_rate
        FROM backtest_spread_results
        WHERE run_date = ?
        GROUP BY strategy ORDER BY avg_pnl DESC
    """, (run_date,)).fetchall()
    spread_summary = [dict(r) for r in spread_q]

    # 0DTE summary
    dte0_q = conn.execute("""
        SELECT strategy, COUNT(*) as n, AVG(pnl_pct) as avg_pnl,
               AVG(CASE WHEN win=1 THEN 1.0 ELSE 0.0 END)*100 as win_rate
        FROM backtest_0dte_results
        WHERE run_date = ?
        GROUP BY strategy ORDER BY avg_pnl DESC
    """, (run_date,)).fetchall()
    dte0_summary = [dict(r) for r in dte0_q]

    # Monthly equity
    monthly_q = conn.execute("""
        SELECT month, SUM(pnl) as total_pnl, AVG(win_rate) as avg_wr
        FROM backtest_monthly_breakdown
        WHERE run_date = ?
        GROUP BY month ORDER BY month
    """, (run_date,)).fetchall()
    monthly = [dict(r) for r in monthly_q]

    conn.close()

    # Recommended allocation based on Sharpe ranking
    alloc_recs = []
    total_sharpe = sum(r["avg_sharpe"] for r in top10 if r["avg_sharpe"] > 0) + 1e-9
    for r in top10:
        w = max(0, r["avg_sharpe"]) / total_sharpe * 100
        alloc_recs.append({"strategy": r["strategy"], "weight_pct": round(w, 1)})

    return {
        "run_date": run_date,
        "backtest_days": BACKTEST_DAYS,
        "universe_size": len(MASTER_UNIVERSE),
        "spy_return_pct": spy_return,
        "top10_strategies": top10,
        "best_strategy_per_symbol": sym_best,
        "regime_breakdown": regime_data,
        "options_summary": opts_summary,
        "spread_summary": spread_summary,
        "dte0_summary": dte0_summary,
        "monthly_equity": monthly,
        "recommended_allocation": alloc_recs,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Master orchestrator
# ═══════════════════════════════════════════════════════════════════════════

def run_master_backtest(days: int = BACKTEST_DAYS, compare: bool = True) -> dict:
    """Run comprehensive backtest across all 9 strategy tiers.

    Downloads 24-ticker universe, runs each tier, saves 9 DB tables,
    returns summary dict with top-10 strategies, allocation weights,
    and optional side-by-side comparison vs previous run.
    """
    _init_tables()
    run_date = datetime.now().strftime("%Y-%m-%d")
    start_ts = time.time()
    logger.info("Master backtest started: %s (%d days, %d tickers)",
                run_date, days, len(MASTER_UNIVERSE))

    # 1. Download universe data
    logger.info("Downloading universe...")
    td = _download_universe(days + 60)  # extra warmup buffer

    # Build VIX map and trading days
    vix_df = td.get("^VIX", pd.DataFrame())
    vix_map: dict = {}
    if not vix_df.empty and "Close" in vix_df.columns:
        for idx, row in vix_df.iterrows():
            vix_map[idx] = float(row["Close"])

    trading_days = _get_trading_days(td, days)
    if not trading_days:
        return {"status": "error", "message": "No trading days found (SPY not downloaded)"}

    # SPY buy-and-hold benchmark
    spy_df = td.get("SPY", pd.DataFrame())
    spy_return = 0.0
    if not spy_df.empty:
        spy_s = float(spy_df[spy_df.index <= trading_days[0]]["Close"].iloc[-1]) if len(spy_df[spy_df.index <= trading_days[0]]) > 0 else float(spy_df["Close"].iloc[0])
        spy_e = float(spy_df[spy_df.index <= trading_days[-1]]["Close"].iloc[-1])
        spy_return = round((spy_e - spy_s) / spy_s * 100, 2)

    # ── Capture previous run for side-by-side comparison ───────────────────
    prev_top10: list[dict] = []
    prev_meta:  dict       = {}
    if compare:
        try:
            conn_prev = _conn()
            prev_date_row = conn_prev.execute("""
                SELECT MAX(run_date) FROM backtest_master_results
                WHERE run_date < ?
            """, (run_date,)).fetchone()
            prev_date = prev_date_row[0] if prev_date_row and prev_date_row[0] else None
            if prev_date:
                prev_rows = conn_prev.execute("""
                    SELECT strategy, AVG(realistic_sharpe) as avg_sharpe,
                           AVG(total_return) as avg_return, AVG(win_rate) as avg_wr,
                           SUM(num_trades) as total_trades,
                           AVG(CASE WHEN needs_validation=1 THEN 1 ELSE 0 END) as pct_flagged
                    FROM backtest_master_results
                    WHERE run_date=? AND num_trades>0 AND regime='ALL'
                    GROUP BY strategy ORDER BY avg_sharpe DESC LIMIT 10
                """, (prev_date,)).fetchall()
                prev_top10 = [dict(r) for r in prev_rows]
                eq_row = conn_prev.execute("""
                    SELECT MIN(equity) as min_eq, MAX(equity) as max_eq,
                           (SELECT equity FROM backtest_equity_curve
                            WHERE run_date=? ORDER BY trade_date ASC LIMIT 1) as start_eq,
                           (SELECT equity FROM backtest_equity_curve
                            WHERE run_date=? ORDER BY trade_date DESC LIMIT 1) as end_eq
                    FROM backtest_equity_curve WHERE run_date=?
                """, (prev_date, prev_date, prev_date)).fetchone()
                prev_meta = {"run_date": prev_date, "equity": dict(eq_row) if eq_row else {}}
            conn_prev.close()
        except Exception as e:
            logger.debug("Could not load previous run for comparison: %s", e)

    # Clear existing results for today
    conn = _conn()
    for tbl in ["backtest_master_results", "backtest_monthly_breakdown",
                "backtest_greeks_summary", "options_strategy_heatmap"]:
        conn.execute(f"DELETE FROM {tbl} WHERE run_date = ?", (run_date,))
    conn.commit()
    conn.close()

    # ── TIER 1: VectorBT ────────────────────────────────────────────────────
    logger.info("Tier 1: VectorBT technical strategies...")
    for sym in MASTER_UNIVERSE:
        df = td.get(sym)
        if df is None or "Close" not in df.columns or len(df) < 60:
            continue
        # Use only the backtest window
        bt_start = trading_days[0] - timedelta(days=60)
        closes   = df[df.index >= bt_start]["Close"].dropna()
        if len(closes) < 30:
            continue

        t1_results = _run_tier1_vbt(closes, sym)
        for r in t1_results:
            strat   = r["strategy"]
            metrics = {
                "total_return":    r.get("total_return", 0) or 0,
                "win_rate":        r.get("win_rate", 0) or 0,
                "sharpe":          r.get("sharpe", 0) or 0,
                "max_drawdown":    -(abs(r.get("max_drawdown", 0) or 0)),
                "avg_hold_hours":  (r.get("avg_hold", 0) or 0) * 24,
                "num_trades":      r.get("num_trades", 0) or 0,
                "profit_factor":   r.get("profit_factor", 0) or 0,
                "calmar":          0.0,
                "best_trade_pct":  0.0,
                "worst_trade_pct": 0.0,
                "max_consec_wins": 0,
                "max_consec_losses": 0,
            }
            if metrics["max_drawdown"] != 0:
                metrics["calmar"] = round(metrics["total_return"] / abs(metrics["max_drawdown"]), 3)
            _save_master_results(run_date, 1, "Core Technical", strat, sym, metrics, spy_return)

    # ── TIERS 2-4, 9: Event-driven ─────────────────────────────────────────
    logger.info("Tiers 2-4, 9: Event-driven signal loop...")
    event_trades = _run_event_loop(td, trading_days, vix_map)

    # Dalio Metals (Tier 4, special)
    metals_trades = _run_dalio_metals(td, trading_days)
    for t in metals_trades:
        event_trades.setdefault("dalio_metals", []).append(t)

    TIER_MAP = {
        "gap_and_go": (2, "Intraday Momentum"), "momentum_breakout": (2, "Intraday Momentum"),
        "reversal_bounce": (2, "Intraday Momentum"), "volatility_breakout": (2, "Intraday Momentum"),
        "vwap_reclaim": (2, "Intraday Momentum"),
        "hammer_candle": (3, "Holly-Style"), "bull_bear_trap": (3, "Holly-Style"),
        "falling_knife": (3, "Holly-Style"), "avwap_bounce": (3, "Holly-Style"),
        "five_day_bounce": (3, "Holly-Style"), "alpha_predator": (3, "Holly-Style"),
        "ollie_super": (4, "Agent-Specific"), "neo_plutus": (4, "Agent-Specific"),
        "super_agent": (4, "Agent-Specific"), "congress_copycat": (4, "Agent-Specific"),
        "dalio_metals": (4, "Agent-Specific"), "dayblade_0dte": (4, "Agent-Specific"),
        "short_equity": (9, "Short Strategies"), "inverse_etf": (9, "Short Strategies"),
        "short_put_spread": (9, "Short Strategies"),
    }

    # Per-symbol aggregation for event trades
    from collections import defaultdict
    strat_sym_trades: dict[tuple, list] = defaultdict(list)
    for strat, trades in event_trades.items():
        for t in trades:
            strat_sym_trades[(strat, t["ticker"])].append(t)

    for (strat, sym), trades in strat_sym_trades.items():
        if not trades:
            continue
        tier_num, tier_name = TIER_MAP.get(strat, (2, "Unknown"))
        metrics  = _trade_metrics(trades)
        monthly  = _monthly_breakdown(trades)
        vix_vals = [vix_map.get(pd.Timestamp(t["entry_date"]).normalize(), 18.0) for t in trades]
        regimes  = [_classify_regime(v) for v in vix_vals]

        _save_master_results(run_date, tier_num, tier_name, strat, sym, metrics, spy_return)
        _save_monthly(run_date, strat, sym, monthly)

        # Per-regime heatmap
        for reg in ["BULL", "CAUTIOUS", "BEAR", "CRISIS"]:
            reg_trades = [t for t, r in zip(trades, regimes) if r == reg]
            if len(reg_trades) >= 3:
                reg_metrics = _trade_metrics(reg_trades)
                _save_master_results(run_date, tier_num, tier_name,
                                     strat, sym, reg_metrics, spy_return, regime=reg)

    # ── TIERS 5-8: Options loop ─────────────────────────────────────────────
    logger.info("Tiers 5-8: Options simulation...")
    opt_trades, spread_trades, dte0_trades = _run_options_loop(td, trading_days, vix_map)

    # Save options results
    for t in opt_trades:
        try:
            _save_options_trade(run_date, t)
        except Exception as e:
            logger.debug("options save: %s", e)

    for t in spread_trades:
        try:
            _save_spread_trade(run_date, t)
        except Exception as e:
            logger.debug("spread save: %s", e)

    for t in dte0_trades:
        try:
            _save_0dte_trade(run_date, t)
        except Exception as e:
            logger.debug("0dte save: %s", e)

    def _ensure_pnl_pct(t: dict) -> dict:
        """Guarantee every trade dict has pnl_pct; compute from pnl/credit if missing."""
        if "pnl_pct" not in t or t["pnl_pct"] is None:
            pnl  = t.get("pnl", 0) or 0
            base = abs(t.get("credit", t.get("max_loss", t.get("premium", 1)))) or 1
            t = {**t, "pnl_pct": round(pnl / base * 100, 2)}
        return t

    # Aggregate options/spreads/0dte for master_results table
    all_opt = {}
    for t in opt_trades:
        key = (t["strategy"], t["ticker"])
        all_opt.setdefault(key, []).append({**_ensure_pnl_pct(t), "hold_days": t.get("days", 5)})
    for t in spread_trades:
        key = (t["strategy"], t["ticker"])
        all_opt.setdefault(key, []).append({**_ensure_pnl_pct(t), "hold_days": t.get("days_held", 10)})
    for t in dte0_trades:
        key = (t["strategy"], t["ticker"])
        all_opt.setdefault(key, []).append({**_ensure_pnl_pct(t), "hold_days": 1})

    OPT_TIER = {
        "long_call": (5, "Options Single Leg"), "long_put": (5, "Options Single Leg"),
        "csp": (5, "Options Single Leg"), "covered_call": (5, "Options Single Leg"),
        "bull_call_spread": (6, "Vertical Spreads"), "bear_put_spread": (6, "Vertical Spreads"),
        "bull_put_spread": (6, "Vertical Spreads"), "bear_call_spread": (6, "Vertical Spreads"),
        "iron_condor": (7, "Iron Condors"), "broken_wing_ic": (7, "Iron Condors"),
        "spy_0dte_call": (8, "0DTE"), "spy_0dte_put": (8, "0DTE"),
        "tqqq_0dte_straddle": (8, "0DTE"), "qqq_0dte_scalp": (8, "0DTE"),
    }

    for (strat, sym), trades in all_opt.items():
        if not trades:
            continue
        tier_num, tier_name = OPT_TIER.get(strat, (5, "Options"))
        metrics = _trade_metrics(trades)
        monthly = _monthly_breakdown(trades)
        _save_master_results(run_date, tier_num, tier_name, strat, sym, metrics, spy_return)
        _save_monthly(run_date, strat, sym, monthly)
        _save_greeks(run_date, strat, sym, trades)
        # Heatmap
        vix_vals = [vix_map.get(pd.Timestamp(t.get("entry_date", t.get("trade_date", ""))).normalize(), 18.0)
                    for t in trades]
        for reg in ["BULL", "CAUTIOUS", "BEAR", "CRISIS"]:
            reg_trades = [t for t, v in zip(trades, vix_vals) if _classify_regime(v) == reg]
            _save_heatmap(run_date, sym, strat, reg, reg_trades)

    # ── Equity curve (portfolio-level) ─────────────────────────────────────
    logger.info("Building equity curve...")
    equity    = STARTING_CASH
    curve     = []
    all_trades_by_date: dict[str, list] = defaultdict(list)

    for strat, trades in event_trades.items():
        for t in trades:
            all_trades_by_date[t.get("exit_date", t.get("date", ""))].append(t["pnl_pct"])

    # Use exit_date for options/spreads (not entry_date) so P&L lands on close day
    for t in opt_trades + spread_trades:
        key = t.get("exit_date") or t.get("entry_date", "")
        all_trades_by_date[key].append(t.get("pnl_pct", 0))

    for day in trading_days:
        day_str  = day.strftime("%Y-%m-%d")
        day_pnls = all_trades_by_date.get(day_str, [])
        # Cap daily return at ±2% to prevent compounding blow-up from batch scan days
        day_ret  = float(np.clip(np.mean(day_pnls) / 100, -0.02, 0.02)) if day_pnls else 0.0
        daily_pnl = equity * day_ret
        equity   += daily_pnl
        vix_val   = vix_map.get(day, 18.0)
        curve.append({"date": day_str, "equity": round(equity, 2),
                      "daily_pnl": round(daily_pnl, 2), "regime": _classify_regime(vix_val)})

    _save_equity_curve(run_date, curve)

    # ── Symbol params summary ────────────────────────────────────────────────
    conn = _conn()
    for sym in MASTER_UNIVERSE:
        rows = conn.execute("""
            SELECT strategy, sharpe FROM backtest_master_results
            WHERE run_date = ? AND ticker = ? AND num_trades > 0
            ORDER BY sharpe DESC LIMIT 1
        """, (run_date, sym)).fetchall()
        if not rows:
            continue
        best_row = rows[0]
        # Best time-of-day from extras if available
        tod_row = conn.execute("""
            SELECT session FROM extras_time_of_day
            WHERE ticker = ? ORDER BY avg_return DESC LIMIT 1
        """, (sym,)).fetchone()
        best_tod = tod_row["session"] if tod_row else "Morning"
        # Best options strategy
        opt_row = conn.execute("""
            SELECT strategy FROM backtest_options_results
            WHERE run_date = ? AND ticker = ?
            GROUP BY strategy ORDER BY AVG(pnl_pct) DESC LIMIT 1
        """, (run_date, sym)).fetchone()
        best_opt = opt_row["strategy"] if opt_row else "csp"
        _save_symbol_params(run_date, sym, best_row["strategy"],
                            best_row["sharpe"] or 0.0, best_tod, best_opt)
    conn.close()

    elapsed = round(time.time() - start_ts, 1)
    logger.info("Master backtest complete in %.0fs", elapsed)

    # Generate and return summary
    summary = _generate_summary(run_date, spy_return)
    equity_end = round(curve[-1]["equity"] if curve else STARTING_CASH, 2)
    summary.update({
        "status":              "ok",
        "elapsed_seconds":     elapsed,
        "backtest_days":       days,
        "opt_slippage_per_leg":OPT_SLIP_PER_LEG,
        "exec_delay_penalty":  EXEC_DELAY,
        "event_strategies":    len(event_trades),
        "total_event_trades":  sum(len(v) for v in event_trades.values()),
        "total_options_trades":len(opt_trades),
        "total_spread_trades": len(spread_trades),
        "total_0dte_trades":   len(dte0_trades),
        "equity_start":        STARTING_CASH,
        "equity_end":          equity_end,
        "total_return_pct":    round((equity_end - STARTING_CASH) / STARTING_CASH * 100, 2),
    })

    # ── Sharpe validation report ─────────────────────────────────────────────
    try:
        conn_val = _conn()
        flagged = conn_val.execute("""
            SELECT strategy, ticker, sharpe, realistic_sharpe, total_return, num_trades
            FROM backtest_master_results
            WHERE run_date = ? AND needs_validation = 1 AND regime = 'ALL'
            ORDER BY sharpe DESC
        """, (run_date,)).fetchall()
        conn_val.close()
        summary["needs_validation"] = [dict(r) for r in flagged]
        summary["validation_count"] = len(flagged)
    except Exception:
        summary["needs_validation"] = []
        summary["validation_count"] = 0

    # ── Side-by-side comparison vs previous run ──────────────────────────────
    if compare and prev_top10:
        new_top10 = summary.get("top10_strategies", [])
        new_by_name = {r["strategy"]: r for r in new_top10}
        old_by_name = {r["strategy"]: r for r in prev_top10}
        comparison = []
        all_strats  = sorted(set(list(new_by_name) + list(old_by_name)))
        for strat in all_strats:
            new_r = new_by_name.get(strat, {})
            old_r = old_by_name.get(strat, {})
            comparison.append({
                "strategy":         strat,
                "old_sharpe":       round(float(old_r.get("avg_sharpe") or 0), 3),
                "new_sharpe":       round(float(new_r.get("avg_sharpe") or new_r.get("realistic_sharpe") or 0), 3),
                "sharpe_delta":     round(float(new_r.get("avg_sharpe") or 0) - float(old_r.get("avg_sharpe") or 0), 3),
                "old_return":       round(float(old_r.get("avg_return") or 0), 2),
                "new_return":       round(float(new_r.get("avg_return") or 0), 2),
                "return_delta":     round(float(new_r.get("avg_return") or 0) - float(old_r.get("avg_return") or 0), 2),
                "old_wr":           round(float(old_r.get("avg_wr") or 0), 1),
                "new_wr":           round(float(new_r.get("avg_wr") or 0), 1),
                "old_trades":       int(old_r.get("total_trades") or 0),
                "new_trades":       int(new_r.get("total_trades") or 0),
                "old_flagged":      bool(old_r.get("pct_flagged", 0)),
                "new_flagged":      strat in [r["strategy"] for r in summary.get("needs_validation", [])],
            })
        # Sort by largest absolute change in return
        comparison.sort(key=lambda x: abs(x["return_delta"]), reverse=True)
        summary["comparison"] = {
            "prev_run_date": prev_meta.get("run_date", "N/A"),
            "curr_run_date": run_date,
            "prev_days":     90,        # assumed from last run
            "curr_days":     days,
            "prev_equity_end": (prev_meta.get("equity", {}) or {}).get("end_eq"),
            "curr_equity_end": equity_end,
            "strategy_comparison": comparison,
        }
    else:
        summary["comparison"] = None

    return summary
