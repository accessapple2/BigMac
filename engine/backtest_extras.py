"""Backtest Extras — additional analysis modules for USS TradeMinds.

Four tests:
  1. run_earnings_straddle()  — ATM straddle P&L around earnings dates
  2. run_regime_filter()      — strategy performance bucketed by VIX regime
  3. run_time_of_day()        — intraday entry window analysis (1h bars)
  4. run_cg_ratio()           — Copper/Gold ratio crossover as SPY entry signal

Transaction cost model (matches holly_nightly_backtest.py):
  - $0 commissions (Alpaca)
  - 0.1% slippage per trade
  - $0.65 per options contract
"""
from __future__ import annotations

import logging
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
_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_BACKTEST_DB = os.path.join(_DIR, "data", "backtest.db")
_TRADER_DB   = os.path.join(_DIR, "data", "trader.db")

# ── Transaction costs ────────────────────────────────────────────────────────
SLIPPAGE_PCT     = 0.001   # 0.1% per trade
COMMISSION_STOCK = 0.00
OPTIONS_CONTRACT = 0.65    # per contract
FEES             = SLIPPAGE_PCT

# ── Earnings straddle symbols ─────────────────────────────────────────────────
STRADDLE_SYMBOLS = ["NFLX", "NVDA", "AMD", "META", "TSLA", "GOOGL", "AMZN", "AAPL"]

# ── VIX regime thresholds ─────────────────────────────────────────────────────
REGIME_BULL     = 18.0
REGIME_CAUTIOUS = 25.0
REGIME_BEAR     = 35.0
# >35 → CRISIS


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
        CREATE TABLE IF NOT EXISTS extras_earnings_straddle (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date         TEXT NOT NULL,
            symbol           TEXT NOT NULL,
            earnings_date    TEXT NOT NULL,
            dte_at_entry     INTEGER NOT NULL,
            entry_price      REAL,
            straddle_cost    REAL,
            post_move_pct    REAL,
            pnl_per_straddle REAL,
            win              INTEGER,
            iv_rank_est      REAL,
            iv_crush_est_pct REAL,
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS extras_regime_filter (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date     TEXT NOT NULL,
            strategy     TEXT NOT NULL,
            ticker       TEXT NOT NULL,
            regime       TEXT NOT NULL,
            avg_return   REAL,
            win_rate     REAL,
            num_trades   INTEGER,
            sharpe       REAL,
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS extras_time_of_day (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date     TEXT NOT NULL,
            strategy     TEXT NOT NULL,
            ticker       TEXT NOT NULL,
            session      TEXT NOT NULL,
            avg_return   REAL,
            win_rate     REAL,
            num_trades   INTEGER,
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS extras_cg_ratio (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date         TEXT NOT NULL,
            entry_date       TEXT NOT NULL,
            exit_date        TEXT,
            entry_price      REAL,
            exit_price       REAL,
            pnl_pct          REAL,
            cg_ratio         REAL,
            cg_ma20          REAL,
            win              INTEGER,
            hold_days        INTEGER,
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# Shared data helpers
# ═══════════════════════════════════════════════════════════════════════════

def _yf_download(symbol: str, period: str = "2y", interval: str = "1d") -> pd.DataFrame:
    """Download OHLCV from yfinance with retry and column flattening."""
    import yfinance as yf
    for attempt in range(2):
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                df = yf.download(symbol, period=period, interval=interval,
                                 auto_adjust=True, progress=False, threads=False)
            if df is None or df.empty:
                raise ValueError(f"Empty data for {symbol}")
            # Flatten MultiIndex columns (yfinance returns them for single ticker too sometimes)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
            df.columns = [str(c).strip() for c in df.columns]
            return df
        except Exception as e:
            if attempt == 0:
                time.sleep(2)
            else:
                logger.warning("yfinance failed for %s: %s", symbol, e)
                return pd.DataFrame()
    return pd.DataFrame()


def _vix_series(days: int = 730) -> pd.Series:
    """Return daily VIX close series indexed by date."""
    df = _yf_download("^VIX", period=f"{days // 365 + 1}y", interval="1d")
    if df.empty or "Close" not in df.columns:
        return pd.Series(dtype=float)
    s = df["Close"].dropna()
    s.index = pd.to_datetime(s.index).normalize()
    return s


def _classify_regime(vix_val: float) -> str:
    if vix_val <= REGIME_BULL:
        return "BULL"
    elif vix_val <= REGIME_CAUTIOUS:
        return "CAUTIOUS"
    elif vix_val <= REGIME_BEAR:
        return "BEAR"
    else:
        return "CRISIS"


# ═══════════════════════════════════════════════════════════════════════════
# 1. Earnings Straddle
# ═══════════════════════════════════════════════════════════════════════════

def _get_earnings_dates(symbol: str) -> list[datetime]:
    """Return list of past earnings dates for symbol using yfinance."""
    import yfinance as yf
    try:
        t = yf.Ticker(symbol)
        cal = t.earnings_dates
        if cal is None or cal.empty:
            return []
        dates = []
        for idx in cal.index:
            d = pd.Timestamp(idx).to_pydatetime()
            if d < datetime.now() - timedelta(days=2):
                dates.append(d)
        return sorted(dates, reverse=True)[:12]  # up to 3 years of quarterly earnings
    except Exception as e:
        logger.warning("earnings dates failed for %s: %s", symbol, e)
        return []


def _estimate_straddle_cost(price: float, hist_vol_20d: float, dte: int) -> float:
    """Rough ATM straddle cost using Black-Scholes approximation.
    Straddle ≈ 2 × price × IV × sqrt(DTE/252) × 0.4 (put-call combined)
    The 0.4 factor ~ N(d1) - N(d2) for ATM options.
    """
    if hist_vol_20d <= 0 or price <= 0:
        return 0.0
    # Use 1.3× historical vol as proxy for implied vol (typical for pre-earnings)
    iv_proxy = hist_vol_20d * 1.3
    straddle = price * iv_proxy * (dte / 252) ** 0.5 * 0.8
    # Add options transaction costs: 2 contracts (call + put), $0.65 each
    contract_cost = 2 * OPTIONS_CONTRACT / 100  # per $100 notional (1 share equiv)
    return straddle + contract_cost


def run_earnings_straddle(symbols: list[str] | None = None, lookback_days: int = 730) -> dict:
    """Backtest ATM straddle strategy: buy 2 days before earnings, sell at open after.

    Tracks:
    - avg IV crush estimate %
    - win rate by IV rank bucket
    - optimal DTE (tests 1–5 days before earnings)
    - P&L per straddle including $0.65/contract cost

    Results saved to extras_earnings_straddle table.
    """
    _init_tables()
    symbols = symbols or STRADDLE_SYMBOLS
    today_str = datetime.now().strftime("%Y-%m-%d")
    results = []

    for sym in symbols:
        logger.info("Earnings straddle: %s", sym)
        df = _yf_download(sym, period="3y", interval="1d")
        if df.empty or "Close" not in df.columns or len(df) < 30:
            continue

        df.index = pd.to_datetime(df.index).normalize()
        closes = df["Close"].dropna()

        earnings_dates = _get_earnings_dates(sym)
        if not earnings_dates:
            logger.warning("No earnings dates for %s", sym)
            continue

        for dte_offset in range(1, 6):  # test 1–5 days before earnings
            for edate in earnings_dates:
                edate_norm = pd.Timestamp(edate).normalize()

                # Entry = dte_offset trading days before earnings
                entry_candidates = closes.index[closes.index < edate_norm]
                if len(entry_candidates) < dte_offset + 5:
                    continue
                entry_date = entry_candidates[-dte_offset]

                # Exit = first close after earnings date
                exit_candidates = closes.index[closes.index > edate_norm]
                if len(exit_candidates) < 1:
                    continue
                exit_date = exit_candidates[0]

                entry_price = float(closes.loc[entry_date])
                exit_price  = float(closes.loc[exit_date])

                # 20-day historical vol at entry
                window_end   = entry_candidates[-1]
                window_start = entry_candidates[-21] if len(entry_candidates) >= 21 else entry_candidates[0]
                hv_series    = closes.loc[window_start:window_end]
                if len(hv_series) < 5:
                    continue
                log_rets  = np.log(hv_series / hv_series.shift(1)).dropna()
                hv_20d    = float(log_rets.std() * np.sqrt(252))

                straddle_cost = _estimate_straddle_cost(entry_price, hv_20d, dte_offset)
                actual_move   = abs(exit_price - entry_price)
                pnl           = actual_move - straddle_cost
                win           = 1 if pnl > 0 else 0
                post_move_pct = (exit_price - entry_price) / entry_price * 100

                # IV rank estimate: compare current HV to 52-week HV range
                lookback_end   = entry_candidates[-1]
                lookback_start = entry_candidates[-252] if len(entry_candidates) >= 252 else entry_candidates[0]
                hv_year = closes.loc[lookback_start:lookback_end]
                if len(hv_year) >= 20:
                    rolling_hv = hv_year.pct_change().rolling(20).std() * np.sqrt(252)
                    iv_rank = float((hv_20d - rolling_hv.min()) / (rolling_hv.max() - rolling_hv.min() + 1e-9) * 100)
                else:
                    iv_rank = 50.0

                # IV crush estimate: HV 5 days after vs HV before
                post_window = closes.loc[exit_date:][:5]
                if len(post_window) >= 3:
                    post_rets  = np.log(post_window / post_window.shift(1)).dropna()
                    hv_post    = float(post_rets.std() * np.sqrt(252)) if len(post_rets) >= 2 else hv_20d
                    iv_crush   = (hv_20d - hv_post) / (hv_20d + 1e-9) * 100
                else:
                    iv_crush = 0.0

                results.append({
                    "run_date":         today_str,
                    "symbol":           sym,
                    "earnings_date":    edate_norm.strftime("%Y-%m-%d"),
                    "dte_at_entry":     dte_offset,
                    "entry_price":      round(entry_price, 4),
                    "straddle_cost":    round(straddle_cost, 4),
                    "post_move_pct":    round(post_move_pct, 2),
                    "pnl_per_straddle": round(pnl, 4),
                    "win":              win,
                    "iv_rank_est":      round(iv_rank, 1),
                    "iv_crush_est_pct": round(iv_crush, 1),
                })

    if not results:
        return {"status": "no_data", "results": []}

    # Save to DB
    conn = _conn()
    conn.execute("DELETE FROM extras_earnings_straddle WHERE run_date = ?", (today_str,))
    for r in results:
        conn.execute("""
            INSERT INTO extras_earnings_straddle
              (run_date,symbol,earnings_date,dte_at_entry,entry_price,straddle_cost,
               post_move_pct,pnl_per_straddle,win,iv_rank_est,iv_crush_est_pct)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (r["run_date"], r["symbol"], r["earnings_date"], r["dte_at_entry"],
              r["entry_price"], r["straddle_cost"], r["post_move_pct"],
              r["pnl_per_straddle"], r["win"], r["iv_rank_est"], r["iv_crush_est_pct"]))
    conn.commit()
    conn.close()

    df_res = pd.DataFrame(results)

    # Summary by DTE
    by_dte = df_res.groupby("dte_at_entry").agg(
        win_rate=("win", "mean"),
        avg_pnl=("pnl_per_straddle", "mean"),
        avg_iv_crush=("iv_crush_est_pct", "mean"),
        n_trades=("win", "count"),
    ).reset_index()

    # Summary by symbol
    by_sym = df_res.groupby("symbol").agg(
        win_rate=("win", "mean"),
        avg_pnl=("pnl_per_straddle", "mean"),
        avg_iv_crush=("iv_crush_est_pct", "mean"),
        n_trades=("win", "count"),
    ).reset_index()

    # IV rank buckets (0-33 low, 33-66 mid, 66-100 high)
    df_res["iv_bucket"] = pd.cut(df_res["iv_rank_est"], bins=[0, 33, 66, 100],
                                  labels=["LOW_IV", "MID_IV", "HIGH_IV"])
    by_iv = df_res.groupby("iv_bucket", observed=True).agg(
        win_rate=("win", "mean"),
        avg_pnl=("pnl_per_straddle", "mean"),
        n_trades=("win", "count"),
    ).reset_index()

    best_dte = int(by_dte.loc[by_dte["win_rate"].idxmax(), "dte_at_entry"])

    return {
        "status": "ok",
        "total_trades": len(df_res),
        "overall_win_rate": round(float(df_res["win"].mean()), 3),
        "avg_pnl_per_straddle": round(float(df_res["pnl_per_straddle"].mean()), 4),
        "avg_iv_crush_pct": round(float(df_res["iv_crush_est_pct"].mean()), 1),
        "best_dte": best_dte,
        "by_dte": by_dte.round(3).to_dict("records"),
        "by_symbol": by_sym.round(3).to_dict("records"),
        "by_iv_rank": by_iv.round(3).to_dict("records"),
    }


# ═══════════════════════════════════════════════════════════════════════════
# 2. Regime Filter
# ═══════════════════════════════════════════════════════════════════════════

def run_regime_filter(lookback_days: int = 365) -> dict:
    """Analyze holly_backtests results bucketed by VIX regime.

    Reads existing holly_backtests rows and joins each trade date to the
    daily VIX level to classify into BULL / CAUTIOUS / BEAR / CRISIS.
    Returns per-strategy, per-regime win rates and avg returns.
    Saves summary to extras_regime_filter.
    """
    _init_tables()
    today_str = datetime.now().strftime("%Y-%m-%d")

    # Load holly backtests
    try:
        conn = _conn()
        rows = conn.execute("""
            SELECT run_date, ticker, strategy, total_return, win_rate, sharpe, num_trades
            FROM holly_backtests
            ORDER BY run_date DESC
            LIMIT 2000
        """).fetchall()
        conn.close()
    except Exception as e:
        return {"status": "error", "message": str(e)}

    if not rows:
        return {"status": "no_data", "message": "No holly_backtests rows found. Run comprehensive backtest first."}

    vix = _vix_series(lookback_days + 30)
    if vix.empty:
        return {"status": "error", "message": "VIX data unavailable"}

    records = []
    for r in rows:
        run_date = pd.Timestamp(r["run_date"]).normalize()
        # Find closest VIX date
        vix_idx = vix.index.get_indexer([run_date], method="nearest")[0]
        if vix_idx < 0:
            continue
        vix_val = float(vix.iloc[vix_idx])
        regime  = _classify_regime(vix_val)
        records.append({
            "run_date":    r["run_date"],
            "ticker":      r["ticker"],
            "strategy":    r["strategy"],
            "total_return": r["total_return"] or 0.0,
            "win_rate":    r["win_rate"] or 0.0,
            "sharpe":      r["sharpe"] or 0.0,
            "num_trades":  r["num_trades"] or 0,
            "regime":      regime,
            "vix":         round(vix_val, 2),
        })

    if not records:
        return {"status": "no_data", "message": "Could not match any backtest rows to VIX dates"}

    df = pd.DataFrame(records)

    # Summary: strategy × regime
    summary = df.groupby(["strategy", "regime"]).agg(
        avg_return=("total_return", "mean"),
        avg_win_rate=("win_rate", "mean"),
        avg_sharpe=("sharpe", "mean"),
        total_trades=("num_trades", "sum"),
        n_runs=("run_date", "count"),
    ).reset_index()

    # Save to DB
    conn = _conn()
    conn.execute("DELETE FROM extras_regime_filter WHERE run_date = ?", (today_str,))
    for _, row in summary.iterrows():
        conn.execute("""
            INSERT INTO extras_regime_filter
              (run_date, strategy, ticker, regime, avg_return, win_rate, num_trades, sharpe)
            VALUES (?,?,?,?,?,?,?,?)
        """, (today_str, row["strategy"], "ALL", row["regime"],
              round(float(row["avg_return"]), 4), round(float(row["avg_win_rate"]), 4),
              int(row["total_trades"]), round(float(row["avg_sharpe"]), 4)))
    conn.commit()
    conn.close()

    # Best strategy per regime
    best_per_regime: dict[str, Any] = {}
    for regime in ["BULL", "CAUTIOUS", "BEAR", "CRISIS"]:
        sub = summary[summary["regime"] == regime].sort_values("avg_return", ascending=False)
        if not sub.empty:
            row = sub.iloc[0]
            best_per_regime[regime] = {
                "strategy": row["strategy"],
                "avg_return": round(float(row["avg_return"]), 4),
                "avg_win_rate": round(float(row["avg_win_rate"]), 4),
            }

    regime_counts = df["regime"].value_counts().to_dict()

    return {
        "status": "ok",
        "regime_distribution": regime_counts,
        "best_strategy_per_regime": best_per_regime,
        "detail": summary.round(4).to_dict("records"),
    }


# ═══════════════════════════════════════════════════════════════════════════
# 3. Time-of-Day Analysis
# ═══════════════════════════════════════════════════════════════════════════

# Session windows: (label, start_hour_ET, end_hour_ET)
_SESSIONS = [
    ("Opening",    9,  10),   # 9:30–10:00 AM ET
    ("Morning",   10,  12),   # 10:00–12:00 PM ET
    ("Lunch",     12,  14),   # 12:00–2:00 PM ET
    ("PowerHour", 15,  16),   # 3:00–4:00 PM ET
]

def _session_label(hour: int) -> str | None:
    for label, start, end in _SESSIONS:
        if start <= hour < end:
            return label
    return None


def run_time_of_day(tickers: list[str] | None = None, lookback_days: int = 180) -> dict:
    """Analyze RSI/momentum returns by time-of-day session using 1h bars.

    Downloads 1h bars for each ticker, computes hourly returns, and
    aggregates win rate and avg return per session window.
    Saves to extras_time_of_day.
    """
    _init_tables()
    today_str = datetime.now().strftime("%Y-%m-%d")

    if tickers is None:
        from engine.holly_nightly_backtest import COMPREHENSIVE_TICKERS
        tickers = COMPREHENSIVE_TICKERS[:10]  # limit to 10 for speed

    records = []

    for sym in tickers:
        logger.info("Time-of-day: %s", sym)
        df = _yf_download(sym, period="6mo", interval="1h")
        if df.empty or "Close" not in df.columns or len(df) < 50:
            continue

        df.index = pd.to_datetime(df.index)

        # Localize to ET if needed
        try:
            if df.index.tz is None:
                df.index = df.index.tz_localize("UTC").tz_convert("US/Eastern")
            else:
                df.index = df.index.tz_convert("US/Eastern")
        except Exception:
            pass

        df["ret"] = df["Close"].pct_change()
        df["hour"] = df.index.hour
        df["session"] = df["hour"].map(_session_label)
        df = df.dropna(subset=["ret", "session"])

        # Add net return after slippage
        df["net_ret"] = df["ret"] - SLIPPAGE_PCT

        for session in ["Opening", "Morning", "Lunch", "PowerHour"]:
            sub = df[df["session"] == session]["net_ret"]
            if len(sub) < 5:
                continue
            records.append({
                "run_date":  today_str,
                "strategy":  "hourly_momentum",
                "ticker":    sym,
                "session":   session,
                "avg_return": round(float(sub.mean()), 5),
                "win_rate":  round(float((sub > 0).mean()), 4),
                "num_trades": int(len(sub)),
            })

    if not records:
        return {"status": "no_data", "message": "No 1h bars returned"}

    # Save to DB
    conn = _conn()
    conn.execute("DELETE FROM extras_time_of_day WHERE run_date = ?", (today_str,))
    for r in records:
        conn.execute("""
            INSERT INTO extras_time_of_day
              (run_date, strategy, ticker, session, avg_return, win_rate, num_trades)
            VALUES (?,?,?,?,?,?,?)
        """, (r["run_date"], r["strategy"], r["ticker"], r["session"],
              r["avg_return"], r["win_rate"], r["num_trades"]))
    conn.commit()
    conn.close()

    df_res = pd.DataFrame(records)

    # Aggregate across all tickers per session
    by_session = df_res.groupby("session").agg(
        avg_return=("avg_return", "mean"),
        avg_win_rate=("win_rate", "mean"),
        total_bars=("num_trades", "sum"),
    ).reset_index()
    by_session = by_session.sort_values("avg_return", ascending=False)

    # Best ticker per session
    best_ticker_per_session: dict[str, Any] = {}
    for session in ["Opening", "Morning", "Lunch", "PowerHour"]:
        sub = df_res[df_res["session"] == session].sort_values("avg_return", ascending=False)
        if not sub.empty:
            best_ticker_per_session[session] = {
                "ticker": sub.iloc[0]["ticker"],
                "avg_return": sub.iloc[0]["avg_return"],
                "win_rate": sub.iloc[0]["win_rate"],
            }

    best_session = by_session.iloc[0]["session"] if not by_session.empty else "Unknown"

    return {
        "status": "ok",
        "best_session": best_session,
        "by_session": by_session.round(5).to_dict("records"),
        "best_ticker_per_session": best_ticker_per_session,
        "detail": df_res.sort_values(["session", "avg_return"], ascending=[True, False]).to_dict("records"),
    }


# ═══════════════════════════════════════════════════════════════════════════
# 4. CG Ratio Strategy (Copper/Gold)
# ═══════════════════════════════════════════════════════════════════════════

def run_cg_ratio(lookback_days: int = 730, ma_window: int = 20) -> dict:
    """Backtest Copper/Gold ratio crossover as SPY entry signal.

    Logic:
    - CG Ratio = CPER (copper ETF) / GLD (gold ETF) daily close
    - Entry: CG Ratio crosses ABOVE its {ma_window}-day MA
    - Exit: CG Ratio crosses BELOW its {ma_window}-day MA
    - Vehicle: SPY (long only, no short)
    - Costs: 0.1% slippage on entry and exit

    Tracks: total return, win rate, max drawdown, avg hold time,
            CG ratio level at entry/exit, comparison vs SPY B&H.
    """
    _init_tables()
    today_str = datetime.now().strftime("%Y-%m-%d")

    logger.info("CG Ratio: downloading CPER, GLD, SPY")
    period = f"{lookback_days // 365 + 1}y"

    df_cper = _yf_download("CPER", period=period, interval="1d")
    df_gld  = _yf_download("GLD",  period=period, interval="1d")
    df_spy  = _yf_download("SPY",  period=period, interval="1d")

    for name, df in [("CPER", df_cper), ("GLD", df_gld), ("SPY", df_spy)]:
        if df.empty or "Close" not in df.columns:
            return {"status": "error", "message": f"Could not download {name} data"}

    # Align on common dates
    cper = df_cper["Close"].dropna()
    gld  = df_gld["Close"].dropna()
    spy  = df_spy["Close"].dropna()

    cper.index = pd.to_datetime(cper.index).normalize()
    gld.index  = pd.to_datetime(gld.index).normalize()
    spy.index  = pd.to_datetime(spy.index).normalize()

    common_idx = cper.index.intersection(gld.index).intersection(spy.index)
    if len(common_idx) < ma_window + 10:
        return {"status": "error", "message": "Insufficient overlapping data"}

    cper = cper.loc[common_idx]
    gld  = gld.loc[common_idx]
    spy  = spy.loc[common_idx]

    cg_ratio = cper / gld
    cg_ma    = cg_ratio.rolling(ma_window).mean()

    # Generate signals: +1 when ratio > MA, 0 otherwise
    above_ma = (cg_ratio > cg_ma).astype(int)
    signal   = above_ma.diff()   # +1 = cross above, -1 = cross below

    trades = []
    in_trade   = False
    entry_date = None
    entry_spy  = None
    entry_cg   = None

    for date in common_idx[ma_window:]:
        sig = signal.get(date, 0)
        if sig == 1 and not in_trade:
            # Entry: CG crosses above MA
            in_trade   = True
            entry_date = date
            entry_spy  = float(spy.loc[date]) * (1 + SLIPPAGE_PCT)  # buy at ask
            entry_cg   = float(cg_ratio.loc[date])
        elif sig == -1 and in_trade:
            # Exit: CG crosses below MA
            exit_spy = float(spy.loc[date]) * (1 - SLIPPAGE_PCT)  # sell at bid
            pnl_pct  = (exit_spy - entry_spy) / entry_spy * 100
            hold_days = (date - entry_date).days
            trades.append({
                "entry_date":  entry_date.strftime("%Y-%m-%d"),
                "exit_date":   date.strftime("%Y-%m-%d"),
                "entry_price": round(entry_spy, 4),
                "exit_price":  round(exit_spy, 4),
                "pnl_pct":     round(pnl_pct, 3),
                "cg_ratio":    round(entry_cg, 6),
                "cg_ma20":     round(float(cg_ma.loc[entry_date]), 6),
                "win":         1 if pnl_pct > 0 else 0,
                "hold_days":   hold_days,
            })
            in_trade = False

    # Close any open trade at last price
    if in_trade and entry_date is not None:
        last_date = common_idx[-1]
        exit_spy  = float(spy.iloc[-1]) * (1 - SLIPPAGE_PCT)
        pnl_pct   = (exit_spy - entry_spy) / entry_spy * 100
        trades.append({
            "entry_date":  entry_date.strftime("%Y-%m-%d"),
            "exit_date":   last_date.strftime("%Y-%m-%d"),
            "entry_price": round(entry_spy, 4),
            "exit_price":  round(exit_spy, 4),
            "pnl_pct":     round(pnl_pct, 3),
            "cg_ratio":    round(entry_cg, 6),
            "cg_ma20":     round(float(cg_ma.loc[entry_date]), 6),
            "win":         1 if pnl_pct > 0 else 0,
            "hold_days":   (last_date - entry_date).days,
        })

    if not trades:
        return {"status": "no_trades", "message": "CG ratio generated no crossover signals"}

    # Save to DB
    conn = _conn()
    conn.execute("DELETE FROM extras_cg_ratio WHERE run_date = ?", (today_str,))
    for t in trades:
        conn.execute("""
            INSERT INTO extras_cg_ratio
              (run_date, entry_date, exit_date, entry_price, exit_price,
               pnl_pct, cg_ratio, cg_ma20, win, hold_days)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (today_str, t["entry_date"], t["exit_date"], t["entry_price"],
              t["exit_price"], t["pnl_pct"], t["cg_ratio"], t["cg_ma20"],
              t["win"], t["hold_days"]))
    conn.commit()
    conn.close()

    df_t = pd.DataFrame(trades)

    # Compound return
    total_return = float(((1 + df_t["pnl_pct"] / 100).prod() - 1) * 100)

    # SPY buy-and-hold return over same period
    spy_bh = float((spy.iloc[-1] / spy.iloc[0] - 1) * 100)

    # Max drawdown
    cum = (1 + df_t["pnl_pct"] / 100).cumprod()
    roll_max = cum.cummax()
    dd = ((cum - roll_max) / roll_max).min() * 100

    # Sharpe (using trade returns as the series)
    returns = df_t["pnl_pct"] / 100
    sharpe  = float(returns.mean() / (returns.std() + 1e-9) * (252 ** 0.5 / (365 / 252)))

    return {
        "status": "ok",
        "total_trades": len(trades),
        "win_rate": round(float(df_t["win"].mean()), 3),
        "total_return_pct": round(total_return, 2),
        "spy_bh_return_pct": round(spy_bh, 2),
        "vs_spy_pct": round(total_return - spy_bh, 2),
        "max_drawdown_pct": round(float(dd), 2),
        "avg_hold_days": round(float(df_t["hold_days"].mean()), 1),
        "sharpe": round(sharpe, 3),
        "avg_pnl_pct": round(float(df_t["pnl_pct"].mean()), 3),
        "ma_window": ma_window,
        "trades": trades,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Public entry point
# ═══════════════════════════════════════════════════════════════════════════

def run_all_extras() -> dict:
    """Run all four extra backtest modules and return combined results."""
    _init_tables()
    out = {}
    for name, fn in [
        ("earnings_straddle", run_earnings_straddle),
        ("regime_filter",     run_regime_filter),
        ("time_of_day",       run_time_of_day),
        ("cg_ratio",          run_cg_ratio),
    ]:
        try:
            out[name] = fn()
        except Exception as e:
            logger.exception("extras %s failed: %s", name, e)
            out[name] = {"status": "error", "message": str(e)}
    return out
