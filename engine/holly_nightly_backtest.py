"""Holly Nightly Backtest — VectorBT-powered comprehensive strategy backtester.

Two modes:
  1. run_comprehensive_backtest()  — 90-day sweep on fixed ticker list, all 4 strategies
  2. run_holly_nightly()           — 3 AM ET, top 50 volume movers from universe_scan

Results saved to data/backtest.db (holly_backtests + holly_winning_strategies tables).
Top 10 winning strategies from nightly run are cached for morning scan prioritization.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Paths ────────────────────────────────────────────────────────────────────
_BACKTEST_DB = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "backtest.db"
)
_TRADER_DB = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "trader.db"
)

# ── Fixed comprehensive backtest ticker list ──────────────────────────────────
COMPREHENSIVE_TICKERS = [
    "NVDA", "AMD", "MU", "AVGO", "META", "GOOGL", "AAPL",
    "AMZN", "MSFT", "TSLA", "TQQQ", "QQQ", "SPY", "IWM", "DELL",
    "XLE", "INTC", "NUKZ", "STAA", "SMR",  # Webull holdings additions
]

# ── Transaction cost model ────────────────────────────────────────────────────
# Alpaca: $0 commissions. Slippage: 0.1% per trade. Options: $0.65/contract.
SLIPPAGE_PCT     = 0.001   # 0.1% per trade (stock)
COMMISSION_STOCK = 0.00    # Alpaca equities
OPTIONS_CONTRACT = 0.65    # per contract (Alpaca options)
FEES             = SLIPPAGE_PCT  # used in vbt.Portfolio.from_signals(fees=FEES)

STRATEGY_TYPES = ["rsi", "macd", "bollinger", "sma_cross"]

# ── RSI default param ranges for sweep ───────────────────────────────────────
RSI_SWEEP = dict(windows=[10, 14, 20], entry_thresholds=[25, 30, 35], exit_thresholds=[65, 70, 75])
MACD_SWEEP = dict(fast_periods=[8, 10, 12], slow_periods=[21, 26, 30], signal_periods=[7, 9])
BB_SWEEP   = dict(windows=[15, 20, 25], std_devs=[1.5, 2.0, 2.5])
SMA_SWEEP  = dict(fast_windows=[5, 10, 20], slow_windows=[50, 100, 200])

# ── Gap strategy thresholds for Holly nightly ────────────────────────────────
GAP_UP_ENTRY   = 2.0   # % gap up to enter
GAP_DOWN_EXIT  = -1.0  # % gap down to exit


# ═══════════════════════════════════════════════════════════════════════════
# DB Setup
# ═══════════════════════════════════════════════════════════════════════════

def _conn_backtest():
    c = sqlite3.connect(_BACKTEST_DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def _init_tables():
    conn = _conn_backtest()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS holly_backtests (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date        TEXT NOT NULL,
            run_type        TEXT NOT NULL DEFAULT 'nightly',
            ticker          TEXT NOT NULL,
            strategy        TEXT NOT NULL,
            params          TEXT,
            period_days     INTEGER DEFAULT 90,
            total_return    REAL,
            win_rate        REAL,
            sharpe          REAL,
            max_drawdown    REAL,
            avg_hold_days   REAL,
            num_trades      INTEGER,
            profit_factor   REAL,
            spy_return      REAL,
            vs_spy          REAL,
            final_value     REAL,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS holly_winning_strategies (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date        TEXT NOT NULL,
            rank            INTEGER NOT NULL,
            ticker          TEXT NOT NULL,
            strategy        TEXT NOT NULL,
            params          TEXT,
            total_return    REAL,
            sharpe          REAL,
            win_rate        REAL,
            profit_factor   REAL,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(run_date, rank)
        );
    """)
    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

def _s(v):
    try:
        # Handle pandas Series/scalar
        f = float(v.iloc[0]) if hasattr(v, "iloc") else float(v) if v is not None else 0.0
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if (np.isnan(f) or np.isinf(f)) else f


def _stat(stats, key, decimals=2):
    v = stats.get(key, 0)
    return round(_s(v), decimals)


def _download(symbol: str, days: int = 90):
    """Download price data — tries backtest_market_data cache first, then yfinance."""
    cutoff = (datetime.now() - timedelta(days=days + 10)).strftime('%Y-%m-%d')

    # 1. Try local backtest.db cache first (fast, no rate limits)
    try:
        conn = _conn_backtest()
        rows = conn.execute("""
            SELECT trade_date, close FROM backtest_market_data
            WHERE symbol = ? AND trade_date >= ?
            ORDER BY trade_date ASC
        """, (symbol, cutoff)).fetchall()
        conn.close()
        if rows and len(rows) >= 20:
            dates  = pd.to_datetime([r[0] for r in rows])
            closes = pd.Series([r[1] for r in rows], index=dates, name=symbol, dtype=float)
            return closes.dropna()
    except Exception:
        pass

    # 2. Try trader.db universe_scan close prices
    try:
        conn2 = sqlite3.connect(_TRADER_DB, timeout=15)
        rows2 = conn2.execute("""
            SELECT scan_date, close FROM universe_scan
            WHERE ticker = ? AND scan_date >= ?
            ORDER BY scan_date ASC
        """, (symbol, cutoff)).fetchall()
        conn2.close()
        if rows2 and len(rows2) >= 20:
            dates  = pd.to_datetime([r[0] for r in rows2])
            closes = pd.Series([r[1] for r in rows2], index=dates, name=symbol, dtype=float)
            return closes.dropna()
    except Exception:
        pass

    # 3. Fallback: yfinance (may be rate-limited)
    import time as _time
    for attempt in range(2):
        try:
            import yfinance as yf
            df = yf.download(symbol, start=cutoff, progress=False, auto_adjust=True)
            if not df.empty:
                close = df["Close"].squeeze()
                close.index = pd.to_datetime(close.index)
                # Cache into backtest_market_data for future use
                try:
                    conn3 = _conn_backtest()
                    for date_idx, val in close.items():
                        conn3.execute("""
                            INSERT OR IGNORE INTO backtest_market_data
                            (symbol, trade_date, close)
                            VALUES (?, ?, ?)
                        """, (symbol, date_idx.strftime('%Y-%m-%d'), float(val)))
                    conn3.commit()
                    conn3.close()
                except Exception:
                    pass
                return close.dropna()
        except Exception as e:
            if attempt == 0:
                _time.sleep(2)
            else:
                logger.warning("_download %s failed: %s", symbol, e)
    return None


def _spy_return(days: int = 90) -> float:
    """Buy-and-hold SPY return over period."""
    try:
        import yfinance as yf
        start = (datetime.now() - timedelta(days=days + 5)).strftime('%Y-%m-%d')
        df = yf.download("SPY", start=start, progress=False, auto_adjust=True)
        closes = df["Close"].dropna()
        if len(closes) < 2:
            return 0.0
        return round(float((closes.iloc[-1] - closes.iloc[0]) / closes.iloc[0] * 100), 2)
    except Exception:
        return 0.0


def _avg_hold_days(pf) -> float:
    """Calculate average trade hold duration in days."""
    try:
        trades = pf.trades.records_readable
        if trades.empty:
            return 0.0
        durations = []
        for _, t in trades.iterrows():
            entry = t.get("Entry Timestamp")
            exit_ = t.get("Exit Timestamp")
            if entry and exit_:
                d = (pd.Timestamp(exit_) - pd.Timestamp(entry)).days
                durations.append(max(0, d))
        return round(float(np.mean(durations)), 1) if durations else 0.0
    except Exception:
        return 0.0


# ═══════════════════════════════════════════════════════════════════════════
# Strategy runners — return dict of metrics
# ═══════════════════════════════════════════════════════════════════════════

def _run_rsi(data, params: dict, cash: float = 10_000, fees: float = 0.001) -> dict | None:
    try:
        import vectorbt as vbt
        window   = params.get("window", 14)
        entry_th = params.get("entry", 30)
        exit_th  = params.get("exit", 70)
        rsi      = vbt.RSI.run(data, window=window)
        entries  = rsi.rsi_crossed_above(entry_th)
        exits    = rsi.rsi_crossed_below(exit_th)
        # Force-close open trades on last bar so win_rate/profit_factor aren't nan
        exits = exits.copy()
        exits.iloc[-1] = True
        pf       = vbt.Portfolio.from_signals(data, entries, exits, freq="1D", fees=fees, init_cash=cash)
        stats    = pf.stats()
        return {
            "total_return": _stat(stats, "Total Return [%]"),
            "win_rate":     _stat(stats, "Win Rate [%]"),
            "sharpe":       _stat(stats, "Sharpe Ratio", 3),
            "max_drawdown": _stat(stats, "Max Drawdown [%]"),
            "profit_factor":_stat(stats, "Profit Factor"),
            "num_trades":   int(_s(stats.get("Total Trades", 0))),
            "final_value":  round(_s(pf.final_value()), 2),
            "avg_hold":     _avg_hold_days(pf),
        }
    except Exception as e:
        logger.debug("_run_rsi error: %s", e)
        return None


def _run_macd(data, params: dict, cash: float = 10_000, fees: float = 0.001) -> dict | None:
    try:
        import vectorbt as vbt
        fast   = params.get("fast", 12)
        slow   = params.get("slow", 26)
        sig    = params.get("signal", 9)
        if fast >= slow:
            return None
        macd     = vbt.MACD.run(data, fast_window=fast, slow_window=slow, signal_window=sig)
        entries  = macd.macd_crossed_above(macd.signal)
        exits    = macd.macd_crossed_below(macd.signal)
        # Force-close open trades on last bar so win_rate/profit_factor aren't nan
        exits = exits.copy()
        exits.iloc[-1] = True
        pf       = vbt.Portfolio.from_signals(data, entries, exits, freq="1D", fees=fees, init_cash=cash)
        stats    = pf.stats()
        return {
            "total_return": _stat(stats, "Total Return [%]"),
            "win_rate":     _stat(stats, "Win Rate [%]"),
            "sharpe":       _stat(stats, "Sharpe Ratio", 3),
            "max_drawdown": _stat(stats, "Max Drawdown [%]"),
            "profit_factor":_stat(stats, "Profit Factor"),
            "num_trades":   int(_s(stats.get("Total Trades", 0))),
            "final_value":  round(_s(pf.final_value()), 2),
            "avg_hold":     _avg_hold_days(pf),
        }
    except Exception as e:
        logger.debug("_run_macd error: %s", e)
        return None


def _run_bollinger(data, params: dict, cash: float = 10_000, fees: float = 0.001) -> dict | None:
    try:
        import vectorbt as vbt
        window   = params.get("window", 20)
        std      = params.get("std", 2.0)
        bb       = vbt.BBANDS.run(data, window=window, alpha=std)
        entries  = data < bb.lower
        exits    = data > bb.middle
        # Force-close open trades on last bar so win_rate/profit_factor aren't nan
        exits = exits.copy()
        exits.iloc[-1] = True
        pf       = vbt.Portfolio.from_signals(data, entries, exits, freq="1D", fees=fees, init_cash=cash)
        stats    = pf.stats()
        return {
            "total_return": _stat(stats, "Total Return [%]"),
            "win_rate":     _stat(stats, "Win Rate [%]"),
            "sharpe":       _stat(stats, "Sharpe Ratio", 3),
            "max_drawdown": _stat(stats, "Max Drawdown [%]"),
            "profit_factor":_stat(stats, "Profit Factor"),
            "num_trades":   int(_s(stats.get("Total Trades", 0))),
            "final_value":  round(_s(pf.final_value()), 2),
            "avg_hold":     _avg_hold_days(pf),
        }
    except Exception as e:
        logger.debug("_run_bollinger error: %s", e)
        return None


def _run_sma_cross(data, params: dict, cash: float = 10_000, fees: float = 0.001) -> dict | None:
    try:
        import vectorbt as vbt
        fast     = params.get("fast", 10)
        slow     = params.get("slow", 50)
        if fast >= slow:
            return None
        fast_ma  = vbt.MA.run(data, window=fast)
        slow_ma  = vbt.MA.run(data, window=slow)
        entries  = fast_ma.ma_crossed_above(slow_ma.ma)
        exits    = fast_ma.ma_crossed_below(slow_ma.ma)
        # Force-close open trades on last bar so win_rate/profit_factor aren't nan
        exits = exits.copy()
        exits.iloc[-1] = True
        pf       = vbt.Portfolio.from_signals(data, entries, exits, freq="1D", fees=fees, init_cash=cash)
        stats    = pf.stats()
        return {
            "total_return": _stat(stats, "Total Return [%]"),
            "win_rate":     _stat(stats, "Win Rate [%]"),
            "sharpe":       _stat(stats, "Sharpe Ratio", 3),
            "max_drawdown": _stat(stats, "Max Drawdown [%]"),
            "profit_factor":_stat(stats, "Profit Factor"),
            "num_trades":   int(_s(stats.get("Total Trades", 0))),
            "final_value":  round(_s(pf.final_value()), 2),
            "avg_hold":     _avg_hold_days(pf),
        }
    except Exception as e:
        logger.debug("_run_sma_cross error: %s", e)
        return None


def _run_gap(data, params: dict, cash: float = 10_000, fees: float = 0.001) -> dict | None:
    """Gap-up entry strategy: buy when open > prev_close by gap_pct%, sell on close."""
    try:
        import vectorbt as vbt
        import yfinance as yf

        # Need OHLC for gap detection
        symbol = params.get("_symbol", "SPY")
        days   = params.get("_days", 90)
        start  = (datetime.now() - timedelta(days=days + 10)).strftime('%Y-%m-%d')
        df     = yf.download(symbol, start=start, progress=False, auto_adjust=True)
        df     = df.dropna()
        if len(df) < 20:
            return None

        gap_pct  = params.get("gap_pct", GAP_UP_ENTRY)
        # Flatten any MultiIndex columns from yfinance
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
        close_col = df["Close"].squeeze()
        open_col  = df["Open"].squeeze()
        prev_cls  = close_col.shift(1)
        gaps      = (open_col - prev_cls) / prev_cls * 100
        entries   = (gaps >= gap_pct).fillna(False)
        # Exit: next bar (hold 1 day)
        exits     = entries.shift(1).fillna(False)

        close_s  = close_col
        # Force-close open trades on last bar so win_rate/profit_factor aren't nan
        exits = exits.copy()
        exits.iloc[-1] = True
        pf       = vbt.Portfolio.from_signals(
            close_s, entries, exits, freq="1D", fees=fees, init_cash=cash
        )
        stats    = pf.stats()
        return {
            "total_return": _stat(stats, "Total Return [%]"),
            "win_rate":     _stat(stats, "Win Rate [%]"),
            "sharpe":       _stat(stats, "Sharpe Ratio", 3),
            "max_drawdown": _stat(stats, "Max Drawdown [%]"),
            "profit_factor":_stat(stats, "Profit Factor"),
            "num_trades":   int(_s(stats.get("Total Trades", 0))),
            "final_value":  round(_s(pf.final_value()), 2),
            "avg_hold":     _avg_hold_days(pf),
        }
    except Exception as e:
        logger.debug("_run_gap error: %s", e)
        return None


# ═══════════════════════════════════════════════════════════════════════════
# Best-params sweep — fast mini sweep to find optimal params per strategy
# ═══════════════════════════════════════════════════════════════════════════

def _sweep_best_params(data, strategy: str) -> dict:
    """Run mini param sweep and return the best params by total return.
    Caps window sizes to available data length so short series still run."""
    n = len(data)
    best = {"total_return": -9999}
    best_params: dict = {}

    if strategy == "rsi":
        windows = [w for w in RSI_SWEEP["windows"] if w < n - 5]
        if not windows:
            windows = [min(RSI_SWEEP["windows"])]
        for w in windows:
            for e in RSI_SWEEP["entry_thresholds"]:
                for x in RSI_SWEEP["exit_thresholds"]:
                    if e >= x:
                        continue
                    r = _run_rsi(data, {"window": w, "entry": e, "exit": x})
                    if r and r["total_return"] > best["total_return"]:
                        best = r
                        best_params = {"window": w, "entry": e, "exit": x}

    elif strategy == "macd":
        slow_max = n - 5
        for f in MACD_SWEEP["fast_periods"]:
            for s in [sp for sp in MACD_SWEEP["slow_periods"] if sp <= slow_max]:
                if f >= s:
                    continue
                for sig in MACD_SWEEP["signal_periods"]:
                    r = _run_macd(data, {"fast": f, "slow": s, "signal": sig})
                    if r and r["total_return"] > best["total_return"]:
                        best = r
                        best_params = {"fast": f, "slow": s, "signal": sig}

    elif strategy == "bollinger":
        windows = [w for w in BB_SWEEP["windows"] if w < n - 5]
        if not windows:
            windows = [min(BB_SWEEP["windows"])]
        for w in windows:
            for std in BB_SWEEP["std_devs"]:
                r = _run_bollinger(data, {"window": w, "std": std})
                if r and r["total_return"] > best["total_return"]:
                    best = r
                    best_params = {"window": w, "std": std}

    elif strategy == "sma_cross":
        # Only use windows that fit within available data
        slow_max = n - 5
        for f in SMA_SWEEP["fast_windows"]:
            for s in [sp for sp in SMA_SWEEP["slow_windows"] if sp <= slow_max]:
                if f >= s:
                    continue
                r = _run_sma_cross(data, {"fast": f, "slow": s})
                if r and r["total_return"] > best["total_return"]:
                    best = r
                    best_params = {"fast": f, "slow": s}

    return best_params


def _run_strategy(data, strategy: str, params: dict) -> dict | None:
    if strategy == "rsi":
        return _run_rsi(data, params)
    elif strategy == "macd":
        return _run_macd(data, params)
    elif strategy == "bollinger":
        return _run_bollinger(data, params)
    elif strategy == "sma_cross":
        return _run_sma_cross(data, params)
    elif strategy == "gap":
        return _run_gap(data, params)
    return None


# ═══════════════════════════════════════════════════════════════════════════
# Save helpers
# ═══════════════════════════════════════════════════════════════════════════

def _save_result(run_date: str, run_type: str, ticker: str, strategy: str,
                 params: dict, metrics: dict, spy_ret: float, days: int = 90):
    conn = _conn_backtest()
    vs_spy = round(metrics["total_return"] - spy_ret, 2)
    conn.execute("""
        INSERT INTO holly_backtests
        (run_date, run_type, ticker, strategy, params, period_days,
         total_return, win_rate, sharpe, max_drawdown, avg_hold_days,
         num_trades, profit_factor, spy_return, vs_spy, final_value)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        run_date, run_type, ticker, strategy, json.dumps(params), days,
        metrics["total_return"], metrics["win_rate"], metrics["sharpe"],
        metrics["max_drawdown"], metrics.get("avg_hold", 0.0),
        metrics["num_trades"], metrics["profit_factor"],
        spy_ret, vs_spy, metrics["final_value"],
    ))
    conn.commit()
    conn.close()


def _save_winning_strategies(run_date: str, winners: list[dict]):
    """Persist top-N winners to holly_winning_strategies for morning scan."""
    conn = _conn_backtest()
    conn.execute("DELETE FROM holly_winning_strategies WHERE run_date = ?", (run_date,))
    for i, w in enumerate(winners[:10], start=1):
        conn.execute("""
            INSERT OR REPLACE INTO holly_winning_strategies
            (run_date, rank, ticker, strategy, params,
             total_return, sharpe, win_rate, profit_factor)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            run_date, i, w["ticker"], w["strategy"], json.dumps(w.get("params", {})),
            w["total_return"], w["sharpe"], w["win_rate"], w["profit_factor"],
        ))
    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# Top-N volume movers from universe_scan (previous trading day)
# ═══════════════════════════════════════════════════════════════════════════

def _get_top_volume_movers(n: int = 50) -> list[str]:
    """Pull top N tickers by volume_ratio from the most recent universe_scan."""
    try:
        conn = sqlite3.connect(_TRADER_DB, timeout=30)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT ticker FROM universe_scan
            WHERE scan_date = (SELECT MAX(scan_date) FROM universe_scan)
            ORDER BY volume_ratio DESC, volume DESC
            LIMIT ?
        """, (n,)).fetchall()
        conn.close()
        return [r["ticker"] for r in rows] if rows else []
    except Exception as e:
        logger.error("_get_top_volume_movers failed: %s", e)
        return []


# ═══════════════════════════════════════════════════════════════════════════
# Report generation
# ═══════════════════════════════════════════════════════════════════════════

def _generate_report(results: list[dict], spy_ret: float, run_type: str, run_date: str) -> str:
    """Generate a text summary report."""
    lines = [
        f"╔{'═'*70}╗",
        f"║  HOLLY BACKTEST REPORT — {run_date}  ({run_type.upper()})".ljust(71) + "║",
        f"╚{'═'*70}╝",
        f"  SPY Buy-and-Hold (90d): {spy_ret:+.2f}%",
        f"  Tickers tested: {len(set(r['ticker'] for r in results))}",
        f"  Total strategy runs: {len(results)}",
        "",
        f"{'Ticker':<8} {'Strategy':<12} {'Return':>8} {'WinRate':>8} {'Sharpe':>7} {'MaxDD':>8} {'Trades':>7} {'PF':>6} {'vs SPY':>8}",
        f"{'─'*8} {'─'*12} {'─'*8} {'─'*8} {'─'*7} {'─'*8} {'─'*7} {'─'*6} {'─'*8}",
    ]

    sorted_results = sorted(results, key=lambda x: x["total_return"], reverse=True)
    for r in sorted_results[:30]:
        vs = r["total_return"] - spy_ret
        lines.append(
            f"{r['ticker']:<8} {r['strategy']:<12} "
            f"{r['total_return']:>+7.2f}% {r['win_rate']:>7.1f}% "
            f"{r['sharpe']:>7.3f} {r['max_drawdown']:>+7.2f}% "
            f"{r['num_trades']:>7d} {r['profit_factor']:>6.2f} "
            f"{vs:>+7.2f}%"
        )

    # Per-strategy summary
    lines += ["", "── Strategy Averages ─────────────────────────────────────────────────"]
    for strat in (STRATEGY_TYPES + ["gap"]):
        subset = [r for r in results if r["strategy"] == strat]
        if not subset:
            continue
        avg_ret  = np.mean([r["total_return"] for r in subset])
        avg_sr   = np.mean([r["sharpe"] for r in subset])
        avg_wr   = np.mean([r["win_rate"] for r in subset])
        lines.append(f"  {strat:<12}: avg_return={avg_ret:+.2f}%  sharpe={avg_sr:.3f}  win_rate={avg_wr:.1f}%")

    # Top 10 winners
    lines += ["", "── Top 10 Winning Strategies ─────────────────────────────────────────"]
    for i, r in enumerate(sorted_results[:10], 1):
        lines.append(
            f"  #{i:>2}  {r['ticker']:<6} {r['strategy']:<12} "
            f"return={r['total_return']:+.2f}%  sharpe={r['sharpe']:.3f}  "
            f"trades={r['num_trades']}  params={r.get('params', {})}"
        )

    lines += ["", f"Generated: {datetime.now().isoformat()}", ""]
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════
# PUBLIC: 90-day comprehensive backtest
# ═══════════════════════════════════════════════════════════════════════════

def run_comprehensive_backtest(
    tickers: list[str] | None = None,
    days: int = 90,
    cash: float = 10_000,
) -> dict:
    """
    Run 90-day comprehensive backtest on the fixed ticker list.
    For each ticker × strategy: sweep params first, then run full backtest.
    Saves all results to data/backtest.db holly_backtests table.
    Returns summary dict + text report.
    """
    _init_tables()
    tickers   = tickers or COMPREHENSIVE_TICKERS
    run_date  = datetime.now().strftime("%Y-%m-%d")
    spy_ret   = _spy_return(days)
    all_results: list[dict] = []

    logger.info("Holly 90d comprehensive backtest: %d tickers × %d strategies", len(tickers), len(STRATEGY_TYPES))

    for ticker in tickers:
        logger.info("  → %s", ticker)
        data = _download(ticker, days=days)
        if data is None or len(data) < 20:
            logger.warning("  skip %s: insufficient data", ticker)
            continue

        for strategy in STRATEGY_TYPES:
            try:
                # Sweep to find best params
                best_params = _sweep_best_params(data, strategy)
                if not best_params:
                    continue

                metrics = _run_strategy(data, strategy, best_params)
                if not metrics:
                    continue

                record = {
                    "ticker":       ticker,
                    "strategy":     strategy,
                    "params":       best_params,
                    **metrics,
                }
                all_results.append(record)
                _save_result(run_date, "comprehensive", ticker, strategy,
                             best_params, metrics, spy_ret, days)

            except Exception as e:
                logger.error("  %s/%s error: %s", ticker, strategy, e)

    if not all_results:
        return {"status": "error", "message": "No results produced"}

    # Save top 10 winners
    sorted_results = sorted(all_results, key=lambda x: x["total_return"], reverse=True)
    _save_winning_strategies(run_date, sorted_results)

    report = _generate_report(all_results, spy_ret, "comprehensive", run_date)
    # Write report to data dir
    report_path = os.path.join(os.path.dirname(_BACKTEST_DB), f"holly_report_{run_date}.txt")
    try:
        with open(report_path, "w") as f:
            f.write(report)
        logger.info("Report written → %s", report_path)
    except Exception as e:
        logger.warning("Could not write report: %s", e)

    top = sorted_results[0] if sorted_results else {}
    return {
        "status":         "ok",
        "run_date":       run_date,
        "tickers_tested": len(tickers),
        "total_runs":     len(all_results),
        "spy_return":     spy_ret,
        "best":           top,
        "top_10":         sorted_results[:10],
        "report_path":    report_path,
        "report":         report,
    }


# ═══════════════════════════════════════════════════════════════════════════
# PUBLIC: Holly Nightly Backtest — 3 AM ET
# ═══════════════════════════════════════════════════════════════════════════

def run_holly_nightly(days: int = 90, cash: float = 10_000) -> dict:
    """
    Holly-style nightly backtest:
      - Fetch top 50 volume movers from universe_scan (previous day)
      - Run RSI, MACD, Bollinger, and gap strategies on each
      - Save top 10 winning strategies to holly_winning_strategies
      - Returns summary with top winners for morning scan prioritization
    """
    _init_tables()
    run_date = datetime.now().strftime("%Y-%m-%d")
    spy_ret  = _spy_return(days)

    tickers = _get_top_volume_movers(50)
    if not tickers:
        logger.warning("Holly nightly: no volume movers found in universe_scan, falling back to comprehensive list")
        tickers = COMPREHENSIVE_TICKERS

    nightly_strategies = ["rsi", "macd", "bollinger", "gap"]
    all_results: list[dict] = []

    logger.info("Holly nightly backtest: %d tickers × %d strategies", len(tickers), len(nightly_strategies))

    for ticker in tickers:
        data = _download(ticker, days=days)
        if data is None or len(data) < 20:
            continue

        for strategy in nightly_strategies:
            try:
                if strategy == "gap":
                    params  = {"gap_pct": GAP_UP_ENTRY, "_symbol": ticker, "_days": days}
                    metrics = _run_gap(data, params, cash=cash)
                    clean_params = {"gap_pct": GAP_UP_ENTRY}
                else:
                    best_params = _sweep_best_params(data, strategy)
                    if not best_params:
                        continue
                    metrics = _run_strategy(data, strategy, best_params)
                    clean_params = best_params

                if not metrics:
                    continue

                record = {
                    "ticker":   ticker,
                    "strategy": strategy,
                    "params":   clean_params,
                    **metrics,
                }
                all_results.append(record)
                _save_result(run_date, "nightly", ticker, strategy,
                             clean_params, metrics, spy_ret, days)

            except Exception as e:
                logger.error("Holly nightly %s/%s error: %s", ticker, strategy, e)

    if not all_results:
        return {"status": "error", "message": "No results from nightly backtest"}

    sorted_results = sorted(all_results, key=lambda x: x["total_return"], reverse=True)
    _save_winning_strategies(run_date, sorted_results)

    report = _generate_report(all_results, spy_ret, "nightly", run_date)
    report_path = os.path.join(os.path.dirname(_BACKTEST_DB), f"holly_nightly_{run_date}.txt")
    try:
        with open(report_path, "w") as f:
            f.write(report)
    except Exception as e:
        logger.warning("Could not write nightly report: %s", e)

    logger.info(
        "Holly nightly complete: %d results, top=%s %s %.2f%%",
        len(all_results),
        sorted_results[0]["ticker"] if sorted_results else "?",
        sorted_results[0]["strategy"] if sorted_results else "?",
        sorted_results[0]["total_return"] if sorted_results else 0,
    )

    return {
        "status":       "ok",
        "run_date":     run_date,
        "tickers":      len(tickers),
        "total_runs":   len(all_results),
        "spy_return":   spy_ret,
        "top_10":       sorted_results[:10],
        "report_path":  report_path,
    }


# ═══════════════════════════════════════════════════════════════════════════
# PUBLIC: Get winning strategies for morning scan prioritization
# ═══════════════════════════════════════════════════════════════════════════

def get_holly_winning_tickers(n: int = 10) -> list[dict]:
    """
    Return today's (or most recent) top-N winning tickers/strategies.
    Called by the morning scan to boost priority for matching symbols.
    Returns list of {ticker, strategy, total_return, sharpe, params}.
    """
    try:
        _init_tables()
        conn = _conn_backtest()
        rows = conn.execute("""
            SELECT w.ticker, w.strategy, w.total_return, w.sharpe, w.win_rate,
                   w.profit_factor, w.params, w.run_date
            FROM holly_winning_strategies w
            WHERE w.run_date = (SELECT MAX(run_date) FROM holly_winning_strategies)
            ORDER BY w.rank ASC
            LIMIT ?
        """, (n,)).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error("get_holly_winning_tickers failed: %s", e)
        return []


# Initialize tables at import
_init_tables()
