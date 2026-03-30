"""Custom tools for strategy crew agents.

Standalone functions (no crewai dependency — Python 3.9 compatible).
Tools are called directly by strategy_crew.py agents.
"""
from __future__ import annotations
import json
import sqlite3
from datetime import datetime

DB = "data/trader.db"


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def query_backtest_history(limit=20):
    """Query strategy_backtests for past results."""
    conn = _conn()
    rows = conn.execute(
        "SELECT strategy_type, ticker, total_return, sharpe_ratio, "
        "max_drawdown, win_rate, profit_factor, num_trades, created_at "
        "FROM strategy_backtests ORDER BY created_at DESC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_market_regime():
    """Get current market regime."""
    import requests
    try:
        resp = requests.get("http://127.0.0.1:8080/api/regime", timeout=10)
        return resp.json() if resp.ok else {}
    except Exception:
        return {}


def get_strategy_lab_results():
    """Get current Strategy Lab strategies."""
    import requests
    try:
        resp = requests.get("http://127.0.0.1:8080/api/strategy-lab/strategies", timeout=10)
        return resp.json() if resp.ok else {}
    except Exception:
        return {}


def run_vectorbt_backtest(params):
    """Run a VectorBT backtest and save to strategy_backtests."""
    import vectorbt as vbt
    import yfinance as yf
    import numpy as np

    symbol = params.get("symbol", "SPY")
    period = params.get("period", "2y")
    rsi_window = params.get("rsi_window", 14)
    rsi_buy = params.get("rsi_buy_threshold", 30)
    rsi_sell = params.get("rsi_sell_threshold", 70)
    stop_loss = params.get("stop_loss_pct", 0.05)
    take_profit = params.get("take_profit_pct", 0.10)

    data = yf.download(symbol, period=period, interval="1d", progress=False)
    close = data["Close"].squeeze()
    rsi = vbt.RSI.run(close, window=rsi_window)
    entries = rsi.rsi_below(rsi_buy)
    exits = rsi.rsi_above(rsi_sell)
    pf = vbt.Portfolio.from_signals(
        close, entries, exits,
        sl_stop=stop_loss, tp_stop=take_profit,
        init_cash=10000, fees=0.001, freq='1D'
    )

    n_trades = len(pf.trades.records_readable)
    sharpe = pf.sharpe_ratio()
    results = {
        "strategy_name": params.get("name", "unnamed"),
        "symbol": symbol,
        "total_return": round(pf.total_return() * 100, 2),
        "sharpe_ratio": round(sharpe if not np.isnan(sharpe) else 0, 3),
        "max_drawdown": round(pf.max_drawdown() * 100, 2),
        "win_rate": round(pf.trades.win_rate() * 100, 2) if n_trades > 0 else 0,
        "profit_factor": round(pf.trades.profit_factor(), 3) if n_trades > 0 else 0,
        "total_trades": n_trades,
    }

    conn = _conn()
    conn.execute("""
        INSERT INTO strategy_backtests
        (source, ticker, strategy_type, parameters, total_return,
         sharpe_ratio, max_drawdown, win_rate, profit_factor, num_trades, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        "crewai", symbol, results["strategy_name"],
        json.dumps(params), results["total_return"],
        results["sharpe_ratio"], results["max_drawdown"],
        results["win_rate"], results["profit_factor"],
        results["total_trades"], "CrewAI backtest"
    ))
    conn.commit()
    conn.close()
    return results
