"""VectorBT Holodeck Expansion — Walk-forward, regime-aware, portfolio simulation.

All backtest results are saved to the strategy_backtests table.
"""
from __future__ import annotations
import json
import sqlite3
import numpy as np
import pandas as pd
from datetime import datetime
from rich.console import Console

console = Console()
DB = "data/trader.db"


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def _save_backtest(strategy_name, ticker, params, total_return, sharpe, max_dd,
                   win_rate, profit_factor, num_trades, notes=""):
    """Save results to strategy_backtests table."""
    conn = _conn()
    conn.execute("""
        INSERT INTO strategy_backtests
        (source, ticker, strategy_type, parameters, total_return, sharpe_ratio,
         max_drawdown, win_rate, profit_factor, num_trades, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        "holodeck", ticker, strategy_name, json.dumps(params),
        total_return, sharpe, max_dd, win_rate, profit_factor, num_trades, notes
    ))
    conn.commit()
    conn.close()


# ─── Feature 3A: Walk-Forward Optimization ─────────────────────────────────

def walk_forward_backtest(symbol="SPY", period="5y", in_sample_pct=0.7,
                          n_windows=5, rsi_range=(10, 40, 5),
                          strategy_name="walk_forward_rsi"):
    """Walk-forward optimization: optimize on in-sample, validate on out-of-sample."""
    import vectorbt as vbt
    import yfinance as yf

    data = yf.download(symbol, period=period, interval="1d", progress=False)
    close = data["Close"].squeeze()
    total_len = len(close)
    window_size = total_len // n_windows

    all_results = []

    for i in range(n_windows - 1):
        is_start = i * window_size
        is_end = is_start + int(window_size * in_sample_pct)
        oos_start = is_end
        oos_end = (i + 1) * window_size if i < n_windows - 2 else total_len

        is_data = close.iloc[is_start:is_end]
        oos_data = close.iloc[oos_start:oos_end]

        if len(is_data) < 20 or len(oos_data) < 10:
            continue

        # Optimize on in-sample
        best_sharpe = -999
        best_params = {}

        for rsi_w in range(*rsi_range):
            for rsi_buy in range(20, 40, 5):
                for rsi_sell in range(60, 85, 5):
                    try:
                        rsi = vbt.RSI.run(is_data, window=rsi_w)
                        entries = rsi.rsi_below(rsi_buy)
                        exits = rsi.rsi_above(rsi_sell)
                        pf = vbt.Portfolio.from_signals(
                            is_data, entries, exits,
                            init_cash=10000, fees=0.001, freq='1D'
                        )
                        sharpe = pf.sharpe_ratio()
                        if not np.isnan(sharpe) and sharpe > best_sharpe:
                            best_sharpe = sharpe
                            best_params = {"rsi_window": rsi_w, "rsi_buy": rsi_buy, "rsi_sell": rsi_sell}
                    except Exception:
                        continue

        if not best_params:
            continue

        # Validate on out-of-sample
        try:
            rsi_oos = vbt.RSI.run(oos_data, window=best_params["rsi_window"])
            entries_oos = rsi_oos.rsi_below(best_params["rsi_buy"])
            exits_oos = rsi_oos.rsi_above(best_params["rsi_sell"])
            pf_oos = vbt.Portfolio.from_signals(
                oos_data, entries_oos, exits_oos,
                init_cash=10000, fees=0.001, freq='1D'
            )

            oos_sharpe = pf_oos.sharpe_ratio()
            window_result = {
                "window": i + 1,
                "is_sharpe": round(best_sharpe, 3),
                "oos_sharpe": round(oos_sharpe if not np.isnan(oos_sharpe) else 0, 3),
                "oos_return": round(pf_oos.total_return() * 100, 2),
                "oos_max_dd": round(pf_oos.max_drawdown() * 100, 2),
                "best_params": best_params,
                "is_dates": f"{is_data.index[0].date()} to {is_data.index[-1].date()}",
                "oos_dates": f"{oos_data.index[0].date()} to {oos_data.index[-1].date()}"
            }
            all_results.append(window_result)
        except Exception:
            continue

    if not all_results:
        return {"error": "No valid windows produced results", "strategy_name": strategy_name}

    avg_oos_sharpe = np.mean([r["oos_sharpe"] for r in all_results])
    avg_oos_return = np.mean([r["oos_return"] for r in all_results])
    degradation = np.mean([r["is_sharpe"] - r["oos_sharpe"] for r in all_results])

    summary = {
        "strategy_name": strategy_name,
        "type": "walk_forward",
        "symbol": symbol,
        "n_windows": n_windows,
        "avg_oos_sharpe": round(avg_oos_sharpe, 3),
        "avg_oos_return": round(avg_oos_return, 2),
        "avg_degradation": round(degradation, 3),
        "overfitting_risk": "HIGH" if degradation > 0.5 else "MEDIUM" if degradation > 0.2 else "LOW",
        "windows": all_results
    }

    _save_backtest(strategy_name, symbol, summary, avg_oos_return, avg_oos_sharpe,
                   0, 0, 0, len(all_results), f"Walk-forward {n_windows} windows")

    return summary


# ─── Feature 3B: Regime-Aware Backtesting ──────────────────────────────────

def regime_aware_backtest(symbol="SPY", period="5y", rsi_window=14,
                          rsi_buy=30, rsi_sell=70,
                          strategy_name="regime_aware_rsi"):
    """Backtest partitioned by BEAR/BULL/SIDEWAYS regime (VIX-based)."""
    import vectorbt as vbt
    import yfinance as yf

    spy = yf.download(symbol, period=period, interval="1d", progress=False)
    vix = yf.download("^VIX", period=period, interval="1d", progress=False)
    close = spy["Close"].squeeze()
    vix_close = vix["Close"].squeeze().reindex(close.index, method="ffill")

    # Define regimes
    regime = pd.Series("SIDEWAYS", index=close.index)
    regime[vix_close > 25] = "BEAR"
    regime[vix_close < 15] = "BULL"

    # Run full backtest
    rsi = vbt.RSI.run(close, window=rsi_window)
    entries = rsi.rsi_below(rsi_buy)
    exits = rsi.rsi_above(rsi_sell)
    pf = vbt.Portfolio.from_signals(close, entries, exits, init_cash=10000, fees=0.001, freq='1D')

    # Partition by regime
    regime_results = {}
    for r in ["BEAR", "BULL", "SIDEWAYS"]:
        mask = regime == r
        if mask.sum() < 20:
            regime_results[r] = {"total_return": 0, "sharpe_ratio": 0, "n_trades": 0, "bar_count": int(mask.sum())}
            continue
        try:
            regime_close = close[mask]
            regime_rsi = vbt.RSI.run(regime_close, window=rsi_window)
            r_entries = regime_rsi.rsi_below(rsi_buy)
            r_exits = regime_rsi.rsi_above(rsi_sell)
            rpf = vbt.Portfolio.from_signals(regime_close, r_entries, r_exits, init_cash=10000, fees=0.001, freq='1D')
            sharpe = rpf.sharpe_ratio()
            regime_results[r] = {
                "total_return": round(rpf.total_return() * 100, 2),
                "sharpe_ratio": round(sharpe if not np.isnan(sharpe) else 0, 3),
                "max_drawdown": round(rpf.max_drawdown() * 100, 2),
                "n_trades": len(rpf.trades.records_readable),
                "bar_count": int(mask.sum())
            }
        except Exception:
            regime_results[r] = {"total_return": 0, "sharpe_ratio": 0, "n_trades": 0, "bar_count": int(mask.sum())}

    # Deployment recommendations
    recs = []
    for reg, data in regime_results.items():
        if data.get("sharpe_ratio", 0) > 0.5 and data.get("total_return", 0) > 5:
            recs.append(f"DEPLOY in {reg} (Sharpe {data['sharpe_ratio']}, Return {data['total_return']}%)")
        elif data.get("total_return", 0) < -10:
            recs.append(f"AVOID in {reg} (Return {data['total_return']}%)")
        else:
            recs.append(f"NEUTRAL in {reg}")

    overall_sharpe = pf.sharpe_ratio()
    summary = {
        "strategy_name": strategy_name,
        "type": "regime_aware",
        "symbol": symbol,
        "overall": {
            "total_return": round(pf.total_return() * 100, 2),
            "sharpe_ratio": round(overall_sharpe if not np.isnan(overall_sharpe) else 0, 3),
            "max_drawdown": round(pf.max_drawdown() * 100, 2),
        },
        "by_regime": regime_results,
        "recommendation": recs
    }

    _save_backtest(strategy_name, symbol, summary,
                   summary["overall"]["total_return"], summary["overall"]["sharpe_ratio"],
                   summary["overall"]["max_drawdown"], 0, 0, 0,
                   f"Regime-aware: {json.dumps(recs)}")

    return summary


# ─── Feature 3C: Portfolio-Level Simulation ────────────────────────────────

def portfolio_simulation(season=5):
    """Simulate entire arena as a portfolio — find concentration risk and correlation."""
    conn = _conn()

    positions = pd.read_sql("""
        SELECT player_id, symbol, qty, avg_price
        FROM positions
        WHERE player_id != 'steve-webull'
    """, conn)

    if positions.empty:
        conn.close()
        return {"error": "No positions found"}

    # Concentration by symbol
    symbol_exposure = positions.groupby("symbol").agg(
        total_qty=("qty", "sum"),
        num_models=("player_id", "nunique"),
        models=("player_id", lambda x: list(x))
    ).reset_index()

    total_cost = (positions["qty"] * positions["avg_price"]).sum()
    positions["cost"] = positions["qty"] * positions["avg_price"]
    symbol_cost = positions.groupby("symbol")["cost"].sum()
    concentration = (symbol_cost / total_cost * 100).round(2).to_dict() if total_cost > 0 else {}

    # Correlated holdings (same symbol, multiple models)
    correlated = {}
    for _, row in symbol_exposure.iterrows():
        if row["num_models"] > 1:
            correlated[row["symbol"]] = {"models": row["models"], "count": row["num_models"]}

    result = {
        "type": "portfolio_simulation",
        "season": season,
        "total_models": int(positions["player_id"].nunique()),
        "total_positions": len(positions),
        "unique_symbols": int(positions["symbol"].nunique()),
        "concentration": concentration,
        "correlated_holdings": correlated,
        "top_concentration_risk": sorted(concentration.items(), key=lambda x: x[1], reverse=True)[:5],
    }

    # Save to strategy_backtests
    conn.execute("""
        INSERT INTO strategy_backtests
        (source, ticker, strategy_type, parameters, total_return, notes)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        "holodeck", "PORTFOLIO", f"portfolio_sim_s{season}",
        json.dumps(result), 0, f"Season {season} portfolio concentration analysis"
    ))
    conn.commit()
    conn.close()

    return result
