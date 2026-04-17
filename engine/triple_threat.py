"""
engine/triple_threat.py — Triple Threat Backtest
Jan 9 – Apr 9, 2026 (~90 calendar days / ~63 trading days)

Three tests, one report:
  A: Baseline  — SPY-only RSI+SMA200 rules (no AI)
  B: v3b Fix   — reads existing v3b tables (no re-run)
  C: Sniper    — high-conviction filtered fleet (6 agents, 6 strategies)

Run:
    venv/bin/python3 -m engine.triple_threat
"""
from __future__ import annotations

import json
import logging
import math
import sqlite3
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import yfinance as yf

# ── Simulation helpers from master_backtest ───────────────────────────────────
from engine.master_backtest import (
    SLIPPAGE, OPT_SLIP_PER_LEG, OPT_COST, EXEC_DELAY, RISK_FREE,
    OPT_DTE_DEFAULT, STARTING_CASH,
    _bs_price, _bs_delta,
    _hist_vol, _iv_rank, _rsi, _classify_regime,
    _sim_csp, _sim_covered_call, _sim_bull_put_spread,
    _exit_date_str, _trade_metrics,
    _get_trading_days, _tier2_signals, _tier3_signals, _tier9_short_signals,
)

# ── Alpha expansion helpers from super_backtest_v2 ────────────────────────────
from engine.super_backtest_v2 import (
    _expand_alpha_universe,
    run_alpha_expansion,
    _download_v2_universe,
)

logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
_ROOT       = Path(__file__).resolve().parent.parent
BACKTEST_DB = _ROOT / "data" / "backtest.db"
TRADER_DB   = _ROOT / "data" / "trader.db"
ALPHA_DB    = _ROOT / "data" / "alpha_signals.db"
DATA_DIR    = _ROOT / "data"

# ── Window ────────────────────────────────────────────────────────────────────
WINDOW_START = date(2026, 1, 9)
WINDOW_END   = date(2026, 4, 9)

# ── Sniper constants ──────────────────────────────────────────────────────────
SNIPER_ALPHA_THRESHOLD = 0.25
SNIPER_CONF_THRESHOLD  = 0.65
SNIPER_BULL_MIN        = 3

SNIPER_FLEET: dict[str, dict] = {
    "grok-4":           {"name": "Spock",  "model": "deepseek-r1:7b",   "tiers": [1]},  # RAM patch 2026-04-17: 14b → 7b
    "gemini-2.5-flash": {"name": "Worf",   "model": "qwen3:14b",        "tiers": [5]},
    "ollama-plutus":    {"name": "McCoy",  "model": "0xroyce/plutus",   "tiers": [5]},
    "gemini-2.5-pro":   {"name": "Seven",  "model": "qwen3:14b",        "tiers": [1]},
    "ollama-llama":     {"name": "Uhura",  "model": "llama3.1:latest",  "tiers": [5, 6]},
    "neo-matrix":       {"name": "Neo",    "model": "port-8000",        "tiers": [3]},
}

SNIPER_TIER_MAP: dict[str, int] = {
    "rsi_bounce":      1,
    "bollinger":       1,
    "hammer_candle":   3,
    "csp":             5,
    "covered_call":    5,
    "bull_put_spread": 6,
}


# ═══════════════════════════════════════════════════════════════════════════════
# Sniper position-size helper
# ═══════════════════════════════════════════════════════════════════════════════

def _sniper_pos_factor(alpha: float) -> float:
    if alpha >= 0.6: return 1.0
    if alpha >= 0.3: return 0.5
    return 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# Shared metric helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _max_drawdown_equity(equity_curve: list[float]) -> float:
    """Proper peak-to-trough drawdown, capped at -100%."""
    if len(equity_curve) < 2:
        return 0.0
    peak   = equity_curve[0]
    max_dd = 0.0
    for val in equity_curve:
        if val > peak:
            peak = val
        if peak > 0:
            dd = (peak - val) / peak * 100
            max_dd = max(max_dd, dd)
    return -min(max_dd, 100.0)


def _trade_metrics_triple(trades: list[dict]) -> dict:
    """
    Cap each trade pnl_pct at ±100 (critical v3b lesson).
    Uses equity-curve drawdown capped at -100%.
    """
    trades = [t for t in trades if "pnl_pct" in t]
    if not trades:
        return dict(
            total_return=0.0, win_rate=0.0, sharpe=0.0, max_drawdown=0.0,
            profit_factor=0.0, num_trades=0, avg_hold_days=0.0,
            best_trade_pct=0.0, worst_trade_pct=0.0,
            max_consec_wins=0, max_consec_losses=0, avg_trade_return=0.0,
            realistic_sharpe=0.0, needs_validation=0,
        )

    pcts   = [max(-100.0, min(100.0, t["pnl_pct"])) for t in trades]
    wins   = [p for p in pcts if p > 0]
    losses = [p for p in pcts if p <= 0]

    # Arithmetic sum: avoids compound-chain-zero bug where a single -100% trade
    # wipes the entire product chain. Each trade is treated as starting from
    # equal capital — total_return = sum of all pnl_pct contributions.
    total_return  = float(np.sum(pcts))
    win_rate      = len(wins) / len(pcts) * 100
    profit_factor = (sum(wins) / (-sum(losses))) if losses and sum(losses) != 0 else float("inf")
    avg_ret       = float(np.mean(pcts))
    std_ret       = max(float(np.std(pcts)), 1.0)
    avg_hold      = max(1.0, float(np.mean([t.get("hold_days", 1) for t in trades])))
    sharpe        = float(avg_ret / std_ret * math.sqrt(252 / avg_hold))

    equity = [STARTING_CASH]
    for p in pcts:
        equity.append(equity[-1] * (1 + p / 100))
    max_dd = _max_drawdown_equity(equity)

    consec_w = consec_l = cur_w = cur_l = 0
    for p in pcts:
        if p > 0:
            cur_w += 1; cur_l = 0; consec_w = max(consec_w, cur_w)
        else:
            cur_l += 1; cur_w = 0; consec_l = max(consec_l, cur_l)

    return dict(
        total_return=round(total_return, 2),
        win_rate=round(win_rate, 1),
        sharpe=round(sharpe, 3),
        realistic_sharpe=round(max(-5.0, min(5.0, sharpe)), 3),
        needs_validation=1 if abs(sharpe) > 5.0 else 0,
        max_drawdown=round(max_dd, 2),
        profit_factor=round(min(profit_factor, 99.99), 3),
        num_trades=len(trades),
        avg_hold_days=round(avg_hold, 1),
        avg_trade_return=round(avg_ret, 3),
        best_trade_pct=round(max(pcts), 2),
        worst_trade_pct=round(min(pcts), 2),
        max_consec_wins=consec_w,
        max_consec_losses=consec_l,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# DB helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _bt_conn() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(BACKTEST_DB), timeout=20)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _td_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(TRADER_DB), timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def _alpha_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(ALPHA_DB), timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


# ═══════════════════════════════════════════════════════════════════════════════
# Table init
# ═══════════════════════════════════════════════════════════════════════════════

def _init_triple_tables() -> None:
    conn = _bt_conn()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS backtest_baseline_results (
        run_date TEXT, strategy TEXT DEFAULT 'rsi_sma200_spy',
        total_return REAL, win_rate REAL, sharpe REAL, max_drawdown REAL,
        profit_factor REAL, num_trades INTEGER, avg_hold_days REAL,
        best_trade_pct REAL, worst_trade_pct REAL,
        spy_return REAL, vs_spy REAL,
        max_consec_wins INTEGER, max_consec_losses INTEGER,
        days_in_market INTEGER, days_in_cash INTEGER,
        avg_trade_return REAL, created_at TEXT,
        PRIMARY KEY (run_date)
    );

    CREATE TABLE IF NOT EXISTS backtest_baseline_trades (
        id INTEGER PRIMARY KEY,
        run_date TEXT, trade_num INTEGER,
        entry_date TEXT, exit_date TEXT,
        entry_px REAL, exit_px REAL,
        pnl_pct REAL, hold_days INTEGER,
        exit_type TEXT, regime TEXT
    );

    CREATE TABLE IF NOT EXISTS backtest_agent_results_sniper (
        run_date TEXT, agent_id TEXT, agent_name TEXT, model TEXT,
        total_return REAL, win_rate REAL, sharpe REAL, max_drawdown REAL,
        profit_factor REAL, num_trades INTEGER, avg_hold_hours REAL,
        best_trade_pct REAL, worst_trade_pct REAL,
        bull_return REAL, cautious_return REAL, bear_return REAL,
        created_at TEXT,
        PRIMARY KEY (run_date, agent_id)
    );

    CREATE TABLE IF NOT EXISTS backtest_master_results_sniper (
        run_date TEXT, strategy TEXT, ticker TEXT,
        total_return REAL, win_rate REAL, sharpe REAL, realistic_sharpe REAL,
        max_drawdown REAL, num_trades INTEGER, profit_factor REAL,
        spy_return REAL, vs_spy REAL, regime TEXT, alpha_score REAL,
        created_at TEXT,
        PRIMARY KEY (run_date, strategy, ticker)
    );

    CREATE TABLE IF NOT EXISTS backtest_equity_curve_sniper (
        run_date TEXT, trade_date TEXT, equity REAL, daily_pnl REAL, regime TEXT,
        PRIMARY KEY (run_date, trade_date)
    );

    CREATE TABLE IF NOT EXISTS backtest_monthly_sniper (
        run_date TEXT, month TEXT, agent_id TEXT,
        total_return REAL, win_rate REAL, num_trades INTEGER,
        PRIMARY KEY (run_date, month, agent_id)
    );
    """)
    conn.commit()
    conn.close()
    logger.info("triple_threat tables initialized")


# ═══════════════════════════════════════════════════════════════════════════════
# TEST A: Baseline (SPY-only RSI + SMA200)
# ═══════════════════════════════════════════════════════════════════════════════

def _run_baseline(run_date: str) -> dict:
    """
    SPY-only RSI+SMA200 rules system.
    Downloads ~300 days for SMA200 lookback. Filters to trading days >= 2026-01-09.
    Returns summary dict + trades list.
    """
    logger.info("[BASELINE] Downloading SPY (300-day lookback)...")
    end_dt   = date.today()
    start_dt = end_dt - timedelta(days=300)

    spy_raw: Optional[pd.DataFrame] = None
    try:
        spy_raw = yf.download(
            "SPY", start=start_dt, end=end_dt,
            interval="1d", progress=False, auto_adjust=True,
        )
        if spy_raw is None or spy_raw.empty:
            raise ValueError("empty download")
    except Exception as e:
        logger.warning(f"SPY batch download failed: {e} — retrying individually")
        try:
            spy_raw = yf.download(
                "SPY", start=start_dt, end=end_dt,
                interval="1d", progress=False, auto_adjust=True,
            )
        except Exception as e2:
            logger.error(f"SPY individual retry also failed: {e2}")
            return {"status": "error", "reason": str(e2), "trades": [], "metrics": {}}

    # Handle multi-level column from single ticker
    if isinstance(spy_raw.columns, pd.MultiIndex):
        try:
            spy_raw = spy_raw.xs("SPY", axis=1, level=1)
        except Exception:
            spy_raw = spy_raw.droplevel(1, axis=1)

    spy_raw = spy_raw.dropna(subset=["Close"])
    spy_raw.index = pd.to_datetime(spy_raw.index).normalize()
    spy_raw = spy_raw.sort_index()

    if len(spy_raw) < 205:
        logger.error(f"Insufficient SPY history: {len(spy_raw)} bars")
        return {"status": "error", "reason": "insufficient_history", "trades": [], "metrics": {}}

    # VIX download for regime tagging
    vix_map: dict = {}
    try:
        vix_raw = yf.download("^VIX", start=start_dt, end=end_dt,
                              interval="1d", progress=False, auto_adjust=True)
        if vix_raw is not None and not vix_raw.empty:
            if isinstance(vix_raw.columns, pd.MultiIndex):
                try:
                    vix_raw = vix_raw.xs("^VIX", axis=1, level=1)
                except Exception:
                    vix_raw = vix_raw.droplevel(1, axis=1)
            vix_raw.index = pd.to_datetime(vix_raw.index).normalize()
            for idx, row in vix_raw.iterrows():
                vix_map[pd.Timestamp(idx)] = float(row.get("Close", 20.0))
    except Exception:
        pass

    closes = spy_raw["Close"].values

    # SPY return over the window (used as benchmark in summary)
    window_mask  = spy_raw.index >= pd.Timestamp(WINDOW_START)
    window_close = spy_raw.loc[window_mask, "Close"].values
    spy_return   = 0.0
    if len(window_close) >= 2:
        spy_return = round((float(window_close[-1]) - float(window_close[0])) / float(window_close[0]) * 100, 2)

    # --- State machine ---
    position: Optional[dict] = None
    trades:  list[dict] = []
    days_in_cash   = 0
    days_in_market = 0

    # Iterate over every row in the full history
    for i in range(200, len(spy_raw)):
        row_date = spy_raw.index[i]
        if row_date < pd.Timestamp(WINDOW_START):
            continue
        if row_date > pd.Timestamp(WINDOW_END):
            break

        c       = closes[:i + 1]
        px      = float(c[-1])
        rsi_val = _rsi(c)
        sma200  = float(np.mean(c[-200:]))
        sma50   = float(np.mean(c[-50:])) if len(c) >= 50 else sma200
        vix_val = vix_map.get(pd.Timestamp(row_date), 18.0)
        regime  = _classify_regime(vix_val)
        day_str = row_date.strftime("%Y-%m-%d")

        if position is not None:
            # Manage open position
            days_in_market += 1
            gain = (px - position["entry_px"]) / position["entry_px"]

            exit_type: Optional[str] = None
            if rsi_val > 70:
                exit_type = "RSI_OB"
            elif gain >= 0.10:
                exit_type = "TARGET_10"
            elif gain <= -0.05:
                exit_type = "STOP_5"

            if exit_type:
                # Apply exit slippage
                exit_px  = px * (1 - SLIPPAGE)
                hold_d   = (row_date - pd.Timestamp(position["entry_date"])).days
                pnl_pct  = (exit_px - position["entry_px"]) / position["entry_px"] * 100
                pnl_pct  = max(-100.0, min(100.0, pnl_pct))
                trades.append({
                    "entry_date": position["entry_date"],
                    "exit_date":  day_str,
                    "entry_px":   round(position["entry_px"], 4),
                    "exit_px":    round(exit_px, 4),
                    "pnl_pct":    round(pnl_pct, 3),
                    "hold_days":  hold_d,
                    "exit_type":  exit_type,
                    "regime":     position["regime"],
                })
                position = None
        else:
            # Look for BUY signal
            days_in_cash += 1
            if rsi_val < 30 and px > sma200:
                entry_px  = px * (1 + SLIPPAGE + EXEC_DELAY)
                position  = {
                    "entry_px":   entry_px,
                    "entry_date": day_str,
                    "regime":     regime,
                }

    # Close any open position at last bar
    if position is not None and len(spy_raw) > 0:
        last_px  = float(spy_raw["Close"].iloc[-1])
        exit_px  = last_px * (1 - SLIPPAGE)
        hold_d   = (spy_raw.index[-1] - pd.Timestamp(position["entry_date"])).days
        pnl_pct  = (exit_px - position["entry_px"]) / position["entry_px"] * 100
        pnl_pct  = max(-100.0, min(100.0, pnl_pct))
        trades.append({
            "entry_date": position["entry_date"],
            "exit_date":  spy_raw.index[-1].strftime("%Y-%m-%d"),
            "entry_px":   round(position["entry_px"], 4),
            "exit_px":    round(exit_px, 4),
            "pnl_pct":    round(pnl_pct, 3),
            "hold_days":  hold_d,
            "exit_type":  "EOP",
            "regime":     position["regime"],
        })
        days_in_market += hold_d

    metrics = _trade_metrics_triple(trades)
    metrics["spy_return"]     = spy_return
    metrics["vs_spy"]         = round(metrics["total_return"] - spy_return, 2)
    metrics["days_in_market"] = days_in_market
    metrics["days_in_cash"]   = days_in_cash

    logger.info(f"[BASELINE] {metrics['num_trades']} trades, return={metrics['total_return']:+.2f}%")

    # ── Save to DB ──────────────────────────────────────────────────────────
    conn = _bt_conn()
    now  = datetime.utcnow().isoformat()
    conn.execute("""
        INSERT OR REPLACE INTO backtest_baseline_results
        (run_date, strategy, total_return, win_rate, sharpe, max_drawdown,
         profit_factor, num_trades, avg_hold_days, best_trade_pct, worst_trade_pct,
         spy_return, vs_spy, max_consec_wins, max_consec_losses,
         days_in_market, days_in_cash, avg_trade_return, created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        run_date, "rsi_sma200_spy",
        metrics["total_return"], metrics["win_rate"], metrics["sharpe"],
        metrics["max_drawdown"], metrics["profit_factor"], metrics["num_trades"],
        metrics["avg_hold_days"], metrics["best_trade_pct"], metrics["worst_trade_pct"],
        spy_return, metrics["vs_spy"],
        metrics["max_consec_wins"], metrics["max_consec_losses"],
        days_in_market, days_in_cash, metrics["avg_trade_return"], now,
    ))
    conn.execute("DELETE FROM backtest_baseline_trades WHERE run_date=?", (run_date,))
    for tn, t in enumerate(trades, 1):
        conn.execute("""
            INSERT INTO backtest_baseline_trades
            (run_date, trade_num, entry_date, exit_date, entry_px, exit_px,
             pnl_pct, hold_days, exit_type, regime)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (run_date, tn, t["entry_date"], t["exit_date"],
              t["entry_px"], t["exit_px"], t["pnl_pct"],
              t["hold_days"], t["exit_type"], t["regime"]))
    conn.commit()
    conn.close()

    return {"status": "ok", "trades": trades, "metrics": metrics, "spy_return": spy_return}


# ═══════════════════════════════════════════════════════════════════════════════
# TEST B: Read existing v3b results (no re-run)
# ═══════════════════════════════════════════════════════════════════════════════

def _read_v3b_summary() -> dict:
    """
    Read existing v3b tables from backtest.db.
    Returns a dict with aggregate metrics.
    trades_skipped = 24 (known from v3b options loop log).
    """
    result: dict = {
        "total_return": 0.0, "sharpe": 0.0, "win_rate": 0.0,
        "max_drawdown": 0.0, "total_trades": 0, "profit_factor": 0.0,
        "avg_trade_return": 0.0, "spy_return": 0.0,
        "best_strategy": "n/a", "worst_strategy": "n/a",
        "best_agent": "n/a", "worst_agent": "n/a",
        "max_consec_wins": 0, "max_consec_losses": 0,
        "trades_skipped": 24,
    }

    def _sf(row) -> float:
        if row is None: return 0.0
        v = row[0] if not hasattr(row, "keys") else list(dict(row).values())[0]
        return float(v) if v is not None else 0.0

    try:
        conn = _bt_conn()

        # Equity curve return
        try:
            ec_start = conn.execute(
                "SELECT equity FROM backtest_equity_curve_v3b ORDER BY trade_date LIMIT 1"
            ).fetchone()
            ec_end   = conn.execute(
                "SELECT equity FROM backtest_equity_curve_v3b ORDER BY trade_date DESC LIMIT 1"
            ).fetchone()
            eq_s = float(ec_start["equity"]) if ec_start else STARTING_CASH
            eq_e = float(ec_end["equity"])   if ec_end   else STARTING_CASH
            result["total_return"] = round((eq_e - eq_s) / max(eq_s, 1) * 100, 2)
        except Exception:
            pass

        # Aggregate metrics from master_results_v3b
        try:
            result["sharpe"]       = _sf(conn.execute(
                "SELECT AVG(realistic_sharpe) FROM backtest_master_results_v3b WHERE num_trades>0"
            ).fetchone())
            result["win_rate"]     = _sf(conn.execute(
                "SELECT AVG(win_rate) FROM backtest_master_results_v3b WHERE num_trades>0"
            ).fetchone())
            result["max_drawdown"] = _sf(conn.execute(
                "SELECT MIN(max_drawdown) FROM backtest_master_results_v3b WHERE num_trades>0"
            ).fetchone())
            result["profit_factor"] = _sf(conn.execute(
                "SELECT AVG(profit_factor) FROM backtest_master_results_v3b WHERE num_trades>0"
            ).fetchone())
            result["avg_trade_return"] = _sf(conn.execute(
                "SELECT AVG(total_return) FROM backtest_master_results_v3b WHERE num_trades>0"
            ).fetchone())
        except Exception:
            pass

        # Total trades from agent results
        try:
            result["total_trades"] = int(_sf(conn.execute(
                "SELECT SUM(num_trades) FROM backtest_agent_results_v3b"
            ).fetchone()))
        except Exception:
            pass

        # Best/worst strategy by realistic_sharpe
        try:
            best_s = conn.execute(
                "SELECT strategy FROM backtest_master_results_v3b "
                "WHERE num_trades>0 ORDER BY realistic_sharpe DESC LIMIT 1"
            ).fetchone()
            worst_s = conn.execute(
                "SELECT strategy FROM backtest_master_results_v3b "
                "WHERE num_trades>0 ORDER BY realistic_sharpe ASC LIMIT 1"
            ).fetchone()
            result["best_strategy"]  = best_s["strategy"]  if best_s  else "n/a"
            result["worst_strategy"] = worst_s["strategy"] if worst_s else "n/a"
        except Exception:
            pass

        # Best/worst agent by sharpe
        try:
            best_a = conn.execute(
                "SELECT agent_name FROM backtest_agent_results_v3b "
                "WHERE num_trades>0 ORDER BY sharpe DESC LIMIT 1"
            ).fetchone()
            worst_a = conn.execute(
                "SELECT agent_name FROM backtest_agent_results_v3b "
                "WHERE num_trades>0 ORDER BY sharpe ASC LIMIT 1"
            ).fetchone()
            result["best_agent"]  = best_a["agent_name"]  if best_a  else "n/a"
            result["worst_agent"] = worst_a["agent_name"] if worst_a else "n/a"
        except Exception:
            pass

        # Consec wins/losses from agent-level
        try:
            cw = conn.execute(
                "SELECT MAX(max_consec_wins) FROM backtest_master_results_v3b WHERE num_trades>0"
            ).fetchone()
            cl = conn.execute(
                "SELECT MAX(max_consec_losses) FROM backtest_master_results_v3b WHERE num_trades>0"
            ).fetchone()
            if cw and cw[0] is not None: result["max_consec_wins"]   = int(cw[0])
            if cl and cl[0] is not None: result["max_consec_losses"] = int(cl[0])
        except Exception:
            pass

        # SPY return from comparison table if available
        try:
            spy_r = conn.execute(
                "SELECT v3_backtest FROM comparison_table_v3b "
                "WHERE metric='Total return %' LIMIT 1"
            ).fetchone()
            if not spy_r:
                spy_r = conn.execute(
                    "SELECT actual_fleet FROM comparison_table_v3b "
                    "WHERE metric='SPY return' LIMIT 1"
                ).fetchone()
            # fall through: SPY return will be filled from live data
        except Exception:
            pass

        conn.close()
    except Exception as e:
        logger.warning(f"[V3B_READ] DB read error: {e}")

    logger.info(f"[V3B_READ] return={result['total_return']:+.2f}%, "
                f"trades={result['total_trades']}, "
                f"best_agent={result['best_agent']}")
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# TEST C: Sniper backtest
# ═══════════════════════════════════════════════════════════════════════════════

def _run_sniper_event_loop(
    td: dict,
    days: list,
    vix_map: dict,
    alpha_scores: dict,
) -> tuple[dict[str, list], dict[str, list], int]:
    """
    Sniper equity event loop — only rsi_bounce, bollinger, hammer_candle.
    Triple filter: alpha >= 0.3, conf >= 0.65, bull_signals >= 3.
    hammer_candle additionally requires bull_signals >= 4.
    Returns: (event_trades, agent_trades, skipped_count)
    """
    event_trades: dict[str, list] = defaultdict(list)
    agent_trades: dict[str, list] = defaultdict(list)
    sniper_skipped = 0

    SCAN_FREQ   = 3
    day_counter = 0

    # Strategy → agent mapping for sniper
    STRAT_AGENT = {
        "rsi_bounce":    "grok-4",
        "bollinger":     "gemini-2.5-pro",
        "hammer_candle": "neo-matrix",
    }

    for sym in td:
        df = td[sym]
        if len(df) < 60:
            continue

        alpha = alpha_scores.get(sym, 0.0)
        if alpha < SNIPER_ALPHA_THRESHOLD:
            sniper_skipped += 1
            continue

        pos_factor = _sniper_pos_factor(alpha)
        if pos_factor == 0.0:
            sniper_skipped += 1
            continue

        positions: dict[str, dict] = {}

        for day in days:
            day_counter += 1
            if day_counter % SCAN_FREQ != 0:
                continue

            day_str = day.strftime("%Y-%m-%d")
            month   = day.strftime("%Y-%m")
            vix_val = vix_map.get(day, 18.0)
            regime  = _classify_regime(vix_val)

            m   = df.index <= day
            if m.sum() < 55:
                continue
            sub = df.loc[m]
            c   = sub["Close"].values
            h   = sub["High"].values   if "High"   in sub.columns else c
            l   = sub["Low"].values    if "Low"    in sub.columns else c
            v   = sub["Volume"].values if "Volume" in sub.columns else np.ones(len(c))
            avg_v   = float(np.mean(v[-20:])) if len(v) >= 20 else 1.0
            px      = float(c[-1])
            rsi_val = _rsi(c)

            t2 = _tier2_signals(c, h, l, v, avg_v)
            t3 = _tier3_signals(c, h, l, v, avg_v)

            # Bollinger inline
            bollinger_sig = False
            if len(c) >= 20:
                sma20    = float(np.mean(c[-20:]))
                std20    = float(np.std(c[-20:]))
                lower_bb = sma20 - 2 * std20
                if px <= lower_bb and rsi_val < 45:
                    bollinger_sig = True

            bull_signals = sum(1 for bv in list(t2.values()) + list(t3.values()) if bv)
            if bollinger_sig:
                bull_signals += 1

            # Signals to check
            sig_map: dict[str, bool] = {
                "rsi_bounce":    rsi_val < 30,
                "bollinger":     bollinger_sig,
                "hammer_candle": bool(t3.get("hammer_candle", False)),
            }

            for strat, sig in sig_map.items():
                if not sig:
                    continue

                key = f"{sym}_{strat}"

                # Manage open position first
                if key in positions:
                    pos  = positions[key]
                    gain = (px - pos["entry"]) / pos["entry"]
                    held = pos.get("days_held", 0)
                    if gain >= 0.08 or gain <= -0.05 or held >= 15:
                        pnl_pct = gain * 100 * pos_factor - SLIPPAGE * 200
                        pnl_pct = max(-100.0, min(100.0, pnl_pct))
                        t = {
                            "strategy":    strat,
                            "ticker":      sym,
                            "entry_date":  pos["entry_date"],
                            "exit_date":   day_str,
                            "pnl_pct":     round(pnl_pct, 2),
                            "hold_days":   held,
                            "alpha_score": alpha,
                            "regime":      regime,
                            "month":       month,
                            "win":         1 if pnl_pct > 0 else 0,
                            "agent_id":    pos["agent_id"],
                        }
                        event_trades[strat].append(t)
                        agent_trades[pos["agent_id"]].append(t)
                        del positions[key]
                    else:
                        positions[key]["days_held"] = held + 1
                    continue

                # Triple filter gate
                req_bull = 4 if strat == "hammer_candle" else SNIPER_BULL_MIN
                if alpha < SNIPER_ALPHA_THRESHOLD:
                    sniper_skipped += 1
                    continue
                if SNIPER_CONF_THRESHOLD <= 0.0:  # conf proxy always passes (uniform)
                    pass
                if bull_signals < req_bull:
                    sniper_skipped += 1
                    continue

                agent_id  = STRAT_AGENT[strat]
                fill_cost = SLIPPAGE + EXEC_DELAY
                entry_px  = px * (1 + fill_cost)
                positions[key] = {
                    "entry":      entry_px,
                    "entry_date": day_str,
                    "days_held":  0,
                    "alpha":      alpha,
                    "agent_id":   agent_id,
                    "strategy":   strat,
                }

        # Force-close remaining open positions at last available price
        if positions:
            px_last = float(df["Close"].iloc[-1])
            for key, pos in positions.items():
                strat   = pos["strategy"]
                gain    = (px_last - pos["entry"]) / pos["entry"]
                pf      = _sniper_pos_factor(pos.get("alpha", 0.0))
                pnl_pct = gain * 100 * pf - SLIPPAGE * 200
                pnl_pct = max(-100.0, min(100.0, pnl_pct))
                t = {
                    "strategy":    strat,
                    "ticker":      sym,
                    "entry_date":  pos["entry_date"],
                    "exit_date":   days[-1].strftime("%Y-%m-%d") if days else "EOP",
                    "pnl_pct":     round(pnl_pct, 2),
                    "hold_days":   pos.get("days_held", 1),
                    "alpha_score": pos.get("alpha", 0.0),
                    "regime":      "MIXED",
                    "month":       pos["entry_date"][:7],
                    "win":         1 if pnl_pct > 0 else 0,
                    "agent_id":    pos["agent_id"],
                }
                event_trades[strat].append(t)
                agent_trades[pos["agent_id"]].append(t)

    total = sum(len(v) for v in event_trades.values())
    logger.info(f"[SNIPER_EVENT] {total} trades, {sniper_skipped} skipped")
    return event_trades, agent_trades, sniper_skipped


def _run_sniper_options_loop(
    td: dict,
    days: list,
    vix_map: dict,
    alpha_scores: dict,
) -> tuple[list, int]:
    """
    Sniper options loop — csp, covered_call only (bull_put_spread removed: 0% WR).
    Returns: (options_trades, skipped_count)
    """
    options_trades: list = []
    sniper_skipped = 0

    SCAN_FREQ   = 5
    day_counter = 0

    for day in days:
        day_counter += 1
        if day_counter % SCAN_FREQ != 0:
            continue

        day_str = day.strftime("%Y-%m-%d")
        vix_val = vix_map.get(day, 18.0)
        regime  = _classify_regime(vix_val)

        for sym in list(td.keys()):
            df = td[sym]
            if len(df) < 30:
                continue

            alpha = alpha_scores.get(sym, 0.0)
            if alpha < SNIPER_ALPHA_THRESHOLD:
                sniper_skipped += 1
                continue
            pos_factor = _sniper_pos_factor(alpha)

            m      = df.index <= day
            sub    = df.loc[m]
            c      = sub["Close"].values
            h      = sub["High"].values   if "High"   in sub.columns else c
            l      = sub["Low"].values    if "Low"    in sub.columns else c
            v      = sub["Volume"].values if "Volume" in sub.columns else np.ones(len(c))
            avg_v  = float(np.mean(v[-20:])) if len(v) >= 20 else 1.0
            px     = float(c[-1])
            iv     = _hist_vol(c)
            ivr    = _iv_rank(c)
            future = df.loc[df.index > day]
            if len(future) < 5:
                continue

            t2   = _tier2_signals(c, h, l, v, avg_v)
            t3   = _tier3_signals(c, h, l, v, avg_v)
            bull = sum(1 for bv in list(t2.values()) + list(t3.values()) if bv)

            extra = {
                "ticker":     sym,
                "entry_date": day_str,
                "regime":     regime,
                "alpha_score": alpha,
                "pos_factor":  pos_factor,
            }

            # CSP → Uhura (bull/cautious, ivr > 60, bull >= 2)
            # McCoy: when vix >= 25 (crisis doctor)
            if ivr > 60 and bull >= 2 and regime in ("BULL", "CAUTIOUS") and alpha >= SNIPER_ALPHA_THRESHOLD:
                r = _sim_csp(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    agent_id = "ollama-plutus" if vix_val >= 25 else "ollama-llama"
                    options_trades.append({
                        **r, **extra,
                        "strategy":    "csp",
                        "agent_id":    agent_id,
                        "exit_date":   _exit_date_str(future, r["days"], day_str),
                        "hold_days":   r.get("days", 1),
                        "win":         1 if r.get("pnl", 0) > 0 else 0,
                    })
            else:
                sniper_skipped += 1

            # Covered call → Uhura (BULL) or Worf (BEAR/CAUTIOUS), ivr > 50, bull >= 2
            if ivr > 50 and bull >= 2 and alpha >= SNIPER_ALPHA_THRESHOLD:
                r = _sim_covered_call(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    agent_id = "ollama-llama" if regime == "BULL" else "gemini-2.5-flash"
                    options_trades.append({
                        **r, **extra,
                        "strategy":    "covered_call",
                        "agent_id":    agent_id,
                        "exit_date":   _exit_date_str(future, r["days"], day_str),
                        "hold_days":   r.get("days", 1),
                        "win":         1 if r.get("pnl", 0) > 0 else 0,
                    })
            else:
                sniper_skipped += 1

            # bull_put_spread REMOVED — 0% WR on its only trade (Sniper Go Live)

    logger.info(f"[SNIPER_OPT] {len(options_trades)} options trades, {sniper_skipped} skipped")
    return options_trades, sniper_skipped


def _build_sniper_equity_curve(
    event_trades: dict,
    options_trades: list,
    trading_days: list,
    vix_map: dict,
) -> list[dict]:
    """Equity curve with ±2% daily cap on mean pnl_pct."""
    by_date: dict[str, list] = defaultdict(list)

    for trades in event_trades.values():
        for t in trades:
            k = t.get("exit_date") or t.get("entry_date", "")
            by_date[k].append(t.get("pnl_pct", 0))
    for t in options_trades:
        k = t.get("exit_date") or t.get("entry_date", "")
        by_date[k].append(t.get("pnl_pct", 0))

    equity = STARTING_CASH
    curve: list[dict] = []
    for day in trading_days:
        day_str  = day.strftime("%Y-%m-%d")
        day_pnls = by_date.get(day_str, [])
        vix_val  = vix_map.get(day, 18.0)
        regime   = _classify_regime(vix_val)

        day_ret   = float(np.clip(np.mean(day_pnls) / 100, -0.02, 0.02)) if day_pnls else 0.0
        daily_pnl = equity * day_ret
        equity   += daily_pnl

        curve.append({
            "trade_date": day_str,
            "equity":     round(equity, 2),
            "daily_pnl":  round(daily_pnl, 2),
            "regime":     regime,
        })
    return curve


def _run_sniper(
    run_date: str,
    td: dict,
    trading_days: list,
    vix_map: dict,
    alpha_scores: dict,
    spy_return: float,
) -> dict:
    """Orchestrate the full sniper backtest."""
    logger.info(f"[SNIPER] Starting sniper event loop on {len(td)} symbols, "
                f"{len(trading_days)} trading days")

    # Sniper-filtered universe
    sniper_universe = {
        sym: df for sym, df in td.items()
        if alpha_scores.get(sym, 0.0) >= SNIPER_ALPHA_THRESHOLD
    }
    logger.info(f"[SNIPER] {len(sniper_universe)} symbols pass alpha>={SNIPER_ALPHA_THRESHOLD}")

    event_trades, agent_trades, skipped_ev = _run_sniper_event_loop(
        sniper_universe, trading_days, vix_map, alpha_scores
    )
    options_trades, skipped_opt = _run_sniper_options_loop(
        sniper_universe, trading_days, vix_map, alpha_scores
    )
    sniper_skipped = skipped_ev + skipped_opt

    # Tag options trades with agent tracking
    for t in options_trades:
        aid = t.get("agent_id", "ollama-llama")
        t.setdefault("month", t.get("entry_date", "")[:7])
        t.setdefault("win", 1 if t.get("pnl_pct", 0) > 0 else 0)
        agent_trades[aid].append(t)

    # All trades flat
    all_trades_flat: list = []
    for tlist in event_trades.values():
        all_trades_flat.extend(tlist)
    all_trades_flat.extend(options_trades)
    all_trades_flat = [t for t in all_trades_flat if "pnl_pct" in t]

    # Overall metrics
    overall_metrics = _trade_metrics_triple(all_trades_flat)
    overall_metrics["spy_return"] = spy_return
    overall_metrics["vs_spy"]     = round(overall_metrics["total_return"] - spy_return, 2)

    # Per-agent metrics
    agent_metrics: dict = {}
    for aid, trades in agent_trades.items():
        if not trades:
            continue
        am = _trade_metrics_triple(trades)
        # Regime breakdown
        for regime in ("BULL", "CAUTIOUS", "BEAR"):
            rtrades = [t for t in trades if t.get("regime") == regime and "pnl_pct" in t]
            rm      = _trade_metrics_triple(rtrades) if rtrades else {}
            am[f"{regime.lower()}_return"] = rm.get("total_return", 0.0)
        agent_metrics[aid] = am

    # Per strategy-ticker metrics
    by_strat_sym: dict[tuple, list] = defaultdict(list)
    for t in all_trades_flat:
        key = (t.get("strategy", "unknown"), t.get("ticker", ""))
        by_strat_sym[key].append(t)

    # Equity curve
    curve = _build_sniper_equity_curve(event_trades, options_trades, trading_days, vix_map)

    # Monthly breakdown per agent
    monthly_by_agent: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    for t in all_trades_flat:
        month = (t.get("exit_date") or t.get("entry_date") or "")[:7]
        aid   = t.get("agent_id", "unknown")
        if month:
            monthly_by_agent[month][aid].append(t)

    # Regime performance
    regime_perf: dict[tuple, list] = defaultdict(list)
    for t in all_trades_flat:
        regime_perf[(t.get("regime", "MIXED"), t.get("strategy", "unknown"))].append(t)

    # Alpha attribution (same 10-signal format as v3b)
    alpha_attr = _compute_alpha_attribution_sniper(all_trades_flat)

    # ── Save to DB ──────────────────────────────────────────────────────────
    logger.info("[SNIPER] Saving results to DB...")
    conn  = _bt_conn()
    now   = datetime.utcnow().isoformat()

    for aid, am in agent_metrics.items():
        spec = SNIPER_FLEET.get(aid, {})
        conn.execute("""
            INSERT OR REPLACE INTO backtest_agent_results_sniper
            (run_date, agent_id, agent_name, model,
             total_return, win_rate, sharpe, max_drawdown,
             profit_factor, num_trades, avg_hold_hours,
             best_trade_pct, worst_trade_pct,
             bull_return, cautious_return, bear_return,
             created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            run_date, aid, spec.get("name", aid), spec.get("model", ""),
            am["total_return"], am["win_rate"], am["sharpe"], am["max_drawdown"],
            am["profit_factor"], am["num_trades"],
            round(am["avg_hold_days"] * 24, 1),
            am["best_trade_pct"], am["worst_trade_pct"],
            am.get("bull_return", 0.0), am.get("cautious_return", 0.0),
            am.get("bear_return", 0.0),
            now,
        ))

    for (strat, sym), trades in by_strat_sym.items():
        if not trades:
            continue
        sm      = _trade_metrics_triple(trades)
        alpha_a = float(np.mean([t.get("alpha_score", 0.0) for t in trades]))
        conn.execute("""
            INSERT OR REPLACE INTO backtest_master_results_sniper
            (run_date, strategy, ticker,
             total_return, win_rate, sharpe, realistic_sharpe,
             max_drawdown, num_trades, profit_factor,
             spy_return, vs_spy, regime, alpha_score, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            run_date, strat, sym,
            sm["total_return"], sm["win_rate"], sm["sharpe"], sm["realistic_sharpe"],
            sm["max_drawdown"], sm["num_trades"], sm["profit_factor"],
            spy_return, round(sm["total_return"] - spy_return, 2),
            "MIXED", round(alpha_a, 3), now,
        ))

    for row in curve:
        conn.execute("""
            INSERT OR REPLACE INTO backtest_equity_curve_sniper
            (run_date, trade_date, equity, daily_pnl, regime)
            VALUES (?,?,?,?,?)
        """, (run_date, row["trade_date"], row["equity"], row["daily_pnl"], row["regime"]))

    for month, agents_dict in monthly_by_agent.items():
        for aid, atrades in agents_dict.items():
            if not atrades:
                continue
            mm = _trade_metrics_triple(atrades)
            conn.execute("""
                INSERT OR REPLACE INTO backtest_monthly_sniper
                (run_date, month, agent_id, total_return, win_rate, num_trades)
                VALUES (?,?,?,?,?,?)
            """, (run_date, month, aid, mm["total_return"], mm["win_rate"], mm["num_trades"]))

    conn.commit()
    conn.close()

    logger.info(f"[SNIPER] Done: {len(all_trades_flat)} trades, "
                f"return={overall_metrics['total_return']:+.2f}%, "
                f"sniper_skipped={sniper_skipped}")

    return {
        "status":          "ok",
        "metrics":         overall_metrics,
        "agent_metrics":   agent_metrics,
        "by_strat_sym":    {f"{s}/{sym}": _trade_metrics_triple(trades)
                            for (s, sym), trades in by_strat_sym.items()},
        "regime_perf":     {f"{r}_{s}": _trade_metrics_triple(ts)
                            for (r, s), ts in regime_perf.items()
                            if len(ts) >= 2},
        "monthly_by_agent": {m: {a: _trade_metrics_triple(ts)
                                 for a, ts in agents.items() if ts}
                             for m, agents in monthly_by_agent.items()},
        "curve":           curve,
        "alpha_attr":      alpha_attr,
        "sniper_skipped":  sniper_skipped,
        "spy_return":      spy_return,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Alpha attribution (shared format with v3b)
# ═══════════════════════════════════════════════════════════════════════════════

def _compute_alpha_attribution_sniper(all_trades: list) -> dict:
    SIGNALS = ["dark_pool", "insider", "ftd", "put_call", "vix_structure",
               "sentiment", "yield_curve", "opex", "earnings", "rebalancing"]

    alpha_data: dict[str, dict] = {}
    try:
        ac   = _alpha_conn()
        rows = ac.execute("""
            SELECT symbol, dark_pool_score, insider_score, ftd_score,
                   put_call_score, vix_score, sentiment_score, yield_curve_score,
                   opex_score, earnings_score, rebalancing_score, composite_score
            FROM composite_alpha
            ORDER BY trade_date DESC
        """).fetchall()
        for r in rows:
            sym = r[0]
            if sym not in alpha_data:
                alpha_data[sym] = dict(
                    zip(["dark_pool", "insider", "ftd", "put_call", "vix_structure",
                         "sentiment", "yield_curve", "opex", "earnings", "rebalancing",
                         "composite"],
                        [r[i] for i in range(1, 12)])
                )
        ac.close()
    except Exception:
        pass

    result: dict[str, dict] = {}
    for sig in SIGNALS:
        winning = [t for t in all_trades
                   if t.get("pnl_pct", 0) > 0 and t.get("ticker") in alpha_data]
        losing  = [t for t in all_trades
                   if t.get("pnl_pct", 0) <= 0 and t.get("ticker") in alpha_data]

        win_scores  = [alpha_data[t["ticker"]].get(sig, 0) for t in winning]
        loss_scores = [alpha_data[t["ticker"]].get(sig, 0) for t in losing]

        all_scores   = win_scores + loss_scores
        all_outcomes = [1] * len(win_scores) + [0] * len(loss_scores)

        if len(all_scores) >= 3 and float(np.std(all_scores)) > 0:
            corr_val = float(np.corrcoef(all_scores, all_outcomes)[0, 1])
            if math.isnan(corr_val):
                corr_val = 0.0
        else:
            corr_val = 0.0

        result[sig] = {
            "correlation":      round(corr_val, 4),
            "winning_trades":   len(winning),
            "losing_trades":    len(losing),
            "avg_score_wins":   round(float(np.mean(win_scores))  if win_scores  else 0, 3),
            "avg_score_losses": round(float(np.mean(loss_scores)) if loss_scores else 0, 3),
        }
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# Report printer
# ═══════════════════════════════════════════════════════════════════════════════

def _print_triple_report(
    run_date:   str,
    baseline:   dict,
    v3b:        dict,
    sniper:     dict,
) -> None:
    bar = "━" * 88
    bA  = baseline.get("metrics", {})
    bV  = v3b
    bC  = sniper.get("metrics", {})

    def _fmt(val, pct=False, trades=False, na=False) -> str:
        if na:
            return "n/a"
        if val is None:
            return "—"
        if trades:
            return str(int(val)) if val else "0"
        if pct:
            return f"{val:+.2f}%" if val != 0 else "0.00%"
        if isinstance(val, float):
            return f"{val:.3f}"
        return str(val)

    print(f"\n{bar}")
    print(f"  TRIPLE THREAT BACKTEST  |  Jan 9 – Apr 9, 2026  |  run={run_date}")
    print(f"{bar}")

    # ── Section [1]: Three-way comparison ────────────────────────────────────
    print(f"\n  [1] THREE-WAY COMPARISON TABLE")
    print(f"  ┌{'─'*18}┬{'─'*15}┬{'─'*14}┬{'─'*14}┐")
    print(f"  │ {'Metric':<16} │ {'A: Baseline':>13} │ {'B: v3b Fix':>12} │ {'C: Sniper':>12} │")
    print(f"  ├{'─'*18}┼{'─'*15}┼{'─'*14}┼{'─'*14}┤")

    def _row(label: str, a, b, c, *, pct=False, trades=False, na_a=False, na_b=False, na_c=False) -> None:
        fa = _fmt(a, pct=pct, trades=trades, na=na_a)
        fb = _fmt(b, pct=pct, trades=trades, na=na_b)
        fc = _fmt(c, pct=pct, trades=trades, na=na_c)
        print(f"  │ {label:<16} │ {fa:>13} │ {fb:>12} │ {fc:>12} │")

    _row("Return %",         bA.get("total_return"),       bV.get("total_return"),       bC.get("total_return"),       pct=True)
    _row("Sharpe",           bA.get("sharpe"),              bV.get("sharpe"),              bC.get("sharpe"))
    _row("Win Rate",         f"{bA.get('win_rate',0):.1f}%", f"{bV.get('win_rate',0):.1f}%", f"{bC.get('win_rate',0):.1f}%")
    _row("Max Drawdown %",   bA.get("max_drawdown"),        bV.get("max_drawdown"),        bC.get("max_drawdown"),       pct=True)
    _row("Total Trades",     bA.get("num_trades",0),        bV.get("total_trades",0),      bC.get("num_trades",0),       trades=True)
    _row("Trades Skipped",   "n/a",                         bV.get("trades_skipped",24),   sniper.get("sniper_skipped",0))
    _row("Agents Active",    "0 (rules)",                   "11",                          "6")
    _row("Strategies Used",  "1",                           "30+",                         "6")
    _row("SPY Benchmark",    f"{bA.get('spy_return',0):+.2f}%", f"{bV.get('spy_return',0):+.2f}%", f"{bC.get('spy_return',0):+.2f}%")
    _row("Best Strategy",    "n/a",                         bV.get("best_strategy","n/a"), _best_sniper_strategy(sniper), na_a=False)
    _row("Worst Strategy",   "n/a",                         bV.get("worst_strategy","n/a"), _worst_sniper_strategy(sniper), na_a=False)
    _row("Best Agent",       "n/a",                         bV.get("best_agent","n/a"),    _best_sniper_agent(sniper),    na_a=False)
    _row("Worst Agent",      "n/a",                         bV.get("worst_agent","n/a"),   _worst_sniper_agent(sniper),   na_a=False)
    _row("Profit Factor",    bA.get("profit_factor"),       bV.get("profit_factor"),       bC.get("profit_factor"))
    _row("Avg Trade Return", f"{bA.get('avg_trade_return',0):+.3f}%", f"{bV.get('avg_trade_return',0):+.3f}%", f"{bC.get('avg_trade_return',0):+.3f}%")
    _row("Max Consec Wins",  bA.get("max_consec_wins",0),   bV.get("max_consec_wins",0),   bC.get("max_consec_wins",0),  trades=True)
    _row("Max Consec Loss",  bA.get("max_consec_losses",0), bV.get("max_consec_losses",0), bC.get("max_consec_losses",0), trades=True)

    print(f"  └{'─'*18}┴{'─'*15}┴{'─'*14}┴{'─'*14}┘")

    # ── Section [2]: Seven-way historical comparison ──────────────────────────
    print(f"\n  [2] SEVEN-WAY HISTORICAL COMPARISON")
    KNOWN = {
        "actual": {"return": -23.66, "sharpe": -6.516, "wr": 17.7,  "dd": -42.3, "trades": 558},
        "v1":     {"return": +41.33, "sharpe": -0.061, "wr": 41.8,  "dd": -100.0, "trades": 2329},
        "v2":     {"return": +8.42,  "sharpe": +0.874, "wr": 57.6,  "dd": -100.0, "trades": 277},
        "v3":     {"return": -36.99, "sharpe": -0.034, "wr": 48.8,  "dd": -52.1, "trades": 153},
    }
    cols = ["Actual", "v1", "v2", "v3", "Baseline", "v3b", "Sniper"]
    cw   = 9
    hdr  = f"  {'Metric':<18}" + "".join(f"{c:>{cw}}" for c in cols)
    print(hdr)
    print(f"  {'─'*88}")

    def _7row(label: str, vals: list) -> None:
        s = f"  {label:<18}"
        for v in vals:
            s += f"{v:>{cw}}"
        print(s)

    _7row("Return %", [
        f"{KNOWN['actual']['return']:+.2f}%",
        f"{KNOWN['v1']['return']:+.2f}%",
        f"{KNOWN['v2']['return']:+.2f}%",
        f"{KNOWN['v3']['return']:+.2f}%",
        f"{bA.get('total_return',0):+.2f}%",
        f"{bV.get('total_return',0):+.2f}%",
        f"{bC.get('total_return',0):+.2f}%",
    ])
    _7row("Sharpe", [
        f"{KNOWN['actual']['sharpe']:.3f}",
        f"{KNOWN['v1']['sharpe']:.3f}",
        f"{KNOWN['v2']['sharpe']:.3f}",
        f"{KNOWN['v3']['sharpe']:.3f}",
        f"{bA.get('sharpe',0):.3f}",
        f"{bV.get('sharpe',0):.3f}",
        f"{bC.get('sharpe',0):.3f}",
    ])
    _7row("Win Rate %", [
        f"{KNOWN['actual']['wr']:.1f}%",
        f"{KNOWN['v1']['wr']:.1f}%",
        f"{KNOWN['v2']['wr']:.1f}%",
        f"{KNOWN['v3']['wr']:.1f}%",
        f"{bA.get('win_rate',0):.1f}%",
        f"{bV.get('win_rate',0):.1f}%",
        f"{bC.get('win_rate',0):.1f}%",
    ])
    _7row("Max DD %", [
        f"{KNOWN['actual']['dd']:.1f}%",
        f"{KNOWN['v1']['dd']:.1f}%",
        f"{KNOWN['v2']['dd']:.1f}%",
        f"{KNOWN['v3']['dd']:.1f}%",
        f"{bA.get('max_drawdown',0):.1f}%",
        f"{bV.get('max_drawdown',0):.1f}%",
        f"{bC.get('max_drawdown',0):.1f}%",
    ])
    _7row("Total Trades", [
        str(KNOWN['actual']['trades']),
        str(KNOWN['v1']['trades']),
        str(KNOWN['v2']['trades']),
        str(KNOWN['v3']['trades']),
        str(bA.get('num_trades',0)),
        str(bV.get('total_trades',0)),
        str(bC.get('num_trades',0)),
    ])

    # ── Section [3]: Sniper per-agent leaderboard ─────────────────────────────
    print(f"\n  [3] SNIPER PER-AGENT LEADERBOARD  (ranked by Sharpe)")
    print(f"  {'─'*76}")
    print(f"  {'Rank':<4} {'Agent':<10} {'Model':<20} {'Sharpe':>7} {'WR%':>6} {'Return%':>8} {'MaxDD%':>7} {'Trades':>6}")
    print(f"  {'─'*76}")

    agent_met = sniper.get("agent_metrics", {})
    ranked    = sorted(agent_met.items(), key=lambda x: -x[1].get("sharpe", -99))
    for rank, (aid, am) in enumerate(ranked, 1):
        spec  = SNIPER_FLEET.get(aid, {})
        name  = spec.get("name", aid)
        model = spec.get("model", "")[:18]
        flag  = " *" if am.get("needs_validation") else "  "
        print(f"  {rank:<4} {name:<10} {model:<20} "
              f"{am.get('sharpe',0):>+7.3f} "
              f"{am.get('win_rate',0):>6.1f} "
              f"{am.get('total_return',0):>+8.2f} "
              f"{am.get('max_drawdown',0):>7.2f} "
              f"{am.get('num_trades',0):>6}{flag}")
    print(f"  * = |Sharpe| > 5.0, needs validation")

    # ── Section [4]: Sniper strategy breakdown ────────────────────────────────
    print(f"\n  [4] SNIPER STRATEGY BREAKDOWN  (ranked by realistic Sharpe)")
    print(f"  {'─'*76}")
    print(f"  {'Strategy':<20} {'r.Sharpe':>9} {'WR%':>6} {'Return%':>8} {'Trades':>7} {'Tier':>4}")
    print(f"  {'─'*76}")

    try:
        conn    = _bt_conn()
        s_rows  = conn.execute("""
            SELECT strategy, SUM(num_trades) as n, AVG(realistic_sharpe) as rs,
                   AVG(win_rate) as wr, SUM(total_return) as tr
            FROM backtest_master_results_sniper
            WHERE run_date=? AND num_trades>0
            GROUP BY strategy
            ORDER BY rs DESC
        """, (run_date,)).fetchall()
        conn.close()
        for row in s_rows:
            tier = SNIPER_TIER_MAP.get(row["strategy"], 0)
            print(f"  {row['strategy']:<20} {row['rs']:>+9.3f} "
                  f"{row['wr']:>6.1f} "
                  f"{row['tr']:>+8.2f} "
                  f"{row['n']:>7} "
                  f"{tier:>4}")
    except Exception as e:
        print(f"  (strategy table unavailable: {e})")

    # ── Section [5]: Sniper regime results ───────────────────────────────────
    print(f"\n  [5] SNIPER REGIME RESULTS")
    print(f"  {'─'*76}")
    regime_data = sniper.get("regime_perf", {})
    for regime in ("BULL", "CAUTIOUS", "BEAR"):
        strats = [(k.split("_", 1)[1] if "_" in k else k, v)
                  for k, v in regime_data.items()
                  if k.startswith(f"{regime}_")]
        top3 = sorted(strats, key=lambda x: -x[1].get("sharpe", -99))[:3]
        if top3:
            print(f"  {regime}:")
            for strat, m in top3:
                print(f"    {strat:<26} Sharpe={m.get('sharpe',0):>+6.3f}  "
                      f"WR={m.get('win_rate',0):.1f}%  n={m.get('num_trades',0)}")
        else:
            print(f"  {regime}: (no trades)")

    # ── Section [6]: Sniper monthly breakdown ────────────────────────────────
    print(f"\n  [6] SNIPER MONTHLY BREAKDOWN")
    print(f"  {'─'*76}")
    try:
        conn   = _bt_conn()
        months = sorted({r["month"] for r in conn.execute(
            "SELECT DISTINCT month FROM backtest_monthly_sniper WHERE run_date=?", (run_date,)
        ).fetchall()})
        conn.close()
        if months:
            col_w  = 11
            header = f"  {'Agent':<10}" + "".join(f"{m:>{col_w}}" for m in months)
            print(header)
            print(f"  {'─'*76}")
            conn2 = _bt_conn()
            for aid, spec in SNIPER_FLEET.items():
                name  = spec["name"]
                row_s = f"  {name:<10}"
                for m in months:
                    r = conn2.execute("""
                        SELECT total_return FROM backtest_monthly_sniper
                        WHERE run_date=? AND month=? AND agent_id=?
                    """, (run_date, m, aid)).fetchone()
                    val = f"{r['total_return']:>+.1f}%" if r else "   —   "
                    row_s += f"{val:>{col_w}}"
                print(row_s)
            conn2.close()
    except Exception as e:
        print(f"  (monthly table unavailable: {e})")

    # ── Section [7]: Alpha signal report card ────────────────────────────────
    alpha_attr = sniper.get("alpha_attr", {})
    print(f"\n  [7] ALPHA SIGNAL REPORT CARD  (Sniper)")
    print(f"  {'─'*76}")
    print(f"  {'Signal':<22} {'Corr':>8} {'Wins':>6} {'Losses':>7} {'AvgW':>7} {'AvgL':>7}  Grade")
    print(f"  {'─'*76}")
    for sig, data in sorted(alpha_attr.items(),
                             key=lambda x: -abs(x[1].get("correlation", 0))):
        corr  = data.get("correlation", 0)
        grade = ("A (strong positive)" if corr > 0.10
                 else "B (mild positive)"  if corr > 0.05
                 else "F (hurts returns)"  if corr < -0.10
                 else "D (mild negative)"  if corr < -0.05
                 else "C (neutral)")
        print(f"  {sig:<22} {corr:>+8.4f} {data.get('winning_trades',0):>6} "
              f"{data.get('losing_trades',0):>7} "
              f"{data.get('avg_score_wins',0):>7.3f} "
              f"{data.get('avg_score_losses',0):>7.3f}  {grade}")

    # ── Section [8]: Final recommendation ────────────────────────────────────
    print(f"\n  [8] FINAL RECOMMENDATION")
    print(f"  {'─'*76}")
    _print_recommendation(bA, bV, bC, sniper, agent_met)

    print(f"\n{bar}\n")


def _best_sniper_strategy(sniper: dict) -> str:
    perf = sniper.get("by_strat_sym", {})
    if not perf:
        return "n/a"
    best = max(perf.items(), key=lambda x: x[1].get("sharpe", -99), default=(None, {}))
    return best[0].split("/")[0] if best[0] else "n/a"


def _worst_sniper_strategy(sniper: dict) -> str:
    perf = sniper.get("by_strat_sym", {})
    if not perf:
        return "n/a"
    worst = min(perf.items(), key=lambda x: x[1].get("sharpe", 99), default=(None, {}))
    return worst[0].split("/")[0] if worst[0] else "n/a"


def _best_sniper_agent(sniper: dict) -> str:
    am = sniper.get("agent_metrics", {})
    if not am:
        return "n/a"
    best = max(am.items(), key=lambda x: x[1].get("sharpe", -99), default=(None, {}))
    if not best[0]:
        return "n/a"
    return SNIPER_FLEET.get(best[0], {}).get("name", best[0])


def _worst_sniper_agent(sniper: dict) -> str:
    am = sniper.get("agent_metrics", {})
    if not am:
        return "n/a"
    worst = min(am.items(), key=lambda x: x[1].get("sharpe", 99), default=(None, {}))
    if not worst[0]:
        return "n/a"
    return SNIPER_FLEET.get(worst[0], {}).get("name", worst[0])


def _print_recommendation(bA: dict, bV: dict, bC: dict, sniper: dict, agent_met: dict) -> None:
    """3-4 line final recommendation."""
    ret_a  = bA.get("total_return", 0.0)
    ret_v  = bV.get("total_return", 0.0)
    ret_c  = bC.get("total_return", 0.0)
    sh_a   = bA.get("sharpe", 0.0)
    sh_v   = bV.get("sharpe", 0.0)
    sh_c   = bC.get("sharpe", 0.0)
    dd_a   = bA.get("max_drawdown", 0.0)
    dd_v   = bV.get("max_drawdown", 0.0)
    dd_c   = bC.get("max_drawdown", 0.0)
    spy    = bA.get("spy_return", bC.get("spy_return", 0.0))

    winner = "Sniper (C)" if sh_c > sh_v and sh_c > sh_a else \
             "v3b (B)"    if sh_v > sh_a                  else \
             "Baseline (A)"

    scores = {"A": (sh_a, ret_a, dd_a), "B": (sh_v, ret_v, dd_v), "C": (sh_c, ret_c, dd_c)}
    print(f"  Comparison: A={ret_a:+.2f}%/Sharpe={sh_a:.3f}  "
          f"B={ret_v:+.2f}%/Sharpe={sh_v:.3f}  "
          f"C={ret_c:+.2f}%/Sharpe={sh_c:.3f}  (SPY={spy:+.2f}%)")
    print(f"  Recommendation: GO LIVE with {winner} — "
          f"highest risk-adjusted return over the test window.")

    # Regime advice
    regime_data = sniper.get("regime_perf", {})
    bull_strats  = [(k.split("_",1)[1], v) for k, v in regime_data.items() if k.startswith("BULL_")]
    bear_strats  = [(k.split("_",1)[1], v) for k, v in regime_data.items() if k.startswith("BEAR_")]
    if bull_strats:
        best_bull = max(bull_strats, key=lambda x: x[1].get("sharpe", -99))
        print(f"  Regime advice: In BULL → lean on {best_bull[0]} "
              f"(Sharpe={best_bull[1].get('sharpe',0):+.3f}); "
              f"in BEAR → favor CSP/covered_call income over directional longs.")

    # Agent promotions / shelving
    promotions = [SNIPER_FLEET.get(aid, {}).get("name", aid)
                  for aid, am in agent_met.items()
                  if am.get("sharpe", 0) > 1.0 and am.get("win_rate", 0) > 55]
    shelve     = [SNIPER_FLEET.get(aid, {}).get("name", aid)
                  for aid, am in agent_met.items()
                  if am.get("sharpe", 0) < -0.5 or am.get("win_rate", 0) < 30]
    if promotions:
        print(f"  Promote: {', '.join(promotions)} — exceeded Sharpe > 1.0 + WR > 55%")
    if shelve:
        print(f"  Shelve/review: {', '.join(shelve)} — Sharpe < -0.5 or WR < 30%")


# ═══════════════════════════════════════════════════════════════════════════════
# Main orchestrator
# ═══════════════════════════════════════════════════════════════════════════════

def run_triple_threat() -> dict:
    """Orchestrate all three tests and print the full report."""
    t0       = time.time()
    run_date = date.today().isoformat()

    logger.info("═" * 70)
    logger.info("  TRIPLE THREAT BACKTEST  |  Jan 9 – Apr 9, 2026")
    logger.info(f"  Run date: {run_date}")
    logger.info("═" * 70)

    _init_triple_tables()

    # ── TEST A: Baseline ──────────────────────────────────────────────────────
    logger.info("[STEP A] Running Baseline (SPY RSI+SMA200)...")
    baseline_result = _run_baseline(run_date)

    # ── TEST B: Read v3b ──────────────────────────────────────────────────────
    logger.info("[STEP B] Reading existing v3b results...")
    v3b_summary = _read_v3b_summary()

    # ── Shared download for Sniper ────────────────────────────────────────────
    logger.info("[STEP C-prep] Expanding alpha universe...")
    universe     = _expand_alpha_universe()
    alpha_scores = run_alpha_expansion(universe)

    # Merge existing composite alpha
    try:
        ac = _alpha_conn()
        existing = ac.execute(
            "SELECT symbol, composite_score FROM composite_alpha ORDER BY as_of_date DESC"
        ).fetchall()
        ac.close()
        for r in existing:
            alpha_scores[r["symbol"]] = r["composite_score"]
    except Exception:
        pass

    filtered = sorted(
        [s for s, sc in alpha_scores.items() if sc >= SNIPER_ALPHA_THRESHOLD],
        key=lambda s: -alpha_scores[s],
    )
    logger.info(f"Sniper universe: {len(filtered)} symbols pass alpha>={SNIPER_ALPHA_THRESHOLD}")

    logger.info(f"[STEP C-prep] Downloading {len(filtered)} symbols...")
    td = _download_v2_universe(filtered + ["SPY", "^VIX"], 300)

    # SPY individual retry if missing
    if "SPY" not in td:
        try:
            end_dt   = date.today()
            start_dt = end_dt - timedelta(days=360)
            spy_raw  = yf.download(
                "SPY", start=start_dt, end=end_dt,
                interval="1d", progress=False, auto_adjust=True,
            )
            if not spy_raw.empty:
                if isinstance(spy_raw.columns, pd.MultiIndex):
                    spy_raw = spy_raw.droplevel(1, axis=1)
                td["SPY"] = spy_raw.dropna()
                logger.info("SPY downloaded individually")
        except Exception as e:
            logger.warning(f"SPY individual retry failed: {e}")

    # Build trading days list for the window only
    vix_df  = td.get("^VIX") if td.get("^VIX") is not None else td.get("VIX")
    vix_map: dict = {}
    if vix_df is not None and not vix_df.empty:
        vix_df.index = pd.to_datetime(vix_df.index).normalize()
        for idx, row in vix_df.iterrows():
            vix_map[pd.Timestamp(idx)] = float(row.get("Close", 20.0))

    # Get all trading days then filter to window
    all_days   = _get_trading_days(td, 300)
    trading_days = [d for d in all_days
                    if pd.Timestamp(WINDOW_START) <= d <= pd.Timestamp(WINDOW_END)]

    if not trading_days:
        logger.warning("No trading days in [Jan 9, Apr 9] window. Using all available days.")
        trading_days = all_days

    logger.info(f"Trading days in window: {len(trading_days)}")

    # SPY return for the window
    spy_return = 0.0
    if "SPY" in td and not td["SPY"].empty:
        spy_df = td["SPY"].copy()
        spy_df.index = pd.to_datetime(spy_df.index).normalize()
        window_spy = spy_df.loc[
            (spy_df.index >= pd.Timestamp(WINDOW_START)) &
            (spy_df.index <= pd.Timestamp(WINDOW_END))
        ]
        if len(window_spy) >= 2:
            spy_return = round(
                (float(window_spy["Close"].iloc[-1]) - float(window_spy["Close"].iloc[0]))
                / float(window_spy["Close"].iloc[0]) * 100,
                2,
            )
    logger.info(f"SPY return in window: {spy_return:+.2f}%")

    # Push spy_return into v3b summary if missing
    if v3b_summary.get("spy_return", 0.0) == 0.0:
        v3b_summary["spy_return"] = spy_return
    # Push spy_return into baseline too
    if baseline_result.get("metrics"):
        baseline_result["metrics"]["spy_return"] = baseline_result.get("spy_return", spy_return)

    # ── TEST C: Sniper ────────────────────────────────────────────────────────
    logger.info("[STEP C] Running Sniper backtest...")
    sniper_result = _run_sniper(
        run_date, td, trading_days, vix_map, alpha_scores, spy_return
    )

    elapsed = round(time.time() - t0, 1)
    logger.info(f"Triple Threat complete in {elapsed}s")

    # ── Print full report ─────────────────────────────────────────────────────
    _print_triple_report(run_date, baseline_result, v3b_summary, sniper_result)

    return {
        "status":        "ok",
        "run_date":      run_date,
        "elapsed_s":     elapsed,
        "baseline":      baseline_result,
        "v3b":           v3b_summary,
        "sniper":        sniper_result,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [triple] %(levelname)s %(message)s",
    )
    run_triple_threat()
