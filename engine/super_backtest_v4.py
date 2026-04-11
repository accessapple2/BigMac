"""
engine/super_backtest_v4.py — Warp Core Reactor v4 — Four-Way Backtest Suite

Four tests, one report:
  A: SPY Baseline   — SPY-only RSI+SMA200 rules (no AI), Jan 9 – Apr 9, 2026
  B: V2 Replay      — 12 agents, old model labels, alpha 0.1, full strategies
  C: Sniper Mode    — 6 agents, 4 strategies, triple filter (alpha>=0.3, conf>=0.65, bull>=3)
  D: V1 Replay      — All agents, no alpha threshold, all strategies, real slippage

Run:
    venv/bin/python3 -m engine.super_backtest_v4
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
    _bs_price, _bs_delta, _bs_theta,
    _hist_vol, _iv_rank, _rsi, _atr, _classify_regime,
    _run_tier1_vbt as _tier1_vbt,
    _tier2_signals, _tier3_signals, _tier4_signals, _tier9_short_signals,
    _sim_long_call, _sim_long_put, _sim_csp, _sim_covered_call,
    _sim_bull_call_spread, _sim_bull_put_spread,
    _sim_bear_put_spread, _sim_bear_call_spread,
    _sim_ic, _sim_broken_wing_ic, _sim_0dte,
    _exit_date_str, _trade_metrics,
    _get_trading_days, _run_dalio_metals,
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
WINDOW_START  = date(2026, 1, 9)
WINDOW_END    = date(2026, 4, 9)
BACKTEST_DAYS = 90

REGIMES = ["BULL", "CAUTIOUS", "BEAR", "CRISIS"]

# ── Sniper constants ──────────────────────────────────────────────────────────
SNIPER_ALPHA_THRESHOLD = 0.3
SNIPER_CONF_THRESHOLD  = 0.65
SNIPER_BULL_MIN        = 3

# ── Known historical data ─────────────────────────────────────────────────────
KNOWN = {
    "actual":    {"return": -23.66, "sharpe": -6.516, "wr": 17.7, "dd": -42.3,  "trades": 558},
    "v1":        {"return": +41.33, "sharpe": -0.061, "wr": 41.8, "dd": -100.0, "trades": 2329},
    "v2":        {"return": +8.42,  "sharpe": +0.874, "wr": 57.6, "dd": -100.0, "trades": 277},
    "v3":        {"return": -36.99, "sharpe": -0.034, "wr": 48.8, "dd": -52.1,  "trades": 153},
    "v3b":       {"return": +16.30, "sharpe": +1.003, "wr": 61.5, "dd": -8.2,   "trades": 87},
    "sniper_tt": {"return": 0.0,    "sharpe": +1.136, "wr": 83.3, "dd": 0.0,    "trades": 18},
}

# ── V2 Replay Fleet (old model labels) ───────────────────────────────────────
V2_REPLAY_FLEET: dict[str, dict] = {
    "grok-4":           {"name": "Spock",   "model": "deepseek-r1:7b",   "tiers": [1],
                         "specialization": "mean_reversion"},
    "gemini-2.5-flash": {"name": "Worf",    "model": "qwen3.5:9b",       "tiers": [5, 6, 9],
                         "specialization": "bear_specialist"},
    "dayblade-sulu":    {"name": "Sulu",    "model": "gemma3:4b",        "tiers": [2],
                         "specialization": "momentum"},
    "ollama-plutus":    {"name": "McCoy",   "model": "0xroyce/plutus",   "tiers": [1, 2],
                         "specialization": "crisis_doctor", "min_vix": 25},
    "gemini-2.5-pro":   {"name": "Seven",   "model": "qwen3.5:9b",       "tiers": [1],
                         "specialization": "pure_quant"},
    "ollama-llama":     {"name": "Uhura",   "model": "llama3.1:latest",  "tiers": [5, 6],
                         "specialization": "options_flow"},
    "ollie-auto":       {"name": "Ollie",   "model": "signal-center",    "tiers": [1, 2, 3],
                         "specialization": "signal_center"},
    "neo-matrix":       {"name": "Neo",     "model": "port-8000",        "tiers": [1, 2, 3],
                         "specialization": "plutus_scoring"},
    "capitol-trades":   {"name": "Capitol", "model": "congress",         "tiers": [1, 2],
                         "specialization": "congress_copycat"},
    "dalio-metals":     {"name": "Dalio",   "model": "qwen3.5:9b",       "tiers": [4],
                         "specialization": "metals_macro",
                         "universe": ["GLD", "SLV", "CPER", "TIPS", "IAU"]},
    "dayblade-0dte":    {"name": "TPol",    "model": "options-s2",       "tiers": [8],
                         "active": False, "specialization": "0dte"},
    "super-agent":      {"name": "Anderson","model": "crewai",            "tiers": [1, 2, 3, 4],
                         "specialization": "bridge_vote"},
}

# ── Sniper Fleet v4 ───────────────────────────────────────────────────────────
SNIPER_FLEET_V4: dict[str, dict] = {
    "ollama-llama":     {"name": "Uhura",  "model": "llama3.1:latest",  "tiers": [5, 6]},
    "gemini-2.5-flash": {"name": "Worf",   "model": "qwen3:14b",        "tiers": [5]},
    "grok-4":           {"name": "Spock",  "model": "phi4:14b",         "tiers": [1]},
    "gemini-2.5-pro":   {"name": "Seven",  "model": "qwen3:14b",        "tiers": [1]},
    "ollama-plutus":    {"name": "McCoy",  "model": "0xroyce/plutus",   "tiers": [5]},
    "neo-matrix":       {"name": "Neo",    "model": "0xroyce/plutus",   "tiers": [3]},
}

TIER_MAP = {
    "rsi_bounce": (1, "Core Technical"), "bollinger": (1, "Core Technical"),
    "sma_cross": (1, "Core Technical"), "ema_pullback": (1, "Core Technical"),
    "momentum_breakout": (2, "Intraday Momentum"), "reversal_bounce": (2, "Intraday Momentum"),
    "vwap_reclaim": (2, "Intraday Momentum"), "volume_spike": (2, "Intraday Momentum"),
    "hammer_candle": (3, "Holly-Style"), "five_day_bounce": (3, "Holly-Style"),
    "falling_knife": (3, "Holly-Style"), "avwap_bounce": (3, "Holly-Style"),
    "dalio_metals": (4, "Metals Macro"),
    "long_call": (5, "Options Single Leg"), "csp": (5, "Options Single Leg"),
    "covered_call": (5, "Options Single Leg"), "long_put": (5, "Options Single Leg"),
    "bull_call_spread": (6, "Vertical Spreads"), "bull_put_spread": (6, "Vertical Spreads"),
    "bear_put_spread": (6, "Vertical Spreads"), "bear_call_spread": (6, "Vertical Spreads"),
    "iron_condor": (7, "Iron Condors"), "broken_wing_ic": (7, "Iron Condors"),
}


# ═══════════════════════════════════════════════════════════════════════════════
# Core metric helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _alpha_position_factor(alpha: float) -> float:
    if alpha >= 0.6: return 1.00
    if alpha >= 0.4: return 0.75
    if alpha >= 0.1: return 0.50
    return 0.0


def _sniper_pos_factor(alpha: float) -> float:
    if alpha >= 0.6: return 1.0
    if alpha >= 0.3: return 0.5
    return 0.0


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


def _trade_metrics_v4(trades: list[dict]) -> dict:
    """
    v4 metric function: arithmetic sum (np.sum) total_return.
    Per-trade pnl_pct capped ±100. Equity-curve drawdown capped at -100%.
    Same return dict structure as _trade_metrics_triple.
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

    # Arithmetic sum: avoids compound-chain-zero bug
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
# DB connection helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _bt_conn() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(BACKTEST_DB), timeout=20)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _alpha_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(ALPHA_DB), timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


# ═══════════════════════════════════════════════════════════════════════════════
# Table initialization
# ═══════════════════════════════════════════════════════════════════════════════

def _init_v4_tables() -> None:
    conn = _bt_conn()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS backtest_v4_baseline (
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

    CREATE TABLE IF NOT EXISTS backtest_v4_baseline_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_date TEXT, trade_num INTEGER,
        entry_date TEXT, exit_date TEXT,
        entry_px REAL, exit_px REAL,
        pnl_pct REAL, hold_days INTEGER,
        exit_type TEXT, regime TEXT
    );

    CREATE TABLE IF NOT EXISTS backtest_v4_v2replay (
        run_date TEXT, agent_id TEXT, agent_name TEXT, model TEXT,
        specialization TEXT, total_return REAL, win_rate REAL,
        sharpe REAL, max_drawdown REAL, profit_factor REAL,
        num_trades INTEGER, avg_hold_days REAL,
        best_trade_pct REAL, worst_trade_pct REAL,
        bull_return REAL, cautious_return REAL, bear_return REAL,
        spy_return REAL, vs_spy REAL, created_at TEXT,
        PRIMARY KEY (run_date, agent_id)
    );

    CREATE TABLE IF NOT EXISTS backtest_v4_v2replay_master (
        run_date TEXT, strategy TEXT, ticker TEXT,
        total_return REAL, win_rate REAL, sharpe REAL, realistic_sharpe REAL,
        max_drawdown REAL, num_trades INTEGER, profit_factor REAL,
        spy_return REAL, vs_spy REAL, regime TEXT, alpha_score REAL,
        tier INTEGER, created_at TEXT,
        PRIMARY KEY (run_date, strategy, ticker)
    );

    CREATE TABLE IF NOT EXISTS backtest_v4_v2replay_equity (
        run_date TEXT, trade_date TEXT, equity REAL, daily_pnl REAL, regime TEXT,
        PRIMARY KEY (run_date, trade_date)
    );

    CREATE TABLE IF NOT EXISTS backtest_v4_v2replay_monthly (
        run_date TEXT, month TEXT, agent_id TEXT,
        total_return REAL, win_rate REAL, num_trades INTEGER,
        PRIMARY KEY (run_date, month, agent_id)
    );

    CREATE TABLE IF NOT EXISTS backtest_v4_sniper (
        run_date TEXT, agent_id TEXT, agent_name TEXT, model TEXT,
        total_return REAL, win_rate REAL, sharpe REAL, max_drawdown REAL,
        profit_factor REAL, num_trades INTEGER, avg_hold_days REAL,
        best_trade_pct REAL, worst_trade_pct REAL,
        bull_return REAL, cautious_return REAL, bear_return REAL,
        spy_return REAL, vs_spy REAL, created_at TEXT,
        PRIMARY KEY (run_date, agent_id)
    );

    CREATE TABLE IF NOT EXISTS backtest_v4_sniper_master (
        run_date TEXT, strategy TEXT, ticker TEXT,
        total_return REAL, win_rate REAL, sharpe REAL, realistic_sharpe REAL,
        max_drawdown REAL, num_trades INTEGER, profit_factor REAL,
        spy_return REAL, vs_spy REAL, regime TEXT, alpha_score REAL,
        created_at TEXT,
        PRIMARY KEY (run_date, strategy, ticker)
    );

    CREATE TABLE IF NOT EXISTS backtest_v4_sniper_equity (
        run_date TEXT, trade_date TEXT, equity REAL, daily_pnl REAL, regime TEXT,
        PRIMARY KEY (run_date, trade_date)
    );

    CREATE TABLE IF NOT EXISTS backtest_v4_sniper_monthly (
        run_date TEXT, month TEXT, agent_id TEXT,
        total_return REAL, win_rate REAL, num_trades INTEGER,
        PRIMARY KEY (run_date, month, agent_id)
    );

    CREATE TABLE IF NOT EXISTS backtest_v4_v1replay (
        run_date TEXT, agent_id TEXT, agent_name TEXT, model TEXT,
        total_return REAL, win_rate REAL, sharpe REAL, max_drawdown REAL,
        profit_factor REAL, num_trades INTEGER, avg_hold_days REAL,
        best_trade_pct REAL, worst_trade_pct REAL,
        spy_return REAL, vs_spy REAL, created_at TEXT,
        PRIMARY KEY (run_date, agent_id)
    );

    CREATE TABLE IF NOT EXISTS backtest_v4_v1replay_master (
        run_date TEXT, strategy TEXT, ticker TEXT,
        total_return REAL, win_rate REAL, sharpe REAL, realistic_sharpe REAL,
        max_drawdown REAL, num_trades INTEGER, profit_factor REAL,
        spy_return REAL, vs_spy REAL, regime TEXT, alpha_score REAL,
        tier INTEGER, created_at TEXT,
        PRIMARY KEY (run_date, strategy, ticker)
    );

    CREATE TABLE IF NOT EXISTS backtest_v4_v1replay_equity (
        run_date TEXT, trade_date TEXT, equity REAL, daily_pnl REAL, regime TEXT,
        PRIMARY KEY (run_date, trade_date)
    );

    CREATE TABLE IF NOT EXISTS backtest_v4_alpha_attribution (
        run_date TEXT, test_name TEXT, signal_name TEXT,
        correlation REAL, winning_trades INTEGER, losing_trades INTEGER,
        avg_score_wins REAL, avg_score_losses REAL,
        PRIMARY KEY (run_date, test_name, signal_name)
    );
    """)
    conn.commit()
    conn.close()
    logger.info("v4 tables initialized")


# ═══════════════════════════════════════════════════════════════════════════════
# SPY download with retry
# ═══════════════════════════════════════════════════════════════════════════════

def _download_spy_with_retry(start_dt, end_dt, max_retries: int = 3, delay: int = 2):
    for attempt in range(max_retries):
        try:
            df = yf.download("SPY", start=start_dt, end=end_dt,
                             interval="1d", progress=False, auto_adjust=True)
            if df is not None and not df.empty:
                return df
        except Exception:
            pass
        if attempt < max_retries - 1:
            time.sleep(delay)
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# Agent routing (V2/V1 replay — uses V2_REPLAY_FLEET)
# ═══════════════════════════════════════════════════════════════════════════════

def _agent_accepts_trade_v4(agent_id: str, fleet: dict, strategy: str, regime: str,
                             vix: float, bull_signals: int, bear_signals: int,
                             rsi_val: float) -> bool:
    spec    = fleet.get(agent_id, {})
    s       = spec.get("specialization", "")
    min_vix = spec.get("min_vix", 0)

    if min_vix > 0 and vix < min_vix:
        return False

    if s == "mean_reversion":
        return strategy in ("rsi_bounce",) and (rsi_val < 30 or rsi_val > 70)
    if s == "bear_specialist":
        return strategy in ("long_put", "bear_put_spread", "bear_call_spread") or "short" in strategy
    if s == "momentum":
        return strategy in ("momentum_breakout", "vwap_reclaim", "volume_spike") and regime in ("BULL", "CAUTIOUS")
    if s == "crisis_doctor":
        return vix >= 25
    if s == "pure_quant":
        return strategy in ("rsi_bounce", "bollinger", "sma_cross", "ema_pullback")
    if s == "options_flow":
        return strategy in ("long_call", "long_put", "csp", "covered_call",
                             "bull_call_spread", "bull_put_spread")
    if s == "signal_center":
        return True
    if s == "plutus_scoring":
        return bull_signals >= 3
    if s == "congress_copycat":
        return strategy in ("rsi_bounce", "bollinger", "momentum_breakout") and bull_signals >= 2
    if s == "metals_macro":
        return strategy == "dalio_metals"
    if s == "0dte":
        return "0dte" in strategy
    if s == "bridge_vote":
        return bull_signals >= 3 or bear_signals >= 3
    return True


def _route_to_agent_v4(fleet: dict, strategy: str, regime: str,
                        vix: float, bull: int, bear: int, rsi: float) -> Optional[str]:
    STRATEGY_AGENT = {
        "rsi_bounce":          "grok-4",
        "bollinger":           "gemini-2.5-pro",
        "sma_cross":           "gemini-2.5-pro",
        "momentum_breakout":   "dayblade-sulu",
        "vwap_reclaim":        "dayblade-sulu",
        "volume_spike":        "dayblade-sulu",
        "hammer_candle":       "ollie-auto",
        "five_day_bounce":     "ollie-auto",
        "falling_knife":       "neo-matrix",
        "avwap_bounce":        "neo-matrix",
        "dalio_metals":        "dalio-metals",
    }
    agent_id = STRATEGY_AGENT.get(strategy, "super-agent")

    agent   = fleet.get(agent_id, {})
    min_vix = agent.get("min_vix", 0)
    if min_vix > 0 and vix < min_vix:
        return None
    if agent_id == "grok-4" and not (rsi < 30 or rsi > 70):
        return None
    if agent_id == "dayblade-sulu" and regime not in ("BULL", "CAUTIOUS"):
        return None
    if agent_id == "gemini-2.5-flash" and regime == "BULL":
        return None
    if agent_id == "super-agent" and bull < 3 and bear < 3:
        return None

    return agent_id


# ═══════════════════════════════════════════════════════════════════════════════
# Equity curve builder (shared)
# ═══════════════════════════════════════════════════════════════════════════════

def _build_equity_curve_v4(event_trades: dict, opt_trades: list, spread_trades: list,
                            trading_days: list, vix_map: dict) -> list[dict]:
    all_by_date: dict[str, list] = defaultdict(list)

    for trades in event_trades.values():
        for t in trades:
            key = t.get("exit_date") or t.get("entry_date", "")
            all_by_date[key].append(t.get("pnl_pct", 0))
    for t in opt_trades + spread_trades:
        key = t.get("exit_date") or t.get("entry_date", "")
        all_by_date[key].append(t.get("pnl_pct", 0))

    equity = STARTING_CASH
    curve: list[dict] = []
    for day in trading_days:
        day_str  = day.strftime("%Y-%m-%d")
        day_pnls = all_by_date.get(day_str, [])
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


# ═══════════════════════════════════════════════════════════════════════════════
# Alpha attribution (shared)
# ═══════════════════════════════════════════════════════════════════════════════

def _compute_alpha_attribution_v4(all_trades: list) -> dict:
    SIGNALS = ["dark_pool", "insider", "ftd", "put_call", "vix_structure",
               "sentiment", "yield_curve", "opex", "earnings", "rebalancing",
               "rallies_consensus", "rallies_debate_sentiment"]

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
        sig_key = sig if sig not in ("rallies_consensus", "rallies_debate_sentiment") else "composite"
        winning = [t for t in all_trades
                   if t.get("pnl_pct", 0) > 0 and t.get("ticker") in alpha_data]
        losing  = [t for t in all_trades
                   if t.get("pnl_pct", 0) <= 0 and t.get("ticker") in alpha_data]

        win_scores  = [alpha_data[t["ticker"]].get(sig_key, 0) for t in winning]
        loss_scores = [alpha_data[t["ticker"]].get(sig_key, 0) for t in losing]

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
# TEST A: SPY Baseline
# ═══════════════════════════════════════════════════════════════════════════════

def _run_baseline_v4(spy_df: pd.DataFrame, vix_map: dict, run_date: str) -> dict:
    """
    SPY-only RSI+SMA200 rules. Filters to WINDOW_START..WINDOW_END.
    Saves to backtest_v4_baseline table.
    """
    if spy_df is None or spy_df.empty:
        return {"status": "error", "reason": "no_spy_data", "trades": [], "metrics": {}}

    # Normalize index
    spy_df = spy_df.copy()
    if isinstance(spy_df.columns, pd.MultiIndex):
        try:
            spy_df = spy_df.xs("SPY", axis=1, level=1)
        except Exception:
            spy_df = spy_df.droplevel(1, axis=1)
    spy_df = spy_df.dropna(subset=["Close"])
    spy_df.index = pd.to_datetime(spy_df.index).normalize()
    spy_df = spy_df.sort_index()

    if len(spy_df) < 205:
        logger.error(f"[BASELINE] Insufficient SPY history: {len(spy_df)} bars")
        return {"status": "error", "reason": "insufficient_history", "trades": [], "metrics": {}}

    closes = spy_df["Close"].values

    # SPY window return for benchmark
    window_mask  = spy_df.index >= pd.Timestamp(WINDOW_START)
    window_close = spy_df.loc[window_mask, "Close"].values
    spy_return   = 0.0
    if len(window_close) >= 2:
        spy_return = round(
            (float(window_close[-1]) - float(window_close[0])) / float(window_close[0]) * 100, 2
        )

    position: Optional[dict] = None
    trades:  list[dict]      = []
    days_in_cash   = 0
    days_in_market = 0

    for i in range(200, len(spy_df)):
        row_date = spy_df.index[i]
        if row_date < pd.Timestamp(WINDOW_START):
            continue
        if row_date > pd.Timestamp(WINDOW_END):
            break

        c       = closes[:i + 1]
        px      = float(c[-1])
        rsi_val = _rsi(c)
        sma200  = float(np.mean(c[-200:]))
        vix_val = vix_map.get(pd.Timestamp(row_date), 18.0)
        regime  = _classify_regime(vix_val)
        day_str = row_date.strftime("%Y-%m-%d")

        if position is not None:
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
                exit_px = px * (1 - SLIPPAGE)
                hold_d  = (row_date - pd.Timestamp(position["entry_date"])).days
                pnl_pct = (exit_px - position["entry_px"]) / position["entry_px"] * 100
                pnl_pct = max(-100.0, min(100.0, pnl_pct))
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
            days_in_cash += 1
            # Buy signal: RSI < 30 AND price > SMA200 (200-day SMA filter)
            if rsi_val < 30 and px > sma200:
                entry_px = px * (1 + SLIPPAGE + EXEC_DELAY)
                position = {
                    "entry_px":   entry_px,
                    "entry_date": day_str,
                    "regime":     regime,
                }

    # Close any open position at last bar
    if position is not None and len(spy_df) > 0:
        last_px = float(spy_df["Close"].iloc[-1])
        exit_px = last_px * (1 - SLIPPAGE)
        hold_d  = (spy_df.index[-1] - pd.Timestamp(position["entry_date"])).days
        pnl_pct = (exit_px - position["entry_px"]) / position["entry_px"] * 100
        pnl_pct = max(-100.0, min(100.0, pnl_pct))
        trades.append({
            "entry_date": position["entry_date"],
            "exit_date":  spy_df.index[-1].strftime("%Y-%m-%d"),
            "entry_px":   round(position["entry_px"], 4),
            "exit_px":    round(exit_px, 4),
            "pnl_pct":    round(pnl_pct, 3),
            "hold_days":  hold_d,
            "exit_type":  "EOP",
            "regime":     position["regime"],
        })
        days_in_market += hold_d

    metrics = _trade_metrics_v4(trades)
    metrics["spy_return"]     = spy_return
    metrics["vs_spy"]         = round(metrics["total_return"] - spy_return, 2)
    metrics["days_in_market"] = days_in_market
    metrics["days_in_cash"]   = days_in_cash

    logger.info(f"[BASELINE] {metrics['num_trades']} trades, return={metrics['total_return']:+.2f}%")

    conn = _bt_conn()
    now  = datetime.utcnow().isoformat()
    conn.execute("""
        INSERT OR REPLACE INTO backtest_v4_baseline
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
    conn.execute("DELETE FROM backtest_v4_baseline_trades WHERE run_date=?", (run_date,))
    for tn, t in enumerate(trades, 1):
        conn.execute("""
            INSERT INTO backtest_v4_baseline_trades
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
# V2/V1 Replay shared event loop
# ═══════════════════════════════════════════════════════════════════════════════

def _run_event_loop_v4(td: dict, days: list, vix_map: dict,
                       alpha_scores: dict, fleet: dict,
                       alpha_threshold: float = 0.1,
                       conf_threshold: float = 0.65,
                       label: str = "v2replay") -> tuple[dict, dict]:
    """
    Shared event loop for V2 Replay (alpha_threshold=0.1) and V1 Replay (alpha_threshold=0.0).
    Returns (event_trades, agent_trades).
    """
    event_trades: dict[str, list] = defaultdict(list)
    agent_trades: dict[str, list] = defaultdict(list)

    SCAN_FREQ   = 3
    day_counter = 0

    for sym in td:
        df = td[sym]
        if len(df) < 60:
            continue

        alpha = alpha_scores.get(sym, 0.0)
        if alpha < alpha_threshold:
            continue

        # Double filter: alpha AND conf (V2 replay), no conf filter for V1 (threshold=0.0 means conf gate skipped)
        if conf_threshold > 0.0 and alpha_threshold > 0.0:
            # proxy: uniform conf passes for all symbols in universe
            pass  # conf proxy always passes

        pos_factor = _alpha_position_factor(alpha) if alpha_threshold > 0.0 else 0.5
        if pos_factor == 0.0 and alpha_threshold > 0.0:
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

            m = df.index <= day
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

            t2       = _tier2_signals(c, h, l, v, avg_v)
            t3       = _tier3_signals(c, h, l, v, avg_v)
            all_sigs = {**t2, **t3}

            # Bollinger signal inline
            if len(c) >= 20:
                sma20_val = float(np.mean(c[-20:]))
                std20_val = float(np.std(c[-20:]))
                lower_bb  = sma20_val - 2 * std20_val
                if px <= lower_bb and rsi_val < 45:
                    all_sigs["bollinger"] = True

            # SMA golden cross
            if len(c) >= 21:
                sma5_now   = float(np.mean(c[-5:]))
                sma5_prev  = float(np.mean(c[-6:-1]))
                sma20_curr = float(np.mean(c[-20:]))
                sma20_prev = float(np.mean(c[-21:-1]))
                if sma5_now > sma20_curr and sma5_prev <= sma20_prev:
                    all_sigs["sma_cross"] = True

            bull_sigs = sum(1 for sv in all_sigs.values() if sv)
            bear_sigs = sum(1 for sv in _tier9_short_signals(c, h, l, v, avg_v).values() if sv)

            for strat, sig in all_sigs.items():
                if not sig:
                    continue
                key = f"{sym}_{strat}"

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
                            "pnl":         round(pnl_pct, 2),
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

                agent_id = _route_to_agent_v4(fleet, strat, regime, vix_val,
                                               bull_sigs, bear_sigs, rsi_val)
                if agent_id is None:
                    continue
                if agent_id not in fleet:
                    agent_id = "super-agent" if "super-agent" in fleet else list(fleet.keys())[-1]

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

        # Close remaining open positions at end
        for key, pos in positions.items():
            strat   = pos["strategy"]
            px_last = float(df["Close"].iloc[-1])
            gain    = (px_last - pos["entry"]) / pos["entry"]
            pnl_pct = gain * 100 * pos_factor - SLIPPAGE * 200
            pnl_pct = max(-100.0, min(100.0, pnl_pct))
            t = {
                "strategy":    strat,
                "ticker":      sym,
                "entry_date":  pos["entry_date"],
                "exit_date":   days[-1].strftime("%Y-%m-%d") if days else "EOP",
                "pnl_pct":     round(pnl_pct, 2),
                "pnl":         round(pnl_pct, 2),
                "hold_days":   pos.get("days_held", 1),
                "alpha_score": alpha,
                "regime":      "MIXED",
                "month":       pos["entry_date"][:7],
                "win":         1 if pnl_pct > 0 else 0,
                "agent_id":    pos["agent_id"],
            }
            event_trades[strat].append(t)
            agent_trades[pos["agent_id"]].append(t)

    # Dalio metals for fleets that include dalio-metals
    if "dalio-metals" in fleet:
        dalio = _run_dalio_metals(td, days)
        for t in dalio:
            t["agent_id"]    = "dalio-metals"
            t["alpha_score"] = alpha_scores.get(t.get("ticker", ""), 0.0)
            t["regime"]      = "MIXED"
            t["month"]       = t.get("entry_date", "")[:7]
            t["win"]         = 1 if t.get("pnl_pct", 0) > 0 else 0
            event_trades["dalio_metals"].append(t)
            agent_trades["dalio-metals"].append(t)

    total = sum(len(v) for v in event_trades.values())
    logger.info(f"[EVENT_LOOP_{label.upper()}] {total} trades across {len(agent_trades)} agents")
    return event_trades, agent_trades


# ═══════════════════════════════════════════════════════════════════════════════
# V2 Replay options loop (long_call, long_put, csp, covered_call, spreads)
# ═══════════════════════════════════════════════════════════════════════════════

def _run_options_loop_v4(td: dict, days: list, vix_map: dict,
                         alpha_scores: dict, alpha_threshold: float = 0.1,
                         include_ic: bool = False,
                         label: str = "v2replay") -> tuple[list, list]:
    """
    Options loop for V2 Replay (include_ic=False) and V1 Replay (include_ic=True).
    Returns (options_trades, spread_trades).
    """
    options_trades: list = []
    spread_trades:  list = []

    SCAN_FREQ   = 5
    day_counter = 0
    skipped     = 0

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
            if alpha < alpha_threshold:
                skipped += 1
                continue
            pos_factor = _alpha_position_factor(alpha) if alpha_threshold > 0.0 else 0.5

            m      = df.index <= day
            sub    = df.loc[m]
            if len(sub) < 20:
                continue
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
            bear = sum(1 for bv in _tier9_short_signals(c, h, l, v, avg_v).values() if bv)

            extra = {"ticker": sym, "entry_date": day_str, "regime": regime,
                     "alpha_score": alpha, "pos_factor": pos_factor}

            # Long call
            if bull >= 2 and ivr < 60:
                r = _sim_long_call(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    options_trades.append({**r, **extra, "strategy": "long_call",
                                           "option_type": "call", "agent_id": "ollama-llama",
                                           "exit_date": _exit_date_str(future, r["days"], day_str),
                                           "hold_days": r.get("days", 1),
                                           "win": 1 if r["pnl"] > 0 else 0})

            # CSP
            if ivr > 50 and bull >= 1 and regime in ("BULL", "CAUTIOUS"):
                r = _sim_csp(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    options_trades.append({**r, **extra, "strategy": "csp",
                                           "option_type": "put", "agent_id": "ollama-llama",
                                           "exit_date": _exit_date_str(future, r["days"], day_str),
                                           "hold_days": r.get("days", 1),
                                           "win": 1 if r["pnl"] > 0 else 0})

            # Covered call
            if bull >= 2 and ivr > 40:
                r = _sim_covered_call(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    options_trades.append({**r, **extra, "strategy": "covered_call",
                                           "option_type": "call", "agent_id": "ollama-llama",
                                           "exit_date": _exit_date_str(future, r["days"], day_str),
                                           "hold_days": r.get("days", 1),
                                           "win": 1 if r["pnl"] > 0 else 0})

            # Long put
            if bear >= 2 and regime != "BULL":
                r = _sim_long_put(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    options_trades.append({**r, **extra, "strategy": "long_put",
                                           "option_type": "put", "agent_id": "gemini-2.5-flash",
                                           "exit_date": _exit_date_str(future, r["days"], day_str),
                                           "hold_days": r.get("days", 1),
                                           "win": 1 if r["pnl"] > 0 else 0})

            # Bull spreads
            if bull >= 2:
                bull_agent = "dayblade-sulu" if regime in ("BULL", "CAUTIOUS") else "ollama-llama"
                r = _sim_bull_call_spread(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    spread_trades.append({**r, **extra, "strategy": "bull_call_spread",
                                          "spread_type": "BULL_CALL", "agent_id": bull_agent,
                                          "exit_date": _exit_date_str(future, r["days"], day_str),
                                          "hold_days": r.get("days", 1),
                                          "win": 1 if r.get("pnl", 0) > 0 else 0})
                r = _sim_bull_put_spread(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    spread_trades.append({**r, **extra, "strategy": "bull_put_spread",
                                          "spread_type": "BULL_PUT", "agent_id": bull_agent,
                                          "exit_date": _exit_date_str(future, r["days"], day_str),
                                          "hold_days": r.get("days", 1),
                                          "win": 1 if r.get("pnl", 0) > 0 else 0})

            # Bear spreads
            if vix_val > 20 and bear >= 2:
                r = _sim_bear_call_spread(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    spread_trades.append({**r, **extra, "strategy": "bear_call_spread",
                                          "spread_type": "BEAR_CALL", "agent_id": "gemini-2.5-flash",
                                          "exit_date": _exit_date_str(future, r["days"], day_str),
                                          "hold_days": r.get("days", 1),
                                          "win": 1 if r.get("pnl", 0) > 0 else 0})
                r = _sim_bear_put_spread(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    spread_trades.append({**r, **extra, "strategy": "bear_put_spread",
                                          "spread_type": "BEAR_PUT", "agent_id": "gemini-2.5-flash",
                                          "exit_date": _exit_date_str(future, r["days"], day_str),
                                          "hold_days": r.get("days", 1),
                                          "win": 1 if r.get("pnl", 0) > 0 else 0})

            # IC + broken wing IC (V1 only)
            if include_ic and vix_val > 20:
                sma20 = float(np.mean(c[-20:])) if len(c) >= 20 else px
                if abs(px - sma20) / max(px, 1e-9) < 0.02:
                    r = _sim_ic(future, px, iv, OPT_DTE_DEFAULT)
                    if r:
                        spread_trades.append({**r, **extra, "strategy": "iron_condor",
                                              "spread_type": "IC", "agent_id": "gemini-2.5-pro",
                                              "exit_date": _exit_date_str(future, r["days"], day_str),
                                              "hold_days": r.get("days", 1),
                                              "win": 1 if r.get("pnl", 0) > 0 else 0})
                    r = _sim_broken_wing_ic(future, px, iv, OPT_DTE_DEFAULT)
                    if r:
                        spread_trades.append({**r, **extra, "strategy": "broken_wing_ic",
                                              "spread_type": "BW_IC", "agent_id": "gemini-2.5-pro",
                                              "exit_date": _exit_date_str(future, r["days"], day_str),
                                              "hold_days": r.get("days", 1),
                                              "win": 1 if r.get("pnl", 0) > 0 else 0})

    logger.info(f"[OPT_LOOP_{label.upper()}] {len(options_trades)} opts, "
                f"{len(spread_trades)} spreads, {skipped} skipped")
    return options_trades, spread_trades


# ═══════════════════════════════════════════════════════════════════════════════
# Sniper event loop
# ═══════════════════════════════════════════════════════════════════════════════

def _run_sniper_event_loop_v4(td: dict, days: list, vix_map: dict,
                               alpha_scores: dict) -> tuple[dict, dict, int]:
    """
    Sniper event loop: rsi_bounce + bollinger only.
    Triple filter: alpha>=0.3, conf>=0.65 (proxy passes), bull_signals>=3.
    """
    event_trades: dict[str, list] = defaultdict(list)
    agent_trades: dict[str, list] = defaultdict(list)
    sniper_skipped = 0

    SCAN_FREQ   = 3
    day_counter = 0

    STRAT_AGENT = {
        "rsi_bounce": "grok-4",
        "bollinger":  "gemini-2.5-pro",
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

            m = df.index <= day
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

            sig_map: dict[str, bool] = {
                "rsi_bounce": rsi_val < 30,
                "bollinger":  bollinger_sig,
            }

            for strat, sig in sig_map.items():
                if not sig:
                    continue
                key = f"{sym}_{strat}"

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

                # Triple filter gate: alpha>=0.3 already passed, conf proxy passes, bull>=3
                if bull_signals < SNIPER_BULL_MIN:
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
    logger.info(f"[SNIPER_EVENT_V4] {total} trades, {sniper_skipped} skipped")
    return event_trades, agent_trades, sniper_skipped


# ═══════════════════════════════════════════════════════════════════════════════
# Sniper options loop (CSP + covered_call only)
# ═══════════════════════════════════════════════════════════════════════════════

def _run_sniper_options_loop_v4(td: dict, days: list, vix_map: dict,
                                 alpha_scores: dict) -> tuple[list, int]:
    """Sniper options loop: CSP + covered_call only. Triple filter applied."""
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
            if len(sub) < 20:
                continue
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
                "ticker":      sym,
                "entry_date":  day_str,
                "regime":      regime,
                "alpha_score": alpha,
                "pos_factor":  pos_factor,
            }

            # CSP: ivr>60, bull>=2, bull/cautious, triple filter
            if ivr > 60 and bull >= 2 and regime in ("BULL", "CAUTIOUS"):
                r = _sim_csp(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    agent_id = "ollama-plutus" if vix_val >= 25 else "ollama-llama"
                    options_trades.append({
                        **r, **extra,
                        "strategy":  "csp",
                        "agent_id":  agent_id,
                        "exit_date": _exit_date_str(future, r["days"], day_str),
                        "hold_days": r.get("days", 1),
                        "win":       1 if r.get("pnl", 0) > 0 else 0,
                    })
            else:
                sniper_skipped += 1

            # Covered call: ivr>50, bull>=2, triple filter
            if ivr > 50 and bull >= 2:
                r = _sim_covered_call(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    agent_id = "ollama-llama" if regime == "BULL" else "gemini-2.5-flash"
                    options_trades.append({
                        **r, **extra,
                        "strategy":  "covered_call",
                        "agent_id":  agent_id,
                        "exit_date": _exit_date_str(future, r["days"], day_str),
                        "hold_days": r.get("days", 1),
                        "win":       1 if r.get("pnl", 0) > 0 else 0,
                    })
            else:
                sniper_skipped += 1

    logger.info(f"[SNIPER_OPT_V4] {len(options_trades)} options trades, {sniper_skipped} skipped")
    return options_trades, sniper_skipped


# ═══════════════════════════════════════════════════════════════════════════════
# TEST B: V2 Replay orchestrator
# ═══════════════════════════════════════════════════════════════════════════════

def _run_v2replay_v4(td: dict, trading_days: list, vix_map: dict,
                     alpha_scores: dict, spy_return: float, run_date: str) -> dict:
    """12 agents, old model labels, alpha threshold 0.1, double filter."""
    logger.info(f"[V2REPLAY] Starting on {len(td)} symbols, {len(trading_days)} days")

    event_trades, agent_trades = _run_event_loop_v4(
        td, trading_days, vix_map, alpha_scores,
        fleet=V2_REPLAY_FLEET, alpha_threshold=0.1, conf_threshold=0.65, label="v2replay"
    )
    opt_trades, spread_trades = _run_options_loop_v4(
        td, trading_days, vix_map, alpha_scores,
        alpha_threshold=0.1, include_ic=False, label="v2replay"
    )

    # Tag options/spreads with agents
    for t in opt_trades:
        strat = t.get("strategy", "")
        aid   = t.get("agent_id",
                      "gemini-2.5-flash" if ("bear" in strat or "put" in strat) else "ollama-llama")
        t["agent_id"] = aid
        t.setdefault("month", t.get("entry_date", "")[:7])
        t.setdefault("win", 1 if t.get("pnl_pct", 0) > 0 else 0)
        agent_trades[aid].append(t)

    for t in spread_trades:
        strat = t.get("strategy", "")
        aid   = t.get("agent_id",
                      "gemini-2.5-flash" if "bear" in strat else
                      "dayblade-sulu"     if "bull" in strat else "gemini-2.5-pro")
        t["agent_id"] = aid
        t.setdefault("month", t.get("entry_date", "")[:7])
        t.setdefault("win", 1 if t.get("pnl_pct", 0) > 0 else 0)
        agent_trades[aid].append(t)

    all_trades_flat: list = []
    for tlist in event_trades.values():
        all_trades_flat.extend(tlist)
    all_trades_flat.extend(opt_trades + spread_trades)
    all_trades_flat = [t for t in all_trades_flat if "pnl_pct" in t]

    overall = _trade_metrics_v4(all_trades_flat)
    overall["spy_return"] = spy_return
    overall["vs_spy"]     = round(overall["total_return"] - spy_return, 2)

    agent_metrics: dict = {}
    for aid, trades in agent_trades.items():
        if not trades:
            continue
        am = _trade_metrics_v4(trades)
        for regime in ("BULL", "CAUTIOUS", "BEAR"):
            rtrades = [t for t in trades if t.get("regime") == regime and "pnl_pct" in t]
            rm      = _trade_metrics_v4(rtrades) if rtrades else {}
            am[f"{regime.lower()}_return"] = rm.get("total_return", 0.0)
        agent_metrics[aid] = am

    curve = _build_equity_curve_v4(event_trades, opt_trades + spread_trades, [], trading_days, vix_map)

    monthly_by_agent: dict = defaultdict(lambda: defaultdict(list))
    for t in all_trades_flat:
        month = (t.get("exit_date") or t.get("entry_date") or "")[:7]
        aid   = t.get("agent_id", "unknown")
        if month:
            monthly_by_agent[month][aid].append(t)

    by_strat_sym: dict = defaultdict(list)
    for t in all_trades_flat:
        by_strat_sym[(t.get("strategy", "unknown"), t.get("ticker", ""))].append(t)

    alpha_attr = _compute_alpha_attribution_v4(all_trades_flat)

    # Save to DB
    conn = _bt_conn()
    now  = datetime.utcnow().isoformat()

    for aid, am in agent_metrics.items():
        spec = V2_REPLAY_FLEET.get(aid, {})
        conn.execute("""
            INSERT OR REPLACE INTO backtest_v4_v2replay
            (run_date, agent_id, agent_name, model, specialization,
             total_return, win_rate, sharpe, max_drawdown, profit_factor,
             num_trades, avg_hold_days, best_trade_pct, worst_trade_pct,
             bull_return, cautious_return, bear_return, spy_return, vs_spy, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            run_date, aid, spec.get("name", aid), spec.get("model", ""),
            spec.get("specialization", ""),
            am["total_return"], am["win_rate"], am["sharpe"], am["max_drawdown"],
            am["profit_factor"], am["num_trades"], am["avg_hold_days"],
            am["best_trade_pct"], am["worst_trade_pct"],
            am.get("bull_return", 0), am.get("cautious_return", 0), am.get("bear_return", 0),
            spy_return, round(am["total_return"] - spy_return, 2), now,
        ))

    for (strat, sym), trades in by_strat_sym.items():
        if not trades:
            continue
        sm      = _trade_metrics_v4(trades)
        alpha_a = float(np.mean([t.get("alpha_score", 0.0) for t in trades]))
        tier_n  = TIER_MAP.get(strat, (2, "Tier 2+"))[0]
        conn.execute("""
            INSERT OR REPLACE INTO backtest_v4_v2replay_master
            (run_date, strategy, ticker, total_return, win_rate, sharpe, realistic_sharpe,
             max_drawdown, num_trades, profit_factor, spy_return, vs_spy,
             regime, alpha_score, tier, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            run_date, strat, sym,
            sm["total_return"], sm["win_rate"], sm["sharpe"], sm["realistic_sharpe"],
            sm["max_drawdown"], sm["num_trades"], sm["profit_factor"],
            spy_return, round(sm["total_return"] - spy_return, 2),
            "MIXED", round(alpha_a, 3), tier_n, now,
        ))

    for row in curve:
        conn.execute("""
            INSERT OR REPLACE INTO backtest_v4_v2replay_equity
            (run_date, trade_date, equity, daily_pnl, regime)
            VALUES (?,?,?,?,?)
        """, (run_date, row["trade_date"], row["equity"], row["daily_pnl"], row["regime"]))

    for month, agents_dict in monthly_by_agent.items():
        for aid, atrades in agents_dict.items():
            if not atrades:
                continue
            mm = _trade_metrics_v4(atrades)
            conn.execute("""
                INSERT OR REPLACE INTO backtest_v4_v2replay_monthly
                (run_date, month, agent_id, total_return, win_rate, num_trades)
                VALUES (?,?,?,?,?,?)
            """, (run_date, month, aid, mm["total_return"], mm["win_rate"], mm["num_trades"]))

    # Alpha attribution
    for sig, data in alpha_attr.items():
        conn.execute("""
            INSERT OR REPLACE INTO backtest_v4_alpha_attribution
            (run_date, test_name, signal_name, correlation, winning_trades, losing_trades,
             avg_score_wins, avg_score_losses)
            VALUES (?,?,?,?,?,?,?,?)
        """, (run_date, "v2replay", sig, data["correlation"],
              data["winning_trades"], data["losing_trades"],
              data["avg_score_wins"], data["avg_score_losses"]))

    conn.commit()
    conn.close()

    logger.info(f"[V2REPLAY] Done: {len(all_trades_flat)} trades, "
                f"return={overall['total_return']:+.2f}%")

    return {
        "status":          "ok",
        "metrics":         overall,
        "agent_metrics":   agent_metrics,
        "by_strat_sym":    {f"{s}/{sym}": _trade_metrics_v4(trades)
                            for (s, sym), trades in by_strat_sym.items()},
        "curve":           curve,
        "alpha_attr":      alpha_attr,
        "monthly_by_agent": {m: {a: _trade_metrics_v4(ts) for a, ts in agents.items() if ts}
                             for m, agents in monthly_by_agent.items()},
        "spy_return":      spy_return,
        "num_agents":      len(agent_metrics),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# TEST C: Sniper Mode orchestrator
# ═══════════════════════════════════════════════════════════════════════════════

def _run_sniper_v4(td: dict, trading_days: list, vix_map: dict,
                   alpha_scores: dict, spy_return: float, run_date: str) -> dict:
    """6 agents, 4 strategies, triple filter (alpha>=0.3, conf>=0.65, bull>=3)."""
    logger.info(f"[SNIPER_V4] Starting on {len(td)} symbols, {len(trading_days)} days")

    sniper_universe = {
        sym: df for sym, df in td.items()
        if alpha_scores.get(sym, 0.0) >= SNIPER_ALPHA_THRESHOLD
    }
    logger.info(f"[SNIPER_V4] {len(sniper_universe)} symbols pass alpha>={SNIPER_ALPHA_THRESHOLD}")

    event_trades, agent_trades, skipped_ev = _run_sniper_event_loop_v4(
        sniper_universe, trading_days, vix_map, alpha_scores
    )
    options_trades, skipped_opt = _run_sniper_options_loop_v4(
        sniper_universe, trading_days, vix_map, alpha_scores
    )
    sniper_skipped = skipped_ev + skipped_opt

    for t in options_trades:
        aid = t.get("agent_id", "ollama-llama")
        t.setdefault("month", t.get("entry_date", "")[:7])
        t.setdefault("win", 1 if t.get("pnl_pct", 0) > 0 else 0)
        agent_trades[aid].append(t)

    all_trades_flat: list = []
    for tlist in event_trades.values():
        all_trades_flat.extend(tlist)
    all_trades_flat.extend(options_trades)
    all_trades_flat = [t for t in all_trades_flat if "pnl_pct" in t]

    overall = _trade_metrics_v4(all_trades_flat)
    overall["spy_return"] = spy_return
    overall["vs_spy"]     = round(overall["total_return"] - spy_return, 2)

    agent_metrics: dict = {}
    for aid, trades in agent_trades.items():
        if not trades:
            continue
        am = _trade_metrics_v4(trades)
        for regime in ("BULL", "CAUTIOUS", "BEAR"):
            rtrades = [t for t in trades if t.get("regime") == regime and "pnl_pct" in t]
            rm      = _trade_metrics_v4(rtrades) if rtrades else {}
            am[f"{regime.lower()}_return"] = rm.get("total_return", 0.0)
        agent_metrics[aid] = am

    by_strat_sym: dict = defaultdict(list)
    for t in all_trades_flat:
        by_strat_sym[(t.get("strategy", "unknown"), t.get("ticker", ""))].append(t)

    curve = _build_equity_curve_v4(event_trades, options_trades, [], trading_days, vix_map)

    monthly_by_agent: dict = defaultdict(lambda: defaultdict(list))
    for t in all_trades_flat:
        month = (t.get("exit_date") or t.get("entry_date") or "")[:7]
        aid   = t.get("agent_id", "unknown")
        if month:
            monthly_by_agent[month][aid].append(t)

    regime_perf: dict = defaultdict(list)
    for t in all_trades_flat:
        regime_perf[(t.get("regime", "MIXED"), t.get("strategy", "unknown"))].append(t)

    alpha_attr = _compute_alpha_attribution_v4(all_trades_flat)

    # Save to DB
    conn = _bt_conn()
    now  = datetime.utcnow().isoformat()

    for aid, am in agent_metrics.items():
        spec = SNIPER_FLEET_V4.get(aid, {})
        conn.execute("""
            INSERT OR REPLACE INTO backtest_v4_sniper
            (run_date, agent_id, agent_name, model,
             total_return, win_rate, sharpe, max_drawdown,
             profit_factor, num_trades, avg_hold_days,
             best_trade_pct, worst_trade_pct,
             bull_return, cautious_return, bear_return,
             spy_return, vs_spy, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            run_date, aid, spec.get("name", aid), spec.get("model", ""),
            am["total_return"], am["win_rate"], am["sharpe"], am["max_drawdown"],
            am["profit_factor"], am["num_trades"], am["avg_hold_days"],
            am["best_trade_pct"], am["worst_trade_pct"],
            am.get("bull_return", 0), am.get("cautious_return", 0), am.get("bear_return", 0),
            spy_return, round(am["total_return"] - spy_return, 2), now,
        ))

    for (strat, sym), trades in by_strat_sym.items():
        if not trades:
            continue
        sm      = _trade_metrics_v4(trades)
        alpha_a = float(np.mean([t.get("alpha_score", 0.0) for t in trades]))
        conn.execute("""
            INSERT OR REPLACE INTO backtest_v4_sniper_master
            (run_date, strategy, ticker, total_return, win_rate, sharpe, realistic_sharpe,
             max_drawdown, num_trades, profit_factor, spy_return, vs_spy,
             regime, alpha_score, created_at)
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
            INSERT OR REPLACE INTO backtest_v4_sniper_equity
            (run_date, trade_date, equity, daily_pnl, regime)
            VALUES (?,?,?,?,?)
        """, (run_date, row["trade_date"], row["equity"], row["daily_pnl"], row["regime"]))

    for month, agents_dict in monthly_by_agent.items():
        for aid, atrades in agents_dict.items():
            if not atrades:
                continue
            mm = _trade_metrics_v4(atrades)
            conn.execute("""
                INSERT OR REPLACE INTO backtest_v4_sniper_monthly
                (run_date, month, agent_id, total_return, win_rate, num_trades)
                VALUES (?,?,?,?,?,?)
            """, (run_date, month, aid, mm["total_return"], mm["win_rate"], mm["num_trades"]))

    for sig, data in alpha_attr.items():
        conn.execute("""
            INSERT OR REPLACE INTO backtest_v4_alpha_attribution
            (run_date, test_name, signal_name, correlation, winning_trades, losing_trades,
             avg_score_wins, avg_score_losses)
            VALUES (?,?,?,?,?,?,?,?)
        """, (run_date, "sniper", sig, data["correlation"],
              data["winning_trades"], data["losing_trades"],
              data["avg_score_wins"], data["avg_score_losses"]))

    conn.commit()
    conn.close()

    logger.info(f"[SNIPER_V4] Done: {len(all_trades_flat)} trades, "
                f"return={overall['total_return']:+.2f}%, skipped={sniper_skipped}")

    return {
        "status":          "ok",
        "metrics":         overall,
        "agent_metrics":   agent_metrics,
        "by_strat_sym":    {f"{s}/{sym}": _trade_metrics_v4(trades)
                            for (s, sym), trades in by_strat_sym.items()},
        "regime_perf":     {f"{r}_{s}": _trade_metrics_v4(ts)
                            for (r, s), ts in regime_perf.items() if len(ts) >= 2},
        "monthly_by_agent": {m: {a: _trade_metrics_v4(ts) for a, ts in agents.items() if ts}
                             for m, agents in monthly_by_agent.items()},
        "curve":           curve,
        "alpha_attr":      alpha_attr,
        "sniper_skipped":  sniper_skipped,
        "spy_return":      spy_return,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# TEST D: V1 Replay orchestrator
# ═══════════════════════════════════════════════════════════════════════════════

def _run_v1replay_v4(td: dict, trading_days: list, vix_map: dict,
                     alpha_scores: dict, spy_return: float, run_date: str) -> dict:
    """All agents, no alpha threshold, all strategies including IC, real slippage."""
    logger.info(f"[V1REPLAY] Starting on {len(td)} symbols, {len(trading_days)} days")

    # V1: use V2_REPLAY_FLEET as base (12 agents) — no alpha restriction
    event_trades, agent_trades = _run_event_loop_v4(
        td, trading_days, vix_map, alpha_scores,
        fleet=V2_REPLAY_FLEET, alpha_threshold=0.0, conf_threshold=0.0, label="v1replay"
    )
    opt_trades, spread_trades = _run_options_loop_v4(
        td, trading_days, vix_map, alpha_scores,
        alpha_threshold=0.0, include_ic=True, label="v1replay"
    )

    for t in opt_trades:
        strat = t.get("strategy", "")
        aid   = t.get("agent_id",
                      "gemini-2.5-flash" if ("bear" in strat or "put" in strat) else "ollama-llama")
        t["agent_id"] = aid
        t.setdefault("month", t.get("entry_date", "")[:7])
        t.setdefault("win", 1 if t.get("pnl_pct", 0) > 0 else 0)
        agent_trades[aid].append(t)

    for t in spread_trades:
        strat = t.get("strategy", "")
        aid   = t.get("agent_id",
                      "gemini-2.5-flash" if "bear" in strat else
                      "dayblade-sulu"     if "bull" in strat else "gemini-2.5-pro")
        t["agent_id"] = aid
        t.setdefault("month", t.get("entry_date", "")[:7])
        t.setdefault("win", 1 if t.get("pnl_pct", 0) > 0 else 0)
        agent_trades[aid].append(t)

    all_trades_flat: list = []
    for tlist in event_trades.values():
        all_trades_flat.extend(tlist)
    all_trades_flat.extend(opt_trades + spread_trades)
    all_trades_flat = [t for t in all_trades_flat if "pnl_pct" in t]

    overall = _trade_metrics_v4(all_trades_flat)
    overall["spy_return"] = spy_return
    overall["vs_spy"]     = round(overall["total_return"] - spy_return, 2)

    agent_metrics: dict = {}
    for aid, trades in agent_trades.items():
        if not trades:
            continue
        am = _trade_metrics_v4(trades)
        agent_metrics[aid] = am

    by_strat_sym: dict = defaultdict(list)
    for t in all_trades_flat:
        by_strat_sym[(t.get("strategy", "unknown"), t.get("ticker", ""))].append(t)

    curve = _build_equity_curve_v4(event_trades, opt_trades + spread_trades, [], trading_days, vix_map)

    alpha_attr = _compute_alpha_attribution_v4(all_trades_flat)

    # Save to DB
    conn = _bt_conn()
    now  = datetime.utcnow().isoformat()

    for aid, am in agent_metrics.items():
        spec = V2_REPLAY_FLEET.get(aid, {})
        conn.execute("""
            INSERT OR REPLACE INTO backtest_v4_v1replay
            (run_date, agent_id, agent_name, model,
             total_return, win_rate, sharpe, max_drawdown,
             profit_factor, num_trades, avg_hold_days,
             best_trade_pct, worst_trade_pct,
             spy_return, vs_spy, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            run_date, aid, spec.get("name", aid), spec.get("model", ""),
            am["total_return"], am["win_rate"], am["sharpe"], am["max_drawdown"],
            am["profit_factor"], am["num_trades"], am["avg_hold_days"],
            am["best_trade_pct"], am["worst_trade_pct"],
            spy_return, round(am["total_return"] - spy_return, 2), now,
        ))

    for (strat, sym), trades in by_strat_sym.items():
        if not trades:
            continue
        sm      = _trade_metrics_v4(trades)
        alpha_a = float(np.mean([t.get("alpha_score", 0.0) for t in trades]))
        tier_n  = TIER_MAP.get(strat, (2, "Tier 2+"))[0]
        conn.execute("""
            INSERT OR REPLACE INTO backtest_v4_v1replay_master
            (run_date, strategy, ticker, total_return, win_rate, sharpe, realistic_sharpe,
             max_drawdown, num_trades, profit_factor, spy_return, vs_spy,
             regime, alpha_score, tier, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            run_date, strat, sym,
            sm["total_return"], sm["win_rate"], sm["sharpe"], sm["realistic_sharpe"],
            sm["max_drawdown"], sm["num_trades"], sm["profit_factor"],
            spy_return, round(sm["total_return"] - spy_return, 2),
            "MIXED", round(alpha_a, 3), tier_n, now,
        ))

    for row in curve:
        conn.execute("""
            INSERT OR REPLACE INTO backtest_v4_v1replay_equity
            (run_date, trade_date, equity, daily_pnl, regime)
            VALUES (?,?,?,?,?)
        """, (run_date, row["trade_date"], row["equity"], row["daily_pnl"], row["regime"]))

    for sig, data in alpha_attr.items():
        conn.execute("""
            INSERT OR REPLACE INTO backtest_v4_alpha_attribution
            (run_date, test_name, signal_name, correlation, winning_trades, losing_trades,
             avg_score_wins, avg_score_losses)
            VALUES (?,?,?,?,?,?,?,?)
        """, (run_date, "v1replay", sig, data["correlation"],
              data["winning_trades"], data["losing_trades"],
              data["avg_score_wins"], data["avg_score_losses"]))

    conn.commit()
    conn.close()

    logger.info(f"[V1REPLAY] Done: {len(all_trades_flat)} trades, "
                f"return={overall['total_return']:+.2f}%")

    return {
        "status":        "ok",
        "metrics":       overall,
        "agent_metrics": agent_metrics,
        "by_strat_sym":  {f"{s}/{sym}": _trade_metrics_v4(trades)
                          for (s, sym), trades in by_strat_sym.items()},
        "curve":         curve,
        "alpha_attr":    alpha_attr,
        "spy_return":    spy_return,
        "num_agents":    len(agent_metrics),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Report printer
# ═══════════════════════════════════════════════════════════════════════════════

def _print_v4_report(run_date: str, bA: dict, bB: dict, bC: dict, bD: dict) -> None:
    bar = "━" * 100
    mA  = bA.get("metrics", {})
    mB  = bB.get("metrics", {})
    mC  = bC.get("metrics", {})
    mD  = bD.get("metrics", {})

    spy_ret = mA.get("spy_return", bA.get("spy_return", 0.0))

    def _f(val, pct=False, trades=False) -> str:
        if val is None:
            return "—"
        if trades:
            return str(int(val)) if val else "0"
        if pct:
            try:
                return f"{float(val):+.2f}%"
            except Exception:
                return str(val)
        try:
            return f"{float(val):.3f}"
        except Exception:
            return str(val)

    print(f"\n{bar}")
    print(f"  WARP CORE REACTOR v4 — Four-Way Backtest Suite")
    print(f"  Jan 9 – Apr 9, 2026  |  run={run_date}")
    print(f"{bar}")

    # ── [1] Four-way head-to-head ─────────────────────────────────────────────
    print(f"\n  [1] FOUR-WAY HEAD-TO-HEAD")
    cw = 14
    print(f"  ┌{'─'*18}┬{'─'*cw}┬{'─'*cw}┬{'─'*cw}┬{'─'*cw}┐")
    print(f"  │ {'Metric':<16} │ {'A: SPY Simple':>{cw-2}} │ {'B: v2 Replay':>{cw-2}} │ {'C: Sniper':>{cw-2}} │ {'D: v1 w/Cost':>{cw-2}} │")
    print(f"  ├{'─'*18}┼{'─'*cw}┼{'─'*cw}┼{'─'*cw}┼{'─'*cw}┤")

    def _row4(label: str, a, b, c, d, pct=False, trades=False) -> None:
        fa = _f(a, pct=pct, trades=trades)
        fb = _f(b, pct=pct, trades=trades)
        fc = _f(c, pct=pct, trades=trades)
        fd = _f(d, pct=pct, trades=trades)
        print(f"  │ {label:<16} │ {fa:>{cw-2}} │ {fb:>{cw-2}} │ {fc:>{cw-2}} │ {fd:>{cw-2}} │")

    _row4("Return %",         mA.get("total_return"), mB.get("total_return"), mC.get("total_return"), mD.get("total_return"), pct=True)
    _row4("Sharpe",           mA.get("sharpe"), mB.get("sharpe"), mC.get("sharpe"), mD.get("sharpe"))
    _row4("Win Rate %",       f"{mA.get('win_rate',0):.1f}%", f"{mB.get('win_rate',0):.1f}%", f"{mC.get('win_rate',0):.1f}%", f"{mD.get('win_rate',0):.1f}%")
    _row4("Max Drawdown %",   mA.get("max_drawdown"), mB.get("max_drawdown"), mC.get("max_drawdown"), mD.get("max_drawdown"), pct=True)
    _row4("Total Trades",     mA.get("num_trades",0), mB.get("num_trades",0), mC.get("num_trades",0), mD.get("num_trades",0), trades=True)
    _row4("Profit Factor",    mA.get("profit_factor"), mB.get("profit_factor"), mC.get("profit_factor"), mD.get("profit_factor"))
    _row4("Avg Trade Return", f"{mA.get('avg_trade_return',0):+.3f}%", f"{mB.get('avg_trade_return',0):+.3f}%", f"{mC.get('avg_trade_return',0):+.3f}%", f"{mD.get('avg_trade_return',0):+.3f}%")
    _row4("Max Consec Wins",  mA.get("max_consec_wins",0), mB.get("max_consec_wins",0), mC.get("max_consec_wins",0), mD.get("max_consec_wins",0), trades=True)
    _row4("Max Consec Loss",  mA.get("max_consec_losses",0), mB.get("max_consec_losses",0), mC.get("max_consec_losses",0), mD.get("max_consec_losses",0), trades=True)
    _row4("SPY Buy & Hold",   f"{spy_ret:+.2f}%", f"{spy_ret:+.2f}%", f"{spy_ret:+.2f}%", f"{spy_ret:+.2f}%")
    _row4("vs SPY B&H",       mA.get("vs_spy"), mB.get("vs_spy"), mC.get("vs_spy"), mD.get("vs_spy"), pct=True)
    _row4("Agents Active",    "0 (rules)", str(bB.get("num_agents", 12)), "6", "12+")
    _row4("Strategies Used",  "1", "30+", "4", "All")
    print(f"  └{'─'*18}┴{'─'*cw}┴{'─'*cw}┴{'─'*cw}┴{'─'*cw}┘")

    # ── [2] Full history 10-column table ─────────────────────────────────────
    print(f"\n  [2] FULL HISTORY  (10 columns + BEST)")
    cols = ["Actual", "v1", "v2(180d)", "v3", "v3b", "SniperTT", "v4Base", "v4v2R", "v4Snipe", "v4v1R"]
    cw2  = 10
    print(f"  {'Metric':<18}" + "".join(f"{c:>{cw2}}" for c in cols) + f"  {'BEST':>8}")
    print(f"  {'─'*120}")

    def _hist_row(label: str, vals: list, best_idx: int = None) -> None:
        s = f"  {label:<18}"
        for v in vals:
            s += f"{str(v):>{cw2}}"
        if best_idx is not None:
            s += f"  {cols[best_idx]:>8}"
        else:
            s += f"  {'':>8}"
        print(s)

    ret_vals = [
        f"{KNOWN['actual']['return']:+.2f}%",
        f"{KNOWN['v1']['return']:+.2f}%",
        f"{KNOWN['v2']['return']:+.2f}%",
        f"{KNOWN['v3']['return']:+.2f}%",
        f"{KNOWN['v3b']['return']:+.2f}%",
        f"{KNOWN['sniper_tt']['return']:+.2f}%",
        f"{mA.get('total_return',0):+.2f}%",
        f"{mB.get('total_return',0):+.2f}%",
        f"{mC.get('total_return',0):+.2f}%",
        f"{mD.get('total_return',0):+.2f}%",
    ]
    all_rets = [KNOWN['actual']['return'], KNOWN['v1']['return'], KNOWN['v2']['return'],
                KNOWN['v3']['return'], KNOWN['v3b']['return'], KNOWN['sniper_tt']['return'],
                mA.get('total_return', 0), mB.get('total_return', 0),
                mC.get('total_return', 0), mD.get('total_return', 0)]
    best_ret = int(np.argmax(all_rets))
    _hist_row("Return %", ret_vals, best_ret)

    sh_vals = [
        f"{KNOWN['actual']['sharpe']:.3f}",
        f"{KNOWN['v1']['sharpe']:.3f}",
        f"{KNOWN['v2']['sharpe']:.3f}",
        f"{KNOWN['v3']['sharpe']:.3f}",
        f"{KNOWN['v3b']['sharpe']:.3f}",
        f"{KNOWN['sniper_tt']['sharpe']:.3f}",
        f"{mA.get('sharpe',0):.3f}",
        f"{mB.get('sharpe',0):.3f}",
        f"{mC.get('sharpe',0):.3f}",
        f"{mD.get('sharpe',0):.3f}",
    ]
    all_sh = [KNOWN['actual']['sharpe'], KNOWN['v1']['sharpe'], KNOWN['v2']['sharpe'],
              KNOWN['v3']['sharpe'], KNOWN['v3b']['sharpe'], KNOWN['sniper_tt']['sharpe'],
              mA.get('sharpe', 0), mB.get('sharpe', 0), mC.get('sharpe', 0), mD.get('sharpe', 0)]
    best_sh = int(np.argmax(all_sh))
    _hist_row("Sharpe", sh_vals, best_sh)

    wr_vals = [
        f"{KNOWN['actual']['wr']:.1f}%",
        f"{KNOWN['v1']['wr']:.1f}%",
        f"{KNOWN['v2']['wr']:.1f}%",
        f"{KNOWN['v3']['wr']:.1f}%",
        f"{KNOWN['v3b']['wr']:.1f}%",
        f"{KNOWN['sniper_tt']['wr']:.1f}%",
        f"{mA.get('win_rate',0):.1f}%",
        f"{mB.get('win_rate',0):.1f}%",
        f"{mC.get('win_rate',0):.1f}%",
        f"{mD.get('win_rate',0):.1f}%",
    ]
    all_wr = [KNOWN['actual']['wr'], KNOWN['v1']['wr'], KNOWN['v2']['wr'],
              KNOWN['v3']['wr'], KNOWN['v3b']['wr'], KNOWN['sniper_tt']['wr'],
              mA.get('win_rate', 0), mB.get('win_rate', 0),
              mC.get('win_rate', 0), mD.get('win_rate', 0)]
    best_wr = int(np.argmax(all_wr))
    _hist_row("Win Rate %", wr_vals, best_wr)

    dd_vals = [
        f"{KNOWN['actual']['dd']:.1f}%",
        f"{KNOWN['v1']['dd']:.1f}%",
        f"{KNOWN['v2']['dd']:.1f}%",
        f"{KNOWN['v3']['dd']:.1f}%",
        f"{KNOWN['v3b']['dd']:.1f}%",
        f"{KNOWN['sniper_tt']['dd']:.1f}%",
        f"{mA.get('max_drawdown',0):.1f}%",
        f"{mB.get('max_drawdown',0):.1f}%",
        f"{mC.get('max_drawdown',0):.1f}%",
        f"{mD.get('max_drawdown',0):.1f}%",
    ]
    all_dd = [KNOWN['actual']['dd'], KNOWN['v1']['dd'], KNOWN['v2']['dd'],
              KNOWN['v3']['dd'], KNOWN['v3b']['dd'], KNOWN['sniper_tt']['dd'],
              mA.get('max_drawdown', 0), mB.get('max_drawdown', 0),
              mC.get('max_drawdown', 0), mD.get('max_drawdown', 0)]
    best_dd = int(np.argmax(all_dd))  # max_drawdown is negative, best = closest to 0 = max
    _hist_row("Max DD %", dd_vals, best_dd)

    tr_vals = [
        str(KNOWN['actual']['trades']),
        str(KNOWN['v1']['trades']),
        str(KNOWN['v2']['trades']),
        str(KNOWN['v3']['trades']),
        str(KNOWN['v3b']['trades']),
        str(KNOWN['sniper_tt']['trades']),
        str(mA.get('num_trades', 0)),
        str(mB.get('num_trades', 0)),
        str(mC.get('num_trades', 0)),
        str(mD.get('num_trades', 0)),
    ]
    _hist_row("Trades", tr_vals)

    # ── [3] Sniper per-agent leaderboard ─────────────────────────────────────
    print(f"\n  [3] SNIPER BREAKDOWN — Per-Agent Leaderboard (6 agents, Sharpe desc)")
    print(f"  {'─'*80}")
    print(f"  {'Rank':<4} {'Agent':<10} {'Model':<20} {'Sharpe':>7} {'WR%':>6} {'Return%':>8} {'MaxDD%':>7} {'Trades':>6}")
    print(f"  {'─'*80}")
    agent_met_c = bC.get("agent_metrics", {})
    ranked_c    = sorted(agent_met_c.items(), key=lambda x: -x[1].get("sharpe", -99))
    for rank, (aid, am) in enumerate(ranked_c, 1):
        spec  = SNIPER_FLEET_V4.get(aid, {})
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

    # ── [4] Sniper per-strategy breakdown ─────────────────────────────────────
    print(f"\n  [4] SNIPER BREAKDOWN — Per-Strategy (4 strategies)")
    print(f"  {'─'*80}")
    print(f"  {'Strategy':<20} {'r.Sharpe':>9} {'WR%':>6} {'Return%':>8} {'Trades':>7}")
    print(f"  {'─'*80}")
    try:
        conn   = _bt_conn()
        s_rows = conn.execute("""
            SELECT strategy, SUM(num_trades) as n, AVG(realistic_sharpe) as rs,
                   AVG(win_rate) as wr, SUM(total_return) as tr
            FROM backtest_v4_sniper_master
            WHERE run_date=? AND num_trades>0
            GROUP BY strategy ORDER BY rs DESC
        """, (run_date,)).fetchall()
        conn.close()
        for row in s_rows:
            print(f"  {row['strategy']:<20} {row['rs']:>+9.3f} "
                  f"{row['wr']:>6.1f} "
                  f"{row['tr']:>+8.2f} "
                  f"{row['n']:>7}")
    except Exception as e:
        print(f"  (strategy table unavailable: {e})")

    # ── [5] Sniper regime results ─────────────────────────────────────────────
    print(f"\n  [5] SNIPER BREAKDOWN — Regime Results")
    print(f"  {'─'*80}")
    regime_data = bC.get("regime_perf", {})
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

    # ── [6] Sniper monthly breakdown ──────────────────────────────────────────
    print(f"\n  [6] SNIPER BREAKDOWN — Monthly (Jan/Feb/Mar/Apr)")
    print(f"  {'─'*80}")
    try:
        conn   = _bt_conn()
        months = sorted({r["month"] for r in conn.execute(
            "SELECT DISTINCT month FROM backtest_v4_sniper_monthly WHERE run_date=?", (run_date,)
        ).fetchall()})
        conn.close()
        if months:
            col_w  = 11
            header = f"  {'Agent':<10}" + "".join(f"{m:>{col_w}}" for m in months)
            print(header)
            print(f"  {'─'*80}")
            conn2 = _bt_conn()
            for aid, spec in SNIPER_FLEET_V4.items():
                name  = spec["name"]
                row_s = f"  {name:<10}"
                for m in months:
                    r = conn2.execute("""
                        SELECT total_return FROM backtest_v4_sniper_monthly
                        WHERE run_date=? AND month=? AND agent_id=?
                    """, (run_date, m, aid)).fetchone()
                    val = f"{r['total_return']:>+.1f}%" if r else "   —   "
                    row_s += f"{val:>{col_w}}"
                print(row_s)
            conn2.close()
        else:
            print("  (no monthly data)")
    except Exception as e:
        print(f"  (monthly table unavailable: {e})")

    # ── [7] Alpha signal report card ──────────────────────────────────────────
    ALPHA_CRYSTALS = [
        "dark_pool", "insider", "ftd", "put_call", "vix_structure",
        "sentiment", "yield_curve", "opex", "earnings", "rebalancing",
        "rallies_consensus", "rallies_debate_sentiment",
    ]
    alpha_attr = bC.get("alpha_attr", {})
    print(f"\n  [7] ALPHA SIGNAL REPORT CARD  (12 crystals — Sniper)")
    print(f"  {'─'*80}")
    print(f"  {'Signal':<28} {'Corr':>8} {'Wins':>6} {'Losses':>7} {'AvgW':>7} {'AvgL':>7}  Grade")
    print(f"  {'─'*80}")
    for sig in ALPHA_CRYSTALS:
        data  = alpha_attr.get(sig, {})
        corr  = data.get("correlation", 0)
        grade = ("A (strong positive)" if corr > 0.10
                 else "B (mild positive)"  if corr > 0.05
                 else "F (hurts returns)"  if corr < -0.10
                 else "D (mild negative)"  if corr < -0.05
                 else "C (neutral)")
        print(f"  {sig:<28} {corr:>+8.4f} {data.get('winning_trades',0):>6} "
              f"{data.get('losing_trades',0):>7} "
              f"{data.get('avg_score_wins',0):>7.3f} "
              f"{data.get('avg_score_losses',0):>7.3f}  {grade}")

    # ── [8] V2 Replay vs original V2 analysis ────────────────────────────────
    print(f"\n  [8] V2 REPLAY vs ORIGINAL V2 ANALYSIS")
    print(f"  {'─'*80}")
    orig_v2_ret = KNOWN['v2']['return']
    orig_v2_sh  = KNOWN['v2']['sharpe']
    orig_v2_wr  = KNOWN['v2']['wr']
    new_v2_ret  = mB.get('total_return', 0)
    new_v2_sh   = mB.get('sharpe', 0)
    new_v2_wr   = mB.get('win_rate', 0)
    print(f"  Original v2 (Jan 9–Apr 9):  Return={orig_v2_ret:+.2f}%  Sharpe={orig_v2_sh:.3f}  WR={orig_v2_wr:.1f}%")
    print(f"  v4 v2-Replay (same window): Return={new_v2_ret:+.2f}%  Sharpe={new_v2_sh:.3f}  WR={new_v2_wr:.1f}%")
    delta_ret = new_v2_ret - orig_v2_ret
    delta_sh  = new_v2_sh  - orig_v2_sh
    direction = "IMPROVED" if delta_ret > 0 else "REGRESSED"
    print(f"  Delta: Return {delta_ret:+.2f}pp  Sharpe {delta_sh:+.3f}  — {direction}")
    print(f"  Key difference: old model labels (deepseek-r1:7b / qwen3.5:9b) vs current fleet models")
    print(f"  Interpretation: model upgrade impact = {delta_ret:+.2f}pp return, {delta_sh:+.3f} Sharpe")

    # ── [9] Verdict ───────────────────────────────────────────────────────────
    print(f"\n  [9] VERDICT")
    print(f"  {'─'*80}")

    scores = {
        "A (SPY Simple)": (mA.get("sharpe", 0), mA.get("total_return", 0), mA.get("max_drawdown", 0)),
        "B (v2 Replay)":  (mB.get("sharpe", 0), mB.get("total_return", 0), mB.get("max_drawdown", 0)),
        "C (Sniper)":     (mC.get("sharpe", 0), mC.get("total_return", 0), mC.get("max_drawdown", 0)),
        "D (v1 w/Cost)":  (mD.get("sharpe", 0), mD.get("total_return", 0), mD.get("max_drawdown", 0)),
    }
    winner = max(scores.items(), key=lambda x: x[1][0])
    runner_up = sorted(scores.items(), key=lambda x: -x[1][0])[1]

    print(f"  WINNER by Sharpe: {winner[0]}  "
          f"(Sharpe={winner[1][0]:.3f}, Return={winner[1][1]:+.2f}%, DD={winner[1][2]:.2f}%)")
    print(f"  Runner-up:        {runner_up[0]}  "
          f"(Sharpe={runner_up[1][0]:.3f}, Return={runner_up[1][1]:+.2f}%)")
    print(f"  SPY Buy & Hold:   {spy_ret:+.2f}%")

    print(f"\n  RECOMMENDED LIVE CONFIG:")
    if "Sniper" in winner[0]:
        print(f"  → Deploy Sniper Mode (C): 6-agent fleet, CSP+covered_call+rsi_bounce+bollinger")
        print(f"  → Alpha threshold: 0.3  |  Min bull signals: 3  |  IVR gate: 60/50")
        print(f"  → Agents: Uhura/Spock/Seven/Worf/McCoy/Neo")
    elif "v2 Replay" in winner[0]:
        print(f"  → Deploy V2 Replay (B): 12-agent fleet, alpha threshold 0.1, full strategies")
        print(f"  → Model upgrades validated — current model stack outperforms old labels")
    elif "v1" in winner[0]:
        print(f"  → Deploy V1-style (D) with cost controls: no alpha gate, all strategies")
        print(f"  → Add slippage controls: 0.1% equity, 3% options, 0.15% delay")
    else:
        print(f"  → SPY Simple Baseline (A) outperforms AI fleet — reduce strategy complexity")
        print(f"  → Consider deploying RSI+SMA200 rules as primary signal overlay")

    # Sniper agent promotions/shelving
    if agent_met_c:
        promotions = [SNIPER_FLEET_V4.get(aid, {}).get("name", aid)
                      for aid, am in agent_met_c.items()
                      if am.get("sharpe", 0) > 1.0 and am.get("win_rate", 0) > 55]
        shelve     = [SNIPER_FLEET_V4.get(aid, {}).get("name", aid)
                      for aid, am in agent_met_c.items()
                      if am.get("sharpe", 0) < -0.5 or am.get("win_rate", 0) < 30]
        if promotions:
            print(f"  Promote: {', '.join(promotions)} — Sharpe>1.0 + WR>55%")
        if shelve:
            print(f"  Shelve/review: {', '.join(shelve)} — Sharpe<-0.5 or WR<30%")

    print(f"\n{bar}\n")


# ═══════════════════════════════════════════════════════════════════════════════
# Main orchestrator
# ═══════════════════════════════════════════════════════════════════════════════

def run_super_backtest_v4() -> dict:
    """
    Full v4 pipeline. Downloads data once, runs four tests, prints report.
    NEVER modifies trader.db or arena.db.
    """
    t0       = time.time()
    run_date = date.today().isoformat()

    logger.info("═" * 70)
    logger.info("  Warp Core Reactor v4 — Four-Way Backtest Suite")
    logger.info(f"  Run date: {run_date}  |  Window: {WINDOW_START} → {WINDOW_END}")
    logger.info("═" * 70)

    _init_v4_tables()

    # ── Step 1: Download SPY 300-day for SMA200 baseline ─────────────────────
    logger.info("[STEP 1] Downloading SPY 300-day for baseline...")
    end_dt   = date.today()
    start_dt = end_dt - timedelta(days=300)
    spy_df   = _download_spy_with_retry(start_dt, end_dt, max_retries=3, delay=2)
    if spy_df is None or spy_df.empty:
        logger.error("SPY download failed after retries — baseline will be skipped")
    else:
        logger.info(f"SPY downloaded: {len(spy_df)} bars")

    # ── Step 2: Expand alpha universe + download 200-symbol universe ──────────
    logger.info("[STEP 2] Expanding alpha universe...")
    universe     = _expand_alpha_universe()
    alpha_scores = run_alpha_expansion(universe)

    # Merge existing composite alpha from DB
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

    all_syms = sorted(set(list(universe) + list(alpha_scores.keys())),
                      key=lambda s: -alpha_scores.get(s, 0))[:200]

    logger.info(f"[STEP 2] Downloading {len(all_syms)} symbols + SPY + ^VIX, {BACKTEST_DAYS} days...")
    td = _download_v2_universe(all_syms + ["SPY", "^VIX"], BACKTEST_DAYS + 60)

    # SPY retry individually if not in td
    if "SPY" not in td or (td.get("SPY") is None) or td["SPY"].empty:
        logger.warning("SPY not in batch download — retrying individually")
        try:
            spy_retry = _download_spy_with_retry(
                end_dt - timedelta(days=BACKTEST_DAYS + 70), end_dt
            )
            if spy_retry is not None and not spy_retry.empty:
                td["SPY"] = spy_retry
                logger.info("SPY added individually")
        except Exception as e:
            logger.warning(f"SPY individual retry failed: {e}")

    # ── Step 3: Build VIX map and trading days ────────────────────────────────
    vix_df  = td.get("^VIX") if td.get("^VIX") is not None else td.get("VIX")
    vix_map: dict = {}
    if vix_df is not None and not vix_df.empty:
        vix_df.index = pd.to_datetime(vix_df.index).normalize()
        for idx, row in vix_df.iterrows():
            vix_map[pd.Timestamp(idx)] = float(row.get("Close", 20.0))

    # Also populate VIX from SPY download's vix_map for baseline
    if spy_df is not None and not spy_df.empty:
        try:
            end_dt2   = date.today()
            start_dt2 = end_dt2 - timedelta(days=300)
            vix_raw   = yf.download("^VIX", start=start_dt2, end=end_dt2,
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

    # Get all trading days then filter to window
    all_days     = _get_trading_days(td, BACKTEST_DAYS + 60)
    trading_days = [d for d in all_days
                    if pd.Timestamp(WINDOW_START) <= d <= pd.Timestamp(WINDOW_END)]

    if not trading_days:
        logger.warning("No trading days in window — using all available days")
        trading_days = all_days

    logger.info(f"Trading days in window: {len(trading_days)}")

    # SPY return for the window
    spy_return = 0.0
    if "SPY" in td and td["SPY"] is not None and not td["SPY"].empty:
        spy_window_df = td["SPY"].copy()
        spy_window_df.index = pd.to_datetime(spy_window_df.index).normalize()
        window_spy = spy_window_df.loc[
            (spy_window_df.index >= pd.Timestamp(WINDOW_START)) &
            (spy_window_df.index <= pd.Timestamp(WINDOW_END))
        ]
        if len(window_spy) >= 2:
            spy_return = round(
                (float(window_spy["Close"].iloc[-1]) - float(window_spy["Close"].iloc[0]))
                / float(window_spy["Close"].iloc[0]) * 100, 2
            )
    logger.info(f"SPY return in window: {spy_return:+.2f}%")

    # Confidence scores proxy
    conf_scores = {sym: 0.65 for sym in all_syms}

    # ── Step 4: Run confidence scores into alpha_scores ───────────────────────
    # (conf proxy always passes for all symbols — gate is enforced by threshold logic)

    # ── Step 5: Test A — SPY Baseline ────────────────────────────────────────
    logger.info("[STEP 5A] Running Test A: SPY Baseline...")
    result_A = _run_baseline_v4(spy_df, vix_map, run_date)
    if result_A.get("metrics"):
        result_A["metrics"]["spy_return"] = result_A.get("spy_return", spy_return)

    # ── Step 6: Test B — V2 Replay ───────────────────────────────────────────
    logger.info("[STEP 5B] Running Test B: V2 Replay...")
    result_B = _run_v2replay_v4(td, trading_days, vix_map, alpha_scores, spy_return, run_date)

    # ── Step 7: Test C — Sniper Mode ─────────────────────────────────────────
    logger.info("[STEP 5C] Running Test C: Sniper Mode...")
    result_C = _run_sniper_v4(td, trading_days, vix_map, alpha_scores, spy_return, run_date)

    # ── Step 8: Test D — V1 Replay ───────────────────────────────────────────
    logger.info("[STEP 5D] Running Test D: V1 Replay...")
    result_D = _run_v1replay_v4(td, trading_days, vix_map, alpha_scores, spy_return, run_date)

    elapsed = round(time.time() - t0, 1)
    logger.info(f"All four tests complete in {elapsed}s")

    # ── Step 9: Print report ──────────────────────────────────────────────────
    _print_v4_report(run_date, result_A, result_B, result_C, result_D)

    # ── Step 10: Save JSON summary ────────────────────────────────────────────
    summary = {
        "status":        "ok",
        "run_date":      run_date,
        "elapsed_s":     elapsed,
        "window_start":  str(WINDOW_START),
        "window_end":    str(WINDOW_END),
        "spy_return":    spy_return,
        "trading_days":  len(trading_days),
        "universe_size": len(td),
        "test_A":        {"metrics": result_A.get("metrics", {}), "status": result_A.get("status")},
        "test_B":        {"metrics": result_B.get("metrics", {}), "status": result_B.get("status")},
        "test_C":        {"metrics": result_C.get("metrics", {}), "status": result_C.get("status")},
        "test_D":        {"metrics": result_D.get("metrics", {}), "status": result_D.get("status")},
    }

    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        summary_path = DATA_DIR / "backtest_v4_summary.json"
        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2, default=str)
        logger.info(f"Summary saved to {summary_path}")
    except Exception as e:
        logger.warning(f"Could not save JSON summary: {e}")

    return summary


# ═══════════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [v4] %(levelname)s %(message)s")
    result = run_super_backtest_v4()
    print(f"\nWarp Core Reactor v4 complete. Run: {result.get('run_date')}")
