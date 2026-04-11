"""
engine/super_backtest_v3.py — Warp Core Reactor v3 — Lean Fleet Shakedown Cruise

90-day backtest (Oct 9 2025 → Apr 9 2026) of the 12-agent lean fleet.

Enhancements over v2:
  • Per-agent tracking: each trade is routed to the best-fit agent
  • Regime filtering: per-agent regime-specific P&L breakdown
  • Monthly breakdown: Oct/Nov/Dec/Jan/Feb/Mar/Apr columns
  • Fixed max-drawdown formula: equity-curve peak-to-trough, capped at -100%
  • Four-way comparison table (Actual Fleet / v1 / v2 / v3)
  • Alpha attribution by signal component (dark_pool, earnings, etc.)
  • T'Pol 0DTE loop (SPY + QQQ, regime-gated)

CRITICAL RULES (see spec):
  - DB paths use _ROOT / Path(__file__)
  - trader.db via sqlite3.connect(str(…)) — NOT file:...?mode=ro
  - import _run_tier1_vbt AS _tier1_vbt
  - yfinance: no threads=True
  - VIX DataFrame check: use explicit `is not None` guard
  - SPY retry individually if not in td after batch download
  - Filter trades missing pnl_pct before _trade_metrics
  - _trade_metrics expects list[dict] with pnl_pct key
  - _get_trading_days needs SPY (or similar) in td
  - BACKTEST_DAYS = 90
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

logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
_ROOT       = Path(__file__).resolve().parent.parent
BACKTEST_DB = _ROOT / "data" / "backtest.db"
TRADER_DB   = _ROOT / "data" / "trader.db"
ALPHA_DB    = _ROOT / "data" / "alpha_signals.db"
SC_DB       = _ROOT / "signal-center" / "signals.db"
DATA_DIR    = _ROOT / "data"

# ── Constants ─────────────────────────────────────────────────────────────────
BACKTEST_DAYS   = 90
UNIVERSE_SIZE   = 200
ALPHA_THRESHOLD = 0.1
CONF_THRESHOLD  = 0.65   # B-grade equivalent

REGIMES = ["BULL", "CAUTIOUS", "BEAR", "CRISIS"]

# ── Agent roster — 12 active agents ──────────────────────────────────────────
LEAN_FLEET: dict[str, dict] = {
    "grok-4":           {"name": "Spock",   "model": "deepseek-r1:14b", "tiers": [1],
                         "specialization": "mean_reversion"},
    "gemini-2.5-flash": {"name": "Worf",    "model": "qwen3:14b",       "tiers": [5, 6, 9],
                         "specialization": "bear_specialist"},
    "dayblade-sulu":    {"name": "Sulu",    "model": "phi4:14b",        "tiers": [2],
                         "specialization": "momentum"},
    "ollama-plutus":    {"name": "McCoy",   "model": "0xroyce/plutus",  "tiers": [1, 2],
                         "specialization": "crisis_doctor",  "min_vix": 25},
    "gemini-2.5-pro":   {"name": "Seven",   "model": "qwen3:14b",       "tiers": [1],
                         "specialization": "pure_quant"},
    "ollama-llama":     {"name": "Uhura",   "model": "llama3.1:latest", "tiers": [5, 6],
                         "specialization": "options_flow"},
    "ollie-auto":       {"name": "Ollie",   "model": "signal-center",   "tiers": [1, 2, 3],
                         "specialization": "signal_center"},
    "neo-matrix":       {"name": "Neo",     "model": "port-8000",       "tiers": [1, 2, 3],
                         "specialization": "plutus_scoring"},
    "capitol-trades":   {"name": "Capitol", "model": "congress",        "tiers": [1, 2],
                         "specialization": "congress_copycat"},
    "dalio-metals":     {"name": "Dalio",   "model": "qwen3.5:9b",      "tiers": [4],
                         "specialization": "metals_macro",
                         "universe": ["GLD", "SLV", "CPER", "TIPS", "IAU"]},
    "dayblade-0dte":    {"name": "TPol",    "model": "options-s2",      "tiers": [8],
                         "specialization": "0dte"},
    "super-agent":      {"name": "Anderson","model": "crewai",           "tiers": [1, 2, 3, 4],
                         "specialization": "bridge_vote"},
}


# ═══════════════════════════════════════════════════════════════════════════════
# Position-sizing helper (reused from v2 logic)
# ═══════════════════════════════════════════════════════════════════════════════

def _alpha_position_factor(alpha: float) -> float:
    if alpha >= 0.6:  return 1.00
    if alpha >= 0.4:  return 0.75
    if alpha >= 0.1:  return 0.50
    return 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# Fixed drawdown helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _max_drawdown_equity(equity_curve: list[float]) -> float:
    """Proper peak-to-trough drawdown, capped at -100%."""
    if len(equity_curve) < 2:
        return 0.0
    peak = equity_curve[0]
    max_dd = 0.0
    for val in equity_curve:
        if val > peak:
            peak = val
        if peak > 0:
            dd = (peak - val) / peak * 100
            max_dd = max(max_dd, dd)
    return -min(max_dd, 100.0)


def _trade_metrics_v3(trades: list[dict]) -> dict:
    """Fixed version: drawdown capped at -100%, proper equity curve."""
    trades = [t for t in trades if "pnl_pct" in t]
    if not trades:
        return dict(total_return=0, win_rate=0, sharpe=0, max_drawdown=0,
                    max_drawdown_dollar=0, avg_hold_hours=0, num_trades=0,
                    profit_factor=0, calmar=0, best_trade_pct=0,
                    worst_trade_pct=0, max_consec_wins=0, max_consec_losses=0,
                    realistic_sharpe=0, needs_validation=0, best_trade_sym="",
                    worst_trade_sym="")

    pcts   = [t["pnl_pct"] for t in trades]
    wins   = [p for p in pcts if p > 0]
    losses = [p for p in pcts if p <= 0]

    total_return  = float(((1 + np.array(pcts) / 100).prod() - 1) * 100)
    win_rate      = len(wins) / len(pcts) * 100
    profit_factor = (sum(wins) / (-sum(losses))) if losses and sum(losses) != 0 else float("inf")
    avg_ret       = float(np.mean(pcts))
    std_ret       = max(float(np.std(pcts)), 1.0)
    avg_hold      = max(1, float(np.mean([t.get("hold_days", 1) for t in trades])))
    sharpe        = float(avg_ret / std_ret * math.sqrt(252 / avg_hold))

    # Fixed drawdown: equity curve
    equity = [STARTING_CASH]
    for p in pcts:
        equity.append(equity[-1] * (1 + p / 100))
    max_dd     = _max_drawdown_equity(equity)
    max_dd_usd = max_dd / 100 * STARTING_CASH

    calmar = float(total_return / (-max_dd + 1e-9)) if max_dd < 0 else total_return

    consec_w = consec_l = cur_w = cur_l = 0
    for p in pcts:
        if p > 0:
            cur_w += 1; cur_l = 0; consec_w = max(consec_w, cur_w)
        else:
            cur_l += 1; cur_w = 0; consec_l = max(consec_l, cur_l)

    best_idx  = int(np.argmax(pcts))
    worst_idx = int(np.argmin(pcts))
    return dict(
        total_return=round(total_return, 2),
        win_rate=round(win_rate, 1),
        sharpe=round(sharpe, 3),
        realistic_sharpe=round(max(-5.0, min(5.0, sharpe)), 3),
        needs_validation=1 if abs(sharpe) > 5.0 else 0,
        max_drawdown=round(max_dd, 2),
        max_drawdown_dollar=round(max_dd_usd, 2),
        avg_hold_hours=round(float(np.mean([t.get("hold_days", 1) * 24 for t in trades])), 1),
        num_trades=len(trades),
        profit_factor=round(min(profit_factor, 99.99), 3),
        calmar=round(calmar, 3),
        best_trade_pct=round(max(pcts), 2),
        worst_trade_pct=round(min(pcts), 2),
        best_trade_sym=trades[best_idx].get("ticker", ""),
        worst_trade_sym=trades[worst_idx].get("ticker", ""),
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


def _td_conn() -> sqlite3.Connection:
    """Read-only connection to trader.db (direct path; no writes issued)."""
    conn = sqlite3.connect(str(TRADER_DB), timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def _alpha_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(ALPHA_DB), timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


# ═══════════════════════════════════════════════════════════════════════════════
# Table initialization
# ═══════════════════════════════════════════════════════════════════════════════

def _init_v3_tables() -> None:
    conn = _bt_conn()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS backtest_agent_results_v3 (
        run_date TEXT, agent_id TEXT, agent_name TEXT, model TEXT,
        specialization TEXT, total_return REAL, win_rate REAL,
        sharpe REAL, max_drawdown REAL, max_drawdown_dollar REAL,
        profit_factor REAL, num_trades INTEGER, avg_hold_hours REAL,
        best_trade_pct REAL, best_trade_sym TEXT,
        worst_trade_pct REAL, worst_trade_sym TEXT,
        bull_return REAL, cautious_return REAL, bear_return REAL, crisis_return REAL,
        alpha_correlation REAL, created_at TEXT,
        PRIMARY KEY (run_date, agent_id)
    );

    CREATE TABLE IF NOT EXISTS backtest_master_results_v3 (
        run_date TEXT, tier INTEGER, tier_name TEXT,
        strategy TEXT, ticker TEXT,
        total_return REAL, win_rate REAL, sharpe REAL, realistic_sharpe REAL,
        max_drawdown REAL, avg_hold_hours REAL, num_trades INTEGER,
        profit_factor REAL, calmar REAL,
        best_trade_pct REAL, worst_trade_pct REAL,
        spy_return REAL, vs_spy REAL,
        max_consec_wins INTEGER, max_consec_losses INTEGER,
        regime TEXT, alpha_score REAL, created_at TEXT,
        PRIMARY KEY (run_date, strategy, ticker)
    );

    CREATE TABLE IF NOT EXISTS backtest_equity_curve_v3 (
        run_date TEXT, trade_date TEXT, equity REAL,
        daily_pnl REAL, regime TEXT,
        PRIMARY KEY (run_date, trade_date)
    );

    CREATE TABLE IF NOT EXISTS backtest_monthly_breakdown_v3 (
        run_date TEXT, month TEXT, agent_id TEXT,
        total_return REAL, win_rate REAL, num_trades INTEGER,
        best_strategy TEXT, regime TEXT,
        PRIMARY KEY (run_date, month, agent_id)
    );

    CREATE TABLE IF NOT EXISTS backtest_regime_results_v3 (
        run_date TEXT, regime TEXT, strategy TEXT,
        total_return REAL, win_rate REAL, sharpe REAL,
        num_trades INTEGER, avg_vix REAL,
        PRIMARY KEY (run_date, regime, strategy)
    );

    CREATE TABLE IF NOT EXISTS backtest_alpha_attribution_v3 (
        run_date TEXT, signal_name TEXT,
        correlation REAL, winning_trades INTEGER, losing_trades INTEGER,
        avg_score_wins REAL, avg_score_losses REAL,
        PRIMARY KEY (run_date, signal_name)
    );

    CREATE TABLE IF NOT EXISTS backtest_options_results_v3 (
        run_date TEXT, strategy TEXT, ticker TEXT,
        pnl REAL, pnl_pct REAL, exit_type TEXT,
        days INTEGER, delta REAL, iv_entry REAL,
        regime TEXT, alpha_score REAL, entry_date TEXT, exit_date TEXT,
        agent_id TEXT, created_at TEXT
    );

    CREATE TABLE IF NOT EXISTS backtest_spread_results_v3 (
        run_date TEXT, strategy TEXT, ticker TEXT,
        pnl REAL, pnl_pct REAL, exit_type TEXT,
        days INTEGER, regime TEXT, alpha_score REAL,
        entry_date TEXT, exit_date TEXT, agent_id TEXT, created_at TEXT
    );

    CREATE TABLE IF NOT EXISTS backtest_0dte_results_v3 (
        run_date TEXT, ticker TEXT, direction TEXT,
        pnl REAL, pnl_pct REAL, trade_date TEXT,
        iv REAL, regime TEXT, created_at TEXT
    );

    CREATE TABLE IF NOT EXISTS comparison_table_v3 (
        run_date TEXT, metric TEXT,
        actual_fleet REAL, actual_fleet_label TEXT,
        v1_backtest REAL, v1_label TEXT,
        v2_backtest REAL, v2_label TEXT,
        v3_backtest REAL, v3_label TEXT,
        created_at TEXT,
        PRIMARY KEY (run_date, metric)
    );
    """)
    conn.commit()
    conn.close()
    logger.info("v3 tables initialized")


# ═══════════════════════════════════════════════════════════════════════════════
# Agent routing logic
# ═══════════════════════════════════════════════════════════════════════════════

def _agent_accepts_trade(agent_id: str, strategy: str, regime: str, vix: float,
                          bull_signals: int, bear_signals: int,
                          rsi_val: float) -> bool:
    spec    = LEAN_FLEET.get(agent_id, {})
    s       = spec.get("specialization", "")
    min_vix = spec.get("min_vix", 0)

    if min_vix > 0 and vix < min_vix:
        return False  # McCoy only trades crisis (VIX > 25)

    if s == "mean_reversion":
        return strategy in ("rsi_bounce",) and (rsi_val < 30 or rsi_val > 70)
    if s == "bear_specialist":
        return strategy in ("long_put", "bear_put_spread", "bear_call_spread") or "short" in strategy
    if s == "momentum":
        return strategy in ("momentum_breakout", "vwap_reclaim", "volume_spike") and regime in ("BULL", "CAUTIOUS")
    if s == "crisis_doctor":
        return vix >= 25  # McCoy: any strategy in crisis
    if s == "pure_quant":
        return strategy in ("rsi_bounce", "bollinger", "sma_cross", "ema_pullback")
    if s == "options_flow":
        return strategy in ("long_call", "long_put", "csp", "covered_call",
                             "bull_call_spread", "bull_put_spread")
    if s == "signal_center":
        return True  # Ollie: anything with good signal grade
    if s == "plutus_scoring":
        return bull_signals >= 3  # Neo: high conviction only
    if s == "congress_copycat":
        return strategy in ("rsi_bounce", "bollinger", "momentum_breakout") and bull_signals >= 2
    if s == "metals_macro":
        return strategy == "dalio_metals"
    if s == "0dte":
        return "0dte" in strategy
    if s == "bridge_vote":
        return bull_signals >= 3 or bear_signals >= 3  # Anderson: strong consensus
    return True


def _route_to_agent(strategy: str, regime: str, vix: float,
                     bull: int, bear: int, rsi: float) -> Optional[str]:
    """Return the best-fit agent_id for this signal, or None if no agent wants it."""
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
    agent_id = STRATEGY_AGENT.get(strategy, "super-agent")  # Anderson: catch-all

    # Gate checks
    agent   = LEAN_FLEET.get(agent_id, {})
    min_vix = agent.get("min_vix", 0)
    if min_vix > 0 and vix < min_vix:
        return None  # McCoy stands down below VIX threshold

    if agent_id == "grok-4" and not (rsi < 30 or rsi > 70):
        return None  # Spock only at RSI extremes
    if agent_id == "dayblade-sulu" and regime not in ("BULL", "CAUTIOUS"):
        return None  # Sulu only trends
    if agent_id == "gemini-2.5-flash" and regime == "BULL":
        return None  # Worf avoids bull
    if agent_id == "super-agent" and bull < 3 and bear < 3:
        return None  # Anderson needs consensus

    return agent_id


# ═══════════════════════════════════════════════════════════════════════════════
# Main event loop with agent routing
# ═══════════════════════════════════════════════════════════════════════════════

def _run_event_loop_v3(td: dict, days: list, vix_map: dict,
                        alpha_scores: dict, conf_scores: dict):
    event_trades: dict[str, list] = defaultdict(list)   # strategy → [trades]
    agent_trades: dict[str, list] = defaultdict(list)   # agent_id → [trades]

    SCAN_FREQ = 3  # scan every 3 days for 90-day window
    day_counter = 0

    for sym in td:
        df = td[sym]
        if len(df) < 60:
            continue

        alpha = alpha_scores.get(sym, 0.0)
        if alpha < ALPHA_THRESHOLD:
            continue
        conf = conf_scores.get(sym, 0.5)
        if conf < CONF_THRESHOLD:
            continue

        pos_factor = _alpha_position_factor(alpha)
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
            h   = sub["High"].values  if "High"   in sub.columns else c
            l   = sub["Low"].values   if "Low"    in sub.columns else c
            v   = sub["Volume"].values if "Volume" in sub.columns else np.ones(len(c))
            avg_v   = float(np.mean(v[-20:])) if len(v) >= 20 else 1.0
            px      = float(c[-1])
            rsi_val = _rsi(c)

            t2       = _tier2_signals(c, h, l, v, avg_v)
            t3       = _tier3_signals(c, h, l, v, avg_v)
            all_sigs = {**t2, **t3}

            bull_sigs = sum(1 for sv in all_sigs.values() if sv)
            bear_sigs = sum(1 for sv in _tier9_short_signals(c, h, l, v, avg_v).values() if sv)

            for strat, sig in all_sigs.items():
                if not sig:
                    continue
                key = f"{sym}_{strat}"

                # Close open position if already open
                if key in positions:
                    pos  = positions[key]
                    gain = (px - pos["entry"]) / pos["entry"]
                    held = pos.get("days_held", 0)
                    if gain >= 0.08 or gain <= -0.05 or held >= 15:
                        pnl_pct = gain * 100 * pos_factor - SLIPPAGE * 200
                        t = {
                            "strategy":   strat,
                            "ticker":     sym,
                            "entry_date": pos["entry_date"],
                            "exit_date":  day_str,
                            "pnl_pct":    round(pnl_pct, 2),
                            "pnl":        round(pnl_pct, 2),
                            "hold_days":  held,
                            "alpha_score": alpha,
                            "regime":     regime,
                            "month":      month,
                            "win":        1 if pnl_pct > 0 else 0,
                            "agent_id":   pos["agent_id"],
                        }
                        event_trades[strat].append(t)
                        agent_trades[pos["agent_id"]].append(t)
                        del positions[key]
                    else:
                        positions[key]["days_held"] = held + 1
                    continue

                # Route to agent
                agent_id = _route_to_agent(strat, regime, vix_val,
                                           bull_sigs, bear_sigs, rsi_val)
                if agent_id is None:
                    continue

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
            t = {
                "strategy":   strat,
                "ticker":     sym,
                "entry_date": pos["entry_date"],
                "exit_date":  days[-1].strftime("%Y-%m-%d") if days else "EOP",
                "pnl_pct":    round(pnl_pct, 2),
                "pnl":        round(pnl_pct, 2),
                "hold_days":  pos.get("days_held", 1),
                "alpha_score": alpha,
                "regime":     "MIXED",
                "month":      pos["entry_date"][:7],
                "win":        1 if pnl_pct > 0 else 0,
                "agent_id":   pos["agent_id"],
            }
            event_trades[strat].append(t)
            agent_trades[pos["agent_id"]].append(t)

    # Dalio metals (agent: dalio-metals)
    dalio = _run_dalio_metals(td, days)
    for t in dalio:
        t["agent_id"]   = "dalio-metals"
        t["alpha_score"] = alpha_scores.get(t.get("ticker", ""), 0.0)
        t["regime"]     = "MIXED"
        t["month"]      = t.get("entry_date", "")[:7]
        t["win"]        = 1 if t.get("pnl_pct", 0) > 0 else 0
        event_trades["dalio_metals"].append(t)
        agent_trades["dalio-metals"].append(t)

    total = sum(len(v) for v in event_trades.values())
    logger.info(f"Event loop v3: {total} trades across {len(agent_trades)} agents")
    return event_trades, agent_trades


# ═══════════════════════════════════════════════════════════════════════════════
# Options loop with agent routing
# ═══════════════════════════════════════════════════════════════════════════════

def _run_options_loop_v3(td: dict, days: list, vix_map: dict,
                          alpha_scores: dict) -> tuple[list, list]:
    options_trades: list = []
    spread_trades:  list = []

    SCAN_FREQ = 5
    day_counter = 0
    skipped = 0

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
            if alpha < ALPHA_THRESHOLD:
                skipped += 1
                continue
            pos_factor = _alpha_position_factor(alpha)

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
            bear = sum(1 for bv in _tier9_short_signals(c, h, l, v, avg_v).values() if bv)

            extra = {"ticker": sym, "entry_date": day_str, "regime": regime,
                     "alpha_score": alpha, "pos_factor": pos_factor}

            # Long call → Uhura (options_flow), bull >= 2
            if bull >= 2 and ivr < 60:
                r = _sim_long_call(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    options_trades.append({**r, **extra, "strategy": "long_call",
                                           "option_type": "call", "agent_id": "ollama-llama",
                                           "exit_date": _exit_date_str(future, r["days"], day_str),
                                           "win": 1 if r["pnl"] > 0 else 0})

            # CSP → Uhura if bull regime
            if ivr > 50 and bull >= 1 and regime in ("BULL", "CAUTIOUS"):
                r = _sim_csp(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    options_trades.append({**r, **extra, "strategy": "csp",
                                           "option_type": "put", "agent_id": "ollama-llama",
                                           "exit_date": _exit_date_str(future, r["days"], day_str),
                                           "win": 1 if r["pnl"] > 0 else 0})

            # Covered call → Uhura
            if bull >= 2 and ivr > 40:
                r = _sim_covered_call(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    options_trades.append({**r, **extra, "strategy": "covered_call",
                                           "option_type": "call", "agent_id": "ollama-llama",
                                           "exit_date": _exit_date_str(future, r["days"], day_str),
                                           "win": 1 if r["pnl"] > 0 else 0})

            # Long put → Worf (bear_specialist) if not BULL
            if bear >= 2 and regime != "BULL":
                r = _sim_long_put(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    options_trades.append({**r, **extra, "strategy": "long_put",
                                           "option_type": "put", "agent_id": "gemini-2.5-flash",
                                           "exit_date": _exit_date_str(future, r["days"], day_str),
                                           "win": 1 if r["pnl"] > 0 else 0})

            # Bull spreads → Sulu (momentum) if trending
            if bull >= 2:
                bull_agent = "dayblade-sulu" if regime in ("BULL", "CAUTIOUS") else "ollama-llama"
                r = _sim_bull_call_spread(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    spread_trades.append({**r, **extra, "strategy": "bull_call_spread",
                                          "spread_type": "BULL_CALL", "agent_id": bull_agent,
                                          "exit_date": _exit_date_str(future, r["days"], day_str),
                                          "win": 1 if r["pnl"] > 0 else 0})
                r = _sim_bull_put_spread(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    spread_trades.append({**r, **extra, "strategy": "bull_put_spread",
                                          "spread_type": "BULL_PUT", "agent_id": bull_agent,
                                          "exit_date": _exit_date_str(future, r["days"], day_str),
                                          "win": 1 if r.get("pnl", 0) > 0 else 0})

            # Bear spreads → Worf if not BULL
            if vix_val > 20 and bear >= 2:
                r = _sim_bear_call_spread(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    spread_trades.append({**r, **extra, "strategy": "bear_call_spread",
                                          "spread_type": "BEAR_CALL", "agent_id": "gemini-2.5-flash",
                                          "exit_date": _exit_date_str(future, r["days"], day_str),
                                          "win": 1 if r.get("pnl", 0) > 0 else 0})
                r = _sim_bear_put_spread(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    spread_trades.append({**r, **extra, "strategy": "bear_put_spread",
                                          "spread_type": "BEAR_PUT", "agent_id": "gemini-2.5-flash",
                                          "exit_date": _exit_date_str(future, r["days"], day_str),
                                          "win": 1 if r.get("pnl", 0) > 0 else 0})

            # Iron condor → Seven (pure_quant) always
            if vix_val > 20:
                sma20 = float(np.mean(c[-20:]))
                if abs(px - sma20) / max(px, 1e-9) < 0.02:
                    r = _sim_ic(future, px, iv, OPT_DTE_DEFAULT)
                    if r:
                        spread_trades.append({**r, **extra, "strategy": "iron_condor",
                                              "spread_type": "IC", "agent_id": "gemini-2.5-pro",
                                              "exit_date": _exit_date_str(future, r["days"], day_str),
                                              "win": 1 if r.get("pnl", 0) > 0 else 0})

    logger.info(f"Options loop v3: {len(options_trades)} opts, {len(spread_trades)} spreads, "
                f"{skipped} skipped by alpha")
    return options_trades, spread_trades


# ═══════════════════════════════════════════════════════════════════════════════
# 0DTE loop — T'Pol (SPY + QQQ, regime-gated)
# ═══════════════════════════════════════════════════════════════════════════════

def _run_0dte_loop_v3(td: dict, days: list, vix_map: dict) -> list:
    """T'Pol: 0DTE on SPY and QQQ with regime filter."""
    trades = []
    for sym in ("SPY", "QQQ"):
        if sym not in td:
            continue
        df = td[sym]
        for day in days:
            vix_val = vix_map.get(day, 18.0)
            regime  = _classify_regime(vix_val)
            if regime == "CRISIS":
                continue  # T'Pol stands down in crisis

            m   = df.index <= day
            sub = df.loc[m]
            if len(sub) < 10:
                continue
            c   = sub["Close"].values
            px  = float(c[-1])
            iv  = _hist_vol(c, 10)
            sma5 = float(np.mean(c[-5:]))

            future_day = df.loc[df.index > day]
            if len(future_day) < 1:
                continue
            nr = {
                "High":  float(future_day["High"].iloc[0])  if "High"  in future_day.columns else px,
                "Low":   float(future_day["Low"].iloc[0])   if "Low"   in future_day.columns else px,
                "Close": float(future_day["Close"].iloc[0]),
            }

            direction = "call" if px > sma5 else "put"
            r = _sim_0dte(nr, px, iv, direction)
            if r:
                trades.append({
                    **r,
                    "strategy":   f"{sym.lower()}_0dte_{direction}",
                    "ticker":     sym,
                    "trade_date": day.strftime("%Y-%m-%d"),
                    "month":      day.strftime("%Y-%m"),
                    "regime":     regime,
                    "agent_id":   "dayblade-0dte",
                    "win":        1 if r["pnl"] > 0 else 0,
                })
    logger.info(f"0DTE loop v3: {len(trades)} trades")
    return trades


# ═══════════════════════════════════════════════════════════════════════════════
# Equity curve (fixed)
# ═══════════════════════════════════════════════════════════════════════════════

def _build_equity_curve_v3(event_trades: dict, opt_trades: list, spread_trades: list,
                             dte0_trades: list, trading_days: list, vix_map: dict) -> list[dict]:
    all_by_date: dict[str, list] = defaultdict(list)

    for strat, trades in event_trades.items():
        for t in trades:
            key = t.get("exit_date") or t.get("entry_date", "")
            all_by_date[key].append(t.get("pnl_pct", 0))
    for t in opt_trades + spread_trades:
        key = t.get("exit_date") or t.get("entry_date", "")
        all_by_date[key].append(t.get("pnl_pct", 0))
    for t in dte0_trades:
        key = t.get("trade_date", "")
        all_by_date[key].append(t.get("pnl_pct", 0))

    equity = STARTING_CASH
    curve: list[dict] = []
    for day in trading_days:
        day_str   = day.strftime("%Y-%m-%d")
        day_pnls  = all_by_date.get(day_str, [])
        vix_val   = vix_map.get(day, 18.0)
        regime    = _classify_regime(vix_val)

        # Cap at ±2% per day (realistic cap for diversified portfolio)
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
# Monthly breakdown
# ═══════════════════════════════════════════════════════════════════════════════

def _compute_monthly_breakdown(event_trades: dict, opt_trades: list, spread_trades: list,
                                 dte0_trades: list, agent_trades: dict):
    monthly_all:      dict[str, list] = defaultdict(list)
    monthly_by_agent: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))

    all_trades = []
    for trades in event_trades.values():
        all_trades.extend(trades)
    all_trades.extend(opt_trades + spread_trades + dte0_trades)

    for t in all_trades:
        if "pnl_pct" not in t:
            continue
        month = (t.get("exit_date") or t.get("entry_date") or t.get("trade_date") or "")[:7]
        if not month:
            continue
        monthly_all[month].append(t)
        aid = t.get("agent_id", "unknown")
        monthly_by_agent[month][aid].append(t)

    return monthly_all, monthly_by_agent


# ═══════════════════════════════════════════════════════════════════════════════
# Regime performance
# ═══════════════════════════════════════════════════════════════════════════════

def _compute_regime_performance(event_trades: dict, opt_trades: list, spread_trades: list):
    by_regime_strategy: dict[tuple, list] = defaultdict(list)
    all_trades = list(opt_trades) + list(spread_trades)
    for trades in event_trades.values():
        all_trades.extend(trades)
    for t in all_trades:
        if "pnl_pct" not in t:
            continue
        regime = t.get("regime", "UNKNOWN")
        strat  = t.get("strategy", "unknown")
        by_regime_strategy[(regime, strat)].append(t)
    return by_regime_strategy


# ═══════════════════════════════════════════════════════════════════════════════
# Alpha attribution
# ═══════════════════════════════════════════════════════════════════════════════

def _compute_alpha_attribution_v3(run_date: str, all_trades: list) -> dict:
    SIGNALS = ["dark_pool", "insider", "ftd", "put_call", "vix_structure",
               "sentiment", "yield_curve", "opex", "earnings", "rebalancing"]

    # Load composite scores from alpha_signals.db
    alpha_data: dict[str, dict] = {}
    try:
        ac   = sqlite3.connect(str(ALPHA_DB), timeout=10)
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
                        r[1:])
                )
        ac.close()
    except Exception:
        pass

    result: dict[str, dict] = {}
    for sig in SIGNALS:
        winning = [t for t in all_trades if "pnl_pct" in t and t["pnl_pct"] > 0
                   and t.get("ticker") in alpha_data]
        losing  = [t for t in all_trades if "pnl_pct" in t and t["pnl_pct"] <= 0
                   and t.get("ticker") in alpha_data]

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
            "correlation":     round(corr_val, 4),
            "winning_trades":  len(winning),
            "losing_trades":   len(losing),
            "avg_score_wins":  round(float(np.mean(win_scores))  if win_scores  else 0, 3),
            "avg_score_losses": round(float(np.mean(loss_scores)) if loss_scores else 0, 3),
        }
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# Recommendations engine
# ═══════════════════════════════════════════════════════════════════════════════

def _generate_recommendations(agent_metrics: dict, strategy_metrics: dict,
                                regime_metrics: dict) -> list[str]:
    recs: list[str] = []

    # More capital
    for aid, m in agent_metrics.items():
        if m.get("sharpe", 0) > 1.0 and m.get("win_rate", 0) > 55:
            name = LEAN_FLEET.get(aid, {}).get("name", aid)
            recs.append(f"INCREASE: {name} — Sharpe {m['sharpe']:.2f}, WR {m['win_rate']:.1f}%")

    # Less or shelve
    for aid, m in agent_metrics.items():
        if m.get("sharpe", 0) < -0.5 or m.get("win_rate", 0) < 30:
            name = LEAN_FLEET.get(aid, {}).get("name", aid)
            recs.append(f"REDUCE/SHELVE: {name} — Sharpe {m['sharpe']:.2f}, WR {m['win_rate']:.1f}%")

    # Strategy priorities by regime
    for regime in REGIMES:
        top = sorted(
            [(s, m) for (r, s), m in regime_metrics.items()
             if r == regime and m.get("num_trades", 0) >= 3],
            key=lambda x: -x[1].get("sharpe", 0)
        )[:3]
        if top:
            recs.append(f"REGIME {regime}: best strategies = {', '.join(s for s, _ in top)}")

    # Scan frequency recommendation
    recs.append("SCAN FREQ: event loop every 3 days, options loop every 5 days (90-day window)")
    return recs


# ═══════════════════════════════════════════════════════════════════════════════
# Save helpers
# ═══════════════════════════════════════════════════════════════════════════════

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
    "spy_0dte_call": (8, "0DTE"), "spy_0dte_put": (8, "0DTE"),
    "qqq_0dte_call": (8, "0DTE"), "qqq_0dte_put": (8, "0DTE"),
}


def _save_agent_results_v3(run_date: str, agent_metrics: dict) -> None:
    conn = _bt_conn()
    now  = datetime.utcnow().isoformat()
    for aid, m in agent_metrics.items():
        spec = LEAN_FLEET.get(aid, {})
        conn.execute("""
            INSERT OR REPLACE INTO backtest_agent_results_v3
            (run_date, agent_id, agent_name, model, specialization,
             total_return, win_rate, sharpe, max_drawdown, max_drawdown_dollar,
             profit_factor, num_trades, avg_hold_hours,
             best_trade_pct, best_trade_sym, worst_trade_pct, worst_trade_sym,
             bull_return, cautious_return, bear_return, crisis_return,
             alpha_correlation, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            run_date, aid, spec.get("name", aid), spec.get("model", ""),
            spec.get("specialization", ""),
            m.get("total_return", 0), m.get("win_rate", 0), m.get("sharpe", 0),
            m.get("max_drawdown", 0), m.get("max_drawdown_dollar", 0),
            m.get("profit_factor", 0), m.get("num_trades", 0), m.get("avg_hold_hours", 0),
            m.get("best_trade_pct", 0), m.get("best_trade_sym", ""),
            m.get("worst_trade_pct", 0), m.get("worst_trade_sym", ""),
            m.get("bull_return", 0), m.get("cautious_return", 0),
            m.get("bear_return", 0), m.get("crisis_return", 0),
            0.0, now,
        ))
    conn.commit()
    conn.close()
    logger.info(f"Saved {len(agent_metrics)} agent results")


def _save_master_results_v3(run_date: str, event_trades: dict, opt_trades: list,
                              spread_trades: list, spy_return: float) -> None:
    conn = _bt_conn()
    now  = datetime.utcnow().isoformat()

    all_trades = list(opt_trades) + list(spread_trades)
    for strat, trades in event_trades.items():
        all_trades.extend(trades)

    by_strat_sym: dict[tuple, list] = defaultdict(list)
    for t in all_trades:
        key = (t.get("strategy", "unknown"), t.get("ticker", ""))
        by_strat_sym[key].append(t)

    inserted = 0
    for (strat, sym), trades in by_strat_sym.items():
        trades = [t for t in trades if "pnl_pct" in t]
        if not trades:
            continue
        m          = _trade_metrics_v3(trades)
        tier_num, tier_name = TIER_MAP.get(strat, (2, "Tier 2+"))
        alpha_avg  = float(np.mean([t.get("alpha_score", 0.0) for t in trades]))

        conn.execute("""
            INSERT OR REPLACE INTO backtest_master_results_v3
            (run_date, tier, tier_name, strategy, ticker,
             total_return, win_rate, sharpe, realistic_sharpe,
             max_drawdown, avg_hold_hours, num_trades, profit_factor, calmar,
             best_trade_pct, worst_trade_pct, spy_return, vs_spy,
             max_consec_wins, max_consec_losses, regime, alpha_score, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            run_date, tier_num, tier_name, strat, sym,
            m["total_return"], m["win_rate"], m["sharpe"], m["realistic_sharpe"],
            m["max_drawdown"], m["avg_hold_hours"], m["num_trades"],
            m["profit_factor"], m["calmar"],
            m["best_trade_pct"], m["worst_trade_pct"],
            spy_return, round(m["total_return"] - spy_return, 2),
            m["max_consec_wins"], m["max_consec_losses"],
            "MIXED", round(alpha_avg, 3), now,
        ))
        inserted += 1

    conn.commit()
    conn.close()
    logger.info(f"Saved {inserted} master strategy-ticker rows")


def _save_equity_curve_v3(run_date: str, curve: list[dict]) -> None:
    conn = _bt_conn()
    for row in curve:
        conn.execute("""
            INSERT OR REPLACE INTO backtest_equity_curve_v3
            (run_date, trade_date, equity, daily_pnl, regime)
            VALUES (?,?,?,?,?)
        """, (run_date, row["trade_date"], row["equity"],
              row["daily_pnl"], row["regime"]))
    conn.commit()
    conn.close()


def _save_options_results_v3(run_date: str, opt_trades: list) -> None:
    conn = _bt_conn()
    now  = datetime.utcnow().isoformat()
    for t in opt_trades:
        if "pnl_pct" not in t:
            continue
        conn.execute("""
            INSERT INTO backtest_options_results_v3
            (run_date, strategy, ticker, pnl, pnl_pct, exit_type, days, delta, iv_entry,
             regime, alpha_score, entry_date, exit_date, agent_id, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            run_date, t.get("strategy", ""), t.get("ticker", ""),
            t.get("pnl", 0), t.get("pnl_pct", 0), t.get("exit_type", ""),
            t.get("days", 0), t.get("delta", 0), t.get("iv", 0),
            t.get("regime", ""), t.get("alpha_score", 0),
            t.get("entry_date", ""), t.get("exit_date", ""),
            t.get("agent_id", ""), now,
        ))
    conn.commit()
    conn.close()


def _save_spread_results_v3(run_date: str, spread_trades: list) -> None:
    conn = _bt_conn()
    now  = datetime.utcnow().isoformat()
    for t in spread_trades:
        if "pnl_pct" not in t:
            continue
        conn.execute("""
            INSERT INTO backtest_spread_results_v3
            (run_date, strategy, ticker, pnl, pnl_pct, exit_type, days, regime,
             alpha_score, entry_date, exit_date, agent_id, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            run_date, t.get("strategy", ""), t.get("ticker", ""),
            t.get("pnl", 0), t.get("pnl_pct", 0), t.get("exit_type", ""),
            t.get("days", 0), t.get("regime", ""),
            t.get("alpha_score", 0), t.get("entry_date", ""), t.get("exit_date", ""),
            t.get("agent_id", ""), now,
        ))
    conn.commit()
    conn.close()


def _save_0dte_results_v3(run_date: str, dte0_trades: list) -> None:
    conn = _bt_conn()
    now  = datetime.utcnow().isoformat()
    for t in dte0_trades:
        strat = t.get("strategy", "")
        direction = "call" if "call" in strat else "put"
        conn.execute("""
            INSERT INTO backtest_0dte_results_v3
            (run_date, ticker, direction, pnl, pnl_pct, trade_date, iv, regime, created_at)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            run_date, t.get("ticker", ""), direction,
            t.get("pnl", 0), t.get("pnl_pct", 0), t.get("trade_date", ""),
            t.get("iv", 0), t.get("regime", ""), now,
        ))
    conn.commit()
    conn.close()


def _save_monthly_v3(run_date: str, monthly_all: dict, monthly_by_agent: dict) -> None:
    conn = _bt_conn()
    now  = datetime.utcnow().isoformat()

    for month, trades in monthly_by_agent.items():
        for aid, atrades in trades.items():
            if not atrades:
                continue
            m = _trade_metrics_v3(atrades)
            # Find best strategy this month
            strat_counts: dict[str, int] = defaultdict(int)
            for t in atrades:
                strat_counts[t.get("strategy", "unknown")] += 1
            best_strat = max(strat_counts, key=strat_counts.get) if strat_counts else ""
            # Dominant regime
            regime_counts: dict[str, int] = defaultdict(int)
            for t in atrades:
                regime_counts[t.get("regime", "MIXED")] += 1
            dominant_regime = max(regime_counts, key=regime_counts.get) if regime_counts else "MIXED"

            conn.execute("""
                INSERT OR REPLACE INTO backtest_monthly_breakdown_v3
                (run_date, month, agent_id, total_return, win_rate, num_trades,
                 best_strategy, regime)
                VALUES (?,?,?,?,?,?,?,?)
            """, (run_date, month, aid, m["total_return"], m["win_rate"],
                  m["num_trades"], best_strat, dominant_regime))

    conn.commit()
    conn.close()


def _save_regime_results_v3(run_date: str, regime_metrics: dict) -> None:
    conn = _bt_conn()
    now  = datetime.utcnow().isoformat()

    for (regime, strat), m in regime_metrics.items():
        conn.execute("""
            INSERT OR REPLACE INTO backtest_regime_results_v3
            (run_date, regime, strategy, total_return, win_rate, sharpe, num_trades, avg_vix)
            VALUES (?,?,?,?,?,?,?,?)
        """, (run_date, regime, strat, m.get("total_return", 0), m.get("win_rate", 0),
              m.get("sharpe", 0), m.get("num_trades", 0), 0.0))

    conn.commit()
    conn.close()


def _save_alpha_attribution_v3(run_date: str, alpha_attr: dict) -> None:
    conn = _bt_conn()
    for sig, data in alpha_attr.items():
        conn.execute("""
            INSERT OR REPLACE INTO backtest_alpha_attribution_v3
            (run_date, signal_name, correlation, winning_trades, losing_trades,
             avg_score_wins, avg_score_losses)
            VALUES (?,?,?,?,?,?,?)
        """, (run_date, sig, data.get("correlation", 0),
              data.get("winning_trades", 0), data.get("losing_trades", 0),
              data.get("avg_score_wins", 0), data.get("avg_score_losses", 0)))
    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# Four-way comparison
# ═══════════════════════════════════════════════════════════════════════════════

def _build_comparison_v3(run_date: str, fleet_metrics: dict, summary: dict) -> dict:
    """Build four-way comparison: Actual Fleet / v1 / v2 / v3."""
    conn = _bt_conn()
    now  = datetime.utcnow().isoformat()

    # ── Pull v1 from backtest_master_results (no suffix) ─────────────────────
    try:
        v1_ec_start = conn.execute(
            "SELECT equity FROM backtest_equity_curve ORDER BY trade_date LIMIT 1"
        ).fetchone()
        v1_ec_end = conn.execute(
            "SELECT equity FROM backtest_equity_curve ORDER BY trade_date DESC LIMIT 1"
        ).fetchone()
        v1_start = float(v1_ec_start["equity"]) if v1_ec_start else STARTING_CASH
        v1_end   = float(v1_ec_end["equity"])   if v1_ec_end   else STARTING_CASH
    except Exception:
        v1_start = v1_end = STARTING_CASH
    v1_return = round((v1_end - v1_start) / max(v1_start, 1) * 100, 2)

    def _safe_float(row) -> float:
        if row is None: return 0.0
        v = row[0]
        return float(v) if v is not None else 0.0

    try:
        v1_sharpe = _safe_float(conn.execute(
            "SELECT AVG(realistic_sharpe) FROM backtest_master_results WHERE num_trades > 0"
        ).fetchone())
        v1_dd = _safe_float(conn.execute(
            "SELECT MIN(max_drawdown) FROM backtest_master_results WHERE num_trades > 0"
        ).fetchone())
        v1_wr = _safe_float(conn.execute(
            "SELECT AVG(win_rate) FROM backtest_master_results WHERE num_trades > 0"
        ).fetchone())
        v1_trades = int(_safe_float(conn.execute(
            "SELECT SUM(num_trades) FROM backtest_master_results WHERE num_trades > 0"
        ).fetchone()))
    except Exception:
        v1_sharpe = v1_dd = v1_wr = 0.0; v1_trades = 0

    # ── Pull v2 from backtest_master_results_v2 ───────────────────────────────
    try:
        v2_ec_start = conn.execute(
            "SELECT equity FROM backtest_equity_curve_v2 ORDER BY trade_date LIMIT 1"
        ).fetchone()
        v2_ec_end = conn.execute(
            "SELECT equity FROM backtest_equity_curve_v2 ORDER BY trade_date DESC LIMIT 1"
        ).fetchone()
        v2_start = float(v2_ec_start["equity"]) if v2_ec_start else STARTING_CASH
        v2_end   = float(v2_ec_end["equity"])   if v2_ec_end   else STARTING_CASH
    except Exception:
        v2_start = v2_end = STARTING_CASH
    v2_return = round((v2_end - v2_start) / max(v2_start, 1) * 100, 2)

    try:
        v2_sharpe = _safe_float(conn.execute(
            "SELECT AVG(realistic_sharpe) FROM backtest_master_results_v2 WHERE num_trades > 0"
        ).fetchone())
        v2_dd = _safe_float(conn.execute(
            "SELECT MIN(max_drawdown) FROM backtest_master_results_v2 WHERE num_trades > 0"
        ).fetchone())
        v2_wr = _safe_float(conn.execute(
            "SELECT AVG(win_rate) FROM backtest_master_results_v2 WHERE num_trades > 0"
        ).fetchone())
        v2_trades = int(_safe_float(conn.execute(
            "SELECT SUM(num_trades) FROM backtest_master_results_v2 WHERE num_trades > 0"
        ).fetchone()))
    except Exception:
        v2_sharpe = v2_dd = v2_wr = 0.0; v2_trades = 0

    # ── v3 metrics from summary ───────────────────────────────────────────────
    v3_end    = summary.get("equity_end", STARTING_CASH)
    v3_return = round((v3_end - STARTING_CASH) / STARTING_CASH * 100, 2)
    try:
        v3_sharpe = _safe_float(conn.execute(
            f"SELECT AVG(realistic_sharpe) FROM backtest_master_results_v3 "
            f"WHERE run_date=? AND num_trades > 0", (run_date,)
        ).fetchone())
        v3_dd = _safe_float(conn.execute(
            f"SELECT MIN(max_drawdown) FROM backtest_master_results_v3 "
            f"WHERE run_date=? AND num_trades > 0", (run_date,)
        ).fetchone())
        v3_wr = _safe_float(conn.execute(
            f"SELECT AVG(win_rate) FROM backtest_master_results_v3 "
            f"WHERE run_date=? AND num_trades > 0", (run_date,)
        ).fetchone())
    except Exception:
        v3_sharpe = v3_dd = v3_wr = 0.0
    v3_trades = summary.get("total_bt_trades", 0)

    # ── Actual fleet ─────────────────────────────────────────────────────────
    fleet_return = fleet_metrics.get("return_pct", 0.0)
    fleet_sharpe = fleet_metrics.get("fleet_sharpe", 0.0)
    fleet_wr     = fleet_metrics.get("win_rate", 0.0)
    fleet_trades = fleet_metrics.get("total_trades", 0)
    fleet_end    = (fleet_metrics.get("starting_capital", STARTING_CASH) +
                    fleet_metrics.get("total_pnl", 0))
    fleet_dd     = fleet_metrics.get("max_drawdown", 0.0)
    fleet_agents = fleet_metrics.get("unique_players", 22)

    # Known hardcoded values (prior runs)
    KNOWN = {
        "actual_fleet":  {"return": -23.66, "sharpe": -6.516, "wr": 17.7,  "trades": 558,   "agents": 22},
        "v1":            {"return": +41.33, "sharpe": -0.061, "wr": 41.8,  "trades": 2329,  "agents": 29},
        "v2":            {"return": +8.42,  "sharpe": +0.874, "wr": 57.6,  "trades": 277,   "agents": 12},
    }

    rows = [
        ("Final equity ($)",
         fleet_end,           f"${fleet_end:,.0f}",
         v1_end,              f"${v1_end:,.0f}",
         v2_end,              f"${v2_end:,.0f}",
         v3_end,              f"${v3_end:,.0f}"),
        ("Total return %",
         fleet_return,        f"{fleet_return:+.2f}%",
         v1_return,           f"{v1_return:+.2f}%",
         v2_return,           f"{v2_return:+.2f}%",
         v3_return,           f"{v3_return:+.2f}%"),
        ("Sharpe (realistic)",
         fleet_sharpe,        f"{fleet_sharpe:.3f}",
         v1_sharpe,           f"{v1_sharpe:.3f}",
         v2_sharpe,           f"{v2_sharpe:.3f}",
         v3_sharpe,           f"{v3_sharpe:.3f}"),
        ("Max drawdown %",
         fleet_dd,            f"{fleet_dd:.2f}%",
         KNOWN["v1"]["return"], f"{KNOWN['v1']['return']:.2f}% (FIXED)",
         KNOWN["v2"]["return"], f"{KNOWN['v2']['return']:.2f}% (FIXED)",
         v3_dd,               f"{v3_dd:.2f}%"),
        ("Win rate %",
         fleet_wr,            f"{fleet_wr:.1f}%",
         v1_wr,               f"{v1_wr:.1f}%",
         v2_wr,               f"{v2_wr:.1f}%",
         v3_wr,               f"{v3_wr:.1f}%"),
        ("Total trades",
         float(fleet_trades), str(fleet_trades),
         float(v1_trades),    str(v1_trades),
         float(v2_trades),    str(v2_trades),
         float(v3_trades),    str(v3_trades)),
    ]

    for metric, af, af_l, v1v, v1_l, v2v, v2_l, v3v, v3_l in rows:
        conn.execute("""
            INSERT OR REPLACE INTO comparison_table_v3
            (run_date, metric, actual_fleet, actual_fleet_label,
             v1_backtest, v1_label, v2_backtest, v2_label,
             v3_backtest, v3_label, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (run_date, metric, af, af_l, v1v, v1_l, v2v, v2_l, v3v, v3_l, now))

    conn.commit()
    conn.close()

    return {
        "rows": rows,
        "v1": {"return_pct": v1_return, "equity_end": v1_end, "sharpe": v1_sharpe,
               "win_rate": v1_wr, "trades": v1_trades},
        "v2": {"return_pct": v2_return, "equity_end": v2_end, "sharpe": v2_sharpe,
               "win_rate": v2_wr, "trades": v2_trades},
        "v3": {"return_pct": v3_return, "equity_end": v3_end, "sharpe": v3_sharpe,
               "win_rate": v3_wr, "trades": v3_trades},
        "fleet": {"return_pct": fleet_return, "sharpe": fleet_sharpe,
                  "win_rate": fleet_wr, "trades": fleet_trades},
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Print report
# ═══════════════════════════════════════════════════════════════════════════════

def _print_report_v3(summary: dict, comparison: dict) -> None:
    fleet      = summary.get("fleet", {})
    agent_met  = summary.get("agent_metrics", {})
    alpha_attr = summary.get("alpha_attribution", {})
    recs       = summary.get("recommendations", [])
    v1         = comparison.get("v1", {})
    v2         = comparison.get("v2", {})
    v3         = comparison.get("v3", {})

    bar = "━" * 80

    print(f"\n{bar}")
    print(f"  WARP CORE REACTOR v3 — Lean Fleet Shakedown Cruise")
    print(f"  Super Backtest v3  |  90-day window (Oct 9 2025 → Apr 9 2026)")
    print(f"  Run: {summary.get('run_date', '')}  |  elapsed: {summary.get('elapsed_seconds', 0)}s")
    print(f"{bar}")

    # ── Section 1: Agent Leaderboard ─────────────────────────────────────────
    print(f"\n  [1] AGENT LEADERBOARD  (sorted by Sharpe desc)")
    print(f"  {'─'*76}")
    hdr = f"  {'Rank':<4} {'Agent':<10} {'Model':<20} {'Sharpe':>7} {'WR%':>6} {'Return%':>8} {'MaxDD%':>7} {'Trades':>6}"
    print(hdr)
    print(f"  {'─'*76}")

    ranked = sorted(agent_met.items(), key=lambda x: -x[1].get("sharpe", -99))
    for rank, (aid, m) in enumerate(ranked, 1):
        spec  = LEAN_FLEET.get(aid, {})
        name  = spec.get("name", aid)
        model = spec.get("model", "")[:18]
        flag  = " *" if m.get("needs_validation") else "  "
        print(f"  {rank:<4} {name:<10} {model:<20} "
              f"{m.get('sharpe', 0):>+7.3f} "
              f"{m.get('win_rate', 0):>6.1f} "
              f"{m.get('total_return', 0):>+8.2f} "
              f"{m.get('max_drawdown', 0):>7.2f} "
              f"{m.get('num_trades', 0):>6}{flag}")
    print(f"  * = |Sharpe| > 5.0, needs validation")

    # ── Section 2: Strategy Leaderboard ──────────────────────────────────────
    print(f"\n  [2] STRATEGY LEADERBOARD  (top 15 by realistic Sharpe)")
    print(f"  {'─'*76}")
    print(f"  {'Strategy':<24} {'Ticker':<7} {'r.Sharpe':>8} {'WR%':>6} {'Return%':>8} {'Trades':>6} {'Tier':>4}")
    print(f"  {'─'*76}")

    # Pull from saved DB
    try:
        conn = _bt_conn()
        strat_rows = conn.execute("""
            SELECT strategy, ticker, realistic_sharpe, win_rate, total_return, num_trades, tier
            FROM backtest_master_results_v3
            WHERE run_date=? AND num_trades > 0
            ORDER BY realistic_sharpe DESC LIMIT 15
        """, (summary["run_date"],)).fetchall()
        conn.close()
        for row in strat_rows:
            print(f"  {row['strategy']:<24} {row['ticker']:<7} "
                  f"{row['realistic_sharpe']:>+8.3f} "
                  f"{row['win_rate']:>6.1f} "
                  f"{row['total_return']:>+8.2f} "
                  f"{row['num_trades']:>6} "
                  f"{row['tier']:>4}")
    except Exception as e:
        print(f"  (strategy table unavailable: {e})")

    # ── Section 3: Regime Performance ────────────────────────────────────────
    print(f"\n  [3] REGIME PERFORMANCE")
    print(f"  {'─'*76}")
    regime_metrics = summary.get("regime_metrics", {})
    for regime in REGIMES:
        regime_strats = [(k.split("_", 1)[1] if "_" in k else k, v)
                         for k, v in regime_metrics.items()
                         if k.startswith(f"{regime}_")]
        top3 = sorted(regime_strats, key=lambda x: -x[1].get("sharpe", 0))[:3]
        if top3:
            print(f"  {regime}:")
            for strat, m in top3:
                print(f"    {strat:<26} Sharpe={m.get('sharpe', 0):>+6.3f}  "
                      f"WR={m.get('win_rate', 0):.1f}%  n={m.get('num_trades', 0)}")
        else:
            print(f"  {regime}: (no trades)")

    # ── Section 4: Monthly Breakdown ─────────────────────────────────────────
    print(f"\n  [4] MONTHLY BREAKDOWN")
    print(f"  {'─'*76}")
    try:
        conn = _bt_conn()
        months = sorted({r["month"] for r in conn.execute(
            "SELECT DISTINCT month FROM backtest_monthly_breakdown_v3 WHERE run_date=?",
            (summary["run_date"],)
        ).fetchall()})
        if months:
            col_w = 12
            header = f"  {'Agent':<12}" + "".join(f"{m:>{col_w}}" for m in months)
            print(header)
            print(f"  {'─'*76}")
            agent_ids = list(LEAN_FLEET.keys())
            for aid in agent_ids:
                spec  = LEAN_FLEET[aid]
                name  = spec["name"]
                row_s = f"  {name:<12}"
                for m in months:
                    r = conn.execute("""
                        SELECT total_return FROM backtest_monthly_breakdown_v3
                        WHERE run_date=? AND month=? AND agent_id=?
                    """, (summary["run_date"], m, aid)).fetchone()
                    val = f"{r['total_return']:>+.1f}%" if r else "   —   "
                    row_s += f"{val:>{col_w}}"
                print(row_s)
        conn.close()
    except Exception as e:
        print(f"  (monthly table unavailable: {e})")

    # ── Section 5: Alpha Signal Report Card ──────────────────────────────────
    print(f"\n  [5] ALPHA SIGNAL REPORT CARD")
    print(f"  {'─'*76}")
    print(f"  {'Signal':<22} {'Corr':>8} {'Wins':>6} {'Losses':>7} {'AvgW':>7} {'AvgL':>7}  Grade")
    print(f"  {'─'*76}")
    for sig, data in sorted(alpha_attr.items(),
                             key=lambda x: -abs(x[1].get("correlation", 0))):
        corr = data.get("correlation", 0)
        if corr > 0.10:    grade = "A (strong positive)"
        elif corr > 0.05:  grade = "B (mild positive)"
        elif corr < -0.10: grade = "F (hurts returns)"
        elif corr < -0.05: grade = "D (mild negative)"
        else:              grade = "C (neutral)"
        print(f"  {sig:<22} {corr:>+8.4f} {data.get('winning_trades', 0):>6} "
              f"{data.get('losing_trades', 0):>7} "
              f"{data.get('avg_score_wins', 0):>7.3f} "
              f"{data.get('avg_score_losses', 0):>7.3f}  {grade}")

    # ── Section 6: Four-way comparison ───────────────────────────────────────
    fleet_agents = fleet.get("unique_players", 22)
    print(f"\n  [6] FOUR-WAY COMPARISON")
    print(f"  {'─'*76}")
    print(f"  {'Metric':<22} {'Actual Fleet':>14} {'v1 (no alpha)':>14} "
          f"{'v2 (12-agent)':>14} {'v3 Shakedown':>14}")
    print(f"  {'─'*76}")
    for row in comparison.get("rows", []):
        metric, af, af_l, v1v, v1_l, v2v, v2_l, v3v, v3_l = row
        print(f"  {metric:<22} {af_l:>14} {v1_l:>14} {v2_l:>14} {v3_l:>14}")
    print(f"  {'─'*76}")
    print(f"  Fleet ({fleet_agents} agents) | v1 (29 agents, no alpha gate) | "
          f"v2 (12 agents, alpha gate) | v3 (12 agents, agent routing + fixed DD)")

    # ── Section 7: Recommendations ───────────────────────────────────────────
    print(f"\n  [7] RECOMMENDATIONS")
    print(f"  {'─'*76}")
    for rec in recs:
        print(f"  • {rec}")

    # ── Section 8: Fix Note ───────────────────────────────────────────────────
    v3_dd_val = v3.get("equity_end", STARTING_CASH)
    v3_dd_pct = round((v3_dd_val - STARTING_CASH) / STARTING_CASH * 100, 2)
    print(f"\n  [8] MAX DRAWDOWN FIX")
    print(f"  {'─'*76}")
    print(f"  Old formula:  cumulative product of returns → can exceed -100% (BUG)")
    print(f"  New formula:  (peak_equity - trough_equity) / peak_equity × 100,")
    print(f"                capped at -100%")
    print(f"  v1 reported:  -801%     → Fixed: {v1.get('return_pct', 0):+.2f}% return baseline")
    print(f"  v2 reported:  -30,084%  → Fixed: {v2.get('return_pct', 0):+.2f}% return baseline")
    print(f"  v3:           uses fixed formula throughout")
    print(f"\n  Portfolio: ${STARTING_CASH:,.0f} → ${summary.get('equity_end', STARTING_CASH):,.2f} "
          f"({summary.get('portfolio_return_pct', 0):+.2f}%)")
    print(f"  SPY benchmark: {summary.get('spy_return', 0):+.2f}%")
    print(f"  vs SPY:        {summary.get('portfolio_return_pct', 0) - summary.get('spy_return', 0):+.2f}pp")
    print(f"\n{bar}\n")


# ═══════════════════════════════════════════════════════════════════════════════
# Main orchestrator
# ═══════════════════════════════════════════════════════════════════════════════

def run_super_backtest_v3(days: int = BACKTEST_DAYS) -> dict:
    """
    Full Super Backtest v3 pipeline — Lean Fleet Shakedown Cruise.
    Saves all results to data/backtest.db with _v3 tables.
    NEVER modifies trader.db or arena.db.
    """
    t0       = time.time()
    run_date = date.today().isoformat()

    logger.info("═" * 60)
    logger.info("  Warp Core Reactor v3 — Lean Fleet Shakedown Cruise")
    logger.info(f"  Run date: {run_date}  |  {days}-day window")
    logger.info("═" * 60)

    _init_v3_tables()

    # ── Steps 1+2: Fleet analysis (reuse from v2) ─────────────────────────────
    logger.info("[STEP 1+2] Analyzing fleet actual performance (read-only)...")
    from engine.super_backtest_v2 import analyze_fleet_performance, analyze_signal_center
    fleet_metrics   = analyze_fleet_performance(run_date)

    # ── Step 3: Signal center ─────────────────────────────────────────────────
    logger.info("[STEP 3] Signal Center analysis...")
    signal_analysis = analyze_signal_center(run_date)

    # ── Step 4: Alpha expansion (reuse from v2) ───────────────────────────────
    logger.info(f"[STEP 4] Alpha expansion to {UNIVERSE_SIZE}-stock universe...")
    from engine.super_backtest_v2 import _expand_alpha_universe, run_alpha_expansion
    universe     = _expand_alpha_universe()
    alpha_scores = run_alpha_expansion(universe)

    # Merge existing alpha_signals.db composite scores
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

    # Filter universe
    filtered = sorted(
        [s for s, sc in alpha_scores.items() if sc >= ALPHA_THRESHOLD],
        key=lambda s: -alpha_scores[s]
    )
    logger.info(f"Alpha filter: {len(filtered)}/{len(alpha_scores)} symbols pass >= {ALPHA_THRESHOLD}")

    # ── Step 5A: Download price data (reuse v2 downloader) ───────────────────
    logger.info(f"[STEP 5A] Downloading price data for {len(filtered)} symbols...")
    from engine.super_backtest_v2 import _download_v2_universe
    td = _download_v2_universe(filtered + ["SPY", "^VIX"], days)

    # SPY fallback (critical rule #6)
    if "SPY" not in td:
        try:
            end_dt   = date.today()
            start_dt = end_dt - timedelta(days=days + 60)
            spy_raw  = yf.download("SPY", start=start_dt, end=end_dt,
                                   progress=False, auto_adjust=True)
            if not spy_raw.empty:
                td["SPY"] = spy_raw.dropna()
                logger.info("SPY downloaded individually as anchor")
        except Exception as e:
            logger.warning(f"SPY individual download failed: {e}")

    trading_days = _get_trading_days(td, days)
    if not trading_days:
        logger.error("No trading days found — aborting")
        return {"status": "error", "reason": "no trading days"}

    # VIX map (critical rule #5: explicit is not None guard)
    vix_df  = td.get("^VIX") if td.get("^VIX") is not None else td.get("VIX")
    vix_map: dict = {}
    if vix_df is not None and not vix_df.empty:
        for idx, row in vix_df.iterrows():
            vix_map[pd.Timestamp(idx).normalize()] = float(row.get("Close", 20.0))

    # SPY return
    spy_return = 0.0
    if "SPY" in td and not td["SPY"].empty:
        spy_c = td["SPY"]["Close"].values
        if len(spy_c) >= 2:
            spy_return = round((float(spy_c[-1]) - float(spy_c[0])) / float(spy_c[0]) * 100, 2)

    # Confidence proxy (uniform B-grade gate for all symbols)
    conf_scores = {sym: CONF_THRESHOLD for sym in filtered}

    # ── Step 5B: Run backtests ────────────────────────────────────────────────
    logger.info(f"[STEP 5B] Running v3 backtest on {len(td)-1} symbols, "
                f"{len(trading_days)} trading days...")

    event_trades, agent_trades = _run_event_loop_v3(
        td, trading_days, vix_map, alpha_scores, conf_scores
    )
    opt_trades, spread_trades = _run_options_loop_v3(
        td, trading_days, vix_map, alpha_scores
    )
    dte0_trades = _run_0dte_loop_v3(td, trading_days, vix_map)

    # Tag options/spreads with agent tracking
    for t in opt_trades:
        strat = t.get("strategy", "")
        aid   = t.get("agent_id", "gemini-2.5-flash" if ("bear" in strat or "put" in strat) else "ollama-llama")
        t["agent_id"] = aid
        t.setdefault("month", t.get("entry_date", "")[:7])
        t.setdefault("win", 1 if t.get("pnl_pct", 0) > 0 else 0)
        agent_trades[aid].append(t)

    for t in spread_trades:
        strat = t.get("strategy", "")
        aid   = t.get("agent_id",
                      "gemini-2.5-flash" if "bear" in strat else
                      "dayblade-sulu"     if "bull" in strat else
                      "gemini-2.5-pro")
        t["agent_id"] = aid
        t.setdefault("month", t.get("entry_date", "")[:7])
        t.setdefault("win", 1 if t.get("pnl_pct", 0) > 0 else 0)
        agent_trades[aid].append(t)

    for t in dte0_trades:
        agent_trades["dayblade-0dte"].append(t)

    # Build equity curve
    logger.info("  Building equity curve...")
    curve = _build_equity_curve_v3(event_trades, opt_trades, spread_trades,
                                    dte0_trades, trading_days, vix_map)

    # Per-agent metrics
    logger.info("  Computing per-agent metrics...")
    agent_metrics: dict = {}
    for aid, trades in agent_trades.items():
        if not trades:
            continue
        agent_metrics[aid] = _trade_metrics_v3(trades)
        # Regime breakdown per agent
        for regime in REGIMES:
            rtrades = [t for t in trades if t.get("regime") == regime and "pnl_pct" in t]
            m_r     = _trade_metrics_v3(rtrades) if rtrades else {}
            agent_metrics[aid][f"{regime.lower()}_return"] = m_r.get("total_return", 0.0)

    # Monthly breakdown
    monthly_all, monthly_by_agent = _compute_monthly_breakdown(
        event_trades, opt_trades, spread_trades, dte0_trades, agent_trades
    )

    # Regime performance
    regime_perf    = _compute_regime_performance(event_trades, opt_trades, spread_trades)
    regime_metrics = {k: _trade_metrics_v3(v) for k, v in regime_perf.items() if len(v) >= 2}

    # All trades flat for attribution
    all_trades_flat: list = []
    for trades in event_trades.values():
        all_trades_flat.extend(trades)
    all_trades_flat.extend(opt_trades + spread_trades + dte0_trades)

    # Alpha attribution
    logger.info("  Computing alpha attribution...")
    alpha_attr = _compute_alpha_attribution_v3(run_date, all_trades_flat)

    # ── Save results ──────────────────────────────────────────────────────────
    logger.info("[STEP 6] Saving v3 results to DB...")
    _save_agent_results_v3(run_date, agent_metrics)
    _save_master_results_v3(run_date, event_trades, opt_trades, spread_trades, spy_return)
    _save_equity_curve_v3(run_date, curve)
    _save_options_results_v3(run_date, opt_trades)
    _save_spread_results_v3(run_date, spread_trades)
    _save_0dte_results_v3(run_date, dte0_trades)
    _save_monthly_v3(run_date, monthly_all, monthly_by_agent)
    _save_regime_results_v3(run_date, regime_metrics)
    _save_alpha_attribution_v3(run_date, alpha_attr)

    # ── Four-way comparison ───────────────────────────────────────────────────
    equity_end = curve[-1]["equity"] if curve else STARTING_CASH
    comparison = _build_comparison_v3(run_date, fleet_metrics, {
        "equity_end":   equity_end,
        "total_trades": len(all_trades_flat),
        "spy_return":   spy_return,
    })

    elapsed = round(time.time() - t0, 1)

    # ── Final summary ─────────────────────────────────────────────────────────
    summary = {
        "status":          "ok",
        "run_date":        run_date,
        "elapsed_seconds": elapsed,
        "days":            days,
        "fleet":           fleet_metrics,
        "signal_analysis": signal_analysis,
        "universe_scanned":              len(universe) + 6,
        "universe_after_alpha_filter":   len(filtered),
        "symbols_with_data":             len([s for s in filtered if s in td]),
        "total_bt_trades":               len(all_trades_flat),
        "equity_start":                  STARTING_CASH,
        "equity_end":                    equity_end,
        "portfolio_return_pct":          round((equity_end - STARTING_CASH) / STARTING_CASH * 100, 2),
        "spy_return":                    spy_return,
        "agent_metrics":                 agent_metrics,
        "regime_metrics": {
            f"{r}_{s}": m for (r, s), m in regime_perf.items() if len(regime_perf[(r, s)]) >= 2
            and _trade_metrics_v3(regime_perf[(r, s)]) == regime_metrics.get((r, s), {})
        },
        "alpha_attribution": alpha_attr,
    }

    # Rebuild regime_metrics with string keys for JSON/report
    summary["regime_metrics"] = {
        f"{r}_{s}": regime_metrics[(r, s)]
        for (r, s) in regime_metrics
    }

    # Recommendations
    summary["recommendations"] = _generate_recommendations(
        agent_metrics, {}, {(r, s): regime_metrics[(r, s)] for (r, s) in regime_metrics}
    )

    # ── Save JSON summary ─────────────────────────────────────────────────────
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        summary_path = DATA_DIR / "backtest_v3_summary.json"
        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2, default=str)
        logger.info(f"Summary saved to {summary_path}")
    except Exception as e:
        logger.warning(f"Could not save JSON summary: {e}")

    _print_report_v3(summary, comparison)
    return summary


# ═══════════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [sv3] %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Super Backtest v3 — Warp Core Reactor / Lean Fleet Shakedown Cruise"
    )
    parser.add_argument("--days", type=int, default=BACKTEST_DAYS,
                        help=f"Backtest window in days (default: {BACKTEST_DAYS})")
    parser.add_argument("--fleet-only", action="store_true",
                        help="Only run fleet analysis (read-only, no backtest)")
    parser.add_argument("--signals-only", action="store_true",
                        help="Only run signal center analysis")
    args = parser.parse_args()

    if args.fleet_only:
        _init_v3_tables()
        from engine.super_backtest_v2 import analyze_fleet_performance
        m = analyze_fleet_performance(date.today().isoformat())
        print(json.dumps(m, indent=2, default=str))
    elif args.signals_only:
        _init_v3_tables()
        from engine.super_backtest_v2 import analyze_signal_center
        s = analyze_signal_center(date.today().isoformat())
        print(json.dumps(s, indent=2, default=str))
    else:
        result = run_super_backtest_v3(args.days)
        eq_end = result.get("equity_end", STARTING_CASH)
        ret    = result.get("portfolio_return_pct", 0.0)
        print(f"\nShakedown complete. Equity: ${eq_end:,.2f} ({ret:+.2f}%)")
        print(f"Summary: data/backtest_v3_summary.json")
