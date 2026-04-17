"""
engine/super_backtest_oos.py — OOS Validation: Held-Out 2024 Backtest

Priority-1 out-of-sample validation of the Season 6.3 engine.
Tests whether the in-sample Sharpe 4.845 / 100% WR is real or artifact.

KEY DIFFERENCES FROM super_backtest_v5.py:
  • Window: 2024-01-01 → 2024-12-31 (full calendar year, never tuned)
  • Universe: Point-in-time S&P 500 as of Jan 2024 (survivorship bias removed)
    - Reconstructed from Wikipedia changes table (45 post-2024 changes reversed)
    - Validated via yfinance; top 200 by 2024 avg daily volume
    - 14 acquired/delisted tickers (WBA, PXD, K, HES, etc.) unavailable in
      yfinance post-delisting — documented limitation, unavoidable with free data
  • Alpha scores: neutral 0.5 for all PIT symbols (alpha_signals.db is Apr 2026
    data; using it for 2024 window would be forward contamination)
  • Gate thresholds: REVERTED to CLAUDE.md spec (not in-sample-fitted values)
    - sniper alpha: 0.25 (was 0.3)
    - neo-matrix: 1.75 (was missing from AGENT_THRESHOLDS, defaulted to 2.7)
    - chekov: OLLIE_THRESHOLD 2.7 (was hardcoded 2.3, in-sample fitted)
    - ollama-qwen3: 2.7 (was 2.4, fitted from shadow analysis)
    - capitol-trades: 2.7 (was 2.4, fitted from shadow analysis)
  • Results saved to _oos tables only — v5 tables untouched (sacred-data rule)

Run:
    venv/bin/python3 -m engine.super_backtest_oos 2>&1 | tee data/oos_run.log
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
from engine.super_backtest_v2 import (
    _expand_alpha_universe,
    run_alpha_expansion,
    _download_v2_universe,
)
from engine.super_backtest_v4 import (
    _trade_metrics_v4,
    _max_drawdown_equity,
    _alpha_position_factor,
    _sniper_pos_factor,
    _download_spy_with_retry,
    _build_equity_curve_v4,
    _compute_alpha_attribution_v4,
    KNOWN,
)

logger = logging.getLogger(__name__)

_ROOT       = Path(__file__).resolve().parent.parent
BACKTEST_DB = _ROOT / "data" / "backtest.db"
TRADER_DB   = _ROOT / "data" / "trader.db"
ALPHA_DB    = _ROOT / "data" / "alpha_signals.db"
DATA_DIR    = _ROOT / "data"

WINDOW_START  = date(2024, 1, 1)    # OOS: full calendar year 2024
WINDOW_END    = date(2024, 12, 31)
BACKTEST_DAYS = 700                 # extra warmup buffer for SMA200 on 2024 data

# ── Sniper v5 config ──────────────────────────────────────────────────────────
SNIPER_ALPHA_THRESHOLD = 0.25           # OOS: CLAUDE.md spec (was 0.3, in-sample fitted)
SNIPER_CONF_THRESHOLD  = 0.70          # raised from 0.65 per Season 6
SNIPER_BULL_MIN        = 2

# Ollie Commander thresholds
OLLIE_THRESHOLD  = 2.7
OLLIE_W_GRADE    = 0.25
OLLIE_W_ALPHA    = 0.35
OLLIE_W_AGENT_WR = 0.20
OLLIE_W_REGIME   = 0.20

# Per-agent threshold overrides — OOS: REVERTED to CLAUDE.md spec
# Removed in-sample-fitted overrides on qwen3 (2.4) and capitol-trades (2.4).
# neo-matrix: 1.75 per CLAUDE.md (was missing → defaulted to OLLIE_THRESHOLD 2.7).
# chekov handled separately in _ollie_score (removed hardcoded 2.3, uses OLLIE_THRESHOLD).
AGENT_THRESHOLDS: dict[str, float] = {
    "neo-matrix":     1.75,  # OOS: CLAUDE.md spec (was missing, now explicit)
    "ollama-qwen3":   2.7,   # OOS: reverted to OLLIE_THRESHOLD (was 2.4, shadow-fitted)
    "ollama-llama":   2.5,   # Uhura: unchanged (not flagged as overfit)
    "capitol-trades": 2.7,   # OOS: reverted to OLLIE_THRESHOLD (was 2.4, shadow-fitted)
    "navigator":      2.5,   # navigator: unchanged (not flagged as overfit)
}

# Regime alignment bonus for each strategy
REGIME_ALIGN = {
    "covered_call":  {"BULL": 1.2, "CAUTIOUS": 2.0, "BEAR": 2.0, "CRISIS": 0.0},
    "csp":           {"BULL": 2.0, "CAUTIOUS": 1.5, "BEAR": 0.5, "CRISIS": 0.0},
    "rsi_bounce":    {"BULL": 2.0, "CAUTIOUS": 1.0, "BEAR": 0.5, "CRISIS": 0.0},
    "bull_momentum": {"BULL": 2.0, "CAUTIOUS": 1.5, "BEAR": 0.0, "CRISIS": 0.0},
}

# Agent assignment: strategy → agent_id
STRAT_AGENT_V5 = {
    "rsi_bounce":    "navigator",       # Navigator
    "csp":           "ollama-plutus",    # McCoy (high-VIX) or Uhura
    "covered_call":  "capitol-trades",    # Capitol (CAUTIOUS/BEAR) or Uhura (BULL)
}

SNIPER_FLEET_V5 = {
    "ollie-auto":     {"name": "Ollie",     "model": "commander",        "role": "gate"},
    "ollama-llama":   {"name": "Uhura",     "model": "llama3.1:latest",  "tiers": [5, 6]},
    "navigator":      {"name": "Navigator", "model": "qwen2.5:7b",       "tiers": [5]},
    "chekov":         {"name": "Chekov",    "model": "qwen2.5:7b",       "tiers": [5]},
    "ollama-plutus":  {"name": "McCoy",     "model": "0xroyce/plutus",   "tiers": [5]},
    "ollama-qwen3":   {"name": "Dax",       "model": "qwen3:8b",         "tiers": [1, 5]},
    "ollama-coder":   {"name": "Data",      "model": "qwen2.5-coder:7b", "tiers": [3]},
    "neo-matrix":     {"name": "Neo",       "model": "0xroyce/plutus",   "tiers": [3]},
    "capitol-trades": {"name": "Capitol",   "model": "congress",         "tiers": [5]},
}

V5_STRATEGIES = ("rsi_bounce", "csp", "covered_call", "bull_momentum")


# ═══════════════════════════════════════════════════════════════════════════════
# DB helpers
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


def _init_oos_tables() -> None:
    """Create OOS result tables with _oos suffix. Never modifies v5 tables."""
    conn = _bt_conn()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS backtest_results_oos (
        run_date TEXT, agent_id TEXT, agent_name TEXT, model TEXT,
        total_return REAL, win_rate REAL, sharpe REAL, max_drawdown REAL,
        profit_factor REAL, num_trades INTEGER, avg_hold_days REAL,
        best_trade_pct REAL, worst_trade_pct REAL,
        bull_return REAL, cautious_return REAL, bear_return REAL,
        spy_return REAL, vs_spy REAL, created_at TEXT,
        expectancy REAL, recovery_factor REAL,
        PRIMARY KEY (run_date, agent_id)
    );

    CREATE TABLE IF NOT EXISTS backtest_trades_oos (
        run_date TEXT, strategy TEXT, ticker TEXT,
        total_return REAL, win_rate REAL, sharpe REAL, realistic_sharpe REAL,
        max_drawdown REAL, num_trades INTEGER, profit_factor REAL,
        spy_return REAL, vs_spy REAL, regime TEXT,
        alpha_score REAL, created_at TEXT,
        PRIMARY KEY (run_date, strategy, ticker)
    );

    CREATE TABLE IF NOT EXISTS backtest_equity_oos (
        run_date TEXT, trade_date TEXT, equity REAL,
        daily_pnl REAL, regime TEXT,
        PRIMARY KEY (run_date, trade_date)
    );

    CREATE TABLE IF NOT EXISTS backtest_monthly_oos (
        run_date TEXT, month TEXT, agent_id TEXT,
        total_return REAL, win_rate REAL, num_trades INTEGER,
        PRIMARY KEY (run_date, month, agent_id)
    );

    CREATE TABLE IF NOT EXISTS backtest_decisions_oos (
        run_date TEXT, symbol TEXT, strategy TEXT, agent_id TEXT,
        decision TEXT, ollie_score REAL,
        grade_pts REAL, alpha_pts REAL, agent_wr_pts REAL, regime_pts REAL,
        trade_alpha REAL, regime TEXT,
        shadow_pnl_pct REAL,
        created_at TEXT
    );

    CREATE TABLE IF NOT EXISTS backtest_summary_oos (
        run_date TEXT PRIMARY KEY,
        total_return REAL, win_rate REAL, sharpe REAL, max_drawdown REAL,
        num_trades INTEGER, spy_return REAL, vs_spy REAL,
        ollie_submitted INTEGER, ollie_approved INTEGER, ollie_rejected INTEGER,
        approved_wr REAL, rejected_shadow_wr REAL,
        ollie_value_added INTEGER,
        alpha_gate_impact_return REAL, alpha_gate_impact_sharpe REAL,
        created_at TEXT
    );
    """)
    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# Ollie Commander scoring
# ═══════════════════════════════════════════════════════════════════════════════

def _ollie_grade_pts(composite_score: float) -> float:
    if composite_score >= 1.5: return 5.0
    if composite_score >= 1.0: return 4.0
    if composite_score >= 0.5: return 3.0
    if composite_score >= 0.0: return 2.0
    return 1.0


def _ollie_alpha_pts(composite_score: float) -> float:
    clamped = max(-2.0, min(2.0, composite_score))
    return round((clamped + 2.0) / 4.0 * 3.0, 3)


def _ollie_regime_pts(strategy: str, regime: str) -> float:
    table = REGIME_ALIGN.get(strategy, {})
    for k, v in table.items():
        if k in regime:
            return v
    return 1.0


def _ollie_score(
    alpha: float,
    regime: str,
    strategy: str,
    agent_wr: float,          # 0.0–1.0 rolling win rate
    agent_id: str = "",       # optional: enables per-agent threshold override
) -> tuple[float, bool]:
    """Return (ollie_score, approved)."""
    grade_pts    = _ollie_grade_pts(alpha)
    alpha_pts    = _ollie_alpha_pts(alpha)
    agent_wr_pts = 2.0 * agent_wr   # 0–2 scale directly

    regime_pts   = _ollie_regime_pts(strategy, regime)

    norm_grade   = grade_pts                   # already 0-5
    norm_alpha   = alpha_pts  * (5.0 / 3.0)   # 0-3 → 0-5
    norm_wr      = agent_wr_pts * (5.0 / 2.0) # 0-2 → 0-5
    norm_regime  = regime_pts   * (5.0 / 2.0) # 0-2 → 0-5

    score = round(
        OLLIE_W_GRADE    * norm_grade  +
        OLLIE_W_ALPHA    * norm_alpha  +
        OLLIE_W_AGENT_WR * norm_wr     +
        OLLIE_W_REGIME   * norm_regime,
        3,
    )
    # OOS: chekov hardcoded 2.3 was in-sample fitted. CLAUDE.md says "chekov ≥5"
    # which refers to the live agent's own 0-10 signal score, not the Ollie 0-5 composite.
    # For backtest simulation, chekov uses global OLLIE_THRESHOLD (2.7) — no special case.
    threshold = AGENT_THRESHOLDS.get(agent_id, OLLIE_THRESHOLD)
    return score, score >= threshold


# ═══════════════════════════════════════════════════════════════════════════════
# Alpha signals (FIXED: composite_score + created_at)
# ═══════════════════════════════════════════════════════════════════════════════

def _load_alpha_scores_v5() -> dict[str, float]:
    """
    Load composite_score from alpha_signals.db using CORRECT column names.
    Bug fix: v4 live gate used composite_alpha_score + timestamp (wrong).
    v5 uses composite_score + created_at (correct).
    """
    scores: dict[str, float] = {}
    try:
        ac = _alpha_conn()
        # Get most recent score per symbol (ORDER BY created_at DESC)
        rows = ac.execute("""
            SELECT symbol, composite_score
            FROM composite_alpha
            WHERE composite_score IS NOT NULL
            ORDER BY created_at DESC
        """).fetchall()
        ac.close()
        for r in rows:
            sym = r["symbol"]
            if sym not in scores:          # first row = most recent
                scores[sym] = float(r["composite_score"])
        logger.info(f"[V5_ALPHA] Loaded {len(scores)} alpha scores (fixed columns)")
    except Exception as e:
        logger.warning(f"[V5_ALPHA] Alpha DB load error: {e}")
    return scores


# ═══════════════════════════════════════════════════════════════════════════════
# OOS: Point-in-time universe as of Jan 1, 2024
# ═══════════════════════════════════════════════════════════════════════════════

_PIT_UNIVERSE_CACHE: list[str] = []   # module-level cache to avoid re-fetching

def _build_pit_universe_2024() -> list[str]:
    """
    Reconstruct S&P 500 constituents as of 2024-01-01 using Wikipedia changes table.

    Method:
      1. Fetch current S&P 500 from Wikipedia (503 tickers)
      2. Parse changes table; reverse all changes dated >= 2024-01-01
         (remove tickers ADDED after Jan 2024, re-add tickers REMOVED after Jan 2024)
      3. Validate via yfinance: confirm 2024 price data exists, price >= $3
      4. Rank by 2024 avg daily volume → return top 200

    Limitations:
      - 14 acquired/delisted tickers (WBA, PXD, K, HES, etc.) are unavailable
        in yfinance post-delisting and cannot be included despite being active in 2024.
        This is unavoidable with free data; documented in oos_audit_report.txt.
      - Does not include sub-S&P-500 names that the live scan_universe captures.
        OOS universe is therefore more conservative (large-cap only).
    """
    global _PIT_UNIVERSE_CACHE
    if _PIT_UNIVERSE_CACHE:
        logger.info(f"[PIT_UNIVERSE] Using cached {len(_PIT_UNIVERSE_CACHE)} symbols")
        return _PIT_UNIVERSE_CACHE

    # Check for on-disk cache first (avoids re-fetching on re-runs)
    cache_path = DATA_DIR / "pit_universe_2024.json"
    if cache_path.exists():
        import json as _json
        tickers = _json.loads(cache_path.read_text())
        logger.info(f"[PIT_UNIVERSE] Loaded {len(tickers)} symbols from disk cache {cache_path}")
        _PIT_UNIVERSE_CACHE = tickers
        return tickers

    logger.info("[PIT_UNIVERSE] Building point-in-time S&P 500 as of 2024-01-01...")
    import json as _json, urllib.request as _urllib

    # Step 1: Fetch Wikipedia
    tickers_jan2024: set[str] = set()
    try:
        req = _urllib.Request(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            headers={"User-Agent": "Mozilla/5.0 (compatible; research/OOS-validation)"},
        )
        with _urllib.urlopen(req, timeout=30) as resp:
            html = resp.read()
        tables = pd.read_html(html)
        sp500_now = set(tables[0]["Symbol"].str.strip().tolist())

        # Step 2: Reverse post-2024 changes
        changes = tables[1].copy()
        changes.columns = ["date", "add_ticker", "add_name", "rem_ticker", "rem_name", "reason"]
        changes = changes.iloc[1:]   # drop repeated header row
        changes["date_parsed"] = pd.to_datetime(changes["date"], errors="coerce")
        cutoff = pd.Timestamp("2024-01-01")
        post_2024 = changes[changes["date_parsed"] >= cutoff]

        tickers_jan2024 = sp500_now.copy()
        for _, row in post_2024.iterrows():
            add_t = str(row["add_ticker"]).strip()
            rem_t = str(row["rem_ticker"]).strip()
            if add_t and add_t != "nan":
                tickers_jan2024.discard(add_t)   # was added after 2024 → not in Jan 2024
            if rem_t and rem_t != "nan":
                tickers_jan2024.add(rem_t)        # was removed after 2024 → was in Jan 2024
        logger.info(f"[PIT_UNIVERSE] Reconstructed {len(tickers_jan2024)} Jan-2024 S&P 500 tickers")
    except Exception as e:
        logger.warning(f"[PIT_UNIVERSE] Wikipedia fetch failed: {e}. Using fallback universe.")
        tickers_jan2024 = set()

    # Step 3: Validate via yfinance — confirm 2024 data, rank by volume
    if tickers_jan2024:
        try:
            logger.info("[PIT_UNIVERSE] Downloading 2024 volume data for validation...")
            _sym_list = sorted(tickers_jan2024)
            df_2024 = yf.download(
                _sym_list, start="2024-01-01", end="2024-12-31",
                auto_adjust=True, progress=False, threads=True,
            )
            vol   = df_2024["Volume"].mean()
            close = df_2024["Close"].mean()
            valid = vol[(vol >= 500_000) & (close >= 3.0)].sort_values(ascending=False)
            top200 = valid.head(200).index.tolist()
            logger.info(f"[PIT_UNIVERSE] {len(valid)} tickers passed validation; using top {len(top200)}")
        except Exception as e:
            logger.warning(f"[PIT_UNIVERSE] yfinance validation failed: {e}. Using raw list.")
            top200 = sorted(tickers_jan2024)[:200]
    else:
        # Fallback: use current scan_universe (documented as survivorship-biased)
        logger.warning("[PIT_UNIVERSE] Using current scan_universe as fallback — SURVIVORSHIP BIAS PRESENT")
        from engine.super_backtest_v2 import _expand_alpha_universe
        top200 = _expand_alpha_universe()

    # Save disk cache
    try:
        cache_path.write_text(_json.dumps(sorted(top200)))
        logger.info(f"[PIT_UNIVERSE] Saved PIT universe to {cache_path}")
    except Exception as e:
        logger.warning(f"[PIT_UNIVERSE] Cache write failed: {e}")

    _PIT_UNIVERSE_CACHE = top200
    return top200


# ═══════════════════════════════════════════════════════════════════════════════
# Sniper v5 event loop (rsi_bounce only)
# ═══════════════════════════════════════════════════════════════════════════════

def _run_sniper_v5_event_loop(
    td: dict,
    days: list,
    vix_map: dict,
    alpha_scores: dict,
    agent_wr_tracker: dict,   # mutable: {agent_id: [win/loss, ...]}
    spy_map: dict = {},
) -> tuple[dict, dict, list, list, int]:
    """
    Sniper event loop: rsi_bounce only.
    Returns: event_trades, agent_trades, ollie_decisions, shadow_trades, skipped
    """
    event_trades:  dict[str, list] = defaultdict(list)
    agent_trades:  dict[str, list] = defaultdict(list)
    ollie_decisions: list = []
    shadow_trades:   list = []
    sniper_skipped   = 0

    SCAN_FREQ   = 3
    day_counter = 0

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

        positions:       dict[str, dict] = {}   # approved positions
        shadow_positions:dict[str, dict] = {}   # rejected (shadow) positions

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
            bull_signals = sum(1 for bv in list(t2.values()) + list(t3.values()) if bv)

            # ── ALMU Pattern: 200 SMA proximity bonus ─────────────────────────
            # Stock within 15% below 200 SMA = target above, add bull signal
            if len(c) >= 200:
                sma200 = float(np.mean(c[-200:]))
                below_200 = (sma200 - px) / sma200
                if 0.0 < below_200 < 0.15:
                    bull_signals += 1  # 200 SMA is nearby target above

            # ── Relative Strength: stock up when SPY down ─────────────────────
            if len(c) >= 5:
                stock_5d = (c[-1] - c[-5]) / c[-5] if c[-5] > 0 else 0
                spy_day  = spy_map.get(day.date() if hasattr(day, "date") else day, None)
                if spy_day is not None and stock_5d > 0 and spy_day < 0:
                    bull_signals += 1  # green when market red = relative strength

            # ── Exit existing approved positions ──────────────────────────────
            rsi_sig = rsi_val < 30
            key = f"{sym}_rsi_bounce"

            if key in positions:
                pos  = positions[key]
                gain = (px - pos["entry"]) / pos["entry"]
                held = pos.get("days_held", 0)
                if gain >= 0.08 or gain <= -0.05 or held >= 15:
                    pnl_pct = gain * 100 * pos_factor - SLIPPAGE * 200
                    pnl_pct = max(-100.0, min(100.0, pnl_pct))
                    t = {
                        "strategy":    "rsi_bounce",
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
                        "ollie_approved": True,
                    }
                    event_trades["rsi_bounce"].append(t)
                    agent_trades[pos["agent_id"]].append(t)
                    agent_wr_tracker[pos["agent_id"]].append(1 if pnl_pct > 0 else 0)
                    del positions[key]
                else:
                    positions[key]["days_held"] = held + 1

            # ── Exit shadow positions ─────────────────────────────────────────
            if key in shadow_positions:
                spos = shadow_positions[key]
                sgain = (px - spos["entry"]) / spos["entry"]
                sheld = spos.get("days_held", 0)
                if sgain >= 0.08 or sgain <= -0.05 or sheld >= 15:
                    shadow_pnl = sgain * 100 * pos_factor - SLIPPAGE * 200
                    shadow_pnl = max(-100.0, min(100.0, shadow_pnl))
                    shadow_trades.append({
                        "strategy":   "rsi_bounce",
                        "ticker":     sym,
                        "pnl_pct":    round(shadow_pnl, 2),
                        "win":        1 if shadow_pnl > 0 else 0,
                        "agent_id":   spos["agent_id"],
                        "month":      month,
                    })
                    del shadow_positions[key]
                else:
                    shadow_positions[key]["days_held"] = sheld + 1

            # ── BULL MOMENTUM BREAKOUT ────────────────────────────────────────
            # Exits run every day (ungated). Entry gated by its own conditions.
            bm_key = f"{sym}_bull_momentum"
            bm_qualify = (
                regime == "BULL" or (regime == "CAUTIOUS" and bull_signals >= 3)
            )
            # momentum_breakout: price > 21-day high AND relative volume > 1.5x
            if len(c) >= 21:
                high_21 = float(np.max(c[-22:-1]))   # 21 days before today
                vol_ratio = (float(v[-1]) / avg_v) if avg_v > 0 else 1.0
                bm_breakout = (px > high_21) and (vol_ratio >= 1.5)
            else:
                bm_breakout = False
            bm_rsi_ok = 50 <= rsi_val <= 70
            bm_alpha_ok = alpha >= 0.35

            # Exit existing bull_momentum positions
            if bm_key in positions:
                bpos = positions[bm_key]
                bgain = (px - bpos["entry"]) / bpos["entry"]
                bheld = bpos.get("days_held", 0)
                if bgain >= 0.10 or bgain <= -0.05 or bheld >= 10:
                    bpnl = bgain * 100 * pos_factor - SLIPPAGE * 200
                    bpnl = max(-100.0, min(100.0, bpnl))
                    bt = {
                        "strategy":    "bull_momentum",
                        "ticker":      sym,
                        "entry_date":  bpos["entry_date"],
                        "exit_date":   day_str,
                        "pnl_pct":     round(bpnl, 2),
                        "hold_days":   bheld,
                        "alpha_score": alpha,
                        "regime":      regime,
                        "month":       month,
                        "win":         1 if bpnl > 0 else 0,
                        "agent_id":    bpos["agent_id"],
                        "ollie_approved": True,
                    }
                    event_trades["bull_momentum"].append(bt)
                    agent_trades[bpos["agent_id"]].append(bt)
                    agent_wr_tracker[bpos["agent_id"]].append(1 if bpnl > 0 else 0)
                    del positions[bm_key]
                else:
                    positions[bm_key]["days_held"] = bheld + 1

            # ── Exit bull_momentum shadow positions ───────────────────────────
            if bm_key in shadow_positions:
                bspos = shadow_positions[bm_key]
                bsgain = (px - bspos["entry"]) / bspos["entry"]
                bsheld = bspos.get("days_held", 0)
                if bsgain >= 0.10 or bsgain <= -0.05 or bsheld >= 10:
                    bspnl = bsgain * 100 * pos_factor - SLIPPAGE * 200
                    bspnl = max(-100.0, min(100.0, bspnl))
                    shadow_trades.append({
                        "strategy":  "bull_momentum",
                        "ticker":    sym,
                        "pnl_pct":   round(bspnl, 2),
                        "win":       1 if bspnl > 0 else 0,
                        "agent_id":  bspos["agent_id"],
                        "month":     month,
                    })
                    del shadow_positions[bm_key]
                else:
                    shadow_positions[bm_key]["days_held"] = bsheld + 1

            # Enter new bull_momentum position
            if bm_qualify and bm_breakout and bm_rsi_ok and bm_alpha_ok and bm_key not in positions:
                bm_agent = "navigator" if regime == "BULL" else "chekov"
                wr_bm    = agent_wr_tracker.get(bm_agent, [])
                bm_wr    = (sum(wr_bm) / len(wr_bm)) if len(wr_bm) >= 3 else 0.55
                bm_score, bm_approved = _ollie_score(alpha, regime, "bull_momentum", bm_wr, bm_agent)

                ollie_decisions.append({
                    "symbol":      sym,
                    "strategy":    "bull_momentum",
                    "agent_id":    bm_agent,
                    "decision":    "APPROVE" if bm_approved else "REJECT",
                    "ollie_score": bm_score,
                    "trade_alpha": alpha,
                    "regime":      regime,
                    "day":         day_str,
                })

                bm_entry = px * (1 + SLIPPAGE + EXEC_DELAY)
                if bm_approved:
                    positions[bm_key] = {
                        "entry":      bm_entry,
                        "entry_date": day_str,
                        "days_held":  0,
                        "alpha":      alpha,
                        "agent_id":   bm_agent,
                        "strategy":   "bull_momentum",
                    }
                else:
                    if bm_key not in shadow_positions:
                        shadow_positions[bm_key] = {
                            "entry":      bm_entry,
                            "entry_date": day_str,
                            "days_held":  0,
                            "alpha":      alpha,
                            "agent_id":   bm_agent,
                        }
                    sniper_skipped += 1

            # CAUTIOUS rsi_bounce disabled — 0% WR across all runs
            if regime == "CAUTIOUS":
                sniper_skipped += 1
                continue
            if not rsi_sig:   # entry gate: rsi_val < 30
                continue

            # ── Triple filter ─────────────────────────────────────────────────
            if sym in {"TME", "BSX", "SCHG", "NVD", "DUST", "HDB", "CRCL", "AAPD"}:  # rsi_bounce 0% WR losers / invalid ETFs — all regimes
                sniper_skipped += 1
                continue
            _bull_min = SNIPER_BULL_MIN if regime == "BULL" else 1
            if bull_signals < _bull_min:
                sniper_skipped += 1
                continue

            if regime == "BULL":
                _rb_agents = ["navigator", "chekov"]
            elif regime in ("BEAR", "CRISIS"):
                _rb_agents = ["neo-matrix", "chekov"]  # neo expanded - premium-aware
            else:
                _rb_agents = ["ollama-llama", "chekov"]
            agent_id = _rb_agents[day_counter % 2]

            # ── Gate 8: Ollie Commander ───────────────────────────────────────
            wr_hist  = agent_wr_tracker.get(agent_id, [])
            agent_wr = (sum(wr_hist) / len(wr_hist)) if len(wr_hist) >= 3 else 0.55
            o_score, o_approved = _ollie_score(alpha, regime, "rsi_bounce", agent_wr, agent_id)

            ollie_decisions.append({
                "symbol":       sym,
                "strategy":     "rsi_bounce",
                "agent_id":     agent_id,
                "decision":     "APPROVE" if o_approved else "REJECT",
                "ollie_score":  o_score,
                "trade_alpha":  alpha,
                "regime":       regime,
                "day":          day_str,
            })

            fill_cost = SLIPPAGE + EXEC_DELAY
            entry_px  = px * (1 + fill_cost)

            if o_approved:
                if key not in positions:
                    positions[key] = {
                        "entry":      entry_px,
                        "entry_date": day_str,
                        "days_held":  0,
                        "alpha":      alpha,
                        "agent_id":   agent_id,
                        "strategy":   "rsi_bounce",
                    }
            else:
                # Shadow track: record what this would have done
                if key not in shadow_positions:
                    shadow_positions[key] = {
                        "entry":      entry_px,
                        "entry_date": day_str,
                        "days_held":  0,
                        "alpha":      alpha,
                        "agent_id":   agent_id,
                    }
                sniper_skipped += 1

        # ── Force-close open positions at end-of-period ───────────────────────
        px_last = float(df["Close"].iloc[-1])
        for key, pos in positions.items():
            gain    = (px_last - pos["entry"]) / pos["entry"]
            pf      = _sniper_pos_factor(pos.get("alpha", 0.0))
            pnl_pct = gain * 100 * pf - SLIPPAGE * 200
            pnl_pct = max(-100.0, min(100.0, pnl_pct))
            t = {
                "strategy":    pos["strategy"],
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
                "ollie_approved": True,
            }
            event_trades[pos["strategy"]].append(t)
            agent_trades[pos["agent_id"]].append(t)
            agent_wr_tracker[pos["agent_id"]].append(1 if pnl_pct > 0 else 0)

        for key, spos in shadow_positions.items():
            sgain = (px_last - spos["entry"]) / spos["entry"]
            shadow_pnl = max(-100.0, min(100.0, sgain * 100 * _sniper_pos_factor(spos.get("alpha", 0.0)) - SLIPPAGE * 200))
            shadow_trades.append({
                "strategy": "rsi_bounce", "ticker": sym,
                "pnl_pct": round(shadow_pnl, 2), "win": 1 if shadow_pnl > 0 else 0,
                "agent_id": spos["agent_id"],
            })

    total = sum(len(v) for v in event_trades.values())
    logger.info(f"[SNIPER_V5_EVENT] {total} approved trades, {len(shadow_trades)} rejected shadows, {sniper_skipped} skipped")
    return event_trades, agent_trades, ollie_decisions, shadow_trades, sniper_skipped


# ═══════════════════════════════════════════════════════════════════════════════
# Sniper v5 options loop (CSP + covered_call)
# ═══════════════════════════════════════════════════════════════════════════════

def _run_sniper_v5_options_loop(
    td: dict,
    days: list,
    vix_map: dict,
    alpha_scores: dict,
    agent_wr_tracker: dict,
) -> tuple[list, list, list, int]:
    """CSP + covered_call with Ollie Commander gate."""
    options_trades: list = []
    ollie_decisions: list = []
    shadow_trades:   list = []
    sniper_skipped   = 0

    SCAN_FREQ   = 5
    day_counter = 0

    for day in days:
        day_counter += 1
        if day_counter % SCAN_FREQ != 0:
            continue

        day_str = day.strftime("%Y-%m-%d")
        vix_val = vix_map.get(day, 18.0)
        regime  = _classify_regime(vix_val)

        # Blacklist: tickers with proven poor options performance
        OPTIONS_BLACKLIST = {"TME", "SCHG", "DUST"}  # confirmed losers; DUST re-enters after loss
        RSI_BLACKLIST    = {"TME", "BSX", "SCHG", "AAPD"}  # AAPD: inverse leveraged ETF, RSI bounce invalid

        for sym in list(td.keys()):
            if sym in OPTIONS_BLACKLIST:
                continue
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


            # ── CSP ───────────────────────────────────────────────────────────
            if ivr > 60 and bull >= 2 and regime in ("BULL", "CAUTIOUS", "NEUTRAL", "BEAR", "CRISIS"):
                if regime in ("BEAR", "CRISIS"):
                    # neo IV floor check
                    _rv20 = float(np.std(np.diff(np.log(c[-21:]))) if len(c) >= 21 else 0.0)
                    if _rv20 < 0.015:
                        sniper_skipped += 1
                        continue
                    agent_id = "neo-matrix"  # neo expanded - premium-aware
                else:
                    agent_id = "ollama-plutus" if vix_val >= 20 else ("capitol-trades" if day_counter % 3 == 0 else "ollama-qwen3")
                wr_hist  = agent_wr_tracker.get(agent_id, [])
                agent_wr = (sum(wr_hist) / len(wr_hist)) if len(wr_hist) >= 3 else 0.55
                o_score, o_approved = _ollie_score(alpha, regime, "csp", agent_wr, agent_id)

                ollie_decisions.append({
                    "symbol": sym, "strategy": "csp", "agent_id": agent_id,
                    "decision": "APPROVE" if o_approved else "REJECT",
                    "ollie_score": o_score, "trade_alpha": alpha, "regime": regime, "day": day_str,
                })

                r = _sim_csp(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    trade_base = {
                        **r, **extra,
                        "strategy":  "csp",
                        "agent_id":  agent_id,
                        "exit_date": _exit_date_str(future, r["days"], day_str),
                        "hold_days": r.get("days", 1),
                        "win":       1 if r.get("pnl", 0) > 0 else 0,
                        "ollie_approved": o_approved,
                    }
                    if o_approved:
                        options_trades.append(trade_base)
                        agent_wr_tracker[agent_id].append(trade_base["win"])
                    else:
                        shadow_trades.append({**trade_base, "pnl_pct": trade_base.get("pnl_pct", trade_base.get("pnl", 0))})
                        sniper_skipped += 1
            else:
                sniper_skipped += 1

            # ── Covered Call ──────────────────────────────────────────────────
            if ivr > 50 and bull >= 2:
                if regime == "BULL":
                    # Rotate capitol-trades in every 3rd BULL covered_call
                    agent_id = "capitol-trades" if day_counter % 3 == 0 else "ollama-llama"
                elif regime in ("CAUTIOUS", "NEUTRAL"):
                    agent_id = "capitol-trades" if day_counter % 3 == 0 else "ollama-llama"
                elif regime in ("BEAR", "CRISIS"):  # neo expanded - premium-aware
                    # neo IV floor check
                    _rv20 = float(np.std(np.diff(np.log(c[-21:]))) if len(c) >= 21 else 0.0)
                    if _rv20 < 0.015:
                        sniper_skipped += 1
                        continue
                    agent_id = "neo-matrix"
                wr_hist  = agent_wr_tracker.get(agent_id, [])
                agent_wr = (sum(wr_hist) / len(wr_hist)) if len(wr_hist) >= 3 else 0.55
                o_score, o_approved = _ollie_score(alpha, regime, "covered_call", agent_wr, agent_id)

                ollie_decisions.append({
                    "symbol": sym, "strategy": "covered_call", "agent_id": agent_id,
                    "decision": "APPROVE" if o_approved else "REJECT",
                    "ollie_score": o_score, "trade_alpha": alpha, "regime": regime, "day": day_str,
                })

                r = _sim_covered_call(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    trade_base = {
                        **r, **extra,
                        "strategy":  "covered_call",
                        "agent_id":  agent_id,
                        "exit_date": _exit_date_str(future, r["days"], day_str),
                        "hold_days": r.get("days", 1),
                        "win":       1 if r.get("pnl", 0) > 0 else 0,
                        "ollie_approved": o_approved,
                    }
                    if o_approved:
                        options_trades.append(trade_base)
                        agent_wr_tracker[agent_id].append(trade_base["win"])
                    else:
                        shadow_trades.append({**trade_base, "pnl_pct": trade_base.get("pnl_pct", trade_base.get("pnl", 0))})
                        sniper_skipped += 1
            else:
                sniper_skipped += 1

    logger.info(f"[SNIPER_V5_OPT] {len(options_trades)} approved, {len(shadow_trades)} shadows, {sniper_skipped} skipped")
    return options_trades, ollie_decisions, shadow_trades, sniper_skipped


# ═══════════════════════════════════════════════════════════════════════════════
# Sniper v5 orchestrator
# ═══════════════════════════════════════════════════════════════════════════════

def _run_sniper_v5(td: dict, trading_days: list, vix_map: dict,
                   alpha_scores: dict, spy_return: float, run_date: str) -> dict:
    """
    Sniper Mode v5 — alpha gate fixed + Ollie Commander gate.
    3 strategies: rsi_bounce, csp, covered_call.
    """
    logger.info(f"[SNIPER_V5] Starting on {len(td)} symbols, {len(trading_days)} days")

    # Alpha filter — using CORRECT composite_score from DB
    sniper_universe = {
        sym: df for sym, df in td.items()
        if alpha_scores.get(sym, 0.0) >= SNIPER_ALPHA_THRESHOLD
    }
    logger.info(f"[SNIPER_V5] {len(sniper_universe)} symbols pass alpha>={SNIPER_ALPHA_THRESHOLD}")

    # Rolling win-rate tracker (shared across event + options loops)
    agent_wr_tracker: dict[str, list] = defaultdict(list)

    # Build SPY daily return map for relative strength signal
    spy_map: dict = {}
    try:
        import pandas as pd
        _spy_start = min(trading_days) - timedelta(days=10)
        _spy_end   = max(trading_days) + timedelta(days=2)
        _spy_df    = _download_spy_with_retry(_spy_start, _spy_end, max_retries=2, delay=1)
        if _spy_df is not None and not _spy_df.empty:
            _spy_df.index = pd.to_datetime(_spy_df.index).normalize()
            _spy_c = _spy_df["Close"].values
            _spy_d = list(_spy_df.index)
            for i in range(1, len(_spy_c)):
                spy_map[_spy_d[i].date()] = (_spy_c[i] - _spy_c[i-1]) / _spy_c[i-1]
    except Exception as _e:
        logger.warning(f"spy_map build failed: {_e}")

    # Run loops
    event_trades, agent_trades, ev_ollie, ev_shadow, ev_skipped = _run_sniper_v5_event_loop(
        sniper_universe, trading_days, vix_map, alpha_scores, agent_wr_tracker, spy_map
    )
    opt_trades, opt_ollie, opt_shadow, opt_skipped = _run_sniper_v5_options_loop(
        sniper_universe, trading_days, vix_map, alpha_scores, agent_wr_tracker
    )

    all_ollie_decisions = ev_ollie + opt_ollie
    all_shadow_trades   = ev_shadow + opt_shadow
    total_skipped       = ev_skipped + opt_skipped

    # Merge options into agent_trades
    for t in opt_trades:
        aid = t.get("agent_id", "ollama-llama")
        t.setdefault("month",  t.get("entry_date", "")[:7])
        t.setdefault("win",    1 if t.get("pnl_pct", t.get("pnl", 0)) > 0 else 0)
        agent_trades[aid].append(t)
        # normalise pnl_pct
        if "pnl_pct" not in t and "pnl" in t:
            t["pnl_pct"] = t["pnl"]

    all_trades_flat: list = []
    for tlist in event_trades.values():
        all_trades_flat.extend(tlist)
    all_trades_flat.extend(opt_trades)
    all_trades_flat = [t for t in all_trades_flat if "pnl_pct" in t]

    overall = _trade_metrics_v4(all_trades_flat)
    overall["spy_return"] = spy_return
    overall["vs_spy"]     = round(overall["total_return"] - spy_return, 2)

    # ── Ollie stats ────────────────────────────────────────────────────────────
    ollie_submitted = len(all_ollie_decisions)
    ollie_approved  = sum(1 for d in all_ollie_decisions if d["decision"] == "APPROVE")
    ollie_rejected  = ollie_submitted - ollie_approved

    approved_pnls  = [t.get("pnl_pct", 0) for t in all_trades_flat]
    rejected_pnls  = [t.get("pnl_pct", 0) for t in all_shadow_trades if "pnl_pct" in t]

    approved_wr       = (sum(1 for p in approved_pnls if p > 0) / len(approved_pnls) * 100) if approved_pnls else 0.0
    rejected_shadow_wr = (sum(1 for p in rejected_pnls if p > 0) / len(rejected_pnls) * 100) if rejected_pnls else 0.0
    ollie_value_added  = 1 if approved_wr >= rejected_shadow_wr else 0

    # v4 reference for comparison
    v4_sniper_ret    = -7.65
    v4_sniper_sharpe = -0.098
    alpha_impact_ret    = round(overall["total_return"] - v4_sniper_ret, 2)
    alpha_impact_sharpe = round(overall["sharpe"]       - v4_sniper_sharpe, 3)

    # ── Per-agent metrics ──────────────────────────────────────────────────────
    agent_metrics: dict = {}
    for aid, trades in agent_trades.items():
        if not trades:
            continue
        am = _trade_metrics_v4([t for t in trades if "pnl_pct" in t])
        for regime in ("BULL", "CAUTIOUS", "BEAR"):
            rtrades = [t for t in trades if t.get("regime") == regime and "pnl_pct" in t]
            rm      = _trade_metrics_v4(rtrades) if rtrades else {}
            am[f"{regime.lower()}_return"] = rm.get("total_return", 0.0)
        agent_metrics[aid] = am

    # ── Per-strategy breakdown ─────────────────────────────────────────────────
    by_strategy: dict[str, list] = defaultdict(list)
    for t in all_trades_flat:
        by_strategy[t.get("strategy", "unknown")].append(t)

    # ── Per-strategy+symbol ────────────────────────────────────────────────────
    by_strat_sym: dict = defaultdict(list)
    for t in all_trades_flat:
        by_strat_sym[(t.get("strategy", "unknown"), t.get("ticker", ""))].append(t)

    # ── Regime breakdown ───────────────────────────────────────────────────────
    regime_perf: dict = defaultdict(list)
    for t in all_trades_flat:
        regime_perf[(t.get("regime", "MIXED"), t.get("strategy", "unknown"))].append(t)

    # ── Monthly breakdown ──────────────────────────────────────────────────────
    monthly_by_agent: dict = defaultdict(lambda: defaultdict(list))
    for t in all_trades_flat:
        month = (t.get("exit_date") or t.get("entry_date") or "")[:7]
        aid   = t.get("agent_id", "unknown")
        if month:
            monthly_by_agent[month][aid].append(t)

    # ── Equity curve ──────────────────────────────────────────────────────────
    curve = _build_equity_curve_v4(event_trades, opt_trades, [], trading_days, vix_map)

    # ── Alpha attribution ──────────────────────────────────────────────────────
    alpha_attr = _compute_alpha_attribution_v4(all_trades_flat)

    # ── Alpha signal report card ───────────────────────────────────────────────
    alpha_report = _build_alpha_signal_report(all_trades_flat, alpha_scores)

    # ── Save to DB ─────────────────────────────────────────────────────────────
    conn = _bt_conn()
    now  = datetime.utcnow().isoformat()

    for aid, am in agent_metrics.items():
        spec = SNIPER_FLEET_V5.get(aid, {})
        conn.execute("""
            INSERT OR REPLACE INTO backtest_results_oos
            (run_date, agent_id, agent_name, model,
             total_return, win_rate, sharpe, max_drawdown,
             profit_factor, num_trades, avg_hold_days,
             best_trade_pct, worst_trade_pct,
             bull_return, cautious_return, bear_return,
             spy_return, vs_spy, created_at,
             expectancy, recovery_factor)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            run_date, aid, spec.get("name", aid), spec.get("model", ""),
            am["total_return"], am["win_rate"], am["sharpe"], am["max_drawdown"],
            am["profit_factor"], am["num_trades"], am["avg_hold_days"],
            am["best_trade_pct"], am["worst_trade_pct"],
            am.get("bull_return", 0), am.get("cautious_return", 0), am.get("bear_return", 0),
            spy_return, round(am["total_return"] - spy_return, 2), now,
            am.get("expectancy", 0.0), am.get("recovery_factor", 0.0),
        ))

    for (strat, sym), trades in by_strat_sym.items():
        if not trades:
            continue
        sm      = _trade_metrics_v4(trades)
        alpha_a = float(np.mean([t.get("alpha_score", 0.0) for t in trades]))
        conn.execute("""
            INSERT OR REPLACE INTO backtest_trades_oos
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
            INSERT OR REPLACE INTO backtest_equity_oos
            (run_date, trade_date, equity, daily_pnl, regime)
            VALUES (?,?,?,?,?)
        """, (run_date, row["trade_date"], row["equity"], row["daily_pnl"], row["regime"]))

    for month, agents_dict in monthly_by_agent.items():
        for aid, atrades in agents_dict.items():
            if not atrades:
                continue
            mm = _trade_metrics_v4(atrades)
            conn.execute("""
                INSERT OR REPLACE INTO backtest_monthly_oos
                (run_date, month, agent_id, total_return, win_rate, num_trades)
                VALUES (?,?,?,?,?,?)
            """, (run_date, month, aid, mm["total_return"], mm["win_rate"], mm["num_trades"]))

    for d in all_ollie_decisions:
        # Find shadow pnl for rejected decisions
        shadow_pnl = None
        if d["decision"] == "REJECT":
            match = [s.get("pnl_pct") for s in all_shadow_trades
                     if s.get("ticker") == d.get("symbol") and s.get("strategy") == d.get("strategy")]
            shadow_pnl = match[0] if match else None
        conn.execute("""
            INSERT INTO backtest_decisions_oos
            (run_date, symbol, strategy, agent_id, decision, ollie_score,
             trade_alpha, regime, shadow_pnl_pct, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            run_date, d.get("symbol", ""), d.get("strategy", ""), d.get("agent_id", ""),
            d["decision"], d["ollie_score"],
            d.get("trade_alpha", 0), d.get("regime", ""), shadow_pnl, now,
        ))

    conn.execute("""
        INSERT OR REPLACE INTO backtest_summary_oos
        (run_date, total_return, win_rate, sharpe, max_drawdown, num_trades,
         spy_return, vs_spy,
         ollie_submitted, ollie_approved, ollie_rejected,
         approved_wr, rejected_shadow_wr, ollie_value_added,
         alpha_gate_impact_return, alpha_gate_impact_sharpe, created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        run_date,
        overall["total_return"], overall["win_rate"], overall["sharpe"], overall["max_drawdown"],
        overall["num_trades"],   spy_return, overall["vs_spy"],
        ollie_submitted, ollie_approved, ollie_rejected,
        round(approved_wr, 1), round(rejected_shadow_wr, 1), ollie_value_added,
        alpha_impact_ret, alpha_impact_sharpe, now,
    ))

    conn.commit()
    conn.close()

    # Save summary JSON (alongside v4)
    summary_path = DATA_DIR / "backtest_oos_summary.json"
    with open(summary_path, "w") as f:
        json.dump({
            "run_date": run_date,
            "metrics":  overall,
            "ollie": {
                "submitted": ollie_submitted, "approved": ollie_approved,
                "rejected": ollie_rejected,
                "approved_wr": round(approved_wr, 1),
                "rejected_shadow_wr": round(rejected_shadow_wr, 1),
                "value_added": bool(ollie_value_added),
            },
            "alpha_impact": {
                "return_pp":  alpha_impact_ret,
                "sharpe_pts": alpha_impact_sharpe,
                "vs_v4_sniper_ret":    v4_sniper_ret,
                "vs_v4_sniper_sharpe": v4_sniper_sharpe,
            },
        }, f, indent=2)

    logger.info(
        f"[SNIPER_V5] Done: {overall['num_trades']} trades, "
        f"return={overall['total_return']:+.2f}%, Sharpe={overall['sharpe']:.3f}, "
        f"Ollie: {ollie_approved}/{ollie_submitted} approved"
    )

    return {
        "status":          "ok",
        "metrics":         overall,
        "agent_metrics":   agent_metrics,
        "by_strategy":     {s: _trade_metrics_v4(t) for s, t in by_strategy.items() if t},
        "by_strat_sym":    {f"{s}/{sym}": _trade_metrics_v4(trades)
                            for (s, sym), trades in by_strat_sym.items()},
        "regime_perf":     {f"{r}_{s}": _trade_metrics_v4(ts)
                            for (r, s), ts in regime_perf.items() if len(ts) >= 2},
        "monthly_by_agent": {m: {a: _trade_metrics_v4(ts) for a, ts in agents.items() if ts}
                             for m, agents in monthly_by_agent.items()},
        "curve":            curve,
        "alpha_attr":       alpha_attr,
        "alpha_report":     alpha_report,
        "ollie": {
            "submitted":     ollie_submitted,
            "approved":      ollie_approved,
            "rejected":      ollie_rejected,
            "approved_wr":   round(approved_wr, 1),
            "shadow_wr":     round(rejected_shadow_wr, 1),
            "value_added":   bool(ollie_value_added),
            "decisions":     all_ollie_decisions,
        },
        "alpha_impact": {
            "return_pp":  alpha_impact_ret,
            "sharpe_pts": alpha_impact_sharpe,
        },
        "spy_return":   spy_return,
        "sniper_skipped": total_skipped,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Alpha signal report card (12 crystals)
# ═══════════════════════════════════════════════════════════════════════════════

def _build_alpha_signal_report(all_trades: list, alpha_scores: dict) -> dict:
    """
    Cross-reference which alpha signals were active on winning vs losing trades.
    Uses composite_score as proxy since granular signal scores need the full DB.
    """
    if not all_trades:
        return {}

    winners = [t for t in all_trades if t.get("pnl_pct", 0) > 0]
    losers  = [t for t in all_trades if t.get("pnl_pct", 0) <= 0]

    win_alphas  = [alpha_scores.get(t.get("ticker", ""), 0.0) for t in winners]
    lose_alphas = [alpha_scores.get(t.get("ticker", ""), 0.0) for t in losers]

    avg_win_alpha  = float(np.mean(win_alphas))  if win_alphas  else 0.0
    avg_lose_alpha = float(np.mean(lose_alphas)) if lose_alphas else 0.0

    # Try to load granular signal data from alpha_signals.db
    signal_report: dict = {
        "avg_composite_on_wins":   round(avg_win_alpha,  3),
        "avg_composite_on_losses": round(avg_lose_alpha, 3),
        "composite_signal_edge":   round(avg_win_alpha - avg_lose_alpha, 3),
        "total_wins":              len(winners),
        "total_losses":            len(losers),
    }

    try:
        ac = _alpha_conn()
        # Try to pull granular signal data for each signal type
        signal_cols = [
            "dark_pool_score", "ftd_score", "insider_score",
            "put_call_score", "vix_structure_score", "sentiment_score",
            "yield_curve_score", "opex_score", "earnings_score",
            "rebalancing_score", "rallies_consensus_score", "rallies_debate_score",
        ]
        existing_cols = set()
        rows = ac.execute("PRAGMA table_info(composite_alpha)").fetchall()
        for r in rows:
            existing_cols.add(r[1])

        for col in signal_cols:
            if col not in existing_cols:
                continue
            rows = ac.execute(f"""
                SELECT symbol, {col}, composite_score
                FROM composite_alpha
                ORDER BY created_at DESC
            """).fetchall()
            if not rows:
                continue
            sig_scores = {r["symbol"]: float(r[col] or 0) for r in rows}

            win_sig  = [sig_scores.get(t.get("ticker", ""), 0.0) for t in winners]
            lose_sig = [sig_scores.get(t.get("ticker", ""), 0.0) for t in losers]
            signal_report[col] = {
                "avg_on_wins":   round(float(np.mean(win_sig))  if win_sig  else 0.0, 3),
                "avg_on_losses": round(float(np.mean(lose_sig)) if lose_sig else 0.0, 3),
                "edge":          round((float(np.mean(win_sig)) if win_sig else 0) -
                                       (float(np.mean(lose_sig)) if lose_sig else 0), 3),
            }
        ac.close()
    except Exception as e:
        logger.warning(f"[V5_ALPHA_REPORT] {e}")

    return signal_report


# ═══════════════════════════════════════════════════════════════════════════════
# Report printer
# ═══════════════════════════════════════════════════════════════════════════════

def _print_v5_report(run_date: str, bC_v5: dict, spy_return: float) -> None:
    bar  = "━" * 100
    m    = bC_v5.get("metrics", {})
    ollie = bC_v5.get("ollie", {})
    impact = bC_v5.get("alpha_impact", {})

    def _f(val, pct=False, trades=False) -> str:
        if val is None: return "—"
        if trades: return str(int(val)) if val else "0"
        if pct:
            try: return f"{float(val):+.2f}%"
            except: return str(val)
        try: return f"{float(val):.3f}"
        except: return str(val)

    print(f"\n{bar}")
    print(f"  WARP CORE REACTOR v5 — Alpha Gate Fixed + Ollie Commander")
    print(f"  Jan 9 – Apr 9, 2026  |  run={run_date}")
    print(f"  3 strategies: rsi_bounce · csp · covered_call")
    print(f"  7 agents: Ollie (gate) + Uhura + Worf + Spock + Seven + McCoy + Neo")
    print(f"{bar}")

    # ── [1] Full history table (8 data columns + BEST) ────────────────────────
    print(f"\n  [1] FULL HISTORY COMPARISON")

    HIST = {
        "Actual":    {"ret": -23.66, "sh": -6.516, "wr": 17.7,  "tr": 558},
        "v1":        {"ret": +41.33, "sh": -0.061, "wr": 41.8,  "tr": 2329},
        "v2(180d)":  {"ret": +8.42,  "sh": +0.874, "wr": 57.6,  "tr": 277},
        "v3b":       {"ret": +16.30, "sh": +1.003, "wr": 61.5,  "tr": 87},
        "SniperTT":  {"ret":  0.0,   "sh": +1.136, "wr": 83.3,  "tr": 18},
        "v4 v2R":    {"ret": +7.49,  "sh": +0.011, "wr": 55.9,  "tr": 145},
        "v4 Snipe":  {"ret": -7.65,  "sh": -0.098, "wr": 80.0,  "tr": 15},
        "v5":        {"ret": m.get("total_return", 0),
                      "sh":  m.get("sharpe", 0),
                      "wr":  m.get("win_rate", 0),
                      "tr":  m.get("num_trades", 0)},
    }
    cols   = list(HIST.keys())
    cw     = 10
    labels = ["Return %", "Sharpe", "Win Rate", "Trades"]

    print(f"\n  {'Metric':<14}" + "".join(f"{c:>{cw}}" for c in cols) + f"  {'BEST':>8}")
    print(f"  {'─'*110}")

    def _hist_row(label, key, pct=False, trades=False):
        vals = [HIST[c][key] for c in cols]
        strs = []
        for v in vals:
            if trades: strs.append(f"{int(v):>{cw}}")
            elif pct:  strs.append(f"{v:>+{cw}.2f}%"[: cw] if abs(v) < 1000 else f"{v:>{cw}.0f}%")
            else:      strs.append(f"{v:>{cw}.3f}")
        best_i = int(np.argmax(vals)) if not trades else int(np.argmax(vals))
        print(f"  {label:<14}" + "".join(strs) + f"  {cols[best_i]:>8}")

    # Return: higher is better
    ret_vals = [HIST[c]["ret"] for c in cols]
    ret_strs = [f"{v:+.2f}%" for v in ret_vals]
    best_r   = cols[int(np.argmax(ret_vals))]
    print(f"  {'Return %':<14}" + "".join(f"{s:>{cw}}" for s in ret_strs) + f"  {best_r:>8}")

    sh_vals = [HIST[c]["sh"] for c in cols]
    sh_strs = [f"{v:+.3f}" for v in sh_vals]
    best_s  = cols[int(np.argmax(sh_vals))]
    print(f"  {'Sharpe':<14}" + "".join(f"{s:>{cw}}" for s in sh_strs) + f"  {best_s:>8}")

    wr_vals = [HIST[c]["wr"] for c in cols]
    wr_strs = [f"{v:.1f}%" for v in wr_vals]
    best_w  = cols[int(np.argmax(wr_vals))]
    print(f"  {'Win Rate':<14}" + "".join(f"{s:>{cw}}" for s in wr_strs) + f"  {best_w:>8}")

    tr_vals = [HIST[c]["tr"] for c in cols]
    tr_strs = [f"{int(v)}" for v in tr_vals]
    print(f"  {'Trades':<14}" + "".join(f"{s:>{cw}}" for s in tr_strs))

    # ── [2] Ollie Commander Report ────────────────────────────────────────────
    print(f"\n  [2] OLLIE COMMANDER REPORT")
    print(f"  ┌{'─'*25}┬{'─'*12}┐")
    print(f"  │ {'Metric':<23} │ {'Value':>10} │")
    print(f"  ├{'─'*25}┼{'─'*12}┤")
    print(f"  │ {'Trades submitted':<23} │ {ollie.get('submitted',0):>10} │")
    print(f"  │ {'Ollie approved':<23} │ {ollie.get('approved',0):>10} │")
    print(f"  │ {'Ollie rejected':<23} │ {ollie.get('rejected',0):>10} │")
    print(f"  │ {'Approved win rate':<23} │ {ollie.get('approved_wr',0):>9.1f}% │")
    print(f"  │ {'Rejected would-have WR':<23} │ {ollie.get('shadow_wr',0):>9.1f}% │")
    ov = "YES ✓" if ollie.get("value_added") else "NO  ✗"
    print(f"  │ {'Ollie added value?':<23} │ {ov:>10} │")
    print(f"  └{'─'*25}┴{'─'*12}┘")

    # ── [3] Per-agent leaderboard (by Sharpe) ─────────────────────────────────
    print(f"\n  [3] PER-AGENT LEADERBOARD (by Sharpe)")
    print(f"  {'Rank':<6}{'Agent':<22}{'Trades':>8}{'Return':>10}{'WR':>8}{'Sharpe':>10}{'DD':>10}{'Bull':>8}{'Bear':>8}")
    print(f"  {'─'*90}")
    agent_rows = []
    for aid, am in sorted(bC_v5.get("agent_metrics", {}).items(),
                          key=lambda x: -x[1].get("sharpe", -99)):
        spec = SNIPER_FLEET_V5.get(aid, {})
        agent_rows.append((spec.get("name", aid), am))
    for i, (name, am) in enumerate(agent_rows, 1):
        star = " *" if abs(am.get("sharpe", 0)) > 5 or am.get("num_trades", 0) == 1 else ""
        print(f"  {i:<6}{name:<22}{am.get('num_trades',0):>8}"
              f"{am.get('total_return',0):>+9.2f}%"
              f"{am.get('win_rate',0):>7.1f}%"
              f"{am.get('sharpe',0):>+10.3f}"
              f"{am.get('max_drawdown',0):>9.2f}%"
              f"{am.get('bull_return',0):>+7.2f}%"
              f"{am.get('bear_return',0):>+7.2f}%{star}")

    # ── [4] Per-strategy breakdown ────────────────────────────────────────────
    print(f"\n  [4] PER-STRATEGY BREAKDOWN (by Sharpe)")
    print(f"  {'Strategy':<20}{'Trades':>8}{'Return':>10}{'WR':>8}{'Sharpe':>10}{'DD':>10}{'Best':>8}{'Worst':>8}")
    print(f"  {'─'*84}")
    strat_rows = sorted(bC_v5.get("by_strategy", {}).items(),
                        key=lambda x: -x[1].get("sharpe", -99))
    for strat, sm in strat_rows:
        print(f"  {strat:<20}{sm.get('num_trades',0):>8}"
              f"{sm.get('total_return',0):>+9.2f}%"
              f"{sm.get('win_rate',0):>7.1f}%"
              f"{sm.get('sharpe',0):>+10.3f}"
              f"{sm.get('max_drawdown',0):>9.2f}%"
              f"{sm.get('best_trade_pct',0):>+7.2f}%"
              f"{sm.get('worst_trade_pct',0):>+7.2f}%")

    # ── [5] Regime results ────────────────────────────────────────────────────
    print(f"\n  [5] REGIME RESULTS")
    print(f"  {'Regime+Strategy':<28}{'Trades':>8}{'Return':>10}{'WR':>8}{'Sharpe':>10}")
    print(f"  {'─'*64}")
    regime_rows = sorted(
        [(k, v) for k, v in bC_v5.get("regime_perf", {}).items()],
        key=lambda x: -x[1].get("sharpe", -99)
    )
    for key, rm in regime_rows[:12]:
        print(f"  {key:<28}{rm.get('num_trades',0):>8}"
              f"{rm.get('total_return',0):>+9.2f}%"
              f"{rm.get('win_rate',0):>7.1f}%"
              f"{rm.get('sharpe',0):>+10.3f}")

    # ── [6] Monthly breakdown ─────────────────────────────────────────────────
    print(f"\n  [6] MONTHLY BREAKDOWN")
    month_data: dict[str, list] = defaultdict(list)
    for month, agents_dict in bC_v5.get("monthly_by_agent", {}).items():
        for aid, am in agents_dict.items():
            month_data[month].append(am.get("total_return", 0))
    for month in sorted(month_data.keys()):
        vals  = month_data[month]
        total = sum(vals)
        avg   = total / len(vals) if vals else 0
        print(f"  {month}  total={total:+.2f}%  avg/agent={avg:+.2f}%  agents={len(vals)}")

    # ── [7] Alpha signal report card ──────────────────────────────────────────
    print(f"\n  [7] ALPHA SIGNAL REPORT CARD (12 Dilithium Crystals)")
    ar = bC_v5.get("alpha_report", {})
    print(f"  Avg composite on WINS:   {ar.get('avg_composite_on_wins', 0):+.3f}")
    print(f"  Avg composite on LOSSES: {ar.get('avg_composite_on_losses', 0):+.3f}")
    print(f"  Composite signal edge:   {ar.get('composite_signal_edge', 0):+.3f}")
    print()
    signal_labels = {
        "dark_pool_score":           "Dark Pool          (wt 0.20)",
        "insider_score":             "Insider Activity   (wt 0.20)",
        "ftd_score":                 "Failure-to-Deliver (wt 0.15)",
        "put_call_score":            "Put/Call Ratio     (wt 0.10)",
        "vix_structure_score":       "VIX Structure      (wt 0.10)",
        "sentiment_score":           "Sentiment          (wt 0.10)",
        "opex_score":                "OpEx               (wt 0.05)",
        "yield_curve_score":         "Yield Curve        (wt 0.03)",
        "earnings_score":            "Earnings           (wt 0.03)",
        "rebalancing_score":         "Rebalancing        (wt 0.00)",
        "rallies_consensus_score":   "Rallies Consensus  (wt 0.05)",
        "rallies_debate_score":      "Rallies Debate     (wt 0.05)",
    }
    for col, label in signal_labels.items():
        sig = ar.get(col)
        if not sig:
            print(f"  {label:<35} ← no data")
            continue
        edge   = sig.get("edge", 0)
        edge_s = "✓" if edge > 0.05 else "✗" if edge < -0.05 else "~"
        print(f"  {label:<35}  wins={sig['avg_on_wins']:+.3f}  "
              f"losses={sig['avg_on_losses']:+.3f}  "
              f"edge={edge:+.3f} {edge_s}")

    # ── [8] Verdict ───────────────────────────────────────────────────────────
    print(f"\n{bar}")
    print(f"  VERDICT")
    print(f"{bar}")

    v4_ret, v4_sh = -7.65, -0.098
    v5_ret = m.get("total_return", 0)
    v5_sh  = m.get("sharpe", 0)
    ret_delta = v5_ret - v4_ret
    sh_delta  = v5_sh  - v4_sh

    print(f"\n  ALPHA GATE IMPACT (v5 vs v4 Sniper):")
    arrow_r = "↑" if ret_delta > 0 else "↓"
    arrow_s = "↑" if sh_delta  > 0 else "↓"
    print(f"    Return:  {v4_ret:+.2f}% → {v5_ret:+.2f}%  ({ret_delta:+.2f}% {arrow_r})")
    print(f"    Sharpe:  {v4_sh:+.3f}  → {v5_sh:+.3f}   ({sh_delta:+.3f} {arrow_s})")
    print(f"    ALPHA GATE IMPACT: {ret_delta:+.2f}% return,  {sh_delta:+.3f} Sharpe")

    print(f"\n  OLLIE COMMANDER IMPACT:")
    ov_str = "ADDING VALUE" if ollie.get("value_added") else "BLOCKING GOOD TRADES"
    print(f"    Approved WR:  {ollie.get('approved_wr', 0):.1f}%")
    print(f"    Rejected WR:  {ollie.get('shadow_wr',   0):.1f}%  (shadow — would-have)")
    print(f"    Delta:        {ollie.get('approved_wr', 0) - ollie.get('shadow_wr', 0):+.1f}pp")
    print(f"    Verdict:      Ollie is {ov_str}")

    print(f"\n  SNIPER MODE STATUS:")
    if v5_ret > v4_ret and v5_sh > v4_sh:
        print(f"    ✓ Alpha gate fix improved both return and Sharpe vs v4 sniper")
    elif v5_ret > v4_ret or v5_sh > v4_sh:
        print(f"    ~ Alpha gate fix improved one metric — check regime mix")
    else:
        print(f"    ✗ Alpha gate fix did not improve results — alpha signals need calibration")

    if m.get("win_rate", 0) >= 65:
        print(f"    ✓ Win rate {m.get('win_rate', 0):.1f}% ≥ 65% Proving Ground target")
    else:
        print(f"    ✗ Win rate {m.get('win_rate', 0):.1f}% below 65% Proving Ground target")

    print(f"\n  RECOMMENDATION:")
    if v5_ret > 0 and m.get("win_rate", 0) >= 60:
        print(f"    PROCEED — Sniper Mode with alpha gate fixed shows positive expected value.")
    elif m.get("win_rate", 0) >= 60:
        print(f"    CAUTIOUS — Win rate strong but return negative. Check SPY regime headwind.")
    else:
        print(f"    INVESTIGATE — Both return and WR below target. Review alpha signal calibration.")

    print(f"\n  SPY Buy & Hold:  {spy_return:+.2f}%  |  v5 vs SPY: {m.get('vs_spy', 0):+.2f}%")
    print(f"\n  * = single-trade Sharpe (std ≈ 0, magnitude unreliable)")
    print(f"{bar}\n")


# ═══════════════════════════════════════════════════════════════════════════════
# Main entry point
# ═══════════════════════════════════════════════════════════════════════════════

def run_oos_backtest_2024() -> dict:
    """
    OOS Validation: held-out 2024 backtest.
    Runs the SAME strategy/gate logic as v5 on data it has never seen.
    NEVER modifies trader.db, arena.db, or any v5 tables.
    Results written to *_oos tables only.
    """
    t0       = time.time()
    run_date = date.today().isoformat()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    logger.info("═" * 70)
    logger.info("  OOS Validation: Held-Out 2024 Backtest (Candidate A)")
    logger.info(f"  Run date: {run_date}  |  Window: {WINDOW_START} → {WINDOW_END}")
    logger.info("═" * 70)

    _init_oos_tables()

    # ── Step 1: Download SPY ──────────────────────────────────────────────────
    end_dt   = date.today()
    start_dt = end_dt - timedelta(days=BACKTEST_DAYS + 30)
    logger.info("[STEP 1] Downloading SPY for return calculation...")
    spy_df = _download_spy_with_retry(start_dt, end_dt, max_retries=3, delay=2)
    spy_return = 0.0
    if spy_df is not None and not spy_df.empty:
        try:
            w_start = pd.Timestamp(WINDOW_START)
            w_end   = pd.Timestamp(WINDOW_END)
            spy_df.index = pd.to_datetime(spy_df.index).normalize()
            spy_sub = spy_df.loc[(spy_df.index >= w_start) & (spy_df.index <= w_end)]
            if len(spy_sub) >= 2:
                spy_return = round(
                    (float(spy_sub["Close"].iloc[-1]) / float(spy_sub["Close"].iloc[0]) - 1) * 100, 2
                )
                logger.info(f"SPY return in window: {spy_return:+.2f}%")
        except Exception as e:
            logger.warning(f"SPY return calc error: {e}")

    # ── Step 2: OOS universe — point-in-time S&P 500 as of 2024-01-01 ──────────
    # alpha_signals.db contains Apr 2026 data — using it for 2024 would be forward
    # contamination. Instead: neutral score 0.5 for all PIT symbols.
    # 0.5 > SNIPER_ALPHA_THRESHOLD (0.25), so all symbols pass the gate equally.
    # Position sizing via _sniper_pos_factor(0.5) will be uniform.
    logger.info("[STEP 2] Building point-in-time S&P 500 universe as of 2024-01-01...")
    pit_symbols = _build_pit_universe_2024()
    merged_scores = {sym: 0.5 for sym in pit_symbols}
    all_syms = pit_symbols[:200]
    logger.info(
        f"[STEP 2] PIT universe: {len(all_syms)} symbols | alpha=0.5 (neutral, no look-ahead)"
    )

    # ── Step 3: Download price universe ──────────────────────────────────────
    logger.info(f"[STEP 3] Downloading {len(all_syms)} symbols + ^VIX ({BACKTEST_DAYS+60}d)...")
    td = _download_v2_universe(all_syms + ["SPY", "^VIX"], BACKTEST_DAYS + 60)

    # ── Step 4: Build VIX map and trading days ────────────────────────────────
    vix_df  = td.get("^VIX") if td.get("^VIX") is not None else td.get("VIX")
    vix_map: dict = {}
    if vix_df is not None and not vix_df.empty:
        vix_df.index = pd.to_datetime(vix_df.index).normalize()
        for idx, row in vix_df.iterrows():
            vix_map[pd.Timestamp(idx)] = float(row.get("Close", 20.0))

    all_days     = _get_trading_days(td, BACKTEST_DAYS + 60)
    trading_days = [d for d in all_days
                    if pd.Timestamp(WINDOW_START) <= d <= pd.Timestamp(WINDOW_END)]
    if not trading_days:
        trading_days = all_days
    logger.info(f"[STEP 4] {len(trading_days)} trading days, {len(vix_map)} VIX entries")

    # ── Step 5: Run Sniper v5 ─────────────────────────────────────────────────
    logger.info("[STEP 5] Running Sniper v5 (alpha fixed + Ollie gate)...")
    bC = _run_sniper_v5(td, trading_days, vix_map, merged_scores, spy_return, run_date)

    # ── Step 6: Print report ──────────────────────────────────────────────────
    _print_v5_report(run_date, bC, spy_return)

    elapsed = round(time.time() - t0, 1)
    logger.info(f"OOS Backtest 2024 complete in {elapsed}s")
    logger.info(
        f"Results in backtest.db tables: backtest_results_oos, backtest_trades_oos, "
        f"backtest_summary_oos, backtest_equity_oos, backtest_monthly_oos, backtest_decisions_oos"
    )

    return {"status": "ok", "run_date": run_date, "sniper_oos": bC, "elapsed_s": elapsed}


if __name__ == "__main__":
    run_oos_backtest_2024()
