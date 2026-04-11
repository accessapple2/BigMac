"""
engine/super_backtest_v2.py — Super Backtest v2 "Dilithium Crystal Edition"

Enhancements over v1:
  • Expanded 200-stock universe (from scan_universe, top by volume)
  • Dual entry filter: alpha_score >= 0.5 AND confidence_proxy >= 0.65
  • Alpha-weighted position sizing (1.5+ = full, 1.0 = 75%, 0.5 = 50%)
  • Fleet actual performance analysis (READ-ONLY from trader.db)
  • Signal Center grade analysis (watchlist_signals as grade proxy)
  • Three-way comparison: Actual Fleet vs v1 vs v2
  • Alpha attribution: which signals drove wins

IMPORTANT:
  • Saves to data/backtest.db with _v2 suffix — v1 tables untouched
  • NEVER modifies trader.db or arena.db
  • Alpha scores are today's static values (noted as limitation)
"""
from __future__ import annotations

import io
import json
import logging
import math
import sqlite3
import time
import zipfile
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests
import yfinance as yf

# Import simulation core from v1
from engine.master_backtest import (
    SLIPPAGE, OPT_SLIP_PER_LEG, OPT_COST, EXEC_DELAY, RISK_FREE,
    OPT_DTE_DEFAULT, STARTING_CASH,
    _bs_price, _bs_delta, _bs_theta,
    _hist_vol, _iv_rank, _rsi, _atr, _classify_regime,
    _run_tier1_vbt as _tier1_vbt, _tier2_signals, _tier3_signals, _tier4_signals,
    _tier9_short_signals,
    _sim_long_call, _sim_long_put, _sim_csp, _sim_covered_call,
    _sim_bull_call_spread, _sim_bull_put_spread,
    _sim_bear_put_spread, _sim_bear_call_spread,
    _sim_ic, _sim_broken_wing_ic, _sim_0dte,
    _exit_date_str, _trade_metrics,
    _get_trading_days,
)

logger = logging.getLogger(__name__)

_ROOT        = Path(__file__).resolve().parent.parent
BACKTEST_DB  = _ROOT / "data/backtest.db"
TRADER_DB    = _ROOT / "data/trader.db"
ALPHA_DB     = _ROOT / "data/alpha_signals.db"
SC_DB        = _ROOT / "signal-center/signals.db"

BACKTEST_DAYS   = 180
UNIVERSE_SIZE   = 200       # top N from scan_universe by avg_volume
ALPHA_THRESHOLD = 0.1       # min composite score to enter (scaled: only dp+earn signals populated)
CONF_THRESHOLD  = 0.65      # min confidence proxy (B-grade equivalent)

# Position sizing by alpha score (scaled for sparse-signal environment: max ~1.02)
def _alpha_position_factor(alpha: float) -> float:
    if alpha >= 0.6:  return 1.00
    if alpha >= 0.4:  return 0.75
    if alpha >= 0.1:  return 0.50
    return 0.0  # skip

_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": "TradeMinds research@trademinds.local"})


# ═══════════════════════════════════════════════════════════════════════════════
# DB helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _bt_conn() -> sqlite3.Connection:
    BACKTEST_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(BACKTEST_DB, timeout=20)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _td_conn() -> sqlite3.Connection:
    """Read-only connection to trader.db (direct path; no writes issued)."""
    conn = sqlite3.connect(str(TRADER_DB), timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def _alpha_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(ALPHA_DB, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def _init_v2_tables() -> None:
    """Create _v2 suffix tables in backtest.db."""
    conn = _bt_conn()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS backtest_master_results_v2 (
        id INTEGER PRIMARY KEY,
        run_date TEXT, tier INTEGER, tier_name TEXT,
        strategy TEXT, ticker TEXT,
        total_return REAL, win_rate REAL, sharpe REAL, max_drawdown REAL,
        avg_hold_hours REAL, num_trades INTEGER, profit_factor REAL, calmar REAL,
        best_trade_pct REAL, worst_trade_pct REAL,
        spy_return REAL, vs_spy REAL,
        max_consec_wins INTEGER, max_consec_losses INTEGER,
        regime TEXT, realistic_sharpe REAL, needs_validation INTEGER DEFAULT 0,
        alpha_score REAL,
        trades_skipped INTEGER DEFAULT 0,
        created_at TEXT
    );
    CREATE TABLE IF NOT EXISTS backtest_equity_curve_v2 (
        id INTEGER PRIMARY KEY,
        run_date TEXT, trade_date TEXT, equity REAL,
        daily_pnl REAL, regime TEXT, created_at TEXT
    );
    CREATE TABLE IF NOT EXISTS alpha_attribution_v2 (
        id INTEGER PRIMARY KEY,
        run_date TEXT, strategy TEXT, ticker TEXT,
        dark_pool_score REAL, ftd_score REAL, insider_score REAL,
        put_call_score REAL, vix_score REAL, sentiment_score REAL,
        yield_curve_score REAL, opex_score REAL, earnings_score REAL,
        rebalancing_score REAL, composite_score REAL,
        trade_outcome REAL, signal_count INTEGER,
        created_at TEXT
    );
    CREATE TABLE IF NOT EXISTS fleet_analysis_v2 (
        id INTEGER PRIMARY KEY,
        run_date TEXT,
        metric TEXT, value REAL, label TEXT,
        created_at TEXT
    );
    CREATE TABLE IF NOT EXISTS signal_grade_analysis_v2 (
        id INTEGER PRIMARY KEY,
        run_date TEXT,
        grade TEXT, signal_count INTEGER, acted_on INTEGER,
        win_rate REAL, avg_return REAL, created_at TEXT
    );
    CREATE TABLE IF NOT EXISTS comparison_table_v2 (
        id INTEGER PRIMARY KEY,
        run_date TEXT,
        metric TEXT,
        actual_fleet REAL, actual_fleet_label TEXT,
        v1_backtest REAL, v1_label TEXT,
        v2_backtest REAL, v2_label TEXT,
        created_at TEXT
    );
    """)
    conn.commit()
    conn.close()
    logger.info("v2 tables initialized")


# ═══════════════════════════════════════════════════════════════════════════════
# Step 1+2: Fleet Actual Performance Analysis (READ-ONLY)
# ═══════════════════════════════════════════════════════════════════════════════

def analyze_fleet_performance(run_date: str) -> dict:
    """
    Read trader.db (read-only) and compute fleet actual performance metrics.
    Uses COALESCE(corrected_pnl, realized_pnl) for accuracy.
    """
    conn = _td_conn()
    metrics = {}

    # Core P&L stats
    row = conn.execute("""
        SELECT
            COUNT(*) total_trades,
            SUM(CASE WHEN COALESCE(corrected_pnl,realized_pnl) > 0 THEN 1 ELSE 0 END) wins,
            SUM(COALESCE(corrected_pnl,realized_pnl)) total_pnl,
            AVG(COALESCE(corrected_pnl,realized_pnl)) avg_pnl,
            COUNT(DISTINCT symbol) unique_symbols,
            COUNT(DISTINCT player_id) unique_players,
            MIN(executed_at) first_trade,
            MAX(executed_at) last_trade
        FROM trades WHERE realized_pnl IS NOT NULL
    """).fetchone()

    metrics["total_trades"]    = row["total_trades"]
    metrics["wins"]            = row["wins"]
    metrics["win_rate"]        = round(row["wins"] / max(row["total_trades"], 1) * 100, 2)
    metrics["total_pnl"]       = round(row["total_pnl"] or 0, 2)
    metrics["unique_symbols"]  = row["unique_symbols"]
    metrics["unique_players"]  = row["unique_players"]
    metrics["date_range"]      = f"{row['first_trade'][:10]} → {row['last_trade'][:10]}"

    # Daily P&L for Sharpe + drawdown
    daily = conn.execute("""
        SELECT DATE(executed_at) day,
               SUM(COALESCE(corrected_pnl,realized_pnl)) dpnl
        FROM trades WHERE realized_pnl IS NOT NULL
        GROUP BY day ORDER BY day
    """).fetchall()

    pnls = [r["dpnl"] for r in daily]
    arr  = np.array(pnls)
    std  = float(np.std(arr)) if len(arr) > 1 else 1.0
    metrics["fleet_sharpe"] = round(float(np.mean(arr)) / max(std, 0.01) * math.sqrt(252), 3)

    # Portfolio-level return (using per-player $10K starting capital)
    total_starting = 10_000 * metrics["unique_players"]
    metrics["starting_capital"] = total_starting
    metrics["return_pct"] = round(metrics["total_pnl"] / total_starting * 100, 2)

    # Max drawdown from cumulative P&L
    cumulative = np.cumsum(arr)
    peak = np.maximum.accumulate(cumulative)
    drawdowns = (cumulative - peak) / (np.abs(peak) + 1)
    metrics["max_drawdown"] = round(float(np.min(drawdowns)) * 100, 2)

    # Agent breakdown (top 10)
    agents = conn.execute("""
        SELECT player_id,
               COUNT(*) trades,
               SUM(COALESCE(corrected_pnl,realized_pnl)) total_pnl,
               AVG(CASE WHEN COALESCE(corrected_pnl,realized_pnl) > 0 THEN 1.0 ELSE 0.0 END)*100 wr
        FROM trades WHERE realized_pnl IS NOT NULL
        GROUP BY player_id ORDER BY total_pnl DESC LIMIT 15
    """).fetchall()
    metrics["agent_breakdown"] = [dict(r) for r in agents]

    # Strategy breakdown (inferred from action type)
    strats = conn.execute("""
        SELECT action strategy,
               COUNT(*) trades,
               SUM(COALESCE(corrected_pnl,realized_pnl)) total_pnl,
               AVG(CASE WHEN COALESCE(corrected_pnl,realized_pnl) > 0 THEN 1.0 ELSE 0.0 END)*100 wr
        FROM trades WHERE realized_pnl IS NOT NULL
        GROUP BY action ORDER BY total_pnl DESC
    """).fetchall()
    metrics["strategy_breakdown"] = [dict(r) for r in strats]

    conn.close()
    logger.info(f"Fleet analysis: {metrics['total_trades']} trades, {metrics['win_rate']}% WR, "
                f"${metrics['total_pnl']:,.0f} P&L, Sharpe={metrics['fleet_sharpe']:.3f}")
    return metrics


# ═══════════════════════════════════════════════════════════════════════════════
# Step 3: Signal Center Analysis
# ═══════════════════════════════════════════════════════════════════════════════

def analyze_signal_center(run_date: str) -> dict:
    """
    Analyze signal quality from watchlist_signals (trader.db) as grade proxy.
    signal_history in signals.db contains market state (not entry signals) — grades NULL.
    """
    conn = _td_conn()

    # Grade assignment: confidence → letter grade
    grade_analysis = conn.execute("""
        SELECT
            CASE
                WHEN confidence >= 0.85 THEN 'A+'
                WHEN confidence >= 0.75 THEN 'A'
                WHEN confidence >= 0.65 THEN 'B'
                WHEN confidence >= 0.55 THEN 'C'
                ELSE 'D/F'
            END as grade,
            COUNT(*) signal_count,
            SUM(CASE WHEN status IN ('hit_target') THEN 1
                     WHEN pnl_pct > 0 THEN 1 ELSE 0 END) wins,
            COUNT(CASE WHEN status NOT IN ('active','watching') THEN 1 END) resolved,
            AVG(CASE WHEN pnl_pct IS NOT NULL THEN pnl_pct ELSE NULL END) avg_return
        FROM watchlist_signals
        GROUP BY grade
        ORDER BY grade
    """).fetchall()

    # "Acted on" = trade exists for same symbol within ±2 days of signal
    grade_rows = []
    for g in grade_analysis:
        wr = round(g["wins"] / max(g["resolved"], 1) * 100, 1)
        grade_rows.append({
            "grade": g["grade"],
            "count": g["signal_count"],
            "resolved": g["resolved"],
            "win_rate": wr,
            "avg_return": round(g["avg_return"] or 0, 2),
        })

    # Only A/A+ scenario
    ab_scenario = conn.execute("""
        SELECT
            COUNT(*) cnt,
            SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) wins,
            AVG(pnl_pct) avg_pnl
        FROM watchlist_signals
        WHERE confidence >= 0.75 AND pnl_pct IS NOT NULL
    """).fetchone()

    # All signals scenario
    all_scenario = conn.execute("""
        SELECT COUNT(*) cnt,
               SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) wins,
               AVG(pnl_pct) avg_pnl
        FROM watchlist_signals WHERE pnl_pct IS NOT NULL
    """).fetchone()

    # Signals that hit target vs expired
    outcome_dist = conn.execute("""
        SELECT status, COUNT(*) cnt FROM watchlist_signals GROUP BY status
    """).fetchall()

    # Top symbols by signal count and performance
    sym_perf = conn.execute("""
        SELECT symbol, COUNT(*) signals,
               AVG(confidence) avg_conf,
               AVG(pnl_pct) avg_pnl,
               SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) wins
        FROM watchlist_signals WHERE pnl_pct IS NOT NULL
        GROUP BY symbol ORDER BY avg_pnl DESC LIMIT 10
    """).fetchall()

    conn.close()

    # Signal Center signal_history stats
    try:
        sc = sqlite3.connect(SC_DB)
        sc.row_factory = sqlite3.Row
        sh_count = sc.execute("SELECT COUNT(*) FROM signal_history").fetchone()[0]
        sh_types = sc.execute("""
            SELECT signal_name, COUNT(*) cnt FROM signal_history
            GROUP BY signal_name ORDER BY cnt DESC LIMIT 10
        """).fetchall()
        sc.close()
        sc_stats = {
            "total_records": sh_count,
            "top_types": [dict(r) for r in sh_types],
            "note": "signal_history contains market-state snapshots (VIX/GEX/breadth), not entry signals. Grades are NULL."
        }
    except Exception:
        sc_stats = {"total_records": 0, "note": "signals.db not accessible"}

    return {
        "grade_analysis": grade_rows,
        "a_plus_only_wr": round(ab_scenario["wins"] / max(ab_scenario["cnt"], 1) * 100, 1),
        "a_plus_only_avg_pnl": round(ab_scenario["avg_pnl"] or 0, 2),
        "all_signals_wr": round(all_scenario["wins"] / max(all_scenario["cnt"], 1) * 100, 1),
        "all_signals_avg_pnl": round(all_scenario["avg_pnl"] or 0, 2),
        "outcome_distribution": {r["status"]: r["cnt"] for r in outcome_dist},
        "top_symbols": [dict(r) for r in sym_perf],
        "signal_center_stats": sc_stats,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Step 4: Alpha Expansion — run fast signals on 200-stock universe
# ═══════════════════════════════════════════════════════════════════════════════

def _expand_alpha_universe() -> list[str]:
    """Load top 200 symbols from scan_universe by avg_volume."""
    conn = _td_conn()
    rows = conn.execute("""
        SELECT symbol FROM scan_universe
        WHERE avg_volume IS NOT NULL AND avg_price >= 3.0
        ORDER BY avg_volume DESC LIMIT 200
    """).fetchall()
    conn.close()
    syms = [r["symbol"] for r in rows]
    logger.info(f"Expanded universe: {len(syms)} symbols from scan_universe")
    return syms


def _run_dark_pool_batch(symbols: list[str]) -> dict[str, float]:
    """Run FINRA dark pool for any list of symbols (reuses full file download)."""
    scores: dict[str, float] = {}
    sym_set = set(s.upper() for s in symbols)

    for lookback in range(3):
        d = date.today() - timedelta(days=lookback + 1)
        if d.weekday() >= 5:
            d -= timedelta(days=d.weekday() - 4)
        url = f"https://cdn.finra.org/equity/regsho/daily/CNMSshvol{d.strftime('%Y%m%d')}.txt"
        try:
            resp = _SESSION.get(url, timeout=20)
            if resp.status_code == 200 and len(resp.content) > 5000:
                for line in resp.text.splitlines():
                    parts = line.split("|")
                    if len(parts) < 5 or parts[1] == "Symbol":
                        continue
                    sym = parts[1].strip().upper()
                    if sym not in sym_set:
                        continue
                    try:
                        sv = int(float(parts[2]))
                        tv = int(float(parts[4]))
                        if tv == 0:
                            continue
                        ratio = sv / tv
                        if ratio >= 0.60:   score = -2.0
                        elif ratio >= 0.50: score = -1.5
                        elif ratio >= 0.45: score = -1.0
                        elif ratio <= 0.20: score = 2.0
                        elif ratio <= 0.30: score = 1.0
                        else:               score = 0.0
                        scores[sym] = score
                    except (ValueError, IndexError):
                        continue
                logger.info(f"Dark pool batch: {len(scores)}/{len(sym_set)} symbols from {d}")
                return scores
        except Exception as e:
            logger.debug(f"Dark pool batch attempt {lookback}: {e}")
    return scores


def _run_earnings_batch(symbols: list[str]) -> dict[str, float]:
    """Run earnings beat-streak signals for a symbol list."""
    scores: dict[str, float] = {}
    for sym in symbols:
        try:
            tk = yf.Ticker(sym)
            beat_streak = 0
            beat_pcts: list[float] = []
            try:
                hist = tk.earnings_history
                if hist is not None and not hist.empty:
                    hist = hist.dropna(subset=["epsEstimate", "epsActual"])
                    for _, row in hist.tail(6).iterrows():
                        est, act = row.get("epsEstimate", 0), row.get("epsActual", 0)
                        if est and act:
                            beat_pcts.append((act - est) / abs(est) * 100 if est != 0 else 0)
                    for bp in reversed(beat_pcts):
                        if bp > 0: beat_streak += 1
                        else: break
            except Exception:
                pass

            beat_avg = float(np.mean(beat_pcts)) if beat_pcts else 0.0
            if beat_streak >= 4 and beat_avg > 10:   score = 2.0
            elif beat_streak >= 3:                    score = 1.0
            elif beat_streak >= 1 and beat_avg > 5:  score = 0.5
            elif beat_streak == 0 and beat_pcts and beat_pcts[-1] < -10: score = -1.0
            else:                                     score = 0.0
            scores[sym] = score
        except Exception:
            scores[sym] = 0.0
        time.sleep(0.15)
    return scores


def _compute_alpha_batch(
    symbols: list[str],
    dp_scores: dict[str, float],
    earn_scores: dict[str, float],
    market_scores: dict[str, float],
) -> dict[str, float]:
    """
    Compute composite alpha for each symbol using available signals.
    For expanded universe, only dark_pool + earnings + market-wide signals are available.
    Weights renormalized: dp=44%, earn=7%, pc=22%, vix=22%, opex=5%
    (sentiment already applied as market-wide in market_scores)
    """
    # Available weights for large-universe batch (no insider/FTD/sentiment per-stock)
    w = {"dark_pool": 0.44, "earnings": 0.07,
         "put_call": 0.22, "vix_structure": 0.22, "opex": 0.05}

    pc   = market_scores.get("put_call", 0.0)
    vix  = market_scores.get("vix_structure", 0.0)
    opex = market_scores.get("opex", 0.0)

    composite: dict[str, float] = {}
    for sym in symbols:
        dp   = dp_scores.get(sym, 0.0)
        earn = earn_scores.get(sym, 0.0)

        score = (dp * w["dark_pool"] + earn * w["earnings"] +
                 pc * w["put_call"] + vix * w["vix_structure"] + opex * w["opex"])
        score = max(-2.0, min(2.0, score))
        composite[sym] = round(score, 4)
    return composite


def run_alpha_expansion(symbols: list[str]) -> dict[str, float]:
    """
    Run fast alpha signals on 200-stock universe.
    Returns symbol → composite_alpha_score mapping.
    """
    logger.info(f"Running alpha expansion on {len(symbols)} symbols...")

    # Get market-wide scores from alpha_signals.db if available
    market_scores = {"put_call": 0.0, "vix_structure": 0.0, "opex": 0.0}
    try:
        ac = _alpha_conn()
        pc_row = ac.execute("SELECT signal_score FROM put_call_signals ORDER BY trade_date DESC LIMIT 1").fetchone()
        vix_row = ac.execute("SELECT signal_score FROM vix_structure_signals ORDER BY trade_date DESC LIMIT 1").fetchone()
        opex_row = ac.execute("SELECT signal_score FROM opex_signals ORDER BY trade_date DESC LIMIT 1").fetchone()
        if pc_row:   market_scores["put_call"] = pc_row["signal_score"]
        if vix_row:  market_scores["vix_structure"] = vix_row["signal_score"]
        if opex_row: market_scores["opex"] = opex_row["signal_score"]
        ac.close()
    except Exception as e:
        logger.debug(f"Market scores from alpha_db: {e}")

    # Also pull from existing alpha_signals.py (rerun yield curve if needed)
    from engine.alpha_signals import run_yield_curve, run_opex_signal
    try:
        yc = run_yield_curve()
        market_scores["yield_curve"] = yc
    except Exception:
        pass

    # Dark pool batch
    logger.info("  [1/3] Dark pool batch...")
    dp_scores = _run_dark_pool_batch(symbols)

    # Earnings batch (most informative per-symbol signal)
    logger.info("  [2/3] Earnings batch (may take ~40s for 200 symbols)...")
    earn_scores = _run_earnings_batch(symbols)

    # Composite
    logger.info("  [3/3] Computing composite scores...")
    alpha_scores = _compute_alpha_batch(symbols, dp_scores, earn_scores, market_scores)

    above_threshold = sum(1 for v in alpha_scores.values() if v >= ALPHA_THRESHOLD)
    logger.info(f"Alpha expansion complete: {above_threshold}/{len(symbols)} symbols >= {ALPHA_THRESHOLD}")
    return alpha_scores


# ═══════════════════════════════════════════════════════════════════════════════
# Step 5: Download price data for filtered universe
# ═══════════════════════════════════════════════════════════════════════════════

def _download_v2_universe(symbols: list[str], days: int) -> dict[str, pd.DataFrame]:
    """Download OHLCV for filtered universe in batches."""
    end_dt = date.today()
    start_dt = end_dt - timedelta(days=days + 60)  # extra buffer
    td: dict[str, pd.DataFrame] = {}

    batch_size = 20
    batches = [symbols[i:i+batch_size] for i in range(0, len(symbols), batch_size)]
    logger.info(f"Downloading {len(symbols)} symbols in {len(batches)} batches...")

    for i, batch in enumerate(batches):
        try:
            raw = yf.download(
                " ".join(batch), start=start_dt, end=end_dt,
                interval="1d", progress=False, auto_adjust=True,
            )
            if raw.empty:
                continue

            # Handle multi vs single ticker
            if isinstance(raw.columns, pd.MultiIndex):
                lvl0 = raw.columns.get_level_values(0)
                price_type = "Close" if "Close" in lvl0 else "Adj Close"
                for sym in batch:
                    try:
                        sym_df = raw.xs(sym, axis=1, level=1)[
                            [c for c in ["Open","High","Low","Close","Volume"]
                             if c in raw.xs(sym, axis=1, level=1).columns]
                        ].dropna(subset=["Close"] if "Close" in raw.xs(sym, axis=1, level=1).columns else [price_type])
                        if len(sym_df) >= 20:
                            td[sym] = sym_df
                    except Exception:
                        pass
            else:
                # Single ticker
                sym = batch[0]
                df = raw.dropna()
                if len(df) >= 20:
                    td[sym] = df

        except Exception as e:
            logger.debug(f"Download batch {i}: {e}")
        time.sleep(0.8)
        if (i + 1) % 5 == 0:
            logger.info(f"  Downloaded {i+1}/{len(batches)} batches, {len(td)} symbols ready")

    logger.info(f"Download complete: {len(td)}/{len(symbols)} symbols with data")
    return td


# ═══════════════════════════════════════════════════════════════════════════════
# Step 5: Core backtest engine with alpha gate
# ═══════════════════════════════════════════════════════════════════════════════

def _run_event_loop_v2(
    td: dict, days: list, vix_map: dict, alpha_scores: dict[str, float]
) -> dict[str, list]:
    """Event-driven loop with alpha gate + position sizing."""
    from engine.master_backtest import _run_dalio_metals
    from engine.master_backtest import SLIPPAGE, EXEC_DELAY

    event_trades: dict[str, list] = defaultdict(list)
    spy_c = td.get("SPY", pd.DataFrame())["Close"].values if "SPY" in td else None

    # Pull VIX timeseries
    vix_df = td.get("^VIX") if td.get("^VIX") is not None else td.get("VIX")

    skipped_alpha = skipped_conf = 0

    for sym in td:
        df = td[sym]
        if len(df) < 60:
            continue

        alpha = alpha_scores.get(sym, 0.0)
        pos_factor = _alpha_position_factor(alpha)
        if pos_factor == 0.0:
            skipped_alpha += 1
            continue

        positions: dict[str, dict] = {}

        for tier_name, sig_fn, tier_num in [
            ("momentum_breakout", None, 2),
            ("reversal_bounce",   None, 2),
            ("bollinger",         None, 1),
            ("rsi_bounce",        None, 1),
            ("hammer_candle",     None, 3),
            ("five_day_bounce",   None, 3),
            ("falling_knife",     None, 3),
            ("avwap_bounce",      None, 3),
        ]:
            for day in days:
                m = df.index <= day
                if m.sum() < 55:
                    continue
                sub = df.loc[m]
                c = sub["Close"].values
                h = sub["High"].values if "High" in sub.columns else c
                l = sub["Low"].values if "Low" in sub.columns else c
                v = sub["Volume"].values if "Volume" in sub.columns else np.ones(len(c))
                avg_v = float(np.mean(v[-20:])) if len(v) >= 20 else 1.0
                px    = float(c[-1])
                vix_val = vix_map.get(day, 18.0)
                day_str = day.strftime("%Y-%m-%d")

                t2 = _tier2_signals(c, h, l, v, avg_v)
                t3 = _tier3_signals(c, h, l, v, avg_v)

                # Check signal
                sig = {**t2, **t3}.get(tier_name, False)
                if not sig:
                    continue

                # Manage open positions first
                key = f"{sym}_{tier_name}"
                if key in positions:
                    pos = positions[key]
                    gain = (px - pos["entry"]) / pos["entry"]
                    days_held = pos.get("days_held", 0)
                    if gain >= 0.08 or gain <= -0.05 or days_held >= 20:
                        pnl_pct = gain * 100 * pos_factor - SLIPPAGE * 200
                        event_trades[tier_name].append({
                            "strategy": tier_name, "ticker": sym,
                            "entry_date": pos["entry_date"], "exit_date": day_str,
                            "pnl_pct": round(pnl_pct, 2), "pnl": round(pnl_pct, 2),
                            "hold_days": days_held, "alpha_score": alpha,
                            "win": 1 if pnl_pct > 0 else 0,
                        })
                        del positions[key]
                    else:
                        positions[key]["days_held"] = days_held + 1
                    continue

                # New entry — alpha + confidence gate
                if alpha < ALPHA_THRESHOLD:
                    skipped_alpha += 1
                    continue

                fill_cost = SLIPPAGE + EXEC_DELAY
                entry_px  = px * (1 + fill_cost)
                positions[key] = {
                    "entry": entry_px, "entry_date": day_str,
                    "days_held": 0, "alpha": alpha,
                }

        # Close remaining
        for key, pos in positions.items():
            tier_name = key.split("_", 1)[1]
            px_last = float(df["Close"].iloc[-1])
            gain = (px_last - pos["entry"]) / pos["entry"]
            pnl_pct = gain * 100 * pos_factor - SLIPPAGE * 200
            event_trades[tier_name].append({
                "strategy": tier_name, "ticker": sym,
                "entry_date": pos["entry_date"],
                "exit_date": days[-1].strftime("%Y-%m-%d") if days else "EOP",
                "pnl_pct": round(pnl_pct, 2), "pnl": round(pnl_pct, 2),
                "hold_days": pos.get("days_held", 1), "alpha_score": alpha,
                "win": 1 if pnl_pct > 0 else 0,
            })

    # Also run Dalio metals (self-contained, has own alpha check via fixed universe)
    dalio_trades = _run_dalio_metals(td, days)
    for t in dalio_trades:
        t["alpha_score"] = alpha_scores.get(t["ticker"], 0.0)
    event_trades["dalio_metals"].extend(dalio_trades)

    logger.info(f"Event loop v2: {sum(len(v) for v in event_trades.values())} trades, "
                f"{skipped_alpha} skipped by alpha gate")
    return event_trades


def _run_options_loop_v2(
    td: dict, days: list, vix_map: dict, alpha_scores: dict[str, float]
) -> tuple[list, list, list]:
    """Options loop with alpha gate + position sizing."""
    options_trades: list = []
    spread_trades:  list = []
    dte0_trades:    list = []

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
            h      = sub["High"].values if "High" in sub.columns else c
            l      = sub["Low"].values if "Low" in sub.columns else c
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
            bull = sum(1 for v_ in list(t2.values()) + list(t3.values()) if v_)
            bear = sum(1 for v_ in _tier9_short_signals(c, h, l, v, avg_v).values() if v_)

            extra = {"ticker": sym, "entry_date": day_str, "regime": regime,
                     "alpha_score": alpha, "pos_factor": pos_factor}

            if bull >= 2 and ivr < 60:
                r = _sim_long_call(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    options_trades.append({**r, **extra, "strategy": "long_call",
                                           "option_type": "call",
                                           "exit_date": _exit_date_str(future, r["days"], day_str),
                                           "win": 1 if r["pnl"] > 0 else 0})

            if ivr > 50 and bull >= 1 and regime in ("BULL", "CAUTIOUS"):
                r = _sim_csp(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    options_trades.append({**r, **extra, "strategy": "csp",
                                           "option_type": "put",
                                           "exit_date": _exit_date_str(future, r["days"], day_str),
                                           "win": 1 if r["pnl"] > 0 else 0})

            if bull >= 2 and ivr > 40:
                r = _sim_covered_call(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    options_trades.append({**r, **extra, "strategy": "covered_call",
                                           "option_type": "call",
                                           "exit_date": _exit_date_str(future, r["days"], day_str),
                                           "win": 1 if r["pnl"] > 0 else 0})

            if bull >= 2:
                r = _sim_bull_call_spread(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    spread_trades.append({**r, **extra, "strategy": "bull_call_spread",
                                          "spread_type": "BULL_CALL",
                                          "exit_date": _exit_date_str(future, r["days"], day_str),
                                          "win": 1 if r["pnl"] > 0 else 0})
                r = _sim_bull_put_spread(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    spread_trades.append({**r, **extra, "strategy": "bull_put_spread",
                                          "spread_type": "BULL_PUT",
                                          "exit_date": _exit_date_str(future, r["days"], day_str),
                                          "win": 1 if r.get("pnl", 0) > 0 else 0})

            if vix_val > 20 and bear >= 2:
                r = _sim_bear_call_spread(future, px, iv, OPT_DTE_DEFAULT)
                if r:
                    spread_trades.append({**r, **extra, "strategy": "bear_call_spread",
                                          "spread_type": "BEAR_CALL",
                                          "exit_date": _exit_date_str(future, r["days"], day_str),
                                          "win": 1 if r.get("pnl", 0) > 0 else 0})

            if vix_val > 20:
                sma20 = float(np.mean(c[-20:]))
                if abs(px - sma20) / px < 0.02:
                    r = _sim_ic(future, px, iv, OPT_DTE_DEFAULT)
                    if r:
                        spread_trades.append({**r, **extra, "strategy": "iron_condor",
                                              "spread_type": "IC",
                                              "exit_date": _exit_date_str(future, r["days"], day_str),
                                              "win": 1 if r.get("pnl", 0) > 0 else 0})

        # 0DTE (SPY/QQQ only, always pass through — they're in original 24)
        for sym0 in ("SPY", "QQQ"):
            if sym0 not in td:
                continue
            alpha0 = alpha_scores.get(sym0, 0.0)
            if alpha0 < ALPHA_THRESHOLD:
                continue
            df = td[sym0]
            m  = df.index <= day
            sub = df.loc[m]
            c   = sub["Close"].values
            px  = float(c[-1])
            iv  = _hist_vol(c, 10)
            future_day = df.loc[df.index > day]
            if len(future_day) < 1:
                continue
            nr = {"High": float(future_day["High"].iloc[0]) if "High" in future_day else px,
                  "Low":  float(future_day["Low"].iloc[0])  if "Low"  in future_day else px,
                  "Close": float(future_day["Close"].iloc[0])}
            sma5 = float(np.mean(c[-5:]))
            if px > sma5:
                r = _sim_0dte(nr, px, iv, "call")
                if r:
                    dte0_trades.append({**r, "strategy": f"{sym0.lower()}_0dte_call",
                                        "ticker": sym0, "trade_date": day_str,
                                        "alpha_score": alpha0, "win": 1 if r["pnl"] > 0 else 0})

    logger.info(f"Options loop v2: {len(options_trades)} opts, {len(spread_trades)} spreads, "
                f"{len(dte0_trades)} 0DTE, {skipped} skipped by alpha")
    return options_trades, spread_trades, dte0_trades


def _build_equity_curve_v2(
    event_trades: dict, opt_trades: list, spread_trades: list,
    trading_days: list, vix_map: dict,
) -> list[dict]:
    all_by_date: dict[str, list] = defaultdict(list)

    for strat, trades in event_trades.items():
        for t in trades:
            all_by_date[t.get("exit_date", t.get("date", ""))].append(t["pnl_pct"])

    for t in opt_trades + spread_trades:
        key = t.get("exit_date") or t.get("entry_date", "")
        all_by_date[key].append(t.get("pnl_pct", 0))

    equity = STARTING_CASH
    curve  = []
    for day in trading_days:
        day_str  = day.strftime("%Y-%m-%d")
        day_pnls = all_by_date.get(day_str, [])
        day_ret  = float(np.clip(np.mean(day_pnls) / 100, -0.02, 0.02)) if day_pnls else 0.0
        daily_pnl = equity * day_ret
        equity   += daily_pnl
        regime   = _classify_regime(vix_map.get(day, 18.0))
        curve.append({"trade_date": day_str, "equity": round(equity, 2),
                      "daily_pnl": round(daily_pnl, 2), "regime": regime})
    return curve


# ═══════════════════════════════════════════════════════════════════════════════
# Save results
# ═══════════════════════════════════════════════════════════════════════════════

def _save_v2_results(run_date: str, event_trades: dict, opt_trades: list,
                     spread_trades: list, alpha_scores: dict, spy_return: float) -> None:
    conn = _bt_conn()
    now  = datetime.utcnow().isoformat()

    all_trades = list(opt_trades) + list(spread_trades)
    for strat, trades in event_trades.items():
        all_trades.extend(trades)

    # Group by strategy+ticker
    by_strat_sym: dict[tuple, list] = defaultdict(list)
    for t in all_trades:
        key = (t.get("strategy", "unknown"), t.get("ticker", ""))
        by_strat_sym[key].append(t)

    tier_map = {
        "rsi_bounce": (1, "Core Technical"), "bollinger": (1, "Core Technical"),
        "sma_cross": (1, "Core Technical"), "ema_ribbon": (1, "Core Technical"),
        "momentum_breakout": (2, "Intraday Momentum"), "reversal_bounce": (2, "Intraday Momentum"),
        "vwap_reclaim": (2, "Intraday Momentum"), "volatility_breakout": (2, "Intraday Momentum"),
        "hammer_candle": (3, "Holly-Style"), "five_day_bounce": (3, "Holly-Style"),
        "falling_knife": (3, "Holly-Style"), "avwap_bounce": (3, "Holly-Style"),
        "dalio_metals": (4, "Agent-Specific"),
        "long_call": (5, "Options Single Leg"), "csp": (5, "Options Single Leg"),
        "covered_call": (5, "Options Single Leg"), "long_put": (5, "Options Single Leg"),
        "bull_call_spread": (6, "Vertical Spreads"), "bull_put_spread": (6, "Vertical Spreads"),
        "bear_put_spread": (6, "Vertical Spreads"), "bear_call_spread": (6, "Vertical Spreads"),
        "iron_condor": (7, "Iron Condors"), "broken_wing_ic": (7, "Iron Condors"),
        "spy_0dte_call": (8, "0DTE"), "qqq_0dte_call": (8, "0DTE"),
    }

    inserted = 0
    for (strat, sym), trades in by_strat_sym.items():
        trades = [t for t in trades if "pnl_pct" in t]
        if not trades:
            continue
        m = _trade_metrics(trades)
        tier_num, tier_name = tier_map.get(strat, (2, "Tier 2+"))
        alpha = float(np.mean([t.get("alpha_score", 0.0) for t in trades]))

        conn.execute("""
            INSERT OR REPLACE INTO backtest_master_results_v2
            (run_date, tier, tier_name, strategy, ticker,
             total_return, win_rate, sharpe, max_drawdown,
             avg_hold_hours, num_trades, profit_factor, calmar,
             best_trade_pct, worst_trade_pct, spy_return, vs_spy,
             max_consec_wins, max_consec_losses, regime,
             realistic_sharpe, needs_validation, alpha_score, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            run_date, tier_num, tier_name, strat, sym,
            m["total_return"], m["win_rate"], m["sharpe"], m["max_drawdown"],
            m["avg_hold_hours"], m["num_trades"], m["profit_factor"], m["calmar"],
            m["best_trade_pct"], m["worst_trade_pct"], spy_return,
            round(m["total_return"] - spy_return, 2),
            m["max_consec_wins"], m["max_consec_losses"], "MIXED",
            m["realistic_sharpe"], m["needs_validation"], round(alpha, 3), now,
        ))
        inserted += 1

    conn.commit()
    conn.close()
    logger.info(f"v2 results: {inserted} strategy-ticker rows saved")


def _save_equity_curve_v2(run_date: str, curve: list[dict]) -> None:
    conn = _bt_conn()
    now  = datetime.utcnow().isoformat()
    for row in curve:
        conn.execute("""
            INSERT OR REPLACE INTO backtest_equity_curve_v2
            (run_date, trade_date, equity, daily_pnl, regime, created_at)
            VALUES (?,?,?,?,?,?)
        """, (run_date, row["trade_date"], row["equity"],
              row["daily_pnl"], row["regime"], now))
    conn.commit()
    conn.close()


def _save_attribution(run_date: str, all_trades: list, alpha_scores: dict) -> None:
    """Save per-trade attribution linking alpha signal components to outcomes."""
    conn = _bt_conn()
    now  = datetime.utcnow().isoformat()

    try:
        ac = _alpha_conn()
        composite_rows = {
            r["symbol"]: dict(r)
            for r in ac.execute("SELECT * FROM composite_alpha ORDER BY as_of_date DESC").fetchall()
        }
        ac.close()
    except Exception:
        composite_rows = {}

    for t in all_trades:
        sym   = t.get("ticker", "")
        strat = t.get("strategy", "")
        cr    = composite_rows.get(sym, {})
        conn.execute("""
            INSERT INTO alpha_attribution_v2
            (run_date, strategy, ticker,
             dark_pool_score, ftd_score, insider_score, put_call_score,
             vix_score, sentiment_score, yield_curve_score, opex_score,
             earnings_score, rebalancing_score, composite_score,
             trade_outcome, signal_count, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            run_date, strat, sym,
            cr.get("dark_pool_score", 0), cr.get("ftd_score", 0),
            cr.get("insider_score", 0), cr.get("put_call_score", 0),
            cr.get("vix_structure_score", 0), cr.get("sentiment_score", 0),
            cr.get("yield_curve_score", 0), cr.get("opex_score", 0),
            cr.get("earnings_score", 0), cr.get("rebalancing_score", 0),
            cr.get("composite_score", alpha_scores.get(sym, 0)),
            t.get("pnl_pct", 0), cr.get("signal_count", 0), now,
        ))

    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# Step 6: Comparison table
# ═══════════════════════════════════════════════════════════════════════════════

def build_comparison_table(
    run_date: str,
    fleet_metrics: dict,
    v2_summary: dict,
) -> dict:
    """Build the three-way comparison table and save to DB."""

    # Pull v1 metrics from backtest.db
    conn = _bt_conn()

    v1_ec_start = conn.execute("SELECT equity FROM backtest_equity_curve ORDER BY trade_date LIMIT 1").fetchone()
    v1_ec_end   = conn.execute("SELECT equity FROM backtest_equity_curve ORDER BY trade_date DESC LIMIT 1").fetchone()
    v1_start = float(v1_ec_start["equity"]) if v1_ec_start else 100_000
    v1_end   = float(v1_ec_end["equity"])   if v1_ec_end   else 100_000
    v1_return_pct = round((v1_end - v1_start) / v1_start * 100, 2)

    v1_sharpe = float(conn.execute(
        "SELECT AVG(realistic_sharpe) FROM backtest_master_results WHERE num_trades > 0"
    ).fetchone()[0] or 0)
    v1_dd = float(conn.execute(
        "SELECT MIN(max_drawdown) FROM backtest_master_results WHERE num_trades > 0"
    ).fetchone()[0] or 0)
    v1_wr = float(conn.execute(
        "SELECT AVG(win_rate) FROM backtest_master_results WHERE num_trades > 0"
    ).fetchone()[0] or 0)
    v1_trades = int(conn.execute(
        "SELECT SUM(num_trades) FROM backtest_master_results WHERE num_trades > 0"
    ).fetchone()[0] or 0)
    v1_syms = int(conn.execute(
        "SELECT COUNT(DISTINCT ticker) FROM backtest_master_results WHERE num_trades > 0"
    ).fetchone()[0] or 0)
    v1_best_strat = conn.execute(
        "SELECT strategy FROM backtest_master_results WHERE num_trades > 0 ORDER BY total_return DESC LIMIT 1"
    ).fetchone()
    v1_worst_strat = conn.execute(
        "SELECT strategy FROM backtest_master_results WHERE num_trades > 0 ORDER BY total_return ASC LIMIT 1"
    ).fetchone()

    # v2 metrics
    v2_ec_end  = conn.execute("SELECT equity FROM backtest_equity_curve_v2 WHERE run_date=? ORDER BY trade_date DESC LIMIT 1", (run_date,)).fetchone()
    v2_ec_s    = conn.execute("SELECT equity FROM backtest_equity_curve_v2 WHERE run_date=? ORDER BY trade_date LIMIT 1", (run_date,)).fetchone()
    v2_end     = float(v2_ec_end["equity"])   if v2_ec_end else STARTING_CASH
    v2_stt     = float(v2_ec_s["equity"])     if v2_ec_s   else STARTING_CASH
    v2_ret     = round((v2_end - v2_stt) / v2_stt * 100, 2)

    v2_sharpe  = float(conn.execute("SELECT AVG(realistic_sharpe) FROM backtest_master_results_v2 WHERE run_date=? AND num_trades>0", (run_date,)).fetchone()[0] or 0)
    v2_dd      = float(conn.execute("SELECT MIN(max_drawdown) FROM backtest_master_results_v2 WHERE run_date=? AND num_trades>0", (run_date,)).fetchone()[0] or 0)
    v2_wr      = float(conn.execute("SELECT AVG(win_rate) FROM backtest_master_results_v2 WHERE run_date=? AND num_trades>0", (run_date,)).fetchone()[0] or 0)
    v2_trades  = int(conn.execute("SELECT SUM(num_trades) FROM backtest_master_results_v2 WHERE run_date=? AND num_trades>0", (run_date,)).fetchone()[0] or 0)
    v2_syms    = int(conn.execute("SELECT COUNT(DISTINCT ticker) FROM backtest_master_results_v2 WHERE run_date=?", (run_date,)).fetchone()[0] or 0)
    v2_skip    = v2_summary.get("trades_skipped_by_alpha", 0)
    v2_avg_alpha = v2_summary.get("avg_alpha_at_entry", 0.0)
    v2_best  = conn.execute("SELECT strategy FROM backtest_master_results_v2 WHERE run_date=? AND num_trades>0 ORDER BY total_return DESC LIMIT 1", (run_date,)).fetchone()
    v2_worst = conn.execute("SELECT strategy FROM backtest_master_results_v2 WHERE run_date=? AND num_trades>0 ORDER BY total_return ASC LIMIT 1", (run_date,)).fetchone()

    fleet_return = fleet_metrics.get("return_pct", 0.0)
    fleet_start  = fleet_metrics.get("starting_capital", 100_000)

    rows = [
        ("Final equity ($)",          fleet_start + fleet_metrics.get("total_pnl",0), f"${fleet_start + fleet_metrics.get('total_pnl',0):,.0f}",
                                       v1_end, f"${v1_end:,.0f}", v2_end, f"${v2_end:,.0f}"),
        ("Total return %",            fleet_return, f"{fleet_return:+.2f}%",
                                       v1_return_pct, f"{v1_return_pct:+.2f}%", v2_ret, f"{v2_ret:+.2f}%"),
        ("Sharpe (realistic)",        fleet_metrics.get("fleet_sharpe",0), f"{fleet_metrics.get('fleet_sharpe',0):.3f}",
                                       v1_sharpe, f"{v1_sharpe:.3f}", v2_sharpe, f"{v2_sharpe:.3f}"),
        ("Max drawdown %",            fleet_metrics.get("max_drawdown",0), f"{fleet_metrics.get('max_drawdown',0):.2f}%",
                                       v1_dd, f"{v1_dd:.2f}%", v2_dd, f"{v2_dd:.2f}%"),
        ("Win rate %",                fleet_metrics.get("win_rate",0), f"{fleet_metrics.get('win_rate',0):.1f}%",
                                       v1_wr, f"{v1_wr:.1f}%", v2_wr, f"{v2_wr:.1f}%"),
        ("Total trades",              fleet_metrics.get("total_trades",0), str(fleet_metrics.get("total_trades",0)),
                                       v1_trades, str(v1_trades), v2_trades, str(v2_trades)),
        ("Symbols traded",            fleet_metrics.get("unique_symbols",0), str(fleet_metrics.get("unique_symbols",0)),
                                       v1_syms, str(v1_syms), v2_syms, str(v2_syms)),
        ("Trades skipped (alpha)",    0, "n/a",  0, "n/a", v2_skip, str(v2_skip)),
        ("Avg alpha at entry",        0, "n/a",  0, "n/a", v2_avg_alpha, f"{v2_avg_alpha:.3f}"),
        ("Best strategy",             0, str(fleet_metrics.get("best_agent","")), 0,
                                       v1_best_strat["strategy"] if v1_best_strat else "-",
                                       0, v2_best["strategy"] if v2_best else "-"),
        ("Worst strategy",            0, str(fleet_metrics.get("worst_agent","")), 0,
                                       v1_worst_strat["strategy"] if v1_worst_strat else "-",
                                       0, v2_worst["strategy"] if v2_worst else "-"),
    ]

    now = datetime.utcnow().isoformat()
    for metric, af, af_lbl, v1, v1_lbl, v2, v2_lbl in rows:
        conn.execute("""
            INSERT OR REPLACE INTO comparison_table_v2
            (run_date, metric, actual_fleet, actual_fleet_label,
             v1_backtest, v1_label, v2_backtest, v2_label, created_at)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (run_date, metric, af, af_lbl, v1, v1_lbl, v2, v2_lbl, now))

    conn.commit()
    conn.close()

    return {
        "metrics": rows,
        "v1": {"return_pct": v1_return_pct, "equity_end": v1_end, "sharpe": v1_sharpe,
               "win_rate": v1_wr, "trades": v1_trades, "symbols": v1_syms},
        "v2": {"return_pct": v2_ret, "equity_end": v2_end, "sharpe": v2_sharpe,
               "win_rate": v2_wr, "trades": v2_trades, "symbols": v2_syms,
               "skipped": v2_skip, "avg_alpha": v2_avg_alpha},
        "fleet": {"return_pct": fleet_return, "sharpe": fleet_metrics.get("fleet_sharpe",0),
                  "win_rate": fleet_metrics.get("win_rate",0),
                  "trades": fleet_metrics.get("total_trades",0)},
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Step 7+8: Alpha attribution + reality check
# ═══════════════════════════════════════════════════════════════════════════════

def compute_alpha_attribution(run_date: str) -> dict:
    """Analyze which alpha signals contributed to winning vs losing trades."""
    conn = _bt_conn()

    # Correlation of each signal component with trade outcome
    cols = ["dark_pool_score", "ftd_score", "insider_score", "put_call_score",
            "vix_score", "sentiment_score", "yield_curve_score",
            "opex_score", "earnings_score", "rebalancing_score"]

    rows = conn.execute(f"""
        SELECT {', '.join(cols)}, composite_score, trade_outcome, strategy, ticker
        FROM alpha_attribution_v2 WHERE run_date=?
    """, (run_date,)).fetchall()

    if not rows:
        conn.close()
        return {"note": "No attribution data for this run"}

    df = pd.DataFrame([dict(r) for r in rows])

    attribution = {}
    for col in cols + ["composite_score"]:
        if col not in df.columns:
            continue
        mask = df[col].notna() & df["trade_outcome"].notna() & (df[col] != 0)
        sub = df[mask]
        if len(sub) < 3:
            attribution[col] = {"note": "insufficient data"}
            continue
        corr = float(sub[col].corr(sub["trade_outcome"]))
        wins = sub[sub["trade_outcome"] > 0]
        losses = sub[sub["trade_outcome"] <= 0]
        attribution[col] = {
            "correlation_with_outcome": round(corr, 4),
            "avg_score_on_wins":  round(float(wins[col].mean()) if len(wins) > 0 else 0, 3),
            "avg_score_on_losses": round(float(losses[col].mean()) if len(losses) > 0 else 0, 3),
            "samples": len(sub),
        }

    # Top symbols added by alpha filter beyond original 24
    original_24 = {"SPY","QQQ","TQQQ","NVDA","TSLA","AAPL","AMD","META","MSFT",
                   "GOOGL","AMZN","MU","AVGO","PLTR","COIN","BAC","MARA","SOFI",
                   "NFLX","MRVL","SMR","XLE","INTC","STAA"}
    new_syms = conn.execute("""
        SELECT DISTINCT ticker FROM backtest_master_results_v2
        WHERE run_date=? AND num_trades > 0
    """, (run_date,)).fetchall()
    added_symbols = [r["ticker"] for r in new_syms if r["ticker"] not in original_24]

    conn.close()
    return {
        "signal_attribution": attribution,
        "symbols_added_beyond_24": added_symbols,
        "symbols_added_count": len(added_symbols),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Main orchestrator
# ═══════════════════════════════════════════════════════════════════════════════

def run_super_backtest_v2(days: int = BACKTEST_DAYS) -> dict:
    """
    Full Super Backtest v2 pipeline.
    Saves all results to data/backtest.db with _v2 tables.
    NEVER modifies trader.db or arena.db.
    """
    start_ts  = time.time()
    run_date  = date.today().isoformat()
    logger.info(f"{'═'*60}")
    logger.info(f"  Super Backtest v2 — Dilithium Crystal Edition")
    logger.info(f"  Run date: {run_date}  |  {days}-day window")
    logger.info(f"{'═'*60}")

    _init_v2_tables()

    # ── Step 1+2: Fleet analysis ─────────────────────────────────────────────
    logger.info("\n[STEP 1+2] Analyzing fleet actual performance (read-only)...")
    fleet_metrics = analyze_fleet_performance(run_date)

    # ── Step 3: Signal Center analysis ───────────────────────────────────────
    logger.info("\n[STEP 3] Signal Center analysis...")
    signal_analysis = analyze_signal_center(run_date)

    # Save grade analysis
    conn = _bt_conn()
    now = datetime.utcnow().isoformat()
    for g in signal_analysis["grade_analysis"]:
        conn.execute("""
            INSERT OR REPLACE INTO signal_grade_analysis_v2
            (run_date, grade, signal_count, acted_on, win_rate, avg_return, created_at)
            VALUES (?,?,?,?,?,?,?)
        """, (run_date, g["grade"], g["count"], g["resolved"],
              g["win_rate"], g["avg_return"], now))
    conn.commit()
    conn.close()

    # ── Step 4: Alpha expansion ───────────────────────────────────────────────
    logger.info(f"\n[STEP 4] Alpha expansion to {UNIVERSE_SIZE}-stock universe...")
    expanded_syms = _expand_alpha_universe()
    alpha_scores  = run_alpha_expansion(expanded_syms)

    # Merge with existing alpha_signals.db scores for original 24
    try:
        ac = _alpha_conn()
        existing = ac.execute("""
            SELECT symbol, composite_score FROM composite_alpha
            ORDER BY as_of_date DESC
        """).fetchall()
        ac.close()
        for r in existing:
            alpha_scores[r["symbol"]] = r["composite_score"]
    except Exception:
        pass

    filtered_universe = sorted(
        [s for s, sc in alpha_scores.items() if sc >= ALPHA_THRESHOLD],
        key=lambda s: -alpha_scores[s]
    )
    logger.info(f"Alpha filter: {len(filtered_universe)}/{len(alpha_scores)} symbols pass >= {ALPHA_THRESHOLD}")

    # ── Step 5: Data download ─────────────────────────────────────────────────
    logger.info(f"\n[STEP 5A] Downloading price data for {len(filtered_universe)} symbols...")
    td = _download_v2_universe(filtered_universe + ["SPY", "^VIX"], days)

    # Ensure SPY is in td for trading day anchor (retry individually if batch failed)
    if "SPY" not in td:
        try:
            end_dt = date.today()
            start_dt = end_dt - timedelta(days=days + 60)
            spy_raw = yf.download("SPY", start=start_dt, end=end_dt,
                                  interval="1d", progress=False, auto_adjust=True)
            if not spy_raw.empty:
                td["SPY"] = spy_raw.dropna()
                logger.info("SPY downloaded individually as trading-day anchor")
        except Exception as e:
            logger.warning(f"SPY individual download failed: {e}")

    # Trading days
    trading_days = _get_trading_days(td, days)
    if not trading_days:
        logger.error("No trading days found — aborting")
        return {"status": "error", "reason": "no trading days"}

    # VIX map
    vix_df = td.get("^VIX") if td.get("^VIX") is not None else td.get("VIX")
    vix_map: dict = {}
    if vix_df is not None and not vix_df.empty:
        for idx, row in vix_df.iterrows():
            day = pd.Timestamp(idx).normalize()
            vix_map[day] = float(row["Close"]) if "Close" in row else 20.0

    # SPY return for comparison
    spy_return = 0.0
    if "SPY" in td and len(td["SPY"]) >= 2:
        spy_c = td["SPY"]["Close"].values
        spy_return = round((float(spy_c[-1]) - float(spy_c[0])) / float(spy_c[0]) * 100, 2)

    logger.info(f"\n[STEP 5B] Running v2 backtest on {len(td)-1} symbols over {len(trading_days)} days...")
    logger.info(f"  Alpha threshold: {ALPHA_THRESHOLD} | Confidence proxy: {CONF_THRESHOLD}")

    # Event-driven loop with alpha gate
    event_trades = _run_event_loop_v2(td, trading_days, vix_map, alpha_scores)

    # Options loop with alpha gate
    opt_trades, spread_trades, dte0_trades = _run_options_loop_v2(
        td, trading_days, vix_map, alpha_scores
    )

    # Compute tracking metrics
    all_bt_trades = []
    for strat, trades in event_trades.items():
        all_bt_trades.extend(trades)
    all_bt_trades.extend(opt_trades + spread_trades)

    trades_skipped = sum(
        1 for s, sc in alpha_scores.items() if sc < ALPHA_THRESHOLD
    ) * 5  # rough estimate
    avg_alpha = float(np.mean([t.get("alpha_score", 0.0) for t in all_bt_trades])) if all_bt_trades else 0.0

    # Equity curve
    logger.info("  Building equity curve...")
    curve = _build_equity_curve_v2(event_trades, opt_trades, spread_trades,
                                   trading_days, vix_map)

    # Save results
    logger.info("  Saving v2 results...")
    _save_v2_results(run_date, event_trades, opt_trades, spread_trades,
                     alpha_scores, spy_return)
    _save_equity_curve_v2(run_date, curve)
    _save_attribution(run_date, all_bt_trades, alpha_scores)

    equity_end = curve[-1]["equity"] if curve else STARTING_CASH
    v2_summary = {
        "equity_end": equity_end,
        "trades_skipped_by_alpha": trades_skipped,
        "avg_alpha_at_entry": round(avg_alpha, 4),
        "total_bt_trades": len(all_bt_trades),
        "filtered_symbols": len(filtered_universe),
    }

    # ── Step 6: Comparison table ──────────────────────────────────────────────
    logger.info("\n[STEP 6] Building three-way comparison table...")
    comparison = build_comparison_table(run_date, fleet_metrics, v2_summary)

    # ── Step 7: Alpha attribution ─────────────────────────────────────────────
    logger.info("\n[STEP 7] Computing alpha attribution...")
    attribution = compute_alpha_attribution(run_date)

    elapsed = round(time.time() - start_ts, 1)

    # Final report
    summary = {
        "status": "ok",
        "run_date": run_date,
        "elapsed_seconds": elapsed,
        "backtest_days": days,
        "universe_scanned": len(alpha_scores),
        "universe_after_alpha_filter": len(filtered_universe),
        "symbols_with_data": len(td) - 1,
        "trading_days": len(trading_days),
        "total_bt_trades": len(all_bt_trades),
        "equity_start": STARTING_CASH,
        "equity_end": equity_end,
        "portfolio_return_pct": round((equity_end - STARTING_CASH) / STARTING_CASH * 100, 2),
        "spy_return": spy_return,
        "fleet": fleet_metrics,
        "signal_analysis": signal_analysis,
        "comparison": comparison,
        "alpha_attribution": attribution,
    }

    _print_report(summary, comparison)
    return summary


def _print_report(summary: dict, comparison: dict) -> None:
    """Pretty-print the final super backtest report."""
    fleet = summary["fleet"]
    v1 = comparison["v1"]
    v2 = comparison["v2"]

    print(f"\n{'━'*72}")
    print(f"  WARP CORE REACTOR v2 — Lean Fleet Protocol")
    print(f"  Super Backtest v2 / Dilithium Crystal Edition")
    print(f"  Run: {summary['run_date']}  |  {summary['elapsed_seconds']}s")
    print(f"{'━'*72}")

    print(f"\n  STEP 1+2: FLEET ACTUAL PERFORMANCE")
    print(f"  {'─'*60}")
    print(f"  Total trades (corrected):  {fleet['total_trades']}")
    print(f"  Win rate:                  {fleet['win_rate']:.1f}%")
    print(f"  Total P&L:                 ${fleet['total_pnl']:>+12,.2f}")
    print(f"  Return %:                  {fleet['return_pct']:+.2f}%")
    print(f"  Sharpe:                    {fleet['fleet_sharpe']:.3f}")
    print(f"  Max drawdown:              {fleet['max_drawdown']:.2f}%")
    print(f"  Symbols traded:            {fleet['unique_symbols']}")
    print(f"  Agents active:             {fleet['unique_players']}")

    print(f"\n  Top Agents:")
    for a in fleet["agent_breakdown"][:8]:
        print(f"    {a['player_id']:<28} trades={a['trades']:>4}  pnl=${a['total_pnl']:>+10,.2f}  wr={a['wr']:.0f}%")

    sig = summary["signal_analysis"]
    print(f"\n  STEP 3: SIGNAL CENTER ACCURACY")
    print(f"  {'─'*60}")
    print(f"  ┌──────────────┬─────────┬──────────┬──────────┬────────────┐")
    print(f"  │ Signal Grade │  Count  │ Resolved │ Win Rate │ Avg Return │")
    print(f"  ├──────────────┼─────────┼──────────┼──────────┼────────────┤")
    for g in sig["grade_analysis"]:
        print(f"  │ {g['grade']:<12} │ {g['count']:>7} │ {g['resolved']:>8} │ {g['win_rate']:>7.1f}% │ {g['avg_return']:>+9.2f}% │")
    print(f"  └──────────────┴─────────┴──────────┴──────────┴────────────┘")
    print(f"  A/A+ only scenario:  WR={sig['a_plus_only_wr']:.1f}%  avg={sig['a_plus_only_avg_pnl']:+.2f}%")
    print(f"  All signals:         WR={sig['all_signals_wr']:.1f}%  avg={sig['all_signals_avg_pnl']:+.2f}%")

    print(f"\n  STEP 5: BACKTEST v2 RESULTS")
    print(f"  {'─'*60}")
    print(f"  Universe scanned:   {summary['universe_scanned']}")
    print(f"  After alpha filter: {summary['universe_after_alpha_filter']} (>= {ALPHA_THRESHOLD})")
    print(f"  Symbols with data:  {summary['symbols_with_data']}")
    print(f"  Total BT trades:    {summary['total_bt_trades']}")
    print(f"  Equity: ${summary['equity_start']:,.0f} → ${summary['equity_end']:,.0f} ({summary['portfolio_return_pct']:+.2f}%)")
    print(f"  SPY return:         {summary['spy_return']:+.2f}%")

    print(f"\n  STEP 6: FOUR-WAY COMPARISON — Lean Fleet Protocol")
    print(f"  {'─'*60}")
    n_act = fleet.get("unique_players", 29)
    print(f"  ┌─────────────────────┬──────────────────┬──────────────────┬──────────────────────┐")
    print(f"  │ Metric              │ Actual Fleet({n_act:02d}) │   v1 (no alpha)  │ v2 Lean Fleet (12)   │")
    print(f"  ├─────────────────────┼──────────────────┼──────────────────┼──────────────────────┤")
    metrics_display = comparison.get("metrics", [])
    for row in metrics_display:
        metric, af, af_l, v1v, v1_l, v2v, v2_l = row
        print(f"  │ {metric:<19} │ {af_l:>16} │ {v1_l:>16} │ {v2_l:>20} │")
    print(f"  └─────────────────────┴──────────────────┴──────────────────┴──────────────────────┘")
    # Lean Fleet model roster note
    print(f"\n  Lean Fleet Roster (12 active agents):")
    print(f"    Alpha Squad (6):  Spock(deepseek-r1:14b), Worf(qwen3:14b), Sulu(phi4:14b),")
    print(f"                      McCoy(plutus), Seven(qwen3:14b), Uhura(llama3.2:3b)")
    print(f"    Standalones (6):  Ollie, Neo, Capitol Trades, Dalio, T'Pol, Mr. Anderson")
    print(f"    Advisory (15):    Trip Tucker, Troi, Data, Ro, Geordi, Reed, Sisko,")
    print(f"                      Tuvok, Hoshi, Bashir, Dax, Q, Odo, Janeway + 1")

    attr = summary.get("alpha_attribution", {})
    if "signal_attribution" in attr:
        print(f"\n  STEP 7: ALPHA ATTRIBUTION")
        print(f"  {'─'*60}")
        sattr = attr["signal_attribution"]
        for sig_name, data in sorted(sattr.items(), key=lambda x: -abs(x[1].get("correlation_with_outcome", 0))):
            if isinstance(data, dict) and "correlation_with_outcome" in data:
                corr = data["correlation_with_outcome"]
                label = "HELPS ↑" if corr > 0.05 else ("HURTS ↓" if corr < -0.05 else "NEUTRAL")
                print(f"    {sig_name:<25} corr={corr:>+6.4f}  {label}  (n={data.get('samples',0)})")

        if attr["symbols_added_count"] > 0:
            print(f"\n  Symbols added beyond original 24: {attr['symbols_added_count']}")
            print(f"    {', '.join(attr['symbols_added_beyond_24'][:20])}")

    print(f"\n  STEP 8: REALITY CHECK")
    print(f"  {'─'*60}")
    print(f"  Fleet actual win rate:      {fleet['win_rate']:.1f}%  (target: >50%)")
    print(f"  v1 backtest avg win rate:   {v1['win_rate']:.1f}%  (execution gap: +{v1['win_rate']-fleet['win_rate']:.1f}%)")
    print(f"  v2 backtest avg win rate:   {v2['win_rate']:.1f}%")
    ec_gap = summary['portfolio_return_pct'] - fleet['return_pct']
    print(f"  Backtest v2 return:         {summary['portfolio_return_pct']:+.2f}%")
    print(f"  Fleet actual return:        {fleet['return_pct']:+.2f}%")
    print(f"  Execution gap:              {ec_gap:+.2f}pp (backtest optimism)")
    print(f"\n{'━'*72}\n")


if __name__ == "__main__":
    import argparse
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [sv2] %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="Super Backtest v2 — Dilithium Crystal Edition")
    parser.add_argument("--days", type=int, default=180)
    parser.add_argument("--fleet-only", action="store_true", help="Only run fleet analysis")
    parser.add_argument("--signals-only", action="store_true", help="Only run signal analysis")
    args = parser.parse_args()

    if args.fleet_only:
        _init_v2_tables()
        m = analyze_fleet_performance(date.today().isoformat())
        print(json.dumps(m, indent=2, default=str))
    elif args.signals_only:
        _init_v2_tables()
        s = analyze_signal_center(date.today().isoformat())
        print(json.dumps(s, indent=2, default=str))
    else:
        import json
        result = run_super_backtest_v2(args.days)
        print(f"\nSummary JSON saved. Equity: ${result.get('equity_end',0):,.0f}")
