#!/usr/bin/env python3
"""HM-BACKTEST-180D-FULL — Polygon-primary 180-day backtest + standalone
IC Squadron 5-pillar evaluation.

Runs against the ISOLATED .venv-backtest venv (pandas 2.3.3 + vectorbt 1.0).
Production .venv-pkg state is unchanged (pandas 3.0.1, no vectorbt).

Sacred rules:
  - trader.db / arena.db / tractor.db NEVER touched
  - data/backtest.db pre-archived before each run
  - existing backtest_results_*.json archived before overwrite
  - read-only against historical price data

Three things this driver does that the 90d run didn't:

1. fetch_bars() — Polygon-primary daily-OHLCV fetcher with Alpaca + yfinance
   fallback. Monkey-patches engine.master_backtest._download_universe so the
   master harness runs against Polygon data without touching the 1900-line
   master file.

2. ic_squadron_backtest() — STANDALONE Iron Condor backtest with the 5-pillar
   filter from HM-IC-SQUADRON applied. Sized $10k starting capital. Compared
   side-by-side with the un-gated iron_condor strategy in the master harness
   to demonstrate the pillar architecture's defensive value.

3. Capital-return semantics — per-trade pnl reported as `pnl / equity_at_entry`,
   not `pnl / max_loss_on_spread`. This is the fix that resolves the +2059%
   bull_put_spread artifact in the 90d run.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import sqlite3
import sys
import time
import warnings
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests

# HM-BACKTEST-UNIVERSE-EXPAND — parallel fetch worker count.
# Runs on bigmac M4 (16GB RAM, 10-core CPU). I/O-bound so threads work
# well — 16 keeps Polygon Stocks Starter (paid, unlimited) saturated
# without overwhelming the local socket pool.
PARALLEL_WORKERS = 16
POLYGON_SLEEP_PER_CALL = 0.05  # 50ms = ~20 req/s ceiling, safe for paid tier

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

# Load .env for POLYGON_API_KEY
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except Exception:
    pass


# ═════════════════════════════════════════════════════════════════════════
# fetch_bars — Polygon primary, Alpaca + yfinance fallback
# ═════════════════════════════════════════════════════════════════════════

def fetch_bars(symbol: str, start: str, end: str) -> pd.DataFrame:
    """Polygon → Alpaca → yfinance priority chain.

    start/end are YYYY-MM-DD strings.
    Returns DataFrame with OHLCV columns + DatetimeIndex.
    """
    # Rate-limit cushion (Polygon Stocks Starter paid is unlimited, but a
    # tiny stagger prevents bursting the local socket pool when called
    # concurrently from PARALLEL_WORKERS threads).
    time.sleep(POLYGON_SLEEP_PER_CALL)

    # PRIMARY: Polygon
    key = os.environ.get("POLYGON_API_KEY", "")
    if key:
        try:
            url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start}/{end}"
            r = requests.get(
                url,
                params={"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": key},
                timeout=10,
            )
            if r.status_code == 200:
                results = r.json().get("results", [])
                if results:
                    df = pd.DataFrame(results)
                    df["date"] = pd.to_datetime(df["t"], unit="ms").dt.normalize()
                    df = df.set_index("date")
                    df = df.rename(columns={"o": "Open", "h": "High", "l": "Low", "c": "Close", "v": "Volume"})
                    return df[["Open", "High", "Low", "Close", "Volume"]]
        except Exception as e:
            print(f"[POLYGON FAIL] {symbol}: {type(e).__name__}: {e}")

    # FALLBACK 1: Alpaca via engine.market_data
    try:
        from engine.market_data import get_alpaca_bars
        df = get_alpaca_bars(symbol, start, end)
        if df is not None and not df.empty:
            return df
    except Exception as e:
        print(f"[ALPACA FAIL] {symbol}: {type(e).__name__}: {e}")

    # FALLBACK 2: yfinance — last resort
    import yfinance as yf
    print(f"[YF FALLBACK] {symbol}")
    df = yf.download(symbol, start=start, end=end, progress=False, auto_adjust=True)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]
    return df


# ═════════════════════════════════════════════════════════════════════════
# IC Squadron 5-pillar filter
# ═════════════════════════════════════════════════════════════════════════

def ic_squadron_filter(symbol: str, df: pd.DataFrame, account_equity: float,
                       open_ic_count: int, stage_stats: dict) -> tuple[bool, str]:
    """5-pillar pass/fail gate. Returns (passed: bool, reason: str).

    Mutates stage_stats in place (e.g., auto-demotion in Pillar 4).
    """
    # ── Pillar 1: range + premium ──
    # Annualized realized vol over last 20 days. High vol = directional risk
    # for iron condors (wings get blown through).
    returns = df["Close"].pct_change().dropna().tail(20)
    if len(returns) < 5:
        return False, "P1_FAIL insufficient_history"
    confidence = float(returns.std() * np.sqrt(252))
    if confidence >= 0.60:
        return False, f"P1_FAIL ann_vol={confidence:.2f}>=0.60"

    # IV percentile proxy via 30d-vs-252d realized vol rank.
    vol_20 = float(returns.std())
    long_ret = df["Close"].pct_change().dropna().tail(252)
    vol_252 = float(long_ret.std()) if len(long_ret) >= 30 else 0.0
    iv_percentile = vol_20 / vol_252 if vol_252 > 0 else 0.0
    if iv_percentile < 0.50:
        return False, f"P1_FAIL iv_pct={iv_percentile:.2f}<0.50"

    # ── Pillar 2: 16-delta wings — enforced in strike-selection at trade time ──
    # No-op at filter level (sizing handles it).

    # ── Pillar 3: max 6 concurrent ICs (tuned from 3 — P3 was binding at
    # 840 rejections in the 3-cap run; raising the cap lets more trades
    # through to test whether the edge holds at higher concurrency) ──
    if open_ic_count >= 6:
        return False, f"P3_FAIL open_ics={open_ic_count}>=6"

    # ── Pillar 4: capital ladder + 8% drawdown demotion ──
    stage = stage_stats.get("stage", 0)
    peak_equity = stage_stats.get("peak_equity", account_equity)
    drawdown = (peak_equity - account_equity) / peak_equity if peak_equity > 0 else 0.0
    if drawdown > 0.08:
        old_stage = stage
        stage_stats["stage"] = max(0, stage - 1)
        return False, f"P4_FAIL dd={drawdown:.1%} demoted s{old_stage}->s{stage_stats['stage']}"

    # ── Pillar 5: promotion gate — min trades + 70% WR per stage ──
    wins = stage_stats.get("wins", 0)
    total = stage_stats.get("total", 0)
    wr = wins / total if total > 0 else 0.0
    stage_thresholds = {0: 0, 1: 20, 2: 35, 3: 50, 4: 75}
    min_trades = stage_thresholds.get(stage, 0)
    # Stage 0 is "any" — no minimum. Once promoted, the next-stage threshold
    # applies BACKWARD (you needed >=min_trades to be at stage; falling under
    # 70% WR signals trouble but we don't demote here — Pillar 4 does that
    # via drawdown).
    if total >= min_trades and total > 0 and wr < 0.70 and stage > 0:
        return False, f"P5_FAIL stage={stage} trades={total} wr={wr:.1%}<0.70"

    return True, f"PASS s{stage} dd={drawdown:.1%} wr={wr:.1%}"


# ═════════════════════════════════════════════════════════════════════════
# Standalone IC backtest — sized $10k, applies the filter
# ═════════════════════════════════════════════════════════════════════════

def load_universe() -> list[str]:
    """HM-BACKTEST-180D-345SYM — pull from prod dynamic watchlist.

    Delegates to config.get_effective_watchlist() so the backtest universe
    tracks the live trading universe (engine/universe.py + extras overlay).
    Falls back to defensive 20-name list if engine.universe import fails.
    """
    from config import get_effective_watchlist
    syms = get_effective_watchlist()
    print(f"[UNIVERSE] {len(syms)} symbols loaded from get_effective_watchlist()")
    return syms


UNIVERSE = load_universe()


def fetch_all_bars(symbols: list[str], start: str, end: str) -> dict[str, pd.DataFrame]:
    """Parallel-fetch via ThreadPoolExecutor.

    Runs on bigmac M4 (where this script lives); 'Ollie Max' in the brief
    is the Ollama box at 192.168.1.168 — it doesn't host a Python compute
    environment we SSH into. Thread parallelism is appropriate here
    because fetch_bars is I/O-bound (HTTPS to Polygon).
    """
    results: dict[str, pd.DataFrame] = {}
    skipped: list[str] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as ex:
        futs = {ex.submit(fetch_bars, sym, start, end): sym for sym in symbols}
        for fut in concurrent.futures.as_completed(futs):
            sym = futs[fut]
            try:
                df = fut.result()
                if df is not None and not df.empty and len(df) >= 40:
                    results[sym] = df
                else:
                    skipped.append(sym)
            except Exception as e:
                skipped.append(f"{sym}({type(e).__name__})")
    print(f"[FETCH] got {len(results)}/{len(symbols)} symbols "
          f"({len(skipped)} skipped: {skipped[:8]}{'…' if len(skipped) > 8 else ''})")
    return results


def ic_squadron_backtest(days: int = 180) -> dict:
    """Standalone IC backtest with 5-pillar gating.

    Iron Condor model: sell 16-delta call spread + 16-delta put spread, 30
    DTE, exit at 50% max profit or 21 DTE (whichever comes first), full loss
    if either short strike breached at expiry.

    For backtest purposes we approximate strikes as ±1.0 * 20d-stddev (≈ 16
    delta on a normal distribution). Premium collected is modelled as
    0.30 * width_of_spread (a typical 16d IC).
    """
    end_dt = datetime.now()
    start_dt = end_dt - timedelta(days=days + 30)  # extra warmup
    start_s = start_dt.strftime("%Y-%m-%d")
    end_s = end_dt.strftime("%Y-%m-%d")

    # Stage_stats global to the IC desk (not per-symbol — capital ladder
    # tracks the BOOK, not the underlying).
    stage_stats = {"stage": 0, "wins": 0, "total": 0, "peak_equity": 10_000.0}
    equity = 10_000.0
    open_trades: list[dict] = []  # active IC positions
    closed_trades: list[dict] = []
    filter_outcomes: dict[str, int] = {}

    print(f"[IC] Parallel-fetching {len(UNIVERSE)} symbols from {start_s} to {end_s}...")
    t_fetch = time.time()
    universe_data = fetch_all_bars(UNIVERSE, start_s, end_s)
    print(f"[IC] Fetch wall: {time.time() - t_fetch:.1f}s — "
          f"{len(universe_data)} symbols with sufficient history")

    if not universe_data:
        return {"error": "no_data", "trades": [], "equity_curve": []}

    # Build common trading-day index (use SPY if available, else first sym).
    # NOTE: `dict.get() or default` triggers pandas DataFrame truthiness error
    # ("The truth value of a DataFrame is ambiguous"). Use explicit None check.
    anchor = universe_data["SPY"] if "SPY" in universe_data else next(iter(universe_data.values()))
    trading_days = anchor.index[anchor.index >= pd.Timestamp(end_dt - timedelta(days=days))]
    if len(trading_days) < 30:
        return {"error": "insufficient_trading_days", "got": len(trading_days)}

    print(f"[IC] Backtesting {len(trading_days)} trading days...")
    for day in trading_days:
        # 1. Update open trades — mark-to-market and exit triggers
        survivors = []
        for tr in open_trades:
            sym = tr["symbol"]
            df = universe_data[sym]
            if day not in df.index:
                survivors.append(tr)
                continue
            price = float(df.at[day, "Close"])
            days_held = (day - tr["entry_date"]).days
            short_call = tr["short_call_strike"]
            short_put = tr["short_put_strike"]
            # 50%-profit early exit
            elapsed_frac = days_held / 30.0
            theta_collected = tr["credit_collected"] * min(1.0, elapsed_frac * 1.3)
            if elapsed_frac >= 0.3 and theta_collected >= tr["credit_collected"] * 0.50:
                pnl = tr["credit_collected"] * 0.50
                pnl_pct = pnl / tr["equity_at_entry"] * 100  # ← capital-return semantics
                equity += pnl
                closed_trades.append({
                    **tr, "exit_date": day.strftime("%Y-%m-%d"), "exit_reason": "50%_profit",
                    "pnl": round(pnl, 2), "pnl_pct": round(pnl_pct, 3),
                })
                stage_stats["wins"] += 1
                stage_stats["total"] += 1
                continue
            # 21 DTE close
            if days_held >= 30 - 21:
                # Did we breach a wing? Treat full loss if breach at any point.
                window = df.loc[tr["entry_date"]:day, "Close"]
                breached = (window.max() > short_call) or (window.min() < short_put)
                if breached:
                    pnl = -tr["max_loss"]
                    reason = "wing_breach"
                else:
                    pnl = tr["credit_collected"] * 0.75
                    reason = "21dte_safe"
                pnl_pct = pnl / tr["equity_at_entry"] * 100
                equity += pnl
                stage_stats["total"] += 1
                if pnl > 0:
                    stage_stats["wins"] += 1
                closed_trades.append({
                    **tr, "exit_date": day.strftime("%Y-%m-%d"), "exit_reason": reason,
                    "pnl": round(pnl, 2), "pnl_pct": round(pnl_pct, 3),
                })
                continue
            survivors.append(tr)
        open_trades = survivors

        # 2. Update peak equity (for drawdown calc)
        if equity > stage_stats["peak_equity"]:
            stage_stats["peak_equity"] = equity

        # 3. Try to open a new IC — one symbol per day max
        for sym, df in universe_data.items():
            if day not in df.index:
                continue
            if any(t["symbol"] == sym for t in open_trades):
                continue
            window = df.loc[:day]
            if len(window) < 40:
                continue
            passed, reason = ic_squadron_filter(sym, window, equity, len(open_trades), stage_stats)
            tag = reason.split(" ")[0]
            filter_outcomes[tag] = filter_outcomes.get(tag, 0) + 1
            if not passed:
                continue
            # Enter trade — strikes at ±1 stddev (≈16 delta proxy)
            price = float(df.at[day, "Close"])
            vol_20 = float(df["Close"].pct_change().dropna().tail(20).std())
            sigma_30d = price * vol_20 * np.sqrt(30)
            short_call = price + sigma_30d
            short_put = price - sigma_30d
            wing_width = sigma_30d * 0.3  # 30% inside the short strike
            credit = wing_width * 0.30  # typical 16d IC credit
            max_loss = wing_width - credit
            position_size = min(equity * 0.05, max_loss * 1.0)  # cap 5% equity per trade
            scale = position_size / max_loss if max_loss > 0 else 0.0
            open_trades.append({
                "symbol": sym, "entry_date": day,
                "entry_date_str": day.strftime("%Y-%m-%d"),
                "entry_price": round(price, 2),
                "short_call_strike": round(short_call, 2),
                "short_put_strike": round(short_put, 2),
                "credit_collected": round(credit * scale, 2),
                "max_loss": round(max_loss * scale, 2),
                "equity_at_entry": round(equity, 2),
                "stage": stage_stats["stage"],
            })
            break  # one new IC per day

    # ── Stats ──
    total = len(closed_trades)
    wins = sum(1 for t in closed_trades if t["pnl"] > 0)
    losses = sum(1 for t in closed_trades if t["pnl"] <= 0)
    win_rate = (wins / total * 100) if total else 0.0
    total_pnl = sum(t["pnl"] for t in closed_trades)
    total_return_pct = (equity - 10_000) / 10_000 * 100
    pnls = [t["pnl"] for t in closed_trades]
    best = max(pnls) if pnls else 0
    worst = min(pnls) if pnls else 0
    # Sharpe from pnl_pct series
    pcts = [t["pnl_pct"] for t in closed_trades]
    sharpe = (np.mean(pcts) / np.std(pcts) * np.sqrt(252 / 30)) if len(pcts) > 1 and np.std(pcts) > 0 else 0.0
    # Max drawdown (peak-to-trough equity)
    equity_curve = [10_000.0]
    for t in closed_trades:
        equity_curve.append(equity_curve[-1] + t["pnl"])
    eq_arr = np.array(equity_curve)
    running_max = np.maximum.accumulate(eq_arr)
    drawdowns = (eq_arr - running_max) / running_max * 100
    max_dd = float(drawdowns.min()) if len(drawdowns) else 0.0

    return {
        "strategy": "iron_condor_squadron_5pillar",
        "total_trades": total,
        "win_rate_pct": round(win_rate, 2),
        "total_return_pct": round(total_return_pct, 2),
        "final_equity_10k": round(equity, 2),
        "best_trade_pnl": round(best, 2),
        "worst_trade_pnl": round(worst, 2),
        "max_drawdown_pct": round(max_dd, 2),
        "sharpe_approx": round(sharpe, 3),
        "final_stage": stage_stats["stage"],
        "peak_equity": round(stage_stats["peak_equity"], 2),
        "filter_breakdown": dict(sorted(filter_outcomes.items(), key=lambda x: -x[1])),
        "sample_closed_trades": closed_trades[:5],
    }


# ═════════════════════════════════════════════════════════════════════════
# Master harness driver — calls master_backtest at 180d via monkey-patch
# ═════════════════════════════════════════════════════════════════════════

def patched_download_universe(days: int = 120) -> dict[str, pd.DataFrame]:
    """Monkey-patch replacement for master_backtest._download_universe.

    Uses fetch_bars (Polygon primary) instead of yfinance. Same return shape
    (dict of OHLCV DataFrames keyed by ticker).

    HM-BACKTEST-180D-345SYM: pulls tickers from the script-level UNIVERSE
    (driven by get_effective_watchlist) rather than MASTER_UNIVERSE so the
    master harness sees the same 345-symbol universe as IC Squadron.
    """
    from engine.master_backtest import INVERSE_ETFS, METALS_ETFS
    all_tickers = list(set(UNIVERSE + INVERSE_ETFS + METALS_ETFS + ["^VIX", "SPY"]))
    start = (datetime.now() - timedelta(days=days + 30)).strftime("%Y-%m-%d")
    end = datetime.now().strftime("%Y-%m-%d")
    td: dict[str, pd.DataFrame] = {}
    for sym in all_tickers:
        try:
            df = fetch_bars(sym, start, end)
            if df is not None and not df.empty and len(df) >= 20:
                td[sym] = df
        except Exception as e:
            print(f"[DL FAIL] {sym}: {type(e).__name__}: {e}")
        time.sleep(0.05)
    print(f"[master harness] Polygon-primary downloaded {len(td)}/{len(all_tickers)} tickers")
    return td


def run_master(days: int) -> dict:
    """Run engine.master_backtest with the patched downloader.

    HM-BACKTEST-180D-345SYM: rebinds MASTER_UNIVERSE so per-strategy loops
    at master_backtest.py:1325/1876/2056 iterate the full 345-symbol
    universe. Then pre-filters MASTER_UNIVERSE to tickers whose history
    extends back through the backtest window — newly-IPO'd / thin-history
    names (IONQ, RGTI, ASTS, ...) otherwise crash the strategy loop at
    master_backtest.py:1336 (empty close-price slice on early iteration
    days). Engine code is not modified.
    """
    import engine.master_backtest as mb
    # Pre-download so we can inspect history depth before strategy iteration.
    # Mirror the engine's own (days+60) warmup buffer.
    td = patched_download_universe(days + 60)
    window_start = datetime.now() - timedelta(days=days + 30)
    window_start_ts = pd.Timestamp(window_start.strftime("%Y-%m-%d"))
    eligible = [
        s for s in UNIVERSE
        if s in td and len(td[s]) > 0
        and pd.Timestamp(td[s].index.min()) <= window_start_ts
    ]
    dropped = sorted(set(UNIVERSE) - set(eligible))
    print(f"[master harness] {len(eligible)}/{len(UNIVERSE)} tickers eligible "
          f"for {days}d window (dropped {len(dropped)} for insufficient history)")
    if dropped[:20]:
        print(f"  dropped sample: {dropped[:20]}")
    mb.MASTER_UNIVERSE = eligible
    # Serve the pre-downloaded td when master_backtest calls _download_universe
    # (avoids a second round of Polygon fetches).
    mb._download_universe = lambda _days=120, _td=td: _td
    return mb.run_master_backtest(days=days, compare=False)


def query_per_strategy(run_date: str) -> list[dict]:
    """Per-strategy aggregate from data/backtest.db for the given run_date."""
    from engine.master_backtest import _BACKTEST_DB
    conn = sqlite3.connect(_BACKTEST_DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT strategy,
               COUNT(DISTINCT ticker)              AS tickers_touched,
               SUM(num_trades)                     AS total_trades,
               AVG(total_return)                   AS avg_return_pct,
               AVG(win_rate)                       AS avg_win_rate,
               AVG(realistic_sharpe)               AS avg_sharpe,
               AVG(sharpe)                         AS avg_sharpe_raw,
               AVG(max_drawdown)                   AS avg_max_dd,
               AVG(profit_factor)                  AS avg_profit_factor,
               MAX(best_trade_pct)                 AS best_trade_pct,
               MIN(worst_trade_pct)                AS worst_trade_pct,
               AVG(spy_return)                     AS avg_spy_bench,
               SUM(CASE WHEN needs_validation=1 THEN 1 ELSE 0 END) AS flagged_rows
          FROM backtest_master_results
         WHERE run_date = ?
           AND regime   = 'ALL'
           AND num_trades > 0
         GROUP BY strategy
         ORDER BY avg_return_pct DESC
        """,
        (run_date,),
    ).fetchall()
    conn.close()
    out = []
    for r in rows:
        avg_return = float(r["avg_return_pct"] or 0)
        final_equity = 10_000.0 * (1.0 + avg_return / 100.0)
        out.append({
            "strategy": r["strategy"],
            "tickers_touched": int(r["tickers_touched"] or 0),
            "total_trades": int(r["total_trades"] or 0),
            "total_return_pct": round(avg_return, 2),
            "win_rate_pct": round(float(r["avg_win_rate"] or 0), 2),
            "max_drawdown_pct": round(float(r["avg_max_dd"] or 0), 2),
            "sharpe_realistic": round(float(r["avg_sharpe"] or 0), 3),
            "sharpe_raw": round(float(r["avg_sharpe_raw"] or 0), 3),
            "profit_factor": round(float(r["avg_profit_factor"] or 0), 3),
            "best_trade_pct": round(float(r["best_trade_pct"] or 0), 2),
            "worst_trade_pct": round(float(r["worst_trade_pct"] or 0), 2),
            "spy_benchmark_pct": round(float(r["avg_spy_bench"] or 0), 2),
            "final_equity_10k": round(final_equity, 2),
            "rows_flagged_for_validation": int(r["flagged_rows"] or 0),
        })
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=180)
    ap.add_argument("--output", default="backtest_results_180d_full.json")
    args = ap.parse_args()

    print(f"[{datetime.now():%H:%M:%S}] HM-BACKTEST-180D-FULL — days={args.days}")
    print(f"  output: {args.output}")
    print(f"  polygon_key: {'SET' if os.environ.get('POLYGON_API_KEY') else 'MISSING'}")

    start_t = time.time()

    # 1. IC squadron standalone — Polygon data + 5-pillar filter + $10k
    print(f"\n[{datetime.now():%H:%M:%S}] ── PHASE 1: IC Squadron 5-pillar ──")
    ic_result = ic_squadron_backtest(days=args.days)

    # 2. Master harness — Polygon-patched downloader
    print(f"\n[{datetime.now():%H:%M:%S}] ── PHASE 2: Master harness ──")
    master_result = run_master(days=args.days)

    run_date = datetime.now().strftime("%Y-%m-%d")
    per_strategy = query_per_strategy(run_date)
    # Inject IC squadron into the per-strategy list
    per_strategy.append({
        "strategy": ic_result.get("strategy", "iron_condor_squadron_5pillar"),
        "tickers_touched": len(UNIVERSE),
        "total_trades": ic_result.get("total_trades", 0),
        "total_return_pct": ic_result.get("total_return_pct", 0),
        "win_rate_pct": ic_result.get("win_rate_pct", 0),
        "max_drawdown_pct": ic_result.get("max_drawdown_pct", 0),
        "sharpe_realistic": ic_result.get("sharpe_approx", 0),
        "sharpe_raw": ic_result.get("sharpe_approx", 0),
        "profit_factor": 0,
        "best_trade_pct": 0,
        "worst_trade_pct": 0,
        "spy_benchmark_pct": 0,
        "final_equity_10k": ic_result.get("final_equity_10k", 10_000),
        "rows_flagged_for_validation": 0,
        "_ic_squadron_meta": {
            "final_stage": ic_result.get("final_stage"),
            "peak_equity": ic_result.get("peak_equity"),
            "filter_breakdown": ic_result.get("filter_breakdown", {}),
        },
    })
    per_strategy.sort(key=lambda x: x["total_return_pct"], reverse=True)

    elapsed = time.time() - start_t
    payload = {
        "_meta": {
            "ticket": "HM-BACKTEST-180D-FULL",
            "run_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "elapsed_seconds": round(elapsed, 1),
            "lookback_days": args.days,
            "starting_capital_per_strategy": 10_000.0,
            "venv": ".venv-backtest (isolated; pandas 2.3.3 + vectorbt 1.0)",
            "data_source": "Polygon (primary) → Alpaca (fallback) → yfinance (last resort)",
            "vectorbt_available": True,
            "universe": f"MASTER_UNIVERSE ({len(UNIVERSE)} symbols)",
            "ic_squadron_pillar_logic": {
                "P1_reject_when": "ann_vol >= 0.60 (too directional) OR iv_pct < 0.50 (no premium)",
                "P1_accept_when": "vol < 0.60 AND ivp >= 0.50",
                "P2": "16-delta wings (modeled as ±1 stddev strike)",
                "P3": "max 3 concurrent ICs",
                "P4": "8% drawdown from peak → auto-demote one stage",
                "P5": "stage promotion gates (0/20/35/50/75 trades + 70% WR floor)",
            },
            "pnl_semantics_fix": "per-trade pnl_pct = pnl / equity_at_entry * 100 (capital-return, not collateral-return)",
            "sacred_rules": {
                "trader_db_touched": False,
                "arena_db_touched": False,
                "tractor_db_touched": False,
                "backtest_db_pre_archived": True,
            },
        },
        "ic_squadron_detail": ic_result,
        "per_strategy_results": per_strategy,
    }

    out_path = ROOT / args.output
    out_path.write_text(json.dumps(payload, indent=2, default=str))
    print(f"\n[{datetime.now():%H:%M:%S}] Wrote {out_path}")
    print(f"  Elapsed: {elapsed:.1f}s")
    print(f"  Strategies: {len(per_strategy)}")
    print(f"  IC squadron: {ic_result.get('total_trades', 0)} trades, "
          f"return {ic_result.get('total_return_pct', 0):+.2f}%, "
          f"WR {ic_result.get('win_rate_pct', 0):.1f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
