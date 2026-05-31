#!/usr/bin/env python3
"""HM-HOLLY-FAITHFUL Phase 2 — INTRADAY-FLAT backtest engine.

The faithful-Holly character change: real Trade Ideas Holly trades INTRADAY and goes
FLAT at the close (no overnight risk). The existing engine/holly_nightly_backtest.py is
a DAILY-bar swing backtester ("Holly-lite") — kept as-is (working baseline). This module
adds the intraday-flat path WITHOUT touching that baseline, so the A/B and eyes-on stay
clean.

Design:
  - DATA: 5-minute bars from Polygon Starter (paid, reliable; probed 2026-05-30 →
    HTTP 200, serves 5min DELAYED — fine for an overnight backtest). Multi-day history
    via pagination. Cached in backtest.db::holly_intraday_cache to absorb the ~100×
    data-volume balloon (5min × symbols × days).
  - EOD-FLAT: every strategy's exits are OR'd with a session-close mask (last bar of each
    trading day forced to exit) so no position is ever held overnight — the defining
    Holly behavior. Re-entry allowed next session.
  - ENGINE: same vectorbt + param-sweep optimization loop as the daily path (keep-best),
    just on intraday bars with freq matching the bar size.
  - FAIL-LOUD: vectorbt missing → explicit error + NTFY (same discipline as the daily
    repair; never silently return nothing).

Runs under .venv-backtest (where vectorbt is installed), via the cron wrapper.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone

# HM-HOLLY-INTRADAY-ENV 2026-05-30: this module runs STANDALONE under .venv-backtest
# (via the cron wrapper / -m), NOT inside the live trader process, so the .env is not
# pre-loaded. Without this, os.getenv("POLYGON_API_KEY") is None → _fetch returns None
# → 0 bars → silent "no results". Load .env at import so the Polygon key is present.
try:
    from dotenv import load_dotenv
    load_dotenv("/Users/bigmac/autonomous-trader/.env")
except Exception:
    pass

logger = logging.getLogger(__name__)

_BACKTEST_DB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "data", "backtest.db")

# 5-min is the faithful-Holly bar. ~78 bars/session.
BAR_MULTIPLIER = 5
BAR_TIMESPAN = "minute"
BAR_LABEL = "5m"
_INTRADAY_CACHE_TTL = 12 * 3600  # 12h — re-fetch at most twice a day

# Reuse the daily module's sweep grids + stat helpers (single source of truth).
from engine.holly_nightly_backtest import (  # noqa: E402
    RSI_SWEEP, MACD_SWEEP, BB_SWEEP, SMA_SWEEP,
    _s, _stat, _get_top_volume_movers, _conn_backtest,
)


# ── DB ───────────────────────────────────────────────────────────────────────

def _init_intraday_tables() -> None:
    conn = sqlite3.connect(_BACKTEST_DB, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS holly_intraday_cache (
            symbol      TEXT NOT NULL,
            bar         TEXT NOT NULL,
            ts          TEXT NOT NULL,       -- ISO bar timestamp (UTC)
            close       REAL,
            fetched_at  REAL,
            UNIQUE(symbol, bar, ts)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS holly_intraday_winners (
            run_date    TEXT NOT NULL,
            rank        INTEGER,
            ticker      TEXT,
            strategy    TEXT,
            params      TEXT,
            total_return REAL,
            sharpe      REAL,
            win_rate    REAL,
            profit_factor REAL,
            bar         TEXT,
            UNIQUE(run_date, rank)
        )
    """)
    conn.commit()
    conn.close()


# ── INTRADAY DATA (Polygon 5min, paginated + cached) ──────────────────────────

def _fetch_polygon_intraday(symbol: str, days: int = 30):
    """Return a pandas Series of 5-min closes (UTC index) for `symbol`, or None.

    Cache-first (12h TTL); on miss, paginate Polygon aggs. Polygon caps ~500 bars/
    response; 5min ≈ 78 bars/session, so we page by date windows. RELIABLE source
    only (Polygon Starter) — NO yfinance (throttled-empty silent-degrade trap).
    """
    import pandas as pd
    sym = symbol.upper()
    now = time.time()

    # 1. cache
    try:
        conn = sqlite3.connect(_BACKTEST_DB, timeout=15)
        cutoff_iso = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        rows = conn.execute(
            "SELECT ts, close, MAX(fetched_at) FROM holly_intraday_cache "
            "WHERE symbol=? AND bar=? AND ts>=? GROUP BY ts ORDER BY ts ASC",
            (sym, BAR_LABEL, cutoff_iso),
        ).fetchall()
        conn.close()
        if rows and len(rows) >= 200:
            newest_fetch = max((r[2] or 0) for r in rows)
            if now - newest_fetch < _INTRADAY_CACHE_TTL:
                idx = pd.to_datetime([r[0] for r in rows])
                if getattr(idx, "tz", None) is not None:
                    idx = idx.tz_localize(None)  # vectorbt needs tz-naive (see fetch path)
                return pd.Series([r[1] for r in rows], index=idx, name=sym, dtype=float).dropna()
    except Exception:
        pass

    # 2. Polygon fetch (reliable). Page in 10-day windows to stay under the 500-bar cap.
    key = os.getenv("POLYGON_API_KEY")
    if not key:
        return None
    import requests
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    all_bars: dict[str, float] = {}
    win_start = start
    while win_start < end:
        win_end = min(win_start + timedelta(days=10), end)
        url = (f"https://api.polygon.io/v2/aggs/ticker/{sym}"
               f"/range/{BAR_MULTIPLIER}/{BAR_TIMESPAN}/{win_start}/{win_end}"
               f"?adjusted=true&sort=asc&limit=50000")
        try:
            # HM-HOLLY-INTRADAY-AUTH 2026-05-30: Polygon intraday aggs require
            # Authorization: Bearer header auth — the ?apiKey= querystring form returns
            # HTTP 401 NOT_AUTHORIZED here (verified: header→200/763 bars, QS→401).
            # Reliable source confirmed; only the auth form was wrong.
            r = requests.get(url, headers={"Authorization": f"Bearer {key}"}, timeout=15)
            if r.status_code == 200:
                for b in (r.json().get("results") or []):
                    ts_ms = b.get("t")
                    if ts_ms is None:
                        continue
                    iso = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
                    all_bars[iso] = float(b.get("c", 0))
        except Exception as e:
            logger.warning("intraday fetch %s %s-%s: %s", sym, win_start, win_end, e)
        win_start = win_end

    if not all_bars:
        return None

    # 3. write-through cache
    try:
        conn = sqlite3.connect(_BACKTEST_DB, timeout=30)
        conn.executemany(
            "INSERT OR IGNORE INTO holly_intraday_cache(symbol, bar, ts, close, fetched_at) "
            "VALUES (?,?,?,?,?)",
            [(sym, BAR_LABEL, ts, c, now) for ts, c in all_bars.items()],
        )
        conn.commit()
        conn.close()
    except Exception:
        pass

    items = sorted(all_bars.items())
    idx = pd.to_datetime([t for t, _ in items])
    # vectorbt 1.0.0 Portfolio.from_signals requires a tz-NAIVE datetime64[ns] index
    # (a tz-aware UTC index raises "Index must be datetime64[ns, UTC], not int64").
    # Bars are UTC; drop the tz after sorting — ordering + EOD-by-day grouping are
    # unaffected (all bars share the offset). Verified: tz-naive backtests cleanly.
    if getattr(idx, "tz", None) is not None:
        idx = idx.tz_localize(None)
    return pd.Series([c for _, c in items], index=idx, name=sym, dtype=float).dropna()


def _eod_flat_exits(close):
    """Boolean Series: True on the LAST bar of each trading day (force EOD-flat).

    OR this into every strategy's exits → no overnight holds (faithful Holly).
    """
    import pandas as pd
    idx = close.index
    # group by calendar date; the max timestamp per day is that session's last bar
    by_day = pd.Series(idx, index=idx).groupby(idx.normalize()).max()
    last_bars = set(by_day.values)
    return pd.Series([t in last_bars for t in idx], index=idx)


# ── INTRADAY RUNNERS (EOD-flat) ───────────────────────────────────────────────

def _bars_per_year(close) -> float:
    """Annualization factor for Sharpe on intraday bars (~252 days × ~78 5m bars)."""
    return 252 * 78


def _run_intraday(close, strategy: str, params: dict, cash: float = 10_000, fees: float = 0.001):
    """Backtest one intraday strategy with EOD-flat exits. Returns metrics or None."""
    import vectorbt as vbt
    import numpy as np  # noqa: F401
    eod = _eod_flat_exits(close)
    if strategy == "rsi":
        rsi = vbt.RSI.run(close, window=params.get("window", 14))
        entries = rsi.rsi_crossed_above(params.get("entry", 30))
        exits = rsi.rsi_crossed_below(params.get("exit", 70))
    elif strategy == "macd":
        m = vbt.MACD.run(close, fast_window=params.get("fast", 12),
                         slow_window=params.get("slow", 26), signal_window=params.get("signal", 9))
        entries = m.macd_crossed_above(m.signal)
        exits = m.macd_crossed_below(m.signal)
    elif strategy == "bollinger":
        bb = vbt.BBANDS.run(close, window=params.get("window", 20), alpha=params.get("std", 2.0))
        entries = close.vbt.crossed_below(bb.lower)
        exits = close.vbt.crossed_above(bb.upper)
    elif strategy == "sma_cross":
        fast = vbt.MA.run(close, params.get("fast", 10))
        slow = vbt.MA.run(close, params.get("slow", 50))
        entries = fast.ma_crossed_above(slow.ma)
        exits = fast.ma_crossed_below(slow.ma)
    else:
        return None
    # EOD-flat: force-exit at each session close (+ final bar so stats aren't nan)
    exits = (exits.astype(bool) | eod.astype(bool)).copy()
    exits.iloc[-1] = True
    try:
        pf = vbt.Portfolio.from_signals(close, entries, exits, freq="5min",
                                        fees=fees, init_cash=cash)
        st = pf.stats()
        return {
            "total_return": _stat(st, "Total Return [%]"),
            "win_rate":     _stat(st, "Win Rate [%]"),
            "sharpe":       _stat(st, "Sharpe Ratio", 3),
            "max_drawdown": _stat(st, "Max Drawdown [%]"),
            "profit_factor":_stat(st, "Profit Factor"),
            "num_trades":   int(_s(st.get("Total Trades", 0))),
            "final_value":  round(_s(pf.final_value()), 2),
            "bar":          BAR_LABEL,
        }
    except Exception as e:
        logger.debug("_run_intraday %s error: %s", strategy, e)
        return None


def _sweep_intraday(close, strategy: str) -> dict:
    """Param sweep → best params by total return (keep-if-better)."""
    best = {"total_return": -9999}
    best_params: dict = {}
    n = len(close)
    if strategy == "rsi":
        for w in [x for x in RSI_SWEEP["windows"] if x < n - 5] or [min(RSI_SWEEP["windows"])]:
            for e in RSI_SWEEP["entry_thresholds"]:
                for x in RSI_SWEEP["exit_thresholds"]:
                    if e >= x:
                        continue
                    r = _run_intraday(close, "rsi", {"window": w, "entry": e, "exit": x})
                    if r and r["total_return"] > best["total_return"]:
                        best, best_params = r, {"window": w, "entry": e, "exit": x}
    elif strategy == "macd":
        for f in MACD_SWEEP["fast_periods"]:
            for s in [sp for sp in MACD_SWEEP["slow_periods"] if sp <= n - 5]:
                if f >= s:
                    continue
                for sig in MACD_SWEEP["signal_periods"]:
                    r = _run_intraday(close, "macd", {"fast": f, "slow": s, "signal": sig})
                    if r and r["total_return"] > best["total_return"]:
                        best, best_params = r, {"fast": f, "slow": s, "signal": sig}
    elif strategy == "bollinger":
        for w in [x for x in BB_SWEEP["windows"] if x < n - 5] or [min(BB_SWEEP["windows"])]:
            for std in BB_SWEEP["std_devs"]:
                r = _run_intraday(close, "bollinger", {"window": w, "std": std})
                if r and r["total_return"] > best["total_return"]:
                    best, best_params = r, {"window": w, "std": std}
    elif strategy == "sma_cross":
        for f in SMA_SWEEP["fast_windows"]:
            for s in [sp for sp in SMA_SWEEP["slow_windows"] if sp <= n - 5]:
                if f >= s:
                    continue
                r = _run_intraday(close, "sma_cross", {"fast": f, "slow": s})
                if r and r["total_return"] > best["total_return"]:
                    best, best_params = r, {"fast": f, "slow": s}
    return best_params


# ── ORCHESTRATOR ──────────────────────────────────────────────────────────────

def run_holly_intraday(days: int = 30, top_n: int = 50) -> dict:
    """Intraday-flat nightly: top movers × strategies × param-sweep, EOD-flat.

    Fails LOUD if vectorbt missing (same discipline as the daily repair).
    """
    _init_intraday_tables()
    try:
        import vectorbt as _probe  # noqa: F401
    except Exception as e:
        msg = (f"Holly INTRADAY aborted — vectorbt unavailable ({type(e).__name__}). "
               f"Run under .venv-backtest.")
        logger.error(msg)
        try:
            from engine.alert_channels import send_alert, AlertLevel
            send_alert(message=msg, level=AlertLevel.WARNING,
                       alert_type="hm-holly-intraday-vectorbt-missing", rate_limit_secs=86400)
        except Exception:
            pass
        return {"status": "error", "message": msg, "cause": "vectorbt_missing"}

    run_date = datetime.now().strftime("%Y-%m-%d")
    tickers = _get_top_volume_movers(top_n) or []
    strategies = ["rsi", "macd", "bollinger", "sma_cross"]
    results: list[dict] = []

    for tk in tickers:
        close = _fetch_polygon_intraday(tk, days=days)
        if close is None or len(close) < 100:   # need enough intraday bars
            continue
        for strat in strategies:
            try:
                bp = _sweep_intraday(close, strat)
                if not bp:
                    continue
                m = _run_intraday(close, strat, bp)
                if not m:
                    continue
                results.append({"ticker": tk, "strategy": strat, "params": bp, **m})
            except Exception as e:
                logger.error("Holly intraday %s/%s: %s", tk, strat, e)

    if not results:
        return {"status": "error", "message": "No intraday results", "cause": "no_results"}

    results.sort(key=lambda x: x["total_return"], reverse=True)
    conn = _conn_backtest()
    conn.execute("DELETE FROM holly_intraday_winners WHERE run_date=?", (run_date,))
    for i, w in enumerate(results[:10], start=1):
        conn.execute(
            "INSERT OR REPLACE INTO holly_intraday_winners"
            "(run_date, rank, ticker, strategy, params, total_return, sharpe, win_rate, profit_factor, bar)"
            " VALUES (?,?,?,?,?,?,?,?,?,?)",
            (run_date, i, w["ticker"], w["strategy"], json.dumps(w["params"]),
             w["total_return"], w["sharpe"], w["win_rate"], w["profit_factor"], BAR_LABEL),
        )
    conn.commit()
    conn.close()
    return {
        "status": "ok",
        "run_date": run_date,
        "bar": BAR_LABEL,
        "tickers": len(tickers),
        "total_runs": len(results),
        "top_10": results[:10],
    }


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    r = run_holly_intraday(days=int(sys.argv[1]) if len(sys.argv) > 1 else 20,
                           top_n=int(sys.argv[2]) if len(sys.argv) > 2 else 8)
    print("status=%s runs=%s top=%s" % (
        r.get("status"), r.get("total_runs"),
        [(w["ticker"], w["strategy"], round(w["total_return"], 1)) for w in r.get("top_10", [])[:3]]))
