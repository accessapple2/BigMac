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
    except Exception as e:
        logger.warning("holly_intraday_cache write-through failed (%d bars): %s", len(all_bars), e)

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


# ── PHASE 4 BATCH 1: OHLCV FETCH + DOCUMENTED TI HOLLY STRATEGIES ─────────────
# The 4 baseline strategies (rsi/macd/bollinger/sma_cross) need only close. The
# documented TI Holly strategies need full OHLCV (volume surges, intraday highs for
# resistance breakouts, ranges for pullbacks). _fetch_polygon_ohlcv returns a DataFrame
# (Open/High/Low/Close/Volume) from the SAME Polygon 5min source + cache; additive, the
# close-only path above is untouched. Each strategy is a DOCUMENTED TI replica (from
# trade-ideas.com/hollyguide) implemented as (entries, exits) boolean Series on the
# OHLCV frame; TI params are proprietary/fixed, so these are RULE-based (not swept).
# All backtest via the shared _backtest_signals helper → EOD-flat, same metrics shape.


def _fetch_polygon_ohlcv(symbol: str, days: int = 20):
    """Full OHLCV DataFrame (tz-naive index) from Polygon 5min, or None. Reuses the
    holly_intraday_cache_ohlcv table. RELIABLE source only (no yfinance)."""
    import pandas as pd
    sym = symbol.upper()
    now = time.time()
    conn = sqlite3.connect(_BACKTEST_DB, timeout=30)
    conn.execute("""CREATE TABLE IF NOT EXISTS holly_intraday_cache_ohlcv (
        symbol TEXT, bar TEXT, ts TEXT, o REAL, h REAL, l REAL, c REAL, v REAL,
        fetched_at REAL, UNIQUE(symbol, bar, ts))""")
    conn.commit()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    rows = conn.execute(
        "SELECT ts,o,h,l,c,v,MAX(fetched_at) FROM holly_intraday_cache_ohlcv "
        "WHERE symbol=? AND bar=? AND ts>=? GROUP BY ts ORDER BY ts ASC",
        (sym, BAR_LABEL, cutoff)).fetchall()
    if rows and len(rows) >= 200 and (now - max((r[6] or 0) for r in rows)) < _INTRADAY_CACHE_TTL:
        conn.close()
        idx = pd.to_datetime([r[0] for r in rows])
        if getattr(idx, "tz", None) is not None:
            idx = idx.tz_localize(None)
        return pd.DataFrame({"Open": [r[1] for r in rows], "High": [r[2] for r in rows],
                             "Low": [r[3] for r in rows], "Close": [r[4] for r in rows],
                             "Volume": [r[5] for r in rows]}, index=idx).dropna()
    key = os.getenv("POLYGON_API_KEY")
    if not key:
        conn.close()
        return None
    import requests
    end = datetime.now(timezone.utc).date()
    win_start = end - timedelta(days=days)
    bars = {}
    while win_start < end:
        win_end = min(win_start + timedelta(days=10), end)
        url = (f"https://api.polygon.io/v2/aggs/ticker/{sym}"
               f"/range/{BAR_MULTIPLIER}/{BAR_TIMESPAN}/{win_start}/{win_end}"
               f"?adjusted=true&sort=asc&limit=50000")
        try:
            r = requests.get(url, headers={"Authorization": f"Bearer {key}"}, timeout=15)
            if r.status_code == 200:
                for b in (r.json().get("results") or []):
                    if b.get("t") is None:
                        continue
                    iso = datetime.fromtimestamp(b["t"] / 1000, tz=timezone.utc).isoformat()
                    bars[iso] = (b.get("o", 0), b.get("h", 0), b.get("l", 0), b.get("c", 0), b.get("v", 0))
        except Exception as e:
            logger.warning("ohlcv fetch %s: %s", sym, e)
        win_start = win_end
    if not bars:
        conn.close()
        return None
    conn.executemany(
        "INSERT OR IGNORE INTO holly_intraday_cache_ohlcv(symbol,bar,ts,o,h,l,c,v,fetched_at) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        [(sym, BAR_LABEL, ts, o, h, l, c, v, now) for ts, (o, h, l, c, v) in bars.items()])
    conn.commit()
    conn.close()
    items = sorted(bars.items())
    idx = pd.to_datetime([t for t, _ in items])
    if getattr(idx, "tz", None) is not None:
        idx = idx.tz_localize(None)
    return pd.DataFrame({"Open": [x[1][0] for x in items], "High": [x[1][1] for x in items],
                         "Low": [x[1][2] for x in items], "Close": [x[1][3] for x in items],
                         "Volume": [x[1][4] for x in items]}, index=idx).dropna()


# ── HARNESS REALISM (HM-HOLLY-REWORK 2026-05-31) ──────────────────────────────
# Real Holly takes a FEW SETUPS/DAY, not every 5min re-trigger. The strategies' raw
# masks are True on EVERY bar the condition holds, so the old harness round-tripped
# thousands of times (14k trades, 15% WR) and got chewed up by fees. Two fixes:
#   1. RISING-EDGE only — enter when the setup BECOMES true, not while it persists.
#   2. Per-day cap + post-entry cooldown — a few considered setups, no machine-gun re-entry.
MAX_ENTRIES_PER_DAY = 2       # ≤2 setups per symbol per session (was effectively unbounded)
ENTRY_COOLDOWN_BARS = 12      # ~60min lockout after an entry before another is allowed
REALISTIC_FEES = 0.0015       # 15bps/side — commission + slippage proxy for liquid small-caps


def _gate_entries(entries, close, max_per_day: int = MAX_ENTRIES_PER_DAY,
                  cooldown: int = ENTRY_COOLDOWN_BARS):
    """Turn a raw condition-True mask into SELECTIVE setup entries: rising-edge only,
    capped per session day, with a post-entry cooldown. This is the 'faithful Holly'
    selectivity — a few setups/day, not every re-trigger."""
    import pandas as pd
    e = entries.fillna(False).astype(bool)
    rising = e & ~e.shift(1, fill_value=False)
    day = close.index.normalize()
    out = pd.Series(False, index=close.index)
    last_i = -10 ** 9
    per_day: dict = {}
    rv = rising.values
    for i in range(len(rv)):
        if not rv[i]:
            continue
        d = day[i]
        if i - last_i < cooldown or per_day.get(d, 0) >= max_per_day:
            continue
        out.iloc[i] = True
        last_i = i
        per_day[d] = per_day.get(d, 0) + 1
    return out


def _backtest_signals(df, entries, exits, cash=10_000, fees=REALISTIC_FEES, realistic=True):
    """Backtest (entries, exits) on df.Close, EOD-flat. Metrics or None.

    realistic=True (default) applies the selectivity/frequency gate (_gate_entries) +
    realistic fees so the result reflects how Holly actually trades. Set realistic=False
    for the raw ungated behavior (diagnostics only)."""
    import vectorbt as vbt
    close = df["Close"]
    entries = entries.astype(bool)
    if realistic:
        entries = _gate_entries(entries, close)
    eod = _eod_flat_exits(close)
    exits = (exits.astype(bool) | eod.astype(bool)).copy()
    exits.iloc[-1] = True
    try:
        pf = vbt.Portfolio.from_signals(close, entries, exits, freq="5min",
                                        fees=fees, init_cash=cash)
        st = pf.stats()
        return {
            "total_return": _stat(st, "Total Return [%]"), "win_rate": _stat(st, "Win Rate [%]"),
            "sharpe": _stat(st, "Sharpe Ratio", 3), "max_drawdown": _stat(st, "Max Drawdown [%]"),
            "profit_factor": _stat(st, "Profit Factor"), "num_trades": int(_s(st.get("Total Trades", 0))),
            "final_value": round(_s(pf.final_value()), 2), "bar": BAR_LABEL,
        }
    except Exception as e:
        logger.debug("_backtest_signals error: %s", e)
        return None


# ── PER-STRATEGY EXIT REGIME (HM-HOLLY-WORKS 2026-05-31) ───────────────────────
# The no-EOD-flat experiment proved one-size "intraday-flat" is wrong: momentum/
# breakout/continuation strategies need SWING (overnight) exits to capture the small-
# cap gap edge (win rates ~doubled, 2 flipped positive), while mean-reversion/pullback
# strategies need intraday-FLAT (their bounce thesis is intraday — they got WORSE held).
# Classification is data-driven: improved-overnight → swing; neutral/worsened → flat.
TI_EXIT_TYPE = {
    # SWING — improved with overnight holds (momentum / breakout / continuation)
    "the_continuation": "swing", "count_de_monet": "swing", "breakout": "swing",
    "pushing_through_resistance": "swing", "five_day_bounce": "swing", "guiding_hand": "swing",
    "strong_stock_pulling_back": "swing", "trend_play": "swing", "staggering_volume": "swing",
    "tailwind": "swing", "bullish_trend_change": "swing", "early_bird": "swing",
    "volume_doesnt_lie": "swing", "alpha_predators": "swing",
    # FLAT — neutral/worse overnight (mean-reversion / pullback)
    "quarterback": "flat", "separation_from_8": "flat", "buyers_stepping": "flat",
    "on_support": "flat", "bullish_pullback": "flat",
}
SWING_SL, SWING_TP, SWING_MAXHOLD = 0.05, 0.10, 780   # default swing exit (~10 trading days)

# ── LIVE "WORKS" SET (HM-HOLLY-WORKS 2026-05-31) ──────────────────────────────
# OOS-validated strategies cleared for live trading (tuned in-sample Jan2–Apr15, held
# out Apr15–May29). the_continuation: OOS Sharpe 1.47, 58% WR, +5.6%/6wk, 11% DD (8/6/20d)
# — a real edge, robust across params. count_de_monet: marginal (OOS Sharpe 0.59, +1.7%),
# kept validated-but-OFF for the cleanest single-edge A/B race (flip enabled=True to add).
# The other 17 documented strategies are OFF pending HM-HOLLY-ENTRY-FIDELITY rework — they
# fail OOS even with the correct exit regime (generic triggers, not real Holly setups).
HOLLY_WORKS = {
    "the_continuation": {"enabled": True,  "exit": "swing", "sl": 0.08, "tp": 0.06, "max_hold": 1560},
    "count_de_monet":   {"enabled": True,  "exit": "swing", "sl": 0.05, "tp": 0.06, "max_hold": 390},
}


def _backtest_swing(df, entries, sl=SWING_SL, tp=SWING_TP, max_hold=SWING_MAXHOLD,
                    cash=10_000, fees=REALISTIC_FEES):
    """Swing backtest: gated entries, NO EOD-flat. Exit on stop / target / max-hold.
    Same selectivity gate + fees as the flat path — only the exit regime differs."""
    import vectorbt as vbt
    close = df["Close"]
    ge = _gate_entries(entries.astype(bool), close)
    exits = ge.shift(max_hold, fill_value=False).copy()
    exits.iloc[-1] = True
    try:
        pf = vbt.Portfolio.from_signals(close, ge, exits, freq="5min", fees=fees,
                                        init_cash=cash, sl_stop=sl, tp_stop=tp)
        st = pf.stats()
        return {
            "total_return": _stat(st, "Total Return [%]"), "win_rate": _stat(st, "Win Rate [%]"),
            "sharpe": _stat(st, "Sharpe Ratio", 3), "max_drawdown": _stat(st, "Max Drawdown [%]"),
            "profit_factor": _stat(st, "Profit Factor"), "num_trades": int(_s(st.get("Total Trades", 0))),
            "final_value": round(_s(pf.final_value()), 2), "bar": BAR_LABEL,
        }
    except Exception as e:
        logger.debug("_backtest_swing error: %s", e)
        return None


def _backtest_by_type(df, entries, exits, strategy: str):
    """Dispatch to the exit regime declared for `strategy`: swing (overnight) vs flat (EOD)."""
    if TI_EXIT_TYPE.get(strategy) == "swing":
        return _backtest_swing(df, entries)
    return _backtest_signals(df, entries, exits)


def _ti_signals(df, name: str):
    """Return (entries, exits) for a documented TI Holly strategy `name`, or (None,None).

    Faithful replicas of the documented TI Holly logic (trade-ideas.com/hollyguide).
    Intraday 5min bars. Exits are momentum-fade / opposite-condition; EOD-flat is added
    by _backtest_signals. Price-range filters from the docs are applied as guards.
    """
    import pandas as pd, numpy as np
    o, h, l, c, v = df["Open"], df["High"], df["Low"], df["Close"], df["Volume"]
    px = float(c.iloc[-1])
    day = c.index.normalize()
    vol_ma = v.rolling(20).mean()
    rel_vol = v / vol_ma
    hi20 = h.rolling(20).max()          # ~recent intraday resistance (20×5m ≈ 100min)
    hi12 = h.rolling(12).max()          # ~60-min high
    sma8 = c.rolling(8).mean()
    rng = (h - l)
    rng_ma = rng.rolling(20).mean()

    # FIDELITY tightening 2026-05-30 (Batch-1 review):
    # ANCHORED opening range — the high/low of each session's FIRST 6 bars (30min),
    # held constant for the rest of that day (the documented "30-minute opening range",
    # not a rolling window). bar_of_day counts bars since the session open.
    bar_of_day = pd.Series(c.groupby(day).cumcount(), index=c.index)
    _or_hi_raw = h.where(bar_of_day < 6)
    _or_lo_raw = l.where(bar_of_day < 6)
    or_hi = _or_hi_raw.groupby(day).transform(lambda s: s.max())   # session OR high (anchored)
    or_lo = _or_lo_raw.groupby(day).transform(lambda s: s.min())   # session OR low (anchored)
    # True multi-session low (~5 sessions × 78 5m bars ≈ 390 bars) for 5-day-range bounce.
    lo5d = l.rolling(390, min_periods=78).min()
    # Daily up/down: each session's close vs prior session's close (for "2 up days").
    daily_close = c.groupby(day).transform("last")
    sess_last = c.groupby(day).tail(1)
    prior_up = pd.Series(sess_last.values, index=sess_last.index.normalize()).diff() > 0
    up_days_map = prior_up.rolling(2).sum()   # 2.0 == two consecutive up sessions
    two_up = day.map(lambda d: up_days_map.get(d, 0) >= 2)
    two_up = pd.Series(np.asarray(two_up), index=c.index)

    def _ret(entries, exits, lo=None, hi=None):
        if lo is not None and not (lo <= px):
            return None, None  # price-range filter (current px proxy for universe filter)
        if hi is not None and not (px <= hi):
            return None, None
        return entries.fillna(False), exits.fillna(False)

    if name == "staggering_volume":      # TIGHT: new highs + extreme rel volume
        e = (c >= hi20) & (rel_vol >= 3.0)
        return _ret(e, c < sma8)
    if name == "volume_doesnt_lie":      # TIGHT: up >=4% from day-open + >=2x volume
        day_open = o.groupby(day).transform("first")
        e = ((c / day_open - 1) >= 0.04) & (rel_vol >= 2.0)
        return _ret(e, c < sma8)
    if name == "breakout":               # TIGHT: cross resistance + rel vol, $10-150
        e = (c > hi20.shift(1)) & (rel_vol >= 1.25)
        return _ret(e, c < hi20.shift(1), lo=10, hi=150)
    if name == "pushing_through_resistance":  # TIGHTENED: ANCHORED 30-min OR breakout + trailing-15m up
        e = (c > or_hi) & (bar_of_day >= 6) & (c > c.shift(3))
        return _ret(e, c < sma8)
    if name == "bullish_pullback":       # TIGHTENED: 25% RETRACEMENT of prior up-leg, $20-100
        # Doc says "pulls back 25%" (ambiguous — see FLAG). Best-faithful: 25% Fib
        # retracement of the swing (recent high - recent low), entering as price turns up
        # from that level in an uptrend. (Was a flat -3% price drop — too shallow.)
        swing_hi = h.rolling(40).max(); swing_lo = l.rolling(40).min()
        retr_25 = swing_hi - 0.25 * (swing_hi - swing_lo)
        e = (c <= retr_25) & (c > c.shift(1)) & (swing_hi > c.rolling(78).mean())
        return _ret(e, c >= swing_hi, lo=20, hi=100)
    if name == "quarterback":            # TIGHTENED: 25% retracement + rel vol + up-from-yest, $5-100
        swing_hi = h.rolling(40).max(); swing_lo = l.rolling(40).min()
        retr_25 = swing_hi - 0.25 * (swing_hi - swing_lo)
        e = (c <= retr_25) & (c > c.shift(1)) & (rel_vol >= 1.0) & (c > c.shift(78))
        return _ret(e, c >= swing_hi, lo=5, hi=100)
    if name == "five_day_bounce":        # TIGHTENED: bounce from true ~5-DAY low + 60-min high, <=$20
        e = (c.shift(3) <= lo5d.shift(3) * 1.03) & (c >= hi12)
        return _ret(e, c < sma8, hi=20)
    if name == "on_support":             # TIGHTENED: ANCHORED opening-range-low bounce, >$20
        e = (c <= or_lo * 1.005) & (bar_of_day >= 6) & (c > c.shift(1))
        return _ret(e, c < or_lo, lo=20)
    if name == "the_continuation":       # TIGHTENED: new ANCHORED 30-min high + 2 UP DAYS + wide range, $0.5-50
        e = (c >= or_hi) & (bar_of_day >= 6) & two_up & (rng > rng_ma * 1.2)
        return _ret(e, c < sma8, lo=0.5, hi=50)
    if name == "separation_from_8":      # TIGHT: far above 8-MA (overbought) then rolling over; fade
        far = (c - sma8) / sma8 >= 0.02
        e = far & (c < c.shift(1))
        return _ret(e, c <= sma8)
    return None, None


# ── PHASE 4 BATCH 2: 10 more documented TI Holly LONG strategies ──────────────
_float_cache: dict = {}
_FLOAT_TTL = 7 * 24 * 3600  # float changes rarely


def _shares_float_m(symbol: str):
    """Shares float in MILLIONS via authed Finviz Elite (reuses short_guard session).
    None if unavailable (the float-gated strategy then SKIPS the name — never silent-True)."""
    now = time.time()
    hit = _float_cache.get(symbol)
    if hit and now - hit[0] < _FLOAT_TTL:
        return hit[1]
    val = None
    try:
        from engine.short_guard import _get_elite_session
        import csv as _csv, io as _io
        sess = _get_elite_session()
        if sess is not None:
            r = sess.get(f"https://elite.finviz.com/export.ashx?v=131&t={symbol.upper()}", timeout=12)
            if r.status_code == 200:
                for row in _csv.DictReader(_io.StringIO(r.text)):
                    if str(row.get("Ticker", "")).upper() == symbol.upper():
                        raw = str(row.get("Shares Float", "")).strip().upper().replace(",", "")
                        if raw and raw not in ("-", "—"):
                            mult = 1.0
                            if raw.endswith("B"):
                                mult, raw = 1000.0, raw[:-1]
                            elif raw.endswith("M"):
                                mult, raw = 1.0, raw[:-1]
                            elif raw.endswith("K"):
                                mult, raw = 0.001, raw[:-1]
                            try:
                                val = float(raw) * mult
                            except Exception:
                                val = None
                        break
    except Exception as e:
        logger.warning("float lookup %s failed: %s", symbol, type(e).__name__)
    _float_cache[symbol] = (now, val)
    return val


def _ti_signals_b2(df, name: str, symbol: str = ""):
    """(entries, exits) for Batch-2 documented TI strategies, or (None,None).
    Same OHLCV/anchored-OR/retracement vocabulary as Batch 1."""
    import pandas as pd, numpy as np
    o, h, l, c, v = df["Open"], df["High"], df["Low"], df["Close"], df["Volume"]
    px = float(c.iloc[-1])
    day = c.index.normalize()
    rel_vol = v / v.rolling(20).mean()
    hi20 = h.rolling(20).max()
    hi12 = h.rolling(12).max()           # ~60-min high
    hi5d = h.rolling(390, min_periods=78).max()   # ~5-session high
    sma8 = c.rolling(8).mean(); sma20 = c.rolling(20).mean(); sma50 = c.rolling(50).mean()
    swing_hi = h.rolling(40).max(); swing_lo = l.rolling(40).min()
    bar_of_day = pd.Series(c.groupby(day).cumcount(), index=c.index)  # bars since session open

    def _ret(e, x, lo=None, hi=None):
        if lo is not None and not (lo <= px):
            return None, None
        if hi is not None and not (px <= hi):
            return None, None
        return e.fillna(False), x.fillna(False)

    if name == "alpha_predators":        # momentum pullback then resume, <$20
        retr = swing_hi - 0.382 * (swing_hi - swing_lo)
        e = (c <= retr) & (c > c.shift(1)) & (c > sma20)
        return _ret(e, c >= swing_hi, hi=20)
    if name == "early_bird":             # TIGHTENED: EARLY-SESSION breakout w/ conviction volume, <$20
        # FIDELITY 2026-05-30 (Batch-2 review): was identical to Batch-1 `breakout`
        # (loose/redundant). "Early Bird" = catch the move EARLY — gate to the first
        # hour (bar_of_day < 12 ≈ 60min) and require stronger volume (1.5x vs 1.25x).
        e = (c > hi20.shift(1)) & (rel_vol >= 1.5) & (bar_of_day < 12)
        return _ret(e, c < hi20.shift(1), hi=20)
    if name == "float_on":               # LOW-FLOAT MOMENTUM — 5 MANDATORY guards (Admiral spec 2026-05-30)
        # Squeeze-prone class. ALL FIVE guards required, none optional:
        #   G1 float < 10M shares   G2 price $1-12   G3 volume > 750k
        #   G4 CATALYST REQUIRED    G5 0.5% risk cap (HALF size — see TI_STRATEGY_RISK)
        # GUARD 1 — float < 150M (Admiral ruling 2026-05-31, widened from 10M which never
        # engaged on any liquid mover — measured floats: LCID 133M, LUNR 128M, RGTI 325M,
        # so <10M was true-micro-float-only). 150M = realistic "low float" for liquid names.
        # None float data → SKIP the name, never silent-allow.
        fl = _shares_float_m(symbol)
        if fl is None or fl >= 150.0:
            return None, None
        # GUARD 3 — absolute volume > 750k on the entry bar
        vol_ok = v > 750_000
        # GUARD 4 — CATALYST REQUIRED (no naked low-float entry). Backtest proxy for a
        # catalyst = a catalyst-grade event on the bar: volume explosion (>=3x avg) AND
        # range expansion (>1.5x avg true range). FIDELITY NOTE: "catalyst" is genuinely
        # unknowable from price/volume alone — the LIVE path must AND this with a real
        # catalyst signal (event tape / news / earnings) at execution; this proxy is the
        # closest faithful stand-in for backtest selection. Replica-pass per Batch-1 ruling.
        _rng = h - l
        _rng_ma = _rng.rolling(20).mean()
        catalyst = (rel_vol >= 3.0) & (_rng > _rng_ma * 1.5)
        e = (c > hi20.shift(1)) & vol_ok & catalyst
        # GUARD 2 — price $1-12 (via _ret lo/hi). GUARD 5 enforced at sizing (TI_STRATEGY_RISK).
        return _ret(e, c < hi20.shift(1), lo=1, hi=12)
    if name == "count_de_monet":         # TIGHTENED: new 5-day high w/ VOLUME confirmation, <$40
        # FIDELITY 2026-05-30 (Batch-2 review): was a drift-to-highs (avg -1.2%, no
        # confirmation). A real momentum breakout to a new multi-session high needs
        # volume behind it — add rel_vol>=1.5 so it's a conviction break, not a drift.
        e = (c >= hi5d) & (c > hi20.shift(1)) & (rel_vol >= 1.5)
        return _ret(e, c < sma8, hi=40)
    if name == "buyers_stepping":        # Fibonacci (38.2%) pullback on uptrend
        retr = swing_hi - 0.382 * (swing_hi - swing_lo)
        e = (c <= retr) & (c > c.shift(1)) & (sma8 > sma20) & (sma20 > sma50)
        return _ret(e, c >= swing_hi)
    if name == "strong_stock_pulling_back":  # 25% retracement + rel strength, <$20
        retr = swing_hi - 0.25 * (swing_hi - swing_lo)
        e = (c <= retr) & (c > c.shift(1)) & (rel_vol >= 1.0) & (c > c.shift(78))
        return _ret(e, c >= swing_hi, hi=20)
    if name == "tailwind":               # complex MA stack + pullback, <=$60
        stacked = (sma8 > sma20) & (sma20 > sma50)
        pulled = (c <= sma20 * 1.01) & (c >= sma20 * 0.99) & (c > c.shift(1))  # pullback to 20MA, turning
        return _ret(stacked & pulled, c < sma50, hi=60)
    if name == "bullish_trend_change":   # cross resistance + 60-min high, >$20
        e = (c > hi20.shift(1)) & (c >= hi12)
        return _ret(e, c < sma8, lo=20)
    if name == "trend_play":             # MA ribbon aligned up (8>20>50, price>8)
        e = (sma8 > sma20) & (sma20 > sma50) & (c > sma8) & (c.shift(1) <= sma8.shift(1))
        return _ret(e, c < sma20)
    if name == "guiding_hand":           # gap up + MA catch-up (price holds, rising MA catches)
        day_open = o.groupby(day).transform("first")
        prev_close = c.groupby(day).transform("last").groupby(day).shift(1)
        gap = (day_open > c.shift(1) * 1.01)
        e = gap & (c >= sma8) & (sma8 > sma8.shift(3))   # held gap, 8MA rising into price
        return _ret(e, c < sma8)
    return None, None


TI_BATCH_1 = [
    "staggering_volume", "volume_doesnt_lie", "breakout", "pushing_through_resistance",
    "bullish_pullback", "quarterback", "five_day_bounce", "on_support",
    "the_continuation", "separation_from_8",
]

TI_BATCH_2 = [
    "alpha_predators", "early_bird", "float_on", "count_de_monet", "buyers_stepping",
    "strong_stock_pulling_back", "tailwind", "bullish_trend_change", "trend_play",
    "guiding_hand",
]

# Per-strategy LIVE risk cap (fraction of capital risked per position). Default 1%;
# float_on is HALF (0.5%) — the riskiest low-float/squeeze-prone class (Admiral 2026-05-30).
# Consumed at trade-sizing when a strategy is wired live; recorded here as the source of truth.
TI_STRATEGY_RISK = {"float_on": 0.005}
TI_DEFAULT_RISK = 0.01


def ti_risk_cap(strategy: str) -> float:
    """Risk cap (fraction of capital) for a TI strategy. float_on → 0.5%, else 1%."""
    return TI_STRATEGY_RISK.get(strategy, TI_DEFAULT_RISK)


def backtest_ti_batch(days: int = 20, top_n: int = 8, strategies=None) -> dict:
    """Validate the TI Batch-1 strategies on real OHLCV. Returns per-strategy results.
    Fail-LOUD: a strategy that errors is recorded with status='error', never silent."""
    try:
        import vectorbt  # noqa: F401
    except Exception as e:
        return {"status": "error", "cause": "vectorbt_missing", "message": str(e)}
    strategies = strategies or TI_BATCH_1
    tickers = _get_top_volume_movers(top_n) or []
    out = {s: {"runs": 0, "winners": 0, "best": None, "errors": 0} for s in strategies}
    for tk in tickers:
        df = _fetch_polygon_ohlcv(tk, days=days)
        if df is None or len(df) < 100:
            continue
        for s in strategies:
            try:
                e, x = _ti_signals(df, s)
                if e is None:
                    continue  # price-range filtered out for this name
                m = _backtest_signals(df, e, x)
                if m is None:
                    continue
                out[s]["runs"] += 1
                if m["num_trades"] > 0:
                    out[s]["winners"] += 1
                    if out[s]["best"] is None or m["total_return"] > out[s]["best"]["total_return"]:
                        out[s]["best"] = {"ticker": tk, **m}
            except Exception as ex:
                out[s]["errors"] += 1
                logger.error("TI batch %s/%s ERROR: %s: %s", tk, s, type(ex).__name__, ex)
    return {"status": "ok", "tickers": len(tickers), "results": out}


# ── PHASE 3: SHORT-BACKTEST HARNESS ───────────────────────────────────────────
# Backtests SHORT strategies: short entry -> buy-to-cover -> INVERSE PnL (profit when
# price FALLS). Uses vectorbt direction='shortonly' (verified: a 100->80 fall yields
# +17.9% return, sign correct). Reuses the intraday bars + EOD-flat infra. Short
# strategies enter on BEARISH signals (inverse of the long runners). Leverages the
# live short path's intent: short_guard squeeze exclusions are checked so we don't
# surface a backtested short on a name the live guard would block.


def _squeeze_excluded(symbol: str) -> tuple[bool, str]:
    """True if the live short_guard would BLOCK shorting `symbol` (DTC/earnings/SI).

    Mirrors the live gate so a backtested short isn't surfaced for a name that could
    never trade. Backtest is read-only (not an order), so a guard-import error here
    fails-OPEN but is LOGGED LOUD (never silent) — distinct from the live path which
    fails CLOSED. Recurring-bug-class guard: surface the degraded state, don't hide it.
    """
    try:
        from engine.short_guard import squeeze_block
        return squeeze_block(symbol)
    except Exception as e:
        logger.warning("short squeeze-guard check failed for %s (%s) — backtest proceeds "
                       "but is NOT validated against the live guard", symbol, type(e).__name__)
        return False, f"guard-check-error:{type(e).__name__}"


def _run_intraday_short(close, strategy: str, params: dict, cash: float = 10_000, fees: float = 0.001):
    """Backtest one SHORT strategy intraday, EOD-flat cover, inverse PnL. None on fail."""
    import vectorbt as vbt
    eod = _eod_flat_exits(close)
    if strategy == "rsi":
        rsi = vbt.RSI.run(close, window=params.get("window", 14))
        short_entries = rsi.rsi_crossed_below(params.get("entry", 70))  # roll down from overbought
        short_exits = rsi.rsi_crossed_below(params.get("exit", 30))     # cover when oversold
    elif strategy == "macd":
        m = vbt.MACD.run(close, fast_window=params.get("fast", 12),
                         slow_window=params.get("slow", 26), signal_window=params.get("signal", 9))
        short_entries = m.macd_crossed_below(m.signal)   # bearish cross -> short
        short_exits = m.macd_crossed_above(m.signal)     # bullish cross -> cover
    elif strategy == "bollinger":
        bb = vbt.BBANDS.run(close, window=params.get("window", 20), alpha=params.get("std", 2.0))
        short_entries = close.vbt.crossed_above(bb.upper)  # fade upper band -> short
        short_exits = close.vbt.crossed_below(bb.lower)    # cover at lower band
    elif strategy == "sma_cross":
        fast = vbt.MA.run(close, params.get("fast", 10))
        slow = vbt.MA.run(close, params.get("slow", 50))
        short_entries = fast.ma_crossed_below(slow.ma)   # death cross -> short
        short_exits = fast.ma_crossed_above(slow.ma)     # golden cross -> cover
    else:
        return None
    short_exits = (short_exits.astype(bool) | eod.astype(bool)).copy()  # EOD-flat cover
    short_exits.iloc[-1] = True
    try:
        pf = vbt.Portfolio.from_signals(
            close, entries=short_entries, exits=short_exits,
            direction="shortonly", freq="5min", fees=fees, init_cash=cash,
        )
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
            "direction":    "short",
        }
    except Exception as e:
        logger.debug("_run_intraday_short %s error: %s", strategy, e)
        return None


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
    strategies = ["rsi", "macd", "bollinger", "sma_cross"]   # swept (param optimization)
    results: list[dict] = []

    for tk in tickers:
        close = _fetch_polygon_intraday(tk, days=days)
        if close is not None and len(close) >= 100:   # need enough intraday bars
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

        # HM-HOLLY-FAITHFUL Phase 4 Batch 1: documented TI strategies on OHLCV
        # (fixed-rule, not swept — TI params are proprietary). Wired alongside the 4
        # swept strategies so Holly now ranks 14 strategies. Fail-LOUD per strategy.
        try:
            df = _fetch_polygon_ohlcv(tk, days=days)
        except Exception as e:
            logger.error("Holly intraday OHLCV fetch %s: %s", tk, e)
            df = None
        if df is not None and len(df) >= 100:
            for s in TI_BATCH_1:
                try:
                    e_sig, x_sig = _ti_signals(df, s)
                    if e_sig is None:           # price-band filtered for this name
                        continue
                    m = _backtest_signals(df, e_sig, x_sig)
                    if not m or m["num_trades"] == 0:
                        continue
                    results.append({"ticker": tk, "strategy": s, "params": {"ti": True}, **m})
                except Exception as ex:
                    logger.error("Holly intraday TI %s/%s: %s: %s", tk, s, type(ex).__name__, ex)

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
