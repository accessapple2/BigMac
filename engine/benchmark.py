"""benchmark.py — Competitive benchmarking vs SPY, QQQ, and 60/40 portfolio.

Computes TradeMinds fleet P&L vs passive benchmarks over rolling windows.
Fires underperformance alerts when fleet lags benchmark by >5% over 30 days.
Tables: benchmark_snapshots (INSERT-only, sacred).

Endpoint wired in dashboard/app.py:
  GET /api/benchmark          — rolling comparison data
  GET /api/benchmark/summary  — quick scorecard
"""
from __future__ import annotations

import logging
import sqlite3
import threading

logger = logging.getLogger(__name__)
import time
from datetime import date, datetime, timedelta
from typing import Any

DB = "autonomous_trader.db"
BENCHMARK_ETFs = {
    "SPY":  "S&P 500",
    "QQQ":  "NASDAQ 100",
    "AGG":  "60/40 Bond Proxy",
}

_lock = threading.Lock()
_CACHE: dict[str, Any] = {}
_CACHE_TS = 0.0
_CACHE_TTL = 600  # 10 min


# ── DB init ──────────────────────────────────────────────────────────────────

def _ensure_table() -> None:
    conn = sqlite3.connect(DB, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS benchmark_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            snap_date       TEXT NOT NULL,
            window_days     INTEGER NOT NULL,
            fleet_return    REAL,
            spy_return      REAL,
            qqq_return      REAL,
            agg_return      REAL,
            blend_60_40     REAL,
            fleet_sharpe    REAL,
            fleet_max_dd    REAL,
            alpha_vs_spy    REAL,
            alpha_vs_qqq    REAL,
            alpha_vs_60_40  REAL,
            calculated_at   TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    conn.close()


# ── Fleet P&L from trader.db ─────────────────────────────────────────────────

def _get_fleet_daily_pnl(days: int) -> list[dict]:
    """Aggregate all players' daily realized PNL from trades table."""
    try:
        since = (date.today() - timedelta(days=days)).isoformat()
        conn = sqlite3.connect("data/trader.db", timeout=10)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT
                date(executed_at) AS trade_date,
                SUM(COALESCE(
                    corrected_pnl,
                    CASE WHEN asset_type='option' OR option_type IS NOT NULL
                         THEN 0
                         ELSE realized_pnl
                    END
                )) AS daily_pnl
            FROM trades
            WHERE action = 'SELL'
              AND realized_pnl IS NOT NULL
              AND player_id != 'steve-webull'
              AND executed_at >= ?
            GROUP BY date(executed_at)
            ORDER BY trade_date
        """, (since,)).fetchall()
        conn.close()
        return [{"date": r["trade_date"], "pnl": r["daily_pnl"] or 0.0} for r in rows]
    except Exception:
        return []


def _get_fleet_starting_capital() -> float:
    """Sum of all non-steve starting cash across models."""
    try:
        conn = sqlite3.connect("data/trader.db", timeout=10)
        row = conn.execute("""
            SELECT SUM(cash) FROM portfolios
            WHERE player_id != 'steve-webull'
        """).fetchone()
        conn.close()
        return float(row[0] or 100000.0)
    except Exception:
        return 100000.0


# ── ETF returns via yfinance ─────────────────────────────────────────────────

def _get_etf_return(ticker: str, days: int) -> float | None:
    """Total return % for an ETF over the last N calendar days."""
    try:
        import yfinance as yf
        end = datetime.today()
        start = end - timedelta(days=days + 5)  # buffer for weekends
        hist = yf.download(ticker, start=start.strftime("%Y-%m-%d"),
                           end=end.strftime("%Y-%m-%d"), progress=False, auto_adjust=True)
        if hist.empty or len(hist) < 2:
            return None
        first_close = float(hist["Close"].iloc[0])
        last_close = float(hist["Close"].iloc[-1])
        return round((last_close / first_close - 1) * 100, 2)
    except Exception:
        return None


# ── Core computation ─────────────────────────────────────────────────────────

def compute_benchmark(days: int = 30) -> dict[str, Any]:
    """Compute fleet vs benchmark returns for the last N days."""
    with _lock:
        global _CACHE, _CACHE_TS
        if time.time() - _CACHE_TS < _CACHE_TTL and _CACHE.get("days") == days:
            return _CACHE

    result: dict[str, Any] = {
        "days": days,
        "calculated_at": datetime.utcnow().isoformat() + "Z",
        "fleet": {},
        "benchmarks": {},
        "alpha": {},
        "alert": None,
    }

    # Fleet performance
    pnl_rows = _get_fleet_daily_pnl(days)
    starting_cap = _get_fleet_starting_capital()
    total_pnl = sum(r["pnl"] for r in pnl_rows)
    fleet_return = round((total_pnl / starting_cap) * 100, 2) if starting_cap > 0 else 0.0

    # Sharpe (simplified: daily PNL / StdDev)
    import statistics
    daily_returns = [r["pnl"] / starting_cap * 100 for r in pnl_rows] if pnl_rows else []
    if len(daily_returns) >= 5:
        avg_r = statistics.mean(daily_returns)
        std_r = statistics.stdev(daily_returns) if len(daily_returns) > 1 else 1.0
        sharpe = round(avg_r / std_r * (252 ** 0.5), 2) if std_r > 0 else 0.0
    else:
        sharpe = None

    # Max drawdown
    cumulative = 0.0
    peak = 0.0
    max_dd = 0.0
    for r in pnl_rows:
        cumulative += r["pnl"]
        if cumulative > peak:
            peak = cumulative
        dd = peak - cumulative
        if dd > max_dd:
            max_dd = dd
    max_dd_pct = round(-max_dd / starting_cap * 100, 2) if starting_cap > 0 else 0.0

    result["fleet"] = {
        "return_pct": fleet_return,
        "total_pnl": round(total_pnl, 2),
        "starting_capital": round(starting_cap, 2),
        "sharpe": sharpe,
        "max_drawdown_pct": max_dd_pct,
        "trading_days": len(pnl_rows),
    }

    # Benchmark returns
    spy_ret = _get_etf_return("SPY", days)
    qqq_ret = _get_etf_return("QQQ", days)
    agg_ret = _get_etf_return("AGG", days)

    result["benchmarks"] = {
        "SPY": {"return_pct": spy_ret, "name": "S&P 500"},
        "QQQ": {"return_pct": qqq_ret, "name": "NASDAQ 100"},
        "AGG": {"return_pct": agg_ret, "name": "Bonds (AGG)"},
    }

    # 60/40 blend
    blend_60_40 = None
    if spy_ret is not None and agg_ret is not None:
        blend_60_40 = round(spy_ret * 0.6 + agg_ret * 0.4, 2)
    result["benchmarks"]["60_40"] = {"return_pct": blend_60_40, "name": "60/40 Blend"}

    # Alpha
    result["alpha"] = {
        "vs_spy":   round(fleet_return - spy_ret, 2) if spy_ret is not None else None,
        "vs_qqq":   round(fleet_return - qqq_ret, 2) if qqq_ret is not None else None,
        "vs_60_40": round(fleet_return - blend_60_40, 2) if blend_60_40 is not None else None,
    }

    # Underperformance alert
    if spy_ret is not None and fleet_return < spy_ret - 5:
        result["alert"] = {
            "level": "WARNING",
            "message": (
                f"Fleet underperforming SPY by {abs(fleet_return - spy_ret):.1f}% "
                f"over {days}d. Consider tightening strategy thresholds."
            ),
        }
        try:
            from engine.signal_poster import post_to_9000
            post_to_9000("BENCHMARK_ALERT", {
                "fleet_return": fleet_return,
                "spy_return": spy_ret,
                "gap": round(fleet_return - spy_ret, 2),
                "days": days,
                "message": result["alert"]["message"],
            })
        except Exception:
            pass

    # Persist snapshot
    try:
        _ensure_table()
        conn = sqlite3.connect(DB, timeout=30)
        conn.execute("""
            INSERT INTO benchmark_snapshots
                (snap_date, window_days, fleet_return, spy_return, qqq_return,
                 agg_return, blend_60_40, fleet_sharpe, fleet_max_dd,
                 alpha_vs_spy, alpha_vs_qqq, alpha_vs_60_40)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            date.today().isoformat(), days, fleet_return, spy_ret, qqq_ret,
            agg_ret, blend_60_40, sharpe, max_dd_pct,
            result["alpha"]["vs_spy"], result["alpha"]["vs_qqq"], result["alpha"]["vs_60_40"],
        ))
        conn.commit()
        conn.close()
    except Exception:
        pass

    with _lock:
        _CACHE.update(result)
        _CACHE["days"] = days
        _CACHE_TS = time.time()

    return result


def get_benchmark_summary() -> dict[str, Any]:
    """Quick scorecard: 7d, 30d, 90d windows."""
    summaries = {}
    for window in (7, 30, 90):
        try:
            data = compute_benchmark(window)
            summaries[f"{window}d"] = {
                "fleet_return": data["fleet"]["return_pct"],
                "spy_return":   data["benchmarks"]["SPY"]["return_pct"],
                "alpha_vs_spy": data["alpha"]["vs_spy"],
                "sharpe":       data["fleet"]["sharpe"],
                "alert":        data.get("alert"),
            }
        except Exception:
            summaries[f"{window}d"] = None
    return summaries


# ── Auto-init ─────────────────────────────────────────────────────────────────
try:
    _ensure_table()
except Exception:
    pass

# ── Startup warning: uncorrected options trades ───────────────────────────────
try:
    _conn = sqlite3.connect("data/trader.db", timeout=5)
    _uncorrected = _conn.execute("""
        SELECT COUNT(*) FROM trades
        WHERE action = 'SELL'
          AND (asset_type = 'option' OR option_type IS NOT NULL)
          AND corrected_pnl IS NULL
    """).fetchone()[0]
    _conn.close()
    if _uncorrected > 0:
        logger.warning(
            f"[BENCHMARK] WARNING: {_uncorrected} options trades with uncorrected P&L "
            f"excluded from calculations"
        )
except Exception:
    pass
