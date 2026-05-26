"""HM-BACKTEST-90D — drive engine.master_backtest at 90-day window + ship JSON.

Sacred rules:
  - trader.db, arena.db, tractor.db are NEVER touched.
  - data/backtest.db is the canonical output store; we pre-archived it
    before this run (see scripts/run_90d_backtest.py invocation).
  - Read-only against historical Polygon/yfinance data.

Brief gaps surfaced (documented in final JSON's _meta block):
  - Universe is the harness's hardcoded 24-symbol MASTER_UNIVERSE
    (NVDA, AMD, MU, AVGO, META, GOOGL, AAPL, AMZN, MSFT, TSLA, TQQQ,
    SPY, QQQ, PLTR, MRVL, NFLX, SOFI, COIN, BAC, MARA, XLE, INTC,
    STAA, SMR), not "all symbols".
  - Data source is yfinance (Polygon was specified as primary in brief
    but the existing harness uses yf.download — see master_backtest.py
    `_download_universe`). yfinance is Polygon's free fallback for
    daily OHLCV, accuracy is comparable for the 90d window.
  - VectorBT is NOT installed → Tier 1 (technical signals like RSI
    divergence, EMA ribbon, SMA20 pullback) will skip; Tiers 2-9
    (options structures, mean-reversion, momentum, dalio metals)
    run normally.
  - Starting capital is $100K per harness internals; we report
    percent-return so the $10K-per-strategy framing is equivalent.
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from engine.master_backtest import run_master_backtest, _BACKTEST_DB  # noqa: E402

OUTPUT = ROOT / "backtest_results_90d.json"
RUN_START = time.time()
RUN_DATE = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def query_per_strategy(conn: sqlite3.Connection, run_date: str) -> list[dict]:
    """Aggregate per-strategy metrics across the universe for this run.

    The harness writes one row per (strategy, ticker, regime) combo.
    We collapse to per-strategy by averaging across tickers/regimes.
    """
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
    out: list[dict] = []
    for r in rows:
        avg_return = float(r["avg_return_pct"] or 0)
        final_equity = 10_000.0 * (1.0 + avg_return / 100.0)
        out.append(
            {
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
            }
        )
    return out


def main() -> int:
    print(f"[{datetime.now():%H:%M:%S}] HM-BACKTEST-90D — starting run_master_backtest(days=90)")
    result = run_master_backtest(days=90, compare=False)
    elapsed = time.time() - RUN_START
    print(f"[{datetime.now():%H:%M:%S}] master_backtest returned in {elapsed:.1f}s")

    run_date = datetime.now().strftime("%Y-%m-%d")
    conn = sqlite3.connect(_BACKTEST_DB)
    conn.row_factory = sqlite3.Row
    per_strategy = query_per_strategy(conn, run_date)
    conn.close()

    payload = {
        "_meta": {
            "ticket": "HM-BACKTEST-90D",
            "run_at": RUN_DATE,
            "elapsed_seconds": round(elapsed, 1),
            "lookback_days": 90,
            "starting_capital_per_strategy": 10_000.0,
            "universe": "MASTER_UNIVERSE (24 symbols) — see engine/master_backtest.py",
            "data_source": "yfinance daily OHLCV (Polygon was brief-specified as primary; existing harness uses yf — comparable for 90d window)",
            "vectorbt_available": False,
            "tier_1_status": "SKIPPED — vectorbt not installed (RSI divergence + EMA ribbon + SMA20 pullback technical signals)",
            "summary_from_harness": {
                "status": result.get("status", "unknown"),
                "strategies_returned": len(result.get("top_strategies", []) or []),
                "spy_benchmark_pct": result.get("spy_benchmark"),
            },
            "sacred_rules": {
                "trader_db_touched": False,
                "arena_db_touched": False,
                "tractor_db_touched": False,
                "backtest_db_pre_archived": True,
            },
        },
        "per_strategy_results": per_strategy,
    }

    OUTPUT.write_text(json.dumps(payload, indent=2))
    print(f"[{datetime.now():%H:%M:%S}] Wrote {OUTPUT} ({len(per_strategy)} strategies)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
