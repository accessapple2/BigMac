"""HM-IC-SQUADRON Pillar 5 — nightly strategy×regime×window backtest sweep.

Orchestrator on top of engine/strategy_lab.py (DO NOT duplicate the 48KB
lab engine). Runs nightly at 20:00 AZ. For each (strategy, dominant_regime,
window∈{30,60,90}) combination across a benchmark symbol set, executes
strategy_lab.run_strategy_backtest, persists stats to strategy_lab_results,
and emits:

  1. data/regime_fit_matrix_update.json   — consumed by future regime_router
                                              auto-tuner (Pillar 1 v2)
  2. NTFY ollietrades-admin with top 3 (regime, strategy) fits
  3. Appends to data/morning_brief.json   — consumed by morning_briefing.py

Spec: ~/.claude/projects/-Users-bigmac/memory/project_hm_ic_squadron_approved.md

The sweep deliberately stays inside a manageable footprint for v1:
  ~10 benchmark symbols × 4 strategies × 3 windows = ~120 backtests/night.
Wider sweeps (full universe × all strategies) belong in a separate
weekly/monthly job — this one is nightly + bounded.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

from rich.console import Console

console = Console()

_REPO = Path(__file__).resolve().parent.parent
_DB_PATH = _REPO / "data" / "trader.db"
_MORNING_BRIEF = _REPO / "data" / "morning_brief.json"
_FIT_UPDATE = _REPO / "data" / "regime_fit_matrix_update.json"

# v1 benchmark universe — broad-index + mega-caps + gold. Tight enough to
# finish in <30 min on Ollie Max, wide enough to expose regime-strategy fits.
_BENCHMARK_SYMBOLS: list[str] = [
    "SPY", "QQQ", "IWM",
    "AAPL", "MSFT", "NVDA", "GOOGL", "META", "AMZN",
    "GLD",
]

# Use the existing strategy_lab runner registry — single source of truth.
_WINDOWS: list[int] = [30, 60, 90]


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def _init_results_table() -> None:
    """Create strategy_lab_results idempotent. Schema:
       (run_id, sweep_ts, strategy, dominant_regime, window_days, symbol,
        win_rate, expectancy, sharpe, max_dd_pct, trades, total_pnl,
        starting_cash, ending_cash, source).
    """
    conn = sqlite3.connect(str(_DB_PATH), timeout=10)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS strategy_lab_results (
              id INTEGER PRIMARY KEY,
              run_id TEXT NOT NULL,
              sweep_ts TIMESTAMP DEFAULT (datetime('now')),
              strategy TEXT NOT NULL,
              dominant_regime TEXT,
              window_days INTEGER NOT NULL,
              symbol TEXT NOT NULL,
              win_rate REAL,
              expectancy REAL,
              sharpe REAL,
              max_dd_pct REAL,
              trades INTEGER,
              total_pnl REAL,
              starting_cash REAL,
              ending_cash REAL,
              source TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_slr_run "
            "ON strategy_lab_results(run_id, dominant_regime, strategy)"
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _dominant_regime(start_date: str, end_date: str) -> str | None:
    """Pick the most-common regime in regime_history over [start, end]."""
    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        try:
            row = conn.execute(
                "SELECT regime, COUNT(*) AS n "
                "  FROM regime_history "
                " WHERE date BETWEEN ? AND ? "
                " GROUP BY regime "
                " ORDER BY n DESC LIMIT 1",
                (start_date, end_date),
            ).fetchone()
            return row[0] if row else None
        finally:
            conn.close()
    except Exception:
        return None


def _expectancy_from_trades(trades: list) -> float:
    """Expectancy = WR × avg_win − (1−WR) × avg_loss.
    Returns 0.0 on empty input.
    """
    if not trades:
        return 0.0
    wins = [t for t in trades if (t.get("pnl") or 0) > 0]
    losses = [t for t in trades if (t.get("pnl") or 0) < 0]
    n = len(trades)
    if not n:
        return 0.0
    wr = len(wins) / n
    avg_win = (sum(t.get("pnl") or 0 for t in wins) / len(wins)) if wins else 0.0
    avg_loss = (
        abs(sum(t.get("pnl") or 0 for t in losses) / len(losses)) if losses else 0.0
    )
    return wr * avg_win - (1 - wr) * avg_loss


def _persist_result(
    *, run_id: str, strategy: str, dominant_regime: str | None,
    window_days: int, symbol: str, result: dict,
) -> None:
    """Insert one backtest result row. Crash-safe."""
    try:
        stats = result.get("stats") or {}
        trades = result.get("trades") or []
        ec = result.get("equity_curve") or []
        starting_cash = float(stats.get("starting_cash") or 10000.0)
        ending_cash = float(ec[-1].get("equity") if ec else stats.get("ending_cash") or starting_cash)
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        try:
            conn.execute(
                "INSERT INTO strategy_lab_results "
                "(run_id, strategy, dominant_regime, window_days, symbol, "
                " win_rate, expectancy, sharpe, max_dd_pct, trades, total_pnl, "
                " starting_cash, ending_cash, source) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    run_id, strategy, dominant_regime, window_days, symbol,
                    float(stats.get("win_rate") or 0.0),
                    _expectancy_from_trades(trades),
                    float(stats.get("sharpe_ratio") or stats.get("sharpe") or 0.0),
                    float(stats.get("max_drawdown_pct") or stats.get("max_dd_pct") or 0.0),
                    int(stats.get("total_trades") or len(trades)),
                    float(stats.get("total_pnl") or (ending_cash - starting_cash)),
                    starting_cash, ending_cash,
                    "HM-IC-SQUADRON-PILLAR-5",
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        console.log(
            f"[red][LAB-SWEEP] persist failed for "
            f"{strategy}/{symbol}/{window_days}d: "
            f"{type(e).__name__}: {e!r}"
        )


def _compute_top3(run_id: str) -> list[dict]:
    """Top 3 (regime, strategy) by mean expectancy across the symbol set
    for this run. Returns list of dicts with regime + strategy + stats.
    """
    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        try:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT dominant_regime AS regime, strategy, "
                "       AVG(win_rate) AS avg_wr, "
                "       AVG(expectancy) AS avg_exp, "
                "       AVG(sharpe) AS avg_sharpe, "
                "       SUM(trades) AS total_trades, "
                "       COUNT(*) AS n_runs "
                "  FROM strategy_lab_results "
                " WHERE run_id = ? AND dominant_regime IS NOT NULL "
                " GROUP BY dominant_regime, strategy "
                " HAVING n_runs >= 3 "
                " ORDER BY avg_exp DESC LIMIT 3",
                (run_id,),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()
    except Exception:
        return []


def _emit_fit_matrix_update(run_id: str, top3: list[dict]) -> None:
    """Write data/regime_fit_matrix_update.json for the future auto-tuner."""
    payload = {
        "run_id": run_id,
        "ts": datetime.utcnow().isoformat() + "Z",
        "top_fits": top3,
        "source": "HM-IC-SQUADRON-PILLAR-5",
    }
    try:
        _FIT_UPDATE.parent.mkdir(parents=True, exist_ok=True)
        _FIT_UPDATE.write_text(json.dumps(payload, indent=2))
    except Exception as e:
        console.log(
            f"[red][LAB-SWEEP] regime_fit_matrix_update.json write failed: "
            f"{type(e).__name__}: {e!r}"
        )


def _append_to_morning_brief(run_id: str, top3: list[dict]) -> None:
    """Merge a lab_sweep section into morning_brief.json. Non-destructive."""
    try:
        data: dict = {}
        if _MORNING_BRIEF.exists():
            try:
                data = json.loads(_MORNING_BRIEF.read_text() or "{}")
            except Exception:
                data = {}
        data["lab_sweep"] = {
            "run_id": run_id,
            "ts": datetime.utcnow().isoformat() + "Z",
            "top_fits": top3,
        }
        _MORNING_BRIEF.write_text(json.dumps(data, indent=2))
    except Exception as e:
        console.log(
            f"[red][LAB-SWEEP] morning_brief.json merge failed: "
            f"{type(e).__name__}: {e!r}"
        )


def _ntfy_top3(top3: list[dict]) -> None:
    """NTFY ollietrades-admin with top 3 regime-strategy fits."""
    if not top3:
        return
    try:
        from engine.alert_channels import _send_ntfy
        lines = [
            f"{i+1}. {r['regime']}/{r['strategy']}: "
            f"WR={r['avg_wr']*100:.1f}% exp={r['avg_exp']:.2f} "
            f"sharpe={r['avg_sharpe']:.2f} (n={r['n_runs']})"
            for i, r in enumerate(top3)
        ]
        msg = "Tomorrow's top regime-strategy fits:\n" + "\n".join(lines)
        _send_ntfy(
            title="IC Squadron — Nightly Lab Sweep",
            message=msg,
            priority="default",
            tags="ollietrades,labsweep,pillar5",
            topic="ollietrades-admin",
        )
    except Exception as e:
        console.log(
            f"[red][LAB-SWEEP] NTFY failed: {type(e).__name__}: {e!r}"
        )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run_strategy_lab_sweep(
    symbols: list[str] | None = None,
    windows: list[int] | None = None,
) -> dict:
    """Nightly orchestrator. Returns summary dict.

    Schedule via main.py: schedule.every().day.at("20:00").do(run_strategy_lab_sweep)
    Inference workload uses VectorBT on the Ollie Max GPU via OLLIE_URL where
    the strategy_lab runners support it. yfinance still pulls daily OHLCV
    for the backtest universe.
    """
    from engine.strategy_lab import STRATEGIES, run_strategy_backtest

    _init_results_table()
    syms = symbols or _BENCHMARK_SYMBOLS
    wins = windows or _WINDOWS
    strategies = list(STRATEGIES.keys())
    run_id = "sweep_" + datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    console.log(
        f"[cyan][LAB-SWEEP] start run_id={run_id} "
        f"strategies={len(strategies)} symbols={len(syms)} windows={wins}"
    )
    total = 0
    errors = 0
    for window in wins:
        end_dt = datetime.utcnow().date()
        start_dt = end_dt - timedelta(days=window + 60)  # +60d warmup buffer
        end_s = end_dt.isoformat()
        start_s = start_dt.isoformat()
        dom_regime = _dominant_regime(start_s, end_s)
        for strategy in strategies:
            strategy_def = STRATEGIES[strategy]
            defaults = {k: v["default"] for k, v in strategy_def["params"].items()}
            for sym in syms:
                total += 1
                try:
                    result = run_strategy_backtest(
                        strategy_name=strategy, params=defaults,
                        symbol=sym, start_date=start_s, end_date=end_s,
                    )
                    if result.get("error"):
                        errors += 1
                        continue
                    _persist_result(
                        run_id=run_id, strategy=strategy,
                        dominant_regime=dom_regime, window_days=window,
                        symbol=sym, result=result,
                    )
                except Exception as e:
                    errors += 1
                    console.log(
                        f"[red][LAB-SWEEP] {strategy}/{sym}/{window}d crashed: "
                        f"{type(e).__name__}: {e!r}"
                    )
    top3 = _compute_top3(run_id)
    _emit_fit_matrix_update(run_id, top3)
    _append_to_morning_brief(run_id, top3)
    _ntfy_top3(top3)
    console.log(
        f"[green][LAB-SWEEP] done run_id={run_id} "
        f"total={total} errors={errors} top3={len(top3)}"
    )
    return {
        "run_id": run_id,
        "total_backtests": total,
        "errors": errors,
        "top_fits": top3,
    }


if __name__ == "__main__":
    print(json.dumps(run_strategy_lab_sweep(), indent=2, default=str))
