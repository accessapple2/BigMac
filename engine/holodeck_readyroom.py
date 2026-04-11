"""
Holodeck Simulation Results — Ready Room Signal Backtester
Backtests 3 strategies vs buy-and-hold using Ready Room condition signals.
"""

import logging
import math
import os
import sqlite3
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

_DB = os.environ.get("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))

BACKTEST_DAYS = 90
MIN_BRIEFING_DAYS = 10


def _init_db() -> None:
    try:
        conn = sqlite3.connect(_DB, timeout=30)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS holodeck_backtest_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_date TEXT NOT NULL,
                strategy_name TEXT NOT NULL,
                days_analyzed INTEGER,
                total_return_pct REAL,
                win_rate_pct REAL,
                avg_win_pct REAL,
                avg_loss_pct REAL,
                max_drawdown_pct REAL,
                sharpe REAL,
                trades_taken INTEGER,
                trades_skipped INTEGER,
                edge_vs_bah REAL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(run_date, strategy_name)
            )
        """)
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error("holodeck_readyroom _init_db failed: %s", e)


def _fetch_spy_bars(days: int = BACKTEST_DAYS) -> list[dict]:
    """Fetch SPY daily OHLC from Alpaca."""
    try:
        from alpaca.data import StockHistoricalDataClient
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame

        client = StockHistoricalDataClient(
            os.environ.get("ALPACA_API_KEY", ""),
            os.environ.get("ALPACA_SECRET_KEY", ""),
        )
        req = StockBarsRequest(
            symbol_or_symbols=["SPY"],
            timeframe=TimeFrame.Day,
            start=datetime.now() - timedelta(days=days + 10),
            feed="iex",
        )
        bars = client.get_stock_bars(req)
        raw = bars.data.get("SPY", [])
        result = []
        for bar in raw:
            ts = bar.timestamp
            date_str = ts.strftime("%Y-%m-%d") if hasattr(ts, "strftime") else str(ts)[:10]
            result.append({
                "date": date_str,
                "open": bar.open,
                "close": bar.close,
                "volume": bar.volume,
            })
        return sorted(result, key=lambda x: x["date"])
    except Exception as e:
        logger.error("_fetch_spy_bars failed: %s", e)
        return []


def _fetch_ready_room_briefings(days: int = BACKTEST_DAYS) -> dict[str, dict]:
    """Fetch ready_room_briefings keyed by session_date."""
    try:
        conn = sqlite3.connect(_DB, timeout=30)
        conn.row_factory = sqlite3.Row
        cutoff = (datetime.now() - timedelta(days=days + 10)).strftime("%Y-%m-%d")
        rows = conn.execute(
            """
            SELECT session_date, session_type, condition, condition_score
            FROM ready_room_briefings
            WHERE session_date >= ?
            ORDER BY session_date ASC
            """,
            (cutoff,),
        ).fetchall()
        conn.close()
        return {r["session_date"]: dict(r) for r in rows}
    except Exception as e:
        logger.error("_fetch_ready_room_briefings failed: %s", e)
        return {}


def _fetch_trade_advisories(days: int = BACKTEST_DAYS) -> dict[str, float]:
    """Fetch average position_size_multiplier per trade_date from trade_advisories."""
    try:
        conn = sqlite3.connect(_DB, timeout=30)
        conn.row_factory = sqlite3.Row
        cutoff = (datetime.now() - timedelta(days=days + 10)).strftime("%Y-%m-%d")
        rows = conn.execute(
            """
            SELECT trade_date, AVG(position_size_multiplier) as avg_multiplier
            FROM trade_advisories
            WHERE trade_date >= ? AND position_size_multiplier IS NOT NULL
            GROUP BY trade_date
            ORDER BY trade_date ASC
            """,
            (cutoff,),
        ).fetchall()
        conn.close()
        return {r["trade_date"]: r["avg_multiplier"] for r in rows}
    except Exception as e:
        logger.error("_fetch_trade_advisories failed: %s", e)
        return {}


def _compute_metrics(daily_returns: list[float], trades_taken: int, trades_skipped: int) -> dict:
    """Compute backtest metrics from a list of daily returns."""
    if not daily_returns:
        return {
            "total_return_pct": 0.0,
            "win_rate_pct": 0.0,
            "avg_win_pct": 0.0,
            "avg_loss_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "sharpe": 0.0,
            "trades_taken": trades_taken,
            "trades_skipped": trades_skipped,
        }

    total_return = sum(daily_returns)
    wins = [r for r in daily_returns if r > 0]
    losses = [r for r in daily_returns if r < 0]
    win_rate = len(wins) / len(daily_returns) * 100 if daily_returns else 0.0
    avg_win = sum(wins) / len(wins) if wins else 0.0
    avg_loss = abs(sum(losses) / len(losses)) if losses else 0.0

    # Max drawdown: running peak-to-trough on cumulative returns
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for r in daily_returns:
        cumulative += r
        if cumulative > peak:
            peak = cumulative
        drawdown = peak - cumulative
        if drawdown > max_drawdown:
            max_drawdown = drawdown

    # Sharpe ratio (annualized)
    n = len(daily_returns)
    mean_r = total_return / n if n > 0 else 0.0
    variance = sum((r - mean_r) ** 2 for r in daily_returns) / n if n > 0 else 0.0
    std_r = math.sqrt(variance) if variance > 0 else 0.0
    sharpe = (mean_r / std_r * math.sqrt(252)) if std_r > 0 else 0.0

    return {
        "total_return_pct": round(total_return, 4),
        "win_rate_pct": round(win_rate, 2),
        "avg_win_pct": round(avg_win, 4),
        "avg_loss_pct": round(avg_loss, 4),
        "max_drawdown_pct": round(max_drawdown, 4),
        "sharpe": round(sharpe, 4),
        "trades_taken": trades_taken,
        "trades_skipped": trades_skipped,
    }


def _save_strategy(run_date: str, strategy_name: str, metrics: dict, edge_vs_bah: float) -> None:
    try:
        conn = sqlite3.connect(_DB, timeout=30)
        conn.execute(
            """
            INSERT INTO holodeck_backtest_results
              (run_date, strategy_name, days_analyzed, total_return_pct, win_rate_pct,
               avg_win_pct, avg_loss_pct, max_drawdown_pct, sharpe, trades_taken,
               trades_skipped, edge_vs_bah)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_date, strategy_name) DO UPDATE SET
              days_analyzed=excluded.days_analyzed,
              total_return_pct=excluded.total_return_pct,
              win_rate_pct=excluded.win_rate_pct,
              avg_win_pct=excluded.avg_win_pct,
              avg_loss_pct=excluded.avg_loss_pct,
              max_drawdown_pct=excluded.max_drawdown_pct,
              sharpe=excluded.sharpe,
              trades_taken=excluded.trades_taken,
              trades_skipped=excluded.trades_skipped,
              edge_vs_bah=excluded.edge_vs_bah
            """,
            (
                run_date,
                strategy_name,
                metrics.get("trades_taken", 0) + metrics.get("trades_skipped", 0),
                metrics["total_return_pct"],
                metrics["win_rate_pct"],
                metrics["avg_win_pct"],
                metrics["avg_loss_pct"],
                metrics["max_drawdown_pct"],
                metrics["sharpe"],
                metrics["trades_taken"],
                metrics["trades_skipped"],
                round(edge_vs_bah, 4),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error("_save_strategy %s failed: %s", strategy_name, e)


def run_holodeck_backtest(force: bool = False) -> dict:
    """
    Run backtest using Ready Room signals across 3 strategies.
    Strategy A: buy_and_hold (every day open-to-close)
    Strategy B: rr_filtered (GREEN or TRENDING_BULL days only)
    Strategy C: troi_sized (every day scaled by position_size_multiplier)
    """
    run_date = datetime.now().strftime("%Y-%m-%d")

    try:
        # Check if already ran today
        if not force:
            try:
                conn = sqlite3.connect(_DB, timeout=30)
                existing = conn.execute(
                    "SELECT COUNT(*) FROM holodeck_backtest_results WHERE run_date = ?",
                    (run_date,),
                ).fetchone()[0]
                conn.close()
                if existing >= 3:
                    return get_latest_holodeck_results()
            except Exception:
                pass

        spy_bars = _fetch_spy_bars(BACKTEST_DAYS)
        briefings = _fetch_ready_room_briefings(BACKTEST_DAYS)
        advisories = _fetch_trade_advisories(BACKTEST_DAYS)

        if not spy_bars:
            return {
                "status": "insufficient_data",
                "days": 0,
                "message": "Could not fetch SPY price data from Alpaca",
            }

        # Find overlapping dates
        bar_dates = {b["date"] for b in spy_bars}
        briefing_dates = set(briefings.keys())
        overlap_dates = sorted(bar_dates & briefing_dates)

        if len(overlap_dates) < MIN_BRIEFING_DAYS:
            return {
                "status": "insufficient_data",
                "days": len(overlap_dates),
                "message": f"Need {MIN_BRIEFING_DAYS}+ trading days of Ready Room data, have {len(overlap_dates)}",
            }

        bar_by_date = {b["date"]: b for b in spy_bars}

        # Strategy returns
        bah_returns: list[float] = []
        rr_returns: list[float] = []
        troi_returns: list[float] = []
        rr_skipped = 0
        troi_skipped = 0

        for date in overlap_dates:
            bar = bar_by_date.get(date)
            if not bar or not bar["open"] or bar["open"] == 0:
                rr_skipped += 1
                troi_skipped += 1
                continue

            daily_ret = (bar["close"] - bar["open"]) / bar["open"] * 100

            # Strategy A: buy and hold every day
            bah_returns.append(daily_ret)

            # Strategy B: rr_filtered — only GREEN or TRENDING_BULL
            briefing = briefings.get(date, {})
            condition = (briefing.get("condition") or "").upper()
            session_type = (briefing.get("session_type") or "").upper()
            go_day = condition == "GREEN" or session_type == "TRENDING_BULL"
            if go_day:
                rr_returns.append(daily_ret)
            else:
                rr_returns.append(0.0)
                rr_skipped += 1

            # Strategy C: troi_sized — scaled by multiplier
            multiplier = advisories.get(date, 0.8)
            troi_returns.append(daily_ret * multiplier)

        days_analyzed = len(overlap_dates)
        bah_taken = len([r for r in bah_returns if r != 0]) if bah_returns else 0

        bah_metrics = _compute_metrics(bah_returns, bah_taken, 0)
        rr_taken = len([r for r in rr_returns if r != 0.0])
        rr_metrics = _compute_metrics([r for r in rr_returns if r != 0.0], rr_taken, rr_skipped)
        troi_taken = days_analyzed - troi_skipped
        troi_metrics = _compute_metrics(troi_returns, troi_taken, troi_skipped)

        bah_total = bah_metrics["total_return_pct"]
        rr_edge = rr_metrics["total_return_pct"] - bah_total
        troi_edge = troi_metrics["total_return_pct"] - bah_total

        _save_strategy(run_date, "buy_and_hold", bah_metrics, 0.0)
        _save_strategy(run_date, "rr_filtered", rr_metrics, rr_edge)
        _save_strategy(run_date, "troi_sized", troi_metrics, troi_edge)

        # Determine best strategy by total return
        strategy_returns = {
            "buy_and_hold": bah_total,
            "rr_filtered": rr_metrics["total_return_pct"],
            "troi_sized": troi_metrics["total_return_pct"],
        }
        best_strategy = max(strategy_returns, key=lambda k: strategy_returns[k])

        return {
            "status": "ok",
            "days_analyzed": days_analyzed,
            "strategies": {
                "buy_and_hold": {**bah_metrics, "edge_vs_bah": 0.0},
                "rr_filtered": {**rr_metrics, "edge_vs_bah": round(rr_edge, 4)},
                "troi_sized": {**troi_metrics, "edge_vs_bah": round(troi_edge, 4)},
            },
            "best_strategy": best_strategy,
            "rr_edge": round(rr_edge, 4),
            "run_date": run_date,
        }

    except Exception as e:
        logger.error("run_holodeck_backtest failed: %s", e)
        return {
            "status": "error",
            "error": str(e),
            "run_date": run_date,
        }


def get_latest_holodeck_results() -> dict:
    """Return latest backtest results from DB, grouped by most recent run_date."""
    try:
        conn = sqlite3.connect(_DB, timeout=30)
        conn.row_factory = sqlite3.Row

        latest_row = conn.execute(
            "SELECT run_date FROM holodeck_backtest_results ORDER BY created_at DESC LIMIT 1"
        ).fetchone()

        if not latest_row:
            conn.close()
            return {"status": "no_data", "message": "No holodeck backtest results found"}

        run_date = latest_row["run_date"]
        rows = conn.execute(
            """
            SELECT strategy_name, days_analyzed, total_return_pct, win_rate_pct,
                   avg_win_pct, avg_loss_pct, max_drawdown_pct, sharpe,
                   trades_taken, trades_skipped, edge_vs_bah, created_at
            FROM holodeck_backtest_results
            WHERE run_date = ?
            ORDER BY strategy_name
            """,
            (run_date,),
        ).fetchall()
        conn.close()

        strategies: dict[str, dict] = {}
        for row in rows:
            strategies[row["strategy_name"]] = {
                "total_return_pct": row["total_return_pct"],
                "win_rate_pct": row["win_rate_pct"],
                "avg_win_pct": row["avg_win_pct"],
                "avg_loss_pct": row["avg_loss_pct"],
                "max_drawdown_pct": row["max_drawdown_pct"],
                "sharpe": row["sharpe"],
                "trades_taken": row["trades_taken"],
                "trades_skipped": row["trades_skipped"],
                "edge_vs_bah": row["edge_vs_bah"],
            }

        bah_total = strategies.get("buy_and_hold", {}).get("total_return_pct", 0.0)
        best_strategy = max(strategies, key=lambda k: strategies[k].get("total_return_pct", 0.0)) if strategies else "unknown"
        rr_edge = strategies.get("rr_filtered", {}).get("edge_vs_bah", 0.0)
        days = rows[0]["days_analyzed"] if rows else 0

        return {
            "status": "ok",
            "run_date": run_date,
            "days_analyzed": days,
            "strategies": strategies,
            "best_strategy": best_strategy,
            "rr_edge": rr_edge,
        }

    except Exception as e:
        logger.error("get_latest_holodeck_results failed: %s", e)
        return {"status": "error", "error": str(e)}


def run_holodeck_weekly() -> None:
    """Called every Sunday at 10:00 AM."""
    logger.info("holodeck_readyroom: running weekly simulation")
    result = run_holodeck_backtest(force=True)
    logger.info("holodeck weekly result: status=%s best=%s rr_edge=%s",
                result.get("status"), result.get("best_strategy"), result.get("rr_edge"))


# Initialize DB on module load
_init_db()
