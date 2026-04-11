"""
Indicator Benchmark — Alpha Engine Component 2
Computes per-indicator performance stats (win rate, avg return, Sharpe) daily at 4:30 PM ET.

Tables:
  indicator_benchmarks — daily snapshot of indicator performance

Key functions:
  run_indicator_bench()     — main daily job
  get_leaderboard(limit)    — ranked leaderboard for UI/API
"""
import sqlite3
import logging
import math
from datetime import datetime, timezone

DB = "data/trader.db"
logger = logging.getLogger("indicator_bench")

_bench_done_today = [False]


def _db():
    return sqlite3.connect(DB, timeout=10)


def ensure_tables():
    con = _db()
    con.execute("""
        CREATE TABLE IF NOT EXISTS indicator_benchmarks (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date    TEXT    NOT NULL,              -- YYYY-MM-DD
            indicator   TEXT    NOT NULL,
            total       INTEGER DEFAULT 0,
            wins        INTEGER DEFAULT 0,
            losses      INTEGER DEFAULT 0,
            win_rate    REAL    DEFAULT 0,             -- 0-100
            avg_return  REAL    DEFAULT 0,             -- avg outcome_pct
            sharpe      REAL    DEFAULT 0,             -- avg_return / std_return (annualized approx)
            best_return REAL    DEFAULT 0,
            worst_return REAL   DEFAULT 0,
            UNIQUE(run_date, indicator)
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS ix_ib_date ON indicator_benchmarks(run_date)")
    con.execute("CREATE INDEX IF NOT EXISTS ix_ib_indicator ON indicator_benchmarks(indicator)")
    con.commit()
    con.close()


def _compute_sharpe(returns: list) -> float:
    """Simple Sharpe approximation: mean/std of return series."""
    if len(returns) < 2:
        return 0.0
    n = len(returns)
    mean = sum(returns) / n
    variance = sum((r - mean) ** 2 for r in returns) / (n - 1)
    std = math.sqrt(variance) if variance > 0 else 0.0
    return round(mean / std, 3) if std > 0 else 0.0


def run_indicator_bench():
    """
    Runs once daily at ~4:30 PM ET. Reads signal_scorecard outcomes,
    computes per-indicator stats, and upserts into indicator_benchmarks.
    """
    import pytz
    az = pytz.timezone("US/Arizona")
    now = datetime.now(az)

    # Reset daily flag at midnight
    if now.hour < 4:
        _bench_done_today[0] = False
        return

    # Weekends: skip
    if now.weekday() >= 5:
        return

    # Fire window: 4:30-5:00 PM ET = 1:30-2:00 PM AZ (AZ is ET-3 in winter, ET-2 in DST)
    # Use UTC: 4:30 PM ET = 21:30 UTC (EST), 20:30 UTC (EDT)
    now_utc = datetime.now(timezone.utc)
    if _bench_done_today[0]:
        return
    if not (21 <= now_utc.hour <= 22):  # rough 4:00-5:00 PM ET window
        return

    _bench_done_today[0] = True
    ensure_tables()

    # Pull all scored signals per indicator
    from engine.signal_scorecard import ensure_tables as sc_tables
    sc_tables()

    con = _db()
    rows = con.execute("""
        SELECT indicator, outcome_pct, win
        FROM signal_scorecard
        WHERE win IS NOT NULL
        ORDER BY indicator
    """).fetchall()
    con.close()

    if not rows:
        logger.info("No scored signals yet — skipping benchmark run")
        return

    # Group by indicator
    by_indicator: dict = {}
    for indicator, pct, win in rows:
        if indicator not in by_indicator:
            by_indicator[indicator] = {"returns": [], "wins": 0, "losses": 0}
        by_indicator[indicator]["returns"].append(pct or 0.0)
        if win == 1:
            by_indicator[indicator]["wins"] += 1
        else:
            by_indicator[indicator]["losses"] += 1

    run_date = now_utc.strftime("%Y-%m-%d")
    con = _db()
    for indicator, data in by_indicator.items():
        returns = data["returns"]
        wins = data["wins"]
        losses = data["losses"]
        total = wins + losses
        win_rate = round(wins / total * 100, 1) if total > 0 else 0.0
        avg_ret = round(sum(returns) / len(returns), 3) if returns else 0.0
        sharpe = _compute_sharpe(returns)
        best = round(max(returns), 3) if returns else 0.0
        worst = round(min(returns), 3) if returns else 0.0

        con.execute("""
            INSERT INTO indicator_benchmarks
              (run_date, indicator, total, wins, losses, win_rate, avg_return, sharpe, best_return, worst_return)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(run_date, indicator) DO UPDATE SET
              total=excluded.total, wins=excluded.wins, losses=excluded.losses,
              win_rate=excluded.win_rate, avg_return=excluded.avg_return,
              sharpe=excluded.sharpe, best_return=excluded.best_return,
              worst_return=excluded.worst_return
        """, (run_date, indicator, total, wins, losses, win_rate, avg_ret, sharpe, best, worst))
    con.commit()
    con.close()
    logger.info(f"Indicator benchmark saved: {len(by_indicator)} indicators for {run_date}")


def get_leaderboard(limit: int = 20) -> list:
    """
    Return ranked indicators from most recent benchmark run.
    Ranked by win_rate DESC, then sharpe DESC.
    """
    ensure_tables()
    con = _db()
    # Get most recent run_date
    latest = con.execute("SELECT MAX(run_date) FROM indicator_benchmarks").fetchone()[0]
    if not latest:
        con.close()
        return []
    rows = con.execute("""
        SELECT indicator, total, wins, losses, win_rate, avg_return, sharpe,
               best_return, worst_return, run_date
        FROM indicator_benchmarks
        WHERE run_date = ?
        ORDER BY win_rate DESC, sharpe DESC
        LIMIT ?
    """, (latest, limit)).fetchall()
    con.close()
    cols = ["indicator", "total", "wins", "losses", "win_rate",
            "avg_return", "sharpe", "best_return", "worst_return", "run_date"]
    return [dict(zip(cols, r)) for r in rows]
