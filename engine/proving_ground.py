"""engine/proving_ground.py — Sniper Mode 30-Day Proving Ground

Runs at 1:15 PM AZ (4:15 PM ET) after market close.
Tracks live Sniper Mode performance vs backtest predictions.
Sends daily ntfy report and weekly comparison.
"""
from __future__ import annotations
import sqlite3
import math
import json
from datetime import datetime, date, timedelta
from typing import Any

import pytz

# ── ntfy ──────────────────────────────────────────────────────────────────────
from engine.ntfy import _fire, P_HIGH, P_DEFAULT, P_MAX, P_LOW

# ── Config ────────────────────────────────────────────────────────────────────
TRADER_DB = "data/trader.db"
PG_DB     = "data/proving_ground.db"
AZ_TZ     = pytz.timezone("US/Arizona")

TRIAL_START = date(2026, 4, 10)
TRIAL_DAYS  = 30

# Backtest reference numbers (v4 sniper + prior sniper run)
BT_RETURN_PCT     = 16.30
BT_SHARPE         = 1.003
BT_WIN_RATE       = 61.5
BT_TRADES_PER_WK  = 5.0

# Active Sniper agents
SNIPER_AGENTS = [
    "ollama-llama",     # Uhura
    "gemini-2.5-flash", # Worf
    "grok-4",           # Spock
    "gemini-2.5-pro",   # Seven
    "ollama-plutus",    # McCoy
    "neo-matrix",       # Neo
]

# Go/No-Go benchmarks
BENCH = {
    "win_rate":       {"target": 65.0,   "direction": "above", "min_trades": 30},
    "sharpe":         {"target": 0.5,    "direction": "above", "min_trades": 10},
    "max_drawdown":   {"target": -15.0,  "direction": "above", "min_trades": 1},
    "exec_gap_pp":    {"target": 10.0,   "direction": "below", "min_trades": 10},
    "max_consec_loss":{"target": 5,      "direction": "below", "min_trades": 1},
    "total_trades":   {"target": 30,     "direction": "above", "min_trades": 0},
}

# Alert consecutive fail threshold
CONSEC_FAIL_DAYS = 5


# ── DB helpers ────────────────────────────────────────────────────────────────

def _conn_pg() -> sqlite3.Connection:
    c = sqlite3.connect(PG_DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def _conn_trader() -> sqlite3.Connection:
    c = sqlite3.connect(TRADER_DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def ensure_tables() -> None:
    """Create proving_ground.db tables if they don't exist."""
    c = _conn_pg()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS daily_trades (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date    TEXT    NOT NULL,
            agent_id      TEXT    NOT NULL,
            strategy      TEXT,
            symbol        TEXT    NOT NULL,
            entry_price   REAL,
            exit_price    REAL,
            pnl_pct       REAL,
            alpha_score   REAL,
            signal_grade  TEXT,
            created_at    TEXT    DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS running_scorecard (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            as_of_date          TEXT    NOT NULL UNIQUE,
            total_trades        INTEGER DEFAULT 0,
            cumulative_return   REAL    DEFAULT 0.0,
            rolling_win_rate    REAL    DEFAULT 0.0,
            rolling_sharpe      REAL    DEFAULT 0.0,
            max_drawdown        REAL    DEFAULT 0.0,
            consec_wins         INTEGER DEFAULT 0,
            consec_losses       INTEGER DEFAULT 0,
            wr_bench_status     TEXT    DEFAULT 'pending',
            sharpe_bench_status TEXT    DEFAULT 'pending',
            dd_bench_status     TEXT    DEFAULT 'pending',
            gap_bench_status    TEXT    DEFAULT 'pending',
            cl_bench_status     TEXT    DEFAULT 'pending',
            trades_bench_status TEXT    DEFAULT 'pending',
            go_count            INTEGER DEFAULT 0,
            exec_gap_pp         REAL    DEFAULT 0.0,
            updated_at          TEXT    DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS agent_scorecard (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            as_of_date    TEXT    NOT NULL,
            agent_id      TEXT    NOT NULL,
            total_trades  INTEGER DEFAULT 0,
            wins          INTEGER DEFAULT 0,
            win_rate      REAL    DEFAULT 0.0,
            total_pnl_pct REAL    DEFAULT 0.0,
            flag          TEXT,
            updated_at    TEXT    DEFAULT (datetime('now')),
            UNIQUE(as_of_date, agent_id)
        );

        CREATE TABLE IF NOT EXISTS benchmark_failures (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            bench_key  TEXT NOT NULL,
            fail_date  TEXT NOT NULL,
            value      REAL,
            created_at TEXT DEFAULT (datetime('now'))
        );
    """)
    c.commit()
    c.close()


# ── Core data pulling ─────────────────────────────────────────────────────────

def _pull_today_trades(trade_date: str) -> list[dict]:
    """Pull closed trades from trader.db for a given date from Sniper agents."""
    tc = _conn_trader()
    rows = tc.execute(
        """SELECT t.player_id, t.symbol,
                  t.entry_price, t.exit_price, t.realized_pnl,
                  t.executed_at, t.reasoning,
                  t.asset_type, t.timeframe
           FROM trades t
           WHERE DATE(t.executed_at) = ?
             AND t.player_id IN ({})
             AND t.exit_price IS NOT NULL
             AND t.realized_pnl IS NOT NULL
        """.format(",".join("?" * len(SNIPER_AGENTS))),
        (trade_date, *SNIPER_AGENTS)
    ).fetchall()
    tc.close()
    return [dict(r) for r in rows]


def _pull_all_closed_trades(since: str) -> list[dict]:
    """Pull all closed trades from TRIAL_START to now."""
    tc = _conn_trader()
    rows = tc.execute(
        """SELECT t.player_id, t.symbol,
                  t.entry_price, t.exit_price, t.realized_pnl,
                  t.executed_at, t.asset_type
           FROM trades t
           WHERE DATE(t.executed_at) >= ?
             AND t.player_id IN ({})
             AND t.exit_price IS NOT NULL
             AND t.realized_pnl IS NOT NULL
           ORDER BY t.executed_at ASC
        """.format(",".join("?" * len(SNIPER_AGENTS))),
        (since, *SNIPER_AGENTS)
    ).fetchall()
    tc.close()
    return [dict(r) for r in rows]


# ── Metric calculations ───────────────────────────────────────────────────────

def _compute_metrics(trades: list[dict]) -> dict[str, Any]:
    """Compute cumulative return, Sharpe, win rate, drawdown, streaks from trade list."""
    if not trades:
        return {
            "total_trades": 0, "cumulative_return": 0.0, "win_rate": 0.0,
            "sharpe": 0.0, "max_drawdown": 0.0,
            "consec_wins": 0, "consec_losses": 0,
        }

    pnl_pcts: list[float] = []
    for t in trades:
        if t.get("entry_price") and t.get("entry_price") > 0 and t.get("exit_price"):
            pct = (t["exit_price"] - t["entry_price"]) / t["entry_price"] * 100.0
        elif t.get("realized_pnl") is not None:
            # Fallback: use realized_pnl as pct directly if it looks like a pct
            pct = float(t["realized_pnl"])
        else:
            pct = 0.0
        pnl_pcts.append(max(-100.0, min(100.0, pct)))

    total = len(pnl_pcts)
    wins  = sum(1 for p in pnl_pcts if p > 0)
    win_rate = wins / total * 100.0 if total > 0 else 0.0

    # Arithmetic sum (no compounding)
    cum_return = sum(pnl_pcts)

    # Sharpe (annualised, 252 trading days)
    if len(pnl_pcts) >= 2:
        mean = sum(pnl_pcts) / len(pnl_pcts)
        variance = sum((x - mean) ** 2 for x in pnl_pcts) / (len(pnl_pcts) - 1)
        std = math.sqrt(variance) if variance > 0 else 0.0
        sharpe = (mean / std * math.sqrt(252)) if std > 0 else 0.0
    else:
        sharpe = 0.0

    # Max drawdown (equity curve)
    equity = 10000.0
    peak   = equity
    max_dd = 0.0
    for pct in pnl_pcts:
        equity += equity * pct / 100.0
        peak    = max(peak, equity)
        dd      = (equity - peak) / peak * 100.0
        max_dd  = min(max_dd, dd)

    # Streaks (from most recent backward)
    consec_wins = consec_losses = 0
    for pct in reversed(pnl_pcts):
        if pct > 0:
            if consec_losses > 0:
                break
            consec_wins += 1
        else:
            if consec_wins > 0:
                break
            consec_losses += 1

    return {
        "total_trades":   total,
        "cumulative_return": round(cum_return, 3),
        "win_rate":       round(win_rate, 1),
        "sharpe":         round(sharpe, 3),
        "max_drawdown":   round(max_dd, 3),
        "consec_wins":    consec_wins,
        "consec_losses":  consec_losses,
        "pnl_pcts":       pnl_pcts,
    }


def _compute_exec_gap(actual_wr: float, actual_return: float) -> float:
    """Return execution gap vs backtest in percentage points (return gap)."""
    return round(abs(actual_return - BT_RETURN_PCT), 2)


# ── Benchmark scoring ─────────────────────────────────────────────────────────

def _status(metric_val: float | int, bench_key: str, total_trades: int) -> str:
    """Return 'pending' | 'pass' | 'fail' for a given benchmark."""
    b = BENCH[bench_key]
    min_t = b["min_trades"]
    if total_trades < min_t:
        return "pending"
    target = b["target"]
    if b["direction"] == "above":
        return "pass" if metric_val >= target else "fail"
    else:
        return "pass" if metric_val <= target else "fail"


def _score_benchmarks(metrics: dict, exec_gap: float) -> dict[str, str]:
    total = metrics["total_trades"]
    return {
        "win_rate":        _status(metrics["win_rate"],       "win_rate",        total),
        "sharpe":          _status(metrics["sharpe"],         "sharpe",          total),
        "max_drawdown":    _status(metrics["max_drawdown"],   "max_drawdown",    total),
        "exec_gap_pp":     _status(exec_gap,                  "exec_gap_pp",     total),
        "max_consec_loss": _status(metrics["consec_losses"],  "max_consec_loss", total),
        "total_trades":    _status(total,                     "total_trades",    total),
    }


# ── Per-agent breakdown ───────────────────────────────────────────────────────

def _per_agent_metrics(all_trades: list[dict]) -> dict[str, dict]:
    agents: dict[str, list[float]] = {a: [] for a in SNIPER_AGENTS}
    for t in all_trades:
        pid = t.get("player_id")
        if pid not in agents:
            continue
        if t.get("entry_price") and t["entry_price"] > 0 and t.get("exit_price"):
            pct = (t["exit_price"] - t["entry_price"]) / t["entry_price"] * 100.0
        else:
            pct = float(t.get("realized_pnl") or 0)
        agents[pid].append(max(-100.0, min(100.0, pct)))

    result = {}
    for aid, pcts in agents.items():
        total = len(pcts)
        wins  = sum(1 for p in pcts if p > 0)
        wr    = wins / total * 100.0 if total > 0 else 0.0
        flag  = None
        if total >= 10:
            if wr < 40.0:
                flag = "LOW_WR"
            elif wr > 80.0:
                flag = "HIGH_WR"
        result[aid] = {
            "total_trades": total,
            "wins":         wins,
            "win_rate":     round(wr, 1),
            "total_pnl":    round(sum(pcts), 2),
            "flag":         flag,
        }
    return result


# ── Main daily runner ─────────────────────────────────────────────────────────

def run_daily_scorecard() -> dict[str, Any]:
    """Main entry point — called at 1:15 PM AZ (4:15 PM ET).
    Returns a summary dict.
    """
    ensure_tables()
    today      = datetime.now(AZ_TZ).date()
    today_str  = today.isoformat()
    start_str  = TRIAL_START.isoformat()
    trial_day  = (today - TRIAL_START).days + 1
    days_left  = max(0, TRIAL_DAYS - trial_day)

    # Pull all trades since trial start (for running metrics)
    all_trades = _pull_all_closed_trades(start_str)
    metrics    = _compute_metrics(all_trades)
    exec_gap   = _compute_exec_gap(metrics["win_rate"], metrics["cumulative_return"])
    statuses   = _score_benchmarks(metrics, exec_gap)
    go_count   = sum(1 for s in statuses.values() if s == "pass")

    # Today's trades for daily_trades table
    today_trades = _pull_today_trades(today_str)
    pg = _conn_pg()
    for t in today_trades:
        entry = t.get("entry_price") or 0.0
        exit_ = t.get("exit_price") or 0.0
        pnl_pct = 0.0
        if entry > 0 and exit_:
            pnl_pct = (exit_ - entry) / entry * 100.0
        pg.execute(
            """INSERT OR IGNORE INTO daily_trades
               (trade_date, agent_id, symbol, entry_price, exit_price, pnl_pct)
               VALUES (?,?,?,?,?,?)""",
            (today_str, t["player_id"], t["symbol"],
             round(entry, 4), round(exit_, 4), round(pnl_pct, 3))
        )

    # Update running_scorecard
    pg.execute(
        """INSERT OR REPLACE INTO running_scorecard
           (as_of_date, total_trades, cumulative_return, rolling_win_rate,
            rolling_sharpe, max_drawdown, consec_wins, consec_losses,
            wr_bench_status, sharpe_bench_status, dd_bench_status,
            gap_bench_status, cl_bench_status, trades_bench_status,
            go_count, exec_gap_pp, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))""",
        (today_str,
         metrics["total_trades"],    metrics["cumulative_return"],
         metrics["win_rate"],        metrics["sharpe"],
         metrics["max_drawdown"],    metrics["consec_wins"],
         metrics["consec_losses"],
         statuses["win_rate"],       statuses["sharpe"],
         statuses["max_drawdown"],   statuses["exec_gap_pp"],
         statuses["max_consec_loss"],statuses["total_trades"],
         go_count,                   exec_gap)
    )

    # Per-agent scorecard
    agent_data = _per_agent_metrics(all_trades)
    for aid, ad in agent_data.items():
        pg.execute(
            """INSERT OR REPLACE INTO agent_scorecard
               (as_of_date, agent_id, total_trades, wins, win_rate,
                total_pnl_pct, flag, updated_at)
               VALUES (?,?,?,?,?,?,?,datetime('now'))""",
            (today_str, aid, ad["total_trades"], ad["wins"],
             ad["win_rate"], ad["total_pnl"], ad["flag"])
        )

    # Track benchmark failures for consecutive-day alerting
    for bench_key, status in statuses.items():
        if status == "fail":
            pg.execute(
                "INSERT OR IGNORE INTO benchmark_failures (bench_key, fail_date, value) VALUES (?,?,?)",
                (bench_key, today_str, 0.0)
            )

    pg.commit()
    pg.close()

    # Check if ALL 6 benchmarks pass — send green alert
    if go_count == 6:
        _fire(
            title="PROVING GROUND: ALL GREEN",
            body=f"Day {trial_day}/{TRIAL_DAYS} — ALL 6 benchmarks passing. "
                 f"WR {metrics['win_rate']:.1f}%  Sharpe {metrics['sharpe']:.3f}  "
                 f"DD {metrics['max_drawdown']:.1f}%  Trades {metrics['total_trades']}\n"
                 "READY for real money deployment.",
            priority=P_MAX,
            tags="trophy"
        )

    # Check for 5+ consecutive days of benchmark failures
    _check_consecutive_failures(pg_conn=None)

    return {
        "trial_day":    trial_day,
        "days_left":    days_left,
        "total_trades": metrics["total_trades"],
        "win_rate":     metrics["win_rate"],
        "sharpe":       metrics["sharpe"],
        "max_drawdown": metrics["max_drawdown"],
        "go_count":     go_count,
        "statuses":     statuses,
        "agent_data":   agent_data,
    }


def _check_consecutive_failures(pg_conn=None) -> None:
    """Check if any benchmark has failed 5+ consecutive days. Push ntfy if so."""
    c = pg_conn or _conn_pg()
    for bench_key in BENCH:
        # Get last 5 days of data
        rows = c.execute(
            """SELECT fail_date FROM benchmark_failures
               WHERE bench_key=?
               ORDER BY fail_date DESC LIMIT ?""",
            (bench_key, CONSEC_FAIL_DAYS)
        ).fetchall()
        if len(rows) >= CONSEC_FAIL_DAYS:
            dates = [r[0] for r in rows]
            # Check consecutive (no gap)
            d = [date.fromisoformat(x) for x in dates]
            consecutive = all(
                (d[i] - d[i+1]).days == 1 for i in range(len(d)-1)
            )
            if consecutive:
                _fire(
                    title=f"PROVING GROUND: {bench_key.upper()} FAILING",
                    body=f"Benchmark '{bench_key}' has failed for {CONSEC_FAIL_DAYS} consecutive days. "
                         "Investigate before continuing Sniper Mode.",
                    priority=P_HIGH,
                    tags="warning"
                )
    if not pg_conn:
        c.close()


# ── Daily ntfy report ─────────────────────────────────────────────────────────

def send_daily_ntfy_report() -> None:
    """Push daily summary at 1:30 PM AZ (4:30 PM ET)."""
    ensure_tables()
    today   = datetime.now(AZ_TZ).date()
    today_s = today.isoformat()

    c = _conn_pg()
    row = c.execute(
        "SELECT * FROM running_scorecard WHERE as_of_date=?",
        (today_s,)
    ).fetchone()
    c.close()

    if not row:
        # No scorecard yet today — compute now
        result = run_daily_scorecard()
        row = type("R", (), result)()  # mock object
        total  = result["total_trades"]
        wr     = result["win_rate"]
        sh     = result["sharpe"]
        dd     = result["max_drawdown"]
        go     = result["go_count"]
    else:
        total = row["total_trades"]
        wr    = row["rolling_win_rate"]
        sh    = row["rolling_sharpe"]
        dd    = row["max_drawdown"]
        go    = row["go_count"]

    trial_day = (today - TRIAL_START).days + 1
    days_left = max(0, TRIAL_DAYS - trial_day)

    # Status emoji for go count
    go_str = "🟢" * go + "⬜" * (6 - go)

    body = (
        f"WR {wr:.1f}%  Sharpe {sh:.3f}  DD {dd:.1f}%\n"
        f"Trades: {total}  Go: {go}/6  {go_str}\n"
        f"{days_left} days left in trial"
    )
    _fire(
        title=f"Day {trial_day}/{TRIAL_DAYS} Proving Ground",
        body=body,
        priority=P_DEFAULT,
        tags="white_check_mark"
    )


# ── Weekly comparison (Sundays) ───────────────────────────────────────────────

def send_weekly_comparison() -> None:
    """Send weekly backtest vs actual comparison every Sunday."""
    ensure_tables()
    today = datetime.now(AZ_TZ).date()

    all_trades = _pull_all_closed_trades(TRIAL_START.isoformat())
    metrics    = _compute_metrics(all_trades)
    trial_day  = (today - TRIAL_START).days + 1

    # Weekly trade count
    week_start = (today - timedelta(days=6)).isoformat()
    week_trades = [
        t for t in all_trades
        if t.get("executed_at", "") >= week_start
    ]
    week_count = len(week_trades)

    body = (
        f"Week {max(1, trial_day // 7)} of Proving Ground — {trial_day} days in\n\n"
        f"METRIC      | BACKTEST | ACTUAL\n"
        f"Return %    | +{BT_RETURN_PCT:.2f}%  | {metrics['cumulative_return']:+.2f}%\n"
        f"Sharpe      | +{BT_SHARPE:.3f}  | {metrics['sharpe']:+.3f}\n"
        f"Win Rate    | {BT_WIN_RATE:.1f}%   | {metrics['win_rate']:.1f}%\n"
        f"Trades/wk   | ~{BT_TRADES_PER_WK:.0f}     | {week_count}\n"
        f"\nTotal trades: {metrics['total_trades']}"
    )
    _fire(
        title="WEEKLY PROVING GROUND REPORT",
        body=body,
        priority=P_DEFAULT,
        tags="bar_chart"
    )


# ── Public summary (for dashboard) ───────────────────────────────────────────

def get_proving_ground_status() -> dict[str, Any]:
    """Return current Proving Ground status dict for dashboard."""
    ensure_tables()
    today = datetime.now(AZ_TZ).date()
    trial_day = (today - TRIAL_START).days + 1
    days_left = max(0, TRIAL_DAYS - trial_day)

    c = _conn_pg()

    # Latest scorecard
    row = c.execute(
        "SELECT * FROM running_scorecard ORDER BY as_of_date DESC LIMIT 1"
    ).fetchone()

    # Per-agent
    agents = c.execute(
        """SELECT agent_id, total_trades, win_rate, total_pnl_pct, flag
           FROM agent_scorecard
           WHERE as_of_date = (SELECT MAX(as_of_date) FROM agent_scorecard)
           ORDER BY agent_id"""
    ).fetchall()
    c.close()

    if not row:
        return {
            "trial_day": trial_day,
            "days_left": days_left,
            "trial_start": TRIAL_START.isoformat(),
            "total_trades": 0,
            "win_rate": 0.0,
            "sharpe": 0.0,
            "max_drawdown": 0.0,
            "go_count": 0,
            "statuses": {},
            "agents": [],
            "backtest": {
                "return_pct": BT_RETURN_PCT,
                "sharpe": BT_SHARPE,
                "win_rate": BT_WIN_RATE,
                "trades_per_week": BT_TRADES_PER_WK,
            }
        }

    statuses = {
        "win_rate":        row["wr_bench_status"],
        "sharpe":          row["sharpe_bench_status"],
        "max_drawdown":    row["dd_bench_status"],
        "exec_gap":        row["gap_bench_status"],
        "max_consec_loss": row["cl_bench_status"],
        "total_trades":    row["trades_bench_status"],
    }

    return {
        "trial_day":       trial_day,
        "days_left":       days_left,
        "trial_start":     TRIAL_START.isoformat(),
        "total_trades":    row["total_trades"],
        "cumulative_return": row["cumulative_return"],
        "win_rate":        row["rolling_win_rate"],
        "sharpe":          row["rolling_sharpe"],
        "max_drawdown":    row["max_drawdown"],
        "consec_wins":     row["consec_wins"],
        "consec_losses":   row["consec_losses"],
        "go_count":        row["go_count"],
        "exec_gap_pp":     row["exec_gap_pp"],
        "statuses":        statuses,
        "agents":          [dict(a) for a in agents],
        "backtest": {
            "return_pct":     BT_RETURN_PCT,
            "sharpe":         BT_SHARPE,
            "win_rate":       BT_WIN_RATE,
            "trades_per_week": BT_TRADES_PER_WK,
        }
    }
