"""Trade Outcome Tracker — records closed trade results for memory-driven learning.

Every time a position closes, we capture the outcome with full market context
(regime, VIX, fear/greed) so models can learn what conditions they perform best in.

Tables used (never dropped):
  trade_outcomes — one row per closed trade
"""
from __future__ import annotations
import os
import re
import sqlite3
from collections import Counter
from datetime import datetime, timedelta

from rich.console import Console

console = Console()
DB = os.environ.get(
    "TRADEMINDS_DB",
    os.path.expanduser("~/autonomous-trader/data/trader.db"),
)


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def init_trade_outcomes_table() -> None:
    """Create trade_outcomes table and ensure all columns exist. Safe to call repeatedly."""
    conn = _conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trade_outcomes (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id              INTEGER,
            player_id             TEXT NOT NULL,
            symbol                TEXT NOT NULL,
            entry_price           REAL,
            exit_price            REAL,
            entry_time            TEXT,
            exit_time             TEXT,
            pnl_dollars           REAL,
            pnl_percent           REAL,
            hold_duration_hours   REAL,
            regime_at_entry       TEXT,
            gex_regime_at_entry   TEXT,
            vix_at_entry          REAL,
            fear_greed_at_entry   REAL,
            strategy_name         TEXT,
            conviction_at_entry   REAL,
            outcome               TEXT,
            created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Context lookups
# ---------------------------------------------------------------------------

def _lookup_regime(entry_time: str) -> str | None:
    """Return BULL/BEAR/NEUTRAL for the day of entry_time from regime_history."""
    try:
        date_str = str(entry_time)[:10]  # YYYY-MM-DD
        conn = _conn()
        row = conn.execute(
            "SELECT regime FROM regime_history WHERE date=?", (date_str,)
        ).fetchone()
        conn.close()
        return row["regime"] if row else None
    except Exception:
        return None


def _parse_strategy_name(reasoning: str | None, sources: str | None) -> str | None:
    """Extract primary strategy name from trade reasoning or sources."""
    try:
        if reasoning:
            m = re.search(r"strategies agree \(([^)]+)\)", reasoning)
            if m:
                names = [n.strip() for n in m.group(1).split(",")]
                return names[0] if names else None
    except Exception:
        pass
    try:
        if sources:
            return sources
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Core recording
# ---------------------------------------------------------------------------

def record_trade_outcome(
    trade_id: int,
    entry_price: float,
    exit_price: float,
    entry_time: str,
    exit_time: str,
    player_id: str,
    symbol: str,
    realized_pnl: float | None = None,
    conviction: float | None = None,
    reasoning: str | None = None,
    sources: str | None = None,
) -> bool:
    """Calculate all fields and insert a trade outcome record.

    Returns True on success, False on any error.
    All context lookups are wrapped — null stored if data unavailable.
    """
    try:
        # PnL: prefer realized_pnl (total $) from paper_trader; calculate percent from prices
        if realized_pnl is not None:
            pnl_dollars = realized_pnl
        elif entry_price and entry_price > 0:
            pnl_dollars = exit_price - entry_price
        else:
            pnl_dollars = 0.0

        if entry_price and entry_price > 0:
            pnl_percent = ((exit_price - entry_price) / entry_price) * 100
        else:
            pnl_percent = 0.0

        # Hold duration
        hold_hours = None
        try:
            t_entry = datetime.fromisoformat(str(entry_time).replace("Z", ""))
            t_exit = datetime.fromisoformat(str(exit_time).replace("Z", ""))
            hold_hours = round((t_exit - t_entry).total_seconds() / 3600, 2)
        except Exception:
            pass

        # Outcome label
        if pnl_dollars > 0:
            outcome = "win"
        elif pnl_dollars < 0:
            outcome = "loss"
        else:
            outcome = "breakeven"

        # Context lookups — all wrapped, null on failure
        regime = _lookup_regime(entry_time)
        gex_regime = None        # GEX not persisted historically
        vix_at_entry = None      # VIX not persisted historically
        fear_greed = None        # F&G not persisted historically

        strategy_name = _parse_strategy_name(reasoning, sources)

        conn = _conn()
        conn.execute("""
            INSERT INTO trade_outcomes
                (trade_id, player_id, symbol, entry_price, exit_price,
                 entry_time, exit_time, pnl_dollars, pnl_percent,
                 hold_duration_hours, regime_at_entry, gex_regime_at_entry,
                 vix_at_entry, fear_greed_at_entry, strategy_name,
                 conviction_at_entry, outcome)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            trade_id, player_id, symbol,
            entry_price, exit_price,
            entry_time, exit_time,
            round(pnl_dollars, 4), round(pnl_percent, 4),
            hold_hours,
            regime, gex_regime, vix_at_entry, fear_greed,
            strategy_name, conviction, outcome,
        ))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        console.log(f"[yellow]trade_outcomes: record failed for {symbol}: {e}")
        return False


# ---------------------------------------------------------------------------
# Auto-backfill scheduler job
# ---------------------------------------------------------------------------

def auto_record_closed_trades() -> None:
    """Scan trades table for closed positions not yet in trade_outcomes. Backfills them.

    A closed trade = SELL with realized_pnl IS NOT NULL.
    Runs every 5 minutes via scheduler in main.py.
    """
    try:
        init_trade_outcomes_table()
        conn = _conn()

        # Find SELL trades not yet recorded
        sells = conn.execute("""
            SELECT t.id, t.player_id, t.symbol,
                   t.price          AS sell_price,
                   t.exit_price     AS exit_price_col,
                   t.realized_pnl,
                   t.executed_at    AS exit_time
            FROM trades t
            WHERE t.action = 'SELL'
              AND t.realized_pnl IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM trade_outcomes o WHERE o.trade_id = t.id
              )
            ORDER BY t.executed_at DESC
            LIMIT 500
        """).fetchall()

        recorded = 0
        for sell in sells:
            # Find the most recent BUY for this player+symbol before the sell
            buy = conn.execute("""
                SELECT id, price AS entry_price, entry_price AS ep_col,
                       executed_at AS entry_time,
                       confidence, reasoning, sources
                FROM trades
                WHERE player_id = ? AND symbol = ? AND action = 'BUY'
                  AND executed_at < ?
                ORDER BY executed_at DESC
                LIMIT 1
            """, (sell["player_id"], sell["symbol"], sell["exit_time"])).fetchone()

            if not buy:
                continue

            # Resolve entry price: prefer entry_price column, fall back to price
            entry_price = buy["ep_col"] or buy["entry_price"]
            # Resolve exit price: prefer exit_price column, fall back to sell price
            exit_price = sell["exit_price_col"] or sell["sell_price"]

            ok = record_trade_outcome(
                trade_id=sell["id"],
                entry_price=entry_price,
                exit_price=exit_price,
                entry_time=buy["entry_time"],
                exit_time=sell["exit_time"],
                player_id=sell["player_id"],
                symbol=sell["symbol"],
                realized_pnl=sell["realized_pnl"],
                conviction=buy["confidence"],
                reasoning=buy["reasoning"],
                sources=buy["sources"],
            )
            if ok:
                recorded += 1

        conn.close()
        if recorded:
            console.log(f"[green]trade_outcomes: recorded {recorded} new closed trade outcome(s)")
    except Exception as e:
        console.log(f"[yellow]auto_record_closed_trades error: {e}")


# ---------------------------------------------------------------------------
# Stats queries
# ---------------------------------------------------------------------------

def get_player_stats(player_id: str, lookback_days: int = 30) -> dict:
    """Return trading performance stats for a player over lookback_days.

    Returns dict with total_trades, wins, losses, win_rate, avg_pnl,
    best_trade, worst_trade, avg_hold_hours, preferred_regime,
    regime_stats, last_10_trades. Returns {"empty": True} if no data.
    """
    try:
        init_trade_outcomes_table()
        conn = _conn()
        cutoff = (datetime.now() - timedelta(days=lookback_days)).isoformat()

        rows = conn.execute("""
            SELECT symbol, pnl_dollars, pnl_percent, outcome,
                   hold_duration_hours, regime_at_entry, exit_time
            FROM trade_outcomes
            WHERE player_id = ? AND created_at >= ?
            ORDER BY created_at DESC
        """, (player_id, cutoff)).fetchall()
        conn.close()

        if not rows:
            return {"empty": True, "player_id": player_id}

        total = len(rows)
        wins = sum(1 for r in rows if r["outcome"] == "win")
        losses = sum(1 for r in rows if r["outcome"] == "loss")
        win_rate = round(wins / total * 100, 1) if total > 0 else 0.0
        avg_pnl = round(sum((r["pnl_dollars"] or 0) for r in rows) / total, 2)

        # Best / worst
        sorted_rows = sorted(rows, key=lambda r: (r["pnl_dollars"] or 0))
        worst = sorted_rows[0]
        best = sorted_rows[-1]

        # Avg hold
        hold_vals = [r["hold_duration_hours"] for r in rows if r["hold_duration_hours"] is not None]
        avg_hold = round(sum(hold_vals) / len(hold_vals), 1) if hold_vals else 0.0

        # Regime breakdown
        regime_stats: dict[str, dict] = {}
        for r in rows:
            reg = r["regime_at_entry"] or "UNKNOWN"
            if reg not in regime_stats:
                regime_stats[reg] = {"wins": 0, "total": 0}
            regime_stats[reg]["total"] += 1
            if r["outcome"] == "win":
                regime_stats[reg]["wins"] += 1

        # Preferred regime (most wins)
        regime_wins = Counter(
            r["regime_at_entry"] for r in rows
            if r["outcome"] == "win" and r["regime_at_entry"]
        )
        preferred_regime = regime_wins.most_common(1)[0][0] if regime_wins else None

        # Last 10 trades
        last_10 = [
            {
                "symbol": r["symbol"],
                "pnl_dollars": round(r["pnl_dollars"] or 0, 2),
                "outcome": r["outcome"],
                "date": str(r["exit_time"] or "")[:10],
                "regime": r["regime_at_entry"],
            }
            for r in list(rows)[:10]
        ]

        return {
            "player_id": player_id,
            "total_trades": total,
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "avg_pnl": avg_pnl,
            "best_trade": {
                "symbol": best["symbol"],
                "pnl": round(best["pnl_dollars"] or 0, 2),
                "regime": best["regime_at_entry"],
                "hold_hours": best["hold_duration_hours"],
            },
            "worst_trade": {
                "symbol": worst["symbol"],
                "pnl": round(worst["pnl_dollars"] or 0, 2),
                "regime": worst["regime_at_entry"],
                "hold_hours": worst["hold_duration_hours"],
            },
            "avg_hold_hours": avg_hold,
            "preferred_regime": preferred_regime,
            "regime_stats": regime_stats,
            "last_10_trades": last_10,
        }
    except Exception as e:
        console.log(f"[yellow]get_player_stats error: {e}")
        return {"error": str(e), "player_id": player_id}


def get_strategy_stats(lookback_days: int = 60) -> dict:
    """Return per-strategy win rate and avg PnL for Chekov's strategies.

    Returns dict of strategy_name → {trades, wins, win_rate, avg_pnl}.
    Used by score_convergence() for weighted scoring and memory injection.
    """
    try:
        init_trade_outcomes_table()
        conn = _conn()
        cutoff = (datetime.now() - timedelta(days=lookback_days)).isoformat()

        rows = conn.execute("""
            SELECT strategy_name, outcome, pnl_dollars
            FROM trade_outcomes
            WHERE strategy_name IS NOT NULL AND created_at >= ?
        """, (cutoff,)).fetchall()
        conn.close()

        if not rows:
            return {}

        stats: dict[str, dict] = {}
        for r in rows:
            name = r["strategy_name"]
            if name not in stats:
                stats[name] = {"trades": 0, "wins": 0, "pnl_total": 0.0}
            stats[name]["trades"] += 1
            if r["outcome"] == "win":
                stats[name]["wins"] += 1
            stats[name]["pnl_total"] += r["pnl_dollars"] or 0.0

        return {
            name: {
                "trades": s["trades"],
                "wins": s["wins"],
                "win_rate": round(s["wins"] / s["trades"] * 100, 1) if s["trades"] > 0 else 0.0,
                "avg_pnl": round(s["pnl_total"] / s["trades"], 2) if s["trades"] > 0 else 0.0,
            }
            for name, s in stats.items()
        }
    except Exception as e:
        console.log(f"[yellow]get_strategy_stats error: {e}")
        return {}
