"""
Crew learning loop — records trade outcomes and surfaces patterns to guide future runs.

Flow:
  sync_closed_positions()  — called after each pipeline run to detect closed Alpaca positions
  get_learning_context()   — injected into Scout prompt before each pipeline run
  get_weekly_summary()     — injected into Sunday review focus areas
  get_performance_stats()  — powers /api/crew/performance endpoint
"""

import logging
import os
import sqlite3
from datetime import datetime
from typing import Optional

log = logging.getLogger("crew.learning")

DB_PATH = os.environ.get("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))


def _db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def ensure_schema():
    """Create crew_trade_results if it doesn't already exist."""
    conn = _db()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS crew_trade_results (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                position_id      INTEGER,
                strategy_id      INTEGER,
                ticker           TEXT NOT NULL,
                direction        TEXT DEFAULT 'long',
                entry_price      REAL,
                exit_price       REAL,
                pnl              REAL,
                pnl_pct          REAL,
                qty              INTEGER,
                dollar_value     REAL,
                conviction_score REAL,
                strategy_type    TEXT,
                outcome          TEXT,   -- 'win' | 'loss' | 'stopped' | 'unknown'
                duration_days    REAL,
                what_worked      TEXT,
                what_failed      TEXT,
                notes            TEXT,
                source_bucket    TEXT,
                source_agent     TEXT,
                recorded_at      TEXT DEFAULT (datetime('now'))
            )
        """)
        for column_name in ("source_bucket", "source_agent"):
            try:
                conn.execute(f"ALTER TABLE crew_trade_results ADD COLUMN {column_name} TEXT")
            except sqlite3.OperationalError:
                pass
        conn.commit()
    finally:
        conn.close()


def _extract_note_value(notes: str, key: str) -> Optional[str]:
    if not notes or f"{key}=" not in notes:
        return None
    try:
        return notes.split(f"{key}=", 1)[1].split("|", 1)[0].strip()
    except Exception:
        return None


def _parse_strategy_id(notes_str: str) -> Optional[int]:
    if "strategy_id=" not in notes_str:
        return None
    try:
        sid_part = notes_str.split("strategy_id=")[1].split("|")[0].strip()
        return int(sid_part)
    except Exception:
        return None


def _assert_closed_position_integrity(pos: dict) -> None:
    if pos.get("status") == "closed":
        if pos.get("current_price") is None:
            raise ValueError(f"Closed position missing current_price for id={pos.get('id')}")
        if pos.get("closed_pnl") is None:
            raise ValueError(f"Closed position missing closed_pnl for id={pos.get('id')}")
        if not pos.get("closed_at"):
            raise ValueError(f"Closed position missing closed_at for id={pos.get('id')}")


    """
    Closed-position integrity guard

    Purpose:
    Fail fast if any routed portfolio_positions rows have been marked closed
    without the required close metadata.

    A valid closed row must have:
    - current_price
    - closed_pnl
    - closed_at

    Why:
    The Anderson reconstruction path depends on closed_pnl for realized P&L.
    If closed rows are missing these fields, leaderboard/account state can
    incorrectly reconstruct as flat.

    Behavior:
    - scans for closed rows missing required fields
    - raises RuntimeError with sample broken rows
    - intended as a guard against silent data corruption/regressions
    """
    bad = conn.execute(
        """
        SELECT id, portfolio_id, ticker
        FROM portfolio_positions
        WHERE status='closed'
          AND (closed_pnl IS NULL OR closed_at IS NULL OR current_price IS NULL)
        LIMIT 20
        """
    ).fetchall()

    if bad:
        raise RuntimeError(f"Broken closed portfolio_positions rows detected: {bad}")


def _record_position_outcome(conn, pos, now: str) -> bool:
    _assert_closed_position_integrity(pos)

    ticker = pos["ticker"].upper()
    exit_price = float(pos["current_price"] or 0)
    entry = float(pos["entry_price"] or 0)
    qty = int(float(pos["quantity"] or 1))
    closed_pnl = pos["closed_pnl"]

    if closed_pnl is not None:
        pnl = round(float(closed_pnl), 2)
    else:
        pnl = round((exit_price - entry) * qty, 2) if entry > 0 and exit_price > 0 else None
    pnl_pct = round((pnl / (entry * qty)) * 100, 2) if pnl is not None and entry > 0 and qty > 0 else None

    outcome = "unknown"
    if pnl is not None:
        sl = float(pos["stop_loss"] or 0)
        tp = float(pos["take_profit"] or 0)
        if tp > 0 and exit_price >= tp:
            outcome = "win"
        elif sl > 0 and exit_price <= sl:
            outcome = "stopped"
        elif pnl >= 0:
            outcome = "win"
        else:
            outcome = "loss"

    notes_str = pos["notes"] or ""
    strategy_id = _parse_strategy_id(notes_str)
    source_bucket = _extract_note_value(notes_str, "source_bucket")
    source_agent = _extract_note_value(notes_str, "source_agent")
    if not source_bucket and strategy_id is not None:
        source_bucket = "LegacyCrew"
    if not source_agent and strategy_id is not None:
        source_agent = f"strategy-{strategy_id}"
    portfolio_id = int(pos["portfolio_id"] or 0) if "portfolio_id" in pos.keys() else 0
    if not source_bucket and portfolio_id == 1:
        source_bucket = "LegacyCrew"
    if not source_agent and source_bucket == "LegacyCrew":
        source_agent = "legacy-unattributed"

    conviction_score = None
    strategy_type = pos["asset_class"] or "stock"
    if strategy_id:
        try:
            row = conn.execute(
                "SELECT conviction_score, asset_class FROM crew_strategies WHERE id=?",
                (strategy_id,),
            ).fetchone()
            if row:
                conviction_score = row["conviction_score"]
                strategy_type = row["asset_class"] or strategy_type
        except Exception:
            pass

    duration_days = None
    try:
        opened = datetime.fromisoformat(str(pos["created_at"]).replace("Z", ""))
        closed_at = pos["closed_at"] or now
        closed_dt = datetime.fromisoformat(str(closed_at).replace("Z", ""))
        duration_days = round((closed_dt - opened).total_seconds() / 86400, 1)
    except Exception:
        pass

    already = conn.execute(
        "SELECT id FROM crew_trade_results WHERE position_id=?",
        (pos["id"],),
    ).fetchone()
    if already:
        updates = []
        params = []
        if source_bucket:
            updates.append("source_bucket=?")
            params.append(source_bucket)
        if source_agent:
            updates.append("source_agent=?")
            params.append(source_agent)
        if updates:
            params.append(pos["id"])
            conn.execute(
                f"UPDATE crew_trade_results SET {', '.join(updates)} WHERE position_id=?",
                tuple(params),
            )
        return False

    conn.execute(
        """INSERT INTO crew_trade_results
           (position_id, strategy_id, ticker, direction, entry_price, exit_price,
            pnl, pnl_pct, qty, dollar_value, conviction_score, strategy_type,
            outcome, duration_days, notes, source_bucket, source_agent, recorded_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            pos["id"],
            strategy_id,
            ticker,
            pos["direction"] or "long",
            entry,
            exit_price,
            pnl,
            pnl_pct,
            qty,
            round(qty * entry, 2) if qty and entry else None,
            conviction_score,
            strategy_type,
            outcome,
            duration_days,
            notes_str or "auto-recorded at close",
            source_bucket,
            source_agent,
            now,
        ),
    )
    return True


# ---------------------------------------------------------------------------
# Sync closed positions → crew_trade_results
# ---------------------------------------------------------------------------

def sync_closed_positions() -> dict:
    """
    Detect positions that closed on Alpaca but are still 'open' in our DB.
    Record each outcome in crew_trade_results and mark the position closed.
    Returns {"recorded": N}.
    """
    ensure_schema()

    live_tickers = set()
    try:
        import sys
        sys.path.insert(0, os.path.expanduser("~/autonomous-trader"))
        from engine.alpaca_bridge import AlpacaBridge
        bridge = AlpacaBridge()
        if bridge.client:
            live_tickers = {p.symbol.upper() for p in bridge.client.get_all_positions()}
    except Exception as e:
        log.warning(f"[learning] Alpaca unavailable during sync_closed_positions: {e}")

    conn = _db()
    recorded = 0
    now = datetime.now().isoformat()

    try:
        open_pos = conn.execute(
            "SELECT id, portfolio_id, ticker, direction, entry_price, stop_loss, take_profit, "
            "quantity, current_price, notes, created_at, closed_at, closed_pnl, asset_class "
            "FROM portfolio_positions "
            "WHERE status='open' AND portfolio_id=1"
        ).fetchall()

        for pos in open_pos:
            ticker = pos["ticker"].upper()
            if ticker in live_tickers:
                continue  # Still open on Alpaca — skip

            # Closed. Use current_price as exit, fall back to market price.
            exit_price = float(pos["current_price"] or 0)
            if exit_price == 0:
                try:
                    from engine.market_data import get_stock_price
                    d = get_stock_price(ticker)
                    exit_price = float(d.get("price", 0) or 0)
                except Exception:
                    pass

            entry = float(pos["entry_price"] or 0)
            qty = int(pos["quantity"] or 1)
            pnl = round((exit_price - entry) * qty, 2) if entry > 0 and exit_price > 0 else None
            pnl_pct = round((exit_price - entry) / entry * 100, 2) if entry > 0 and exit_price > 0 else None

            if any(v is None for v in (exit_price, pnl, now)):
                raise ValueError("Refusing to close position with incomplete close metadata")

            pos = dict(pos)
            pos["current_price"] = exit_price
            pos["closed_pnl"] = pnl
            pos["closed_at"] = now
            pos["status"] = "closed"
            _assert_closed_position_integrity(pos)
            inserted = _record_position_outcome(conn, pos, now)
            conn.execute(
                """
                UPDATE portfolio_positions
                SET status='closed',
                    current_price=?,
                    closed_pnl=?,
                    closed_at=?,
                    updated_at=?
                WHERE id=?
                """,
                (
                    exit_price,
                    pnl,
                    now,
                    now,
                    pos["id"],
                )
            )
            if inserted:
                recorded += 1

        # Also backfill already-closed Anderson rows that were never recorded.
        closed_pos = conn.execute(
            "SELECT id, portfolio_id, ticker, direction, quantity, entry_price, current_price, stop_loss, take_profit, "
            "closed_pnl, notes, created_at, closed_at, asset_class "
            "FROM portfolio_positions "
            "WHERE status='closed' AND portfolio_id=1"
        ).fetchall()
        for pos in closed_pos:
            if _record_position_outcome(conn, pos, now):
                recorded += 1

        assert_no_broken_closed_positions(conn)
        conn.commit()
        return {"recorded": recorded}

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Learning context — injected into crew prompts
# ---------------------------------------------------------------------------

def get_learning_context(limit: int = 20) -> str:
    """
    Return the last N trade results as a formatted block for injection into crew prompts.
    Returns empty string if no results yet.
    """
    ensure_schema()
    conn = _db()
    try:
        rows = conn.execute(
            """SELECT ticker, direction, pnl, pnl_pct, conviction_score,
                      strategy_type, outcome, duration_days
               FROM crew_trade_results
               ORDER BY recorded_at DESC LIMIT ?""",
            (limit,),
        ).fetchall()

        if not rows:
            return ""

        wins = [r for r in rows if r["outcome"] == "win"]
        losses = [r for r in rows if r["outcome"] in ("loss", "stopped")]
        with_pnl = [r for r in rows if r["pnl"] is not None]
        total_pnl = sum(r["pnl"] for r in with_pnl)
        win_rate = round(len(wins) / len(rows) * 100, 1)

        lines = [
            "",
            f"=== CREW LEARNING CONTEXT (last {len(rows)} closed trades) ===",
            f"Win Rate: {win_rate}%  |  Net P&L: ${total_pnl:+.2f}  |  "
            f"Wins: {len(wins)}  Losses: {len(losses)}",
            "Recent outcomes:",
        ]
        for r in rows:
            pnl_s = f"${r['pnl']:+.2f} ({r['pnl_pct']:+.1f}%)" if r["pnl"] is not None else "P&L unknown"
            held = f"{r['duration_days']}d" if r["duration_days"] else "?"
            conv = r["conviction_score"] or "?"
            lines.append(
                f"  {r['outcome'].upper():8s} | {r['ticker']:6s} | {pnl_s:22s} | "
                f"conviction={conv} | type={r['strategy_type'] or '?'} | held={held}"
            )

        if with_pnl:
            best = max(with_pnl, key=lambda r: r["pnl"])
            worst = min(with_pnl, key=lambda r: r["pnl"])
            lines.append(f"Best:  {best['ticker']} ${best['pnl']:+.2f} ({best['strategy_type']})")
            lines.append(f"Worst: {worst['ticker']} ${worst['pnl']:+.2f} ({worst['strategy_type']})")

        # Strategy type win rates
        type_stats: dict = {}
        for r in rows:
            t = r["strategy_type"] or "unknown"
            if t not in type_stats:
                type_stats[t] = {"n": 0, "wins": 0}
            type_stats[t]["n"] += 1
            if r["outcome"] == "win":
                type_stats[t]["wins"] += 1
        for t, s in type_stats.items():
            wr = round(s["wins"] / s["n"] * 100)
            lines.append(f"  {t}: {s['wins']}/{s['n']} wins ({wr}%)")

        lines.append("=== USE THIS DATA TO IMPROVE YOUR DECISIONS ===")
        return "\n".join(lines)

    finally:
        conn.close()


def get_weekly_summary() -> str:
    """Summary of the past 7 days for the Sunday review session."""
    ensure_schema()
    conn = _db()
    try:
        rows = conn.execute(
            """SELECT ticker, direction, pnl, pnl_pct, conviction_score,
                      strategy_type, outcome, duration_days
               FROM crew_trade_results
               WHERE recorded_at >= datetime('now', '-7 days')
               ORDER BY recorded_at DESC""",
        ).fetchall()

        if not rows:
            return "No closed trades in the past 7 days. Consider reviewing open positions."

        wins = [r for r in rows if r["outcome"] == "win"]
        losses = [r for r in rows if r["outcome"] == "loss"]
        stopped = [r for r in rows if r["outcome"] == "stopped"]
        with_pnl = [r for r in rows if r["pnl"] is not None]
        total_pnl = sum(r["pnl"] for r in with_pnl)
        win_rate = round(len(wins) / len(rows) * 100, 1)

        lines = [
            "=== WEEKLY PERFORMANCE REVIEW (past 7 days) ===",
            f"Trades: {len(rows)}  |  Wins: {len(wins)}  |  Losses: {len(losses)}  |  Stopped: {len(stopped)}",
            f"Win Rate: {win_rate}%  |  Net P&L: ${total_pnl:+.2f}",
            "",
            "Trade log:",
        ]
        for r in rows:
            pnl_s = f"${r['pnl']:+.2f}" if r["pnl"] is not None else "?"
            lines.append(
                f"  {r['outcome'].upper():8s} | {r['ticker']:6s} | {pnl_s:10s} | "
                f"{r['strategy_type'] or '?'} | conviction={r['conviction_score'] or '?'}"
            )

        if wins:
            lines.append("\nWHAT WORKED:")
            for w in wins:
                lines.append(
                    f"  + {w['ticker']}: {w['strategy_type']}, conviction={w['conviction_score']}, "
                    f"held {w['duration_days']}d, ${w['pnl']:+.2f}"
                )

        if losses or stopped:
            lines.append("\nWHAT DIDN'T WORK:")
            for l in losses + stopped:
                pnl_s = f"${l['pnl']:.2f}" if l["pnl"] is not None else "?"
                lines.append(
                    f"  - {l['ticker']}: {l['strategy_type']}, conviction={l['conviction_score']}, "
                    f"held {l['duration_days']}d, {pnl_s}"
                )

        lines.append("\n=== ADJUST YOUR APPROACH BASED ON THESE RESULTS ===")
        return "\n".join(lines)

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Performance stats — for /api/crew/performance
# ---------------------------------------------------------------------------

def get_performance_stats() -> dict:
    ensure_schema()
    conn = _db()
    try:
        rows = conn.execute(
            """SELECT id, ticker, direction, entry_price, exit_price, pnl, pnl_pct,
                      conviction_score, strategy_type, outcome, duration_days, recorded_at
               FROM crew_trade_results
               ORDER BY recorded_at DESC"""
        ).fetchall()
        rows = [dict(r) for r in rows]

        if not rows:
            return {
                "total_trades": 0, "wins": 0, "losses": 0, "win_rate": 0,
                "total_pnl": 0, "avg_win": 0, "avg_loss": 0,
                "best_trade": None, "worst_trade": None,
                "by_strategy_type": {}, "recent": [],
            }

        wins = [r for r in rows if r["outcome"] == "win"]
        losses = [r for r in rows if r["outcome"] in ("loss", "stopped")]
        with_pnl = [r for r in rows if r["pnl"] is not None]
        total_pnl = round(sum(r["pnl"] for r in with_pnl), 2)
        avg_win = round(sum(r["pnl"] for r in wins if r["pnl"] is not None) / len(wins), 2) if wins else 0
        avg_loss = round(sum(r["pnl"] for r in losses if r["pnl"] is not None) / len(losses), 2) if losses else 0

        best = max(with_pnl, key=lambda r: r["pnl"]) if with_pnl else None
        worst = min(with_pnl, key=lambda r: r["pnl"]) if with_pnl else None

        by_type: dict = {}
        for r in rows:
            t = r["strategy_type"] or "unknown"
            if t not in by_type:
                by_type[t] = {"trades": 0, "wins": 0, "total_pnl": 0.0, "win_rate": 0}
            by_type[t]["trades"] += 1
            if r["outcome"] == "win":
                by_type[t]["wins"] += 1
            if r["pnl"] is not None:
                by_type[t]["total_pnl"] += r["pnl"]
        for t in by_type:
            n = by_type[t]["trades"]
            by_type[t]["win_rate"] = round(by_type[t]["wins"] / n * 100, 1)
            by_type[t]["total_pnl"] = round(by_type[t]["total_pnl"], 2)

        return {
            "total_trades": len(rows),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": round(len(wins) / len(rows) * 100, 1),
            "total_pnl": total_pnl,
            "avg_win": avg_win,
            "avg_loss": avg_loss,
            "best_trade": best,
            "worst_trade": worst,
            "by_strategy_type": by_type,
            "recent": rows[:20],
        }

    finally:
        conn.close()
