"""HM-POST-EXIT-TRACKER 2026-05-20 — flag exits that proved premature.

When a position is sold, we record the exit context in post_exit_watch.
A daily scanner checks whether the symbol continued >threshold_pct above
the exit price. If so, the row is flagged and a [POST-EXIT-FLAG] log line
is emitted for ops review. Watches age out after 30 days regardless of
outcome to keep the table bounded.

Schema (data/trader.db.post_exit_watch — created by setup_db.py):
    id, player_id, symbol, exit_price, exit_date, exit_pnl,
    peak_price_after, peak_date_after, missed_gain, threshold_pct,
    flagged (0|1), created_at

Public API:
    register_exit(player_id, symbol, exit_price, exit_pnl) -> int | None
    run_daily_scan() -> dict[str, int]   # counts of {checked, newly_flagged, aged_out}
"""
from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

try:
    from rich.console import Console as _Console
    _console = _Console()
    def _log(msg: str) -> None:
        _console.log(msg)
except Exception:
    def _log(msg: str) -> None:
        print(msg)

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DB_PATH = _REPO_ROOT / "data" / "trader.db"

# Default threshold: a sold position is considered "premature" if the
# symbol traded >5% above the exit price within the watch window.
_DEFAULT_THRESHOLD_PCT = 5.0

# Watch window: stop checking a row after this many days regardless of outcome.
_WATCH_WINDOW_DAYS = 30


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(str(_DB_PATH), check_same_thread=False, timeout=10)
    c.execute("PRAGMA busy_timeout=10000")
    c.row_factory = sqlite3.Row
    return c


def register_exit(
    player_id: str,
    symbol: str,
    exit_price: float,
    exit_pnl: float | None,
    threshold_pct: float = _DEFAULT_THRESHOLD_PCT,
) -> int | None:
    """Insert a new post_exit_watch row at SELL time.

    Crash-safe: returns None on any error. Caller (paper_trader.sell()) must
    not propagate failures here into the SELL completion path.
    """
    try:
        c = _conn()
        try:
            cur = c.execute(
                "INSERT INTO post_exit_watch "
                "(player_id, symbol, exit_price, exit_date, exit_pnl, threshold_pct, flagged) "
                "VALUES (?, ?, ?, date('now','localtime'), ?, ?, 0)",
                (player_id, symbol, float(exit_price), exit_pnl, float(threshold_pct)),
            )
            c.commit()
            return cur.lastrowid
        finally:
            c.close()
    except Exception:
        return None


def run_daily_scan() -> dict:
    """Daily check of all unflagged post_exit_watch rows.

    For each row inside the watch window:
      - Fetch current symbol price
      - If current_price > exit_price * (1 + threshold_pct/100): set flagged=1,
        record peak_price_after / peak_date_after / missed_gain, emit a
        [POST-EXIT-FLAG] log line.

    Rows older than _WATCH_WINDOW_DAYS are aged out (flagged=0 stays but no
    further checks). The unique 'unflagged + within window' set is the
    active watchlist.

    Crash-safe — individual symbol failures are logged but don't stop the scan.
    Returns counts dict for the caller (scheduler) to log.
    """
    counts = {"checked": 0, "newly_flagged": 0, "aged_out": 0, "errors": 0}
    try:
        from engine.market_data import get_stock_price
    except Exception as _imp_err:
        _log(f"[red][HM-POST-EXIT-TRACKER] market_data import failed: {_imp_err!r}")
        return counts

    try:
        c = _conn()
    except Exception as _conn_err:
        _log(f"[red][HM-POST-EXIT-TRACKER] DB connect failed: {_conn_err!r}")
        return counts

    try:
        rows = c.execute(
            "SELECT id, player_id, symbol, exit_price, exit_date, exit_pnl, "
            "       threshold_pct, flagged "
            "FROM post_exit_watch "
            "WHERE flagged = 0 "
            "  AND date(exit_date) >= date('now', '-' || ? || ' days')",
            (_WATCH_WINDOW_DAYS,),
        ).fetchall()
    except Exception as _q_err:
        _log(f"[red][HM-POST-EXIT-TRACKER] scan query failed: {_q_err!r}")
        c.close()
        return counts

    for r in rows:
        counts["checked"] += 1
        try:
            px = get_stock_price(r["symbol"]) or {}
            if "error" in px:
                continue
            current = float(px.get("price") or 0.0)
            if current <= 0:
                continue
            exit_price = float(r["exit_price"] or 0.0)
            if exit_price <= 0:
                continue
            threshold = float(r["threshold_pct"] or _DEFAULT_THRESHOLD_PCT)
            trigger_price = exit_price * (1.0 + threshold / 100.0)
            if current > trigger_price:
                # Flag: position was sold but symbol ran >threshold% above exit.
                missed_pct = (current - exit_price) / exit_price * 100.0
                missed_gain = (current - exit_price)  # per-share basis; caller can scale by qty if needed
                try:
                    c.execute(
                        "UPDATE post_exit_watch SET "
                        "    peak_price_after = ?, "
                        "    peak_date_after = date('now','localtime'), "
                        "    missed_gain = ?, "
                        "    flagged = 1 "
                        "WHERE id = ?",
                        (current, missed_gain, r["id"]),
                    )
                    c.commit()
                    counts["newly_flagged"] += 1
                    _log(
                        f"[yellow][POST-EXIT-FLAG] player={r['player_id']} "
                        f"symbol={r['symbol']} exit=${exit_price:.2f} "
                        f"now=${current:.2f} missed={missed_pct:+.2f}%"
                    )
                except Exception as _upd_err:
                    _log(
                        f"[red][HM-POST-EXIT-TRACKER] flag-update failed "
                        f"{r['symbol']}: {type(_upd_err).__name__}: {_upd_err!r}"
                    )
                    counts["errors"] += 1
        except Exception as _row_err:
            _log(
                f"[red][HM-POST-EXIT-TRACKER] row scan error "
                f"{r['symbol']}: {type(_row_err).__name__}: {_row_err!r}"
            )
            counts["errors"] += 1

    # Count rows that have aged out (informational; we don't delete them)
    try:
        aged = c.execute(
            "SELECT COUNT(*) FROM post_exit_watch "
            "WHERE flagged = 0 "
            "  AND date(exit_date) < date('now', '-' || ? || ' days')",
            (_WATCH_WINDOW_DAYS,),
        ).fetchone()
        counts["aged_out"] = int(aged[0]) if aged else 0
    except Exception:
        pass

    c.close()
    return counts


if __name__ == "__main__":
    # Standalone smoke: run a single scan + print the counts dict.
    out = run_daily_scan()
    print(f"[HM-POST-EXIT-TRACKER] scan complete: {out}")
