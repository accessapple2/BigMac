"""HM-EVENTS-BUS-CONSUMER — pending signals_v2 → paper_trader.buy() dispatch.

Reads signals_v2 rows in status='pending', applies staleness check, runs a
fresh-SELECT dedup guard, then dispatches to engine.paper_trader.buy(). On
success marks signals_v2.status='executed'; on rejection (buy() returns None)
marks status='failed'.

The consumer is invoked from main.py::run_events_bus_consumer() on a 1-min
schedule, gated to NYSE regular hours by the wrapper. The entire function
is wrapped in a top-level try/except so a malformed row or DB hiccup never
takes down the scheduler tick (HM-Z/HM-AA error posture).

Known cross-table caveats (banked, not blocking):
  - signals_v2.id != signals.rowid. buy()'s internal stale-signal gate
    (paper_trader.py:678) reads signals(v1) so passing signals_v2.id makes
    that gate fail-safe-open. The consumer's own stale check upstream covers
    this case.
  - HM-EVENTS-BUS-CONSUMER-TRADE-ID 2026-05-26: buy() now returns trade_id
    in its success dict; this consumer wires it into signals_v2.trade_id so
    the FK chain signals_v2 → trades is hydrated at executed-mark time. (The
    earlier note about trade_id staying NULL is obsolete.)
"""
from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

from rich.console import Console

console = Console()

_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "trader.db"


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _parse_iso_utc(s: str | None) -> datetime | None:
    """Best-effort ISO-8601 parse. Returns None on any failure or NULL input.

    Accepts trailing 'Z' (treated as UTC) and microsecond precision both ways.
    """
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def consume_pending_signals(max_batch: int = 10) -> dict:
    """Dispatch up to `max_batch` pending signals_v2 rows to paper_trader.buy().

    Returns a small stats dict the scheduler can log; on any unhandled error
    returns {"error": "..."} without raising.
    """
    stats = {"scanned": 0, "stale": 0, "skipped_dedup": 0,
             "skipped_no_price": 0, "executed": 0, "failed": 0}
    try:
        conn = _conn()
        rows = conn.execute(
            "SELECT id, source, symbol, confidence, timeframe, stale_after "
            "FROM signals_v2 WHERE status='pending' "
            "ORDER BY created_at ASC LIMIT ?",
            (int(max_batch),),
        ).fetchall()
        conn.close()
        stats["scanned"] = len(rows)
        if not rows:
            return stats

        # Lazy imports — keep module import-time cheap and avoid circulars.
        from engine.events_bus import mark_signal_executed
        from engine.paper_trader import buy
        from engine.chekov_autotrade import _get_current_price

        now = datetime.utcnow()
        for r in rows:
            sig_id = int(r["id"])
            symbol = r["symbol"]
            source = r["source"]

            # (a) Staleness check. NULL → never-stale.
            stale_dt = _parse_iso_utc(r["stale_after"])
            if stale_dt is not None and now > stale_dt:
                _mark(sig_id, "stale")
                console.log(
                    f"[dim][EVENTS-BUS-CONSUMER] sig={sig_id} {source} {symbol} "
                    f"marked stale (stale_after={r['stale_after']})"
                )
                stats["stale"] += 1
                continue

            # (b) Dedup guard — re-SELECT the row's current status.
            cur_status = _refetch_status(sig_id)
            if cur_status != "pending":
                stats["skipped_dedup"] += 1
                continue

            # (c) Resolve a price. buy() requires a positional price; if the
            # quote service can't return one, treat as a soft skip — leave
            # the row 'pending' so the next tick can retry.
            price = _get_current_price(symbol)
            if not price:
                stats["skipped_no_price"] += 1
                console.log(
                    f"[yellow][EVENTS-BUS-CONSUMER] sig={sig_id} {source} {symbol} "
                    "skipped — no price available, retry next tick"
                )
                continue

            # (d) Dispatch.
            console.log(
                f"[cyan][EVENTS-BUS-CONSUMER] dispatch sig={sig_id} "
                f"{source} BUY {symbol} @ ${price:.2f} conf={r['confidence'] or 0.0:.2f}"
            )
            result = buy(
                player_id=source, symbol=symbol, price=price,
                confidence=r["confidence"] or 0.0,
                timeframe=r["timeframe"] or "swing",
                signal_id=sig_id,
                reasoning="[EVENTS-BUS-CONSUMER] dispatched from signals_v2",
            )

            if result:
                # HM-EVENTS-BUS-CONSUMER-TRADE-ID 2026-05-26: buy() now returns
                # trade_id (= trades.rowid). Wire it into signals_v2.trade_id
                # so the FK chain signals_v2 → trades is hydrated immediately.
                mark_signal_executed(signal_id=sig_id, trade_id=result.get("trade_id"))
                stats["executed"] += 1
                console.log(
                    f"[green][EVENTS-BUS-CONSUMER] sig={sig_id} {source} "
                    f"{symbol} EXECUTED trade_id={result.get('trade_id')}"
                )
            else:
                _mark(sig_id, "failed")
                stats["failed"] += 1
                console.log(
                    f"[yellow][EVENTS-BUS-CONSUMER] sig={sig_id} {source} "
                    f"{symbol} FAILED — buy() returned None (gated)"
                )
        return stats
    except Exception as e:
        console.log(
            f"[red][EVENTS-BUS-CONSUMER] top-level error: "
            f"{type(e).__name__}: {e!r}"
        )
        return {"error": f"{type(e).__name__}: {e!r}", **stats}


def _refetch_status(signal_id: int) -> str | None:
    try:
        conn = _conn()
        row = conn.execute(
            "SELECT status FROM signals_v2 WHERE id=?", (int(signal_id),)
        ).fetchone()
        conn.close()
        return row["status"] if row else None
    except Exception:
        return None


def _mark(signal_id: int, new_status: str) -> bool:
    """UPDATE signals_v2.status; mark_signal_executed handles the 'executed'
    case so this only owns 'stale' / 'failed' transitions."""
    try:
        conn = _conn()
        try:
            conn.execute(
                "UPDATE signals_v2 SET status=? WHERE id=?",
                (new_status, int(signal_id)),
            )
            conn.commit()
            return True
        finally:
            conn.close()
    except Exception as e:
        console.log(
            f"[yellow][EVENTS-BUS-CONSUMER] _mark sig={signal_id} "
            f"status={new_status}: {type(e).__name__}: {e!r}"
        )
        return False
