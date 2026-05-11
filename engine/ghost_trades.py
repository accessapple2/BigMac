"""Ghost Trades — track HOLD decisions with >60% confidence as phantom trades.

HM-AZ (2026-05-11) — Query rewrite to align with the empirically canonical
schema in data/trader.db.ghost_trades. Prior version queried columns
(player_id, created_at, outcome_price, outcome_pnl_pct) that exist in NEITHER
candidate schema, producing 16+ stack traces in trader_error.log and silently
breaking the dashboard's ghost-stats panel.

Canonical schema (data/trader.db.ghost_trades, active writer scripts/ghost_advisor.py):
    id, ts, symbol, side, qty, price, fill_price, venue, advisor, signal_id,
    status, rationale

Outward dict-key compatibility preserved via SQL aliases so dashboard/app.py
and engine/scan_context.py consumers don't need code changes:
    ts        AS created_at
    advisor   AS player_id
    price     AS entry_price
    rationale AS reasoning

The lean schema in data/ghost_trades.db (writer: engine/ghost_trader.py, last
fire 2026-04-28) is no longer queried here. That file was renamed to
data/ghost_trades.db.legacy_lean_2026-05-11 as part of HM-AZ.2.

Out-of-scope for HM-AZ (deferred to a future ticket):
    - Outcome tracking: trader.db has no exit_price / pnl_pct columns. The
      get_ghost_stats() summary returns zeros for would_have_won/lost/avg_pnl.
      update_ghost_outcomes() becomes a logged no-op.
    - JOIN to ai_players uses LEFT JOIN since the advisor column contains
      strategy labels ('ollie_super_trades', 'trailing_stop', etc.) that
      don't always match ai_players.id. COALESCE picks display_name when
      present, falls back to advisor string.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime

from rich.console import Console

console = Console()
DB = "data/trader.db"


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def log_ghost_trade(player_id: str, symbol: str, confidence: float,
                    reasoning: str, price: float):
    """Log a ghost trade — a HOLD decision that had >60% confidence.

    Inserts a row into trader.db.ghost_trades using the canonical column
    layout.

    HM-BB.3 (2026-05-11): populate the new ``entry_price`` and ``confidence``
    columns directly. The legacy ``conf=…`` rationale prefix is retained for
    one release cycle (Q4) so the 16 pre-migration rows remain readable in
    the same shape as new rows during the transition.
    """
    if confidence < 0.60:
        return
    conn = _conn()
    # === HM-BB.3 ===
    conn.execute(
        "INSERT INTO ghost_trades "
        "(ts, symbol, side, qty, price, fill_price, venue, advisor, status, rationale, "
        " entry_price, confidence, exit_price, pnl_pct) "
        "VALUES (?, ?, 'BUY', 0, ?, ?, 'virtual', ?, 'ghost', ?, ?, ?, NULL, NULL)",
        (
            datetime.utcnow().isoformat() + "+00:00",
            symbol,
            price,
            price,
            player_id,
            f"conf={confidence:.2f}: {reasoning}",
            price,
            float(confidence),
        ),
    )
    # === /HM-BB.3 ===
    conn.commit()
    conn.close()


_OUTCOME_NOOP_WARNED = False


def update_ghost_outcomes(prices: dict):
    """No-op stub.

    HM-AZ note: trader.db.ghost_trades has no exit_price / pnl_pct columns,
    so outcomes can't be tracked under the current schema. A future ticket
    can add an outcome-enrichment path (separate table or schema migration).
    This stub keeps callers happy and logs a warning once per process so
    the silence is visible.
    """
    global _OUTCOME_NOOP_WARNED
    if not _OUTCOME_NOOP_WARNED:
        console.log(
            "[yellow]update_ghost_outcomes: no-op under trader.db schema "
            "(HM-AZ 2026-05-11) — outcome tracking deferred"
        )
        _OUTCOME_NOOP_WARNED = True


def get_ghost_trades(player_id: str = None, limit: int = 50) -> list:
    """Get ghost trades, optionally filtered by player.

    Returns list of dicts. Each dict has the same outward keys the old
    schema-mismatched query implied (player_id, created_at, entry_price,
    reasoning, display_name) via SQL aliases — no consumer-side changes
    needed in dashboard/app.py or engine/scan_context.py.

    HM-BB.4 (2026-05-11): surface new outcome columns. ``entry_price`` now
    COALESCEs the real ``g.entry_price`` (populated for rows written after
    HM-BB.3) with the legacy ``g.price`` alias so the 16 pre-migration rows
    keep their entry-price-shaped value; new rows use the explicit column.
    """
    conn = _conn()
    # === HM-BB.4 ===
    base_select = (
        "SELECT "
        "  g.id, g.symbol, g.side, g.qty, "
        "  g.ts AS created_at, "
        "  g.advisor AS player_id, "
        "  COALESCE(g.entry_price, g.price) AS entry_price, "
        "  g.fill_price, g.venue, g.status, g.signal_id, "
        "  g.rationale AS reasoning, "
        "  g.confidence, g.exit_price, g.pnl_pct, "
        "  COALESCE(p.display_name, g.advisor) AS display_name "
        "FROM ghost_trades g "
        "LEFT JOIN ai_players p ON g.advisor = p.id "
    )
    # === /HM-BB.4 ===
    if player_id:
        rows = conn.execute(
            base_select + "WHERE g.advisor = ? ORDER BY g.ts DESC LIMIT ?",
            (player_id, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            base_select + "ORDER BY g.ts DESC LIMIT ?",
            (limit,),
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_ghost_stats() -> dict:
    """Get aggregate ghost trade statistics.

    HM-BB.4 (2026-05-11): pnl_pct is now populated on SELL rows (see
    scripts/ghost_advisor.py::execute_sell). Outcome aggregates filter on
    ``pnl_pct IS NOT NULL`` so they capture only closed positions and remain
    correct as the 16 pre-migration rows (which have NULL pnl_pct) stay in
    the table. ``total_ghosts`` keeps the all-rows count for visibility.
    """
    conn = _conn()
    # === HM-BB.4 ===
    total_row = conn.execute(
        "SELECT COUNT(*) AS total FROM ghost_trades"
    ).fetchone()

    agg_row = conn.execute(
        "SELECT "
        "  SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) AS wins, "
        "  SUM(CASE WHEN pnl_pct < 0 THEN 1 ELSE 0 END) AS losses, "
        "  AVG(pnl_pct) AS avg_pnl, "
        "  MAX(pnl_pct) AS best, "
        "  MIN(pnl_pct) AS worst "
        "FROM ghost_trades WHERE pnl_pct IS NOT NULL"
    ).fetchone()

    top_rows = conn.execute(
        "SELECT symbol, advisor, pnl_pct "
        "FROM ghost_trades "
        "WHERE pnl_pct IS NOT NULL AND pnl_pct > 0 "
        "ORDER BY pnl_pct DESC LIMIT 5"
    ).fetchall()
    conn.close()

    def _f(x):
        return float(x) if x is not None else 0.0

    return {
        "total_ghosts": int(total_row["total"]) if total_row else 0,
        "would_have_won": int(agg_row["wins"] or 0) if agg_row else 0,
        "would_have_lost": int(agg_row["losses"] or 0) if agg_row else 0,
        "avg_pnl_pct": _f(agg_row["avg_pnl"]) if agg_row else 0.0,
        "best_ghost_pct": _f(agg_row["best"]) if agg_row else 0.0,
        "worst_ghost_pct": _f(agg_row["worst"]) if agg_row else 0.0,
        "top_missed": [
            {"symbol": r["symbol"], "advisor": r["advisor"], "pnl_pct": _f(r["pnl_pct"])}
            for r in top_rows
        ],
    }
    # === /HM-BB.4 ===
