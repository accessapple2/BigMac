"""Migration 006 — HM-TRADES-ENTRY-BACKFILL (HM-NEXT-WAVE Phase 4) 2026-05-23.

Backfills `trades.entry_price` NULLs from before the
HM-MU-PRICE-WRITEBACK fix (2026-05-23 commit 6d03caf) which seeded
entry_price = price at INSERT. Older rows (Jan-May 2026) left
entry_price NULL, breaking PnL math, cockpit queries, and the
HM-TRADES-PRICE-WRITEBACK SELL-side computation.

STRATEGY
========
For every row WHERE entry_price IS NULL AND asset_type='stock':

  * BUY action: entry_price = price (the BUY's own executed price IS
    the entry — no ambiguity).

  * SELL action: lookup the most recent prior BUY by
    (player_id, symbol) WHERE executed_at < this SELL's executed_at.
    If found: entry_price = matching_buy.price. Backfill marks the
    SELL with the actual basis so realized_pnl can be recomputed
    correctly.
    If NOT found (orphan SELL — no matching BUY in our books):
    entry_price = price (the SELL's own price), which yields
    realized_pnl = 0. Captain spec: "set entry_price = exit_price
    (neutral, marks as unresolvable)". Since exit_price IS price for
    SELL semantics, this is the same outcome.

  * SHORT / COVER / other: entry_price = price (treat the action's
    own executed price as canonical — same logic as BUY).

GUARDRAILS HONORED
==================
* Backup data/trader.db BEFORE any UPDATE (cp to backups/).
* Per-row UPDATE inside a single transaction so partial failure
  rolls back (atomic).
* Log every backfill action to backups/entry_backfill_YYYYMMDD.log
  with: trade_id, player_id, symbol, action, OLD entry_price (always
  None), NEW entry_price, source (one of: 'price', 'matched_buy',
  'orphan_fallback').
* Idempotent: re-running is a no-op (filter is NULL entry_price).
* Verify post-run: SELECT COUNT(*) WHERE entry_price IS NULL AND
  asset_type='stock' → must be 0.

NOT DOING
=========
* Recomputing realized_pnl for the SELL rows we backfilled — that's
  a separate analytics-correction ticket. This migration only fills
  NULLs; the recomputation cascades naturally next time the row is
  joined or summarized.
* Touching options trades (asset_type != 'stock') — different
  lifecycle (legs, expiry, multi-action close) needs its own pass.
* Backfilling rows that already have entry_price set — out of scope.
"""
from __future__ import annotations

import shutil
import sqlite3
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DB_PATH = REPO / "data" / "trader.db"
BACKUPS = REPO / "backups"


def _today_iso() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _backup_db() -> Path:
    BACKUPS.mkdir(parents=True, exist_ok=True)
    dst = BACKUPS / f"trader.db.pre-entry-backfill-{_today_iso()}"
    shutil.copy2(DB_PATH, dst)
    return dst


def _matching_buy_price(conn: sqlite3.Connection, player_id: str,
                       symbol: str, executed_at: str) -> float | None:
    """Find the most-recent BUY by (player_id, symbol) WHERE
    executed_at < this SELL's timestamp. Returns its `price` column
    or None if no match.
    """
    try:
        row = conn.execute(
            "SELECT price FROM trades "
            " WHERE player_id=? AND symbol=? AND action LIKE 'BUY%' "
            "   AND executed_at < ? AND price IS NOT NULL "
            " ORDER BY executed_at DESC LIMIT 1",
            (player_id, symbol, executed_at),
        ).fetchone()
        return float(row[0]) if row and row[0] is not None else None
    except Exception:
        return None


def run() -> dict:
    """Execute the backfill. Returns summary dict."""
    if not DB_PATH.exists():
        return {"error": f"db not found: {DB_PATH}"}

    backup_path = _backup_db()
    log_path = BACKUPS / f"entry_backfill_{_today_iso()}.log"
    summary = {
        "backup": str(backup_path),
        "log": str(log_path),
        "total_to_fill": 0,
        "filled_buy_from_price": 0,
        "filled_sell_from_matched_buy": 0,
        "filled_sell_from_orphan_fallback": 0,
        "filled_other_from_price": 0,
        "errors": 0,
        "remaining_after": None,
    }

    log_lines: list[str] = []
    log_lines.append(
        f"# HM-TRADES-ENTRY-BACKFILL log @ {datetime.utcnow().isoformat()}Z"
    )
    log_lines.append(f"# backup={backup_path}")

    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT id, player_id, symbol, action, price, exit_price, "
            "       executed_at "
            "  FROM trades "
            " WHERE entry_price IS NULL AND asset_type='stock' "
            " ORDER BY executed_at"
        ).fetchall()
        summary["total_to_fill"] = len(rows)
        log_lines.append(f"# rows_to_fill={len(rows)}")

        conn.execute("BEGIN")
        for r in rows:
            tid = r["id"]
            pid = r["player_id"]
            sym = r["symbol"]
            action = (r["action"] or "").upper()
            price = r["price"]
            executed_at = r["executed_at"]
            new_entry = None
            source = None

            try:
                if price is None:
                    # No price either — cannot backfill. Skip + log.
                    log_lines.append(
                        f"SKIP id={tid} {pid} {sym} {action} "
                        f"reason=null_price"
                    )
                    summary["errors"] += 1
                    continue

                if action.startswith("BUY"):
                    new_entry = float(price)
                    source = "price"
                    summary["filled_buy_from_price"] += 1
                elif action == "SELL":
                    matched = _matching_buy_price(
                        conn, pid, sym, executed_at
                    )
                    if matched is not None:
                        new_entry = float(matched)
                        source = "matched_buy"
                        summary["filled_sell_from_matched_buy"] += 1
                    else:
                        new_entry = float(price)
                        source = "orphan_fallback"
                        summary["filled_sell_from_orphan_fallback"] += 1
                else:
                    # SHORT, COVER, other — use own price.
                    new_entry = float(price)
                    source = "price"
                    summary["filled_other_from_price"] += 1

                conn.execute(
                    "UPDATE trades SET entry_price=? WHERE id=?",
                    (new_entry, tid),
                )
                log_lines.append(
                    f"FILL id={tid} {pid} {sym} {action} "
                    f"old=None new={new_entry} source={source} "
                    f"executed_at={executed_at}"
                )
            except Exception as e:
                summary["errors"] += 1
                log_lines.append(
                    f"ERR  id={tid} {pid} {sym} {action}: "
                    f"{type(e).__name__}: {e!r}"
                )

        conn.commit()

        # Verify
        row = conn.execute(
            "SELECT COUNT(*) FROM trades "
            " WHERE entry_price IS NULL AND asset_type='stock'"
        ).fetchone()
        summary["remaining_after"] = int(row[0]) if row else None
    except Exception as e:
        conn.rollback()
        log_lines.append(
            f"# OUTER CRASH (rolled back): {type(e).__name__}: {e!r}"
        )
        summary["errors"] += 1
        summary["outer_crash"] = f"{type(e).__name__}: {e!r}"
    finally:
        conn.close()

    log_lines.append(f"# summary={summary}")
    log_path.write_text("\n".join(log_lines) + "\n")
    return summary


if __name__ == "__main__":
    import json
    print(json.dumps(run(), indent=2))
