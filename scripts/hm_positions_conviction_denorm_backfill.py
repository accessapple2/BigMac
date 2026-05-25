"""HM-POSITIONS-CONVICTION-DENORM Phase 2 — backfill positions.conviction
from the most-recent opening trade per (player_id, symbol[, option key]).

For each currently-open position, find the latest matching trade with a
non-NULL confidence value among the opening-action set and copy its
confidence into positions.conviction. Stamp conviction_source='backfill'.

Opening actions per asset_type:
  stock:  BUY, SHORT
  option: BUY_CALL, BUY_PUT, SELL-with-option-key  (long calls/puts AND
          sold-to-open writes like covered calls / short puts — Phase 2.5
          extension 2026-05-24 to capture navigator-style PLD short calls)

Skips positions whose matching key produces no opening trade or whose
matching trade has NULL confidence (legacy rows, edge cases, pre-confidence
schema era). Reports counts; if NULL-remaining > 30% of positions the
script prints a STOP marker so the orchestrator can honor mission guard
rail #7 ("If Phase 2 backfill leaves >30% of positions NULL, STOP").

Idempotent: re-running on a partially-backfilled DB updates additional
rows but does not overwrite live_buy entries (filtered by conviction_source
IS NULL or = 'backfill').
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = str(Path(__file__).resolve().parent.parent / "data" / "trader.db")

# Action vocabulary documented here; SQL uses inline literals to keep
# the (action IN (...) OR ...) compound condition readable for options.


def _fetch_latest_opening_confidence(
    conn: sqlite3.Connection, pos: sqlite3.Row
) -> float | None:
    """Return the most-recent opening-trade confidence for the position
    key, or None if no matching trade exists or matching trade's
    confidence is NULL."""
    if pos["asset_type"] == "stock":
        row = conn.execute(
            """
            SELECT confidence, executed_at FROM trades
             WHERE player_id = ?
               AND symbol = ?
               AND asset_type = 'stock'
               AND action IN ('BUY', 'SHORT')
               AND confidence IS NOT NULL
             ORDER BY executed_at DESC
             LIMIT 1
            """,
            (pos["player_id"], pos["symbol"]),
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT confidence, executed_at FROM trades
             WHERE player_id = ?
               AND symbol = ?
               AND option_type IS ?
               AND strike_price IS ?
               AND expiry_date IS ?
               AND (action IN ('BUY_CALL', 'BUY_PUT')
                    OR (action = 'SELL' AND option_type IS NOT NULL))
               AND confidence IS NOT NULL
             ORDER BY executed_at DESC
             LIMIT 1
            """,
            (
                pos["player_id"],
                pos["symbol"],
                pos["option_type"],
                pos["strike_price"],
                pos["expiry_date"],
            ),
        ).fetchone()

    return row["confidence"] if row else None


def main() -> int:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    positions = conn.execute(
        """
        SELECT id, player_id, symbol, asset_type, option_type,
               strike_price, expiry_date, opened_at, conviction_source
          FROM positions
        """
    ).fetchall()

    total = len(positions)
    backfilled = 0
    skipped_live_buy = 0
    null_remaining: list[dict] = []

    for pos in positions:
        if pos["conviction_source"] == "live_buy":
            skipped_live_buy += 1
            continue

        confidence = _fetch_latest_opening_confidence(conn, pos)
        if confidence is None:
            null_remaining.append(
                {
                    "id": pos["id"],
                    "player_id": pos["player_id"],
                    "symbol": pos["symbol"],
                    "asset_type": pos["asset_type"],
                    "opened_at": pos["opened_at"],
                }
            )
            continue

        conn.execute(
            "UPDATE positions SET conviction=?, conviction_source='backfill' "
            "WHERE id=?",
            (confidence, pos["id"]),
        )
        backfilled += 1

    conn.commit()
    conn.close()

    null_count = len(null_remaining)
    backfill_pct = (backfilled / total * 100.0) if total else 0.0
    null_pct = (null_count / total * 100.0) if total else 0.0

    print(f"Total positions:        {total}")
    print(f"Backfilled:             {backfilled} ({backfill_pct:.1f}%)")
    print(f"Skipped (live_buy):     {skipped_live_buy}")
    print(f"NULL remaining:         {null_count} ({null_pct:.1f}%)")

    if null_remaining:
        sample = null_remaining[:20]
        print("\nSample NULL-remaining rows (up to 20):")
        for n in sample:
            print(
                f"  id={n['id']:>5}  {n['player_id']:<24} {n['symbol']:<10} "
                f"{n['asset_type']:<6} opened={n['opened_at']}"
            )

    if null_pct > 30.0:
        print(
            f"\n*** STOP: NULL-remaining ratio {null_pct:.1f}% > 30% "
            "threshold (mission guard rail #7). Trades→positions linkage "
            "weaker than expected. Admiral input required."
        )
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
