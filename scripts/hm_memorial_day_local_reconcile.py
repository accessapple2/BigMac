"""HM-MEMORIAL-DAY-LOCAL-RECONCILE Stage 2 — archive 3 fictional positions
+ 8 cancelled-order trade rows per docs/DOCTRINE.md Rule #2 (archive-then-delete).

Single archive_session_id (UUID) for the whole rollback batch.

Targets:
  positions: 1300 (neo-matrix QQQ), 1301 (neo-matrix AMD), 1302 (neo-matrix META)
  trades:    2547 (LNTH SELL), 2549/2550/2552 (QQQ BUY), 2551 (AMD BUY),
             2553 (META BUY), 2554 (ZM SELL), 2555 (SYM SELL)

Atomicity: archive INSERTs + source DELETEs in a single sqlite3
transaction. Consistency check (archived_count == deleted_count) fires
rollback on mismatch — Doctrine Rule #2.

SELL position restoration: NOT needed. ollie-auto has no local positions
for LNTH/ZM/SYM (those live on alpaca-mirror, the broker-mirror player).
Alpaca broker state confirmed intact (LNTH 1.19, ZM 1.18, SYM 2.25);
local alpaca-mirror rows match. The SELLs cancelled before fill; nothing
was pre-decremented. Verified pre-flight.

Idempotent at the schema level (CREATE TABLE IF NOT EXISTS); not idempotent
on the data side — re-running with same row IDs is a no-op for already-
deleted rows (rowcount=0) and triggers consistency-check rollback.
"""
from __future__ import annotations

import sqlite3
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path


DB_PATH = str(Path(__file__).resolve().parent.parent / "data" / "trader.db")

POSITION_IDS = [1300, 1301, 1302]
TRADE_IDS = [2547, 2549, 2550, 2551, 2552, 2553, 2554, 2555]

ARCHIVE_REASON = "memorial_day_holiday_violation_rollback"
ARCHIVED_BY = "memorial_day_emergency_rollback"


def _ensure_archive_tables(conn: sqlite3.Connection) -> None:
    """Create positions_archived + trades_archived if not present. Mirrors
    source schemas + audit-trail columns per docs/DOCTRINE.md Rule #2."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS positions_archived (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            original_row_id     INTEGER NOT NULL,
            player_id           TEXT NOT NULL,
            symbol              TEXT NOT NULL,
            qty                 REAL,
            avg_price           REAL,
            asset_type          TEXT,
            option_type         TEXT,
            strike_price        REAL,
            expiry_date         TEXT,
            opened_at           TIMESTAMP,
            high_watermark      REAL,
            conviction          REAL,
            conviction_source   TEXT,
            archived_at         TEXT NOT NULL DEFAULT (datetime('now')),
            archived_by         TEXT NOT NULL,
            archive_reason      TEXT NOT NULL,
            archive_session_id  TEXT NOT NULL,
            restored_at         TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_positions_archived_session
            ON positions_archived(archive_session_id);

        CREATE TABLE IF NOT EXISTS trades_archived (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            original_row_id     INTEGER NOT NULL,
            player_id           TEXT NOT NULL,
            symbol              TEXT NOT NULL,
            action              TEXT,
            qty                 REAL,
            price               REAL,
            asset_type          TEXT,
            option_type         TEXT,
            strike_price        REAL,
            expiry_date         TEXT,
            reasoning           TEXT,
            confidence          REAL,
            executed_at         TIMESTAMP,
            exit_price          REAL,
            realized_pnl        REAL,
            entry_price         REAL,
            season              INTEGER,
            corrected_pnl       REAL,
            sources             TEXT,
            timeframe           TEXT,
            alpaca_order_id     TEXT,
            alpaca_status       TEXT,
            execution_type      TEXT,
            spread_data         TEXT,
            strategy_id         TEXT,
            signal_id           INTEGER,
            stop_loss_order_id  TEXT,
            take_profit_order_id TEXT,
            prompt_version      TEXT,
            archived_at         TEXT NOT NULL DEFAULT (datetime('now')),
            archived_by         TEXT NOT NULL,
            archive_reason      TEXT NOT NULL,
            archive_session_id  TEXT NOT NULL,
            restored_at         TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_trades_archived_session
            ON trades_archived(archive_session_id);
        """
    )


def main() -> int:
    session_id = str(uuid.uuid4())
    print(f"[session_id] {session_id}")
    print(f"[targets] positions={POSITION_IDS}  trades={TRADE_IDS}")

    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        _ensure_archive_tables(conn)
        # Begin explicit transaction.
        conn.execute("BEGIN")

        # ── Archive positions ────────────────────────────────────────────
        pos_archived = 0
        pos_deleted = 0
        for pid in POSITION_IDS:
            row = conn.execute(
                "SELECT id, player_id, symbol, qty, avg_price, asset_type, "
                "option_type, strike_price, expiry_date, opened_at, "
                "high_watermark, conviction, conviction_source "
                "FROM positions WHERE id = ?",
                (pid,),
            ).fetchone()
            if row is None:
                print(f"  [WARN] positions row id={pid} not found — skipping")
                continue
            conn.execute(
                """
                INSERT INTO positions_archived
                  (original_row_id, player_id, symbol, qty, avg_price,
                   asset_type, option_type, strike_price, expiry_date,
                   opened_at, high_watermark, conviction, conviction_source,
                   archived_by, archive_reason, archive_session_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["id"], row["player_id"], row["symbol"], row["qty"],
                    row["avg_price"], row["asset_type"], row["option_type"],
                    row["strike_price"], row["expiry_date"], row["opened_at"],
                    row["high_watermark"], row["conviction"],
                    row["conviction_source"],
                    ARCHIVED_BY, ARCHIVE_REASON, session_id,
                ),
            )
            pos_archived += 1
            d = conn.execute("DELETE FROM positions WHERE id = ?", (pid,))
            pos_deleted += d.rowcount

        print(f"[positions] archived={pos_archived}  deleted={pos_deleted}")
        if pos_archived != pos_deleted:
            conn.rollback()
            print(
                f"*** ROLLBACK: positions archived/deleted mismatch "
                f"({pos_archived} vs {pos_deleted}) ***"
            )
            return 1

        # ── Archive trades ───────────────────────────────────────────────
        tr_archived = 0
        tr_deleted = 0
        for tid in TRADE_IDS:
            row = conn.execute(
                "SELECT * FROM trades WHERE id = ?", (tid,),
            ).fetchone()
            if row is None:
                print(f"  [WARN] trades row id={tid} not found — skipping")
                continue
            conn.execute(
                """
                INSERT INTO trades_archived
                  (original_row_id, player_id, symbol, action, qty, price,
                   asset_type, option_type, strike_price, expiry_date,
                   reasoning, confidence, executed_at, exit_price,
                   realized_pnl, entry_price, season, corrected_pnl,
                   sources, timeframe, alpaca_order_id, alpaca_status,
                   execution_type, spread_data, strategy_id, signal_id,
                   stop_loss_order_id, take_profit_order_id, prompt_version,
                   archived_by, archive_reason, archive_session_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["id"], row["player_id"], row["symbol"], row["action"],
                    row["qty"], row["price"], row["asset_type"],
                    row["option_type"], row["strike_price"], row["expiry_date"],
                    row["reasoning"], row["confidence"], row["executed_at"],
                    row["exit_price"], row["realized_pnl"], row["entry_price"],
                    row["season"], row["corrected_pnl"], row["sources"],
                    row["timeframe"], row["alpaca_order_id"], row["alpaca_status"],
                    row["execution_type"], row["spread_data"], row["strategy_id"],
                    row["signal_id"], row["stop_loss_order_id"],
                    row["take_profit_order_id"], row["prompt_version"],
                    ARCHIVED_BY, ARCHIVE_REASON, session_id,
                ),
            )
            tr_archived += 1
            d = conn.execute("DELETE FROM trades WHERE id = ?", (tid,))
            tr_deleted += d.rowcount

        print(f"[trades]    archived={tr_archived}  deleted={tr_deleted}")
        if tr_archived != tr_deleted:
            conn.rollback()
            print(
                f"*** ROLLBACK: trades archived/deleted mismatch "
                f"({tr_archived} vs {tr_deleted}) ***"
            )
            return 1

        conn.commit()
        print(f"\n*** STAGE 2 COMMIT — session_id {session_id} ***")
        print(f"  positions: {pos_archived} archived + deleted")
        print(f"  trades:    {tr_archived} archived + deleted")
        return 0
    except Exception as e:
        conn.rollback()
        print(f"*** EXCEPTION ROLLBACK: {type(e).__name__}: {e!r} ***")
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
