#!/usr/bin/env python3
"""hm_movers_ticker_type_schema_backfill.py — HM-MOVERS-TICKER-TYPE-SCHEMA+BACKFILL.

Captain-authorized 2026-05-20 Wave 3. Closes Tuesday's HALT:
``mover_watchlist.ticker_type`` column did not exist.

Adds ``ticker_type TEXT`` to ``mover_watchlist`` and backfills it from
``scan_universe.ticker_type`` where the symbol matches. Symbols not in
``scan_universe`` (warrants, OTC, fringe IPOs) keep ticker_type=NULL by
design — that's the natural shape of the data.

Idempotency
-----------
The script checks ``PRAGMA table_info(mover_watchlist)`` before ALTER and
skips the ALTER if the column already exists. The UPDATE is naturally
idempotent (same source → same destination). Safe to re-run.

CLI
---
Default mode is ``--dry-run`` (no writes; prints what would happen).
Use ``--apply`` to commit. Use ``--db PATH`` to target a non-default DB
(useful for tests).

Invoke via:
    venv/bin/python3 scripts/hm_movers_ticker_type_schema_backfill.py --apply
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = PROJECT_ROOT / "data" / "trader.db"


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    """Return True if ``column`` exists in ``table``."""
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r[1] == column for r in rows)


def _verification_stats(conn: sqlite3.Connection) -> dict[str, Any]:
    """Capture post-migration counts + distribution for Captain review."""
    total = conn.execute("SELECT COUNT(*) FROM mover_watchlist").fetchone()[0]
    null_count = conn.execute(
        "SELECT COUNT(*) FROM mover_watchlist WHERE ticker_type IS NULL"
    ).fetchone()[0]
    not_null = total - null_count
    distribution = conn.execute(
        "SELECT COALESCE(ticker_type, '(null)') AS ticker_type, COUNT(*) AS rows "
        "FROM mover_watchlist GROUP BY ticker_type ORDER BY rows DESC"
    ).fetchall()
    return {
        "total_rows": total,
        "still_null": null_count,
        "not_null": not_null,
        "distribution": distribution,
    }


def migrate(db_path: Path, apply: bool) -> dict[str, Any]:
    """Run the migration. Returns verification stats."""
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    try:
        already_has_column = _column_exists(conn, "mover_watchlist", "ticker_type")

        if not apply:
            print(f"[DRY RUN] DB: {db_path}")
            print(
                f"[DRY RUN] ticker_type column present? {already_has_column}"
            )
            if not already_has_column:
                print("[DRY RUN] Would ALTER TABLE mover_watchlist ADD COLUMN ticker_type TEXT")
            print(
                "[DRY RUN] Would UPDATE mover_watchlist.ticker_type from scan_universe "
                "(WHERE symbol matches)"
            )
            return {"dry_run": True, "already_has_column": already_has_column}

        # Apply mode — wrap in transaction.
        conn.execute("BEGIN TRANSACTION")
        try:
            if not already_has_column:
                conn.execute(
                    "ALTER TABLE mover_watchlist ADD COLUMN ticker_type TEXT"
                )
                print("[APPLY] ALTER TABLE ... ADD COLUMN ticker_type TEXT — done")
            else:
                print("[APPLY] ticker_type column already present — skipping ALTER (idempotent)")

            cursor = conn.execute(
                """
                UPDATE mover_watchlist
                   SET ticker_type = (
                     SELECT ticker_type FROM scan_universe
                     WHERE scan_universe.symbol = mover_watchlist.symbol
                   )
                 WHERE EXISTS (
                   SELECT 1 FROM scan_universe
                   WHERE scan_universe.symbol = mover_watchlist.symbol
                 )
                """
            )
            print(f"[APPLY] UPDATE — {cursor.rowcount} rows backfilled")

            stats = _verification_stats(conn)
            conn.commit()
            return stats
        except Exception:
            conn.rollback()
            raise
    finally:
        conn.close()


def _print_stats(stats: dict[str, Any]) -> None:
    if stats.get("dry_run"):
        return
    print()
    print("── Verification stats ──")
    print(f"  total_rows : {stats['total_rows']}")
    print(f"  still_null : {stats['still_null']}")
    print(f"  not_null   : {stats['not_null']}")
    print()
    print("  Distribution:")
    for row in stats["distribution"]:
        print(f"    {row[0]:<10}  {row[1]:>5}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Commit changes to the DB (default is dry-run).",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help=f"Path to DB (default: {DEFAULT_DB})",
    )
    args = parser.parse_args(argv)

    stats = migrate(args.db, apply=args.apply)
    _print_stats(stats)
    return 0


if __name__ == "__main__":
    sys.exit(main())
