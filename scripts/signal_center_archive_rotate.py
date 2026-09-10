#!/usr/bin/env python3
"""Signal Center retention — archive signal_history/intelligence_feed rows
older than the rolling window into signal-center/signals_archive.db, never
removing a row from the live DB without a verified archive copy first.

Finishes the 2026-04-26 scaffold (SUNDAY_DRYDOCK item E1, HM-GEX-unrelated):
signals_archive.db + a 30-day rolling policy (archive_metadata table) were
designed and built then, but the actual mover job was never written —
signal_history grew unbounded for the following 4+ months, to 2.19 GB /
207K rows by 2026-09-10. This script is that job.

Consumer-safety check (2026-09-10, see relay doc for detail): the Signal
Center UI's history view tops out at "Last 30 days"
(signal-center/index.html #hist-days), /api/signals/history and
/api/export default to 7/30 days respectively, and every intelligence_feed
read is ORDER BY created_at DESC LIMIT n (most-recent-N, never a date
range). The one unbounded signal_history query found
(engine/super_backtest_v2.py::analyze_signal_center) has zero live
callers — only engine/_archive/2026-04-26/super_backtest_v3.py and v3b.py,
both retired. 30 days is safe for every live consumer today.

RULE #1 discipline, per batch, inside ONE transaction:
  1. INSERT the batch into signals_archive.db (attached to this same
     connection as `arc`).
  2. Verify: identical row count AND identical content, id-for-id, between
     what was selected from the live table and what's now readable back
     out of the archive.
  3. Only then DELETE that batch from the live table.
  4. If verification fails at any point, the whole transaction (insert
     included) rolls back untouched and the script aborts — never
     delete-then-check, never a bare DELETE.

signals_archive.db gets the same trg_rule1_no_delete_* trigger pattern as
setup_db.py's RULE1_TABLES (its own rows are never deleted either).
Adding it to scripts/offhost_backup.sh's replication set is a separate,
already-applied change — not done by this script.

Before the very first --apply run ever (no archive_metadata
'backup_taken_at' key yet), takes a consistent sqlite backup-API snapshot
of the live DB and gzips it to
signal-center/signals.db.pre-retention-backfill-<timestamp>.gz before
touching anything.

Dry-run by default — reports what WOULD move, zero writes. Pass --apply
to actually run. Pass --vacuum to VACUUM the live DB after a run (skip on
routine weekly runs — the file doesn't shrink without it; only worth the
time+lock cost right after a big backfill).

Best run in a low-traffic window — it holds its own write transactions
against signals.db while the :9000 service may also be writing; WAL mode
+ a 30s busy_timeout means contention retries rather than errors, but it
isn't instant.
"""
from __future__ import annotations

import gzip
import shutil
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path.home() / "autonomous-trader" / "signal-center"
LIVE_DB = ROOT / "signals.db"
ARCHIVE_DB = ROOT / "signals_archive.db"

WINDOW_DAYS = 30
BATCH_SIZE = 5000

# table -> timestamp column used for the rolling cutoff
TABLES = {
    "signal_history": "timestamp",
    "intelligence_feed": "created_at",
}


def _cutoff_for(ts_col: str) -> str:
    cutoff_dt = datetime.now() - timedelta(days=WINDOW_DAYS)
    if ts_col == "created_at":
        # intelligence_feed's created_at is CURRENT_TIMESTAMP-style:
        # "YYYY-MM-DD HH:MM:SS", no 'T', no microseconds.
        return cutoff_dt.strftime("%Y-%m-%d %H:%M:%S")
    # signal_history's timestamp is Python isoformat(): "YYYY-MM-DDTHH:MM:SS.ffffff"
    return cutoff_dt.isoformat()


def _ensure_archive_triggers(conn: sqlite3.Connection) -> None:
    """Idempotent — same trg_rule1_no_delete_* pattern as setup_db.py's
    RULE1_TABLES, but created explicitly in the `arc` schema (schema-
    qualifying the trigger name is what makes SQLite create it against
    arc.<table> instead of main.<table>, since both databases have a
    same-named table attached to this connection)."""
    for table in TABLES:
        conn.execute(
            f"CREATE TRIGGER IF NOT EXISTS arc.trg_rule1_no_delete_{table} "
            f"BEFORE DELETE ON {table} BEGIN "
            f"SELECT RAISE(ABORT, 'RULE #1: {table} rows in signals_archive.db "
            f"are never deleted -- this is the cold copy'); "
            f"END"
        )
    conn.commit()


def _backup_live_db_if_first_run(conn: sqlite3.Connection) -> None:
    row = conn.execute(
        "SELECT value FROM arc.archive_metadata WHERE key='backup_taken_at'"
    ).fetchone()
    if row:
        print(f"[backup] already taken at {row[0]}, skipping")
        return

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = ROOT / f"signals.db.pre-retention-backfill-{ts}.gz"
    tmp_path = ROOT / f".signals_backup_tmp_{ts}.db"
    print(f"[backup] first-ever --apply run: snapshotting live db to {backup_path}")

    src = sqlite3.connect(LIVE_DB)
    dst = sqlite3.connect(tmp_path)
    with dst:
        src.backup(dst)
    src.close()
    dst.close()

    with open(tmp_path, "rb") as f_in, gzip.open(backup_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    tmp_path.unlink()

    conn.execute(
        "INSERT INTO arc.archive_metadata (key, value) VALUES ('backup_taken_at', ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (datetime.now().isoformat(),),
    )
    conn.commit()
    print(f"[backup] done: {backup_path} ({backup_path.stat().st_size / 1e6:.1f} MB)")


def _rotate_table(conn: sqlite3.Connection, table: str, ts_col: str, apply_mode: bool) -> int:
    cutoff = _cutoff_for(ts_col)

    if not apply_mode:
        n = conn.execute(
            f"SELECT COUNT(*) FROM main.{table} WHERE {ts_col} < ?", (cutoff,)
        ).fetchone()[0]
        return n

    cols = [r[1] for r in conn.execute(f"PRAGMA main.table_info({table})").fetchall()]
    col_list = ", ".join(cols)
    placeholders = ", ".join("?" for _ in cols)

    total = 0
    while True:
        batch = conn.execute(
            f"SELECT {col_list} FROM main.{table} WHERE {ts_col} < ? "
            f"ORDER BY id LIMIT ?",
            (cutoff, BATCH_SIZE),
        ).fetchall()
        if not batch:
            break

        ids = [r["id"] for r in batch]
        id_list = ",".join(str(i) for i in ids)

        conn.execute("BEGIN")
        try:
            conn.executemany(
                f"INSERT OR IGNORE INTO arc.{table} ({col_list}) VALUES ({placeholders})",
                [tuple(r) for r in batch],
            )

            archived = conn.execute(
                f"SELECT {col_list} FROM arc.{table} WHERE id IN ({id_list}) ORDER BY id"
            ).fetchall()
            source_sorted = sorted(batch, key=lambda r: r["id"])
            archived_sorted = sorted(archived, key=lambda r: r["id"])

            if len(archived_sorted) != len(source_sorted):
                raise RuntimeError(
                    f"{table}: verify FAILED — selected {len(source_sorted)} rows, "
                    f"found {len(archived_sorted)} readable in archive for the same "
                    f"id batch (first id={ids[0]}, last id={ids[-1]})"
                )
            for s, a in zip(source_sorted, archived_sorted):
                if tuple(s) != tuple(a):
                    raise RuntimeError(
                        f"{table}: verify FAILED — content mismatch on id={s['id']}"
                    )

            conn.execute(f"DELETE FROM main.{table} WHERE id IN ({id_list})")
            conn.commit()
        except Exception:
            conn.rollback()
            print(f"[ABORT] {table}: batch verify/move failed, transaction rolled "
                  f"back untouched. Stopping — no further batches attempted.")
            raise

        total += len(ids)
        print(f"[{table}] verified + moved batch of {len(ids)} (total so far: {total})")

    return total


def main() -> None:
    apply_mode = "--apply" in sys.argv
    do_vacuum = "--vacuum" in sys.argv

    if not LIVE_DB.exists():
        print(f"ERROR: {LIVE_DB} not found")
        sys.exit(1)
    if not ARCHIVE_DB.exists():
        print(f"ERROR: {ARCHIVE_DB} not found — expected the 2026-04-26 scaffold")
        sys.exit(1)

    size_before = LIVE_DB.stat().st_size

    conn = sqlite3.connect(LIVE_DB, timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.row_factory = sqlite3.Row
    conn.execute(f"ATTACH DATABASE '{ARCHIVE_DB}' AS arc")

    print(f"[info] window={WINDOW_DAYS}d, mode={'APPLY' if apply_mode else 'DRY-RUN'}")
    print(f"[info] live db size before: {size_before / 1e6:.1f} MB")

    if apply_mode:
        _ensure_archive_triggers(conn)
        _backup_live_db_if_first_run(conn)

    moved: dict[str, int] = {}
    for table, ts_col in TABLES.items():
        moved[table] = _rotate_table(conn, table, ts_col, apply_mode)

    integrity = None
    if apply_mode:
        integrity = conn.execute("PRAGMA arc.integrity_check").fetchone()[0]
        print(f"[integrity] signals_archive.db PRAGMA integrity_check -> {integrity}")

        now_iso = datetime.now().isoformat()
        for key, value in (
            ("last_run", now_iso),
            ("cutoff", _cutoff_for("timestamp")),
            ("last_integrity_check", integrity),
        ):
            conn.execute(
                "INSERT INTO arc.archive_metadata (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
        conn.commit()

    if apply_mode and do_vacuum:
        conn.execute("DETACH DATABASE arc")
        print("[vacuum] running VACUUM on live signals.db (briefly locks) ...")
        conn.execute("VACUUM")
        print("[vacuum] done")

    conn.close()

    size_after = LIVE_DB.stat().st_size

    print("=== summary ===")
    for table, n in moved.items():
        verb = "moved" if apply_mode else "would move"
        print(f"  {table}: {n} rows {verb}")
    print(f"  live db size: {size_before / 1e6:.1f} MB -> {size_after / 1e6:.1f} MB"
          + ("" if apply_mode and do_vacuum else
             " (unchanged until a --vacuum run reclaims freed pages)"
             if apply_mode else ""))
    if integrity is not None:
        print(f"  archive integrity_check: {integrity}")
    if not apply_mode:
        print("[dry-run] no writes made. Re-run with --apply to execute.")


if __name__ == "__main__":
    main()
