#!/usr/bin/env python3
"""
import_schwab_snapshot.py - Log Schwab equity into ghost_equity_history
from a Schwab Positions CSV export, for ghost-vs-Schwab comparison.

Usage:
    python3 scripts/import_schwab_snapshot.py path/to/file.csv
    python3 scripts/import_schwab_snapshot.py              # scan data/schwab_imports/
    python3 scripts/import_schwab_snapshot.py --no-archive # don't move file after

Workflow:
    1. Export "Positions" from Schwab (the same kind of CSV used for genesis seed)
    2. Drop into data/schwab_imports/  (auto-created)
    3. Run this script; it parses the file, logs to ghost_equity_history,
       computes delta vs current ghost equity, and archives the file.

Tip: run scripts/ghost_advisor.py just before this so ghost equity is fresh.
"""
import argparse
import csv
import re
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "data" / "trader.db"
INBOX = ROOT / "data" / "schwab_imports"
ARCHIVE = INBOX / "archived"

DATE_RE = re.compile(r"(\d{4})/(\d{2})/(\d{2})")


def parse_money(s):
    """Convert '$25,453.05' or '-$6,439.15' or '(123.45)' to float, else None."""
    if s is None:
        return None
    s = s.strip().strip('"')
    if s in ("", "--", "N/A"):
        return None
    s = s.replace("$", "").replace(",", "").replace("(", "-").replace(")", "")
    try:
        return float(s)
    except ValueError:
        return None


def parse_csv(path):
    """Return (snapshot_date_iso, schwab_equity_total)."""
    with open(path, newline="") as f:
        rows = list(csv.reader(f))

    # 1) Find snapshot date in header lines (first ~5 rows)
    snapshot_date = None
    for row in rows[:5]:
        joined = ",".join(row) if row else ""
        m = DATE_RE.search(joined)
        if m:
            snapshot_date = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
            break
    if not snapshot_date:
        raise ValueError(f"Could not find date in header of {path}")

    # 2) Find header row + "Mkt Val" column index
    header_idx = None
    for i, row in enumerate(rows):
        if row and row[0].strip().strip('"') == "Symbol" and any("Mkt Val" in c for c in row):
            header_idx = i
            break
    if header_idx is None:
        raise ValueError(f"Could not find header row in {path}")

    header = rows[header_idx]
    mkt_val_idx = next((i for i, c in enumerate(header) if "Mkt Val" in c), None)
    if mkt_val_idx is None:
        raise ValueError(f"Could not find 'Mkt Val' column in {path}")

    # 3) Prefer the "Positions Total" row's Mkt Val (matches Schwab's reported total)
    total_equity = None
    for row in rows[header_idx + 1:]:
        if not row:
            continue
        sym = (row[0] or "").strip().strip('"')
        if "Positions Total" in sym and len(row) > mkt_val_idx:
            total_equity = parse_money(row[mkt_val_idx])
            break

    # 4) Fallback: sum every Mkt Val (positions + cash row)
    if total_equity is None:
        running = 0.0
        for row in rows[header_idx + 1:]:
            if not row or len(row) <= mkt_val_idx:
                continue
            sym = (row[0] or "").strip().strip('"')
            if not sym or "Positions Total" in sym:
                continue
            mv = parse_money(row[mkt_val_idx])
            if mv is not None:
                running += mv
        total_equity = running if running > 0 else None

    if total_equity is None:
        raise ValueError(f"Could not derive total equity from {path}")

    return snapshot_date, total_equity


def get_ghost_equity(cur):
    """Read most recent ghost equity (recalc'd at end of every ghost_advisor run)."""
    row = cur.execute("SELECT equity FROM ghost_cash WHERE id=1").fetchone()
    return row[0] if row else 0.0


def import_one(path, cur, archive=True):
    print(f"\n--- importing {path.name} ---")
    snap_date, schwab_eq = parse_csv(path)
    ghost_eq = get_ghost_equity(cur)
    delta = ghost_eq - schwab_eq

    existing = cur.execute(
        "SELECT ghost_equity, schwab_equity FROM ghost_equity_history WHERE date=?",
        (snap_date,)
    ).fetchone()

    cur.execute(
        "INSERT OR REPLACE INTO ghost_equity_history "
        "(date, ghost_equity, schwab_equity, delta) VALUES (?, ?, ?, ?)",
        (snap_date, ghost_eq, schwab_eq, delta)
    )

    sign = "+" if delta >= 0 else ""
    direction = "ahead" if delta >= 0 else "behind"
    print(f"  date:    {snap_date}")
    print(f"  schwab:  ${schwab_eq:>12,.2f}")
    print(f"  ghost:   ${ghost_eq:>12,.2f}")
    print(f"  delta:   {sign}${delta:>11,.2f}  (ghost {direction})")
    if existing:
        print(f"  note: replaced existing row for {snap_date}")

    if archive:
        ARCHIVE.mkdir(parents=True, exist_ok=True)
        target = ARCHIVE / f"{snap_date}__{path.name}"
        if target.exists():
            target = ARCHIVE / f"{snap_date}__{datetime.now().strftime('%H%M%S')}__{path.name}"
        shutil.move(str(path), str(target))
        print(f"  archived to: {target.relative_to(ROOT)}")


def main():
    ap = argparse.ArgumentParser(description="Import Schwab Positions CSV into ghost_equity_history")
    ap.add_argument("path", nargs="?", help="CSV file path (default: scan data/schwab_imports/)")
    ap.add_argument("--no-archive", action="store_true", help="Skip moving file after import")
    args = ap.parse_args()

    if not DB.exists():
        print(f"ERROR: db not found at {DB}")
        sys.exit(1)

    if args.path:
        p = Path(args.path)
        if not p.exists():
            print(f"ERROR: file not found: {args.path}")
            sys.exit(1)
        files = [p]
    else:
        INBOX.mkdir(parents=True, exist_ok=True)
        files = sorted(p for p in INBOX.glob("*.csv") if p.parent == INBOX)
        if not files:
            print(f"No CSV files in {INBOX.relative_to(ROOT)}/")
            print(f"Drop a Schwab Positions export there and re-run.")
            print(f"(Or pass a path explicitly: python3 scripts/import_schwab_snapshot.py /path/to/file.csv)")
            sys.exit(0)

    con = sqlite3.connect(str(DB))
    cur = con.cursor()
    try:
        for f in files:
            import_one(f, cur, archive=not args.no_archive)
        con.commit()
    finally:
        con.close()

    print(f"\n=== imported {len(files)} file(s) ===")


if __name__ == "__main__":
    main()
