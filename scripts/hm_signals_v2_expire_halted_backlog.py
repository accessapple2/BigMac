#!/usr/bin/env python3
"""HM-SIGNALS-V2-FIFO-STARVATION fix, option 1 (2026-07-06, Admiral-approved).

Expires signals_v2 pending rows whose source agent is NOT halt_mode='active'
-- these can never execute (paper_trader.buy()'s halt gate rejects any
non-active source unconditionally), so they are pure FIFO-queue starvation
with zero execution value. Archive-not-delete: status='expired', same
convention _expire_signal() already uses in engine/events_bus_consumer.py.

Dry-run by default (prints count + sample rows, NO writes).
Pass --apply to write. Snapshots the exact target ids to a timestamped
file first, so the change is precisely reversible:
    UPDATE signals_v2 SET status='pending', stale_after=NULL
    WHERE id IN (<ids from the snapshot file>)
"""
import sys
import sqlite3
import datetime
from pathlib import Path

ROOT = Path.home() / "autonomous-trader"
DB = ROOT / "data" / "trader.db"

SELECT_TARGETS = """
    SELECT s.id, s.source, s.symbol, s.direction, s.created_at, p.halt_mode
    FROM signals_v2 s
    JOIN ai_players p ON p.id = s.source
    WHERE s.status = 'pending' AND p.halt_mode != 'active'
    ORDER BY s.created_at ASC
"""


def main():
    apply = "--apply" in sys.argv
    conn = sqlite3.connect(DB, timeout=30)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(SELECT_TARGETS).fetchall()

    print(f"Target rows (pending, source halt_mode != 'active'): {len(rows)}")
    by_source = {}
    for r in rows:
        by_source.setdefault((r["source"], r["halt_mode"]), 0)
        by_source[(r["source"], r["halt_mode"])] += 1
    print("\nBy source:")
    for (source, halt_mode), count in sorted(by_source.items(), key=lambda x: -x[1]):
        print(f"  {source:24} {halt_mode:10} {count}")

    print("\nSample rows (first 5, oldest first):")
    for r in rows[:5]:
        print(f"  id={r['id']:6} {r['source']:20} {r['symbol']:8} "
              f"{r['direction'] or '':6} {r['created_at']}")

    if not rows:
        print("\nNothing to do.")
        return

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    snapshot_path = ROOT / "data" / "backups" / f"hm_signals_v2_expire_{ts}.ids"
    ids = [r["id"] for r in rows]

    if not apply:
        print(f"\n[DRY RUN] would snapshot {len(ids)} ids to {snapshot_path}")
        print("[DRY RUN] pass --apply to write")
        return

    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    with open(snapshot_path, "w") as f:
        for i in ids:
            f.write(f"{i}\n")
    print(f"\n[SNAPSHOT] {len(ids)} ids written to {snapshot_path}")

    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    placeholders = ",".join("?" * len(ids))
    conn.execute(
        f"UPDATE signals_v2 SET status='expired', stale_after=? "
        f"WHERE id IN ({placeholders})",
        [now] + ids,
    )
    conn.commit()
    print(f"[APPLIED] {len(ids)} rows set to status='expired'")
    print(f"\nRollback command:")
    print(f"  python3 -c \"import sqlite3; c=sqlite3.connect('{DB}'); "
          f"ids=[int(x) for x in open('{snapshot_path}')]; "
          f"c.execute(f'UPDATE signals_v2 SET status=\\\'pending\\\', "
          f"stale_after=NULL WHERE id IN ({{','.join('?'*len(ids))}})', ids); "
          f"c.commit()\"")


if __name__ == "__main__":
    main()
