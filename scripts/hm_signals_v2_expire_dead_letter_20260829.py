#!/usr/bin/env python3
"""HM-SIGNALS-V2-STARVATION-RECURRENCE resolution (2026-08-29, Admiral directive).

Third recurrence of the same disease HM-SIGNALS-V2-FIFO-STARVATION (2026-07-06)
and its follow-up (2026-07-09) already fixed twice: pending=11,663 (>3000 cap),
oldest 1199h. HM-SIGNALS-V2-STARVATION-RECURRENCE (filed 2026-07-12) explicitly
warned this could recur and proposed two candidate fixes -- neither was ever
implemented pending Admiral sign-off. Verification live 2026-08-29:

    source          halt_mode   pending   already past own stale_after
    ollama-qwen3    active      4,039     4,013  (99.4%)
    ollama-plutus   active      3,955     2,854  (72.2%)
    mlx-qwen3       full        3,666     3,648  (99.5%)
    ollie-auto      exit_only       3          0

Unlike the 2026-07-06 case (91.5% dead-letter via halt_mode alone), this
backlog is dominated by ACTIVE-source rows -- but nearly all of them
(6,867 of 7,994) are independently, definitively dead via their OWN
stale_after marker: engine.events_bus_consumer.consume_pending_signals()
never executes a row whose stale_after has already passed (NULL means
"never-stale" by explicit design, per that module's own comment -- those
rows are deliberately left untouched here, not archived just for being old).

Archives (status='expired') the UNION of:
  (a) pending rows from a non-active-halt_mode source (same criterion as
      the 2026-07-06 fix, scripts/hm_signals_v2_expire_halted_backlog.py)
  (b) pending rows whose stale_after is set AND already in the past --
      these would be rejected by the consumer's own staleness check the
      instant they were dequeued regardless of source status, so archiving
      them loses nothing that could ever have executed.

Leaves the ~1,127 NULL-stale_after active-source rows (1,101 ollama-plutus +
26 ollama-qwen3) untouched -- deliberately never-stale by design, and now
reachable going forward under the same-day hybrid-ordering fix
(engine/events_bus_consumer.py, this same commit) that reserves drain-cap
slots for the oldest pending row every cycle instead of pure newest-first,
so they drain over the following days rather than starving indefinitely.

Archive-not-delete, same convention as both 2026-07-06/07-09 scripts.
Dry-run by default; pass --apply to write. Snapshots target ids first.
"""
import sys
import sqlite3
import datetime
from pathlib import Path

ROOT = Path.home() / "autonomous-trader"
DB = ROOT / "data" / "trader.db"

SELECT_TARGETS = """
    SELECT s.id, s.source, s.symbol, s.direction, s.created_at, s.stale_after,
           COALESCE(p.halt_mode, 'active') as halt_mode
    FROM signals_v2 s
    LEFT JOIN ai_players p ON p.id = s.source
    WHERE s.status = 'pending'
      AND (
            COALESCE(p.halt_mode, 'active') != 'active'
         OR (s.stale_after IS NOT NULL AND s.stale_after < datetime('now'))
      )
    ORDER BY s.created_at ASC
"""


def main():
    apply = "--apply" in sys.argv
    conn = sqlite3.connect(DB, timeout=30)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(SELECT_TARGETS).fetchall()

    print(f"Target rows (pending, dead-letter by halt_mode or stale_after): {len(rows)}")
    by_source = {}
    for r in rows:
        by_source.setdefault((r["source"], r["halt_mode"]), 0)
        by_source[(r["source"], r["halt_mode"])] += 1
    print("\nBy source:")
    for (source, halt_mode), count in sorted(by_source.items(), key=lambda x: -x[1]):
        print(f"  {source:24} {str(halt_mode):10} {count}")

    remaining = conn.execute(
        "SELECT source, COUNT(*) FROM signals_v2 s "
        "LEFT JOIN ai_players p ON p.id = s.source "
        "WHERE s.status='pending' AND COALESCE(p.halt_mode,'active')='active' "
        "AND (s.stale_after IS NULL OR s.stale_after >= datetime('now')) "
        "GROUP BY s.source"
    ).fetchall()
    print(f"\nWould remain pending after this archive (NULL stale_after, active source):")
    for r in remaining:
        print(f"  {r[0]:24} {r[1]}")

    if not rows:
        print("\nNothing to do.")
        return

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    snapshot_path = ROOT / "data" / "backups" / f"hm_signals_v2_expire_dead_letter_{ts}.ids"
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
        f"UPDATE signals_v2 SET status='expired', stale_after=COALESCE(stale_after, ?) "
        f"WHERE id IN ({placeholders})",
        [now] + ids,
    )
    conn.commit()
    print(f"[APPLIED] {len(ids)} rows set to status='expired'")
    print(f"\nRollback command:")
    print(f"  python3 -c \"import sqlite3; c=sqlite3.connect('{DB}'); "
          f"ids=[int(x) for x in open('{snapshot_path}')]; "
          f"c.execute(f'UPDATE signals_v2 SET status=\\\'pending\\\' "
          f"WHERE id IN ({{','.join('?'*len(ids))}})', ids); "
          f"c.commit()\"")


if __name__ == "__main__":
    main()
