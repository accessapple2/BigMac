#!/usr/bin/env python3
"""scripts/fleet_lifecycle_backfill_agents.py — HM-FLEET-LIFECYCLE-2026-08-29,
one-time bulk backfill + HM-FLEET-LIFECYCLE-BACKFILL-TARGET-2026-09-01,
on-demand single-target reconciliation.

**Bulk mode (no --target):** populates fleet_lifecycle_ledger with one row
per current ai_players row, reconstructed from the EXISTING halt_mode/
halt_reason fields (which have served, informally, as this exact record
for most of the project's life). Idempotent: skips any agent that
already has a ledger row. This was the tool's original, one-time-use
shape -- it seeded the ledger when it was first built (2026-08-29) and
has no ongoing reconciliation use in this mode.

**Target mode (--target NAME):** added 2026-09-01 after a real incident
this mode didn't cover -- ollama-qwen3/qwen3-4b-audition were halted via
a raw SQLite UPDATE on 2026-08-31 (bypassing scripts/fleet_lifecycle.py
entirely), and the only tool available to record it afterward was
fleet_lifecycle.py's own NORMAL halt action -- which correctly wrote a
real order doc and a real ledger row, but with backfilled=0, because
nothing in this repo could write backfilled=1 for a SINGLE already-
existing target on demand (this script's bulk mode explicitly skips any
target that already has a row, by design, for its bulk one-time use).
--target closes that gap: reconciles exactly one target's CURRENT live
state into a fresh backfilled=1 ledger row, regardless of whether it
already has ledger history -- this is the intended path when a live
state change happened outside scripts/fleet_lifecycle.py and needs its
paper trail recorded honestly, without a doc claiming a "live order" that
never happened.

Every row (either mode) is marked backfilled=1 and order_doc=NULL --
honest about provenance: these are reconstructed from live state, not a
real dated order. Going forward, use scripts/fleet_lifecycle.py's normal
actions for anything applied THROUGH the tool; use --target here only to
record something that was already applied some other way.

Mapping:
  halt_mode='active'                              -> action='active'
  halt_mode='exit_only'                            -> action='shadow'
  halt_mode='full', halt_reason mentions "retire"  -> action='retire'
  halt_mode='full', otherwise                      -> action='halt'

Usage:
    scripts/fleet_lifecycle_backfill_agents.py [--dry-run]
    scripts/fleet_lifecycle_backfill_agents.py --target NAME [--dry-run]
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "trader.db"


def _action_for(halt_mode: str, halt_reason: str | None) -> str:
    hm = (halt_mode or "active").lower()
    if hm == "active":
        return "active"
    if hm == "exit_only":
        return "shadow"
    # full
    reason_l = (halt_reason or "").lower()
    if "retire" in reason_l:
        return "retire"
    return "halt"


def _backfill_one(conn: sqlite3.Connection, target_name: str, dry_run: bool) -> int:
    """Target mode: reconcile exactly one agent's CURRENT live state into a
    fresh backfilled=1 ledger row, regardless of existing ledger history.
    Returns 0 on success (or dry-run), 1 if the target doesn't exist."""
    r = conn.execute(
        "SELECT id, display_name, halt_mode, halt_reason FROM ai_players WHERE id=?",
        (target_name,),
    ).fetchone()
    if r is None:
        print(f"ERROR: no ai_players row for '{target_name}'.", file=sys.stderr)
        return 1
    action = _action_for(r["halt_mode"], r["halt_reason"])
    reason = (r["halt_reason"] or
              f"halt_mode='{r['halt_mode']}', no halt_reason on record (backfilled).")
    print(f"{'[DRY RUN] ' if dry_run else ''}{r['id']} ({r['display_name']}) "
          f"-> {action}  reason: {reason[:100]}")
    if dry_run:
        print("[DRY RUN] would insert a fresh backfilled=1 ledger row (existing "
              "history, if any, is left in place -- the ledger is INSERT-only, "
              "current state is always the latest row).")
        return 0
    conn.execute(
        "INSERT INTO fleet_lifecycle_ledger "
        "(target_type, target_name, action, reason, order_doc, resume_by, review_by, backfilled) "
        "VALUES ('agent', ?, ?, ?, NULL, NULL, NULL, 1)",
        (r["id"], action, reason),
    )
    conn.commit()
    print(f"OK — backfilled ledger row for '{target_name}' (action={action}).")
    return 0


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--target", default=None,
                    help="reconcile exactly one agent's current live state on demand "
                         "(HM-FLEET-LIFECYCLE-BACKFILL-TARGET-2026-09-01) instead of "
                         "the bulk one-time seed below")
    args = p.parse_args()

    conn = sqlite3.connect(str(DB_PATH), timeout=15)
    conn.row_factory = sqlite3.Row

    if args.target:
        return _backfill_one(conn, args.target, args.dry_run)

    already = {r["target_name"] for r in conn.execute(
        "SELECT DISTINCT target_name FROM fleet_lifecycle_ledger WHERE target_type='agent'"
    ).fetchall()}

    rows = conn.execute(
        "SELECT id, display_name, halt_mode, halt_reason FROM ai_players ORDER BY id"
    ).fetchall()

    inserted = 0
    skipped = 0
    for r in rows:
        if r["id"] in already:
            skipped += 1
            continue
        action = _action_for(r["halt_mode"], r["halt_reason"])
        if action == "active":
            reason = "Active fleet seat, no lifecycle event on record (backfilled baseline)."
        else:
            reason = r["halt_reason"] or f"halt_mode='{r['halt_mode']}', no halt_reason on record (backfilled)."
        print(f"{'[DRY RUN] ' if args.dry_run else ''}{r['id']:30s} ({r['display_name']:25s}) -> {action:8s} {reason[:80]}")
        if not args.dry_run:
            conn.execute(
                "INSERT INTO fleet_lifecycle_ledger "
                "(target_type, target_name, action, reason, order_doc, resume_by, review_by, backfilled) "
                "VALUES ('agent', ?, ?, ?, NULL, NULL, NULL, 1)",
                (r["id"], action, reason),
            )
        inserted += 1

    if not args.dry_run:
        conn.commit()
    print(f"\n{'would insert' if args.dry_run else 'inserted'}: {inserted}, already present (skipped): {skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
