#!/usr/bin/env python3
"""scripts/fleet_lifecycle_backfill_agents.py — HM-FLEET-LIFECYCLE-2026-08-29,
one-time backfill.

Populates fleet_lifecycle_ledger with one row per current ai_players row,
reconstructed from the EXISTING halt_mode/halt_reason fields (which have
served, informally, as this exact record for most of the project's life).
Every row is marked backfilled=1 and order_doc=NULL -- honest about
provenance: these are reconstructed from pre-doctrine state, not real
dated orders. Going forward, all NEW state changes go through
scripts/fleet_lifecycle.py instead.

Mapping:
  halt_mode='active'                              -> action='active'
  halt_mode='exit_only'                            -> action='shadow'
  halt_mode='full', halt_reason mentions "retire"  -> action='retire'
  halt_mode='full', otherwise                      -> action='halt'

Idempotent: skips any agent that already has a ledger row (so re-running
after scripts/fleet_lifecycle.py has been used for real orders doesn't
clobber them).

Usage:
    scripts/fleet_lifecycle_backfill_agents.py [--dry-run]
"""
from __future__ import annotations

import argparse
import sqlite3
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


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    conn = sqlite3.connect(str(DB_PATH), timeout=15)
    conn.row_factory = sqlite3.Row

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
