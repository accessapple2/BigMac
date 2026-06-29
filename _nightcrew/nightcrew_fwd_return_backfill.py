#!/usr/bin/env python3
"""
nightcrew_fwd_return_backfill.py  —  Carrier Night Crew · Task 3

PURPOSE
  Drain the signal_observations fwd_return backlog FASTER than the 200/cycle/30min
  scheduled evaluator, so a trustworthy full-drain alpha read is ready by morning.

SAFETY MODEL (read before running)
  - Uses the EXISTING, VERIFIED evaluate_pending() logic — does NOT reimplement the
    forward-return math. We only call it in a loop. No new SQL writes here.
  - Idempotent: evaluate_pending() filters WHERE evaluated_at IS NULL, so re-runs and
    concurrency with the live scheduler just skip already-done rows. No double-work,
    no mutation of evaluated rows.
  - Box-health gated: aborts/backs off if CPU load spikes, protecting the live process.
  - Does NOT touch the scheduled evaluator, config, flags, or the trader process.
  - RULE #1 / #7 untouched. No execution. No trade behavior. Append-only.

  >>> SCOTTY: adapt the ONE import line below to the real module path, then run. <<<
"""

import argparse, os, sys, time, sqlite3
from datetime import datetime, timezone

# Resolve to repo root so `engine` package imports work regardless of cwd.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

# ============================================================================
# INTEGRATION — Scotty: point this at the real evaluator entrypoint.
# It must be the SAME function the 30-min scheduler calls. Do NOT reimplement.
# Expected signature: evaluate_pending() -> int (rows evaluated this call), or
# adapt the call in drain_once() if it takes/returns something different.
# ============================================================================
try:
    from engine.signal_evaluator import evaluate_pending   # <-- ADAPT IF PATH DIFFERS
except Exception as e:                                       # pragma: no cover
    evaluate_pending = None
    _IMPORT_ERR = e

DB_DEFAULT = "data/trader.db"  # <-- ADAPT if signal_observations lives elsewhere


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def pending_count(db):
    c = sqlite3.connect(db)
    try:
        return c.execute(
            "SELECT COUNT(*) FROM signal_observations WHERE fwd_return_1d IS NULL"
        ).fetchone()[0]
    finally:
        c.close()


def filled_count(db):
    c = sqlite3.connect(db)
    try:
        return c.execute(
            "SELECT COUNT(*) FROM signal_observations WHERE fwd_return_1d IS NOT NULL"
        ).fetchone()[0]
    finally:
        c.close()


def load_ok(max_load):
    """True if 1-min load average is under the ceiling. macOS/Linux."""
    try:
        one_min = os.getloadavg()[0]
        return one_min <= max_load, one_min
    except (OSError, AttributeError):
        return True, -1.0  # can't read load -> don't block on it


def drain_once():
    """Call the verified evaluator once. Returns rows evaluated this call (best-effort)."""
    res = evaluate_pending()
    # evaluate_pending may return an int, a dict, or None depending on the impl.
    if isinstance(res, int):
        return res
    if isinstance(res, dict):
        return int(res.get("evaluated", res.get("filled", 0)) or 0)
    return None  # unknown — caller falls back to pending-delta detection


def main():
    ap = argparse.ArgumentParser(description="Throttled fwd_return backlog backfill (night crew).")
    ap.add_argument("--db", default=DB_DEFAULT)
    ap.add_argument("--sleep", type=float, default=2.0, help="seconds between batches (nice throttle)")
    ap.add_argument("--max-load", type=float, default=6.0, help="abort/backoff if 1-min load exceeds this")
    ap.add_argument("--max-backoffs", type=int, default=5, help="consecutive high-load checks before abort")
    ap.add_argument("--max-minutes", type=float, default=600.0, help="hard time cap")
    ap.add_argument("--dry-run", action="store_true", help="report state and exit, drain nothing")
    args = ap.parse_args()

    if evaluate_pending is None:
        print(f"[ABORT] could not import evaluate_pending: {_IMPORT_ERR}")
        print("        Scotty: fix the import line at top of this script.")
        sys.exit(2)

    if not os.path.exists(args.db):
        print(f"[ABORT] db not found: {args.db} (adapt --db)")
        sys.exit(2)

    start = time.time()
    p0 = pending_count(args.db); f0 = filled_count(args.db)
    total = p0 + f0
    print(f"[{now_iso()}] START  pending={p0}  filled={f0}  total={total}  "
          f"fill_rate={ (100.0*f0/total if total else 0):.1f}%")

    if args.dry_run:
        print("[dry-run] no drain performed."); return

    backoffs = 0
    cycles = 0
    while True:
        # time cap
        if (time.time() - start) / 60.0 > args.max_minutes:
            print(f"[{now_iso()}] STOP  time cap reached ({args.max_minutes} min)."); break

        # box-health gate
        ok, load = load_ok(args.max_load)
        if not ok:
            backoffs += 1
            print(f"[{now_iso()}] HIGH LOAD  1min={load:.2f} > {args.max_load}  "
                  f"backoff {backoffs}/{args.max_backoffs}")
            if backoffs >= args.max_backoffs:
                print(f"[{now_iso()}] ABORT  sustained high load — protecting live process."); break
            time.sleep(max(args.sleep * 4, 10)); continue
        backoffs = 0

        before = pending_count(args.db)
        if before == 0:
            print(f"[{now_iso()}] DONE  backlog fully drained."); break

        evaluated = drain_once()
        after = pending_count(args.db)
        progressed = before - after
        cycles += 1

        shown = evaluated if evaluated is not None else progressed
        print(f"[{now_iso()}] cycle {cycles}  load={load:.2f}  evaluated~{shown}  "
              f"pending {before}->{after}")

        # stall guard: a cycle that drains nothing means evaluate_pending isn't advancing
        if progressed <= 0 and (evaluated in (0, None)):
            print(f"[{now_iso()}] STOP  no progress this cycle — evaluator not advancing. "
                  f"Investigate before re-running."); break

        time.sleep(args.sleep)

    p1 = pending_count(args.db); f1 = filled_count(args.db)
    el = (time.time() - start) / 60.0
    print(f"[{now_iso()}] END    pending={p1}  filled={f1}  drained_this_run={f1 - f0}  "
          f"fill_rate={(100.0*f1/total if total else 0):.1f}%  elapsed={el:.1f}min")
    print("NEXT: capture /api/observations/summary + /api/measurement-health for the morning snapshot.")


if __name__ == "__main__":
    main()
