#!/usr/bin/env python3
"""scripts/fleet_lifecycle.py — HM-FLEET-LIFECYCLE-2026-08-29.

The single command for every agent/job state change (retire, bench,
shadow, halt, revive). Exists so the 2026-07-22 stand-down's class of
mystery can't recur: a state change that touches launchd/cron but never
gets a dated reason on record, never updates what monitoring expects, and
silently drifts for five weeks before anyone notices. See
docs/FLEET_LIFECYCLE.md for the full doctrine this tool enforces.

Every invocation performs, together, in one action:
  (a) a dated order doc (docs/orders/ORDER_<date>_<action>_<type>_<name>.md)
      with the reason -- a tombstone for 'retire', an explicit reversal
      checklist (resume-by/review-by) for 'bench'/'shadow'/'halt'.
  (b) the actual state change -- ai_players.halt_mode for an agent,
      launchctl enable/disable + bootstrap/bootout for a job.
  (c) sentinel registration: scripts/hm_ops_sentinel.py's
      check_launchd_jobs_health reads its job registry LIVE from this
      ledger (not a hardcoded dict) -- writing the ledger row IS the
      registration/deregistration, nothing else to keep in sync. Agent
      monitoring (ratings, audition tracking) already reads halt_mode
      live from ai_players, same principle.
  (d) the dashboard already reads ai_players.halt_mode live for agents
      (no separate update needed, confirmed by design); job state is
      exposed read-only via GET /api/fleet-lifecycle for any future
      dashboard surface to consume.
  (e) the fleet_lifecycle_ledger row itself -- INSERT ONLY, current state
      for a target is its latest row.

Refuses to do partial work: validates everything (target exists, action
is valid for that target type, reason present, resume_by/review_by
present for pause-type actions) BEFORE touching anything live. If the
live change fails after the order doc is written, the doc is marked
FAILED and no ledger row is inserted -- "why is X off" must never point
at a ledger row that lied about what actually happened.

Usage:
    scripts/fleet_lifecycle.py <action> <target_name> --reason "..." \\
        [--type agent|job] [--resume-by YYYY-MM-DD] [--review-by YYYY-MM-DD] \\
        [--dry-run]

    scripts/fleet_lifecycle.py status <target_name>
    scripts/fleet_lifecycle.py list [--type agent|job] [--action ACTION]
"""
from __future__ import annotations

import argparse
import subprocess
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "trader.db"
ORDERS_DIR = ROOT / "docs" / "orders"
LAUNCHAGENTS_DIR = Path.home() / "Library" / "LaunchAgents"

PAUSE_ACTIONS = {"bench", "shadow", "halt"}
PERMANENT_ACTIONS = {"retire"}
RESUME_ACTIONS = {"revive", "active"}
ALL_ACTIONS = PAUSE_ACTIONS | PERMANENT_ACTIONS | RESUME_ACTIONS

ACTIONS_BY_TARGET_TYPE = {
    "agent": ALL_ACTIONS,
    "job": {"retire", "halt", "revive"},  # bench/shadow are agent-only crew concepts
}


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(str(DB_PATH), timeout=15)
    c.row_factory = sqlite3.Row
    return c


# Job labels aren't all com.ollietrades.* -- a few fleet-adjacent jobs
# (e.g. premarket, signal-center) live under com.trademinds.* instead.
# Resolve by scanning for a "<prefix>.<name>.plist" match rather than
# hardcoding one prefix, so the tool works for either without a --label
# escape hatch every time.
_KNOWN_JOB_PREFIXES = ("com.ollietrades", "com.trademinds")


def _resolve_job_label(name: str) -> Optional[str]:
    for prefix in _KNOWN_JOB_PREFIXES:
        label = f"{prefix}.{name}"
        if (LAUNCHAGENTS_DIR / f"{label}.plist").exists():
            return label
    return None


def _detect_target_type(name: str, conn: sqlite3.Connection) -> Optional[str]:
    row = conn.execute("SELECT id FROM ai_players WHERE id = ?", (name,)).fetchone()
    if row:
        return "agent"
    if _resolve_job_label(name):
        return "job"
    return None


def _latest_ledger_row(conn: sqlite3.Connection, target_type: str, target_name: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM fleet_lifecycle_ledger WHERE target_type=? AND target_name=? "
        "ORDER BY created_at DESC, id DESC LIMIT 1",
        (target_type, target_name),
    ).fetchone()


# ── Order doc ─────────────────────────────────────────────────────────────

def _rel_or_abs(path: Path) -> str:
    """path relative to ROOT when possible (the normal case -- ORDERS_DIR
    lives under ROOT in production), else the absolute path. Never raises:
    a ledger row must not fail to write just because a caller (e.g. a
    test) pointed ORDERS_DIR somewhere outside ROOT."""
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _order_doc_path(action: str, target_type: str, target_name: str, today: str) -> Path:
    return ORDERS_DIR / f"ORDER_{today}_{action}_{target_type}_{target_name}.md"


def _ensure_fresh_rating_on_resume(conn: sqlite3.Connection, target_name: str) -> str:
    """HM-FLEET-LIFECYCLE-REVIVE-RATING-2026-09-01: a revived agent must
    never re-enter trading unrestricted just because it has no CURRENT
    rating. Verified live 2026-09-01: this tool's revive path never
    touched agent_ratings at all -- combined with the BENCH gate's 30-day
    staleness fail-open (0730aec, same day) and its pre-existing
    'never-rated fails open' rule, a revived D/E agent (stale rating) or
    a never-rated agent both land back trading with zero rating-based
    protection. Fail CLOSED here instead: try a real recompute first: if
    the agent already has enough clean current-season trades (rare right
    after a revive, but possible for a same-day re-halt/revive), a real
    rating gets computed and inserted by calculate_rating() itself, and
    that's what governs -- no synthetic override needed. Otherwise
    (the N/A path -- calculate_rating() returns 'N/A' and, confirmed by
    reading it, does NOT insert a row in that case) or on any recompute
    failure, insert a conservative, clearly-synthetic probation row
    (rating='D', score=0.0 -- 0.0 specifically to be visually
    distinguishable from a real computed D score, which this fleet's
    D-rated agents run 39-45) so the BENCH gate (engine/paper_trader.py::
    _bench_block_reason) has fresh, real, current data to find and block
    on. This probation naturally clears itself the moment the agent
    accumulates enough real trades for calculate_rating() to compute and
    insert a genuine rating (a later row, superseding this one) -- no
    separate expiry mechanism needed. Never raises -- a rating-system
    hiccup must never block a legitimate revive, but it must also never
    silently skip the probation insert; the try/except below wraps ONLY
    the recompute attempt, the probation insert itself always runs on
    that path.

    Returns a short status string for the caller to print, never raises.
    """
    try:
        from engine.agent_ratings import calculate_rating
        result = calculate_rating(target_name, "alltime")
        if result.get("rating") not in (None, "N/A"):
            return (f"fresh rating computed: {result['rating']} "
                     f"({result.get('rating_score', 0):.0f}/100)")
    except Exception as e:
        print(f"  [rating] recompute failed ({type(e).__name__}: {e}) -- "
              f"falling to conservative probation default", file=sys.stderr)

    # N/A (no clean current-season trades yet) or recompute failed --
    # fail closed: insert a synthetic probation row so BENCH blocks new
    # entries until this agent proves itself with real post-revive trades.
    try:
        conn.execute(
            "INSERT INTO agent_ratings "
            "(player_id, period, total_trades, wins, losses, win_rate, "
            " total_pnl, avg_win, avg_loss, profit_factor, best_trade, "
            " worst_trade, consecutive_losses, consecutive_wins, "
            " avg_confidence, pass_rate, volume_accuracy, rating, rating_score) "
            "VALUES (?, 'alltime', 0,0,0,0.0, 0.0,0.0,0.0,0.0,0.0,0.0,0,0, "
            "        0.0,0.0,0.0, 'D', 0.0)",
            (target_name,),
        )
        conn.commit()
        return "no current rating -- inserted conservative D/0 probation row (fail closed)"
    except Exception as e:
        return (f"WARNING: probation-row insert also failed ({type(e).__name__}: {e}) "
                f"-- agent has NO rating protection, check manually")


def _write_order_doc(path: Path, action: str, target_type: str, target_name: str,
                      reason: str, resume_by: Optional[str], review_by: Optional[str],
                      today: str) -> None:
    ORDERS_DIR.mkdir(parents=True, exist_ok=True)
    lines = []
    if action == "retire":
        lines.append(f"# TOMBSTONE — {target_name} ({target_type})")
        lines.append("")
        lines.append(f"**Retired:** {today}")
        lines.append(f"**Reason:** {reason}")
        lines.append("")
        lines.append("This is permanent under current criteria. No resume-by date — "
                      "revival requires a new explicit `revive` order, not a calendar trigger.")
    else:
        verb = {"halt": "HALTED", "bench": "BENCHED", "shadow": "SHADOWED",
                "revive": "REVIVED", "active": "ACTIVATED"}[action]
        lines.append(f"# ORDER — {verb}: {target_name} ({target_type})")
        lines.append("")
        lines.append(f"**Date:** {today}")
        lines.append(f"**Action:** {action}")
        lines.append(f"**Reason:** {reason}")
        if action in PAUSE_ACTIONS:
            lines.append("")
            lines.append("## Reversal checklist")
            lines.append(f"- Resume-by: {resume_by or '(not set)'}")
            lines.append(f"- Review-by: {review_by or '(not set)'}")
            lines.append("- Reverse with: `scripts/fleet_lifecycle.py revive "
                          f"{target_name} --reason \"...\"`")
            lines.append("- Until reversed, this state is intentional — a sentinel finding "
                          "against this target before its review-by date is a false alarm; "
                          "after it, it is a legitimate 'this pause was forgotten' alert.")
    path.write_text("\n".join(lines) + "\n")


def _mark_order_doc_failed(path: Path, error: str) -> None:
    if path.exists():
        path.write_text(path.read_text() + f"\n\n## FAILED\n\nThe live state change did not "
                         f"complete: {error}\n\nThis order was NOT applied. No ledger row was "
                         f"written. Safe to delete this file or retry the command.\n")


# ── Live state changes ───────────────────────────────────────────────────

def _apply_agent_change(conn: sqlite3.Connection, action: str, target_name: str,
                         reason: str, today: str) -> None:
    halt_mode = {
        "retire": "full", "bench": "full", "shadow": "exit_only",
        "halt": "full", "revive": "active", "active": "active",
    }[action]
    halt_reason = None if action in RESUME_ACTIONS else f"[{today}] {action.upper()}: {reason}"
    conn.execute(
        "UPDATE ai_players SET halt_mode=?, halt_reason=?, halted_at=? WHERE id=?",
        (halt_mode, halt_reason, None if action in RESUME_ACTIONS else datetime.now().isoformat(), target_name),
    )
    conn.commit()


def _job_is_loaded(label: str) -> bool:
    r = subprocess.run(["launchctl", "print", f"gui/501/{label}"],
                        capture_output=True, text=True, timeout=15)
    return r.returncode == 0


def _apply_job_change(action: str, target_name: str) -> None:
    label = _resolve_job_label(target_name)
    if label is None:
        raise RuntimeError(f"no plist found for '{target_name}' under any of {_KNOWN_JOB_PREFIXES}")
    plist = LAUNCHAGENTS_DIR / f"{label}.plist"
    if action == "revive":
        subprocess.run(["launchctl", "enable", f"gui/501/{label}"], check=True, timeout=15)
        # Idempotent: bootstrap only if not already loaded -- re-bootstrapping an
        # already-loaded job fails (exit 5, I/O error) even though nothing is
        # actually wrong. A job that's loaded-but-was-disabled just needed enable.
        if not _job_is_loaded(label):
            subprocess.run(["launchctl", "bootstrap", "gui/501", str(plist)], check=True, timeout=15)
    else:  # halt or retire
        subprocess.run(["launchctl", "bootout", f"gui/501/{label}"], timeout=15)  # ok if already unloaded
        subprocess.run(["launchctl", "disable", f"gui/501/{label}"], check=True, timeout=15)


# ── Ledger ────────────────────────────────────────────────────────────────

def _write_ledger_row(conn: sqlite3.Connection, target_type: str, target_name: str,
                       action: str, reason: str, order_doc: str,
                       resume_by: Optional[str], review_by: Optional[str]) -> None:
    conn.execute(
        "INSERT INTO fleet_lifecycle_ledger "
        "(target_type, target_name, action, reason, order_doc, resume_by, review_by, backfilled) "
        "VALUES (?,?,?,?,?,?,?,0)",
        (target_type, target_name, action, reason, order_doc, resume_by, review_by),
    )
    conn.commit()


# ── Command: apply an action ────────────────────────────────────────────

def cmd_apply(args: argparse.Namespace) -> int:
    conn = _conn()
    target_type = args.type or _detect_target_type(args.name, conn)
    if target_type is None:
        print(f"ERROR: '{args.name}' is not a known ai_players.id nor a "
              f"~/Library/LaunchAgents/com.ollietrades.{args.name}.plist. "
              f"Pass --type agent|job if this is intentional (e.g. a job not yet on disk).",
              file=sys.stderr)
        return 1
    if args.action not in ACTIONS_BY_TARGET_TYPE[target_type]:
        print(f"ERROR: action '{args.action}' is not valid for target_type '{target_type}'. "
              f"Valid actions: {sorted(ACTIONS_BY_TARGET_TYPE[target_type])}", file=sys.stderr)
        return 1
    if not args.reason or not args.reason.strip():
        print("ERROR: --reason is required and cannot be blank.", file=sys.stderr)
        return 1
    if args.action in PAUSE_ACTIONS and not (args.resume_by or args.review_by):
        print(f"ERROR: '{args.action}' is a pause-type action and requires "
              f"--resume-by and/or --review-by (a date, YYYY-MM-DD).", file=sys.stderr)
        return 1
    if target_type == "agent" and args.action != "active":
        row = conn.execute("SELECT id FROM ai_players WHERE id=?", (args.name,)).fetchone()
        if not row:
            print(f"ERROR: no ai_players row for '{args.name}'.", file=sys.stderr)
            return 1
    if target_type == "job" and args.action == "revive":
        if _resolve_job_label(args.name) is None:
            print(f"ERROR: no plist found for '{args.name}' under any of {_KNOWN_JOB_PREFIXES} "
                  f"— cannot revive a job with nothing to bootstrap.", file=sys.stderr)
            return 1

    today = date.today().isoformat()
    doc_path = _order_doc_path(args.action, target_type, args.name, today)

    if args.dry_run:
        print(f"[DRY RUN] would write {doc_path}")
        print(f"[DRY RUN] would apply {target_type} '{args.name}' -> action '{args.action}'")
        print(f"[DRY RUN] would insert ledger row")
        return 0

    _write_order_doc(doc_path, args.action, target_type, args.name, args.reason,
                      args.resume_by, args.review_by, today)

    try:
        if target_type == "agent":
            _apply_agent_change(conn, args.action, args.name, args.reason, today)
        else:
            _apply_job_change(args.action, args.name)
    except Exception as e:
        _mark_order_doc_failed(doc_path, f"{type(e).__name__}: {e}")
        print(f"ERROR: live state change failed, order doc marked FAILED, no ledger row "
              f"written: {type(e).__name__}: {e}", file=sys.stderr)
        return 1

    rating_note = ""
    if target_type == "agent" and args.action in RESUME_ACTIONS:
        rating_note = _ensure_fresh_rating_on_resume(conn, args.name)
        print(f"  [rating] {rating_note}")

    try:
        _write_ledger_row(conn, target_type, args.name, args.action, args.reason,
                           _rel_or_abs(doc_path), args.resume_by, args.review_by)
    except Exception as e:
        print(f"CRITICAL: live state change SUCCEEDED but the ledger row failed to write: "
              f"{type(e).__name__}: {e}. The live state and the ledger now disagree — "
              f"reconcile manually, do not re-run this command blindly.", file=sys.stderr)
        return 2

    print(f"OK — {target_type} '{args.name}' -> {args.action}. "
          f"Order: {_rel_or_abs(doc_path)}")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    conn = _conn()
    target_type = args.type or _detect_target_type(args.name, conn)
    if target_type is None:
        print(f"'{args.name}': not found as an agent or a known job plist.")
        return 1
    row = _latest_ledger_row(conn, target_type, args.name)
    if not row:
        print(f"'{args.name}' ({target_type}): no ledger entry. Reconcile with "
              f"`scripts/fleet_lifecycle_backfill_agents.py --target {args.name}` "
              f"if its live state was already changed outside this tool, or this "
              f"target predates the ledger.")
        return 0
    print(f"{args.name} ({target_type})")
    print(f"  state:      {row['action']}")
    print(f"  reason:     {row['reason']}")
    print(f"  order doc:  {row['order_doc'] or '(backfilled, no doc)'}")
    print(f"  resume_by:  {row['resume_by'] or '—'}")
    print(f"  review_by:  {row['review_by'] or '—'}")
    print(f"  as of:      {row['created_at']}{'  (backfilled)' if row['backfilled'] else ''}")
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    conn = _conn()
    where = []
    params: list = []
    if args.type:
        where.append("l.target_type=?")
        params.append(args.type)
    if args.action:
        where.append("l.action=?")
        params.append(args.action)
    clause = f"WHERE {' AND '.join(where)}" if where else ""
    # Latest row per target
    rows = conn.execute(f"""
        SELECT l.* FROM fleet_lifecycle_ledger l
        INNER JOIN (
            SELECT target_type, target_name, MAX(created_at) AS mx
            FROM fleet_lifecycle_ledger GROUP BY target_type, target_name
        ) latest ON l.target_type=latest.target_type AND l.target_name=latest.target_name
                AND l.created_at=latest.mx
        {clause}
        ORDER BY l.target_type, l.target_name
    """, params).fetchall()
    for r in rows:
        print(f"{r['target_type']:6s} {r['target_name']:35s} {r['action']:8s} "
              f"{r['reason'][:70]}")
    print(f"\n{len(rows)} targets")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    for action in sorted(ALL_ACTIONS):
        sp = sub.add_parser(action, help=f"apply the '{action}' lifecycle action")
        sp.add_argument("name")
        sp.add_argument("--reason", required=True)
        sp.add_argument("--type", choices=["agent", "job"], default=None)
        sp.add_argument("--resume-by", default=None)
        sp.add_argument("--review-by", default=None)
        sp.add_argument("--dry-run", action="store_true")
        sp.set_defaults(func=cmd_apply, action=action)

    sp_status = sub.add_parser("status")
    sp_status.add_argument("name")
    sp_status.add_argument("--type", choices=["agent", "job"], default=None)
    sp_status.set_defaults(func=cmd_status)

    sp_list = sub.add_parser("list")
    sp_list.add_argument("--type", choices=["agent", "job"], default=None)
    sp_list.add_argument("--action", default=None)
    sp_list.set_defaults(func=cmd_list)

    args = p.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
