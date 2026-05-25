"""HM-PROVING-GROUND-FORMALIZE-V2 SUB-2 — Admiral CLI for terminal state.

The state evaluator (engine/proving_ground.py::ship_kill_evaluator)
moves the Sniper Mode trial through {pending → warning → ship_ready |
kill_warning} automatically based on observed metrics, but the
TERMINAL states (shipped, killed) require explicit Admiral action.

This script is the ONLY path to those terminal states.

Usage:
  python scripts/proving_ground_admiral.py --ship --confirm --agent ollie-auto
  python scripts/proving_ground_admiral.py --kill --confirm --agent ollie-auto

Both ``--confirm`` and ``--agent <name>`` are required. The agent name
must match the Sniper Mode role-holder ("ollie-auto" per CLAUDE.md).
A guard prevents transitions from non-ready states (cannot ship from
pending unless evaluator already says ship_ready; cannot kill from
warning unless evaluator already says kill_warning).

The CLI writes:
  - running_scorecard.exit_status = 'shipped' or 'killed' (today's row)
  - state_transitions row with from→to + trigger_metrics_json
  - NTFY HIGH severity to ollietrades-proving-ground

After this, the evaluator becomes a no-op (terminal-sticky).
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path so engine imports work when run directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from engine.proving_ground import (  # noqa: E402
    AZ_TZ,
    P_HIGH,
    PG_DB,
    TERMINAL_STATES,
    _fire,
    _load_recent_scorecard,
    _log_state_transition,
)


ALLOWED_AGENT = "ollie-auto"  # Sniper Mode role-holder per CLAUDE.md
SHIP_FROM = {"ship_ready"}
KILL_FROM = {"warning", "kill_warning", "pending"}


def _read_current_state() -> tuple[str, str]:
    """Return (today_iso, current_state) — latest scorecard row's status."""
    history = _load_recent_scorecard(1)
    if not history:
        return (datetime.now(AZ_TZ).date().isoformat(), "pending")
    return (history[0]["as_of_date"], history[0].get("exit_status") or "pending")


def _commit_terminal_transition(
    target: str, agent: str, current_state: str, today_iso: str
) -> dict:
    conn = sqlite3.connect(PG_DB, timeout=30)
    try:
        # 1) Stamp today's scorecard row (or latest if today not present).
        cur = conn.execute(
            "UPDATE running_scorecard SET exit_status = ? WHERE as_of_date = ?",
            (target, today_iso),
        )
        if cur.rowcount == 0:
            conn.execute(
                "UPDATE running_scorecard SET exit_status = ? "
                " WHERE as_of_date = (SELECT MAX(as_of_date) FROM running_scorecard)",
                (target,),
            )
        # 2) Log the transition (Doctrine Rule #1 append-only).
        metrics = {
            "admiral_terminal": True,
            "agent": agent,
            "from_state": current_state,
            "to_state": target,
            "issued_at": datetime.now(AZ_TZ).isoformat(),
        }
        ntfy_sent = False
        try:
            verb = "GRADUATED" if target == "shipped" else "TERMINATED"
            _fire(
                title=f"PROVING GROUND: {verb} ({agent})",
                body=(
                    f"Admiral issued --{'ship' if target == 'shipped' else 'kill'} on "
                    f"{agent}. Trial state: {current_state} → {target}. "
                    "Evaluator now terminal-sticky."
                ),
                priority=P_HIGH,
                tags="trophy" if target == "shipped" else "red_circle",
            )
            ntfy_sent = True
        except Exception:
            pass
        _log_state_transition(
            from_state=current_state,
            to_state=target,
            metrics=metrics,
            ntfy_sent=ntfy_sent,
            pg_conn=conn,
        )
        conn.commit()
        return {
            "ok": True,
            "from_state": current_state,
            "to_state": target,
            "agent": agent,
            "ntfy_sent": ntfy_sent,
        }
    finally:
        conn.close()


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    grp = p.add_mutually_exclusive_group(required=True)
    grp.add_argument("--ship", action="store_true", help="Graduate ollie-auto from Sniper Mode trial")
    grp.add_argument("--kill", action="store_true", help="Terminate Sniper Mode trial")
    p.add_argument("--confirm", action="store_true", required=False,
                   help="Required safety flag")
    p.add_argument("--agent", default="", help=f"Agent name (must equal '{ALLOWED_AGENT}')")
    args = p.parse_args()

    if not args.confirm:
        print("ERROR: --confirm flag required for terminal state transition.")
        return 2
    if args.agent != ALLOWED_AGENT:
        print(
            f"ERROR: --agent must equal '{ALLOWED_AGENT}' "
            f"(got: {args.agent!r}). Sniper Mode role-holder per CLAUDE.md."
        )
        return 2

    target = "shipped" if args.ship else "killed"
    today_iso, current_state = _read_current_state()

    if current_state in TERMINAL_STATES:
        print(f"ALREADY TERMINAL: current state is '{current_state}'. No-op.")
        return 1

    allowed_from = SHIP_FROM if args.ship else KILL_FROM
    if current_state not in allowed_from:
        print(
            f"BLOCKED: cannot transition --{('ship' if args.ship else 'kill')} from "
            f"'{current_state}'. Allowed from: {sorted(allowed_from)}. "
            "Wait for evaluator to surface the appropriate signal first."
        )
        return 1

    result = _commit_terminal_transition(target, args.agent, current_state, today_iso)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
