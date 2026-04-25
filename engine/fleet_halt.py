"""Fleet halt — file-based emergency stop for USS TradeMinds scheduled jobs.

To activate:  touch ~/autonomous-trader/KILL_SWITCH
To deactivate: rm ~/autonomous-trader/KILL_SWITCH

Integrate by calling fleet_halt.check_or_bail() at the top of any
scheduled job. Returns silently if clear; logs + returns True if halted.

Created 2026-04-20 (Phase C3, zombie hunt hardening).
"""
from __future__ import annotations
from pathlib import Path

_KILL_FILE = Path(__file__).parent.parent / "KILL_SWITCH"


def is_active() -> bool:
    """Return True if the KILL_SWITCH file exists."""
    return _KILL_FILE.exists()


def check_or_bail(context: str = "") -> bool:
    """Return True (and log) if halt is active; return False if clear.

    Usage::
        if fleet_halt.check_or_bail("bridge_vote"):
            return
    """
    if _KILL_FILE.exists():
        tag = f" [{context}]" if context else ""
        try:
            from rich.console import Console
            Console().log(f"[bold red]KILL_SWITCH active{tag} — job skipped")
        except Exception:
            print(f"KILL_SWITCH active{tag} — job skipped")
        return True
    return False
