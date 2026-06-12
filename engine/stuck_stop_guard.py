"""HM-STOP-EXECUTION-GAP trip-safety net (2026-06-12).

NTFY a RED_ALERT (CRITICAL-tier) when a stop-loss has fired on the same
(player, symbol) for MORE THAN 2 consecutive scan cycles without the position
closing — i.e. the stop keeps firing but the order path is silently dropping
the exit (the exact failure mode that left ollama-plutus HIMS stuck 2+ days:
get_position() omitted expiry_date → estimate_option_price() returned $0.00 →
the "< $0.01 protect-position" guard blocked every sell).

This watchdog is deliberately ORTHOGONAL to the order path it watches: it keys
off whether sell() returned a fill, persists its streaks to a restart-survivable
state file, and never touches the order path. Per CLAUDE.md "alarms must not
share a failure mode with what they watch."

Never raises — a watchdog must not be able to crash the cycle it guards.
"""
from __future__ import annotations
import json
import time
from pathlib import Path
from rich.console import Console

console = Console()

_STATE_FILE = Path(__file__).resolve().parent.parent / "data" / "stuck_stop_health.json"
_ALERT_THRESHOLD = 2  # strictly MORE THAN 2 consecutive un-closed fires → alert (i.e. the 3rd)


def _read() -> dict:
    try:
        return json.loads(_STATE_FILE.read_text())
    except Exception:
        return {}


def _write(state: dict) -> None:
    try:
        _STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _STATE_FILE.write_text(json.dumps(state))
    except Exception:
        pass


def record_stop_outcome(player_id: str, symbol: str, executed: bool,
                        detail: str = "") -> int:
    """Record one cycle's stop-loss outcome for (player_id, symbol).

    executed=True  → the exit filled (or the position is closing) → reset streak.
    executed=False → the stop fired but the order path returned no fill →
                     increment the streak; once it exceeds _ALERT_THRESHOLD
                     fire ONE RED_ALERT ntfy (re-armed only after a reset).

    Returns the new consecutive-un-closed streak. Never raises.
    """
    try:
        key = f"{player_id}:{symbol}"
        state = _read()
        rec = state.get(key) or {"streak": 0, "alerted": False}
        if executed:
            if rec.get("streak"):
                console.log(f"[green][stuck-stop] {key} cleared after {rec['streak']} stuck cycle(s)")
            state[key] = {"streak": 0, "alerted": False, "last_ts": time.time()}
            _write(state)
            return 0
        rec["streak"] = int(rec.get("streak", 0)) + 1
        rec["last_ts"] = time.time()
        rec["last_detail"] = detail
        if rec["streak"] > _ALERT_THRESHOLD and not rec.get("alerted"):
            rec["alerted"] = True
            _fire_alert(player_id, symbol, rec["streak"], detail)
        state[key] = rec
        _write(state)
        return rec["streak"]
    except Exception as e:
        console.log(f"[yellow]stuck_stop_guard error: {type(e).__name__}: {e!r}")
        return 0


def _fire_alert(player_id: str, symbol: str, streak: int, detail: str) -> None:
    try:
        from engine.alert_channels import send_alert, AlertLevel
        d = f" — {detail}" if detail else ""
        send_alert(
            message=(f"STUCK STOP: {player_id} {symbol} stop-loss has fired {streak} "
                     f"consecutive cycles but the position is STILL OPEN{d}. The order "
                     f"path is silently dropping the exit — manual intervention required. "
                     f"(trip-safety net, HM-STOP-EXECUTION-GAP)"),
            level=AlertLevel.RED_ALERT,            # CRITICAL-tier (ntfy priority=urgent)
            alert_type=f"stuck_stop:{player_id}:{symbol}",  # per-position dedup, 1/hr
            title=f"🚨 STUCK STOP {player_id} {symbol}",
            audience="admin",                      # → ollietrades-admin
            rate_limit_secs=3600,
        )
        console.log(f"[bold red]🚨 STUCK-STOP RED_ALERT ntfy fired: {player_id} {symbol} x{streak}")
    except Exception as e:
        console.log(f"[yellow]stuck_stop_guard NTFY failed: {type(e).__name__}: {e!r}")
