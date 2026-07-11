#!/usr/bin/env python3
"""
fleet_heartbeat.py  --  Daily "is the whole paper-research machine alive?" report.

WHAT IT DOES
------------
Runs once each morning. Checks each major component (did it run recently? is the
process/endpoint up? did today's data land?) and sends ONE NTFY summarizing the
state of the fleet. Read-only health probes only — never touches trading, never
writes to a sacred DB, never moves money. Paper-research monitoring, full stop.

WHY IT MATTERS
--------------
This is the dead-man's-switch. If every component is green you get a "fleet alive"
ping. If anything is stale/down you get a "DEGRADED" ping naming the culprit. And
its companion watchdog (see HEARTBEAT WATCHDOG note below) fires if this script
ITSELF goes silent — because a heartbeat you can't hear isn't a heartbeat.

WIRING (for Scotty)  <<< FILL IN THE REAL CHECKS >>>
  Each check is a small function returning (ok: bool, detail: str). Replace the
  bodies marked TODO with the real probe against the actual cron/log/endpoint.
  Keep every probe READ-ONLY and fast — a heartbeat must never hang or mutate state.
"""

import os
import socket
import sys
import json
import threading
import time
import datetime as dt
from pathlib import Path

import requests

# ── Config ──────────────────────────────────────────────────────────────────
NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "ollietrades-admin")
NTFY_URL   = f"https://ntfy.sh/{NTFY_TOPIC}"
ROOT       = Path(os.environ.get("TRADER_ROOT", "/Users/bigmac/autonomous-trader"))
STALE_HOURS = 26  # a daily job older than this is considered stale (1 day + buffer)

_ntfy_ipv4_lock = threading.Lock()
_orig_getaddrinfo = socket.getaddrinfo


def _ipv4_only_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    return _orig_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)


def _ntfy(title, body, priority="default"):
    """HM-NTFY-IPV6-NOROUTE-SWEEP 2026-07-10: this box has no working IPv6
    route to ntfy.sh (HM-NTFY-IPV6-NOROUTE, 2026-07-07) — forces IPv4 via a
    local getaddrinfo monkeypatch."""
    try:
        with _ntfy_ipv4_lock:
            socket.getaddrinfo = _ipv4_only_getaddrinfo
            try:
                requests.post(NTFY_URL, data=body.encode("utf-8"),
                              headers={"Title": title, "Priority": priority}, timeout=8)
            finally:
                socket.getaddrinfo = _orig_getaddrinfo
    except requests.RequestException as e:
        print(f"[heartbeat] NTFY failed: {e}", file=sys.stderr)


def _age_hours(path):
    """Hours since a file was last modified, or None if missing."""
    try:
        return (time.time() - Path(path).stat().st_mtime) / 3600.0
    except (FileNotFoundError, OSError):
        return None


def _fresh(path, max_hours=STALE_HOURS):
    age = _age_hours(path)
    if age is None:
        return False, "missing"
    return (age <= max_hours), f"{age:.1f}h old"


# ── Component checks  <<< SCOTTY: WIRE THESE TO REAL PATHS >>> ───────────────
# Each returns (ok, detail). Read-only. Fast. No mutation.

def check_kirk():
    # kirk_briefing_cron.log: written by 4 daily cron modes (premarket/open/power_hour/after_close), M-F.
    # kirk_ingest.log doesn't exist yet — first run Mon 2026-06-23 13:32 AZ.
    return _fresh(ROOT / "logs/kirk_briefing_cron.log")

def check_schwab_sync():
    # real_holdings.json: written by sync_schwab_live.py during RTH.
    # On weekends expected stale — handled by WEEKEND_OPTIONAL in run_checks().
    return _fresh(ROOT / "data/real_holdings.json")

def check_scanners():
    # regime_refresh_cron.log: runs */15 6-13 M-F — most active scanner liveness proxy.
    # (fleet_auditor_cron.log is 0B; scanner.log doesn't exist)
    return _fresh(ROOT / "logs/regime_refresh_cron.log")

def check_trader_up():
    # TODO: liveness probe against the dashboard health endpoint (read-only).
    try:
        r = requests.get("http://localhost:8080/api/health", timeout=5)
        if r.ok:
            j = r.json()
            up = bool(j.get("server_up"))
            return up, f"up={up}, ollama={j.get('ollama_reachable')}"
        return False, f"http {r.status_code}"
    except requests.RequestException as e:
        return False, f"unreachable ({type(e).__name__})"

def check_drawdown_alert():
    # TODO: point at logs/drawdown_alert.log once the recurring cron is live.
    return _fresh(ROOT / "logs/drawdown_alert.log")


CHECKS = {
    "trader":   check_trader_up,
    "kirk":     check_kirk,
    "schwab":   check_schwab_sync,
    "scanners": check_scanners,
    "drawdown": check_drawdown_alert,
}

# Components that are legitimately quiet on weekends (market closed).
WEEKEND_OPTIONAL = {"schwab", "drawdown", "kirk", "scanners"}


def run_checks():
    today = dt.date.today()
    is_weekend = today.weekday() >= 5
    results = {}
    for name, fn in CHECKS.items():
        try:
            ok, detail = fn()
        except Exception as e:  # a probe must never crash the heartbeat
            ok, detail = False, f"probe error: {type(e).__name__}"
        # Don't flag market-hours components as failed on weekends.
        if is_weekend and name in WEEKEND_OPTIONAL and not ok:
            ok, detail = True, f"{detail} (weekend, expected)"
        results[name] = (ok, detail)
    return results, is_weekend


def main():
    results, is_weekend = run_checks()
    failed = [n for n, (ok, _) in results.items() if not ok]
    stamp = dt.datetime.now().strftime("%a %H:%M")

    lines = [f"{'OK ' if ok else 'DOWN'} {name}: {detail}"
             for name, (ok, detail) in results.items()]
    body = f"{stamp}\n" + "\n".join(lines)

    if failed:
        title = f"⚠️ FLEET DEGRADED — {', '.join(failed)}"
        priority = "urgent"
    else:
        title = "✅ Fleet alive"
        priority = "default"

    print(f"[heartbeat] {title}\n{body}")
    _ntfy(title, body, priority)

    # Drop a timestamp file the WATCHDOG reads (see note below).
    try:
        beat = ROOT / "logs/heartbeat_last.txt"
        beat.parent.mkdir(parents=True, exist_ok=True)
        beat.write_text(dt.datetime.now().isoformat())
    except OSError:
        pass


if __name__ == "__main__":
    main()

# ── HEARTBEAT WATCHDOG (the dead-man's-switch) ──────────────────────────────
# This script reports daily. But if cron itself dies, you'd hear SILENCE and
# wrongly assume all-clear. The watchdog closes that hole. It is a SEPARATE,
# tiny check on a DIFFERENT schedule (and ideally a different box — Ollie Max
# watching bigmac) that fires an alarm if logs/heartbeat_last.txt is stale.
#
# Scotty wires it as its own one-liner cron, e.g. 08:00 AZ:
#   age=$(( ($(date +%s) - $(stat -f %m logs/heartbeat_last.txt 2>/dev/null || echo 0)) / 3600 ))
#   [ "$age" -gt 26 ] && curl -s -H "Title: NO HEARTBEAT — fleet may be down" \
#       -H "Priority: urgent" -d "No fleet heartbeat in ${age}h" https://ntfy.sh/ollietrades-admin
#
# Silence from the reporter is no longer silence — the watchdog turns it into a ping.
