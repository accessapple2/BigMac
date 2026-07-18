#!/usr/bin/env python3
"""HM-BRIDGE-CONSENSUS-STALE Monday verification (2026-07-18, one-shot).

Friday 2026-07-17's bridge_consensus (Bridge Vote, engine/bridge_vote.py)
never fired -- source-health sat AMBER at as_of 2026-07-16 13:02 for ~30h,
missing the entire Friday market-hours window. Root cause: engine/
bridge_vote.run_bridge_vote_job() is gated to a single 9:00-9:10 AM ET
window, checked only via main.py's single-threaded `schedule.run_pending()`
queue (schedule.every(5).minutes.do(...), ~15+ same-cadence jobs registered
ahead of it). That queue is a well-documented bottleneck elsewhere in
main.py (see comments at lines ~1416/1687/1724/2003/4184/4216/4242/4255/
4312) -- long-running jobs registered earlier in the list (war room cycles
observed at wall=299.9s-331.2s that same morning) can push execution past
the 10-minute window before bridge_vote's own gate check ever runs, causing
a silent full-day miss with no exception and no log line (the function
returns early, before any logger call, when outside its fire window).
Manually re-run via `run_morning_vote(force=True)` on 2026-07-18 confirmed
GREEN with fresh as_of.

This script is the follow-up freshness check requested for Monday
2026-07-20 pre-market: confirm bridge_consensus is GREEN (age <= 24h per
its `daily` cadence_class in source_registry) at market open, i.e. that
Monday's own 9:00-9:10 AM ET window fired cleanly and the Friday gap was a
one-off rather than a recurring scheduler-jam pattern.

Read-only against trader.db + signal-center/signals.db (via engine.
source_gate.source_freshness -- no writes). NTFYs ollietrades-admin only if
still not GREEN. Meant to fire once via a self-one-shot launchd job, safe
to re-run.
"""
from __future__ import annotations

import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

NTFY_TOPIC = "ollietrades-admin"


def _ntfy(title: str, message: str) -> None:
    try:
        req = urllib.request.Request(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=message.encode("utf-8"),
            headers={"Title": title},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        print(f"[bridge-consensus-monday-check] ntfy failed: {e}", file=sys.stderr)


def main() -> int:
    from engine.source_gate import source_freshness

    health = source_freshness("bridge_consensus")
    state = health.get("state")
    as_of = health.get("as_of")
    age_human = health.get("age_human")

    print(f"[bridge-consensus-monday-check] state={state} as_of={as_of} age={age_human}")

    if state != "GREEN":
        _ntfy(
            f"HM-BRIDGE-CONSENSUS-STALE: Monday check {state}",
            f"bridge_consensus is {state} at Monday market open (as_of={as_of}, "
            f"age={age_human}). Friday 07-17's gate-window miss may be recurring -- "
            "check engine/bridge_vote.run_bridge_vote_job and main.py's "
            "schedule.run_pending() queue for jam/delay ahead of it.",
        )
        print("[bridge-consensus-monday-check] NOT GREEN -- alert sent.")
    else:
        print("[bridge-consensus-monday-check] GREEN -- no alert needed.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
