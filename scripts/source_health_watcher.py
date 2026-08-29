"""HM-SOURCE-HEALTH-WATCHER (2026-06-02).

Independent source-health alerting watcher. Polls the per-source freshness grid
(engine.source_gate.all_health), tracks per-source last-state in its own JSON,
and:

  1. NTFYs ollietrades-admin on a (GREEN|AMBER|UNKNOWN)->RED transition, but ONLY
     for sources that "should be fresh" right now (overdue vs their OWN cadence).
  2. Emits a DAILY DIGEST of everything still-RED-and-should-be-fresh, so a
     lingering death re-pings rather than relying on a one-shot transition alert
     that could be missed. This is the part that would have caught the 11-day
     Movers gap.
  3. Writes a HEARTBEAT every run (dead-man's-switch source-of-truth). The
     trader's in-process scheduler reads this heartbeat and alarms if it goes
     stale (who-watches-the-watcher) — a DIFFERENT mechanism than this cron.

DOCTRINE — an alarm must run on a DIFFERENT mechanism than the thing it watches
(CLAUDE.md "Alarms must not share a failure mode with what they watch"). The
pre-existing in-process tracker (source_gate.check_source_health_alerts) fired
off the /api/sources/health HTTP poll and only covered live_decision sources, so
it shared fate with signal-center AND was blind to `movers` (criticality=context)
— it would NOT have caught the Movers gap on two counts. This standalone cron is
independent of both the producers and the signal-center web process: it imports
source_gate and computes freshness straight from the DBs.

WHY NO SEPARATE CADENCE MAP: the grid is ALREADY per-source. source_gate._classify
maps age->state using cadence-class-specific bands (intraday/daily/monthly/...),
so "overdue vs its own cadence" == grid state == "RED". A monthly source (macro/
FRED) only REDs past ~45d, so it can't false-alert at a daily threshold. Adding a
second cadence map here would be drift-bait (two maps that silently disagree).

SIM-SAFE / READ-ONLY: reads all_health() + writes ONLY its own state + heartbeat
JSON + sends NTFY. Zero executor/trading calls. Never calls set_enabled() or the
auto-quarantine path. (grep-gated in the build.)

Fires via crontab (NOT launchd — launchd doesn't survive reboot on this box;
see CLAUDE.md "LaunchAgent Reboot Lifecycle"), absolute paths (dogfooded).

Exit codes:
  0 - ran clean (alerts may or may not have fired)
  1 - error
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

_ROOT = Path(__file__).resolve().parent.parent
_STATE_PATH = _ROOT / "data" / "source_health_watcher_state.json"
_HEARTBEAT_PATH = _ROOT / "data" / "source_health_watcher_heartbeat.json"

# Sources to silence regardless of state (e.g. a known-flaky context feed). Empty
# by default; retired/snapshot/idle-by-design sources are already excluded below.
SUPPRESS_SOURCE_IDS: set[str] = set()

# How often the cron runs (informational — surfaced in the heartbeat so the DMS
# checker can reason about staleness).
INTERVAL_MINUTES = int(os.environ.get("SOURCE_HEALTH_WATCHER_INTERVAL_MIN", "10"))

# Local hour (box is AZ/MST, no DST) at/after which the daily digest fires once.
DIGEST_HOUR = int(os.environ.get("SOURCE_HEALTH_DIGEST_HOUR", "8"))
# Send a digest even when nothing is RED? Off by default (the heartbeat DMS already
# proves the watcher is alive, so a clear-day digest would be pure noise).
DIGEST_ON_ALL_CLEAR = os.environ.get("SOURCE_HEALTH_DIGEST_ALL_CLEAR", "").lower() in (
    "1", "true", "yes", "on",
)

RED = "RED"
RETIRED = "RETIRED"
NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "ollietrades-admin")


# ── State I/O (atomic) ───────────────────────────────────────────────────────
def _load_json(path: Path) -> Dict[str, Any]:
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(json.dumps(data, indent=2))
        os.replace(tmp, str(path))
    except Exception:
        try:
            os.unlink(tmp)
        except Exception:
            pass
        raise


# ── NTFY (engine.alert_channels when available; plain HTTP fallback) ──────────
def _ntfy(title: str, message: str, priority: str = "high", tags: str = "rotating_light") -> None:
    try:
        from engine.alert_channels import _send_ntfy
        if _send_ntfy(title, message, priority=priority, tags=tags, topic=NTFY_TOPIC):
            print(f"[src-health] ntfy ok: {title}")
            return
    except Exception as e:  # pragma: no cover - import/runtime fallback
        print(f"[src-health] engine.alert_channels unavailable: {e}", file=sys.stderr)
    try:
        import urllib.request
        req = urllib.request.Request(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=f"{title}\n{message}".encode("utf-8"),
            # HM-SRC-HEALTH-FALLBACK-ENCODING-2026-08-29: HTTP header VALUES are
            # latin-1-encoded by Python's http.client, unlike the body above
            # (which is explicitly utf-8) -- the em-dash in the original title
            # ('—') isn't representable in latin-1, so every fallback POST
            # raised UnicodeEncodeError before a request was ever sent. Plain
            # ASCII hyphen instead; this fallback only fires when the primary
            # engine.alert_channels path is unavailable, so it needs to be
            # bulletproof, not stylistically matched to the primary path.
            headers={"Title": "TradeMinds - source health", "Priority": priority, "Tags": tags},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            print(f"[src-health] fallback ntfy POST HTTP {r.status}")
    except Exception as e:
        print(f"[src-health] fallback ntfy failed: {e}", file=sys.stderr)


# ── "Should be fresh" filter ─────────────────────────────────────────────────
def _should_be_fresh(s: Dict[str, Any]) -> bool:
    """A source is alertable iff it is expected to update on a live cadence right
    now. Excludes retired (state==RETIRED), idle-by-design snapshot/archive feeds
    (metals/schwab — UNKNOWN is normal for them), pure non-decision sources, and
    anything explicitly suppressed. live_decision + context sources on a true
    live cadence (realtime/intraday/hourly/daily/daily_batch/weekly/monthly) stay
    in scope — `movers` (context/intraday) included, which the old tracker missed."""
    if s.get("source_id") in SUPPRESS_SOURCE_IDS:
        return False
    if s.get("state") == RETIRED:
        return False
    if s.get("cadence_class") in ("snapshot", "archive", "retired"):
        return False
    if s.get("criticality") not in ("live_decision", "context"):
        return False
    return True


def _fmt_source(s: Dict[str, Any]) -> str:
    return (
        f"{s.get('display_name', s.get('source_id'))} [{s.get('source_id')}] "
        f"{s.get('cadence_class')}/{s.get('criticality')} — "
        f"stale {s.get('age_human')}, as_of {s.get('as_of')}"
    )


# ── Main ─────────────────────────────────────────────────────────────────────
def main() -> int:
    try:
        from engine.source_gate import all_health
    except Exception as e:
        print(f"[src-health] cannot import source_gate: {type(e).__name__}: {e!r}", file=sys.stderr)
        return 1

    now = datetime.now(timezone.utc)
    now_local = datetime.now()
    today = now_local.strftime("%Y-%m-%d")

    try:
        health = all_health(now)
    except Exception as e:
        print(f"[src-health] all_health() failed: {type(e).__name__}: {e!r}", file=sys.stderr)
        return 1

    sources: List[Dict[str, Any]] = health.get("sources", [])
    state = _load_json(_STATE_PATH)
    src_state: Dict[str, Any] = state.get("sources", {}) if isinstance(state.get("sources"), dict) else {}

    transitions: List[str] = []
    red_now: List[Dict[str, Any]] = []

    for s in sources:
        sid = s.get("source_id")
        if not sid or not _should_be_fresh(s):
            # Drop stale per-source state for sources no longer in scope so the file
            # doesn't accrete retired ids forever.
            src_state.pop(sid, None)
            continue

        st = s.get("state")
        prev = src_state.get(sid, {}) if isinstance(src_state.get(sid), dict) else {}
        prev_state = prev.get("last_state")
        red_since = prev.get("red_since")

        if st == RED:
            if red_since is None:
                red_since = now.isoformat()
            if prev_state != RED:
                # (GREEN|AMBER|UNKNOWN|first-seen) -> RED: fire transition alert.
                _ntfy(
                    f"Source RED: {s.get('display_name', sid)}",
                    f"{_fmt_source(s)} — overdue vs its {s.get('cadence_class')} cadence.",
                    priority="high", tags="rotating_light",
                )
                transitions.append(sid)
            red_now.append({**s, "red_since": red_since})
        else:
            red_since = None

        src_state[sid] = {"last_state": st, "red_since": red_since}

    # ── Daily digest (self-gated: once per local day at/after DIGEST_HOUR) ──────
    digest_sent = False
    if now_local.hour >= DIGEST_HOUR and state.get("last_digest_date") != today:
        if red_now or DIGEST_ON_ALL_CLEAR:
            if red_now:
                lines = []
                for s in sorted(red_now, key=lambda x: x.get("red_since") or ""):
                    since = s.get("red_since")
                    lines.append(f"• {_fmt_source(s)} (RED since {since})")
                _ntfy(
                    f"Source-health digest: {len(red_now)} still RED",
                    "Sources overdue vs their own cadence:\n" + "\n".join(lines),
                    priority="high", tags="warning",
                )
            else:
                _ntfy(
                    "Source-health digest: all clear",
                    "All should-be-fresh sources GREEN/AMBER.",
                    priority="default", tags="white_check_mark",
                )
            digest_sent = True
        state["last_digest_date"] = today

    # ── Persist state + heartbeat ──────────────────────────────────────────────
    state["sources"] = src_state
    state["last_run"] = now.isoformat()
    _save_json(_STATE_PATH, state)

    heartbeat = {
        "watcher": "source_health_watcher",
        "last_run": now.timestamp(),
        "last_run_iso": now.isoformat(),
        "interval_minutes": INTERVAL_MINUTES,
        "sources_checked": sum(1 for s in sources if _should_be_fresh(s)),
        "red_should_be_fresh": [s.get("source_id") for s in red_now],
        "transitions_this_run": transitions,
        "digest_sent": digest_sent,
    }
    _save_json(_HEARTBEAT_PATH, heartbeat)

    print(
        f"[src-health] ok — checked {heartbeat['sources_checked']} should-be-fresh, "
        f"RED={[s.get('source_id') for s in red_now]}, transitions={transitions}, "
        f"digest_sent={digest_sent}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
