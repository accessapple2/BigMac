#!/usr/bin/env python3
"""scripts/watch_war_room_layer1_observation.py

HM-WAR-ROOM-LATENCY Layer 1 post-merge observation harness.

Launch AFTER Captain restarts the trader. Watches logs/trader.log +
logs/trader_error.log for a 10-minute window and records first-occurrence
timestamps for four anchor events:

    ARENA_INIT   eager Arena init (HM-WAR-ROOM-INIT-FIX still works) —
                 expected log line:  "[STARTUP] Arena initialized eagerly
                 (HM-WAR-ROOM-INIT-FIX)"
    WR_LAUNCH    cycle entry — "War Room: launching cycle"
    WR_DUR       Layer 1 cycle-end log — "[WR-DUR] cycle wall=...s"
    WR_STALL     Layer 1 stall NTFY — "[WR-STALL]" (healthy run = absent)

The watch ends after 10 minutes total, OR early (after a 60s grace) once all
three positive events have been observed and no stall has fired. NTFYs the
summary to NTFY_ADMIN_TOPIC (ollietrades-admin) via
engine.alert_channels.send_alert. Level=INFO on a clean window, WARNING if
WR_STALL fires or any positive event is missing at window close.

CLAUDE.md log-sink discipline applies: trader.log holds rich console.log
output (incl. ARENA_INIT, WR_LAUNCH, WR_DUR), while NTFY dispatch
acknowledgements ("ntfy sent [200]: ...") land in trader_error.log. The
WR_STALL NTFY message body is also surfaced in trader.log via the helper's
console.log path, so either sink catches the regex.

Usage (background nohup; Captain runs after launchctl kickstart):

    nohup venv/bin/python3 scripts/watch_war_room_layer1_observation.py \\
          > "logs/wr_layer1_watch_$(date +%Y%m%d_%H%M).log" 2>&1 &

Status: SCOPE ONLY (HM-WAR-ROOM-LATENCY 2026-05-15 evening). Not committed
to the Layer 1 PR diff; lives in working tree until Captain decides where to
land it (one-off harness vs reusable observability tool).
"""
from __future__ import annotations

import re
import sys
import time
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

TRADER_LOG = _PROJECT_ROOT / "logs" / "trader.log"
ERROR_LOG = _PROJECT_ROOT / "logs" / "trader_error.log"
WINDOW_SEC = 600       # 10-minute total observation cap
POLL_SEC = 5           # tail-poll cadence
GRACE_SEC = 60         # wait this long after positive events before early-exit

PATTERNS: dict[str, re.Pattern[str]] = {
    "ARENA_INIT": re.compile(r"\[STARTUP\] Arena initialized eagerly"),
    "WR_LAUNCH":  re.compile(r"War Room: launching cycle"),
    "WR_DUR":     re.compile(r"\[WR-DUR\]"),
    "WR_STALL":   re.compile(r"\[WR-STALL\]"),
}


def _hms() -> str:
    return time.strftime("%H:%M:%S")


def _snapshot_offset(path: Path) -> int:
    try:
        return path.stat().st_size if path.exists() else 0
    except OSError:
        return 0


def _read_new(path: Path, offset: int) -> tuple[str, int]:
    """Return (new_content_since_offset, new_offset). Robust to rotation."""
    if not path.exists():
        return "", offset
    try:
        sz = path.stat().st_size
    except OSError:
        return "", offset
    if sz < offset:
        # log truncated/rotated — restart from beginning
        offset = 0
    if sz == offset:
        return "", offset
    try:
        with path.open("rb") as f:
            f.seek(offset)
            chunk = f.read(sz - offset)
        return chunk.decode("utf-8", errors="replace"), sz
    except OSError:
        return "", offset


def _line_containing(text: str, span_start: int, span_end: int) -> str:
    """Extract the full line containing the matched span."""
    start = text.rfind("\n", 0, span_start) + 1
    end = text.find("\n", span_end)
    if end == -1:
        end = len(text)
    return text[start:end].strip()


def main() -> int:
    start_ts = time.time()
    print(f"[{_hms()}] WR-Layer1-watch: starting {WINDOW_SEC}s window "
          f"({time.strftime('%Y-%m-%d %H:%M:%S %Z')})")
    print(f"[{_hms()}] watching: {TRADER_LOG} + {ERROR_LOG}")

    t_off = _snapshot_offset(TRADER_LOG)
    e_off = _snapshot_offset(ERROR_LOG)
    first_hit: dict[str, tuple[int, str]] = {}

    while True:
        elapsed = int(time.time() - start_ts)
        if elapsed >= WINDOW_SEC:
            break

        t_new, t_off = _read_new(TRADER_LOG, t_off)
        e_new, e_off = _read_new(ERROR_LOG, e_off)
        combined = t_new + "\n" + e_new

        for label, regex in PATTERNS.items():
            if label in first_hit:
                continue
            m = regex.search(combined)
            if m:
                line = _line_containing(combined, m.start(), m.end())
                first_hit[label] = (elapsed, line)
                print(f"[{_hms()}] FIRST {label} @ +{elapsed}s: {line[:200]}")

        positives_seen = all(k in first_hit for k in ("ARENA_INIT", "WR_LAUNCH", "WR_DUR"))
        if positives_seen and elapsed >= GRACE_SEC:
            break

        time.sleep(POLL_SEC)

    print("")
    print("─── WR-Layer1-watch summary ───────────────────────────────────")
    for label in ("ARENA_INIT", "WR_LAUNCH", "WR_DUR", "WR_STALL"):
        if label in first_hit:
            sec, line = first_hit[label]
            print(f"  ✓ {label}: +{sec}s | {line[:200]}")
        elif label == "WR_STALL":
            print(f"  ✓ {label}: NONE (healthy — cycle stayed under 10-min threshold)")
        else:
            print(f"  ✗ {label}: NOT SEEN within {WINDOW_SEC}s window")

    stall_observed = "WR_STALL" in first_hit
    missing = [k for k in ("ARENA_INIT", "WR_LAUNCH", "WR_DUR") if k not in first_hit]

    if stall_observed:
        headline = "WR-Layer1 watch: STALL OBSERVED"
        level_name = "WARNING"
    elif missing:
        headline = f"WR-Layer1 watch: missing events ({', '.join(missing)})"
        level_name = "WARNING"
    else:
        headline = "WR-Layer1 watch: all expected events seen, no stall"
        level_name = "INFO"

    body_lines = [headline]
    for label in ("ARENA_INIT", "WR_LAUNCH", "WR_DUR", "WR_STALL"):
        if label in first_hit:
            sec, _ = first_hit[label]
            body_lines.append(f"  {label}: +{sec}s")
        else:
            body_lines.append(f"  {label}: " + ("NONE-OK" if label == "WR_STALL" else "MISSING"))
    message = "\n".join(body_lines)

    try:
        from engine.alert_channels import AlertLevel, send_alert
        level = AlertLevel.WARNING if level_name == "WARNING" else AlertLevel.INFO
        send_alert(
            message=message,
            level=level,
            alert_type="wr_layer1_watch_summary",
            bypass_rate_limit=True,
        )
        print(f"[{_hms()}] NTFY dispatched ({level_name})")
    except Exception as e:
        print(f"[{_hms()}] NTFY dispatch failed: {type(e).__name__}: {e!r}")

    print(f"[{_hms()}] WR-Layer1-watch: done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
