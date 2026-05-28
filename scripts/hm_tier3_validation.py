#!/usr/bin/env python3
"""HM-TIER3-SIGNAL-DROP validation — morning report.

Runs after the 2:00 AM MT scan window opens. Queries the signals table for
the 10 target agents and reports who wrote (PASS) vs who is still silent
(FAIL) since 2:00 AM MT today. Claude-independent: writes a markdown report
to logs/ so it survives session death / reboot.

Context: during market-closed hours (6PM-2AM MT) the scan loop skips by
design, so this MUST run during the open window. The real question is
whether the 9 LLM agents — silent ~3 weeks due to Ollie Box inference
exceeding the scan budget — write once scans actually execute agents.
deepseek-7b-grok4 is the deterministic control (should always write).
"""
import sqlite3
import datetime
import sys
from pathlib import Path

DB = Path.home() / "autonomous-trader" / "data" / "trader.db"
LOG_DIR = Path.home() / "autonomous-trader" / "logs"

# Deterministic control — should write regardless of Ollie Box latency.
CONTROL = ["deepseek-7b-grok4"]
# The 9 active-but-silent LLM agents (HM-TIER3-SIGNAL-DROP investigation).
LLM_AGENTS = [
    "cto-grok42", "energy-arnold", "ollama-coder", "ollama-deepseek",
    "ollama-kimi", "ollama-plutus", "ollama-qwen3", "options-sosnoff",
    "qwen3-8b-flash",
]
TARGETS = CONTROL + LLM_AGENTS


def most_recent_2am_mt() -> str:
    """Return the most recent 2:00 AM Arizona-time boundary as a UTC SQL string.

    Arizona = UTC-7, no DST. The signals.created_at column is UTC. We compute
    'today's 2AM MT' (or yesterday's if it's currently before 2AM MT) and
    convert to a UTC 'YYYY-MM-DD HH:MM:SS' string for the WHERE clause.
    """
    AZ = datetime.timezone(datetime.timedelta(hours=-7))
    now_az = datetime.datetime.now(AZ)
    boundary_az = now_az.replace(hour=2, minute=0, second=0, microsecond=0)
    if now_az < boundary_az:
        boundary_az -= datetime.timedelta(days=1)
    boundary_utc = boundary_az.astimezone(datetime.timezone.utc)
    return boundary_utc.strftime("%Y-%m-%d %H:%M:%S")


def main() -> int:
    since_utc = most_recent_2am_mt()
    conn = sqlite3.connect(str(DB), timeout=15)
    conn.row_factory = sqlite3.Row

    rows = {r["player_id"]: r for r in conn.execute(
        "SELECT player_id, COUNT(*) AS n, MAX(created_at) AS last "
        "FROM signals WHERE created_at >= ? AND player_id IN ({}) "
        "GROUP BY player_id".format(",".join("?" * len(TARGETS))),
        [since_utc, *TARGETS],
    ).fetchall()}

    # Sanity: did ANY agent (fleet-wide) write since 2AM? Distinguishes
    # "scan ran, these agents silent" from "scan never ran / trader down".
    fleet_total = conn.execute(
        "SELECT COUNT(*) FROM signals WHERE created_at >= ?", (since_utc,)
    ).fetchone()[0]
    fleet_writers = conn.execute(
        "SELECT COUNT(DISTINCT player_id) FROM signals WHERE created_at >= ?",
        (since_utc,)
    ).fetchone()[0]
    conn.close()

    now_az = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=-7)))
    lines = []
    lines.append(f"# HM-TIER3-SIGNAL-DROP validation — {now_az.strftime('%Y-%m-%d %H:%M MT')}")
    lines.append("")
    lines.append(f"Window: signals written since **{since_utc} UTC** (= 2:00 AM MT).")
    lines.append(f"Fleet-wide since 2AM: **{fleet_total} signals** from **{fleet_writers} distinct agents**.")
    lines.append("")
    if fleet_total == 0:
        lines.append("> ⚠️ ZERO fleet-wide signals since 2AM. Either the scan window")
        lines.append("> didn't open (trader down?) or scans aren't executing agents.")
        lines.append("> Check `pgrep -f main.py` and the trader.log for 'Market scan triggered'.")
        lines.append("")

    def row_line(pid: str) -> str:
        r = rows.get(pid)
        if r and r["n"] > 0:
            return f"| {pid} | ✅ PASS | {r['n']} | {r['last']} |"
        return f"| {pid} | ❌ silent | 0 | — |"

    lines.append("## Control (deterministic — must write)")
    lines.append("| agent | result | signals | last write (UTC) |")
    lines.append("|---|---|---:|---|")
    for pid in CONTROL:
        lines.append(row_line(pid))
    lines.append("")
    lines.append("## The 9 LLM agents (the actual HM-TIER3-SIGNAL-DROP question)")
    lines.append("| agent | result | signals | last write (UTC) |")
    lines.append("|---|---|---:|---|")
    for pid in LLM_AGENTS:
        lines.append(row_line(pid))
    lines.append("")

    passed = [p for p in LLM_AGENTS if rows.get(p) and rows[p]["n"] > 0]
    silent = [p for p in LLM_AGENTS if p not in passed]
    ctrl_ok = bool(rows.get("deepseek-7b-grok4") and rows["deepseek-7b-grok4"]["n"] > 0)

    lines.append("## Verdict")
    lines.append(f"- Control deepseek-7b-grok4: {'✅ writing' if ctrl_ok else '❌ SILENT (control failed — scan path broken, not just LLM latency)'}")
    lines.append(f"- LLM agents writing: **{len(passed)}/9** {passed if passed else ''}")
    lines.append(f"- LLM agents still silent: **{len(silent)}/9** {silent if silent else ''}")
    lines.append("")
    if ctrl_ok and not passed:
        lines.append("→ **HM-TIER3-SIGNAL-DROP CONFIRMED STILL OPEN**: deterministic path works,")
        lines.append("  all 9 LLM agents silent. Root cause (Ollie Box inference > scan budget)")
        lines.append("  unresolved. The reverted scan-timeout fix needs a queue/keep_alive-layer")
        lines.append("  replacement, not a loop-level timeout.")
    elif passed and silent:
        lines.append(f"→ **PARTIAL**: {len(passed)} LLM agents recovered, {len(silent)} still silent.")
        lines.append("  Investigate the slow ones individually (per-symbol inference wall time).")
    elif len(passed) == 9:
        lines.append("→ **RESOLVED**: all 9 LLM agents writing. HM-TIER3-SIGNAL-DROP no longer reproduces.")
    elif not ctrl_ok:
        lines.append("→ **CONTROL FAILED**: even deepseek-7b-grok4 is silent. This is NOT the LLM-latency")
        lines.append("  issue — the scan path itself isn't executing/persisting. Check trader is up")
        lines.append("  and scans fired in the open window.")

    report = "\n".join(lines)
    LOG_DIR.mkdir(exist_ok=True)
    out = LOG_DIR / f"HM-TIER3-VALIDATION_{now_az.strftime('%Y-%m-%d')}.md"
    out.write_text(report + "\n")
    print(report)
    print(f"\n[written to {out}]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
