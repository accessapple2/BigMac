#!/usr/bin/env python3
"""HM-WEEKLY-DIGEST-2026-07-11 — XO-DEPARTURE-HARDENING Phase 3 item 8.

Sunday digest: sweep summary + tuning results + audition clocks
(suspension-aware) + 30-day spend, one ntfy push. Deterministic only (SQL +
JSON file reads), no LLM calls -- per the departure-hardening constraint
that unattended automation must never be an LLM session (same posture as
scripts/eod_report.py, which this script mirrors in structure).

Cron: Sunday, after both the weekly tuning crew (main.py's
`schedule.every(30).minutes.do(run_weekly_tuning)`, fires ~21:00-21:30 MST)
and the clean-window sweep (`10 22 * * 0`,
fleet_realism_sweep_clean_window.py) have had time to finish. Recommended:
`0 23 * * 0` (23:00 MST Sunday) -- not installed yet, propose-first per
this repo's Workflow doctrine.

Run manually: .venv/bin/python3 scripts/weekly_digest.py --dry-run
"""
from __future__ import annotations

import glob
import json
import sqlite3
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DB = str(ROOT / "data" / "trader.db")


def _conn():
    c = sqlite3.connect(DB, timeout=10)
    c.row_factory = sqlite3.Row
    return c


def sweep_summary() -> dict | None:
    """Latest clean-window fleet-realism sweep JSON
    (reports/fleet_realism_sweep_clean_*.json), excluding any file with an
    INCOMPLETE marker in its name -- a partial/test run, not a real weekly
    result. Returns None if no real report exists yet."""
    pattern = str(ROOT / "reports" / "fleet_realism_sweep_clean_*.json")
    candidates = [p for p in glob.glob(pattern) if "INCOMPLETE" not in Path(p).name]
    if not candidates:
        return None
    latest = max(candidates, key=lambda p: Path(p).stat().st_mtime)
    with open(latest) as f:
        data = json.load(f)

    agents = data.get("agents", [])
    scored = [a for a in agents if "guarded" in a]
    if not scored:
        return {
            "file": Path(latest).name, "generated": data.get("generated"),
            "agents_scored": 0, "agents_no_data": len(agents),
            "total_guarded_pnl": 0.0, "total_guarded_trades": 0,
            "top": None, "top_pnl": None, "bottom": None, "bottom_pnl": None,
        }

    ranked = sorted(scored, key=lambda a: a["guarded"]["total_pnl"], reverse=True)
    return {
        "file": Path(latest).name,
        "generated": data.get("generated"),
        "agents_scored": len(scored),
        "agents_no_data": len(agents) - len(scored),
        "total_guarded_pnl": round(sum(a["guarded"]["total_pnl"] for a in scored), 2),
        "total_guarded_trades": sum(a["guarded"]["trades"] for a in scored),
        "top": ranked[0]["player_id"],
        "top_pnl": ranked[0]["guarded"]["total_pnl"],
        "bottom": ranked[-1]["player_id"],
        "bottom_pnl": ranked[-1]["guarded"]["total_pnl"],
    }


def tuning_results(conn) -> dict:
    """Model scoring/adjustment activity from the most recent weekly
    tuning-crew run (main.py schedules it Sunday ~21:00-21:30 MST). Reads
    back what the crew already persisted -- never re-invokes it (it makes
    LLM calls, which unattended automation in this program must not do)."""
    scores = conn.execute(
        "SELECT COUNT(*) AS n, ROUND(AVG(overall_score), 3) AS avg_score "
        "FROM model_scores WHERE created_at >= datetime('now', '-8 days')"
    ).fetchone()
    adjustments = conn.execute(
        "SELECT COUNT(*) AS n FROM model_adjustments "
        "WHERE created_at >= datetime('now', '-8 days')"
    ).fetchone()
    return {
        "models_scored": scores["n"] or 0,
        "avg_score": scores["avg_score"],
        "adjustments_saved": adjustments["n"] or 0,
    }


def spend_30d(conn) -> dict:
    """30-day API spend by id, regardless of ai_players membership.
    HM-SHADOW-PIPELINE-COST-AUDIT found real paid spend (~$50/30d) hiding
    entirely outside the roster-scoped leaderboard -- this section exists
    specifically so that blind spot can't recur silently, by always
    surfacing anything spending money whether or not it's a roster seat."""
    rows = conn.execute(
        "SELECT player_id, COUNT(*) AS calls, ROUND(SUM(cost_usd), 2) AS cost "
        "FROM api_costs WHERE timestamp >= datetime('now', '-30 days') "
        "GROUP BY player_id HAVING cost > 0 ORDER BY cost DESC"
    ).fetchall()
    total = round(sum(r["cost"] for r in rows), 2)
    roster_ids = {r["id"] for r in conn.execute("SELECT id FROM ai_players").fetchall()}
    off_roster = [dict(r) for r in rows if r["player_id"] not in roster_ids]
    return {
        "total_30d": total,
        "top": [dict(r) for r in rows[:5]],
        "off_roster": off_roster,
    }


def build_digest(conn) -> dict:
    from engine.crew.audition_tracking import track_incumbent_auditions

    return {
        "week_of": date.today().isoformat(),
        "sweep": sweep_summary(),
        "tuning": tuning_results(conn),
        "auditions": track_incumbent_auditions(conn),
        "spend": spend_30d(conn),
    }


def format_digest(d: dict) -> str:
    lines = [f"Weekly Digest — week of {d['week_of']}"]

    sw = d["sweep"]
    if sw:
        lines.append(
            f"Sweep ({sw['file']}): {sw['agents_scored']} scored "
            f"({sw['agents_no_data']} no-data), guarded P&L "
            f"${sw['total_guarded_pnl']:,.2f} across {sw['total_guarded_trades']} trades"
        )
        if sw["top"]:
            lines.append(
                f"  Top: {sw['top']} (${sw['top_pnl']:,.2f})  "
                f"Bottom: {sw['bottom']} (${sw['bottom_pnl']:,.2f})"
            )
    else:
        lines.append("Sweep: no report found")

    t = d["tuning"]
    lines.append(
        f"Tuning: {t['models_scored']} models scored "
        f"(avg {t['avg_score']}), {t['adjustments_saved']} adjustments saved"
    )

    for a in d["auditions"]:
        if a.get("suspended"):
            lines.append(
                f"Audition {a['player_id']}: SUSPENDED — "
                f"{a['clean_guarded_trades']}/{a['target']} broker-executed"
            )
        else:
            lines.append(
                f"Audition {a['player_id']}: {a['clean_guarded_trades']}/{a['target']} "
                f"({a.get('days_remaining', '?')}d left)"
            )

    sp = d["spend"]
    lines.append(f"30d spend: ${sp['total_30d']:,.2f}")
    if sp["off_roster"]:
        off_str = ", ".join(f"{r['player_id']}=${r['cost']:.2f}" for r in sp["off_roster"][:5])
        lines.append(f"  off-roster (not in ai_players): {off_str}")

    return "\n".join(lines)


def main(dry_run: bool = False) -> int:
    conn = _conn()
    try:
        digest = build_digest(conn)
        body = format_digest(digest)
        print(body)
        if dry_run:
            print("\n[DRY RUN] skipping ntfy push")
            return 0
        from engine.alert_channels import send_alert, AlertLevel

        send_alert(
            message=body,
            level=AlertLevel.INFO,
            alert_type="weekly_digest",
            title=f"Weekly Digest — {digest['week_of']}",
            audience="admin",
            rate_limit_secs=3600 * 24,  # cron fires once/week; guards manual re-runs same day
        )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main(dry_run="--dry-run" in sys.argv))
