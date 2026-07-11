#!/usr/bin/env python3
"""PROPOSED -- HM-DEPARTURE-HARDENING Phase 1 item 4a (Door-1 G1-G4 gate).
Not yet in crontab; standalone until approved.

Pre-committed gate criteria: OLLIETRADES_KILL_GATE.md (do not edit that
file's criteria -- G1-G4 are locked as of 2026-06-19, "gates are pass/fail,
not negotiated"). This script only COMPUTES and PUSHES the verdict via
ntfy -- it never halts, pauses, or modifies anything. The decision stays
the Admiral's, made from the pushed numbers.

Window: DAY 0 ~2026-06-24, DAY 30 verdict ~2026-07-24, MSI return wall
~2026-08-18.

Before DAY 30, running this produces a PREVIEW (numbers as of today, not
a final verdict -- the window isn't closed yet). On/after DAY 30 it
produces a VERDICT push. Same computation either way; only the framing
differs, so there's no reason not to run this daily starting now for
early visibility.

Proposed schedule: cron daily during the window, e.g. `0 14 * * 1-5`
(same slot as eod_report.py) or its own slot -- Admiral's call.

G4 (vs. parallel benchmark) is reported INCONCLUSIVE by design: no
parallel-benchmark tracking (JEPI/JEPQ/NANC/KRUZ) exists anywhere in this
codebase as a scored player or series (checked: no ai_players row, no
tracking module). The kill-gate doc itself allows this ("If benchmark is
untested: G4 is inconclusive, not a fail -- note it") -- this script does
exactly that rather than inventing a benchmark comparison that isn't real.

G1's "% of starting equity" cannot be recomputed here either -- the DAY-0
baseline was a manual screenshot per the kill-gate doc's own window table,
not a value stored anywhere queryable. This script reports the absolute
dollar figure against the doc's own pre-committed ~$500 approximation
(the parenthetical in "G1 -- Money: ... >= +0.5% of starting equity
(~+$500)") rather than silently assuming a baseline number that was never
actually pinned down in code.

G3 is scoped to structure='csp' (the CSP-wheel-isolation branch this
whole gate exists for) -- the kill-gate doc's own G3 SQL note omits an
explicit structure filter, which reads as a documentation gap given the
rest of the gate is CSP-specific, not an instruction to count every
options structure. Flagging this interpretation explicitly in the push
so it's visible, not silently assumed.
"""
from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DB_PATH = REPO / "data" / "trader.db"
BASE_URL = "http://localhost:8080"
NTFY_TOPIC = "ollietrades-admin"
NTFY_URL = f"https://ntfy.sh/{NTFY_TOPIC}"

DAY_0 = date(2026, 6, 24)
DAY_30_VERDICT = date(2026, 7, 24)
RETURN_WALL = date(2026, 8, 18)

G1_DOLLAR_FLOOR = 500.0  # doc's own "(~+$500)" approximation of +0.5% starting equity
G3_TAIL_FRACTION = 0.20


def _http_get_json(path: str) -> dict:
    try:
        out = subprocess.run(
            ["curl", "-s", "--max-time", "10", f"{BASE_URL}{path}"],
            capture_output=True, text=True, timeout=15,
        )
        return json.loads(out.stdout)
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


def compute_g1() -> dict:
    """Money: CSP net realized P&L + open-position MTM >= +$500 (doc approx)."""
    pnl_data = _http_get_json("/api/strategy/pnl")
    bucket = (pnl_data.get("buckets") or {}).get("csp_wheel", {})
    realized = float(bucket.get("pnl", 0.0))

    conn = sqlite3.connect(str(DB_PATH))
    try:
        row = conn.execute(
            "SELECT COALESCE(SUM(mtm_intrinsic), 0), COUNT(*) "
            "FROM options_trades WHERE status='open' AND structure='csp'"
        ).fetchone()
        mtm_intrinsic, open_positions = float(row[0] or 0.0), int(row[1] or 0)
    finally:
        conn.close()

    total = realized + mtm_intrinsic
    passed = total >= G1_DOLLAR_FLOOR
    return {
        "gate": "G1_MONEY", "passed": passed,
        "realized_pnl": round(realized, 2),
        "open_mtm_intrinsic": round(mtm_intrinsic, 2),
        "open_positions": open_positions,
        "total": round(total, 2),
        "threshold": G1_DOLLAR_FLOOR,
        "note": ("realized+MTM read from /api/strategy/pnl csp_wheel bucket "
                 "(era-filtered, real-quotes-only per HM-P&L-RECONCILIATION) "
                 "+ options_trades open CSP mtm_intrinsic sum. Threshold is "
                 "the doc's own ~$500 approximation, not a recomputed % of a "
                 "stored DAY-0 baseline (none exists queryable)."),
    }


def _max_drawdown_pct(series: list[float]) -> float:
    if not series:
        return 0.0
    peak = series[0]
    max_dd = 0.0
    for v in series:
        if v > peak:
            peak = v
        if peak > 0:
            dd = (peak - v) / peak * 100.0
            max_dd = max(max_dd, dd)
    return round(max_dd, 2)


def compute_g2() -> dict:
    """Risk: account max DD over the window < SPY max DD (or < 3% if SPY flat)."""
    curve = _http_get_json("/api/account/equity-curve")
    dates = curve.get("dates") or []
    account = curve.get("account") or []
    spy = curve.get("spy") or []

    day0_str = DAY_0.isoformat()
    window_idx = [i for i, d in enumerate(dates) if d >= day0_str]
    if not window_idx:
        return {"gate": "G2_RISK", "passed": None,
                "note": "no equity-curve data on/after DAY 0 yet"}

    acct_window = [account[i] for i in window_idx]
    spy_window = [spy[i] for i in window_idx]
    # Renormalize each sub-series to 100 at the window's own first point --
    # the raw endpoint normalizes to Season 6 start (2026-04-24), not DAY 0.
    a0, s0 = acct_window[0], spy_window[0]
    acct_renorm = [v / a0 * 100 if a0 else v for v in acct_window]
    spy_renorm = [v / s0 * 100 if s0 else v for v in spy_window]

    acct_dd = _max_drawdown_pct(acct_renorm)
    spy_dd = _max_drawdown_pct(spy_renorm)
    spy_return = spy_renorm[-1] - 100.0 if spy_renorm else 0.0
    spy_flat = abs(spy_return) < 1.0

    threshold = 3.0 if spy_flat else spy_dd
    passed = acct_dd < threshold
    return {
        "gate": "G2_RISK", "passed": passed,
        "account_max_dd_pct": acct_dd,
        "spy_max_dd_pct": spy_dd,
        "spy_return_pct": round(spy_return, 2),
        "spy_flat": spy_flat,
        "threshold_pct": threshold,
        "window_days": len(window_idx),
    }


def compute_g3() -> dict:
    """Tail: no single closed CSP loss > 20% of window's total premium collected."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT entry_credit_debit, pnl FROM options_trades "
            "WHERE status='closed' AND structure='csp' AND entry_date >= ?",
            (DAY_0.isoformat(),),
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return {"gate": "G3_TAIL", "passed": True,
                "note": "no closed CSP trades in the window yet -- passes by default per doc"}

    premium_collected = sum(r["entry_credit_debit"] or 0.0 for r in rows if (r["entry_credit_debit"] or 0) > 0)
    worst_loss = min((r["pnl"] or 0.0) for r in rows)
    ratio = abs(worst_loss) / premium_collected if premium_collected > 0 else 0.0
    passed = ratio <= G3_TAIL_FRACTION
    return {
        "gate": "G3_TAIL", "passed": passed,
        "closed_trades": len(rows),
        "premium_collected": round(premium_collected, 2),
        "worst_single_loss": round(worst_loss, 2),
        "worst_loss_ratio": round(ratio, 4),
        "threshold_ratio": G3_TAIL_FRACTION,
    }


def compute_g4() -> dict:
    """vs Paid: no parallel-benchmark tracking exists -- inconclusive by doc's own allowance."""
    return {"gate": "G4_VS_PAID", "passed": None,
            "note": ("inconclusive: no JEPI/JEPQ/NANC/KRUZ parallel-benchmark "
                     "tracking found anywhere in the codebase (no ai_players "
                     "row, no scoring module). Doc allows this explicitly.")}


def build_verdict() -> dict:
    g1, g2, g3, g4 = compute_g1(), compute_g2(), compute_g3(), compute_g4()
    today = date.today()
    is_final = today >= DAY_30_VERDICT
    keep_eligible = bool(g1["passed"] and g2.get("passed") and g3["passed"])
    return {
        "as_of": today.isoformat(),
        "is_final_verdict": is_final,
        "day_30_date": DAY_30_VERDICT.isoformat(),
        "return_wall": RETURN_WALL.isoformat(),
        "gates": [g1, g2, g3, g4],
        "keep_eligible_g1_g2_g3": keep_eligible,
    }


def format_push(v: dict) -> str:
    label = "DOOR-1 VERDICT" if v["is_final_verdict"] else "Door-1 preview"
    lines = [f"{label} ({v['as_of']}, DAY-30={v['day_30_date']})"]
    for g in v["gates"]:
        p = g["passed"]
        mark = "PASS" if p is True else ("FAIL" if p is False else "N/A")
        lines.append(f"{g['gate']}: {mark}")
        if g["gate"] == "G1_MONEY" and p is not None:
            lines.append(f"  realized ${g['realized_pnl']:,.2f} + MTM ${g['open_mtm_intrinsic']:,.2f} "
                         f"= ${g['total']:,.2f} (need >= ${g['threshold']:,.0f})")
        if g["gate"] == "G2_RISK" and p is not None:
            lines.append(f"  acct DD {g['account_max_dd_pct']}% vs SPY DD {g['spy_max_dd_pct']}% "
                         f"(threshold {g['threshold_pct']}%, SPY flat={g['spy_flat']})")
        if g["gate"] == "G3_TAIL" and "worst_loss_ratio" in g:
            lines.append(f"  worst loss ${g['worst_single_loss']:,.2f} / "
                         f"${g['premium_collected']:,.2f} premium = {g['worst_loss_ratio']:.1%} "
                         f"(threshold {g['threshold_ratio']:.0%})")
        if g.get("note"):
            lines.append(f"  {g['note']}")
    lines.append(f"KEEP-eligible on G1+G2+G3: {v['keep_eligible_g1_g2_g3']} (G4 informational only)")
    return "\n".join(lines)


def ntfy_post(message: str, priority: str = "default", title: str = "") -> None:
    try:
        subprocess.run(
            ["curl", "-s", "-H", f"Priority: {priority}"] +
            (["-H", f"Title: {title}"] if title else []) +
            ["-d", message, NTFY_URL],
            capture_output=True, timeout=15,
        )
    except Exception:
        pass


def main() -> int:
    verdict = build_verdict()
    message = format_push(verdict)
    print(message)

    title = "Door-1 KILL GATE VERDICT" if verdict["is_final_verdict"] else "Door-1 gate preview"
    priority = "urgent" if verdict["is_final_verdict"] else "default"
    ntfy_post(message, priority=priority, title=title)
    return 0


if __name__ == "__main__":
    sys.exit(main())
