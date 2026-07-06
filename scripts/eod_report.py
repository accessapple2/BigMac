#!/usr/bin/env python3
"""HM-EOD-REPORT-2026-07-05 — deterministic daily EOD report.

XO-DEPARTURE-HARDENING Phase 1 item 2: the Admiral's daily heartbeat once the
system runs monitoring-only (phone push, no hands-on). Pure SQL + log
parsing, no LLM calls — unattended automation must be deterministic per the
departure-hardening constraint.

Cron: 0 14 * * 1-5 (2:00 PM MST, weekdays only) — one hour after market close
(1:00 PM MST / 4:00 PM ET), positions flat, most settlement done. Admiral's
rationale: same-afternoon awareness beats a bedtime read — a problem
surfaced at 2 PM leaves a whole afternoon to act remotely; one surfaced at
20:30 (this repo's existing scripts/daily_report.py, a different script —
10 PM, writes drafts/DAILY_REPORT_<date>.md + daily_ledger.csv, no ntfy push,
predates this one and serves a different archival purpose) waits for
morning.

Self-correcting delta, not a second push: if an evening process (backup-slot
jobs, late closeouts) revises today's guarded P&L after this report fires,
the delta surfaces as a one-line note at the top of TOMORROW's report
("yesterday finalized: $X, was reported $Y") — computed by comparing
eod_report_log's persisted figure against a fresh recompute for that same
date. Zero extra alert noise. Uses the most recent PRIOR row (not literally
"yesterday"), so weekends/holidays don't break the comparison.

Run manually: .venv/bin/python3 scripts/eod_report.py --dry-run
  (prints the report, skips the ntfy push and the eod_report_log write)
"""
from __future__ import annotations

import re
import sqlite3
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DB = str(ROOT / "data" / "trader.db")
LOG_PATHS = [str(ROOT / "logs" / "trader.log"), str(ROOT / "logs" / "trader_error.log")]

# HM-EOD-REPORT known false-positive allowlist for the genuine-error count.
# NOT claimed complete — add to this as new false positives are found, same
# "verify before claiming" discipline as everywhere else in this repo. Each
# entry is a literal substring match against the full log line.
KNOWN_FALSE_POSITIVE_PATTERNS = (
    # [Kirk] advisory complete: 0 positions, 0 critical, 0 high, 0 medium, 0 low
    # -- "critical"/"high"/"medium"/"low" here are zero-count labels, not a
    # real severity report. Verified during HM-ROSTER-RATIONALIZE 2026-07-05.
    "critical, 0 high, 0 medium, 0 low",
)

_ERROR_PATTERN = re.compile(r"error|exception|traceback|critical|fatal", re.IGNORECASE)


def _conn():
    c = sqlite3.connect(DB, timeout=10)
    c.row_factory = sqlite3.Row
    return c


def signal_trade_conversion(conn, report_date: str) -> dict:
    """Today's signals-table row count vs today's trades-table row count,
    fleet-wide. Conversion is None (not 0) when there were no signals at all
    (e.g. a market holiday), so the report reads "no signals today" instead
    of a misleading 0%."""
    signals = conn.execute(
        "SELECT COUNT(*) FROM signals WHERE created_at >= ? AND created_at < date(?, '+1 day')",
        (report_date, report_date),
    ).fetchone()[0]
    trades = conn.execute(
        "SELECT COUNT(*) FROM trades WHERE executed_at >= ? AND executed_at < date(?, '+1 day')",
        (report_date, report_date),
    ).fetchone()[0]
    conversion_pct = round(100.0 * trades / signals, 1) if signals else None
    return {"signals": signals, "trades": trades, "conversion_pct": conversion_pct}


def guarded_pnl_for_date(conn, report_date: str) -> dict:
    """Clean realized P&L for the single calendar day `report_date`, fleet
    total + per-agent. Uses an explicit date range rather than
    engine.trades_filter.fleet_realized_pnl's since_days=N (relative to
    'now', so it can't recompute a specific PAST date — needed by
    prior_day_delta below) — but reuses that module's CLEAN_TRADES_WHERE so
    the cleanliness definition stays the single source of truth."""
    from engine.trades_filter import CLEAN_TRADES_WHERE

    rows = conn.execute(
        f"""
        SELECT player_id, COUNT(*) AS trade_count, ROUND(SUM(realized_pnl), 2) AS total_pnl
        FROM trades
        WHERE action IN ('SELL', 'COVER') AND realized_pnl IS NOT NULL
          AND executed_at >= ? AND executed_at < date(?, '+1 day')
          AND {CLEAN_TRADES_WHERE}
        GROUP BY player_id
        """,
        (report_date, report_date),
    ).fetchall()
    per_agent = [dict(r) for r in rows]
    total = round(sum((r["total_pnl"] or 0) for r in per_agent), 2)
    return {"fleet_total": total, "per_agent": per_agent}


def csp_wheel_scan_rollup(conn, report_date: str) -> dict:
    """Today's csp_wheel_scan_log grouped by outcome. Empty dict (not an
    error) if the table isn't present yet this boot — mirrors
    engine.crew.audition_tracking.wheel_scan_diagnosis's honest-gap handling."""
    try:
        rows = conn.execute(
            "SELECT outcome, COUNT(*) AS n FROM csp_wheel_scan_log "
            "WHERE scanned_at >= ? AND scanned_at < date(?, '+1 day') GROUP BY outcome",
            (report_date, report_date),
        ).fetchall()
        return {r["outcome"]: r["n"] for r in rows}
    except sqlite3.OperationalError:
        return {}


def genuine_error_count(log_paths: list, report_date: str) -> int:
    """Counts real ERROR/CRITICAL/Exception/Traceback lines timestamped
    `report_date`, excluding KNOWN_FALSE_POSITIVE_PATTERNS. Known-incomplete
    allowlist — see module docstring; add new false positives here as found
    rather than silently tolerating them."""
    date_prefix = f"[{report_date} "
    count = 0
    for path in log_paths:
        try:
            with open(path, "r", errors="ignore") as f:
                for line in f:
                    if not line.startswith(date_prefix):
                        continue
                    if not _ERROR_PATTERN.search(line):
                        continue
                    if any(fp in line for fp in KNOWN_FALSE_POSITIVE_PATTERNS):
                        continue
                    count += 1
        except FileNotFoundError:
            continue
    return count


def real_alpaca_equity() -> dict | None:
    """HM-EDGE-PROVENANCE ruling item 5 (2026-07-05): the one true number,
    pulled read-only straight from the broker on every report -- the same
    call engine.reconciliation.py already makes daily. Returns None (not a
    fabricated 0) on any failure -- network/API errors must degrade, not
    crash the report or claim a number that wasn't actually read."""
    try:
        from config import APCA_API_KEY_ID, APCA_API_SECRET_KEY
        from alpaca.trading.client import TradingClient

        client = TradingClient(APCA_API_KEY_ID, APCA_API_SECRET_KEY, paper=True)
        acct = client.get_account()
        equity = float(acct.equity)
        last_equity = float(acct.last_equity)
        return {"equity": equity, "day_change": round(equity - last_equity, 2)}
    except Exception:
        return None


def prior_day_delta(conn, report_date: str) -> str | None:
    """Compares the most recent PRIOR eod_report_log row's persisted
    guarded_pnl against a fresh recompute for that same date. Returns a
    one-line delta string if they differ by >= 1 cent, else None."""
    prior = conn.execute(
        "SELECT report_date, guarded_pnl FROM eod_report_log "
        "WHERE report_date < ? ORDER BY report_date DESC LIMIT 1",
        (report_date,),
    ).fetchone()
    if not prior:
        return None
    fresh = guarded_pnl_for_date(conn, prior["report_date"])
    reported = prior["guarded_pnl"] or 0.0
    if abs(fresh["fleet_total"] - reported) < 0.01:
        return None
    return (
        f"yesterday finalized: ${fresh['fleet_total']:,.2f}, "
        f"was reported ${reported:,.2f} ({prior['report_date']})"
    )


def build_report(conn, report_date: str) -> dict:
    from engine.crew.audition_tracking import track_incumbent_auditions

    return {
        "report_date": report_date,
        "real_equity": real_alpaca_equity(),
        "delta_note": prior_day_delta(conn, report_date),
        "conversion": signal_trade_conversion(conn, report_date),
        "guarded_pnl": guarded_pnl_for_date(conn, report_date),
        "auditions": track_incumbent_auditions(conn),
        "wheel_scan": csp_wheel_scan_rollup(conn, report_date),
        "genuine_errors": genuine_error_count(LOG_PATHS, report_date),
    }


def format_report(report: dict) -> str:
    lines = []

    # HM-EDGE-PROVENANCE item 5: the one true number, first line, every day.
    real_eq = report.get("real_equity")
    if real_eq:
        sign = "+" if real_eq["day_change"] >= 0 else ""
        lines.append(f"💰 Real Alpaca equity: ${real_eq['equity']:,.2f} ({sign}${real_eq['day_change']:,.2f} today)")
    else:
        lines.append("💰 Real Alpaca equity: unavailable (broker API unreachable)")

    if report["delta_note"]:
        lines.append(f"⚠️ {report['delta_note']}")

    conv = report["conversion"]
    if conv["signals"]:
        lines.append(f"Signals→Trades: {conv['trades']}/{conv['signals']} ({conv['conversion_pct']}%)")
    else:
        lines.append("Signals→Trades: no signals today")

    lines.append(f"Guarded P&L today (internal, unverified venue): ${report['guarded_pnl']['fleet_total']:,.2f}")

    for a in report["auditions"]:
        if a.get("suspended"):
            lines.append(f"Audition {a['player_id']}: SUSPENDED — {a['clean_guarded_trades']}/{a['target']} "
                         f"broker-executed (pending broker routing)")
        else:
            lines.append(f"Audition {a['player_id']}: {a['clean_guarded_trades']}/{a['target']} "
                         f"({a.get('days_remaining', '?')}d left)")
        diag = a.get("structural_diagnosis")
        if diag and diag.get("captured") and diag.get("total_scans"):
            lines.append(f"  → {diag['diagnosis']}")
        elif diag and diag.get("captured"):
            lines.append(f"  → {diag.get('note', 'no scans recorded')}")

    if report["wheel_scan"]:
        wheel_str = ", ".join(f"{k}={v}" for k, v in sorted(report["wheel_scan"].items()))
        lines.append(f"Wheel scans: {wheel_str}")

    lines.append(f"Genuine errors: {report['genuine_errors']}")
    return "\n".join(lines)


def persist_report(conn, report: dict) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO eod_report_log
           (report_date, signals_count, trades_count, conversion_pct, guarded_pnl, error_count)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            report["report_date"],
            report["conversion"]["signals"],
            report["conversion"]["trades"],
            report["conversion"]["conversion_pct"],
            report["guarded_pnl"]["fleet_total"],
            report["genuine_errors"],
        ),
    )
    conn.commit()


def main(dry_run: bool = False) -> int:
    report_date = date.today().isoformat()
    conn = _conn()
    try:
        report = build_report(conn, report_date)
        body = format_report(report)
        print(body)
        if dry_run:
            print("\n[DRY RUN] skipping ntfy push and eod_report_log write")
            return 0
        from engine.alert_channels import send_alert, AlertLevel

        send_alert(
            message=body,
            level=AlertLevel.INFO,
            alert_type="eod_report",
            title=f"📊 EOD Report — {report_date}",
            audience="admin",
            rate_limit_secs=3600,  # cron only ever fires this once/day; guards manual re-runs
        )
        persist_report(conn, report)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main(dry_run="--dry-run" in sys.argv))
