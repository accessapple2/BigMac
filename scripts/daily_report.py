#!/usr/bin/env python3
"""
Daily report generator — scripts/daily_report.py
Outputs drafts/DAILY_REPORT_<date>.md and appends to drafts/daily_ledger.csv.

Run: .venv/bin/python3 scripts/daily_report.py
Cron: 0 22 * * 1-5
"""
from __future__ import annotations

import csv
import json
import re
import sqlite3
import subprocess
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "data" / "trader.db"
DRAFTS = ROOT / "drafts"
GRADES_FILE = DRAFTS / "grades.json"
LEDGER_FILE = DRAFTS / "daily_ledger.csv"
ERROR_LOG = ROOT / "logs" / "trader_error.log"

LEDGER_HEADER = [
    "date", "clean_days", "n_under10", "n_over10", "n_null",
    "artifact_zeros", "evaluator_ok", "measurement_health", "cost_usd",
    "grade_dashboard", "grade_signals", "grade_execution", "grade_measurement",
    "grade_fleet", "grade_risk", "grade_monitoring", "grade_data",
    "grade_models", "grade_resilience",
]

GATE_DAYS = 20
GATE_N = 400


# ── DB helpers ────────────────────────────────────────────────────────────────

def _conn():
    c = sqlite3.connect(str(DB), timeout=10)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def _is_weekday(d: date) -> bool:
    return d.weekday() < 5  # Mon=0 … Fri=4


# ── Data queries ──────────────────────────────────────────────────────────────

def get_gate_stats(db) -> dict:
    """Bucket counts, clean trading days, artifact-zero count.

    clean_days  = distinct weekday dates in ts (collection dates, Python-side
                  to avoid SQLite strftime unreliability on ISO+offset strings).
    artifact_zeros = same-bar pattern: realized=0.0 AND DATE(expiry) <= DATE(ts).
                  Genuine 0.0 returns (different-day expiry) are NOT artifacts.
    Bucket counts exclude artifact-zeros.
    """
    rows = db.execute(
        "SELECT DATE(ts) as ts_day, fwd_return_1d, fwd_return_1d_realized, "
        "DATE(expiry) as exp_day FROM signal_observations"
    ).fetchall()

    obs_days: set[date] = set()
    n_under10 = n_over10 = n_null = artifact_zeros = 0

    for r in rows:
        # Track distinct weekday collection dates
        try:
            d = date.fromisoformat(r["ts_day"][:10])
            if _is_weekday(d):
                obs_days.add(d)
        except (ValueError, TypeError):
            pass

        realized = r["fwd_return_1d_realized"]
        # Same-bar artifact: realized=0.0 AND expiry <= ts (should have been NULL).
        # Genuine zeros (expiry > ts) are real data, not artifacts.
        if realized == 0.0:
            ts_d = r["ts_day"][:10] if r["ts_day"] else ""
            exp_d = r["exp_day"][:10] if r["exp_day"] else ""
            if not exp_d or exp_d <= ts_d:
                artifact_zeros += 1
                continue  # exclude same-bar artifacts from buckets

        # Exclude any remaining confirmed zeros from buckets
        if realized == 0.0:
            continue

        fwd = r["fwd_return_1d"]
        if fwd is None:
            n_null += 1
        elif fwd < 0.10:
            n_under10 += 1
        else:
            n_over10 += 1

    return {
        "clean_days": len(obs_days),
        "n_under10": n_under10,
        "n_over10": n_over10,
        "n_null": n_null,
        "artifact_zeros": artifact_zeros,
    }


def get_pipeline_vitals(db, today: date) -> dict:
    today_s = today.isoformat()

    obs_today = db.execute(
        "SELECT COUNT(*) FROM signal_observations WHERE DATE(ts) = ?", (today_s,)
    ).fetchone()[0]

    realized_today = db.execute(
        "SELECT COUNT(*) FROM signal_observations "
        "WHERE DATE(evaluated_at) = ? AND fwd_return_1d_realized IS NOT NULL "
        "AND fwd_return_1d_realized != 0.0",
        (today_s,),
    ).fetchone()[0]

    last_eval_str = db.execute(
        "SELECT MAX(evaluated_at) FROM signal_observations WHERE evaluated_at IS NOT NULL"
    ).fetchone()[0]

    evaluator_ok = False
    last_eval_age_hrs = None
    if last_eval_str:
        try:
            # Strip timezone offset for naive parse
            clean = re.sub(r"[+-]\d{2}:\d{2}$", "", last_eval_str.replace("Z", ""))
            last_eval_dt = datetime.fromisoformat(clean)
            age_hrs = (datetime.utcnow() - last_eval_dt).total_seconds() / 3600
            last_eval_age_hrs = age_hrs
            evaluator_ok = age_hrs < 4
        except Exception:
            pass

    return {
        "obs_today": obs_today,
        "realized_today": realized_today,
        "evaluator_ok": evaluator_ok,
        "last_eval_age_hrs": last_eval_age_hrs,
    }


def get_cost_today(db, today: date) -> float:
    row = db.execute(
        "SELECT ROUND(SUM(cost_usd), 4) FROM api_costs "
        "WHERE DATE(timestamp, 'localtime') = ?",
        (today.isoformat(),),
    ).fetchone()
    return row[0] or 0.0


def get_war_room_today(db, today: date) -> int:
    try:
        row = db.execute(
            "SELECT COUNT(*) FROM war_room_debates "
            "WHERE DATE(started_at, 'localtime') = ?",
            (today.isoformat(),),
        ).fetchone()
        return row[0] or 0
    except Exception:
        return 0


def get_regime(db) -> dict:
    try:
        row = db.execute(
            "SELECT regime, cross_days_ago, size_modifier "
            "FROM regime_history ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        return dict(row) if row else {}
    except Exception:
        return {}


# ── Support queries ───────────────────────────────────────────────────────────

def get_git_changes_today() -> list[str]:
    try:
        result = subprocess.run(
            ["git", "log", "--oneline", "--since=midnight"],
            capture_output=True, text=True, cwd=str(ROOT), timeout=10,
        )
        lines = [l.strip() for l in result.stdout.strip().splitlines() if l.strip()]
        return lines[:10]
    except Exception:
        return []


def get_error_summary(today: date) -> str:
    if not ERROR_LOG.exists():
        return "log not found"
    try:
        today_s = today.isoformat()
        with open(ERROR_LOG, errors="replace") as f:
            lines = f.readlines()
        today_lines = [l for l in lines if today_s in l]
        # Real errors: contain ERROR/Exception/Traceback but not scanner-level LRS chatter
        real = [l for l in today_lines
                if re.search(r"ERROR|Exception|Traceback", l)
                and "[LRS]" not in l]
        return f"{len(real)} real / {len(today_lines)} total lines today"
    except Exception as e:
        return f"unable to read ({e})"


# ── Ledger helpers ────────────────────────────────────────────────────────────

def load_last_ledger_row() -> dict | None:
    if not LEDGER_FILE.exists():
        return None
    with open(LEDGER_FILE, newline="") as f:
        reader = csv.DictReader(f)
        last = None
        for row in reader:
            last = row
        return last


def ledger_has_today(today: date) -> bool:
    """Return True if the ledger already has a row for today."""
    last = load_last_ledger_row()
    return last is not None and last.get("date") == today.isoformat()


def append_ledger_row(values: list) -> None:
    needs_header = not LEDGER_FILE.exists() or LEDGER_FILE.stat().st_size == 0
    with open(LEDGER_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        if needs_header:
            writer.writerow(LEDGER_HEADER)
        writer.writerow(values)


# ── Grade helpers ─────────────────────────────────────────────────────────────

def load_grades() -> dict:
    with open(GRADES_FILE) as f:
        return json.load(f)


def grade_delta(curr: str, prior_row: dict | None, key: str) -> str:
    if prior_row is None:
        return "—"
    prev = prior_row.get(f"grade_{key}", "")
    if curr == prev:
        return "—"
    return f"**{curr}** ← was {prev}"


# ── Report assembly ───────────────────────────────────────────────────────────

def _pct_to_gate(clean_days: int, n_under10: int, n_over10: int) -> str:
    days_pct = min(100, round(clean_days / GATE_DAYS * 100))
    min_n = min(n_under10, n_over10)
    n_pct = min(100, round(min_n / GATE_N * 100))
    binding = "days-limited" if days_pct <= n_pct else "n-limited"
    return f"{min(days_pct, n_pct)}% ({binding})"


def generate_report(today: date) -> tuple[str, list]:
    db = _conn()
    grades_data = load_grades()
    prior = load_last_ledger_row()

    gate = get_gate_stats(db)
    vitals = get_pipeline_vitals(db, today)
    cost = get_cost_today(db, today)
    wr_debates = get_war_room_today(db, today)
    regime = get_regime(db)
    db.close()

    git_changes = get_git_changes_today()
    error_summary = get_error_summary(today)

    clean_days = gate["clean_days"]
    n_under10 = gate["n_under10"]
    n_over10 = gate["n_over10"]
    n_null = gate["n_null"]
    artifact_zeros = gate["artifact_zeros"]
    evaluator_ok = vitals["evaluator_ok"]
    measurement_health = "GREEN" if (evaluator_ok and artifact_zeros == 0) else "RED"

    days_remaining = max(0, GATE_DAYS - clean_days)
    # Rough estimate: 1 trading day ≈ 1.4 calendar days
    est_ready_dt = today + timedelta(days=int(days_remaining * 1.4) + 2)
    est_ready = est_ready_dt.strftime("%Y-%m-%d")
    gate_pct = _pct_to_gate(clean_days, n_under10, n_over10)
    min_n = min(n_under10, n_over10)

    eval_age_str = (
        f"{vitals['last_eval_age_hrs']:.1f}h ago"
        if vitals["last_eval_age_hrs"] is not None else "unknown"
    )
    eval_status = "GREEN ✓" if evaluator_ok else "RED ✗"

    g = grades_data["grades"]
    reasons = grades_data.get("reasons", {})

    def card_row(num: int, key: str, name: str) -> str:
        curr = g[key]
        delta = grade_delta(curr, prior, key)
        reason = reasons.get(key, "")
        note = f"{delta} — {reason}" if (delta != "—" and reason) else delta
        return f"| {num} | {name} | **{curr}** | {note} |"

    # ── Section 1: Edge Status ────────────────────────────────────────────────
    s1 = f"""\
## 1. EDGE STATUS

**COLLECTING — not testable.**
Clean trading days: **{clean_days}/{GATE_DAYS}** · min bucket n: **{min_n:,}/{GATE_N}** · est. ready ~{est_ready}

*No edge established. Per-source returns are NOT actionable before the gate.*
"""

    # ── Section 2: Gate Progress ──────────────────────────────────────────────
    def gate_row(label, val, target, met, shortfall=""):
        status = "✓" if met else shortfall
        return f"| {label} | **{val}** | {target} | {status} |"

    s2 = f"""\
## 2. GATE PROGRESS

| Metric | Value | Target | Status |
|--------|-------|--------|--------|
{gate_row('Clean trading days', clean_days, '≥20', clean_days >= 20, f'{days_remaining} remaining')}
{gate_row('n_under10 (proj <10%)', f'{n_under10:,}', '≥400', n_under10 >= 400, f'need {max(0,GATE_N-n_under10):,} more')}
{gate_row('n_over10  (proj ≥10%)', f'{n_over10:,}', '≥400', n_over10 >= 400, f'need {max(0,GATE_N-n_over10):,} more')}
| n_null (no projection) | **{n_null:,}** | — | tracking |
| % to gate | **{gate_pct}** | 100% | — |
"""

    # ── Section 3: Pipeline Vitals ────────────────────────────────────────────
    regime_str = (
        f"{regime['regime']} ({regime.get('cross_days_ago','?')}d · size {regime.get('size_modifier','?')}x)"
        if regime else "unknown"
    )
    s3 = f"""\
## 3. PIPELINE VITALS

| Check | Value | Status |
|-------|-------|--------|
| Obs logged today | **{vitals['obs_today']:,}** | — |
| Realized fills today | **{vitals['realized_today']:,}** | — |
| Artifact-zeros | **{artifact_zeros}** | {'GREEN ✓' if artifact_zeros == 0 else 'RED ✗ INVESTIGATE'} |
| Evaluator last run | **{eval_age_str}** | {eval_status} |
| Measurement health | — | **{measurement_health}** |
| War room debates | **{wr_debates}** | — |
| Regime | {regime_str} | — |
| Error log | {error_summary} | — |
"""

    # ── Section 4: Report Card ────────────────────────────────────────────────
    s4 = f"""\
## 4. REPORT CARD

| # | Category | Grade | Δ / Reason |
|---|----------|-------|------------|
{card_row(1,  'dashboard',   'Dashboard / Command UI')}
{card_row(2,  'signals',     'Signal Generation / Scanners')}
{card_row(3,  'execution',   'Execution / Trade Pipeline')}
{card_row(4,  'measurement', 'Performance / Alpha Measurement')}
{card_row(5,  'fleet',       'Fleet / Agents')}
{card_row(6,  'risk',        'Risk / Safety Controls')}
{card_row(7,  'monitoring',  'Monitoring / Alerting')}
{card_row(8,  'data',        'Data / Source Health')}
{card_row(9,  'models',      'Models / Plutus')}
{card_row(10, 'resilience',  'Resilience / Departure-Readiness')}
"""

    # ── Section 5: Cost ───────────────────────────────────────────────────────
    doctrine_ok = cost <= 1.50
    # Pull phantom total to show separately if significant
    phantom_keys = ("wr-shadow-v1", "wr-shadow-v7d")
    s5 = f"""\
## 5. COST

| Item | Value |
|------|-------|
| Real API spend today | **${cost:.4f}** |
| Doctrine ceiling | $1.50 / day |
| Status | **{'✓ WITHIN' if doctrine_ok else '✗ OVER'} DOCTRINE** |

*Note: phantom billing from shadow CSP seats fixed 2026-06-29; historical rows left intact.*
"""

    # ── Section 6: Incidents / Changes ───────────────────────────────────────
    if git_changes:
        shipped_lines = "\n".join(f"- `{c}`" for c in git_changes)
    else:
        shipped_lines = "- *(no commits today)*"

    s6 = f"""\
## 6. INCIDENTS / CHANGES

### Shipped
{shipped_lines}

### Incidents
*(none — edit this section if something broke or was fixed outside git)*
"""

    # ── Section 7: Ledger Row ─────────────────────────────────────────────────
    ledger_values = [
        today.isoformat(),
        clean_days, n_under10, n_over10, n_null,
        artifact_zeros, 1 if evaluator_ok else 0, measurement_health,
        f"{cost:.4f}",
        g["dashboard"], g["signals"], g["execution"], g["measurement"],
        g["fleet"], g["risk"], g["monitoring"], g["data"],
        g["models"], g["resilience"],
    ]
    ledger_row_str = ",".join(str(v) for v in ledger_values)

    s7 = f"""\
## 7. LEDGER ROW

```
{ledger_row_str}
```
*Appended to drafts/daily_ledger.csv (append-only)*
"""

    # ── Assemble ──────────────────────────────────────────────────────────────
    header = (
        f"# OLLIETRADES DAILY REPORT — {today.isoformat()}\n"
        f"_Generated {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC · "
        f"gate: {gate_pct} · measurement: {measurement_health}_\n\n"
    )

    content = header + s1 + "\n" + s2 + "\n" + s3 + "\n" + s4 + "\n" + s5 + "\n" + s6 + "\n" + s7
    return content, ledger_values


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    today = date.today()
    DRAFTS.mkdir(parents=True, exist_ok=True)

    content, ledger_values = generate_report(today)

    out_path = DRAFTS / f"DAILY_REPORT_{today.isoformat()}.md"
    out_path.write_text(content)
    print(f"[daily_report] wrote {out_path}")

    if ledger_has_today(today):
        print(f"[daily_report] ledger already has {today} row — skipping append (idempotent)")
    else:
        append_ledger_row(ledger_values)
        print(f"[daily_report] appended ledger row for {today}")


if __name__ == "__main__":
    main()
