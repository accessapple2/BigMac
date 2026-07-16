#!/usr/bin/env python3
"""
realized_weekly_snapshot.py — Carrier forward-collection weekly meter.

Runs every Friday at 21:00 UTC (after close) via cron.
Writes drafts/REALIZED_WEEKLY_YYYY-MM-DD.md and prints to stdout.

SUFFICIENCY GATE: a source/bucket is TESTABLE when n>=400 AND distinct_days>=20.
Until then: "COLLECTING — not yet testable." No signal conclusions before the gate trips.

Bucket definitions (strict — never fold NULL into proj_under10):
  a_proj_under10  fwd_return_1d IS NOT NULL AND fwd_return_1d < 0.10
  b_proj_over10   fwd_return_1d IS NOT NULL AND fwd_return_1d >= 0.10
  c_proj_null     fwd_return_1d IS NULL
"""
import os
import sqlite3
import sys
from datetime import date, datetime, timezone

try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"),
                override=True)
except ImportError:
    pass

_REPO   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DB     = os.path.join(_REPO, "data", "trader.db")
_DRAFTS = os.path.join(_REPO, "drafts")

N_GATE    = 400
DAYS_GATE = 20


def _gate(n: int, days: int) -> str:
    if n >= N_GATE and days >= DAYS_GATE:
        return "TESTABLE"
    parts = []
    if n < N_GATE:
        parts.append(f"need {N_GATE - n} more obs")
    if days < DAYS_GATE:
        parts.append(f"need {DAYS_GATE - days} more days")
    return "COLLECTING — " + "; ".join(parts)


def run() -> str:
    today   = date.today().isoformat()
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    conn    = sqlite3.connect(_DB, timeout=15)
    conn.row_factory = sqlite3.Row

    lines: list[str] = []
    def emit(s: str = "") -> None:
        lines.append(s)
        print(s)

    emit(f"# REALIZED WEEKLY SNAPSHOT — {today}")
    emit(f"_Generated {now_utc} · exec-pipeline · restart pending Admiral's return_")
    emit()

    # ── overall coverage ──────────────────────────────────────────────────
    cov = conn.execute("""
        SELECT MIN(DATE(ts)) first_day, MAX(DATE(ts)) last_day,
               COUNT(DISTINCT DATE(ts)) distinct_days,
               COUNT(*) total_rows
          FROM signal_observations
         WHERE fwd_return_1d_realized IS NOT NULL
           AND fwd_return_1d_realized <> 0.0
    """).fetchone()

    emit("## Coverage")
    emit(f"- First day: {cov['first_day']}  Last day: {cov['last_day']}")
    emit(f"- Distinct trading days: {cov['distinct_days']}  Total real-bar rows: {cov['total_rows']}")
    emit(f"- Sufficiency gate: n≥{N_GATE} AND days≥{DAYS_GATE} per source/bucket")
    emit()

    # ── per-source ────────────────────────────────────────────────────────
    emit("## Per-Source (real bars only, excl exact-0.0 artifacts)")
    emit("```")
    emit(f"{'source':<22} {'n':>6} {'mean%':>8} {'wr%':>7} {'days':>6}  status")
    emit("-" * 72)
    src_rows = conn.execute("""
        SELECT source,
               COUNT(*)                                   AS n,
               ROUND(AVG(fwd_return_1d_realized)*100, 3) AS mean_pct,
               ROUND(100.0*SUM(CASE WHEN fwd_return_1d_realized>0
                                    THEN 1 ELSE 0 END)/COUNT(*), 1) AS wr,
               COUNT(DISTINCT DATE(ts))                  AS days
          FROM signal_observations
         WHERE fwd_return_1d_realized IS NOT NULL
           AND fwd_return_1d_realized <> 0.0
         GROUP BY source
         ORDER BY n DESC
    """).fetchall()
    for r in src_rows:
        status = _gate(r["n"], r["days"])
        emit(f"{r['source']:<22} {r['n']:>6} {r['mean_pct']:>8} {r['wr']:>7} {r['days']:>6}  {status}")
    emit("```")
    emit()

    # ── per-source, direction-signed ────────────────────────────────────────
    # HM-REALIZED-RETRY 2026-07-15: the unsigned mean above averages BULL and
    # BEAR raw returns together, which is meaningless for edge (a BEAR call
    # that moves -2% is a WIN, not a loss). This is the read that actually
    # answers "does the source carry edge" — flip sign on BEAR obs so a
    # positive number always means the call direction was profitable.
    emit("## Per-Source, Direction-Signed (the read that matters)")
    emit("```")
    emit(f"{'source':<16} {'direction':<10} {'n':>6} {'signed_mean%':>13} {'hit%':>7} {'days':>6}  status")
    emit("-" * 78)
    signed_expr = "CASE WHEN direction='BEAR' THEN -fwd_return_1d_realized ELSE fwd_return_1d_realized END"
    dir_rows = conn.execute(f"""
        SELECT source, direction,
               COUNT(*)                                     AS n,
               ROUND(AVG({signed_expr})*100, 3)              AS signed_mean_pct,
               ROUND(100.0*SUM(CASE WHEN {signed_expr}>0
                                    THEN 1 ELSE 0 END)/COUNT(*), 1) AS hit_rate,
               COUNT(DISTINCT DATE(ts))                      AS days
          FROM signal_observations
         WHERE fwd_return_1d_realized IS NOT NULL
           AND fwd_return_1d_realized <> 0.0
           AND direction IN ('BULL', 'BEAR')
         GROUP BY source, direction
         ORDER BY source, direction
    """).fetchall()
    for r in dir_rows:
        status = _gate(r["n"], r["days"])
        emit(f"{r['source']:<16} {r['direction']:<10} {r['n']:>6} "
             f"{r['signed_mean_pct']:>13} {r['hit_rate']:>7} {r['days']:>6}  {status}")
    emit("-" * 78)
    combined_rows = conn.execute(f"""
        SELECT source,
               COUNT(*)                                     AS n,
               ROUND(AVG({signed_expr})*100, 3)              AS signed_mean_pct,
               ROUND(100.0*SUM(CASE WHEN {signed_expr}>0
                                    THEN 1 ELSE 0 END)/COUNT(*), 1) AS hit_rate,
               COUNT(DISTINCT DATE(ts))                      AS days
          FROM signal_observations
         WHERE fwd_return_1d_realized IS NOT NULL
           AND fwd_return_1d_realized <> 0.0
           AND direction IN ('BULL', 'BEAR')
         GROUP BY source
         ORDER BY n DESC
    """).fetchall()
    for r in combined_rows:
        status = _gate(r["n"], r["days"])
        emit(f"{r['source']:<16} {'ALL':<10} {r['n']:>6} "
             f"{r['signed_mean_pct']:>13} {r['hit_rate']:>7} {r['days']:>6}  {status}")
    emit("```")
    emit()

    # ── per-source × bucket ───────────────────────────────────────────────
    emit("## Per-Source × Bucket (strict definitions)")
    emit("```")
    emit(f"{'source':<22} {'bucket':<22} {'n':>6} {'mean%':>8} {'wr%':>7} {'days':>6}  status")
    emit("-" * 90)
    bkt_rows = conn.execute("""
        SELECT source,
               CASE
                 WHEN fwd_return_1d IS NOT NULL AND fwd_return_1d < 0.10
                      THEN 'a_proj_under10'
                 WHEN fwd_return_1d IS NOT NULL AND fwd_return_1d >= 0.10
                      THEN 'b_proj_over10'
                 ELSE      'c_proj_null'
               END AS bucket,
               COUNT(*)                                   AS n,
               ROUND(AVG(fwd_return_1d_realized)*100, 3) AS mean_pct,
               ROUND(100.0*SUM(CASE WHEN fwd_return_1d_realized>0
                                    THEN 1 ELSE 0 END)/COUNT(*), 1) AS wr,
               COUNT(DISTINCT DATE(ts))                  AS days
          FROM signal_observations
         WHERE fwd_return_1d_realized IS NOT NULL
           AND fwd_return_1d_realized <> 0.0
         GROUP BY source, bucket
         ORDER BY source, bucket
    """).fetchall()
    for r in bkt_rows:
        status = _gate(r["n"], r["days"])
        emit(f"{r['source']:<22} {r['bucket']:<22} {r['n']:>6} "
             f"{r['mean_pct']:>8} {r['wr']:>7} {r['days']:>6}  {status}")
    emit("```")
    emit()

    # ── sufficiency summary ───────────────────────────────────────────────
    testable = [(r["source"], r["bucket"]) for r in bkt_rows
                if r["n"] >= N_GATE and r["days"] >= DAYS_GATE]
    emit("## Sufficiency Gate Summary")
    if testable:
        emit(f"**{len(testable)} source/bucket(s) crossed the gate — significance testing now valid:**")
        for src, bkt in testable:
            emit(f"  - {src} / {bkt}")
    else:
        emit("**No source/bucket has crossed the sufficiency gate yet.**")
        emit("COLLECTING — not yet testable. Return when gate trips (n≥400, days≥20).")
    emit()
    emit("_Bucket definitions (strict):_")
    emit("- `a_proj_under10` — fwd_return_1d IS NOT NULL AND < 0.10")
    emit("- `b_proj_over10`  — fwd_return_1d IS NOT NULL AND >= 0.10")
    emit("- `c_proj_null`    — fwd_return_1d IS NULL (no deep_scan projection)")
    emit("- NULL projections are NEVER folded into proj_under10.")

    conn.close()
    return "\n".join(lines)


def main() -> None:
    content = run()
    today   = date.today().isoformat()
    out_path = os.path.join(_DRAFTS, f"REALIZED_WEEKLY_{today}.md")
    with open(out_path, "w") as f:
        f.write(content + "\n")
    print(f"\n[snapshot written → {out_path}]")


if __name__ == "__main__":
    main()
