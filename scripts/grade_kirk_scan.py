#!/usr/bin/env python3
"""
Kirk Scan Observation Grading — SQLite, real schema, Polygon exit prices.

Architecture (v2):
    Part A  SQL  — work-list: anchored, scoreable rows from signal_observations.
    Polygon      — fetch exit price for each ticker as of horizon_date.
    TEMP TABLE   — grade_results(id, specificity, direction, entry_px, exit_px).
    Part B  SQL  — tier rollup + verdict (edge threshold: Δ > 2.0pp high vs low).
    Optional     — per claim_type breakdown.

Usage:
    python3 scripts/grade_kirk_scan.py            # tier summary (default)
    python3 scripts/grade_kirk_scan.py receipts   # per-observation detail
    python3 scripts/grade_kirk_scan.py claims     # by claim_type
    python3 scripts/grade_kirk_scan.py all        # all three

Why no daily_bars join:
    daily_bars in signals.db is frozen at 2026-05-29.  Horizon dates fall in
    July/Aug. The table has no rows there. Exit prices must come from Polygon.
"""

from __future__ import annotations
import json
import os
import sys
import sqlite3
import urllib.request
import urllib.parse
from datetime import date, timedelta

BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRADER_DB  = os.path.join(BASE_DIR, "data", "trader.db")
SOURCE     = "kirk_super_scan"
EDGE_THRESHOLD_PP = 2.0   # high - low must exceed this to call EDGE

_POLYGON_KEY = os.environ.get("POLYGON_API_KEY", "")

# ── Part A: work-list ─────────────────────────────────────────────────────────
SQL_WORKLIST = """
SELECT
  o.id,
  o.ticker,
  o.direction,
  date(o.ts)                                                           AS captured_at,
  json_extract(o.confluence_meta, '$.claim_type')                      AS claim_type,
  json_extract(o.confluence_meta, '$.specificity')                     AS specificity,
  CAST(json_extract(o.confluence_meta, '$.horizon_days') AS INTEGER)   AS horizon_days,
  CAST(json_extract(o.confluence_meta, '$.entry_price_anchor') AS REAL) AS entry_px,
  date(
    o.ts,
    '+' || CAST(json_extract(o.confluence_meta, '$.horizon_days') AS INTEGER) || ' days'
  )                                                                    AS horizon_date
FROM signal_observations o
WHERE o.source = ?
  AND json_extract(o.confluence_meta, '$.scoreable_in_window') IN (1, 'true')
  AND json_extract(o.confluence_meta, '$.entry_price_anchor') IS NOT NULL;
"""

# ── Part B: tier rollup (against TEMP TABLE grade_results) ────────────────────
SQL_TIERS = """
WITH scored AS (
  SELECT
    specificity,
    CASE
      WHEN direction IN ('long','LONG','bullish','BULLISH')
        THEN (exit_px - entry_px) / entry_px
      WHEN direction IN ('short','SHORT')
        THEN (entry_px - exit_px) / entry_px
    END AS return_pct
  FROM grade_results
  WHERE entry_px IS NOT NULL AND exit_px IS NOT NULL
)
SELECT
  specificity,
  COUNT(*)                                                            AS n,
  ROUND(AVG(return_pct) * 100, 2)                                    AS avg_return_pct,
  ROUND(SUM(CASE WHEN return_pct > 0 THEN 1.0 ELSE 0.0 END)
        / COUNT(*) * 100, 1)                                         AS win_rate_pct
FROM scored
GROUP BY specificity
ORDER BY CASE specificity WHEN 'high' THEN 1 WHEN 'med' THEN 2 ELSE 3 END;
"""

SQL_RECEIPTS = """
WITH scored AS (
  SELECT
    g.id,
    g.ticker,
    g.claim_type,
    g.specificity,
    g.direction,
    g.entry_px,
    g.exit_px,
    g.horizon_date,
    CASE
      WHEN g.direction IN ('long','LONG','bullish','BULLISH')
        THEN (g.exit_px - g.entry_px) / g.entry_px
      WHEN g.direction IN ('short','SHORT')
        THEN (g.entry_px - g.exit_px) / g.entry_px
    END AS return_pct
  FROM grade_results g
)
SELECT
  ticker, claim_type, specificity, direction,
  ROUND(entry_px, 2) AS entry_px,
  ROUND(exit_px,  2) AS exit_px,
  horizon_date,
  ROUND(return_pct * 100, 2) AS return_pct,
  CASE
    WHEN exit_px IS NULL THEN 'UNPRICED'
    WHEN return_pct > 0  THEN 'WIN'
    ELSE                      'LOSS'
  END AS result
FROM scored
ORDER BY specificity,
         COALESCE(return_pct, -9999) DESC;
"""

SQL_CLAIMS = """
WITH scored AS (
  SELECT
    claim_type,
    CASE
      WHEN direction IN ('long','LONG','bullish','BULLISH')
        THEN (exit_px - entry_px) / entry_px
      WHEN direction IN ('short','SHORT')
        THEN (entry_px - exit_px) / entry_px
    END AS return_pct
  FROM grade_results
  WHERE entry_px IS NOT NULL AND exit_px IS NOT NULL
)
SELECT
  claim_type,
  COUNT(*)                                                            AS n,
  ROUND(AVG(return_pct) * 100, 2)                                    AS avg_return_pct,
  ROUND(SUM(CASE WHEN return_pct > 0 THEN 1.0 ELSE 0.0 END)
        / COUNT(*) * 100, 1)                                         AS win_rate_pct
FROM scored
GROUP BY claim_type
ORDER BY avg_return_pct DESC;
"""


# ── Polygon: close on or before a target date ─────────────────────────────────
def _polygon_close_at(ticker: str, target: str) -> float | None:
    """
    Fetch the closing price of ticker on the last trading day <= target.
    Uses /range/1/day with a 7-day lookback window to clear weekends/holidays.
    """
    if not _POLYGON_KEY:
        return None
    from_dt = (date.fromisoformat(target) - timedelta(days=7)).isoformat()
    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{urllib.parse.quote(ticker)}"
        f"/range/1/day/{from_dt}/{target}"
        f"?adjusted=true&sort=desc&limit=1&apiKey={_POLYGON_KEY}"
    )
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            data = json.loads(r.read())
        results = data.get("results", [])
        if results:
            return float(results[0]["c"])
    except Exception as e:
        print(f"  [polygon] {ticker}@{target}: {e}")
    return None


# ── Helpers ───────────────────────────────────────────────────────────────────
def _today_or_past(horizon: str) -> bool:
    """True if the horizon date has already passed (exit price is available)."""
    return date.fromisoformat(horizon) <= date.today()


def _print_table(title: str, headers: list[str], rows: list) -> None:
    print(f"\n{'='*64}")
    print(f"  {title}")
    print(f"{'='*64}")
    if not rows:
        print("  (no rows)")
        return
    col_w = [max(len(h), max((len(str(r[i] or "")) for r in rows), default=0))
             for i, h in enumerate(headers)]
    fmt = "  " + "  ".join(f"{{:<{w}}}" for w in col_w)
    print(fmt.format(*headers))
    print("  " + "  ".join("-" * w for w in col_w))
    for row in rows:
        print(fmt.format(*[str(v) if v is not None else "—" for v in row]))


def _verdict(rows: list) -> None:
    """Print edge verdict from tier rollup rows."""
    by_tier = {r[0]: r for r in rows}
    high = by_tier.get("high")
    low  = by_tier.get("low")
    if not (high and low):
        return
    avg_h = high[2]
    avg_l = low[2]
    if avg_h is None or avg_l is None:
        return
    delta = round(float(avg_h) - float(avg_l), 2)
    if delta > EDGE_THRESHOLD_PP:
        verdict = f"EDGE — high beats low by +{delta}pp (threshold {EDGE_THRESHOLD_PP}pp)"
    else:
        verdict = f"NO EDGE — Δ={delta}pp < {EDGE_THRESHOLD_PP}pp — Kirk echoes the tape → CUT"
    print(f"\n  VERDICT: {verdict}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> int:
    mode = (sys.argv[1].lower() if len(sys.argv) > 1 else "tiers")
    run_tiers    = mode in ("tiers", "all")
    run_receipts = mode in ("receipts", "all")
    run_claims   = mode in ("claims", "all")
    if not any([run_tiers, run_receipts, run_claims]):
        print(f"Unknown mode '{mode}'. Use: tiers | receipts | claims | all")
        return 1

    con = sqlite3.connect(TRADER_DB)
    cur = con.cursor()

    # ── Part A: fetch work list ───────────────────────────────────────────────
    cur.execute(SQL_WORKLIST, (SOURCE,))
    worklist = cur.fetchall()
    cols = [d[0] for d in cur.description]
    # cols: id, ticker, direction, captured_at, claim_type, specificity,
    #       horizon_days, entry_px, horizon_date

    if not worklist:
        print("[grade-kirk] No scoreable rows (entry_price_anchor not stamped yet).")
        con.close()
        return 0

    # ── Polygon: fetch exit prices (only for past horizon dates) ─────────────
    print(f"[grade-kirk] {len(worklist)} scoreable rows — fetching exit prices...")
    results = []  # (id, ticker, claim_type, specificity, direction, entry_px, exit_px, horizon_date)
    for row in worklist:
        d = dict(zip(cols, row))
        horizon = d["horizon_date"]
        exit_px = None
        if _today_or_past(horizon):
            exit_px = _polygon_close_at(d["ticker"], horizon)
            status = f"exit={exit_px:.2f}" if exit_px else "no price yet"
        else:
            status = f"horizon {horizon} not yet reached"
        print(f"  {d['ticker']:6}  {d['specificity']:4}  {status}")
        results.append((
            d["id"], d["ticker"], d["claim_type"], d["specificity"],
            d["direction"], d["entry_px"], exit_px, horizon,
        ))

    # ── TEMP TABLE ────────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TEMP TABLE grade_results (
            id          INTEGER,
            ticker      TEXT,
            claim_type  TEXT,
            specificity TEXT,
            direction   TEXT,
            entry_px    REAL,
            exit_px     REAL,
            horizon_date TEXT
        )
    """)
    cur.executemany(
        "INSERT INTO grade_results VALUES (?,?,?,?,?,?,?,?)",
        results,
    )

    # ── Part B: rollup queries ────────────────────────────────────────────────
    if run_tiers:
        cur.execute(SQL_TIERS)
        rows = cur.fetchall()
        _print_table(
            "(1) TIER COMPARISON — does specificity predict edge?",
            ["specificity", "n", "avg_return_pct", "win_rate_pct"],
            rows,
        )
        _verdict(rows)

    if run_receipts:
        cur.execute(SQL_RECEIPTS)
        rows = cur.fetchall()
        _print_table(
            "(2) RECEIPTS — per-observation detail",
            ["ticker", "claim_type", "specificity", "dir",
             "entry_px", "exit_px", "horizon_date", "return_pct", "result"],
            rows,
        )

    if run_claims:
        cur.execute(SQL_CLAIMS)
        rows = cur.fetchall()
        _print_table(
            "(3) BY CLAIM TYPE — which bucket carried the edge",
            ["claim_type", "n", "avg_return_pct", "win_rate_pct"],
            rows,
        )

    con.close()
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
