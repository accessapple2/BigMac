"""
agents/scotty/scotty.py

Scotty v1 — Short Squeeze Surveillance Agent (daily job).

Philosophy:
- Surveillance only. Never trades, never votes in Bridge consensus.
- Catches the SETUP, not the trigger. (The $CAR lesson.)
- Reuses engine.squeeze_scanner.run_scan() — no duplicate Finviz plumbing.
- v1 = 4 signals. v2 will add 13D/13G concentration via Uhura (separate sprint).

Run:
    python -m agents.scotty.scotty
    python -m agents.scotty.scotty --dry-run    # no DB writes, no notifications

Schedule: launchd/cron weekdays 5:30 PM ET.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

# --- imports from existing repo ---
# engine/squeeze_scanner.py already does the Finviz + yfinance heavy lifting.
# We re-use run_scan() and re-score with Scotty's rubric.
from engine.squeeze_scanner import run_scan

from agents.scotty.scoring import (
    SqueezeScore,
    TickerSnapshot,
    score_ticker,
    snapshot_from_run_scan_row,
    WATCHLIST_SCORE,
    ALERT_SCORE,
)

log = logging.getLogger("scotty")

# --- config ---
DB_PATH = os.environ.get("TRADER_DB_PATH", "data/trader.db")
NTFY_TOPIC = os.environ.get("SCOTTY_NTFY_TOPIC", "ollietrades-scotty")


# ---------- core scan ----------

def run_daily_scan() -> List[SqueezeScore]:
    """
    Pull candidates via run_scan(), re-score with Scotty's 4-signal rubric,
    return sorted by score desc then short_pct desc.
    """
    log.info("Scotty: starting daily scan")

    # run_scan() returns {"results": [...], "scanned_at": "...", "candidate_count": N}
    # Pass force=True — we want a fresh read, not the 5-min cache.
    raw = run_scan(force=True)
    rows = raw.get("results", []) if isinstance(raw, dict) else []
    log.info(f"Scotty: run_scan returned {len(rows)} candidates")

    scored: List[SqueezeScore] = []
    for row in rows:
        snap = snapshot_from_run_scan_row(row)
        if not snap.ticker:
            continue
        scored.append(score_ticker(snap))

    scored.sort(
        key=lambda r: (r.score, r.snapshot.short_pct or 0.0),
        reverse=True,
    )
    return scored


# ---------- persistence (insert-only; sacred DB rule) ----------

def persist_results(results: List[SqueezeScore], db_path: str) -> int:
    """Write watchlist-grade rows. Returns count written."""
    if not results:
        return 0
    scan_date = datetime.now(timezone.utc).date().isoformat()
    written = 0

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        for r in results:
            if r.score < WATCHLIST_SCORE:
                continue
            s = r.snapshot
            cur.execute(
                """
                INSERT INTO scotty_watchlist
                  (scan_date, ticker, score, short_pct, float_shares_m,
                   days_to_cover, vol_ratio, price, rsi, above_10d_high,
                   signals_json, scotty_version)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'v1')
                """,
                (
                    scan_date, r.ticker, r.score,
                    s.short_pct, s.float_shares_m, s.days_to_cover, s.vol_ratio,
                    s.price, s.rsi,
                    1 if s.above_10d_high else (0 if s.above_10d_high is False else None),
                    json.dumps(r.signals),
                ),
            )
            written += 1
            _update_first_seen(cur, r.ticker, r.score, scan_date)
        conn.commit()
    finally:
        conn.close()
    log.info(f"Scotty: wrote {written} rows to scotty_watchlist")
    return written


def _update_first_seen(cur, ticker: str, score: int, scan_date: str) -> None:
    cur.execute(
        "SELECT peak_score FROM scotty_first_seen WHERE ticker = ?",
        (ticker,),
    )
    row = cur.fetchone()
    if row is None:
        cur.execute(
            """
            INSERT INTO scotty_first_seen
              (ticker, first_date, first_score, peak_score, peak_date)
            VALUES (?, ?, ?, ?, ?)
            """,
            (ticker, scan_date, score, score, scan_date),
        )
    elif score > row[0]:
        cur.execute(
            """
            UPDATE scotty_first_seen
            SET peak_score = ?, peak_date = ?, updated_at = CURRENT_TIMESTAMP
            WHERE ticker = ?
            """,
            (score, scan_date, ticker),
        )


# ---------- notifications ----------

def notify(results: List[SqueezeScore]) -> None:
    """
    v1 notification strategy:
    - Score 4/4: push to ntfy (urgent — these are rare)
    - Score >=3: log for Kirk Advisor to pick up from DB on next advisor run
      (we do NOT import Kirk here — Kirk queries scotty_watchlist directly)
    """
    alerts = [r for r in results if r.score >= ALERT_SCORE]
    if alerts:
        _send_ntfy(alerts)


def _send_ntfy(alerts: List[SqueezeScore]) -> None:
    try:
        import requests
    except ImportError:
        log.warning("requests not installed; skipping ntfy push")
        return
    tickers = ", ".join(
        f"{r.ticker}(SI:{r.snapshot.short_pct:.0f}%)" for r in alerts if r.snapshot.short_pct
    )
    if not tickers:
        return
    try:
        requests.post(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=f"Scotty 4/4 alert: {tickers}".encode("utf-8"),
            headers={"Title": "Squeeze pressure critical", "Priority": "high"},
            timeout=5,
        )
        log.info(f"Scotty: ntfy pushed for {len(alerts)} tickers")
    except Exception as e:
        log.warning(f"ntfy push failed: {e}")


# ---------- CLI ----------

def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description="Scotty v1 — squeeze surveillance")
    parser.add_argument("--dry-run", action="store_true",
                        help="Run scan and print results, but do not write DB or send notifications.")
    parser.add_argument("--db", default=DB_PATH, help=f"Path to trader.db (default: {DB_PATH})")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    results = run_daily_scan()

    if args.dry_run:
        print(f"\n=== Scotty Scan (DRY RUN) {datetime.now().isoformat(timespec='seconds')} ===")
    else:
        Path(args.db).parent.mkdir(parents=True, exist_ok=True)
        persist_results(results, args.db)
        notify(results)
        print(f"\n=== Scotty Scan {datetime.now().isoformat(timespec='seconds')} ===")

    # top-10 summary to stdout (launchd log will capture this)
    for r in results[:10]:
        s = r.snapshot
        print(f"  {r.ticker:6s}  score={r.score}/4  "
              f"SI={(s.short_pct or 0):5.1f}%  "
              f"float={(s.float_shares_m or 0):5.1f}M  "
              f"d2c={(s.days_to_cover or 0):4.1f}  "
              f"volx={(s.vol_ratio or 0):4.1f}")
    if not results:
        print("  (no candidates returned)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
