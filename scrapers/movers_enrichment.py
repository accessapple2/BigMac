#!/usr/bin/env python3
"""HM-BK Phase 2A — ticker metadata enrichment.

Cadence: Sunday 23:00 AZ weekly (launchd). Self-gates to maintenance
window Sun 22:00 AZ - Mon 04:00 AZ unless --force.

Loads POLYGON_API_KEY from ~/autonomous-trader/.env (Phase 1 pattern).
"""
import argparse
import datetime as dt
import os
import sqlite3
import sys
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "data" / "trader.db"
ENV_PATH = ROOT / ".env"
JOB_ID = "movers_enrichment"
RATE_LIMIT_DELAY = 0.06
OPTIONABLE_REFRESH_DAYS = 30

LOG = lambda msg: print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def load_env():
    if not ENV_PATH.exists():
        return
    for line in ENV_PATH.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        v = v.strip().strip('"').strip("'")
        os.environ.setdefault(k.strip(), v)


def in_maintenance_window() -> bool:
    now = dt.datetime.now()
    wd, hr = now.weekday(), now.hour
    if wd == 6 and hr >= 22:
        return True
    if wd == 0 and hr < 4:
        return True
    return False


def get_targets(conn, limit=None):
    cur = conn.execute(
        """
        SELECT symbol FROM (
          SELECT DISTINCT symbol FROM mover_watchlist
          UNION
          SELECT symbol FROM ticker_metadata
            WHERE last_mcap_refresh IS NULL
               OR julianday('now') - julianday(last_mcap_refresh) > 7
        )
        ORDER BY symbol
        """
    )
    rows = [r[0] for r in cur.fetchall()]
    if limit:
        rows = rows[:limit]
    return rows


def fetch_ticker_details(symbol, api_key):
    url = f"https://api.polygon.io/v3/reference/tickers/{symbol}"
    try:
        r = requests.get(url, params={"apiKey": api_key}, timeout=5)
        if r.status_code != 200:
            return None
        data = r.json().get("results", {})
        return {
            "market_cap": data.get("market_cap"),
            "primary_exchange": data.get("primary_exchange"),
            "ticker_type": data.get("type"),
            "active": 1 if data.get("active") else 0,
        }
    except (requests.RequestException, ValueError):
        return None


def fetch_optionable(symbol, api_key):
    url = "https://api.polygon.io/v3/reference/options/contracts"
    try:
        r = requests.get(
            url,
            params={"underlying_ticker": symbol, "limit": 1, "apiKey": api_key},
            timeout=5,
        )
        if r.status_code != 200:
            return None
        return 1 if r.json().get("results") else 0
    except (requests.RequestException, ValueError):
        return None


def optionable_needs_refresh(conn, symbol):
    cur = conn.execute(
        "SELECT last_optionable_refresh FROM ticker_metadata WHERE symbol=?",
        (symbol,),
    )
    row = cur.fetchone()
    if not row or row[0] is None:
        return True
    try:
        last = dt.datetime.fromisoformat(row[0])
    except ValueError:
        return True
    age_days = (dt.datetime.now() - last).total_seconds() / 86400
    return age_days > OPTIONABLE_REFRESH_DAYS


def upsert_metadata(conn, symbol, details, optionable):
    now = dt.datetime.now().isoformat(timespec="seconds")
    cur = conn.execute("SELECT 1 FROM ticker_metadata WHERE symbol=?", (symbol,))
    exists = cur.fetchone() is not None
    if exists:
        if optionable is None:
            conn.execute(
                """UPDATE ticker_metadata
                   SET market_cap=?, primary_exchange=?, ticker_type=?, active=?, last_mcap_refresh=?
                   WHERE symbol=?""",
                (
                    details.get("market_cap"),
                    details.get("primary_exchange"),
                    details.get("ticker_type"),
                    details.get("active"),
                    now,
                    symbol,
                ),
            )
        else:
            conn.execute(
                """UPDATE ticker_metadata
                   SET market_cap=?, primary_exchange=?, ticker_type=?, active=?,
                       last_mcap_refresh=?, optionable=?, last_optionable_refresh=?
                   WHERE symbol=?""",
                (
                    details.get("market_cap"),
                    details.get("primary_exchange"),
                    details.get("ticker_type"),
                    details.get("active"),
                    now,
                    optionable,
                    now,
                    symbol,
                ),
            )
    else:
        conn.execute(
            """INSERT INTO ticker_metadata
               (symbol, market_cap, primary_exchange, ticker_type, active,
                last_mcap_refresh, optionable, last_optionable_refresh)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                symbol,
                details.get("market_cap"),
                details.get("primary_exchange"),
                details.get("ticker_type"),
                details.get("active"),
                now,
                optionable,
                now if optionable is not None else None,
            ),
        )


def update_state(conn, **fields):
    cur = conn.execute("SELECT 1 FROM enrichment_state WHERE job_id=?", (JOB_ID,))
    if cur.fetchone():
        sets = ", ".join(f"{k}=?" for k in fields)
        conn.execute(
            f"UPDATE enrichment_state SET {sets} WHERE job_id=?",
            (*fields.values(), JOB_ID),
        )
    else:
        keys = ["job_id"] + list(fields.keys())
        vals = [JOB_ID] + list(fields.values())
        cols = ", ".join(keys)
        qs = ", ".join("?" * len(keys))
        conn.execute(f"INSERT INTO enrichment_state ({cols}) VALUES ({qs})", vals)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    load_env()

    if not args.force and not in_maintenance_window():
        LOG("Outside maintenance window - exit clean (use --force to override)")
        return 0

    api_key = os.environ.get("POLYGON_API_KEY", "").strip()
    if not api_key:
        LOG("POLYGON_API_KEY not set - exit 1")
        return 1

    conn = sqlite3.connect(str(DB))
    conn.isolation_level = None
    targets = get_targets(conn, limit=args.limit)
    total = len(targets)
    LOG(f"Targets: {total} tickers")

    update_state(
        conn,
        last_run_started_at=dt.datetime.now().isoformat(timespec="seconds"),
        status="running",
        total_target=total,
        total_processed=0,
        notes="",
    )

    processed = 0
    enriched_mcap = 0
    enriched_opt = 0
    for sym in targets:
        details = fetch_ticker_details(sym, api_key)
        if details:
            enriched_mcap += 1
        else:
            details = {}

        opt = None
        if optionable_needs_refresh(conn, sym):
            opt = fetch_optionable(sym, api_key)
            if opt is not None:
                enriched_opt += 1
            time.sleep(RATE_LIMIT_DELAY)

        upsert_metadata(conn, sym, details, opt)
        update_state(
            conn,
            last_completed_symbol=sym,
            total_processed=processed + 1,
        )
        processed += 1
        time.sleep(RATE_LIMIT_DELAY)

        if processed % 25 == 0:
            LOG(f"Progress: {processed}/{total}  (mcap+={enriched_mcap}, opt+={enriched_opt})")

    update_state(
        conn,
        status="completed",
        last_run_finished_at=dt.datetime.now().isoformat(timespec="seconds"),
        notes=f"mcap+={enriched_mcap} opt+={enriched_opt}",
    )
    LOG(f"DONE  processed={processed}  mcap+={enriched_mcap}  opt+={enriched_opt}")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
