#!/usr/bin/env python3
"""Refresh options_trades.mtm_intrinsic for all open CSP positions.

MTM intrinsic for a short put = -max(0, strike - spot) * 100 * contracts
  OTM (spot >= strike): 0           (no liability)
  ITM (spot <  strike): negative    (mark-to-market loss)

Usage:
  python3 scripts/refresh_mtm_intrinsic.py          # update all open rows
  python3 scripts/refresh_mtm_intrinsic.py --dry-run # print without writing

Cron (daily 15:55 ET Mon-Fri, after close):
  55 15 * * 1-5 cd ~/autonomous-trader && .venv/bin/python3 scripts/refresh_mtm_intrinsic.py >> logs/mtm_refresh.log 2>&1
"""
import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

DB_PATH = ROOT / "data" / "trader.db"
APCA_KEY = os.getenv("APCA_API_KEY_ID", "")
APCA_SEC = os.getenv("APCA_API_SECRET_KEY", "")
DATA_BASE = "https://data.alpaca.markets/v2/stocks"


def fetch_spots(symbols: list[str]) -> dict[str, float]:
    """Fetch latest trade price for each symbol via Alpaca snapshots."""
    if not APCA_KEY:
        sys.exit("ERROR: APCA_API_KEY_ID not set")
    hdrs = {"APCA-API-KEY-ID": APCA_KEY, "APCA-API-SECRET-KEY": APCA_SEC}
    url = f"{DATA_BASE}/snapshots"
    r = requests.get(url, headers=hdrs,
                     params={"symbols": ",".join(symbols), "feed": "iex"},
                     timeout=10)
    r.raise_for_status()
    data = r.json()
    spots = {}
    for sym, snap in data.items():
        price = (snap.get("latestTrade") or {}).get("p") \
             or (snap.get("minuteBar") or {}).get("c") \
             or (snap.get("dailyBar") or {}).get("c")
        if price:
            spots[sym] = float(price)
    return spots


def compute_intrinsic(legs_json: str, spot: float, contracts: int) -> float:
    """Short-put intrinsic = -max(0, strike - spot) * 100 * contracts."""
    try:
        legs = json.loads(legs_json)
    except Exception:
        return 0.0
    total = 0.0
    for leg in legs:
        if leg.get("type") == "put" and leg.get("side") == "short":
            strike = float(leg.get("strike", 0))
            qty = int(leg.get("qty", 1))
            intrinsic = max(0.0, strike - spot)
            total -= intrinsic * 100 * qty * contracts
    return round(total, 2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row

    rows = db.execute(
        "SELECT id, symbol, legs_json, contracts FROM options_trades WHERE status='open'"
    ).fetchall()

    if not rows:
        print("No open options_trades rows — nothing to refresh.")
        return

    symbols = list({r["symbol"] for r in rows})
    print(f"Fetching spots for {len(symbols)} symbols: {', '.join(sorted(symbols))}")
    spots = fetch_spots(symbols)
    print(f"Received: { {k: v for k, v in spots.items()} }")

    missing = [s for s in symbols if s not in spots]
    if missing:
        print(f"WARNING: no price for {missing} — those rows will be skipped")

    updates = []
    for r in rows:
        spot = spots.get(r["symbol"])
        if spot is None:
            continue
        contracts = r["contracts"] or 1
        mtm = compute_intrinsic(r["legs_json"], spot, contracts)
        updates.append((mtm, r["id"]))

    print(f"\n{'DRY-RUN — ' if args.dry_run else ''}Updating {len(updates)}/{len(rows)} rows:")
    for mtm, rid in sorted(updates, key=lambda x: x[0]):
        print(f"  id={rid:4d}  mtm_intrinsic={mtm:>10.2f}")

    if not args.dry_run:
        db.executemany(
            "UPDATE options_trades SET mtm_intrinsic=? WHERE id=?", updates
        )
        db.commit()
        print(f"\nCommitted {len(updates)} rows.")
        null_left = db.execute(
            "SELECT COUNT(*) FROM options_trades WHERE status='open' AND mtm_intrinsic IS NULL"
        ).fetchone()[0]
        total_mtm = db.execute(
            "SELECT SUM(mtm_intrinsic) FROM options_trades WHERE status='open'"
        ).fetchone()[0] or 0.0
        print(f"NULL remaining: {null_left} | SUM(mtm_intrinsic): ${total_mtm:,.2f}")

    db.close()


if __name__ == "__main__":
    main()
