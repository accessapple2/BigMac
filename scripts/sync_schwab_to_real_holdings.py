#!/usr/bin/env python3
"""Sync latest schwab_holdings snapshot into data/real_holdings.json.

Updates the 'schwab' account block in real_holdings.json from the most
recent snapshot in the schwab_holdings table. Leaves all other accounts
(webull, ibkr, tradestation, etc.) untouched.

Usage:
    python3 scripts/sync_schwab_to_real_holdings.py

Designed to be called at the end of import_schwab_csv.py.
"""
from __future__ import annotations
import json
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(os.environ.get("BIGMAC_REPO", "/Users/bigmac/autonomous-trader"))
DB_PATH   = REPO_ROOT / "data" / "trader.db"
JSON_PATH = REPO_ROOT / "data" / "real_holdings.json"


def get_latest_snapshot(conn):
    cur = conn.execute(
        "SELECT snapshot_id, snapshot_ts, account_label, account_last4 "
        "FROM schwab_holdings ORDER BY snapshot_id DESC LIMIT 1"
    )
    row = cur.fetchone()
    if not row:
        sys.exit("ERROR: schwab_holdings table is empty.")
    return row


def get_positions(conn, snapshot_id):
    cur = conn.execute(
        "SELECT symbol, qty, price, market_value, cost_basis, gain_dollar, gain_pct, "
        "asset_type, day_change_pct, day_change_dollar "
        "FROM schwab_holdings "
        "WHERE snapshot_id = ? AND is_summary_row = 0 "
        "ORDER BY market_value DESC",
        (snapshot_id,)
    )
    return cur.fetchall()


def get_grades(conn, symbols):
    """HM-SCHWAB-LINE-DETAIL 2026-05-28: per-symbol Smart Score grade (A-F) from
    stock_fundamentals (refreshed hourly by run_fundamental_scan). Missing →
    None (shown as '—' in the UI; don't compute inline — too slow)."""
    if not symbols:
        return {}
    qs = ",".join("?" * len(symbols))
    cur = conn.execute(
        f"SELECT symbol, smart_score, grade FROM stock_fundamentals WHERE symbol IN ({qs})",
        list(symbols),
    )
    return {row[0]: {"smart_score": row[1], "grade": row[2]} for row in cur.fetchall()}


def main():
    if not JSON_PATH.exists():
        sys.exit(f"ERROR: {JSON_PATH} not found.")
    if not DB_PATH.exists():
        sys.exit(f"ERROR: {DB_PATH} not found.")

    # Load existing JSON
    with open(JSON_PATH) as f:
        data = json.load(f)

    # Get latest Schwab snapshot
    conn = sqlite3.connect(DB_PATH)
    snap_id, snap_ts, acct_label, acct_last4 = get_latest_snapshot(conn)
    rows = get_positions(conn, snap_id)
    grades = get_grades(conn, [r[0] for r in rows if r[0] != "CASH"])
    conn.close()

    # Separate cash from equity positions
    cash_balance = 0.0
    positions = []
    for symbol, qty, price, mkt_val, cost_basis, gain_d, gain_pct, asset_type, day_chg_pct, day_chg_d in rows:
        if symbol == "CASH":
            cash_balance = float(mkt_val or 0)
            continue
        avg_cost = round(float(cost_basis or 0) / float(qty), 4) if qty else 0
        g = grades.get(symbol, {})
        # HM-SCHWAB-LINE-DETAIL 2026-05-28: carry structured per-ticker fields
        # (market_value, total gain $/%, current price) so the UI can show a
        # full holdings line instead of burying gain in `notes`. NOTE: for
        # positions opened today, day_change_pct == gain_pct (day-since-purchase
        # == total gain); they diverge once held overnight.
        positions.append({
            "symbol": symbol,
            "qty": qty,
            "avg_cost": avg_cost,
            "price": round(float(price), 4) if price is not None else None,
            "market_value": round(float(mkt_val), 2) if mkt_val is not None else None,
            "gain_dollar": round(float(gain_d), 2) if gain_d is not None else None,
            "gain_pct": round(float(gain_pct), 2) if gain_pct is not None else None,
            "day_change_pct": round(float(day_chg_pct), 2) if day_chg_pct is not None else None,
            "day_change_dollar": round(float(day_chg_d), 2) if day_chg_d is not None else None,
            "grade": g.get("grade"),
            "smart_score": g.get("smart_score"),
            "notes": (
                f"market_value=${mkt_val:.2f}, gain=${gain_d:+.2f} ({gain_pct:+.2f}%) "
                f"[from snapshot {snap_id}]"
            )
        })

    # Update schwab block
    schwab = data.get("accounts", {}).get("schwab", {})
    schwab["label"] = "Schwab"
    schwab["role"] = "primary"
    schwab["is_active"] = True
    schwab["cash_balance"] = round(cash_balance, 2)
    schwab["account_id"] = f"...{acct_last4}"
    schwab["account_name"] = f"{acct_label} (Brokerage)"
    schwab["positions"] = positions
    schwab["source"] = "csv_snapshot"   # HM-SCHWAB-LIVE-SYNC: stamp path so live_api vs csv is always known
    total_equity = sum(float(p.get("avg_cost", 0)) * float(p.get("qty", 0)) for p in positions)
    schwab["notes"] = (
        f"Auto-synced from schwab_holdings snapshot {snap_id}. "
        f"{len(positions)} equity positions. Snapshot time: {snap_ts}."
    )
    data.setdefault("accounts", {})["schwab"] = schwab

    # Mark webull liquidated (per Apr 23 migration)
    if "webull" in data.get("accounts", {}):
        data["accounts"]["webull"]["is_active"] = False
        if "[LIQUIDATED" not in data["accounts"]["webull"].get("notes", ""):
            data["accounts"]["webull"]["notes"] = (
                "[LIQUIDATED 2026-04-23, migrated to Schwab] " +
                data["accounts"]["webull"].get("notes", "")
            )

    data["last_updated"] = datetime.now().strftime("%Y-%m-%d")

    # Atomic write: temp file + rename
    tmp_path = JSON_PATH.with_suffix(".json.tmp")
    with open(tmp_path, "w") as f:
        json.dump(data, f, indent=2)
    tmp_path.replace(JSON_PATH)

    print(f"✅ Synced schwab_holdings snapshot to real_holdings.json")
    print(f"   Snapshot:   {snap_id} ({snap_ts})")
    print(f"   Account:    {acct_label} ({acct_last4})")
    print(f"   Positions:  {len(positions)}")
    print(f"   Cash:       ${cash_balance:,.2f}")
    print(f"   Webull:     marked is_active=false (liquidated)")


if __name__ == "__main__":
    main()
