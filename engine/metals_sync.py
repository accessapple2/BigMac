#!/usr/bin/env python3
"""
Sync metals_ledger (source of truth) into portfolio_positions with live prices.
Safe to run repeatedly — only UPDATE existing metal rows (id=2 gold, id=3 silver).
Never deletes ledger data, never touches non-metal rows.

Schema notes:
  portfolio_positions uses portfolio_id (int FK), ticker, quantity, entry_price,
  current_price, metal_oz, unrealized_pnl, updated_at.
  No UNIQUE constraint on (portfolio_id, ticker) — UPDATE by known id only.
"""
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

DB = Path(__file__).parent.parent / "data" / "trader.db"

# Known row IDs for metal positions in portfolio_positions (portfolio_id=5)
METAL_ROW_IDS = {
    "gold":   2,   # XAUUSD
    "silver": 3,   # XAGUSD
}
METAL_TICKERS = {
    "gold":   "XAUUSD",
    "silver": "XAGUSD",
}
SPOT_TICKERS = {
    "gold":   "GC=F",
    "silver": "SI=F",
}


def fetch_price(metal: str):
    """Fetch spot price via yfinance futures proxy."""
    try:
        import yfinance as yf
        ticker = SPOT_TICKERS[metal]
        info = yf.Ticker(ticker).fast_info
        price = info.get("lastPrice") or info.get("last_price")
        if price and float(price) > 0:
            return float(price)
    except Exception as e:
        print(f"  [WARN] Price fetch failed for {metal} ({SPOT_TICKERS[metal]}): {e}",
              file=sys.stderr)
    return None


def sync():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    # Aggregate authoritative totals from metals_ledger
    rows = conn.execute("""
        SELECT metal,
               SUM(qty_oz)                              AS total_oz,
               SUM(qty_oz * cost_per_oz) / SUM(qty_oz) AS blended_avg,
               SUM(total_cost)                          AS total_cost
        FROM metals_ledger
        GROUP BY metal
    """).fetchall()

    if not rows:
        print("No rows in metals_ledger — nothing to sync.")
        conn.close()
        return

    updated = 0
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    for row in rows:
        metal      = row["metal"].lower()
        total_oz   = float(row["total_oz"])
        avg_cost   = float(row["blended_avg"])
        total_cost = float(row["total_cost"])

        row_id = METAL_ROW_IDS.get(metal)
        ticker = METAL_TICKERS.get(metal)
        if not row_id or not ticker:
            print(f"  [SKIP] Unknown metal type: {metal}")
            continue

        live_price = fetch_price(metal)
        if live_price is None:
            print(f"  [WARN] Could not fetch live price for {metal} — keeping existing current_price")
            # Still update qty/avg in case ledger changed; keep current_price as-is
            conn.execute("""
                UPDATE portfolio_positions
                SET quantity      = ?,
                    entry_price   = ?,
                    metal_oz      = ?,
                    unrealized_pnl = (current_price - ?) * ?,
                    updated_at    = ?
                WHERE id = ?
            """, (total_oz, avg_cost, total_oz, avg_cost, total_oz, now, row_id))
        else:
            pnl = (live_price - avg_cost) * total_oz
            conn.execute("""
                UPDATE portfolio_positions
                SET quantity       = ?,
                    entry_price    = ?,
                    current_price  = ?,
                    metal_oz       = ?,
                    unrealized_pnl = ?,
                    updated_at     = ?
                WHERE id = ?
            """, (total_oz, avg_cost, live_price, total_oz, pnl, now, row_id))
            print(f"  {ticker}: {total_oz} oz @ avg ${avg_cost:.2f}, "
                  f"live ${live_price:.2f}, "
                  f"P&L ${pnl:+.2f} ({pnl / total_cost * 100:+.1f}%)")

        updated += 1

    conn.commit()
    conn.close()
    print(f"Metals sync complete: {updated} position(s) updated at {now}")


if __name__ == "__main__":
    sync()
