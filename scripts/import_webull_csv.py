"""Import Webull trade history CSV into trades table as steve-webull (Captain Kirk)."""
import csv
import sqlite3
from pathlib import Path
from datetime import datetime

DB = Path(__file__).parent.parent / "data" / "trader.db"
PLAYER_ID = "steve-webull"


def parse_date(d: str) -> str:
    """Normalize date to ISO format."""
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(d.strip(), fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    return d.strip()


def import_csv(csv_path: str) -> dict:
    conn = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")

    imported = skipped = dupes = 0

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            symbol = (row.get("Symbol") or "").strip().upper()
            side = (row.get("Side") or "").strip().upper()
            qty = row.get("Quantity") or row.get("Qty") or "0"
            price = row.get("Price") or "0"
            trade_date = row.get("Trade_Date") or row.get("TradeDate") or row.get("Date") or ""
            asset_type = (row.get("Asset_Type") or row.get("AssetType") or "stock").strip().lower()

            if not symbol or not side:
                skipped += 1
                continue

            action = "BUY" if side == "BUY" else "SELL"
            executed_at = parse_date(trade_date) if trade_date else datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            try:
                qty_val = float(qty)
                price_val = float(price)
            except ValueError:
                skipped += 1
                continue

            # Deduplicate: same symbol + action + price + date + qty
            existing = conn.execute(
                "SELECT 1 FROM trades WHERE player_id=? AND symbol=? AND action=? AND price=? AND qty=? AND date(executed_at)=date(?)",
                (PLAYER_ID, symbol, action, price_val, qty_val, executed_at)
            ).fetchone()
            if existing:
                dupes += 1
                continue

            conn.execute(
                """INSERT INTO trades (player_id, symbol, action, qty, price, asset_type, reasoning, executed_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (PLAYER_ID, symbol, action, qty_val, price_val, asset_type,
                 "Imported from Webull CSV", executed_at)
            )
            imported += 1

    conn.commit()
    conn.close()
    return {"imported": imported, "skipped": skipped, "dupes": dupes}


if __name__ == "__main__":
    import sys
    import glob as _glob

    if len(sys.argv) > 1:
        path = sys.argv[1]
    else:
        matches = sorted(_glob.glob(str(Path(__file__).parent.parent / "data/imports/*.csv")))
        if not matches:
            print("No CSV found in data/imports/")
            sys.exit(1)
        path = matches[-1]

    print(f"Importing: {path}")
    result = import_csv(path)
    print(f"Done — imported: {result['imported']}, dupes: {result['dupes']}, skipped: {result['skipped']}")
