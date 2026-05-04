#!/usr/bin/env python3
"""Import a Schwab Positions CSV export into data/trader.db → schwab_holdings.

Usage:
    python3 scripts/import_schwab_csv.py /path/to/file.csv
    python3 scripts/import_schwab_csv.py --latest   # most recent Schwab CSV in ~/Downloads
"""
from __future__ import annotations

import csv
import os
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(os.environ.get("BIGMAC_REPO", "/Users/bigmac/autonomous-trader"))
DB_PATH   = REPO_ROOT / "data" / "trader.db"
DOWNLOADS = Path.home() / "Downloads"

# ─── Schema ───────────────────────────────────────────────────────────────────

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS schwab_holdings (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id         TEXT    NOT NULL,
    snapshot_ts         TEXT    NOT NULL,
    account_label       TEXT    NOT NULL,
    account_last4       TEXT    NOT NULL,
    symbol              TEXT    NOT NULL,
    description         TEXT,
    qty                 REAL,
    price               REAL,
    market_value        REAL,
    cost_basis          REAL,
    gain_dollar         REAL,
    gain_pct            REAL,
    day_change_dollar   REAL,
    day_change_pct      REAL,
    price_change_dollar REAL,
    price_change_pct    REAL,
    asset_type          TEXT    NOT NULL,
    reinvest            TEXT,
    reinvest_cap_gains  TEXT,
    is_summary_row      INTEGER NOT NULL DEFAULT 0,
    imported_at         TEXT    NOT NULL DEFAULT (datetime('now')),
    csv_source_path     TEXT    NOT NULL,
    UNIQUE(snapshot_id, symbol)
);
CREATE INDEX IF NOT EXISTS ix_schwab_holdings_snapshot
    ON schwab_holdings(snapshot_id);
CREATE INDEX IF NOT EXISTS ix_schwab_holdings_symbol
    ON schwab_holdings(symbol, snapshot_ts DESC);
CREATE INDEX IF NOT EXISTS idx_schwab_holdings_summary
    ON schwab_holdings(is_summary_row);
"""

INSERT_SQL = """
INSERT OR IGNORE INTO schwab_holdings (
    snapshot_id, snapshot_ts, account_label, account_last4,
    symbol, description, qty, price, market_value, cost_basis,
    gain_dollar, gain_pct, day_change_dollar, day_change_pct,
    price_change_dollar, price_change_pct,
    asset_type, reinvest, reinvest_cap_gains,
    is_summary_row, csv_source_path
) VALUES (
    :snapshot_id, :snapshot_ts, :account_label, :account_last4,
    :symbol, :description, :qty, :price, :market_value, :cost_basis,
    :gain_dollar, :gain_pct, :day_change_dollar, :day_change_pct,
    :price_change_dollar, :price_change_pct,
    :asset_type, :reinvest, :reinvest_cap_gains,
    :is_summary_row, :csv_source_path
)
"""

# ─── Title-row parser ─────────────────────────────────────────────────────────

def _parse_title(title: str) -> tuple[str, str, str, str]:
    """
    'Positions for account Scwab New BS ...015 as of 12:48 PM ET, 2026/04/24'
    -> (account_label, account_last4, snapshot_ts, snapshot_id)
    """
    acct_m  = re.search(r"account\s+(.+?)\s+\.\.\.", title)
    last4_m = re.search(r"\.\.\.(\w+)", title)
    ts_m    = re.search(r"as of\s+(.+)", title)

    account_label = acct_m.group(1).strip()  if acct_m  else "Unknown"
    account_last4 = last4_m.group(1)         if last4_m else ""
    snapshot_ts   = ts_m.group(1).strip()    if ts_m    else ""

    # snapshot_id: ISO datetime to minute precision
    # "12:48 PM ET, 2026/04/24"  ->  "2026-04-24T12:48:00"
    snapshot_id = snapshot_ts
    try:
        cleaned = re.sub(r"\s+ET,?\s*", " ", snapshot_ts).strip()
        dt = datetime.strptime(cleaned, "%I:%M %p %Y/%m/%d")
        snapshot_id = dt.strftime("%Y-%m-%dT%H:%M:00")
    except ValueError:
        pass

    return account_label, account_last4, snapshot_ts, snapshot_id

# ─── Field cleaners ───────────────────────────────────────────────────────────

_DASH = {"--", "", "N/A", "n/a"}

def _money(val: str) -> float | None:
    v = val.strip().replace(",", "").replace("$", "")
    if v in _DASH:
        return None
    try:
        return float(v)
    except ValueError:
        return None

def _pct(val: str) -> float | None:
    """Stored as 14.8, not 0.148."""
    v = val.strip().replace("%", "")
    if v in _DASH:
        return None
    try:
        return float(v)
    except ValueError:
        return None

def _qty(val: str) -> float | None:
    v = val.strip().replace(",", "")
    if v in _DASH:
        return None
    try:
        return float(v)
    except ValueError:
        return None

# ─── Row builder ──────────────────────────────────────────────────────────────

_SUMMARY_SYMBOLS = {"Positions Total", "Account Total", "Total"}
_CASH_SYMBOL     = "Cash & Cash Investments"

def _build_row(raw: dict, snapshot_id: str, snapshot_ts: str,
               account_label: str, account_last4: str,
               csv_source: str) -> dict | None:
    """Returns insert-ready dict, or None to skip."""
    symbol_raw = raw.get("Symbol", "").strip().strip('"')
    if not symbol_raw:
        return None

    if symbol_raw in _SUMMARY_SYMBOLS:
        print(f"  [skip] summary row: {symbol_raw!r}")
        return None

    base = dict(
        snapshot_id=snapshot_id, snapshot_ts=snapshot_ts,
        account_label=account_label, account_last4=account_last4,
        csv_source_path=csv_source,
        is_summary_row=0,
    )

    if symbol_raw == _CASH_SYMBOL:
        return {
            **base,
            "symbol":               "CASH",
            "description":          "Cash & Cash Investments",
            "qty":                  None,
            "price":                None,
            "market_value":         _money(raw.get("Mkt Val (Market Value)", "--")),
            "cost_basis":           None,
            "gain_dollar":          None,
            "gain_pct":             None,
            "day_change_dollar":    _money(raw.get("Day Chng $ (Day Change $)", "--")),
            "day_change_pct":       _pct(raw.get("Day Chng % (Day Change %)", "--")),
            "price_change_dollar":  None,
            "price_change_pct":     None,
            "asset_type":           "Cash and Money Market",
            "reinvest":             None,
            "reinvest_cap_gains":   None,
        }

    return {
        **base,
        "symbol":               symbol_raw,
        "description":          raw.get("Description", "").strip().strip('"') or None,
        "qty":                  _qty(raw.get("Qty (Quantity)", "--")),
        "price":                _money(raw.get("Price", "--")),
        "market_value":         _money(raw.get("Mkt Val (Market Value)", "--")),
        "cost_basis":           _money(raw.get("Cost Basis", "--")),
        "gain_dollar":          _money(raw.get("Gain $ (Gain/Loss $)", "--")),
        "gain_pct":             _pct(raw.get("Gain % (Gain/Loss %)", "--")),
        "day_change_dollar":    _money(raw.get("Day Chng $ (Day Change $)", "--")),
        "day_change_pct":       _pct(raw.get("Day Chng % (Day Change %)", "--")),
        "price_change_dollar":  _money(raw.get("Price Chng $ (Price Change $)", "--")),
        "price_change_pct":     _pct(raw.get("Price Chng % (Price Change %)", "--")),
        "asset_type":           raw.get("Asset Type", "").strip().strip('"') or "Equity",
        "reinvest":             raw.get("Reinvest?", "").strip().strip('"') or None,
        "reinvest_cap_gains":   raw.get("Reinvest Capital Gains?", "").strip().strip('"') or None,
    }

# ─── CSV parser ───────────────────────────────────────────────────────────────

def parse_csv(csv_path: Path) -> tuple[str, str, str, str, list[dict]]:
    """Returns (account_label, account_last4, snapshot_ts, snapshot_id, rows)."""
    text  = csv_path.read_text(encoding="utf-8-sig")
    lines = text.splitlines()

    title = lines[0].strip().strip('"')
    account_label, account_last4, snapshot_ts, snapshot_id = _parse_title(title)

    # Lines[2:] skips blank row 2; csv.DictReader handles quoted headers
    reader     = csv.DictReader(lines[2:])
    csv_source = csv_path.name

    rows = []
    for raw in reader:
        row = _build_row(raw, snapshot_id, snapshot_ts,
                         account_label, account_last4, csv_source)
        if row is not None:
            rows.append(row)
    return account_label, account_last4, snapshot_ts, snapshot_id, rows

# ─── Import ───────────────────────────────────────────────────────────────────

def import_csv(csv_path: Path) -> None:
    print(f"Importing: {csv_path.name}")

    account_label, account_last4, snapshot_ts, snapshot_id, rows = parse_csv(csv_path)
    print(f"  Account  : {account_label} (...{account_last4})")
    print(f"  Snapshot : {snapshot_ts}  ->  id={snapshot_id!r}")
    print(f"  Rows     : {len(rows)} (equities + cash)")

    conn = sqlite3.connect(str(DB_PATH))
    try:
        conn.executescript(CREATE_SQL)
        conn.commit()

        inserted = skipped = 0
        for row in rows:
            cur = conn.execute(INSERT_SQL, row)
            if cur.rowcount > 0:
                inserted += 1
            else:
                skipped += 1
        conn.commit()

        print(f"  Inserted : {inserted}  |  Already existed (ignored): {skipped}")

        total_rows = conn.execute(
            "SELECT COUNT(*) FROM schwab_holdings WHERE snapshot_id = ?",
            (snapshot_id,)
        ).fetchone()[0]
        print(f"\n  schwab_holdings rows for snapshot: {total_rows}")
        print(f"\n  {'Symbol':<8} {'Market Value':>14} {'Gain %':>9} {'Day Chg %':>10}")
        print(f"  {'-'*45}")

        portfolio_total = 0.0
        cur = conn.execute(
            "SELECT symbol, market_value, gain_pct, day_change_pct "
            "FROM schwab_holdings "
            "WHERE snapshot_id = ? "
            "ORDER BY CASE symbol WHEN 'CASH' THEN 'ZZZ' ELSE symbol END",
            (snapshot_id,)
        )
        for sym, mv, gp, dp in cur.fetchall():
            mv_s = f"${mv:,.2f}"  if mv is not None else "        N/A"
            gp_s = f"{gp:+.2f}%" if gp is not None else "      N/A"
            dp_s = f"{dp:+.2f}%" if dp is not None else "      N/A"
            print(f"  {sym:<8} {mv_s:>14} {gp_s:>9} {dp_s:>10}")
            if mv:
                portfolio_total += mv

        print(f"  {'-'*45}")
        print(f"  {'TOTAL':<8} ${portfolio_total:>13,.2f}")

    finally:
        conn.close()

# ─── Entry point ──────────────────────────────────────────────────────────────

def _find_latest() -> Path:
    candidates = sorted(
        DOWNLOADS.glob("Sc*Positions*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(
            f"No Schwab Positions CSV found in {DOWNLOADS}  (pattern: Sc*Positions*.csv)"
        )
    return candidates[0]


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    if sys.argv[1] == "--latest":
        csv_path = _find_latest()
        print(f"[--latest] using: {csv_path.name}")
    else:
        csv_path = Path(sys.argv[1])

    if not csv_path.exists():
        print(f"ERROR: file not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    import_csv(csv_path)


if __name__ == "__main__":
    main()
