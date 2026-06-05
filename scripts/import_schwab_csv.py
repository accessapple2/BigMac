#!/usr/bin/env python3
"""Import a Schwab Positions CSV export into data/trader.db → schwab_holdings.

Usage:
    python3 scripts/import_schwab_csv.py /path/to/file.csv
    python3 scripts/import_schwab_csv.py --latest   # most recent Schwab CSV in ~/autonomous-trader/inbox
"""
from __future__ import annotations

import csv
import logging
import os
import re
import shutil
import sqlite3
import sys
import urllib.error
import urllib.request
from datetime import datetime
from decimal import InvalidOperation
from pathlib import Path

REPO_ROOT = Path(os.environ.get("BIGMAC_REPO", "/Users/bigmac/autonomous-trader"))
DB_PATH   = REPO_ROOT / "data" / "trader.db"
# HM-AT-β 2026-05-07: migrated off ~/Downloads/ for TCC (see docs/OPS_LOG.md)
INBOX     = REPO_ROOT / "inbox"
# HM-AY-α #3 (Scotty 2.4 sprint, 2026-05-07): malformed-CSV quarantine + delta guard
QUARANTINE = INBOX / "quarantine"
NTFY_TOPIC = "ollietrades-admin"
NTFY_URL   = f"https://ntfy.sh/{NTFY_TOPIC}"

# Required headers — without these the CSV cannot be meaningfully imported.
# Per HM-AY-α #3 audit: presence enforced upfront so we fail loudly with the
# missing column name, not silently with empty rows.
REQUIRED_COLUMNS = (
    "Symbol",
    "Mkt Val (Market Value)",
)

# Delta thresholds for sanity check (HM-AY-α #3).
# Reject (quarantine + ntfy) if new row count is < (1/RATIO_LO) × prior or > RATIO_HI × prior.
DELTA_RATIO_LO = 2.0   # new < prior/2.0 → suspect (e.g. ≥50% rows missing)
DELTA_RATIO_HI = 5.0   # new > prior*5.0 → suspect (e.g. junk rows added)

logger = logging.getLogger("schwab_import")


class SchwabCSVError(Exception):
    """Raised when a Schwab CSV is structurally unparseable."""

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
    """Returns (account_label, account_last4, snapshot_ts, snapshot_id, rows).

    HM-AY-α #3 hardened: empty-file guard, column-presence validation,
    per-row try/except so one malformed row does not crash the whole import.
    """
    text  = csv_path.read_text(encoding="utf-8-sig")
    lines = text.splitlines()

    # HM-AT-ε 2026-05-18: explicit empty-CSV guard before lines[0] indexing.
    # The downstream `len(lines) < 3` check already trips on empty input but
    # produces a less clear error; this surfaces the empty case distinctly.
    if not lines:
        raise SchwabCSVError(f"Empty CSV: {csv_path.name}")

    if len(lines) < 3:
        raise SchwabCSVError(
            f"CSV has only {len(lines)} line(s); need at least 3 "
            f"(title row, blank, headers, data)"
        )

    title = lines[0].strip().strip('"')
    if not title:
        raise SchwabCSVError("CSV title row (line 1) is empty")
    account_label, account_last4, snapshot_ts, snapshot_id = _parse_title(title)

    # Lines[2:] skips blank row 2; csv.DictReader handles quoted headers
    reader     = csv.DictReader(lines[2:])
    csv_source = csv_path.name

    # Column-presence validation against header row
    header_row = reader.fieldnames or []
    missing = [c for c in REQUIRED_COLUMNS if c not in header_row]
    if missing:
        raise SchwabCSVError(
            f"CSV missing required column(s): {missing}. "
            f"Got headers: {header_row[:8]}{'...' if len(header_row) > 8 else ''}"
        )

    rows: list[dict] = []
    bad_rows = 0
    for row_idx, raw in enumerate(reader, start=3):  # row index = file line number
        try:
            row = _build_row(raw, snapshot_id, snapshot_ts,
                             account_label, account_last4, csv_source)
        except (ValueError, KeyError, InvalidOperation, AttributeError) as e:
            bad_rows += 1
            sym = (raw or {}).get("Symbol", "<?>")
            logger.warning(
                "[skip] row %d (Symbol=%r) parse failed: %s: %s",
                row_idx, sym, type(e).__name__, e,
            )
            continue
        if row is not None:
            rows.append(row)

    if bad_rows:
        logger.info("parse_csv: %d malformed row(s) skipped", bad_rows)

    return account_label, account_last4, snapshot_ts, snapshot_id, rows

# ─── HM-AY-α #3: ntfy + delta-check + quarantine helpers ──────────────────────

def _ntfy(message: str, priority: str = "default") -> None:
    """Best-effort POST to ntfy.sh/ollietrades-admin. Never raises."""
    try:
        req = urllib.request.Request(
            NTFY_URL,
            data=message.encode("utf-8"),
            headers={"Priority": priority},
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            resp.read()
    except (urllib.error.URLError, OSError) as e:
        logger.debug("ntfy post failed (non-fatal): %s", e)


def _ensure_meta_table(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS schwab_holdings_meta (
            key                TEXT PRIMARY KEY,
            value              TEXT,
            updated_at         TEXT NOT NULL DEFAULT (datetime('now'))
        );
    """)
    conn.commit()


def _get_meta_int(conn: sqlite3.Connection, key: str) -> int | None:
    row = conn.execute(
        "SELECT value FROM schwab_holdings_meta WHERE key = ?", (key,)
    ).fetchone()
    if row is None:
        return None
    try:
        return int(row[0])
    except (TypeError, ValueError):
        return None


def _set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO schwab_holdings_meta (key, value, updated_at) "
        "VALUES (?, ?, datetime('now')) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value, "
        "updated_at=excluded.updated_at",
        (key, value),
    )
    conn.commit()


def _check_delta(prior_count: int | None, new_count: int) -> tuple[bool, str]:
    """Returns (is_sane, reason). is_sane=False means quarantine + ntfy."""
    if prior_count is None or prior_count == 0:
        return True, "no prior baseline"
    if new_count <= 1:
        return True, "flat portfolio — 0 or 1 rows (cash-only); treating as legitimate empty import"
    if new_count < prior_count / DELTA_RATIO_LO:
        return False, (
            f"row count {new_count} < {prior_count}/{DELTA_RATIO_LO:g} "
            f"(suspected truncated CSV)"
        )
    if new_count > prior_count * DELTA_RATIO_HI:
        return False, (
            f"row count {new_count} > {prior_count}*{DELTA_RATIO_HI:g} "
            f"(suspected junk rows)"
        )
    return True, f"delta {new_count - prior_count:+d} from prior {prior_count}"


def _quarantine(csv_path: Path, reason: str) -> Path:
    QUARANTINE.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = QUARANTINE / f"{ts}_{csv_path.name}"
    shutil.move(str(csv_path), str(dest))
    (dest.parent / f"{dest.name}.reason.txt").write_text(
        f"{datetime.now().isoformat()}\n{reason}\n", encoding="utf-8"
    )
    return dest


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
        _ensure_meta_table(conn)
        conn.commit()

        # HM-AY-α #3 delta-check vs last successful import
        prior = _get_meta_int(conn, "last_import_row_count")
        sane, delta_reason = _check_delta(prior, len(rows))
        if not sane:
            quarantined = _quarantine(csv_path, f"delta-check failed: {delta_reason}")
            msg = f"❌ Schwab CSV quarantined: {csv_path.name} — {delta_reason}. Moved to {quarantined}"
            print(f"  [QUARANTINE] {msg}")
            _ntfy(msg, priority="high")
            raise SchwabCSVError(msg)
        else:
            print(f"  Delta-check : OK ({delta_reason})")
            if len(rows) <= 1:
                _ntfy(
                    "⚠️ Schwab import: flat portfolio detected — 0 positions imported "
                    f"({csv_path.name}). Verify this is a genuine empty account, not a truncated export.",
                    priority="default",
                )
                print("  [FLAT] flat-portfolio import — advisory ntfy sent")

        inserted = skipped = 0
        for row in rows:
            cur = conn.execute(INSERT_SQL, row)
            if cur.rowcount > 0:
                inserted += 1
            else:
                skipped += 1
        conn.commit()
        _set_meta(conn, "last_import_row_count", str(len(rows)))
        _set_meta(conn, "last_import_snapshot_id", snapshot_id)
        _set_meta(conn, "last_import_csv_name", csv_path.name)

        print(f"  Inserted : {inserted}  |  Already existed (ignored): {skipped}")
        delta_str = f" Δ {len(rows) - (prior or len(rows)):+d}" if prior is not None else ""
        _ntfy(
            f"📊 Schwab CSV imported: {len(rows)} rows{delta_str} "
            f"({inserted} new, {skipped} dup) snapshot={snapshot_id}"
        )

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
        INBOX.glob("Sc*Positions*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(
            f"No Schwab Positions CSV found in {INBOX}  (pattern: Sc*Positions*.csv)"
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
        msg = f"file not found: {csv_path}"
        print(f"ERROR: {msg}", file=sys.stderr)
        _ntfy(f"❌ Schwab CSV import failed: {msg}", priority="high")
        sys.exit(1)

    try:
        import_csv(csv_path)
    except SchwabCSVError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        _ntfy(f"❌ Schwab CSV import failed: {csv_path.name}: {e}", priority="high")
        sys.exit(2)
    except Exception as e:
        # Last-resort guard: any unexpected exception still notifies before re-raise.
        _ntfy(
            f"❌ Schwab CSV import crashed: {csv_path.name}: {type(e).__name__}: {e!r}",
            priority="high",
        )
        raise

    # Kirk-Schwab-realign-2026-05-05: claim now accurate (was stale before
    # Admiral Option A realign — Kirk was reading alpaca-mirror paper).
    # Auto-sync to real_holdings.json (Kirk Advisory data source).
    try:
        import subprocess
        result = subprocess.run(
            [sys.executable, str(REPO_ROOT / "scripts" / "sync_schwab_to_real_holdings.py")],
            capture_output=True, text=True, timeout=15
        )
        if result.returncode == 0:
            print()
            print(result.stdout.strip())
        else:
            print(f"  [warn] sync_schwab_to_real_holdings failed: {result.stderr.strip()}")
    except Exception as e:
        print(f"  [warn] sync hook error: {e}")


if __name__ == "__main__":
    main()
