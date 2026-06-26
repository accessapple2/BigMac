#!/usr/bin/env python3
"""
Scotty Kirk-Scan Ingestion — daily 06:00 AZ (after newsletter lands)

Usage:
    python3 scripts/scotty_kirk_ingest.py newsletter.txt
    cat newsletter.txt | python3 scripts/scotty_kirk_ingest.py -
    python3 scripts/scotty_kirk_ingest.py --file data/kirk/2026-06-22.txt

Behaviour:
    1. Parse OBSERVATION_BLOCK from newsletter text.
       Hard-fail + NTFY if the block is absent (June-20 silent-failure mode).
    2. Stamp entry_price_anchor from Polygon /v2/aggs/ticker/{sym}/prev.
    3. Insert into signal_observations (data/trader.db), is_context=1.
    4. Log row count; NTFY on zero rows inserted.
    5. Idempotent: skips tickers already inserted today for source=kirk_super_scan.
"""

from __future__ import annotations
import json
import os
import re
import sqlite3
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

BASE_DIR   = Path(__file__).resolve().parent.parent
TRADER_DB  = BASE_DIR / "data" / "trader.db"
SOURCE     = "kirk_super_scan"

# ── Polygon ───────────────────────────────────────────────────────────────────
_POLYGON_KEY = os.environ.get("POLYGON_API_KEY", "")

def _polygon_prev_close(ticker: str) -> float | None:
    """Return previous trading-day close from Polygon /prev endpoint."""
    if not _POLYGON_KEY:
        print(f"[scotty-kirk] POLYGON_API_KEY missing — anchor null for {ticker}")
        return None
    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{urllib.parse.quote(ticker)}"
        f"/prev?adjusted=true&apiKey={_POLYGON_KEY}"
    )
    try:
        with urllib.request.urlopen(url, timeout=8) as r:
            data = json.loads(r.read())
        results = data.get("results", [])
        if results:
            return float(results[0]["c"])
    except Exception as e:
        print(f"[scotty-kirk] Polygon error for {ticker}: {e}")
    return None


# ── NTFY ─────────────────────────────────────────────────────────────────────
def _ntfy(title: str, body: str, priority: str = "high") -> None:
    try:
        from engine.alert_channels import send_alert, AlertLevel
        send_alert(message=f"{title}\n{body}", level=AlertLevel.WARNING,
                   alert_type="scotty-kirk-ingest")
        return
    except Exception:
        pass
    try:
        topic = os.environ.get("NTFY_TOPIC", "ollietrades-admin")
        req = urllib.request.Request(
            f"https://ntfy.sh/{topic}",
            data=body.encode(),
            headers={"Title": title, "Priority": priority, "Tags": "warning,kirk"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        print(f"[scotty-kirk] NTFY fallback failed: {e}", file=sys.stderr)


# ── Block parser ──────────────────────────────────────────────────────────────
_FENCE_RE = re.compile(
    r"```json\s+OBSERVATION_BLOCK\s*\n(.*?)```",
    re.DOTALL | re.IGNORECASE,
)

def _extract_block(text: str) -> dict:
    """
    Find and parse the OBSERVATION_BLOCK fence.
    Raises SystemExit(1) with NTFY alert if absent or malformed.
    """
    m = _FENCE_RE.search(text)
    if not m:
        msg = "OBSERVATION_BLOCK fence not found in newsletter."
        print(f"[scotty-kirk] HARD FAIL: {msg}", file=sys.stderr)
        _ntfy("KIRK BLOCK MISSING", msg + " Ingestion aborted — check Grok output.", "urgent")
        sys.exit(1)
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError as e:
        msg = f"OBSERVATION_BLOCK JSON invalid: {e}"
        print(f"[scotty-kirk] HARD FAIL: {msg}", file=sys.stderr)
        _ntfy("KIRK BLOCK MALFORMED", msg, "urgent")
        sys.exit(1)


# ── Normalise direction ───────────────────────────────────────────────────────
def _dir(d: str) -> str:
    return "SHORT" if str(d).lower() == "short" else "LONG"


# ── Already ingested today? ───────────────────────────────────────────────────
def _already_in(cur: sqlite3.Cursor, ticker: str, today: str) -> bool:
    cur.execute(
        "SELECT 1 FROM signal_observations WHERE source=? AND ticker=? AND date(ts)=? LIMIT 1",
        (SOURCE, ticker, today),
    )
    return cur.fetchone() is not None


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> int:
    # ── Read newsletter text ──────────────────────────────────────────────────
    args = sys.argv[1:]
    try:
        if not args or args[0] == "-":
            text = sys.stdin.read()
        elif args[0] in ("--file", "-f") and len(args) >= 2:
            text = Path(args[1]).read_text()
        else:
            path = Path(args[0])
            if not path.exists():
                msg = f"Newsletter file not found: {path}"
                print(f"[scotty-kirk] HARD FAIL: {msg}", file=sys.stderr)
                _ntfy("KIRK FILE MISSING", msg + " Ingest did not run.", "urgent")
                sys.exit(1)
            text = path.read_text()
    except OSError as e:
        msg = f"Cannot read newsletter: {e}"
        print(f"[scotty-kirk] HARD FAIL: {msg}", file=sys.stderr)
        _ntfy("KIRK FILE UNREADABLE", msg, "urgent")
        sys.exit(1)

    # ── Parse block ───────────────────────────────────────────────────────────
    block = _extract_block(text)
    obs_list = block.get("observations", [])
    if not obs_list:
        msg = "OBSERVATION_BLOCK.observations is empty."
        _ntfy("KIRK BLOCK EMPTY", msg, "high")
        print(f"[scotty-kirk] WARNING: {msg}")
        return 0

    meta         = block.get("meta", {})
    captured_at  = meta.get("captured_at", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    baseline_end = (datetime.fromisoformat(captured_at) + timedelta(days=30)).date()

    ts_now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
    today  = ts_now[:10]

    # ── DB connect ────────────────────────────────────────────────────────────
    con = sqlite3.connect(str(TRADER_DB))
    con.execute("PRAGMA journal_mode=WAL")
    cur = con.cursor()

    inserted = skipped = anchored = 0

    for o in obs_list:
        ticker = o.get("ticker", "").strip().upper()
        if not ticker:
            continue

        if _already_in(cur, ticker, today):
            print(f"[scotty-kirk] skip {ticker} — already ingested today")
            skipped += 1
            continue

        horizon_days = int(o.get("horizon_days", 10))
        horizon_date = (datetime.fromisoformat(captured_at).date()
                        + timedelta(days=horizon_days))
        scoreable = bool(o.get("scoreable_in_window", True)) and (
            horizon_date <= baseline_end
        )
        expiry_str = (
            datetime.combine(horizon_date, datetime.min.time())
            .replace(tzinfo=timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
        )

        # Polygon stamp — skip for synthetic/non-exchange tickers
        non_exchange = {"GOLD_SPOT", "SILVER_SPOT", "WTI"}
        if ticker in non_exchange:
            anchor = None
        else:
            anchor = _polygon_prev_close(ticker)
            if anchor is not None:
                anchored += 1

        confluence = {
            "claim_type":          o.get("claim_type"),
            "thesis":              o.get("thesis"),
            "reference_price":     o.get("reference_price"),   # Kirk's claimed price
            "entry_price_anchor":  anchor,                     # Polygon-confirmed
            "entry_zone":          o.get("entry_zone"),
            "stop":                o.get("stop"),
            "target":              o.get("target"),
            "spread_strikes":      o.get("spread_strikes"),
            "dte":                 o.get("dte"),
            "short_pct_float":     o.get("short_pct_float"),
            "horizon_days":        horizon_days,
            "specificity":         o.get("specificity", "low"),
            "scoreable_in_window": scoreable,
            "baseline_window":     meta.get("baseline_window", True),
            "execution":           "NONE",
        }

        cur.execute("""
            INSERT INTO signal_observations
                (ts, source, ticker, direction, conviction, grade,
                 confluence_meta, expiry, is_context, acted_by_fleet,
                 fleet_trade_id, fwd_return_1h, fwd_return_1d, fwd_return_exp, evaluated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            ts_now, SOURCE, ticker, _dir(o.get("direction", "long")),
            None, None,
            json.dumps(confluence),
            expiry_str,
            1, 0,
            None, None, None, None, None,
        ))
        inserted += 1

    con.commit()
    con.close()

    print(f"[scotty-kirk] done — inserted={inserted} skipped={skipped} anchored={anchored}/{inserted}")

    if inserted == 0 and skipped == 0:
        _ntfy("KIRK INGEST ZERO ROWS", "No observations inserted or skipped — block may be empty.", "high")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
