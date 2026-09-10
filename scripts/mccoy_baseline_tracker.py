#!/usr/bin/env python3
"""HM-XO-PLAN-2026-09 Phase 0 — daily McCoy top-20-by-confidence vs SPY baseline.

Read-only: never writes to trader.db. Reads signals + Alpaca bars only, writes
a dated JSON report under data/reports/mccoy_baseline/. Exists to give every
later phase (structure changes, McCoy Rank, hedging) a pre-change baseline
number to compare against — see docs/XO_PLAN_2026-09.md.

Usage:
  python3 scripts/mccoy_baseline_tracker.py                # today (America/Phoenix)
  python3 scripts/mccoy_baseline_tracker.py --date 2026-09-08
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import statistics as st
import sys
from datetime import datetime, date as date_cls, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

REPO = Path(os.environ.get("BIGMAC_REPO", "/Users/bigmac/autonomous-trader"))
sys.path.insert(0, str(REPO))
import config  # noqa: E402 -- loads .env via dotenv for Alpaca creds

from alpaca.data.historical import StockHistoricalDataClient  # noqa: E402
from alpaca.data.requests import StockBarsRequest  # noqa: E402
from alpaca.data.timeframe import TimeFrame  # noqa: E402

DB_PATH = REPO / "data" / "trader.db"
OUT_DIR = REPO / "data" / "reports" / "mccoy_baseline"
MCCOY_ID = "ollama-plutus"
TOP_N = 20
NY = ZoneInfo("America/New_York")


def session_bounds_utc(d: date_cls) -> tuple[datetime, datetime]:
    """Regular session 09:30-16:00 America/New_York for date d, as UTC datetimes.
    DST-correct (unlike a hardcoded UTC offset) across the whole calendar year."""
    open_ny = datetime(d.year, d.month, d.day, 9, 30, tzinfo=NY)
    close_ny = datetime(d.year, d.month, d.day, 16, 0, tzinfo=NY)
    return open_ny.astimezone(timezone.utc), close_ny.astimezone(timezone.utc)


def fetch_top20(conn: sqlite3.Connection, open_utc: datetime, close_utc: datetime) -> list[dict]:
    open_s = open_utc.strftime("%Y-%m-%d %H:%M:%S")
    close_s = close_utc.strftime("%Y-%m-%d %H:%M:%S")
    rows = conn.execute(
        "SELECT symbol, created_at, confidence FROM signals "
        "WHERE player_id=? AND signal='BUY' AND created_at < ? "
        "  AND created_at >= datetime(?, '-16 hours') "  # allow pre-market same-session signals
        "ORDER BY confidence DESC",
        (MCCOY_ID, close_s, open_s),
    ).fetchall()
    best_by_symbol: dict[str, dict] = {}
    for symbol, created_at, confidence in rows:
        if symbol not in best_by_symbol or confidence > best_by_symbol[symbol]["confidence"]:
            best_by_symbol[symbol] = {"symbol": symbol, "ts": created_at, "confidence": confidence}
    ranked = sorted(best_by_symbol.values(), key=lambda r: -r["confidence"])
    return ranked[:TOP_N]


def fetch_bars(symbols: list[str], open_utc: datetime, close_utc: datetime) -> dict[str, list]:
    if not symbols:
        return {}
    client = StockHistoricalDataClient(os.environ["APCA_API_KEY_ID"], os.environ["APCA_API_SECRET_KEY"])
    req = StockBarsRequest(
        symbol_or_symbols=symbols,
        timeframe=TimeFrame.Minute,
        start=open_utc,
        end=close_utc,
        feed="iex",
    )
    try:
        resp = client.get_stock_bars(req)
    except Exception as e:
        print(f"[mccoy_baseline] bars fetch failed: {type(e).__name__}: {e}", file=sys.stderr)
        return {}
    out = {}
    for sym in symbols:
        bl = resp.data.get(sym, [])
        out[sym] = sorted(
            [(b.timestamp, b.open, b.close) for b in bl], key=lambda x: x[0]
        )
    return out


def entry_exit(bars: list, sig_ts_utc: datetime, close_utc: datetime):
    entry = None
    for t, o, c in bars:
        if t >= sig_ts_utc:
            entry = o
            break
    if entry is None and bars:
        entry = bars[-1][2]
    exitp = None
    for t, o, c in bars:
        if t <= close_utc:
            exitp = c
        else:
            break
    return entry, exitp


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="YYYY-MM-DD (America/Phoenix trading day); default today")
    args = ap.parse_args()

    if args.date:
        d = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        d = datetime.now(ZoneInfo("America/Phoenix")).date()

    open_utc, close_utc = session_bounds_utc(d)

    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    top20 = fetch_top20(conn, open_utc, close_utc)
    conn.close()

    if not top20:
        print(f"[mccoy_baseline] {d}: no McCoy BUY signals found (weekend/holiday/no-data) — skipping")
        return

    symbols = [r["symbol"] for r in top20] + ["SPY"]
    bars = fetch_bars(symbols, open_utc, close_utc)

    if not bars.get("SPY"):
        print(f"[mccoy_baseline] {d}: no SPY bar data (market likely closed) — skipping")
        return

    rets = []
    for r in top20:
        sig_ts = datetime.strptime(r["ts"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        entry, exitp = entry_exit(bars.get(r["symbol"], []), sig_ts, close_utc)
        if entry and exitp:
            r["entry"] = round(entry, 4)
            r["exit"] = round(exitp, 4)
            r["ret"] = (exitp - entry) / entry
            rets.append(r["ret"])
        else:
            r["entry"] = r["exit"] = r["ret"] = None

    spy_bars = bars["SPY"]
    spy_open = spy_bars[0][1]
    spy_close = None
    for t, o, c in spy_bars:
        if t <= close_utc:
            spy_close = c
    spy_ret = (spy_close - spy_open) / spy_open if spy_close else None

    mccoy_avg = st.mean(rets) if rets else None
    alpha = (mccoy_avg - spy_ret) if (mccoy_avg is not None and spy_ret is not None) else None

    report = {
        "date": str(d),
        "n_top20_scored": len(rets),
        "n_top20_requested": len(top20),
        "mccoy_avg_ret": mccoy_avg,
        "mccoy_median_ret": st.median(rets) if rets else None,
        "spy_ret": spy_ret,
        "alpha_vs_spy": alpha,
        "top20": top20,
    }

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / f"{d}.json"
    out_path.write_text(json.dumps(report, indent=2, default=str))

    print(f"[mccoy_baseline] {d}: McCoy top-{len(rets)} avg={mccoy_avg*100:+.3f}% "
          f"SPY={spy_ret*100:+.3f}% alpha={alpha*100:+.3f}%  -> {out_path}"
          if mccoy_avg is not None else
          f"[mccoy_baseline] {d}: no scoreable top-20 entries (bar data missing) — report written, no averages")


if __name__ == "__main__":
    main()
