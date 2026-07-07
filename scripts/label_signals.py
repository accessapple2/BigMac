#!/usr/bin/env python3
"""scripts/label_signals.py — P1 measurement layer, 2026-07-07.

Triple-barrier labeling backfill for every historical signal (deduped, all
agents) in signals_v2. Labels +1 (profit-take), -1 (stop), or 0 (time
barrier) using ATR(14)-scaled barriers on real Alpaca daily bars.

v1 defaults (logged per-row in params_json so a future re-label with
different params is traceable, not silently overwriting the convention
used here):
  - profit_take = entry + 2.0 * ATR(14)
  - stop        = entry - 1.0 * ATR(14)
  - time_barrier = 5 trading-ish days (calendar days, nearest-available-bar
    resolution -- same convention as scripts/counterfactual_report.py)
  - Same-day ambiguity (both barriers touched within one daily bar, which
    daily OHLC can't disambiguate without intraday data): STOP CHECKED
    FIRST, i.e. conservative/worst-case ordering. Documented, not hidden.

This produces the labels only -- NOT the meta-label gate or per-agent IC.
Those are separate, later work per the Admiral's explicit sequencing.

Resumable: signal_labels has a UNIQUE constraint on signal_id: re-running
only processes signals not already labeled. Commits every CHECKPOINT_EVERY
rows, so a kill/crash loses at most one checkpoint's worth of work, not the
whole run.

Usage:
    python3 scripts/label_signals.py                 # full backfill, resumable
    python3 scripts/label_signals.py --limit 500      # cap for a quick test
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import time
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

DB_PATH = ROOT / "data" / "trader.db"
ALPACA_BARS = "https://data.alpaca.markets/v2/stocks/{sym}/bars"

ATR_PERIOD = 14
PROFIT_TAKE_ATR_MULT = 2.0
STOP_ATR_MULT = 1.0
TIME_BARRIER_DAYS = 5
CHECKPOINT_EVERY = 200

PARAMS_JSON = (
    '{"atr_period":14,"profit_take_atr_mult":2.0,"stop_atr_mult":1.0,'
    '"time_barrier_days":5,"same_day_tiebreak":"stop_first","version":"v1"}'
)


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(str(DB_PATH), timeout=15)
    c.row_factory = sqlite3.Row
    return c


def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signal_labels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id INTEGER NOT NULL UNIQUE,
            symbol TEXT NOT NULL,
            entry_date TEXT NOT NULL,
            entry_price REAL NOT NULL,
            atr REAL,
            label INTEGER,
            barrier_hit TEXT,
            fwd_return REAL,
            params_json TEXT,
            computed_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_labels_symbol ON signal_labels(symbol)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_labels_label ON signal_labels(label)")
    conn.commit()


def _fetch_ohlc(symbol: str, start: str, end: str) -> dict[str, dict]:
    """{date: {o,h,l,c}} for one symbol, one wide window (cached by caller,
    NOT fetched per-signal)."""
    try:
        import requests
        key = os.environ.get("APCA_API_KEY_ID", "")
        secret = os.environ.get("APCA_API_SECRET_KEY", "")
        if not key or not secret:
            return {}
        r = requests.get(
            ALPACA_BARS.format(sym=symbol),
            headers={"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret},
            params={
                "timeframe": "1Day", "start": start, "end": end,
                "feed": "iex", "sort": "asc", "limit": 10000,
            },
            timeout=8,
        )
        if not r.ok:
            return {}
        bars = r.json().get("bars") or []
        return {
            (b.get("t") or "")[:10]: {"o": b["o"], "h": b["h"], "l": b["l"], "c": b["c"]}
            for b in bars if b.get("t")
        }
    except Exception:
        return {}


def _atr(ohlc: dict[str, dict], as_of_date: str, period: int = ATR_PERIOD) -> float | None:
    """Simple (non-Wilder-smoothed) ATR over the `period` bars ending on or
    before as_of_date. None if fewer than `period` prior bars exist."""
    dates = sorted(d for d in ohlc if d <= as_of_date)
    if len(dates) < period + 1:
        return None
    window = dates[-(period + 1):]
    trs = []
    for i in range(1, len(window)):
        cur, prev = ohlc[window[i]], ohlc[window[i - 1]]
        tr = max(
            cur["h"] - cur["l"],
            abs(cur["h"] - prev["c"]),
            abs(cur["l"] - prev["c"]),
        )
        trs.append(tr)
    return sum(trs) / len(trs) if trs else None


def _label_one(ohlc: dict[str, dict], entry_date: str, entry_price: float, atr: float) -> dict:
    profit_take = entry_price + PROFIT_TAKE_ATR_MULT * atr
    stop = entry_price - STOP_ATR_MULT * atr
    horizon_end = (date.fromisoformat(entry_date) + timedelta(days=TIME_BARRIER_DAYS)).isoformat()

    walk_dates = sorted(d for d in ohlc if entry_date < d <= horizon_end)
    for d in walk_dates:
        bar = ohlc[d]
        # Conservative same-day tiebreak: stop checked first (see module docstring).
        if bar["l"] <= stop:
            return {"label": -1, "barrier_hit": "stop", "fwd_return": round((stop - entry_price) / entry_price, 6)}
        if bar["h"] >= profit_take:
            return {"label": 1, "barrier_hit": "profit_take", "fwd_return": round((profit_take - entry_price) / entry_price, 6)}

    # Time barrier: use the last available close on/before horizon_end.
    time_dates = [d for d in ohlc if d <= horizon_end]
    if not time_dates:
        return {"label": 0, "barrier_hit": "time_no_data", "fwd_return": None}
    last_close = ohlc[max(time_dates)]["c"]
    return {"label": 0, "barrier_hit": "time", "fwd_return": round((last_close - entry_price) / entry_price, 6)}


def _pending_signals(conn: sqlite3.Connection, limit: int | None) -> list[sqlite3.Row]:
    q = """
        SELECT MIN(id) as signal_id, source, symbol, direction, signal_type,
               date(created_at) as entry_date
        FROM signals_v2
        WHERE symbol IS NOT NULL
        GROUP BY source, symbol, direction, signal_type, date(created_at)
        HAVING MIN(id) NOT IN (SELECT signal_id FROM signal_labels)
        ORDER BY entry_date ASC
    """
    if limit:
        q += f" LIMIT {int(limit)}"
    return conn.execute(q).fetchall()


def run(limit: int | None = None) -> dict:
    conn = _conn()
    _ensure_table(conn)

    pending = _pending_signals(conn, limit)
    print(f"Pending signals to label: {len(pending)}")
    if not pending:
        conn.close()
        return {"labeled": 0, "skipped_no_bars": 0}

    symbols = sorted({r["symbol"] for r in pending})
    min_date = min(r["entry_date"] for r in pending)
    bar_end = (date.today() + timedelta(days=TIME_BARRIER_DAYS + 3)).isoformat()
    # ATR needs ATR_PERIOD prior trading days before the EARLIEST signal too.
    bar_start = (date.fromisoformat(min_date) - timedelta(days=ATR_PERIOD * 2)).isoformat()

    print(f"Fetching OHLC bars for {len(symbols)} distinct symbols "
          f"({bar_start} to {bar_end})...")
    ohlc_cache: dict[str, dict[str, dict]] = {}
    for i, sym in enumerate(symbols):
        ohlc_cache[sym] = _fetch_ohlc(sym, bar_start, bar_end)
        if (i + 1) % 100 == 0:
            print(f"  ...{i + 1}/{len(symbols)} symbols fetched")
        time.sleep(0.05)

    have_bars = sum(1 for v in ohlc_cache.values() if v)
    print(f"Bars available for {have_bars}/{len(symbols)} symbols")

    labeled = 0
    skipped = 0
    batch = 0
    for row in pending:
        ohlc = ohlc_cache.get(row["symbol"], {})
        entry_date = row["entry_date"]
        entry_bar = ohlc.get(entry_date)
        if not entry_bar:
            # nearest prior bar as entry proxy (weekend/holiday signal timestamps)
            prior = sorted(d for d in ohlc if d <= entry_date)
            entry_bar = ohlc.get(prior[-1]) if prior else None
            entry_date_resolved = prior[-1] if prior else entry_date
        else:
            entry_date_resolved = entry_date

        if not entry_bar:
            skipped += 1
            continue

        entry_price = entry_bar["c"]
        atr = _atr(ohlc, entry_date_resolved)
        if atr is None or atr <= 0:
            skipped += 1
            continue

        result = _label_one(ohlc, entry_date_resolved, entry_price, atr)
        conn.execute(
            "INSERT OR IGNORE INTO signal_labels "
            "(signal_id, symbol, entry_date, entry_price, atr, label, barrier_hit, fwd_return, params_json) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (row["signal_id"], row["symbol"], entry_date_resolved, entry_price, atr,
             result["label"], result["barrier_hit"], result["fwd_return"], PARAMS_JSON),
        )
        labeled += 1
        batch += 1
        if batch >= CHECKPOINT_EVERY:
            conn.commit()
            print(f"  checkpoint: {labeled} labeled so far ({skipped} skipped, no bars/ATR)")
            batch = 0

    conn.commit()
    conn.close()
    print(f"\nDone. Labeled {labeled}, skipped {skipped} (no bars or insufficient ATR history).")
    return {"labeled": labeled, "skipped_no_bars": skipped}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    run(limit=args.limit)
