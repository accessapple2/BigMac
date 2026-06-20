"""
engine/signals.py — per-symbol strategy signals for the cockpit stations
========================================================================

Reads your REAL scanner tables (not a single `signals` table — you don't have
one). Maps each scanner family to a cockpit weapon station and returns the
4-station contract Scan Algos consumes, plus real last-price from price_ticks.

  s1 PHASER BANK  ← bk_orb_signals      (opening-range breakouts)
  s4 TACTICAL     ← bk_box_signals + bk_avwap_signals  (box / AVWAP technicals)
  s2 PHOTON BAY   ← <options entry table — TELL ME WHICH>   (unmapped for now)
  s3 SENSOR ARRAY ← <news/ML table — TELL ME WHICH>          (unmapped for now)

Each station shows the most-recent signal for the symbol. Side is parsed from
the table's `signal` text; entry/stop come from that scanner's price columns;
target is a 2R projection; qty is a ~$2k-notional placeholder until you wire a
real sizing rule.

GET /api/signals/{symbol} → {"symbol","telem":{"last":..},"signals":{s1..s4}}

Mount:  from engine.signals import router as signals_router
        app.include_router(signals_router)
"""
from __future__ import annotations

import os
import sqlite3
from fastapi import APIRouter

DB_PATH = os.getenv("OT_DB", os.path.join(
    os.getenv("OT_ROOT", "/Users/bigmac/autonomous-trader"), "data", "trader.db"))

# station → ordered list of source specs (first one with a fresh signal wins)
SOURCES = {
    "s1": [
        {"table": "bk_orb_signals", "ts": "created_at", "sym": "symbol", "sig": "signal",
         "entry": "break_price", "long_stop": "or_low", "short_stop": "or_high"},
    ],
    "s4": [
        {"table": "bk_box_signals", "ts": "created_at", "sym": "symbol", "sig": "signal",
         "entry": "box_top", "long_stop": "box_bottom", "short_stop": "box_top"},
        {"table": "bk_avwap_signals", "ts": "created_at", "sym": "symbol", "sig": "signal",
         "entry": "close", "long_stop": "avwap_price", "short_stop": "avwap_price"},
    ],
    # "s2": [...options entry table...],
    # "s3": [...news/ML table...],
}

LONG_HINTS  = ("long", "buy", "bull", "up", "break", "breakout", "above", "reclaim")
SHORT_HINTS = ("short", "sell", "bear", "down", "below", "breakdown", "lose")
NO_SIGNAL   = ("", "none", "neutral", "flat", "no_signal", "hold", "0")

router = APIRouter()


def _conn():
    return sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, check_same_thread=False, timeout=2)


def _last_price(conn, sym):
    try:
        r = conn.execute("SELECT price FROM price_ticks WHERE symbol=? ORDER BY id DESC LIMIT 1",
                         (sym,)).fetchone()
        return float(r[0]) if r else None
    except Exception:
        return None


def _eval_source(conn, spec, sym, last):
    q = (f"SELECT {spec['sig']} AS sig, {spec['entry']} AS entry, "
         f"{spec['long_stop']} AS lstop, {spec['short_stop']} AS sstop, {spec['ts']} AS ts "
         f"FROM {spec['table']} WHERE {spec['sym']}=? ORDER BY {spec['ts']} DESC LIMIT 1")
    try:
        row = conn.execute(q, (sym,)).fetchone()
    except Exception:
        return None
    if not row:
        return None
    sig = str(row[0] or "").strip().lower()
    if not sig or sig in NO_SIGNAL:
        return None
    is_short = any(h in sig for h in SHORT_HINTS) and not any(h in sig for h in LONG_HINTS)
    side = "SHORT" if is_short else "LONG"
    entry = row[1] if row[1] is not None else last
    stop  = row[2] if side == "LONG" else row[3]
    target, rr, qty = None, "—", None
    try:
        e, s = float(entry), float(stop)
        risk = abs(e - s) or (e * 0.01)
        target = round(e + 2 * risk, 2) if side == "LONG" else round(e - 2 * risk, 2)
        rr = "2.0"
        qty = max(1, round(2000 / e))      # ~$2k notional placeholder
    except (TypeError, ValueError):
        pass
    return {"locked": True, "side": side, "qty": qty, "entry": entry, "stop": stop,
            "target": target, "rr": rr, "signal": str(row[0]), "src": spec["table"], "ts": row[4]}


def _load_signals(conn, sym, last):
    out = {}
    for stn, specs in SOURCES.items():
        hit = None
        for spec in specs:
            hit = _eval_source(conn, spec, sym, last)
            if hit:
                break
        out[stn] = hit or {"locked": False}
    for stn in ("s1", "s2", "s3", "s4"):     # stations with no source yet → no lock
        out.setdefault(stn, {"locked": False})
    return out


@router.get("/api/signals/{symbol}")
def signals(symbol: str):
    sym = symbol.upper()
    conn = _conn()
    try:
        last = _last_price(conn, sym)
        sigs = _load_signals(conn, sym, last)
    finally:
        conn.close()
    return {"symbol": sym, "telem": ({"last": last} if last is not None else None), "signals": sigs}
