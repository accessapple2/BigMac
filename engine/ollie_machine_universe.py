"""Ollie Machine — P2b tradeable-universe filter (applied PRE-VOTE).

The broad signal-native universe (squeeze_watch ∪ minervini_trend ∪ rs_rank) lets
deal-pending grinders (CNTA/SILA — ~0.7% monthly range) and structural junk
(leveraged single-stock ETFs, penny names, bad trade-levels marks) spoof the
≥2-of-4 convergence doctrine. This module screens the candidate set to a
*tradeable* universe BEFORE the vote, so the picks are real momentum names.

Filters (all bounded — no unbounded per-symbol fetch):
  • frozen      — 20-30d high-low range% < FROZEN_RANGE_PCT (catches deal grinders)
  • penny       — last close < PRICE_FLOOR
  • illiquid    — median daily dollar-volume < DOLLAR_VOL_FLOOR
  • leveraged   — name matches a leveraged/inverse-ETF pattern (ticker_type is
                  unreliable: AMDL is tagged 'CS' despite being a 2x ETF — match `name`)
  • bad_mark    — trade-levels mark deviates > MARK_DEV_PCT from last close
                  (applied later, bounded to ranked qualifiers — needs a per-symbol call)
  • no_data     — symbol absent from the recent full-market window

Market window data comes from Polygon's GROUPED daily aggregates: ONE call returns
the whole US market for a day, so ~22 calls cover a 30-day window for the entire
2000+ universe with full-market-accurate high/low (NOT the IEX-compressed ranges
that made everything look frozen).
"""
from __future__ import annotations

import os
import re
import sqlite3
import statistics
from datetime import datetime, timedelta, timezone

import requests

SIGNAL_CENTER = os.environ.get("OLLIE_MACHINE_SC", "http://127.0.0.1:9000")
_POLY_BASE = "https://api.polygon.io"

# ── thresholds ──
FROZEN_RANGE_PCT = 3.0          # 30d (high-low)/close below this ⇒ frozen/deal-pending
PRICE_FLOOR = 5.0               # min last close
DOLLAR_VOL_FLOOR = 2_000_000.0  # min median daily $-volume
MARK_DEV_PCT = 20.0             # trade-levels mark vs last close max deviation
WINDOW_TRADING_DAYS = 22        # ~30 calendar days of trading

# leveraged / inverse ETF name patterns (ticker_type is unreliable — match the name)
_LEVERAGED_RE = re.compile(
    r"\b[23]x\b|bull\s*[23]|bear\s*[23]|ultra(pro|short)?|leveraged|inverse|"
    r"direxion daily|daily etf|\b[23]x (long|short)",
    re.I,
)

# reason priority — a symbol failing several is attributed to the first here
REASON_PRIORITY = ("no_data", "leveraged", "penny", "illiquid", "frozen")


def _polygon_key() -> str:
    key = os.environ.get("POLYGON_API_KEY", "")
    if not key:
        try:
            import config  # type: ignore
            key = getattr(config, "POLYGON_API_KEY", "") or ""
        except Exception:
            key = ""
    return key


def fetch_market_window(universe: set[str], trading_days: int = WINDOW_TRADING_DAYS) -> dict:
    """Build {symbol: {last_close, high, low, range_pct, med_dollar_vol, n_days}} for the
    universe via Polygon grouped-daily (~1 call per trading day, whole market each)."""
    key = _polygon_key()
    if not key:
        raise RuntimeError("POLYGON_API_KEY unavailable — cannot build tradeable-universe window")

    acc: dict[str, dict] = {}
    day = datetime.now(timezone.utc).date()
    got = 0
    scanned = 0
    while got < trading_days and scanned < trading_days + 18:
        scanned += 1
        ds = day.isoformat()
        day = day - timedelta(days=1)
        try:
            r = requests.get(
                f"{_POLY_BASE}/v2/aggs/grouped/locale/us/market/stocks/{ds}",
                params={"adjusted": "true", "apiKey": key},
                timeout=20,
            )
            if r.status_code != 200:
                continue
            results = r.json().get("results") or []
        except Exception:
            continue
        if not results:
            continue  # weekend / holiday
        got += 1
        is_latest = got == 1
        for row in results:
            sym = row.get("T")
            if sym not in universe:
                continue
            h, l, c, v = row.get("h"), row.get("l"), row.get("c"), row.get("v")
            vw = row.get("vw") or c
            if c is None:
                continue
            a = acc.setdefault(sym, {"highs": [], "lows": [], "dvols": [], "last_close": None})
            if h is not None:
                a["highs"].append(h)
            if l is not None:
                a["lows"].append(l)
            if v is not None and vw:
                a["dvols"].append(float(v) * float(vw))
            if is_latest:
                a["last_close"] = c

    out: dict[str, dict] = {}
    for sym, a in acc.items():
        if not a["highs"] or not a["lows"]:
            continue
        last_close = a["last_close"] or a["highs"][0]
        hi, lo = max(a["highs"]), min(a["lows"])
        rng_pct = ((hi - lo) / last_close * 100.0) if last_close else None
        med_dv = statistics.median(a["dvols"]) if a["dvols"] else 0.0
        out[sym] = {
            "last_close": last_close, "high": hi, "low": lo,
            "range_pct": rng_pct, "med_dollar_vol": med_dv, "n_days": len(a["highs"]),
        }
    return out


def load_meta(conn: sqlite3.Connection) -> dict:
    """{symbol: {name, ticker_type, avg_price, avg_volume}} from scan_universe."""
    out = {}
    for r in conn.execute("SELECT symbol, name, ticker_type, avg_price, avg_volume FROM scan_universe").fetchall():
        out[r[0]] = {"name": r[1] or "", "ticker_type": r[2], "avg_price": r[3], "avg_volume": r[4]}
    return out


def classify_universe(universe: list[str], window: dict, meta: dict) -> tuple[list[str], dict]:
    """Return (kept_symbols, dropped_by_reason). One primary reason per dropped symbol."""
    kept: list[str] = []
    dropped: dict[str, list[str]] = {k: [] for k in REASON_PRIORITY}
    for sym in universe:
        name = (meta.get(sym, {}) or {}).get("name", "")
        w = window.get(sym)
        reason = None
        if w is None:
            reason = "no_data"
        elif name and _LEVERAGED_RE.search(name):
            reason = "leveraged"
        elif (w["last_close"] or 0) < PRICE_FLOOR:
            reason = "penny"
        elif (w["med_dollar_vol"] or 0) < DOLLAR_VOL_FLOOR:
            reason = "illiquid"
        elif w["range_pct"] is not None and w["range_pct"] < FROZEN_RANGE_PCT:
            reason = "frozen"
        if reason:
            dropped[reason].append(sym)
        else:
            kept.append(sym)
    return kept, dropped


def trade_levels_mark(symbol: str) -> float | None:
    """Current trade-levels mark (for bad-mark sanity). Bounded use — ranked qualifiers only."""
    try:
        r = requests.get(f"{SIGNAL_CENTER}/api/trade-levels/{symbol.upper()}", timeout=6)
        if r.status_code != 200:
            return None
        return r.json().get("price")
    except Exception:
        return None


def mark_deviation(symbol: str, last_close: float | None) -> tuple[bool, float | None, float | None]:
    """(is_bad, mark, dev_pct). is_bad True when |mark-last_close|/last_close > MARK_DEV_PCT."""
    mark = trade_levels_mark(symbol)
    if mark is None or not last_close:
        return False, mark, None  # can't judge ⇒ don't drop on this filter
    dev = abs(mark - last_close) / last_close * 100.0
    return dev > MARK_DEV_PCT, mark, dev
