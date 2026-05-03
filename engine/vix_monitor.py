"""
VIX Term Structure Monitor
--------------------------
Fetches VIX / VIX9D / VIX3M / VIX6M from yfinance and classifies the
term structure state (contango vs backwardation).

Table: vix_term_structure  (never dropped)
Endpoint: GET /api/ready-room/vix
"""
from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timezone
from typing import Any

DB = "autonomous_trader.db"

_cache: dict[str, Any] = {}
_cache_ts: float = 0.0
CACHE_TTL = 300


def _init_db() -> None:
    conn = sqlite3.connect(DB, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS vix_term_structure (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            vix         REAL,
            vix9d       REAL,
            vix3m       REAL,
            vix6m       REAL,
            slope_9d    REAL,
            slope_3m    REAL,
            slope_6m    REAL,
            state       TEXT,
            regime      TEXT,
            signal      TEXT,
            created_at  TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    conn.close()


_init_db()


def _fetch_vix_levels() -> dict[str, Any]:
    import requests as _requests

    def _yahoo_last(sym: str) -> float | None:
        """Fetch latest close for a VIX index via Yahoo Finance HTTP API."""
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/%5E{sym.lstrip('^')}"
            resp = _requests.get(
                url,
                params={"interval": "1d", "range": "5d"},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=8,
            )
            resp.raise_for_status()
            meta = resp.json().get("chart", {}).get("result", [{}])[0].get("meta", {})
            price = meta.get("regularMarketPrice")
            return float(price) if price is not None else None
        except Exception:
            return None

    try:
        return {
            "vix":   _yahoo_last("^VIX"),
            "vix9d": _yahoo_last("^VIX9D"),
            "vix3m": _yahoo_last("^VIX3M"),
            "vix6m": _yahoo_last("^VIX6M"),
        }
    except Exception as exc:
        return {"vix": None, "vix9d": None, "vix3m": None, "vix6m": None, "error": str(exc)}


def _classify(vix, vix9d, vix3m, vix6m):
    if vix is None:
        return "UNKNOWN", "UNKNOWN", "Insufficient VIX data", None, None, None

    slope_9d = round(vix - vix9d, 2) if vix9d is not None else None
    slope_3m = round(vix3m - vix, 2) if vix3m is not None else None
    slope_6m = round(vix6m - vix, 2) if vix6m is not None else None

    if vix < 15:
        regime = "CALM"
    elif vix < 20:
        regime = "ELEVATED"
    elif vix < 30:
        regime = "STRESSED"
    else:
        regime = "CRISIS"

    if vix3m is not None and vix > vix3m:
        state = "BACKWARDATION"
        signal = f"🚨 Full backwardation — VIX ({vix:.1f}) > VIX3M ({vix3m:.1f}). Tail-risk event possible."
    elif vix9d is not None and vix3m is not None:
        spread = abs(vix - vix9d) + abs(vix3m - vix)
        if spread < 2.0:
            state = "FLAT"
            signal = "Flat term structure — market in wait-and-see mode."
        elif vix9d > vix or (vix3m is not None and vix > vix3m * 0.97):
            state = "PARTIAL_BACKWARDATION"
            signal = "⚠️ Partial inversion — near-term fear elevated. Proceed with caution."
        else:
            state = "CONTANGO"
            signal = "Normal contango — hedges cheap near-term. Trend-following favored."
    else:
        state = "UNKNOWN"
        signal = "Partial VIX data — term structure inconclusive."

    return state, regime, signal, slope_9d, slope_3m, slope_6m


def get_vix_term_structure(force: bool = False) -> dict[str, Any]:
    global _cache, _cache_ts
    now = time.time()
    if not force and _cache and (now - _cache_ts) < CACHE_TTL:
        return _cache

    raw = _fetch_vix_levels()
    vix   = raw.get("vix")
    vix9d = raw.get("vix9d")
    vix3m = raw.get("vix3m")
    vix6m = raw.get("vix6m")

    # If ^VIX was rate-limited, fall back to the last known good value from DB
    if vix is None:
        try:
            conn = sqlite3.connect(DB, timeout=10)
            row = conn.execute(
                "SELECT vix FROM vix_term_structure WHERE vix IS NOT NULL ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            conn.close()
            if row:
                vix = row[0]
        except Exception:
            pass

    state, regime, signal, slope_9d, slope_3m, slope_6m = _classify(vix, vix9d, vix3m, vix6m)

    result: dict[str, Any] = {
        "vix":      vix,
        "vix9d":    vix9d,
        "vix3m":    vix3m,
        "vix6m":    vix6m,
        "slope_9d": slope_9d,
        "slope_3m": slope_3m,
        "slope_6m": slope_6m,
        "state":    state,
        "regime":   regime,
        "signal":   signal,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }
    if "error" in raw:
        result["fetch_error"] = raw["error"]

    try:
        conn = sqlite3.connect(DB, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""
            INSERT INTO vix_term_structure
                (vix, vix9d, vix3m, vix6m, slope_9d, slope_3m, slope_6m, state, regime, signal)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (vix, vix9d, vix3m, vix6m, slope_9d, slope_3m, slope_6m, state, regime, signal))
        conn.commit()
        conn.close()
    except Exception:
        pass

    _cache = result
    _cache_ts = now
    return result


def get_latest_vix_snapshot() -> dict[str, Any]:
    try:
        conn = sqlite3.connect(DB, timeout=30)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM vix_term_structure ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        return dict(row) if row else {}
    except Exception as exc:
        return {"error": str(exc)}


def check_vix_spike(threshold_pct: float = 5.0) -> dict[str, Any] | None:
    """Return spike dict if VIX moved >= threshold_pct vs previous snapshot, else None."""
    try:
        conn = sqlite3.connect(DB, timeout=10)
        rows = conn.execute(
            "SELECT vix FROM vix_term_structure ORDER BY id DESC LIMIT 2"
        ).fetchall()
        conn.close()
        if len(rows) < 2 or rows[0][0] is None or rows[1][0] is None:
            return None
        current, previous = rows[0][0], rows[1][0]
        if previous == 0:
            return None
        change_pct = (current - previous) / previous * 100
        if abs(change_pct) >= threshold_pct:
            return {"price": current, "change_pct": change_pct}
        return None
    except Exception:
        return None


def get_vix_status() -> dict[str, Any] | None:
    """Return current VIX price and recent change from latest two snapshots."""
    try:
        conn = sqlite3.connect(DB, timeout=10)
        rows = conn.execute(
            "SELECT vix FROM vix_term_structure ORDER BY id DESC LIMIT 2"
        ).fetchall()
        conn.close()
        if not rows or rows[0][0] is None:
            return None
        current = rows[0][0]
        previous = rows[1][0] if len(rows) > 1 and rows[1][0] else current
        change_pct = (current - previous) / previous * 100 if previous else 0.0
        return {"price": current, "change_pct": change_pct}
    except Exception:
        return None
