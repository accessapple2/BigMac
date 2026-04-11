"""
Open Interest Change Tracker
------------------------------
Takes a morning OI snapshot and compares it to midday to flag
strikes where OI grew > 20% (new positioning activity).

Table: oi_changes  (never dropped)
Endpoint: GET /api/ready-room/oi-changes
"""
from __future__ import annotations

import os
import sqlite3
import time
from datetime import date, datetime, timezone
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
        CREATE TABLE IF NOT EXISTS oi_changes (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol          TEXT NOT NULL DEFAULT 'SPY',
            snap_date       TEXT NOT NULL,
            snap_type       TEXT NOT NULL,  -- 'morning' | 'midday'
            strike          REAL,
            expiry          TEXT,
            opt_type        TEXT,
            open_interest   INTEGER,
            created_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    conn.close()


_init_db()


def _get_snaps(symbol: str = "SPY") -> list[Any]:
    try:
        from alpaca.data import OptionHistoricalDataClient
        from alpaca.data.requests import OptionSnapshotRequest

        api_key = os.environ.get("ALPACA_API_KEY", "")
        secret  = os.environ.get("ALPACA_SECRET_KEY", "")
        client  = OptionHistoricalDataClient(api_key, secret)

        req = OptionSnapshotRequest(underlying_symbols=[symbol])
        snaps = client.get_option_snapshot(req)
        return list(snaps.values()) if snaps else []
    except Exception:
        return []


def _parse_expiry(snap: Any) -> str:
    try:
        sym = getattr(snap, "symbol", "") or ""
        if len(sym) >= 15:
            dp = sym[3:9]
            d = date(2000 + int(dp[:2]), int(dp[2:4]), int(dp[4:6]))
            return d.isoformat()
    except Exception:
        pass
    return ""


def _build_oi_map(snaps: list[Any]) -> dict[str, int]:
    """Build {opt_symbol: oi} map."""
    result = {}
    for snap in snaps:
        sym = getattr(snap, "symbol", None)
        details = getattr(snap, "details", None)
        oi = getattr(details, "open_interest", None) if details else None
        if sym and oi is not None:
            result[sym] = int(oi)
    return result


def _save_snapshot(symbol: str, snap_type: str, snaps: list[Any]) -> None:
    today = date.today().isoformat()
    rows = []
    for snap in snaps:
        details = getattr(snap, "details", None)
        oi = getattr(details, "open_interest", None) if details else None
        strike = getattr(details, "strike_price", None) if details else None
        opt_type = str(getattr(details, "type", "")) if details else ""
        expiry = _parse_expiry(snap)
        if oi is not None:
            rows.append((symbol, today, snap_type, strike, expiry, opt_type, int(oi)))

    if not rows:
        return
    conn = sqlite3.connect(DB, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executemany("""
        INSERT INTO oi_changes (symbol, snap_date, snap_type, strike, expiry, opt_type, open_interest)
        VALUES (?,?,?,?,?,?,?)
    """, rows)
    conn.commit()
    conn.close()


def _load_morning_oi(symbol: str) -> dict[str, int]:
    """Load today's morning OI snapshot from DB, keyed by (strike, expiry, opt_type)."""
    today = date.today().isoformat()
    try:
        conn = sqlite3.connect(DB, timeout=30)
        rows = conn.execute("""
            SELECT strike, expiry, opt_type, open_interest
            FROM oi_changes
            WHERE symbol=? AND snap_date=? AND snap_type='morning'
        """, (symbol, today)).fetchall()
        conn.close()
        return {f"{r[0]}|{r[1]}|{r[2]}": r[3] for r in rows}
    except Exception:
        return {}


def _compare_oi(morning: dict[str, int], snaps: list[Any]) -> list[dict]:
    flagged = []
    for snap in snaps:
        details = getattr(snap, "details", None)
        oi = getattr(details, "open_interest", None) if details else None
        strike = getattr(details, "strike_price", None) if details else None
        opt_type = str(getattr(details, "type", "")) if details else ""
        expiry = _parse_expiry(snap)
        if oi is None or strike is None:
            continue

        key = f"{strike}|{expiry}|{opt_type}"
        am_oi = morning.get(key)
        if am_oi and am_oi > 0:
            change_pct = (int(oi) - am_oi) / am_oi * 100
            if change_pct >= 20:
                flagged.append({
                    "strike":     strike,
                    "expiry":     expiry,
                    "type":       opt_type,
                    "morning_oi": am_oi,
                    "current_oi": int(oi),
                    "change_pct": round(change_pct, 1),
                })

    flagged.sort(key=lambda x: x["change_pct"], reverse=True)
    return flagged[:10]


def take_morning_snapshot(symbol: str = "SPY") -> dict[str, Any]:
    """Call this at market open to record the morning OI baseline."""
    snaps = _get_snaps(symbol)
    _save_snapshot(symbol, "morning", snaps)
    return {"ok": True, "symbol": symbol, "snaps_saved": len(snaps), "snap_type": "morning"}


def get_oi_changes(symbol: str = "SPY", force: bool = False) -> dict[str, Any]:
    global _cache, _cache_ts
    now = time.time()
    if not force and _cache and (now - _cache_ts) < CACHE_TTL:
        return _cache

    morning_oi = _load_morning_oi(symbol)
    snaps = _get_snaps(symbol)

    if not morning_oi:
        result = {
            "symbol": symbol,
            "flagged": [],
            "signal": "No morning OI baseline found. Will auto-populate at next market open.",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
        _cache = result
        _cache_ts = now
        return result

    flagged = _compare_oi(morning_oi, snaps)

    if flagged:
        top = flagged[0]
        signal = (
            f"🔍 {len(flagged)} strike(s) with >20% OI growth. "
            f"Top: {top['type'].upper()} {top['strike']} exp {top['expiry']} "
            f"+{top['change_pct']}%"
        )
    else:
        signal = "✅ No unusual OI accumulation detected (all strikes <20% growth)."

    result = {
        "symbol":   symbol,
        "flagged":  flagged,
        "baseline_strikes": len(morning_oi),
        "current_snaps": len(snaps),
        "signal":   signal,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }

    _cache = result
    _cache_ts = now
    return result
