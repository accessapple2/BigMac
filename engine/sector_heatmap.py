"""
Sector Grid Display — Visual sector rotation display for USS TradeMinds.
Shares data with breadth_scanner to avoid double-fetching. Adds 5-day
momentum context and rotation signal from DB history.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone

from alpaca.data import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

logger = logging.getLogger(__name__)

_DB = os.environ.get("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))

SECTOR_ETFS = ["XLK", "XLF", "XLV", "XLE", "XLI", "XLC", "XLY", "XLP", "XLB", "XLRE", "XLU"]

SECTOR_META = {
    "XLK":  {"name": "Technology",       "type": "risk_on",   "defensive": False},
    "XLF":  {"name": "Financials",       "type": "risk_on",   "defensive": False},
    "XLV":  {"name": "Healthcare",       "type": "neutral",   "defensive": True},
    "XLE":  {"name": "Energy",           "type": "commodity", "defensive": False},
    "XLI":  {"name": "Industrials",      "type": "risk_on",   "defensive": False},
    "XLC":  {"name": "Communications",   "type": "risk_on",   "defensive": False},
    "XLY":  {"name": "Consumer Disc",    "type": "risk_on",   "defensive": False},
    "XLP":  {"name": "Consumer Staples", "type": "defensive", "defensive": True},
    "XLB":  {"name": "Materials",        "type": "cyclical",  "defensive": False},
    "XLRE": {"name": "Real Estate",      "type": "defensive", "defensive": True},
    "XLU":  {"name": "Utilities",        "type": "defensive", "defensive": True},
}

_CACHE_TTL = 300  # 5 minutes
_cache: dict = {}


def _init_db() -> None:
    conn = sqlite3.connect(_DB, timeout=30)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sector_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_date TEXT NOT NULL,
                snap_time TEXT NOT NULL,
                rotation_type TEXT,
                sector_leader TEXT,
                sector_laggard TEXT,
                spy_pct_change REAL,
                sector_data_json TEXT,
                momentum_5d_json TEXT,
                rotation_signal TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        conn.commit()
    finally:
        conn.close()


def _fetch_sector_changes_direct() -> dict[str, float]:
    """
    Fallback: fetch sector ETFs directly from Alpaca if breadth_scanner fails.
    """
    client = StockHistoricalDataClient(
        os.environ.get("ALPACA_API_KEY", ""),
        os.environ.get("ALPACA_SECRET_KEY", ""),
    )
    start = datetime.now(timezone.utc) - timedelta(days=5)
    req = StockBarsRequest(
        symbol_or_symbols=SECTOR_ETFS + ["SPY"],
        timeframe=TimeFrame.Day,
        start=start,
        limit=2,
        feed="iex",
    )
    bars = client.get_stock_bars(req)

    changes: dict[str, float] = {}
    for ticker in SECTOR_ETFS + ["SPY"]:
        raw = bars.data.get(ticker, [])
        if len(raw) < 2:
            changes[ticker] = 0.0
            continue
        yesterday_close = raw[-2].close
        today_bar = raw[-1]
        today_price = today_bar.close if today_bar.close else today_bar.open
        if yesterday_close and yesterday_close != 0:
            changes[ticker] = (today_price - yesterday_close) / yesterday_close * 100
        else:
            changes[ticker] = 0.0

    return changes


def _get_5day_leader_type() -> str | None:
    """
    Query the last 5 days of sector_snapshots to find the dominant leader type.
    Returns "risk_on", "defensive", or None if not enough data.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    try:
        conn = sqlite3.connect(_DB, timeout=30)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")
            rows = conn.execute(
                """
                SELECT sector_leader FROM sector_snapshots
                WHERE trade_date >= ?
                ORDER BY created_at DESC
                LIMIT 10
                """,
                (cutoff,),
            ).fetchall()
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("sector_heatmap: 5-day DB query failed: %s", exc)
        return None

    if len(rows) < 3:
        return None

    type_counts: dict[str, int] = {}
    for (leader,) in rows:
        if leader and leader in SECTOR_META:
            t = SECTOR_META[leader]["type"]
            type_counts[t] = type_counts.get(t, 0) + 1

    if not type_counts:
        return None

    return max(type_counts, key=lambda k: type_counts[k])


def _determine_rotation_signal(today_top3_types: list[str], five_day_leader_type: str | None) -> str:
    if five_day_leader_type is None:
        return "STABLE"

    defensive_types = {"defensive", "neutral"}
    risk_on_types = {"risk_on"}

    today_defensive = sum(1 for t in today_top3_types if t in defensive_types)
    today_risk_on = sum(1 for t in today_top3_types if t in risk_on_types)

    if today_defensive >= 2 and five_day_leader_type in risk_on_types:
        return "ROTATING_TO_DEFENSE"
    if today_risk_on >= 2 and five_day_leader_type in defensive_types:
        return "ROTATING_TO_OFFENSE"
    return "STABLE"


def _build_rotation_narrative(rotation_type: str) -> str:
    narratives = {
        "RISK_ON":    "🚀 Tech + Financials leading — risk appetite healthy",
        "RISK_OFF":   "🛡️ Defensive rotation — Utilities/Staples outperforming",
        "ENERGY_LED": "⛽ Energy leading — commodity/inflation trade",
        "MIXED":      "↔️ Mixed sector picture — no clear rotation theme",
    }
    return narratives.get(rotation_type, "↔️ Mixed sector picture — no clear rotation theme")


def _store_snapshot(data: dict, sector_data_json: str, momentum_5d_json: str) -> None:
    now = datetime.now(timezone.utc)
    trade_date = now.strftime("%Y-%m-%d")
    snap_time = now.strftime("%H:%M:%S")

    conn = sqlite3.connect(_DB, timeout=30)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute(
            """
            INSERT INTO sector_snapshots
                (trade_date, snap_time, rotation_type, sector_leader, sector_laggard,
                 spy_pct_change, sector_data_json, momentum_5d_json, rotation_signal)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trade_date,
                snap_time,
                data.get("rotation_type"),
                data.get("sector_leader"),
                data.get("sector_laggard"),
                data.get("spy_pct_change"),
                sector_data_json,
                momentum_5d_json,
                data.get("rotation_signal"),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_sector_heatmap(force: bool = False) -> dict:
    """
    Get sector heatmap data. Imports from breadth_scanner to avoid double-fetch.
    Adds 5-day momentum context and rotation signal.
    """
    now_ts = datetime.now(timezone.utc).timestamp()
    cached = _cache.get("sector_heatmap")
    if not force and cached and (now_ts - cached["ts"]) < _CACHE_TTL:
        return cached["data"]

    # Prefer breadth_scanner data to avoid double-fetching
    sector_changes: dict[str, float] = {}
    spy_pct = 0.0
    rotation_type = "MIXED"
    sector_leader = ""
    sector_laggard = ""
    error: str | None = None

    try:
        from engine.breadth_scanner import get_breadth_snapshot
        breadth_data = get_breadth_snapshot(force=force)
        if breadth_data.get("error"):
            raise RuntimeError(breadth_data["error"])

        raw_sectors = breadth_data.get("sectors", {})
        sector_changes = {t: raw_sectors[t]["pct_change"] for t in SECTOR_ETFS if t in raw_sectors}
        spy_pct = breadth_data.get("breadth_etfs", {}).get("SPY", {}).get("pct_change", 0.0)
        rotation_type = breadth_data.get("rotation_type", "MIXED")
        sector_leader = breadth_data.get("sector_leader", "")
        sector_laggard = breadth_data.get("sector_laggard", "")

    except Exception as exc:
        logger.warning("sector_heatmap: breadth_scanner unavailable, falling back: %s", exc)
        try:
            fallback = _fetch_sector_changes_direct()
            sector_changes = {t: fallback.get(t, 0.0) for t in SECTOR_ETFS}
            spy_pct = fallback.get("SPY", 0.0)

            ranked = sorted(SECTOR_ETFS, key=lambda t: sector_changes[t], reverse=True)
            sector_leader = ranked[0]
            sector_laggard = ranked[-1]

            top3 = set(ranked[:3])
            if {"XLK", "XLY", "XLF"}.issubset(top3):
                rotation_type = "RISK_ON"
            elif {"XLU", "XLP", "XLV"}.issubset(top3):
                rotation_type = "RISK_OFF"
            elif ranked[0] == "XLE" or ranked[1] == "XLE":
                rotation_type = "ENERGY_LED"
            else:
                rotation_type = "MIXED"

        except Exception as exc2:
            logger.error("sector_heatmap: direct Alpaca fallback also failed: %s", exc2)
            error = str(exc2)

    # Build ranked sector list for heatmap
    ranked_tickers = sorted(SECTOR_ETFS, key=lambda t: sector_changes.get(t, 0.0), reverse=True)
    sectors_out = []
    for rank, ticker in enumerate(ranked_tickers, start=1):
        pct = sector_changes.get(ticker, 0.0)
        meta = SECTOR_META.get(ticker, {"name": ticker, "type": "unknown", "defensive": False})
        sectors_out.append({
            "ticker": ticker,
            "name": meta["name"],
            "pct_change": round(pct, 4),
            "rank": rank,
            "direction": "up" if pct > 0 else "down",
            "type": meta["type"],
            "defensive": meta["defensive"],
            "color_intensity": round(min(1.0, abs(pct) / 2.0), 4),
        })

    # 5-day rotation signal
    today_top3_types = [
        SECTOR_META.get(ranked_tickers[i], {}).get("type", "unknown")
        for i in range(min(3, len(ranked_tickers)))
    ]
    five_day_leader_type = _get_5day_leader_type()
    rotation_signal = _determine_rotation_signal(today_top3_types, five_day_leader_type)
    rotation_narrative = _build_rotation_narrative(rotation_type)

    # Build 5-day momentum JSON from breadth DB history
    momentum_5d: dict = {}
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
        conn = sqlite3.connect(_DB, timeout=30)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")
            rows = conn.execute(
                """
                SELECT sector_data_json FROM breadth_snapshots
                WHERE trade_date >= ?
                ORDER BY created_at DESC
                LIMIT 5
                """,
                (cutoff,),
            ).fetchall()
        finally:
            conn.close()

        if rows:
            ticker_totals: dict[str, list[float]] = {t: [] for t in SECTOR_ETFS}
            for (json_str,) in rows:
                try:
                    snap = json.loads(json_str or "{}")
                    for ticker in SECTOR_ETFS:
                        if ticker in snap:
                            ticker_totals[ticker].append(snap[ticker].get("pct_change", 0.0))
                except Exception:
                    pass
            momentum_5d = {
                t: round(sum(vals) / len(vals), 4)
                for t, vals in ticker_totals.items()
                if vals
            }
    except Exception as exc:
        logger.warning("sector_heatmap: 5-day momentum calc failed: %s", exc)

    sector_data_json = json.dumps({s["ticker"]: s for s in sectors_out})
    momentum_5d_json = json.dumps(momentum_5d)

    result = {
        "sectors": sectors_out,
        "rotation_type": rotation_type,
        "rotation_signal": rotation_signal,
        "sector_leader": sector_leader,
        "sector_laggard": sector_laggard,
        "spy_pct_change": round(spy_pct, 4),
        "rotation_narrative": rotation_narrative,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "error": error,
    }

    try:
        _store_snapshot(result, sector_data_json, momentum_5d_json)
    except Exception as exc:
        logger.warning("sector_heatmap: DB store failed: %s", exc)

    _cache["sector_heatmap"] = {"ts": now_ts, "data": result}
    return result


def get_sector_rotation_narrative() -> str:
    """Quick call returning just the rotation_narrative for briefing injection."""
    heatmap = get_sector_heatmap()
    return heatmap.get("rotation_narrative", "")


_init_db()
